import sys
import argparse
import traceback
from os import listdir, stat
from os.path import join, isdir, expanduser
from datetime import datetime
from cached_property import cached_property
from egcg_core import app_logging, executor, clarity, rest_communication, util, notifications
from egcg_core.archive_management import is_archived, ArchivingError
from egcg_core.config import cfg
from egcg_core.exceptions import EGCGError
from egcg_core.constants import ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_PROJECT_ID, ELEMENT_SAMPLE_EXTERNAL_ID, \
    ELEMENT_RUN_NAME, ELEMENT_LANE


def get_file_list_size(file_list):
    """
    Get the total size of all files. Collapses them by inodes to avoid counting hard links more than once.
    Also descends into directories recursively.
    """
    def files_by_inode(_file_list):
        _inode2file = {}
        for f in _file_list:
            if isdir(f):
                _inode2file.update(files_by_inode(join(f, s) for s in listdir(f)))
            else:
                _inode2file[stat(f).st_ino] = f
        return _inode2file

    return sum(stat(f).st_size for f in files_by_inode(file_list).values())


class Deleter(app_logging.AppLogger):
    alias = None

    def __init__(self, cmd_args=None):
        """
        :param  cmd_args:
        """
        self.cmd_args = cmd_args
        if not self.cmd_args:
            a = argparse.ArgumentParser()
            self.add_args(a)
            self.cmd_args = a.parse_args()

        self.work_dir = self.cmd_args.work_dir
        self.dry_run = self.cmd_args.dry_run
        self.deletion_limit = self.cmd_args.deletion_limit
        self.manual_delete = self.cmd_args.manual_delete
        self.ntf = notifications.NotificationCentre('%s at %s' % (self.__class__.__name__, self._strnow()))

    @staticmethod
    def add_args(argparser):
        """
        :param argparse.ArgumentParser argparser:
        """
        argparser.add_argument('--dry_run', action='store_true')
        argparser.add_argument('--work_dir', default=expanduser('~'))
        argparser.add_argument('--deletion_limit', type=int, default=None)
        argparser.add_argument('--manual_delete', type=str, nargs='+', default=[])

    @cached_property
    def deletion_dir(self):  # need caching because of reference to datetime.now
        return join(self.work_dir, '.data_deletion_' + self._strnow())

    def delete_dir(self, d):
        self.debug('Removing dir %s containing: %s', d, listdir(d))
        self._execute('rm -rfv ' + d, cluster_execution=True)

    def _execute(self, cmd, cluster_execution=False):
        if not cluster_execution:
            status = executor.local_execute(cmd).join()
        else:
            status = executor.cluster_execute(cmd, job_name='data_deletion', working_dir=self.work_dir).join()
        if status:
            raise EGCGError('Command failed: ' + cmd)

    def _compare_lists(self, observed, expected, error_message='List comparison mismatch:'):
        observed = sorted(observed)
        expected = sorted(expected)
        if observed != expected:
            self.error(error_message)
            self.error('observed: ' + str(observed))
            self.error('expected: ' + str(expected))
            raise AssertionError

    @staticmethod
    def _now():
        return datetime.utcnow()

    @classmethod
    def _strnow(cls):
        return cls._now().strftime('%d_%m_%Y_%H:%M:%S')

    def delete_data(self):
        """
        The main behaviour of the Deleter
        :return: None
        """
        raise NotImplementedError

    def run(self):
        """Runs self.delete_data with exception handling and notifications."""
        try:
            self.delete_data()
        except Exception as e:
            etype, value, tb = sys.exc_info()
            stacktrace = ''.join(traceback.format_exception(etype, value, tb))
            self.critical('Encountered a %s exception: %s. Stacktrace below:\n%s', e.__class__.__name__, e, stacktrace)
            self.ntf.notify_all(stacktrace)
            executor.stop_running_jobs()
            sys.exit(9)


class ProcessedSample(app_logging.AppLogger):
    def __init__(self, sample_data):
        self.sample_data = sample_data

    @cached_property
    def release_date(self):
        return clarity.get_sample_release_date(self.sample_id)

    @property
    def sample_id(self):
        return self.sample_data[ELEMENT_SAMPLE_INTERNAL_ID]

    @property
    def project_id(self):
        return self.sample_data[ELEMENT_PROJECT_ID]

    @property
    def external_sample_id(self):
        return self.sample_data[ELEMENT_SAMPLE_EXTERNAL_ID]

    @staticmethod
    def _find_fastqs_for_run_element(run_element):
        return util.find_fastqs(
            join(cfg['data_deletion']['fastqs'], run_element[ELEMENT_RUN_NAME]),
            run_element[ELEMENT_PROJECT_ID],
            run_element[ELEMENT_SAMPLE_INTERNAL_ID],
            lane=run_element[ELEMENT_LANE]
        )

    @cached_property
    def run_elements(self):
        return rest_communication.get_documents(
            'run_elements', quiet=True, where={ELEMENT_SAMPLE_INTERNAL_ID: self.sample_id}, all_pages=True
        )

    @cached_property
    def raw_data_files(self):
        all_fastqs = []
        for e in self.run_elements:
            assert e[ELEMENT_SAMPLE_INTERNAL_ID] == self.sample_id
            fastqs = self._find_fastqs_for_run_element(e)
            if fastqs:
                all_fastqs.extend(fastqs)
            else:
                self.warning(
                    'No fastqs found for run %s lane %s sample %s', e[ELEMENT_RUN_NAME], e[ELEMENT_LANE], self.sample_id
                )
        return all_fastqs

    @cached_property
    def processed_data_files(self):
        files = []
        # TODO: check what type of analysis was perfomed on this data to know exactly which files should be there
        file_extensions = ('_R1.fastq.gz', '_R2.fastq.gz', '.bam', '.bam.bai',
                           '.vcf.gz', '.vcf.gz.tbi', '.g.vcf.gz', '.g.vcf.gz.tbi')

        for ext in file_extensions:
            f = util.find_file(
                cfg['data_deletion']['processed_data'],
                self.project_id,
                self.sample_id,
                self.external_sample_id + ext
            )
            if f:
                files.append(f)
        return files

    @cached_property
    def released_data_folder(self):
        release_folders = util.find_files(cfg['data_deletion']['delivered_data'], self.project_id, '*', self.sample_id)
        if len(release_folders) != 1:
            self.warning(
                'Found %s deletable directories for sample %s: %s',
                len(release_folders),
                self.sample_id,
                release_folders
            )
        else:
            return release_folders[0]

    @cached_property
    def files_to_purge(self):
        if self.released_data_folder:
            return util.find_files(self.released_data_folder, '*')
        return []

    @cached_property
    def files_to_remove_from_lustre(self):
        _files_to_remove_from_lustre = []
        raw_files = self.raw_data_files
        if raw_files:
            _files_to_remove_from_lustre.extend(raw_files)

        processed_files = self.processed_data_files
        if processed_files:
            _files_to_remove_from_lustre.extend(processed_files)

        unarchived_files = [f for f in _files_to_remove_from_lustre if not is_archived(f)]
        if unarchived_files:
            raise ArchivingError('Unarchived files cannot be released from Lustre: %s' % _files_to_remove_from_lustre)
        return _files_to_remove_from_lustre

    @cached_property
    def size_of_files(self):
        return get_file_list_size(self.files_to_purge) + get_file_list_size(self.files_to_remove_from_lustre)

    def mark_as_deleted(self):
        rest_communication.patch_entry('samples', {'data_deleted': 'on lustre'}, 'sample_id', self.sample_id)

    def __repr__(self):
        return self.sample_id + ' (%s)' % self.release_date
