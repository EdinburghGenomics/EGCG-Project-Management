from os import listdir, stat
from os.path import join, isdir
from datetime import datetime
from cached_property import cached_property
from egcg_core import app_logging, executor, clarity, rest_communication, util
from egcg_core.app_logging import AppLogger
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


class DataDeletionError(EGCGError):
    pass


class Deleter(app_logging.AppLogger):
    data_dir = ''
    local_execute_only = False

    def __init__(self, work_dir, dry_run=False, deletion_limit=None):
        self.work_dir = work_dir
        self.dry_run = dry_run
        self.deletion_limit = deletion_limit

    @cached_property
    def deletion_dir(self):  # need caching because of reference to datetime.now
        return join(self.data_dir, '.data_deletion_' + self._strnow())

    def delete_dir(self, d):
        self.debug('Removing deletion dir containing: %s', listdir(d))
        self._execute('rm -rfv ' + d, cluster_execution=True)

    def _execute(self, cmd, cluster_execution=False):
        if self.local_execute_only or not cluster_execution:
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


class ProcessedSample(AppLogger):
    def __init__(self, sample_data):
        self.raw_data_dir = cfg['data_deletion']['fastqs']
        self.processed_data_dir = cfg['data_deletion']['processed_data']
        self.delivered_data_dir = cfg['data_deletion']['delivered_data']
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

    def _find_fastqs_for_run_element(self, run_element):
        return util.find_fastqs(
            join(self.raw_data_dir, run_element[ELEMENT_RUN_NAME]),
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
                self.processed_data_dir,
                self.project_id,
                self.sample_id,
                self.external_sample_id + ext
            )
            if f:
                files.append(f)
        return files

    @cached_property
    def released_data_folder(self):
        release_folders = util.find_files(self.delivered_data_dir, self.project_id, '*', self.sample_id)
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
        _files_to_purge = []
        release_folder = self.released_data_folder
        if release_folder:
            _files_to_purge.append(release_folder)
        return _files_to_purge

    @cached_property
    def files_to_remove_from_lustre(self):
        _files_to_remove_from_lustre = []
        raw_files = self.raw_data_files
        if raw_files:
            _files_to_remove_from_lustre.extend(raw_files)

        processed_files = self.processed_data_files
        if processed_files:
            _files_to_remove_from_lustre.extend(processed_files)
        for f in _files_to_remove_from_lustre:
            if not is_archived(f):
                raise ArchivingError('File %s is not archived so cannot be released from Lustre' % f)
        return _files_to_remove_from_lustre

    @cached_property
    def size_of_files(self):
        return get_file_list_size(self.files_to_purge) + get_file_list_size(self.files_to_remove_from_lustre)

    def mark_as_deleted(self):
        rest_communication.patch_entry(
            'samples',
            {'data_deleted': 'on lustre'},
            'sample_id', self.sample_id
        )

    def __repr__(self):
        return self.sample_id + ' (%s)' % self.release_date
