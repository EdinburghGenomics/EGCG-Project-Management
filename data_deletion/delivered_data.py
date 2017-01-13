import glob
import os
import uuid
from datetime import datetime
from os import listdir
from os.path import join, isdir

from egcg_core import rest_communication, util, clarity, app_logging
from egcg_core.constants import ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_PROJECT_ID, ELEMENT_RUN_NAME, ELEMENT_LANE, \
    ELEMENT_SAMPLE_EXTERNAL_ID
from egcg_core.config import cfg

from data_deletion import Deleter
from data_deletion.archive_management import is_archived, ArchivingError, release_file_from_lustre


def get_file_list_size(file_list):
    """
    Get the size of the files after collapsing all file based on there inodes to avoid counting the hard links more than once.
    """
    def get_files_inode(file_list):
        """
        Collapse the files found in the list based on their inodes and perform the same recursively for directories
        """
        inode2file = {}
        for f in file_list:
            if os.path.isdir(f):
                inode2file.update(
                    get_files_inode([os.path.join(f, s) for s in os.listdir(f)])
                )
            else:
                inode2file[os.stat(f).st_ino] = f
        return inode2file

    # get the uniq inodes
    inode2file = get_files_inode(file_list)
    # get the size of the uniqued inodes
    return sum([os.stat(f).st_size for f in inode2file.values()])


class ProcessedSample(app_logging.AppLogger):

    def __init__(self, sample_data):
        self.raw_data_dir = cfg['data_deletion']['fastqs']
        self.processed_data_dir = cfg['data_deletion']['processed_data']
        self.delivered_data_dir = cfg['data_deletion']['delivered_data']
        self.sample_data = sample_data
        self._release_date = None
        self._release_folders = None
        self._run_elements = None
        self._files_to_purge = None
        self._files_to_remove_from_lustre = None
        self._size_of_files = None

    @property
    def release_date(self):
        if not self._release_date:
            self._release_date = clarity.get_sample_release_date(self.sample_id)
        return self._release_date

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
    @property
    def run_elements(self):
        if self._run_elements is None:
            self._run_elements = rest_communication.get_documents(
                'run_elements', quiet=True, where={ELEMENT_SAMPLE_INTERNAL_ID: self.sample_id}, all_pages=True
            )
        return self._run_elements

    def _raw_data_files(self):
        all_fastqs = []
        for e in self.run_elements:
            assert e[ELEMENT_SAMPLE_INTERNAL_ID] == self.sample_id
            fastqs = self._find_fastqs_for_run_element(e)
            if fastqs:
                all_fastqs.extend(fastqs)
            else:
                self.warning(
                    'Found %s fastq files for run %s lane %s sample %s',
                    len(fastqs),
                    e[ELEMENT_RUN_NAME],
                    e[ELEMENT_LANE],
                    self.sample_id
                )
        return  all_fastqs

    def _processed_data_files(self):
        files_to_delete = []
        # TODO: first check what type of analysis was perfomed on this data to know exactly which files need to be deleted
        files_to_search = [
            '{ext_s_id}_R1.fastq.gz', '{ext_s_id}_R2.fastq.gz',
            '{ext_s_id}.bam', '{ext_s_id}.bam.bai',
            '{ext_s_id}.vcf.gz', '{ext_s_id}.vcf.gz.tbi',
            '{ext_s_id}.g.vcf.gz', '{ext_s_id}.g.vcf.gz.tbi'
        ]
        for f in files_to_search:
            file_to_delete = util.find_file(
                self.processed_data_dir,
                self.project_id, self.sample_id,
                f.format(ext_s_id=self.external_sample_id)
            )
            if file_to_delete:
                files_to_delete.append(file_to_delete)
        return files_to_delete

    @property
    def released_data_folder(self):
        if self._release_folders is None:
            self._release_folders = util.find_files(self.delivered_data_dir, self.project_id, '*', self.sample_id)
        if len(self._release_folders) != 1:
            self.warning(
                'Found %s deletable directories for sample %s: %s',
                len(self._release_folders),
                self.sample_id,
                self._release_folders
            )
        else:
            return self._release_folders[0]

    @property
    def files_to_purge(self):
        if not self._files_to_purge:
            self._files_to_purge = []
            release_folder = self.released_data_folder
            if release_folder:
                self._files_to_purge.append(release_folder)
        return self._files_to_purge

    def files_to_remove_from_lustre(self):
        if not self._files_to_remove_from_lustre:
            self._files_to_remove_from_lustre = []
            raw_files = self._raw_data_files()
            if raw_files:
                self._files_to_remove_from_lustre.extend(raw_files)

            processed_files = self._processed_data_files()
            if processed_files:
                self._files_to_remove_from_lustre.extend(processed_files)
            for f in self._files_to_remove_from_lustre:
                if not is_archived(f):
                    raise ArchivingError('File %s is not archived and thus cannot be released from Lustre' % f)
        return self._files_to_remove_from_lustre


    @property
    def size_of_files(self):
        if not self._size_of_files:
            self._size_of_files = get_file_list_size(self.files_to_purge)
            self._size_of_files += get_file_list_size(self.files_to_remove_from_lustre)
        return self._size_of_files

    def mark_as_deleted(self):
        proc_id = self.sample_data.get('most_recent_proc', {}).get('proc_id')
        if not proc_id:
            self.warning('No pipeline process found for ' + self.sample_id)
        else:
            rest_communication.patch_entry(
                'analysis_driver_procs',
                {'status': 'deleted'},
                'proc_id', proc_id
            )


    def __repr__(self):
        return self.sample_id + ' (%s)'%self.release_date



class DeliveredDataDeleter(Deleter):

    def __init__(self, work_dir, dry_run=False, deletion_limit=None, manual_delete=None, sample_ids=None):
        super().__init__(work_dir, dry_run, deletion_limit)
        self.data_dir = self.work_dir
        self.raw_data_dir = cfg['data_deletion']['fastqs']
        self.raw_archive_dir = cfg['data_deletion']['fastq_archives']
        self.processed_data_dir = cfg['data_deletion']['processed_data']
        self.processed_archive_dir = cfg['data_deletion']['processed_archives']
        self.delivered_data_dir = cfg['data_deletion']['delivered_data']
        self.list_samples = []
        if manual_delete is not None:
            self.list_samples = manual_delete
        self.limit_samples = sample_ids

    @staticmethod
    def _get_sample_from_list(list_samples):
        max_query = 20
        manual_samples = []
        for start in range(0, len(list_samples), max_query):

            manual_samples.extend(rest_communication.get_documents(
                'aggregate/samples',
                quiet=True,
                match={'$or': [{'sample_id': s} for s in list_samples[start:start+max_query]]},
                paginate=False
            ))
        return manual_samples

    def deletable_samples(self):
        manual_samples = []
        if self.list_samples:
            manual_samples = self._get_sample_from_list(self.list_samples)
            manual_samples = [ProcessedSample(s) for s in manual_samples]

        return sorted(manual_samples + self._auto_deletable_samples(), key=lambda e: e.sample_data['sample_id'])

    def _auto_deletable_samples(self):
        samples = []
        # FIXME: _auto_deletable_samples is disabled until we create a deletion step in the LIMS
        # as the date is not enough to be sure data can be deleted.
        return samples
        sample_records = rest_communication.get_documents(
            'aggregate/samples',
            quiet=True,
            match={'proc_status': 'finished', 'useable': 'yes', 'delivered': 'yes'},
            paginate=False
        )
        for r in sample_records:
            s = ProcessedSample(r)
            # TODO: check that the sample went through the deletion workflow in the LIMS
            if s.release_date and self._old_enough_for_deletion(s.release_date):
                samples.append(s)
        return samples

    def _move_to_unique_file_name(self, source, dest_dir):
        source_name = os.path.basename(source)
        dest = join(dest_dir, str(uuid.uuid4()) + '_' + source_name)
        self._execute('mv %s %s'%(source, dest))


    def setup_samples_for_deletion(self, samples, dry_run):
        total_size_to_delete = 0
        for s in samples:
            total_size_to_delete += s.size_of_files
            deletable_data_dir = join(self.deletion_dir, s.sample_id)
            if not dry_run:
                if len(s.files_to_purge):
                    self._execute('mkdir -p ' + deletable_data_dir)
                    for f in s.files_to_purge:
                        self._move_to_unique_file_name(f, deletable_data_dir)
                if len(s.files_to_remove_from_lustre):
                    for f in s.files_to_remove_from_lustre:
                        release_file_from_lustre(f)
            else:
                self.info(
                    'Sample %s has %s files to delete and %s file to remove from lustre (%.2f G)\n%s\n%s',
                    s,
                    len(s.files_to_purge),
                    len(s.files_to_remove_from_lustre),
                    s.size_of_files/1000000000,
                    '\n'.join(s.files_to_purge),
                    '\n'.join(s.files_to_remove_from_lustre)
                )
                if len(s.files_to_purge):
                    self.info('Will run: mv %s %s' % (' '.join(s.files_to_purge), deletable_data_dir))
                if len(s.files_to_remove_from_lustre):
                    self.info('Will run: %s' % ('\n'.join(['lfs hsm_release %s' % f for f in s.files_to_remove_from_lustre])))
        self.info('Will delete %.2f G of data' % (total_size_to_delete / 1000000000))



    @classmethod
    def _old_enough_for_deletion(cls, date_run, age_threshold=90):
        year, month, day = date_run.split('-')
        age = cls._now() - datetime(int(year), int(month), int(day))
        return age.days > age_threshold

    def _try_delete_empty_dir(self, d):
        if isdir(d) and not listdir(d):
            self._execute('rm -r ' + d)

    def _try_archive_run(self, run_name):
        #look for any fastq files in any project/sample directory
        all_fastqs = glob.glob(join(self.raw_data_dir, run_name, '*', '*', '*.fastq.gz'))
        if all_fastqs:
            return
        # There are no fastqs in that run
        self.info('Archive run '+ run_name)
        # Find the undetermined
        undetermined_fastqs = glob.glob(join(self.raw_data_dir, run_name, 'Undetermined_*.fastq.gz'))
        for f in undetermined_fastqs:
            self._execute('rm '+ f)
        if os.path.exists(join(self.raw_data_dir, run_name)):
            self._execute('mv %s %s'%(join(self.raw_data_dir, run_name), self.raw_archive_dir))

    def _try_archive_project(self, project_id):
        sample_records = rest_communication.get_documents(
            'aggregate/samples',
            quiet=True,
            match={'project_id': project_id},
            paginate=False
        )
        status = list(set([s.status for s in sample_records]))
        if len(status) != 1 or status[0] != 'deleted':
            return
        self.info('Archive project '+ project_id)
        if os.path.exists(join(self.processed_data_dir, project_id)):
            self._execute('mv %s %s' % (join(self.processed_data_dir, project_id), self.processed_archive_dir))


    def delete_data(self):
        deletable_samples = self.deletable_samples()
        if self.limit_samples:
            deletable_samples = [s for s in deletable_samples if s.sample_data['sample_id'] in self.limit_samples]

        sample_ids = [str(e.sample_id) for e in deletable_samples]
        self.debug('Found %s samples for deletion: %s' % (len(deletable_samples), sample_ids))
        self.setup_samples_for_deletion(deletable_samples, self.dry_run)

        if not self.deletable_samples or self.dry_run :
            return 0

        for s in deletable_samples:
            s.mark_as_deleted()

        if self.deletion_dir and os.path.isdir(self.deletion_dir):
            self.delete_dir(self.deletion_dir)
        # Data has been deleted now clean up empty directories

        # Remove release batch directories if they are empty
        for folder in set([os.path.dirname(s.released_data_folder) for s in deletable_samples if s.released_data_folder]):
            self._try_delete_empty_dir(folder)

        # Archive run folders if they do not contain any fastq file anymore
        for run_name in set([r[ELEMENT_RUN_NAME] for r in s.run_elements for s in deletable_samples]):
            self._try_archive_run(run_name)

        # Archive project folders if their samples have been deleted
        for project_id in set([r[ELEMENT_PROJECT_ID] for r in s.run_elements for s in deletable_samples]):
            self._try_archive_project(project_id)

