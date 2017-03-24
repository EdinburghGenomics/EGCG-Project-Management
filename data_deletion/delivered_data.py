import os
import uuid
from datetime import datetime
from os import listdir
from os.path import join, isdir
from egcg_core import rest_communication, util
from egcg_core.config import cfg
from data_deletion import Deleter, ProcessedSample
from egcg_core.archive_management import release_file_from_lustre


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
        return []
        # FIXME: disabled until we have a LIMS deletion step (date is not enough to be sure data is deletable)

        # sample_records = rest_communication.get_documents(
        #     'aggregate/samples',
        #     quiet=True,
        #     match={'proc_status': 'finished', 'useable': 'yes', 'delivered': 'yes', 'data_deleted': 'none'},
        #     paginate=False
        # )
        # for r in sample_records:
        #     s = ProcessedSample(r)
        #     # TODO: check that the sample went through the deletion workflow in the LIMS
        #     if s.release_date and self._old_enough_for_deletion(s.release_date):
        #         samples.append(s)
        # return samples

    def _move_to_unique_file_name(self, source, dest_dir):
        source_name = os.path.basename(source)
        dest = join(dest_dir, str(uuid.uuid4()) + '_' + source_name)
        self._execute('mv %s %s' % (source, dest))

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
        # look for any fastq files in any project/sample directory (but not Undetermined in the top level)
        all_fastqs = util.find_files(self.raw_data_dir, run_name, '*', '*', '*.fastq.gz')
        if all_fastqs:
            return

        # There are no fastqs in that run
        self.info('Archive run ' + run_name)
        # Find the undetermined
        undetermined_fastqs = util.find_files(self.raw_data_dir, run_name, 'Undetermined_*.fastq.gz')
        for f in undetermined_fastqs:
            self._execute('rm ' + f)
        if os.path.exists(join(self.raw_data_dir, run_name)):
            self._execute('mv %s %s' % (join(self.raw_data_dir, run_name), self.raw_archive_dir))

    def _try_archive_project(self, project_id):
        sample_records = rest_communication.get_documents(
            'samples',
            quiet=True,
            where={'project_id': project_id},
            all_pages=True
        )
        deletion_status = list(set([s.get('data_deleted') for s in sample_records]))
        if len(deletion_status) != 1 or deletion_status[0] != 'deleted':
            return

        # if the project has a finished date, then all the samples required are done
        project_status = rest_communication.get_documents('lims/status/project_status', match={'project_id': project_id})
        if project_status.get('date_finished'):
            self.info('Archive project ' + project_id)
            if os.path.exists(join(self.processed_data_dir, project_id)):
                self._execute('mv %s %s' % (join(self.processed_data_dir, project_id), self.processed_archive_dir))

    def delete_data(self):
        deletable_samples = self.deletable_samples()
        if self.limit_samples:
            deletable_samples = [s for s in deletable_samples if s.sample_data['sample_id'] in self.limit_samples]

        sample_ids = [str(e.sample_id) for e in deletable_samples]
        self.debug('Found %s samples for deletion: %s' % (len(deletable_samples), sample_ids))
        self.setup_samples_for_deletion(deletable_samples, self.dry_run)

        if not self.deletable_samples or self.dry_run:
            return 0

        for s in deletable_samples:
            s.mark_as_deleted()

        if self.deletion_dir and os.path.isdir(self.deletion_dir):
            self.delete_dir(self.deletion_dir)

        # Data has been deleted, so now clean up empty released directories
        for folder in set(os.path.dirname(s.released_data_folder) for s in deletable_samples if s.released_data_folder):
            self._try_delete_empty_dir(folder)

        # This script releases files from lustre, leaving them on tape.
        # This means no archiving is required.
        # Archive run folders if they do not contain any fastq file anymore
        # for run_name in set([r[ELEMENT_RUN_NAME] for r in s.run_elements for s in deletable_samples]):
        #     self._try_archive_run(run_name)

        # Archive project folders if their samples have been deleted
        # for project_id in set([r[ELEMENT_PROJECT_ID] for r in s.run_elements for s in deletable_samples]):
        #     self._try_archive_project(project_id)
