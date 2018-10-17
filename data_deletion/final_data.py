import os
from egcg_core import rest_communication
from egcg_core.constants import ELEMENT_RUN_NAME, ELEMENT_SAMPLE_INTERNAL_ID
from egcg_core.util import query_dict, find_all_fastqs
from egcg_core.config import cfg

from data_deletion import Deleter, FinalSample
from data_deletion.delivered_data import DeliveredDataDeleter


class FinalDataDeleter(DeliveredDataDeleter):
    alias = 'final_deletion'

    def __init__(self, cmd_args):
        super().__init__(cmd_args)
        self.run_archive_dir = cfg['data_deletion']['fastq_archives']
        self.project_archive_dir = cfg['data_deletion']['processed_archives']
        self.fastq_dir = cfg['data_deletion']['fastqs']
        self.projects_dir = cfg['data_deletion']['processed_data']

    def deletable_samples(self):
        samples = [FinalSample(s) for s in self._manually_deletable_samples()]
        return sorted(samples, key=lambda e: e.sample_data['sample_id'])

    def setup_samples_for_deletion(self, samples):
        for s in samples:
            deletable_data_dir = os.path.join(self.deletion_dir, s.sample_id)
            if not self.dry_run:
                if len(s.files_to_purge):
                    self._execute('mkdir -p ' + deletable_data_dir)
                    for f in s.files_to_purge:
                        self._move_to_unique_file_name(f, deletable_data_dir)
            else:
                self.info(
                    'Sample %s has %s files to delete\n%s',
                    s,
                    len(s.files_to_purge),
                    '\n'.join(s.files_to_purge)
                )
                if len(s.files_to_purge):
                    self.info('Will run: mv %s %s', ' '.join(s.files_to_purge), deletable_data_dir)

    def check_all_deletable(self, final_samples):
        ret = True
        for final_sample in final_samples:
            if not self._old_enough_for_deletion(final_sample.release_date, age_threshold=365):
                self.warning('Sample %s is not old enough: %s', final_sample.sample_id, final_sample.release_date)
                ret = False
            if final_sample.sample_data.get('data_deleted') != 'on lustre':
                self.warning('Sample %s is not marked as deleted from lustre', final_sample.sample_id, final_sample.release_date)
                ret = False
        return ret

    @staticmethod
    def _find_files_with_suffix(location, suffix):
        file_names = []
        for name, dirs, files in os.walk(location):
            file_names.extend(os.path.join(name, f) for f in files if f.endswith(suffix))
        return file_names

    def _try_archive_run(self, run_id):
        # Ensure that all samples in that run have been fully deleted.
        run_elements = rest_communication.get_documents('run_elements', where={'run_id': run_id}, all_pages=True)
        sample_ids = set(re[ELEMENT_SAMPLE_INTERNAL_ID] for re in run_elements)
        samples = (rest_communication.get_document('samples', where={'sample_id': sample_id}) for sample_id in sample_ids)
        if all((sample['data_deleted'] == 'all' for sample in samples)):
            # remove all extra fastq files
            run_dir = os.path.join(self.fastq_dir, run_id)
            files_to_remove = find_all_fastqs(run_dir)  # That should be the undetermined since all others were removed
            files_to_remove.extend(self._find_files_with_suffix(run_dir, 'fastq_discarded.gz'))  # phix and adapters
            files_to_remove.extend(self._find_files_with_suffix(run_dir, 'fastq.gz.original'))  # original when filtered
            if files_to_remove:
                deletable_data_dir = os.path.join(self.deletion_dir, run_id)
                self._execute('mkdir -p ' + deletable_data_dir)
                for f in files_to_remove:
                    self._move_to_unique_file_name(f, deletable_data_dir)

            self.debug('Archiving processed run: ' + run_id)
            self._execute('mv %s %s' % (run_dir, os.path.join(self.run_archive_dir, run_id)))

    def _try_archive_project(self, project_id):
        # Ensure that all samples of that project have been fully deleted.
        samples = rest_communication.get_documents('samples', where={'project_id': project_id}, all_pages=True)
        if all((sample['data_deleted'] == 'all' for sample in samples)):
            # remove the extra vcf file from project process
            project_dir = os.path.join(self.projects_dir, project_id)
            files_to_remove = self._find_files_with_suffix(project_dir, 'genotype_gvcfs.vcf.gz')
            if files_to_remove:
                deletable_data_dir = os.path.join(self.deletion_dir, project_id)
                self._execute('mkdir -p ' + deletable_data_dir)
                for f in files_to_remove:
                    self._move_to_unique_file_name(f, deletable_data_dir)

            self.debug('Archiving processed project: ' + project_id)
            self._execute('mv %s %s' % (project_dir, os.path.join(self.project_archive_dir, project_id)))

    def delete_data(self):
        deletable_samples = self.deletable_samples()
        if self.limit_samples:
            deletable_samples = [s for s in deletable_samples if s.sample_id in self.limit_samples]

        sample_ids = [e.sample_id for e in deletable_samples]
        self.debug('Found %s samples for deletion: %s', len(deletable_samples), sample_ids)
        if not self.check_all_deletable(deletable_samples):
            return 1

        self.setup_samples_for_deletion(deletable_samples)

        if not deletable_samples or self.dry_run:
            return 0

        for s in deletable_samples:
            s.mark_as_deleted()

        # Data has been marked as deleted.
        # Now clean up runs directories if possible
        run_ids = set((re[ELEMENT_RUN_NAME] for s in deletable_samples for re in s.run_elements))
        if run_ids:
            for r in run_ids:
                self._try_archive_run(r)

        # Now clean up project directories if possible
        project_ids = set((s.project_id for s in deletable_samples))
        if project_ids:
            for p in project_ids:
                self._try_archive_project(p)

        if self.deletion_dir and os.path.isdir(self.deletion_dir):
            self.delete_dir(self.deletion_dir)


