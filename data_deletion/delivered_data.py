import os
import uuid
from datetime import datetime
from egcg_core import rest_communication
from data_deletion import Deleter, ProcessedSample
from egcg_core.archive_management import release_file_from_lustre


class DeliveredDataDeleter(Deleter):
    def __init__(self, work_dir, dry_run=False, deletion_limit=None, manual_delete=None, sample_ids=None):
        super().__init__(work_dir, dry_run, deletion_limit, manual_delete)
        self.limit_samples = sample_ids or []

    def _manually_deletable_samples(self):
        max_query = 20
        samples = []
        for start in range(0, len(self.manual_delete), max_query):
            samples.extend(
                rest_communication.get_documents(
                    'aggregate/samples',
                    quiet=True,
                    match={'$or': [{'sample_id': s} for s in self.manual_delete[start:start + max_query]]},
                    paginate=False
                )
            )
        return samples

    def deletable_samples(self):
        samples = [ProcessedSample(s) for s in self._manually_deletable_samples()]
        return sorted(samples + self._auto_deletable_samples(), key=lambda e: e.sample_data['sample_id'])

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
        dest = os.path.join(dest_dir, str(uuid.uuid4()) + '_' + source_name)
        self._execute('mv %s %s' % (source, dest))

    def setup_samples_for_deletion(self, samples):
        total_size_to_delete = 0
        for s in samples:
            total_size_to_delete += s.size_of_files
            deletable_data_dir = os.path.join(self.deletion_dir, s.sample_id)
            if not self.dry_run:
                if len(s.files_to_purge):
                    self._execute('mkdir -p ' + deletable_data_dir)
                    for f in s.files_to_purge:
                        self._move_to_unique_file_name(f, deletable_data_dir)
                if len(s.files_to_remove_from_lustre):
                    for f in s.files_to_remove_from_lustre:
                        release_file_from_lustre(f)
            else:
                self.info(
                    'Sample %s has %s files to delete and %s files to remove from Lustre (%.2f G)\n%s\n%s',
                    s,
                    len(s.files_to_purge),
                    len(s.files_to_remove_from_lustre),
                    s.size_of_files/1000000000,
                    '\n'.join(s.files_to_purge),
                    '\n'.join(s.files_to_remove_from_lustre)
                )
                if len(s.files_to_purge):
                    self.info('Will run: mv %s %s', ' '.join(s.files_to_purge), deletable_data_dir)
                if len(s.files_to_remove_from_lustre):
                    self.info('Will run: %s', '\n'.join(['lfs hsm_release %s' % f for f in s.files_to_remove_from_lustre]))
        self.info('Will delete %.2f G of data', total_size_to_delete / 1000000000)

    @classmethod
    def _old_enough_for_deletion(cls, date_run, age_threshold=90):
        year, month, day = date_run.split('-')
        age = cls._now() - datetime(int(year), int(month), int(day))
        return age.days > age_threshold

    def delete_data(self):
        deletable_samples = self.deletable_samples()
        if self.limit_samples:
            deletable_samples = [s for s in deletable_samples if s.sample_id in self.limit_samples]

        sample_ids = [e.sample_id for e in deletable_samples]
        self.debug('Found %s samples for deletion: %s', len(deletable_samples), sample_ids)
        self.setup_samples_for_deletion(deletable_samples)

        if not deletable_samples or self.dry_run:
            return 0

        for s in deletable_samples:
            s.mark_as_deleted()

        if self.deletion_dir and os.path.isdir(self.deletion_dir):
            self.delete_dir(self.deletion_dir)

        # Data has been deleted, so now clean up empty released directories
        for folder in set(os.path.dirname(s.released_data_folder) for s in deletable_samples if s.released_data_folder):
            if os.path.isdir(folder) and not os.listdir(folder):
                self._execute('rm -r ' + folder)
