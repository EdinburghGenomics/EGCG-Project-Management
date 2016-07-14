from datetime import datetime
from os import listdir
from os.path import join, isdir
from egcg_core import rest_communication, util, clarity
from egcg_core.constants import ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_PROJECT_ID
from egcg_core.config import cfg
from data_deletion import Deleter


class DeliveredDataDeleter(Deleter):

    def __init__(self, work_dir, dry_run=False, deletion_limit=None, manual_delete=None, sample_ids=None):
        super().__init__(work_dir, dry_run, deletion_limit)
        self.data_dir = cfg['delivered_data']
        if manual_delete is None:
            manual_delete = []
        self.list_samples = manual_delete
        self.limit_samples = sample_ids

    def deletable_samples(self):
        manual_samples = []
        if self.list_samples:
            manual_samples = rest_communication.get_documents(
                'aggregate/samples',
                quiet=True,
                match={'$or': [{'sample_id': s} for s in self.list_samples]}
            )
        return sorted(manual_samples + self._auto_deletable_samples(), key=lambda e: e['sample_id'])

    def _auto_deletable_samples(self):
        samples = []
        sample_records = rest_communication.get_documents(
            'aggregate/samples',
            quiet=True,
            match={'$or': [{'proc_status': 'finished'}, {'proc_status': 'aborted'}]}
        )
        for r in sample_records:
            release_date = clarity.get_sample_release_date(r['sample_id'])
            if release_date and self._old_enough_for_deletion(release_date):
                samples.append(r)
        return samples

    def mark_sample_as_deleted(self, sample_id, proc_id):
        if not proc_id:
            self.warning('No pipeline process found for ' + sample_id)
        else:
            rest_communication.patch_entry(
                'analysis_driver_procs',
                {'status': 'deleted'},
                'proc_id', proc_id
            )

    def setup_samples_for_deletion(self, sample_records):
        for s in sample_records:
            self._setup_sample_for_deletion(s['project_id'], s['sample_id'])

    @classmethod
    def _old_enough_for_deletion(cls, date_run, age_threshold=90):
        year, month, day = date_run.split('-')
        age = cls._now() - datetime(int(year), int(month), int(day))
        return age.days > age_threshold

    def _setup_sample_for_deletion(self, project_id, sample_id):
        release_folders = util.find_files(self.data_dir, project_id, '*', sample_id)
        if len(release_folders) != 1:
            self.warning(
                'Found %s deletable directories for sample %s: %s',
                len(release_folders),
                sample_id,
                release_folders
            )
        else:
            deletable_data = join(self.deletion_dir, project_id)
            self._execute('mkdir -p ' + deletable_data)
            self._execute('mv %s %s' % (release_folders[0], deletable_data))

    def _try_delete_empty_dir(self, d):
        if isdir(d) and not listdir(d):
            self._execute('rm -r ' + d)

    def _cleanup_empty_dirs(self, project_id):
        project_folder = join(self.data_dir, project_id)
        for release_dir in listdir(project_folder):
            self._try_delete_empty_dir(join(project_folder, release_dir))
        self._try_delete_empty_dir(project_folder)

    def delete_data(self):
        deletable_samples = self.deletable_samples()
        if self.limit_samples:
            deletable_samples = [s for s in deletable_samples if s['sample_id'] in self.limit_samples]

        sample_ids = [e[ELEMENT_SAMPLE_INTERNAL_ID] for e in deletable_samples]
        self.debug('Found %s samples for deletion: %s' % (len(deletable_samples), sample_ids))

        if self.dry_run or not deletable_samples:
            return 0

        self.setup_samples_for_deletion(deletable_samples)
        samples_to_delete = []
        for p in listdir(self.deletion_dir):
            samples_to_delete.extend(listdir(join(self.deletion_dir, p)))
        self._compare_lists(samples_to_delete, sample_ids)

        for s in deletable_samples:
            self.mark_sample_as_deleted(s['sample_id'], s.get('most_recent_proc', {}).get('proc_id'))

        self.delete_dir(self.deletion_dir)
        for p in set([e[ELEMENT_PROJECT_ID] for e in deletable_samples]):
            self._cleanup_empty_dirs(p)
