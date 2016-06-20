from os import listdir
from os.path import join, isdir
from egcg_core import rest_communication
from egcg_core.constants import ELEMENT_RUN_NAME, ELEMENT_PROCS, ELEMENT_STATUS, DATASET_DELETED, ELEMENT_PROC_ID
from data_deletion import Deleter
from data_deletion.config import default as cfg


class RawDataDeleter(Deleter):
    deletable_sub_dirs = ('Data', 'Logs', 'Thumbnail_Images')
    data_dir = cfg['raw_data']

    def __init__(self, work_dir, dry_run=False, deletion_limit=None, manual_delete=None):
        super().__init__(work_dir, dry_run, deletion_limit)
        self.archive_dir = cfg['raw_archives']
        self.list_runs = manual_delete

    def deletable_runs(self):
        runs = rest_communication.get_documents('aggregate/all_runs', paginate=False, quiet=True, sort=ELEMENT_RUN_NAME)
        deletable_runs = []
        for r in runs:
            if (self.list_runs and r[ELEMENT_RUN_NAME] in self.list_runs) or self._run_deletable(r):
                deletable_runs.append(r)
        return deletable_runs[:self.deletion_limit]

    def _setup_run_for_deletion(self, run_id):
        raw_data = join(self.data_dir, run_id)
        deletable_data = join(self.deletion_dir, run_id)
        self.debug('Creating deletion dir: ' + deletable_data)
        self._execute('mkdir -p ' + deletable_data)

        for d in self.deletable_sub_dirs:
            from_d = join(raw_data, d)
            if isdir(from_d):
                to_d = join(deletable_data, d)
                self._execute('mv %s %s' % (from_d, to_d))

        return listdir(deletable_data)  # Data, Thumbnail_Images, etc.

    def setup_runs_for_deletion(self, runs):
        run_ids = [r[ELEMENT_RUN_NAME] for r in runs]
        for run in run_ids:
            deletable_dirs = self._setup_run_for_deletion(run)
            if sorted(self.deletable_sub_dirs) != sorted(deletable_dirs):
                self.warning(
                    'Not all deletable dirs were present for run %s: %s' % (
                        run, [d for d in self.deletable_sub_dirs if d not in deletable_dirs]
                    )
                )

        self._compare_lists(listdir(self.deletion_dir), run_ids)

    def mark_run_as_deleted(self, run):
        self.debug('Updating dataset status for ' + run[ELEMENT_RUN_NAME])
        if not self.dry_run:
            rest_communication.patch_entry(
                ELEMENT_PROCS,
                {ELEMENT_STATUS: DATASET_DELETED},
                ELEMENT_PROC_ID,
                run['most_recent_proc'][ELEMENT_PROC_ID]
            )

    def archive_run(self, run_id):
        run_to_be_archived = join(self.data_dir, run_id)
        self.debug('Archiving ' + run_id)
        assert not any([d in self.deletable_sub_dirs for d in listdir(run_to_be_archived)])
        self._execute('mv %s %s' % (join(self.data_dir, run_id), join(self.archive_dir, run_id)))

    def delete_data(self):
        deletable_runs = self.deletable_runs()
        self.debug(
            'Found %s runs for deletion: %s' % (
                len(deletable_runs), [r[ELEMENT_RUN_NAME] for r in deletable_runs]
            )
        )
        if self.dry_run or not deletable_runs:
            return 0

        self.setup_runs_for_deletion(deletable_runs)
        runs_to_delete = listdir(self.deletion_dir)
        self._compare_lists(
            runs_to_delete,
            [run[ELEMENT_RUN_NAME] for run in deletable_runs]
        )
        assert all([listdir(join(self.data_dir, r)) for r in runs_to_delete])

        for run in deletable_runs:
            assert run[ELEMENT_RUN_NAME] in runs_to_delete
            self.mark_run_as_deleted(run)
            self.archive_run(run[ELEMENT_RUN_NAME])
            assert listdir(join(self.archive_dir, run[ELEMENT_RUN_NAME]))

        self.delete_dir(self.deletion_dir)

    @staticmethod
    def _run_deletable(e):
        review_statuses = e.get('review_statuses')
        if type(review_statuses) is list:
            review_statuses = [s for s in review_statuses if s]
        if review_statuses and 'not reviewed' not in review_statuses:  # all run elements have been reviewed
            if e.get('most_recent_proc', {}).get('status') in ('finished', 'aborted'):  # run is not deleted
                return True
        return False
