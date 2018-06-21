from os import listdir
from os.path import join, isdir
import datetime
from egcg_core import rest_communication
from egcg_core.constants import ELEMENT_RUN_NAME, ELEMENT_PROCS, ELEMENT_STATUS, DATASET_DELETED, ELEMENT_PROC_ID
from egcg_core.config import cfg
from data_deletion import Deleter

reporting_app_date_format = '%d_%m_%Y_%H:%M:%S'


class RawDataDeleter(Deleter):
    alias = 'raw'
    deletable_sub_dirs = ('Data', 'Logs', 'Thumbnail_Images')

    def __init__(self, cmd_args):
        super().__init__(cmd_args)
        self.raw_data_dir = cfg['data_deletion']['raw_data']
        self.archive_dir = cfg['data_deletion']['raw_archives']

        now = self._now()
        self.deletion_threshold = now - datetime.timedelta(days=cfg['data_deletion'].get('run_age_threshold_in_days', 14))

    def deletable_runs(self):
        manual_runs = [
            rest_communication.get_document('runs', where={'run_id': run_id})
            for run_id in self.manual_delete
        ]
        auto_runs = rest_communication.get_documents(
            'runs',
            where={
                'aggregated.review_statuses': {'$ne': 'not reviewed'},
                'aggregated.most_recent_proc.status': {'$in': ['finished', 'aborted']}
            }
        )
        runs = manual_runs + [r for r in auto_runs if self._run_old_enough_for_deletion(r['run_id'])]
        return runs[:self.deletion_limit]

    def _run_old_enough_for_deletion(self, run_id):
        run_elements = rest_communication.get_documents('run_elements', where={'run_id': run_id}, all_pages=True)
        for e in run_elements:
            useable_date = e.get('useable_date')
            if not useable_date:
                return False

            if datetime.datetime.strptime(useable_date, reporting_app_date_format) > self.deletion_threshold:
                return False

        return True

    def _setup_run_for_deletion(self, run_id):
        raw_data = join(self.raw_data_dir, run_id)
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
                    'Not all deletable dirs were present for run %s: %s',
                    run,
                    [d for d in self.deletable_sub_dirs if d not in deletable_dirs]
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
        run_to_be_archived = join(self.raw_data_dir, run_id)
        self.debug('Archiving ' + run_id)
        assert not any([d in self.deletable_sub_dirs for d in listdir(run_to_be_archived)])
        self._execute('mv %s %s' % (join(self.raw_data_dir, run_id), join(self.archive_dir, run_id)))

    def delete_data(self):
        deletable_runs = self.deletable_runs()
        self.debug('Found %s runs for deletion: %s', len(deletable_runs), [r[ELEMENT_RUN_NAME] for r in deletable_runs])
        if self.dry_run or not deletable_runs:
            return 0

        self.setup_runs_for_deletion(deletable_runs)
        runs_to_delete = listdir(self.deletion_dir)
        self._compare_lists(runs_to_delete, [run[ELEMENT_RUN_NAME] for run in deletable_runs])
        assert all([listdir(join(self.raw_data_dir, r)) for r in runs_to_delete])

        for run in deletable_runs:
            assert run[ELEMENT_RUN_NAME] in runs_to_delete
            self.mark_run_as_deleted(run)
            self.archive_run(run[ELEMENT_RUN_NAME])
            assert listdir(join(self.archive_dir, run[ELEMENT_RUN_NAME]))

        self.delete_dir(self.deletion_dir)
