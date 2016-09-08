from datetime import datetime
from os import listdir
from os.path import join
from egcg_core import app_logging, executor
from egcg_core.exceptions import EGCGError


class DataDeletionError(EGCGError):
    pass


class Deleter(app_logging.AppLogger):
    data_dir = ''
    local_execute_only = False
    _deletion_dir = None

    def __init__(self, work_dir, dry_run=False, deletion_limit=None):
        self.work_dir = work_dir
        self.dry_run = dry_run
        self.deletion_limit = deletion_limit

    @property
    def deletion_dir(self):
        if self._deletion_dir is None:
            # need caching because of reference to datetime.now
            self._deletion_dir = join(self.data_dir, '.data_deletion_' + self._strnow())
        return self._deletion_dir

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
