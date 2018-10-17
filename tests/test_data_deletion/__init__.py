from os.path import join
from unittest.mock import Mock, patch
from data_deletion import Deleter
from tests import TestProjectManagement


patched_patch_entry = patch('data_deletion.raw_data.rest_communication.patch_entry')


class TestDeleter(TestProjectManagement):
    config_file = 'example_data_deletion.yaml'
    cmd_args = Mock(
        work_dir=TestProjectManagement.assets_deletion,
        dry_run=None,
        deletion_limit=None,
        manual_delete=[],
        sample_ids=[]
    )

    def setUp(self):
        self.deleter = Deleter(self.cmd_args)

    def test_deletion_dir(self):
        with patch.object(self.deleter.__class__, '_strnow', return_value='t'):
            assert self.deleter.deletion_dir == join(self.deleter.work_dir, '.data_deletion_t')

    @patch('egcg_core.notifications.log.LogNotification.notify')
    def test_crash_report(self, mocked_notify):
        patched_delete = patch.object(self.deleter.__class__, 'delete_data', side_effect=ValueError('Something broke'))
        with patch('sys.exit'), patched_delete:
            self.deleter.run()

        assert 'ValueError: Something broke' in mocked_notify.call_args[0][0]
