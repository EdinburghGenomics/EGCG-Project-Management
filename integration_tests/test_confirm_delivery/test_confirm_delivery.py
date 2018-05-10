import os
from egcg_core import rest_communication
from egcg_core.config import cfg
from integration_tests import NamedMock, IntegrationTest
from unittest.mock import Mock, patch
from bin import confirm_delivery

downloaded_files = os.path.join(os.path.dirname(__file__), 'downloaded_files.csv')


class TestConfirmDelivery(IntegrationTest):
    patches = (
        patch('bin.confirm_delivery.load_config'),
        patch('bin.confirm_delivery.clarity.connection'),
        patch('bin.confirm_delivery.clarity.get_workflow_stage'),
        patch(
            'bin.confirm_delivery.Queue',
            return_value=Mock(
                artifacts=[
                    Mock(samples=[NamedMock('sample_1')]),
                    Mock(samples=[NamedMock('sample_2')])
                ]
            )
        )
    )

    samples = [
        {
            'sample_id': 'sample_1',
            'project_id': 'project_1',
            'files_delivered': [
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1_R1.fastq.gz', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1_R2.fastq.gz', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1.bam', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_1/sample_1.g.vcf.gz', 'size': 1000000000}
            ]
        },
        {
            'sample_id': 'sample_2',
            'project_id': 'project_1',
            'files_delivered': [
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_2/sample_2_R1.fastq.gz', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_2/sample_2_R2.fastq.gz', 'size': 1000000000}
            ]
        },
        {
            'sample_id': 'sample_3',
            'project_id': 'project_1',
            'files_delivered': [
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_3/sample_3.bam', 'size': 1000000000},
                {'md5': 'an_md5', 'file_path': 'project_1/delivery_date/sample_3/sample_3.g.vcf.gz', 'size': 1000000000}
            ]
        }
    ]

    def setUp(self):
        super().setUp()

        self.delivered_projects = os.path.join(self.run_dir, 'delivered_projects')
        cfg.content = {
            'sample': {},
            'delivery_dest': self.delivered_projects,  # TODO: really!?
            'delivery': {
                'dest': self.delivered_projects
            }
        }

        os.makedirs(self.delivered_projects, exist_ok=True)
        for s in self.samples:
            for d in s['files_delivered']:
                f = os.path.join(self.delivered_projects, d['file_path'])
                os.makedirs(os.path.dirname(f), exist_ok=True)
                open(f, 'w').close()

            rest_communication.post_entry('samples', s)

    def test_samples(self):
        confirm_delivery.main(['--csv_files', downloaded_files, '--samples', 'sample_1', 'sample_2'])
        self._check_outputs('sample delivery')

    def test_all_queued_samples(self):
        confirm_delivery.main(['--csv_files', downloaded_files, '--queued_samples'])
        self._check_outputs('queued sample delivery')

    def _check_outputs(self, check_name):
        for sample_id in ('sample_1', 'sample_2'):
            obs = rest_communication.get_document('samples', where={'sample_id': sample_id})
            for ext in ('_R1.fastq.gz', '_R2.fastq.gz', '.bam', '.g.vcf.gz'):
                exp = {
                    'date': '05_12_2017_09:05:00',
                    'user': 'a_remote_aspera_user',
                    'file_path': 'project_1/delivery_date/{s}/{s}{ext}'.format(s=sample_id, ext=ext),
                    'size': 1000000000
                }
                self.assertIn('%s (%s%s)' % (check_name, sample_id, ext), exp, obs['files_downloaded'])

        obs = rest_communication.get_document('samples', where={'sample_id': 'sample_3'})
        for ext in ('.bam', '.g.vcf.gz'):
            exp = {
                'date': '05_12_2017_09:05:00',
                'user': 'a_remote_aspera_user',
                'file_path': 'project_1/delivery_date/sample_3/sample_3{ext}'.format(ext=ext),
                'size': 1000000000
            }
            self.assertIn('%s (sample_3%s)' % (check_name, ext), exp, obs['files_downloaded'])
