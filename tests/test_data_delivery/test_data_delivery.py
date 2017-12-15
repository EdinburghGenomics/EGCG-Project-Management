import hashlib
import os
from email.mime.multipart import MIMEMultipart
from unittest.mock import patch, Mock, PropertyMock
import shutil
import datetime
from egcg_core.exceptions import EGCGError
from egcg_core.config import cfg
from tests import TestProjectManagement
from bin.deliver_reviewed_data import DataDelivery, _execute

sample1 = {
    'sample_id': 'deliverable_sample',
    'project_id': 'test_project',
    'user_sample_id': 'user_s_id',
    'useable': 'yes',
    'analysis_driver_procs': [
        {
            'proc_id': 'most_recent_proc',
            '_created': '06_03_2016_12:00:00',
            'status': 'finished'
        },
        {
            'proc_id': 'old_recent_proc',
            '_created': '01_02_2016_12:00:00',
            'status': 'aborted'
        }
    ],
    'bam_file_reads': 1,
    'run_elements': [
        {
            'run_element_id': 'run_el_id1',
            'clean_reads': 15,
            'run_id': 'run1',
            'project_id': 'test_project',
            'sample_id': 'deliverable_sample',
            'lane': 1,
            'useable': 'yes'
        }
    ]
}
sample2 = {
    'sample_id': 'deliverable_sample2',
    'project_id': 'test_project',
    'user_sample_id': 'user_s_id2',
    'useable': 'yes',
    'analysis_driver_procs': [
        {
            'proc_id': 'most_recent_proc',
            '_created': '06_03_2016_12:00:00',
            'status': 'finished'
        },
        {
            'proc_id': 'old_recent_proc',
            '_created': '01_02_2016_12:00:00',
            'status': 'aborted'
        }
    ],
    'bam_file_reads': 1,
    'run_elements': [
        {
            'run_element_id': 'run_el_id2',
            'clean_reads': 15,
            'run_id': 'run1',
            'project_id': 'test_project',
            'sample_id': 'deliverable_sample2',
            'lane': 2,
            'useable': 'yes'
        },
        {
            'run_element_id': 'run_el_id3',
            'clean_reads': 15,
            'run_id': 'run1',
            'project_id': 'test_project',
            'sample_id': 'deliverable_sample2',
            'lane': 3,
            'useable': 'yes'
        }
    ]
}


patched_deliverable_project1 = patch(
    'egcg_core.rest_communication.get_documents',
    return_value=[sample1]
)
patched_deliverable_project2 = patch(
    'egcg_core.rest_communication.get_documents',
    return_value=[sample2]
)
patched_error_project = patch(
    'egcg_core.rest_communication.get_documents',
    return_value=[
        {
            'sample_id': 'deliverable_sample',
            'project_id': 'test_project',
            'useable': 'yes',
            'analysis_driver_procs': [
                {
                    'proc_id': 'most_recent_proc',
                    '_created': '06_03_2016_12:00:00',
                    'status': 'aborted'
                },
                {
                    'proc_id': 'old_recent_proc',
                    '_created': '01_02_2016_12:00:00',
                    'status': 'finished'
                }
            ]
        }
    ]
)

patched_get_species = patch(
    'egcg_core.clarity.get_species_from_sample',
    return_value='Homo sapiens'
)


def execute_local(*args, **kwargs):
    if 'env' in kwargs and kwargs['env'] == 'local':
        _execute(*args, **kwargs)


def touch(f, content=None):
    with open(f, 'w') as open_file:
        if content:
            open_file.write(content)


def create_fake_fastq_fastqc_md5_from_commands(instance):
    '''
    This function replaces run_aggregate_commands and take an instance of DataDelivery.
    It will create the output as if the command were run.
    It only supports fastqc and command that redirects there outputs
    '''
    for commands in instance.all_commands_for_cluster:
        for command in commands.split(';'):
            if len(command.split('>')) > 1:
                output = command.split('>')[1].strip()
                if output.endswith('.md5'):
                    touch(output, 'd41d8cd98f00b204e9800998ecf8427e  ' + os.path.basename(output))
                else:
                    touch(output)
            elif command.strip().startswith('fastqc'):
                touch(command.split()[-1].split('.fastq')[0] + '_fastqc.zip')
                touch(command.split()[-1].split('.fastq')[0] + '_fastqc.html')


class TestDataDelivery(TestProjectManagement):
    def __init__(self, *args, **kwargs):
        super(TestDataDelivery, self).__init__(*args, **kwargs)
        cfg.load_config_file(os.path.join(os.path.dirname(self.root_test_path), 'etc', 'example_data_delivery.yaml'))
        os.chdir(os.path.dirname(self.root_test_path))
        self.assets_delivery = os.path.join(self.assets_path, 'data_delivery')
        analysis_files = [
            '{ext_sample_id}.bam', '{ext_sample_id}.bam.bai', '{ext_sample_id}.bam.bai.md5', '{ext_sample_id}.bam.md5',
            '{ext_sample_id}.g.vcf.gz', '{ext_sample_id}.g.vcf.gz.md5', '{ext_sample_id}.g.vcf.gz.tbi',
            '{ext_sample_id}.g.vcf.gz.tbi.md5', '{ext_sample_id}.vcf.gz', '{ext_sample_id}.vcf.gz.md5',
            '{ext_sample_id}.vcf.gz.tbi', '{ext_sample_id}.vcf.gz.tbi.md5'
        ]
        raw_data_files = [
            '{ext_sample_id}_R1.fastq.gz', '{ext_sample_id}_R1.fastq.gz.md5', '{ext_sample_id}_R1_fastqc.html',
            '{ext_sample_id}_R1_fastqc.zip', '{ext_sample_id}_R2.fastq.gz', '{ext_sample_id}_R2.fastq.gz.md5',
            '{ext_sample_id}_R2_fastqc.html', '{ext_sample_id}_R2_fastqc.zip'
        ]
        self.final_files_merged = self._format_list(analysis_files + raw_data_files,  ext_sample_id='user_s_id')
        self.final_files_merged_no_raw = self._format_list(analysis_files, ext_sample_id='user_s_id2')
        self.final_files_merged2 = self._format_list(analysis_files + raw_data_files, ext_sample_id='user_s_id2')

        self.final_files_split = self._format_list(analysis_files + ['raw_data'], ext_sample_id='user_s_id')

        self.dest_dir = cfg.query('delivery', 'dest')

    def _format_list(self, list_, **kwargs):
        return [v.format(**kwargs) for v in list_]

    def setUp(self):
        os.makedirs(self.dest_dir, exist_ok=True)
        self.delivery_dry = DataDelivery(dry_run=True, work_dir=os.path.join(self.assets_delivery, 'staging'), no_cleanup=True, email=False)
        self.delivery_real = DataDelivery(dry_run=False, work_dir=os.path.join(self.assets_delivery, 'staging'), email=False)
        self._create_run_elements(sample1.get('run_elements') + sample2.get('run_elements'))

    def tearDown(self):
        if os.path.exists(self.delivery_dry.staging_dir):
            shutil.rmtree(self.delivery_dry.staging_dir)
        for d in os.listdir(self.dest_dir):
            shutil.rmtree(os.path.join(self.dest_dir, d))
        self._remove_run_elements(sample1.get('run_elements') + sample2.get('run_elements'))
        pass

    def _remove_run_elements(self, list_run_elements):
        sample_dirs = set()
        for run_element in list_run_elements:
            sample_dirs.add(os.path.join(self.assets_delivery, 'runs', run_element.get('run_id')))
        for s in sample_dirs:
            shutil.rmtree(s)

    def _create_run_elements(self, list_run_elements):
        for e in list_run_elements:
            sample_dir = os.path.join(self.assets_delivery, 'runs', e['run_id'], e['project_id'], e['sample_id'])
            os.makedirs(sample_dir, exist_ok=True)
            for t in [
                'S1_L00%s_R1.fastq.gz', 'S1_L00%s_R2.fastq.gz',
                'S1_L00%s_R1_fastqc.html', 'S1_L00%s_R2_fastqc.html',
                'S1_L00%s_R1_fastqc.zip', 'S1_L00%s_R2_fastqc.zip'
            ]:
                self.touch(os.path.join(sample_dir, t % e['lane']))
                self.md5(os.path.join(sample_dir, t % e['lane']))

    def create_analysed_sample_file(self):
        pass

    def test_get_deliverable_projects_samples(self):
        with patched_deliverable_project1:
            project_to_samples = self.delivery_dry.get_deliverable_projects_samples(project_id='test_project')
            assert list(project_to_samples) == ['test_project']
            assert list([sample.get('sample_id') for samples in project_to_samples.values() for sample in samples]) == ['deliverable_sample']

        with patched_error_project:
            self.assertRaises(EGCGError, self.delivery_dry.get_deliverable_projects_samples)

    def test_summarise_metrics_per_sample(self):
        with patched_deliverable_project1:
            self.delivery_dry.get_deliverable_projects_samples(project_id='test_project')
            expected_header = ['Project', 'Sample Id', 'User sample id', 'Read pair sequenced', 'Yield',
                               'Yield Q30', 'Nb reads in bam', 'mapping rate', 'properly mapped reads rate',
                               'duplicate rate', 'Mean coverage', 'Delivery folder']
            expected_lines = [
                'test_project\tdeliverable_sample\tuser_s_id\t15\t0.0\t0.0\t1\t0.0\t0.0\t0.0\t0\tdate_delivery'
            ]
            with patch('egcg_core.clarity.get_species_from_sample', return_value='Homo sapiens'):
                header, lines = self.delivery_dry.summarise_metrics_per_sample(
                    project_id='test_project',
                    delivery_folder='date_delivery'
                )
                assert header == expected_header
                assert lines == expected_lines

    def test_deliver_data_merged(self):
        with patched_deliverable_project1, patched_get_species,\
                patch('egcg_core.clarity.get_sample', return_value=Mock(udf={'Delivery': 'merged'})), \
                patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'):
            self.delivery_dry.deliver_data(project_id='test_project')
            assert os.listdir(self.delivery_dry.staging_dir) == ['deliverable_sample']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry.staging_dir, 'deliverable_sample')))
            assert sorted(list_files) == sorted(self.final_files_merged)

    def test_deliver_data_merged_concat(self):
        with patched_deliverable_project2, patched_get_species,\
                patch('egcg_core.clarity.get_sample', return_value=Mock(udf={'Delivery': 'merged'})), \
             patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'):
            self.delivery_dry.deliver_data(project_id='test_project')
            assert os.listdir(self.delivery_dry.staging_dir) == ['deliverable_sample2']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry.staging_dir, 'deliverable_sample2')))
            assert sorted(list_files) == sorted(self.final_files_merged_no_raw)
            assert len(self.delivery_dry.all_commands_for_cluster) == 2

    def test_deliver_data_split(self):
        with patched_deliverable_project1, patched_get_species,\
             patch('egcg_core.clarity.get_sample', return_value=Mock(udf={'Delivery': 'split'})), \
             patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'):
            self.delivery_dry.deliver_data(project_id='test_project')
            assert os.listdir(self.delivery_dry.staging_dir) == ['deliverable_sample']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry.staging_dir, 'deliverable_sample')))
            assert sorted(list_files) == sorted(self.final_files_split)

    def test_deliver_data_fluidx(self):
        with patched_deliverable_project1, patched_get_species,\
                patch('egcg_core.clarity.get_sample', return_value=Mock(udf={'2D Barcode': 'FluidXBarcode', 'Delivery': 'split'})), \
             patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'):
            self.delivery_dry.deliver_data(project_id='test_project')
            assert os.listdir(self.delivery_dry.staging_dir) == ['FluidXBarcode']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry.staging_dir, 'FluidXBarcode')))
            assert sorted(list_files) == sorted(self.final_files_split)

    def test_deliver_data_merged_real(self):
        with patched_deliverable_project2, patched_get_species,\
                patch('egcg_core.clarity.get_sample', return_value=Mock(udf={'Delivery': 'merged'})),\
                patch('bin.deliver_reviewed_data.DataDelivery.mark_samples_as_released'), \
                patch.object(DataDelivery, 'run_aggregate_commands', new=create_fake_fastq_fastqc_md5_from_commands), \
                patch.object(DataDelivery, 'register_postponed_files'), \
                patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'):
            self.delivery_real.deliver_data(project_id='test_project')
            assert os.listdir(self.dest_dir) == ['test_project']
            today = datetime.date.today().isoformat()
            assert sorted(os.listdir(os.path.join(self.dest_dir, 'test_project'))) == [today, 'all_md5sums.txt', 'summary_metrics.csv']
            assert os.listdir(os.path.join(self.dest_dir, 'test_project', today)) == ['deliverable_sample2']
            list_files = sorted(os.listdir(os.path.join(self.dest_dir, 'test_project', today, 'deliverable_sample2')))
            assert sorted(list_files) == sorted(self.final_files_merged2)

            list_file = ['user_s_id2.g.vcf.gz', 'user_s_id2.g.vcf.gz.tbi', 'user_s_id2.vcf.gz', 'user_s_id2.vcf.gz.tbi',
                         'user_s_id2.bam', 'user_s_id2.bam.bai']
            expected_list_files = [
                {
                    'file_path': 'test_project/%s/deliverable_sample2/%s'% (today, f),
                    'size': 0,
                    'md5': 'd41d8cd98f00b204e9800998ecf8427e'
                } for f in list_file
            ]
            assert self.delivery_real.samples2list_files == {'deliverable_sample2': expected_list_files}

    def test_deliver_data_split_real(self):
        with patched_deliverable_project1, patched_get_species,\
             patch('egcg_core.clarity.get_sample', return_value=Mock(udf={'Delivery': 'split'})),\
             patch.object(DataDelivery, 'run_aggregate_commands'),\
             patch('bin.deliver_reviewed_data.DataDelivery.mark_samples_as_released'), \
             patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'):
            self.delivery_real.deliver_data(project_id='test_project')
            assert os.listdir(self.dest_dir) == ['test_project']
            today = datetime.date.today().isoformat()
            assert sorted(os.listdir(os.path.join(self.dest_dir, 'test_project'))) == [today, 'all_md5sums.txt', 'summary_metrics.csv']
            assert os.listdir(os.path.join(self.dest_dir, 'test_project', today)) == ['deliverable_sample']
            list_files = sorted(os.listdir(os.path.join(self.dest_dir, 'test_project', today, 'deliverable_sample')))
            assert sorted(list_files) == sorted(self.final_files_split)

            list_file = ['raw_data/run_el_id1_R1.fastq.gz', 'raw_data/run_el_id1_R2.fastq.gz', 'user_s_id.g.vcf.gz',
                         'user_s_id.g.vcf.gz.tbi', 'user_s_id.vcf.gz', 'user_s_id.vcf.gz.tbi',
                         'user_s_id.bam', 'user_s_id.bam.bai']
            expected_list_files = [
                {
                    'file_path': 'test_project/%s/deliverable_sample/%s' % (today, f),
                    'size': 0,
                    'md5': 'd41d8cd98f00b204e9800998ecf8427e'
                } for f in list_file
                ]
            assert self.delivery_real.samples2list_files == {'deliverable_sample': expected_list_files}

    def test_mark_only(self):
        with patch.object(DataDelivery, 'get_deliverable_projects_samples', return_value={'test_project': [sample1, sample2]}):
            self.delivery_dry.mark_only()
            # only logs the number of sample marked

        with patch('egcg_core.rest_communication.patch_entry') as mocked_patch, \
            patch('egcg_core.clarity.route_samples_to_delivery_workflow') as mocked_route, \
            patch.object(DataDelivery, 'get_deliverable_projects_samples', return_value={'test_project': [sample1, sample2]}):
            self.delivery_real.mark_only()
            mocked_route.assert_called_with(['deliverable_sample', 'deliverable_sample2'])

    def test_get_email_data(self):
        with patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'), \
             patch.object(DataDelivery, 'today', new_callable=PropertyMock(return_value='2017-11-29')):

            exp = {
                'num_samples': 2,
                'release_batch': '2017-12-15',
                'delivery_queue': 'http://testclarity.com/queue/999',
                'project_id': 'test_project'
            }
            assert exp == self.delivery_dry.get_email_data('test_project', [sample1, sample2])

    def test_emails_report(self):
        with patched_get_species, \
             patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999'), \
             patch('egcg_core.notifications.email.EmailSender._try_send') as mock_send_email:
            self.delivery_dry.email = True
            self.delivery_dry.emails_report(
                {'test_project': [sample1, sample2]},
                {'test_project': os.path.join(self.assets_path, 'data_delivery', 'test_project_report.pdf')}
            )
            assert mock_send_email.call_count == 1
            assert type(mock_send_email.call_args_list[0][0][0]) == MIMEMultipart

