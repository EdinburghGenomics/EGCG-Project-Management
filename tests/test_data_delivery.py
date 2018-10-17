import os
import collections
import operator
import shutil
import datetime
import itertools
from email.mime.multipart import MIMEMultipart
from unittest.mock import patch, Mock, PropertyMock
from egcg_core.config import cfg
from tests import TestProjectManagement, NamedMock
from bin import deliver_reviewed_data as d

sample_templates = {
    'process_id1': {
        'name': 'p1sample%s',
        'nb_sample': 4,
        'already_delivered_samples': [3, 4],
        'samples': {
            'project_id': 'project1',
            'user_sample_id': 'p1_user_s_id%s',
            'species_name': 'Homo sapiens',
            'useable': 'yes',
            'required_yield': 120000000000,
            'required_coverage': 30,
            'aggregated': {
                'clean_reads': 39024000,
                'yield_in_gb': 127,
                'yield_q30_in_gb': 102,
                'pc_q30': 85.2,
                'pc_mapped_reads': 99.1,
                'pc_duplicate_reads': 16.4,
            },
            'coverage': {'mean': 35},

        },
        'lims/samples': {
            'Delivery': 'merged',
            'Total DNA(ng)': 2000
        },
        'lims/status/sample_status': {
            'library_type': 'pcrfree',
            'started_date': '2017-08-02T11:25:14.659000'
        },
        'run_elements': [
            {
                'run_element_id': 'run1_el_s%s_id1',
                'clean_reads': 15,
                'run_id': 'run1',
                'project_id': 'project1',
                'lane': 1,
                'useable': 'yes'
            },
            {
                'run_element_id': 'run1_el_s%s_id2',
                'clean_reads': 15,
                'run_id': 'run1',
                'project_id': 'project1',
                'lane': 2,
                'useable': 'yes'
            }
        ]
    },
    'process_id2': {
        'name': 'p2sample%s',
        'samples': {
            'project_id': 'project2',
            'user_sample_id': 'p2_user_s_id%s',
            'species_name': 'Homo sapiens',
            'useable': 'yes',
            'required_yield': 120000000000,
            'required_coverage': 30,
            'aggregated': {
                'clean_reads': 39024000,
                'yield_in_gb': 127,
                'yield_q30_in_gb': 102,
                'pc_q30': 85.2,
                'pc_mapped_reads': 99.1,
                'pc_duplicate_reads': 16.4,
            },
            'coverage': {'mean': 35},
        },
        'lims/samples': {
            '2D Barcode': 'Fluidx%s',
            'Delivery': 'split',
            'Total DNA(ng)': 2000
        },
        'lims/status/sample_status': {
            'library_type': 'nano',
            'started_date': '2017-08-02T11:25:14.659000'
        },
        'run_elements': [
            {
                'run_element_id': 'run1_el_s%s_id3',
                'clean_reads': 15,
                'run_id': 'run1',
                'project_id': 'project2',
                'lane': 3,
                'useable': 'yes'
            },
            {
                'run_element_id': 'run1_el_s%s_id4',
                'clean_reads': 15,
                'run_id': 'run1',
                'project_id': 'project1',
                'lane': 4,
                'useable': 'yes'
            }
        ]
    }
}


def _get_value(value_template, index):
    """
    Take a template and complete it with the index if the template contains %s.
    If the template is an iterable (but not a string or dict) then it takes the next one and complete the template.
    """
    if not (type(value_template) in [str, dict]) and isinstance(value_template, collections.Iterable):
        value_template = next(value_template)
    if isinstance(value_template, str) and '%s' in value_template:
        return value_template % index
    else:
        return value_template


rest_responses = {'samples': {}, 'lims/samples': {}, 'lims/status/sample_status': {}, 'run_elements': {}}
fake_processes = {}
for process in sample_templates:
    artifacts = []
    for i in range(1, sample_templates[process].get('nb_sample', 2) + 1):
        sample_id = sample_templates[process]['name'] % i
        if i not in sample_templates[process].get('already_delivered_samples', []):
            artifacts.append(Mock(samples=[NamedMock(name=sample_id)]))

        for endpoint in ('samples', 'lims/samples', 'lims/status/sample_status'):
            rest_responses[endpoint][sample_id] = dict([
                (k, _get_value(v, i)) for k, v in sample_templates[process].get(endpoint, {}).items()
            ])
            rest_responses[endpoint][sample_id]['sample_id'] = sample_id
        rest_responses['run_elements'][sample_id] = []
        for re_template in sample_templates[process].get('run_elements', []):
            re = dict((k, _get_value(v, i)) for k, v in re_template.items())
            re['sample_id'] = sample_id
            rest_responses['run_elements'][sample_id].append(re)

    fake_processes[process] = Mock(
        type=NamedMock(name=d.release_trigger_lims_step_name),
        all_inputs=Mock(return_value=artifacts)
    )


def fake_get_document(*args, **kwargs):
    match = kwargs.get('where') or kwargs['match']
    if 'sample_id' in match:
        return rest_responses.get(args[0], {}).get(match['sample_id'])
    if 'project_id' in match:
        return [rr for rr in rest_responses.get(args[0], {}).values() if rr['project_id'] == match['project_id']]


patch_get_document = patch('egcg_core.rest_communication.get_document', side_effect=fake_get_document)
patch_get_documents = patch('egcg_core.rest_communication.get_documents', side_effect=fake_get_document)
patch_get_queue = patch('egcg_core.clarity.get_queue_uri', return_value='http://testclarity.com/queue/999')


class FakeProcessPropertyMock(PropertyMock):
    """PropertyMock specifically to return fake processes."""
    def __get__(self, obj, obj_type):
        return fake_processes.get(obj.process_id)


patch_process = patch.object(d.DataDelivery, 'process', new=FakeProcessPropertyMock())


def touch(f, content=None):
    with open(f, 'w') as open_file:
        if content:
            open_file.write(content)


def create_fake_fastq_fastqc_md5_from_commands(instance):
    """
    This function replaces run_aggregate_commands and take an instance of DataDelivery.
    It will create the output as if the command were run.
    It only supports fastqc and command that redirects there outputs
    """
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
    config_file = 'example_data_delivery.yaml'
    assets_delivery = os.path.join(TestProjectManagement.assets_path, 'data_delivery')
    analysis_exts = ['.bam', '.bam.bai', '.bam.bai.md5', '.bam.md5', '.g.vcf.gz', '.g.vcf.gz.md5', '.g.vcf.gz.tbi',
                     '.g.vcf.gz.tbi.md5', '.vcf.gz', '.vcf.gz.md5', '.vcf.gz.tbi', '.vcf.gz.tbi.md5']
    raw_data_exts = ['_R1.fastq.gz', '_R1.fastq.gz.md5', '_R1_fastqc.html', '_R1_fastqc.zip', '_R2.fastq.gz',
                     '_R2.fastq.gz.md5', '_R2_fastqc.html', '_R2_fastqc.zip']
    final_files_split = ['p2_user_s_id1' + ext for ext in analysis_exts] + ['raw_data']
    final_files_merged = ['p1_user_s_id1' + ext for ext in analysis_exts + raw_data_exts]
    final_files_merged2 = ['p1_user_s_id2' + ext for ext in analysis_exts + raw_data_exts]
    final_files_merged_no_raw = ['p1_user_s_id1' + ext for ext in analysis_exts]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dest_dir = cfg['delivery']['dest']

    def setUp(self):
        os.makedirs(self.dest_dir, exist_ok=True)
        staging_dir = os.path.join(self.assets_delivery, 'staging')
        self.delivery_dry_split_fluidx = d.DataDelivery(dry_run=True, work_dir=staging_dir, process_id='process_id2', no_cleanup=True, email=False)
        self.delivery_real_split_fluidx = d.DataDelivery(dry_run=False, work_dir=staging_dir, process_id='process_id2', email=False)
        self.delivery_dry_merged = d.DataDelivery(dry_run=True, work_dir=staging_dir, process_id='process_id1', no_cleanup=True, email=False)
        self.delivery_real_merged = d.DataDelivery(dry_run=False, work_dir=staging_dir, process_id='process_id1', email=False)

        self._create_run_elements(itertools.chain.from_iterable(rest_responses['run_elements'].values()))
        self._create_analysed_sample_files(rest_responses['samples'].values())

    def tearDown(self):
        for directory in [self.delivery_dry_split_fluidx.staging_dir, self.delivery_dry_merged.staging_dir]:
            if os.path.exists(directory):
                shutil.rmtree(directory)

        for directory in [
            self.dest_dir,
            os.path.join(self.assets_delivery, 'source'),
            os.path.join(self.assets_delivery, 'runs')
        ]:
            for d in os.listdir(directory):
                shutil.rmtree(os.path.join(directory, d))

    def _create_run_elements(self, list_run_elements):
        for e in list_run_elements:
            sample_dir = os.path.join(self.assets_delivery, 'runs', e['run_id'], e['project_id'], e['sample_id'])
            os.makedirs(sample_dir, exist_ok=True)
            for t in [
                'S1_L00%s_R1.fastq.gz', 'S1_L00%s_R2.fastq.gz',
                'S1_L00%s_R1_fastqc.html', 'S1_L00%s_R2_fastqc.html',
                'S1_L00%s_R1_fastqc.zip', 'S1_L00%s_R2_fastqc.zip'
            ]:
                open(os.path.join(sample_dir, t % e['lane']), 'w').close()
                self.md5(os.path.join(sample_dir, t % e['lane']))

    def _create_analysed_sample_files(self, list_samples):
        for s in list_samples:
            sample_dir = os.path.join(self.assets_delivery, 'source', s['project_id'], s['sample_id'])
            os.makedirs(sample_dir, exist_ok=True)
            for t in ['%s.bam', '%s.bam.bai', '%s.g.vcf.gz', '%s.g.vcf.gz.tbi', '%s.vcf.gz', '%s.vcf.gz.tbi']:
                f = os.path.join(sample_dir, t % s['user_sample_id'])
                open(f, 'w').close()
                self.md5(f)

    def test_mark_samples_as_released(self):
        delivered_date = datetime.datetime(2018, 1, 10)
        with patch('bin.deliver_reviewed_data._now', return_value=delivered_date), \
                patch('egcg_core.rest_communication.patch_entry') as mpatch, \
                patch('egcg_core.clarity.route_samples_to_workflow_stage') as mroute:
            self.delivery_real_merged.samples2files = {
                'p1sample1': [{'file_path': 'path to file1'}],
                'p1sample2': [{'file_path': 'path to file2'}],
            }
            self.delivery_real_merged.mark_samples_as_released(['p1sample1', 'p1sample2'])
            mpatch.assert_any_call(
                'samples', element_id='p1sample1', id_field='sample_id',
                payload={
                    'delivered': 'yes',
                    'files_delivered': [{'file_path': 'path to file1'}],
                    'delivery_date': delivered_date
                },
                update_lists=['files_delivered']
            )
            mpatch.assert_called_with(
                'samples', element_id='p1sample2', id_field='sample_id',
                payload={
                    'delivered': 'yes',
                    'files_delivered': [{'file_path': 'path to file2'}],
                    'delivery_date': delivered_date
                },
                update_lists=['files_delivered']
            )
            mroute.assert_called_with(
                ['p1sample1', 'p1sample2'],
                'Data Release workflow',
                stage_name='Data Release stage'
            )

    def test_deliverable_samples(self):
        with patch_process, patch_get_document, patch_get_documents:
            project_to_samples = self.delivery_dry_merged.deliverable_samples
            assert list(project_to_samples) == ['project1']
            assert [sample['sample_id'] for samples in project_to_samples.values() for sample in samples] == ['p1sample1', 'p1sample2']

    def test_summarise_metrics_per_sample(self):
        with patch_process, patch_get_document, patch_get_documents:
            _ = self.delivery_dry_merged.deliverable_samples
            expected_header = ['Project', 'Sample Id', 'User sample id', 'Species', 'Library type','Received date',
                               'DNA QC (ng)', 'Number of Read pair', 'Target Yield', 'Yield', 'Yield Q30', '%Q30',
                               'Mapped reads rate', 'Duplicate rate', 'Target Coverage', 'Mean coverage',
                               'Delivery folder']

            expected_lines = [
                'project1\tp1sample1\tp1_user_s_id1\tHomo sapiens\tTruSeq PCR-Free\t2017-08-02\t2000\t39024000\t'
                '120.0\t127\t102\t85.2\t99.1\t16.4\t30\t35\tdate_delivery',
                'project1\tp1sample2\tp1_user_s_id2\tHomo sapiens\tTruSeq PCR-Free\t2017-08-02\t2000\t39024000\t'
                '120.0\t127\t102\t85.2\t99.1\t16.4\t30\t35\tdate_delivery'
            ]
            header, lines = self.delivery_dry_merged.summarise_metrics_per_sample(
                project_id='project1',
                delivery_folder='date_delivery'
            )
            assert header == expected_header
            assert sorted(lines) == sorted(expected_lines)

    def test_overwrite_metrics_file(self):
        os.makedirs(os.path.join(self.dest_dir, 'project1'), exist_ok=True)
        metrics_lines = [
            'Project\tSample Id\tUser sample id\tSpecies\tLibrary type\tReceived date\tDNA QC (ng)\t'
            'Number of Read pair\tTarget Yield\tYield\tYield Q30\t%Q30\tMapped reads rate\tDuplicate rate\t'
            'Target Coverage\tMean coverage\tDelivery folder',
            'project1\tp1sample3\tp1_user_s_id1\tHomo sapiens\tTruSeq PCR-Free\t2017-08-02\t2000\t39024000\t'
            '120.0\t127\t102\t85.2\t99.1\t16.4\t30\t35\tdate_delivery1',
            'project1\tp1sample4\tp1_user_s_id2\tHomo sapiens\tTruSeq PCR-Free\t2017-08-02\t2000\t39024000\t'
            '120.0\t127\t102\t85.2\t99.1\t16.4\t30\t35\tdate_delivery1'
        ]
        summary_file = os.path.join(self.dest_dir, 'project1', 'summary_metrics.csv')
        with open(summary_file, 'w') as open_file:
            open_file.write('\n'.join(metrics_lines))

        with patch_process, patch_get_document, patch_get_documents:
            _ = self.delivery_dry_merged.deliverable_samples
            self.delivery_dry_merged.write_metrics_file(
                project='project1',
                delivery_folder='date_delivery2'
            )
        with open(summary_file, 'r') as open_file:
            lines = open_file.readlines()
            assert len(lines) == 5
            assert lines[1].endswith('date_delivery1\n')
            assert lines[2].endswith('date_delivery1\n')
            assert lines[3].endswith('date_delivery2\n')
            assert lines[4].endswith('date_delivery2\n')

    def test_overwrite_old_metrics_file(self):
        os.makedirs(os.path.join(self.dest_dir, 'project1'), exist_ok=True)
        metrics_lines = [
            'Project\tSample Id\tUser sample id\tRead pair sequenced\tYield\tYield Q30\tNb reads in bam\t'
            'mapping rate\tproperly mapped reads rate\tduplicate rate\tMean coverage\tDelivery folder',
            'project1\tp1sample3\tp1_user_s_id1\t39024000\t127\t102\t39823000\t99.1\t92.3\t16.4\t35\tdate_delivery1',
            'project1\tp1sample4\tp1_user_s_id2\t39024000\t127\t102\t39823000\t99.1\t92.3\t16.4\t35\tdate_delivery1'
        ]
        summary_file = os.path.join(self.dest_dir, 'project1', 'summary_metrics.csv')
        with open(summary_file, 'w') as open_file:
            open_file.write('\n'.join(metrics_lines))

        with patch_process, patch_get_document, patch_get_documents:
            _ = self.delivery_dry_merged.deliverable_samples
            self.delivery_dry_merged.write_metrics_file(
                project='project1',
                delivery_folder='date_delivery2'
            )
        with open(summary_file, 'r') as open_file:
            lines = open_file.readlines()
            assert len(lines) == 5
            assert lines[1].endswith('date_delivery1\n')
            assert lines[2].endswith('date_delivery1\n')
            assert lines[3].endswith('date_delivery2\n')
            assert lines[4].endswith('date_delivery2\n')

    def test_deliver_data_merged(self):
        with patch_process, patch_get_document, patch_get_documents, patch_get_queue:
            # Remove one of the run_element from rest response so the remaining one gets used as merged
            re = rest_responses['run_elements']['p1sample1'].pop()
            self.delivery_dry_merged.deliver_data()
            assert os.listdir(self.delivery_dry_merged.staging_dir) == ['p1sample1', 'p1sample2']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry_merged.staging_dir, 'p1sample1')))
            assert list_files == sorted(self.final_files_merged)
            # Put it back
            rest_responses['run_elements']['p1sample1'].append(re)

    def test_deliver_data_merged_concat(self):
        with patch_process, patch_get_document, patch_get_documents, patch_get_queue:
            self.delivery_dry_merged.deliver_data()
            assert os.listdir(self.delivery_dry_merged.staging_dir) == ['p1sample1', 'p1sample2']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry_merged.staging_dir, 'p1sample1')))
            assert list_files == sorted(self.final_files_merged_no_raw)
            assert len(self.delivery_dry_merged.all_commands_for_cluster) == 4

    def test_deliver_data_split(self):
        with patch_process, patch_get_document, patch_get_documents, patch_get_queue:
            self.delivery_dry_split_fluidx.deliver_data()
            assert sorted(os.listdir(self.delivery_dry_split_fluidx.staging_dir)) == ['Fluidx1', 'Fluidx2']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry_split_fluidx.staging_dir, 'Fluidx1')))
            assert sorted(list_files) == sorted(self.final_files_split)

    def test_deliver_data_merged_real(self):
        with patch_process, patch_get_document, patch_get_documents, patch_get_queue,\
             patch('bin.deliver_reviewed_data.DataDelivery.mark_samples_as_released'), \
             patch.object(d.DataDelivery, 'run_aggregate_commands', new=create_fake_fastq_fastqc_md5_from_commands), \
             patch.object(d.DataDelivery, 'register_postponed_files'):
            self.delivery_real_merged.deliver_data()
            assert os.listdir(self.dest_dir) == ['project1']
            today = datetime.date.today().isoformat()
            assert sorted(os.listdir(os.path.join(self.dest_dir, 'project1'))) == [today, 'all_md5sums.txt', 'summary_metrics.csv']
            assert os.listdir(os.path.join(self.dest_dir, 'project1', today)) == ['p1sample1', 'p1sample2']
            assert sorted(self.final_files_merged2) == sorted(os.listdir(os.path.join(self.dest_dir, 'project1', today, 'p1sample2')))

            assert self.delivery_real_merged.samples2files['p1sample2'] == [
                {
                    'file_path': 'project1/%s/p1sample2/%s'% (today, f),
                    'size': 0,
                    'md5': 'd41d8cd98f00b204e9800998ecf8427e'
                }
                for f in ('p1_user_s_id2.g.vcf.gz', 'p1_user_s_id2.g.vcf.gz.tbi', 'p1_user_s_id2.vcf.gz',
                          'p1_user_s_id2.vcf.gz.tbi', 'p1_user_s_id2.bam', 'p1_user_s_id2.bam.bai')
            ]

    def test_deliver_data_split_real(self):
        with patch_process, patch_get_document, patch_get_documents, patch_get_queue,\
             patch.object(d.DataDelivery, 'run_aggregate_commands'),\
             patch('bin.deliver_reviewed_data.DataDelivery.mark_samples_as_released'):
            self.delivery_real_split_fluidx.deliver_data()
            assert os.listdir(self.dest_dir) == ['project2']
            today = datetime.date.today().isoformat()
            assert sorted(os.listdir(os.path.join(self.dest_dir, 'project2'))) == [today, 'all_md5sums.txt', 'summary_metrics.csv']
            assert sorted(os.listdir(os.path.join(self.dest_dir, 'project2', today))) == ['Fluidx1', 'Fluidx2']
            assert sorted(self.final_files_split) == sorted(os.listdir(os.path.join(self.dest_dir, 'project2', today, 'Fluidx1')))

            assert sorted(
                self.delivery_real_split_fluidx.samples2files['p2sample1'],
                key=operator.itemgetter('file_path')
            ) == sorted(
                [
                    {
                        'file_path': 'project2/%s/Fluidx1/%s' % (today, f),
                        'size': 0,
                        'md5': 'd41d8cd98f00b204e9800998ecf8427e'
                    }
                    for f in ('raw_data/run1_el_s1_id3_R1.fastq.gz', 'raw_data/run1_el_s1_id3_R2.fastq.gz',
                              'raw_data/run1_el_s1_id4_R1.fastq.gz', 'raw_data/run1_el_s1_id4_R2.fastq.gz',
                              'p2_user_s_id1.g.vcf.gz', 'p2_user_s_id1.g.vcf.gz.tbi', 'p2_user_s_id1.vcf.gz',
                              'p2_user_s_id1.vcf.gz.tbi', 'p2_user_s_id1.bam', 'p2_user_s_id1.bam.bai')
                ],
                key=operator.itemgetter('file_path')
            )

    def test_get_email_data(self):
        with patch_process, patch_get_document, patch_get_documents, patch_get_queue,\
             patch.object(d.DataDelivery, 'today', new_callable=PropertyMock(return_value='2017-12-15')):
            exp = {
                'num_samples': 2,
                'release_batch': '2017-12-15',
                'delivery_queue': 'http://testclarity.com/queue/999',
                'project_id': 'test_project'
            }
            assert exp == self.delivery_dry_merged.get_email_data('test_project', ['sample1', 'sample2'])

    def test_send_reports(self):
        with patch_process, patch_get_document, patch_get_documents, patch_get_queue,\
             patch('egcg_core.notifications.email.EmailSender._try_send') as mock_send_email:
            self.delivery_dry_merged.email = True
            _ = self.delivery_dry_merged.deliverable_samples
            self.delivery_dry_merged.send_reports(
                {'project1': [rest_responses['samples']['p1sample1'], rest_responses['samples']['p1sample2']]},
                {'test_project': os.path.join(self.assets_path, 'data_delivery', 'test_project_report.pdf')}
            )
            assert mock_send_email.call_count == 1
            assert type(mock_send_email.call_args_list[0][0][0]) == MIMEMultipart

    def test_resolve_process_id(self):
        assert d.resolve_process_id('http://test.com/path/to/step/20198') == '24-20198'
        assert d.resolve_process_id('http://test.com/api/2/steps/24-20198') == '24-20198'
        assert d.resolve_process_id('20198') == '24-20198'
        assert d.resolve_process_id('24-20198') == '24-20198'
