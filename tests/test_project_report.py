import os
from unittest.mock import Mock, patch, PropertyMock

import re

from project_report import ProjectReport
from egcg_core.config import cfg
from tests import TestProjectManagement


def ppath(ext):
    return 'project_report.' + ext


class FakeSample:
    def __init__(self, name, udf):
        self.name = name
        self.udf = udf

fake_lims_sample1 = FakeSample(name='sample:01', udf={'Prep Workflow': None, 'Species': 'Thingius thingy'})
fake_lims_sample2 = FakeSample(name='sample:02', udf={'Prep Workflow': None, 'Species': 'Thingius thingy'})
fake_lims_sample3 = FakeSample(name='human_truseq_nano_sample_1', udf={'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Homo sapiens'})
fake_lims_sample4 = FakeSample(name='non_human_truseq_nano_sample_1', udf={'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Thingius thingy'})
fake_lims_sample5 = FakeSample(name='human_pcr_free_sample_1', udf={'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Homo sapiens'})
fake_lims_sample6 = FakeSample(name='non_human_pcr_free_sample_1', udf={'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Thingius thingy'})

fake_lims_sample_list = [fake_lims_sample1, fake_lims_sample2, fake_lims_sample3, fake_lims_sample4, fake_lims_sample5, fake_lims_sample6]
fake_lims_sample_dict = dict([(s.name, s) for s in fake_lims_sample_list])

fake_lims_project = Mock(
    udf={'Project Title': 'a_research_title', 'Enquiry Number': '1337', 'Quote No.': '1338', 'Number of Quoted Samples': 2},
    researcher=Mock(first_name='First', last_name='Last', email='first.last@email.com')
)

patch_get_project = patch('egcg_core.clarity.get_project', return_value=fake_lims_project)

fake_db_sample1 = {'sample_id': 'sample_01'}
fake_db_sample2 = {'sample_id': 'sample_02'}
fake_db_sample_hum_nano = {'sample_id': 'human_truseq_nano_sample_1'}
fake_db_sample_hum_pcrfree = {'sample_id': 'non_human_truseq_nano_sample_1'}
fake_db_sample_nonhum_nano = {'sample_id': 'human_pcr_free_sample_1'}
fake_db_sample_nonhum_pcrfree = {'sample_id': 'non_human_pcr_free_sample_1'}
fake_db_samples = [fake_db_sample1, fake_db_sample2]

def get_fake_sample(sample_name):
    s = fake_lims_sample_dict.get(sample_name)
    if not s:
        sample_name_sub = re.sub("_(\d{2})$", ":\g<1>", sample_name)
        s = fake_lims_sample_dict.get(sample_name_sub)
    return s

patch_get_sample = patch('egcg_core.clarity.get_sample', new=get_fake_sample)

def get_patch_delivered_samples(samples):
        return patch.object(
            ProjectReport,
            'delivered_samples_for_project',
            new=PropertyMock(return_value=samples)
        )

class TestProjectReport(TestProjectManagement):
    def setUp(self):
        cfg.load_config_file(TestProjectManagement.etc_config)
        self.pr = ProjectReport('a_project_name')
        os.chdir(TestProjectManagement.root_path)

    def test_get_project_info(self):
        exp = (
            ('Project name:', self.pr.project_name),
            ('Project title:', 'a_research_title'),
            ('Enquiry no:', '1337'),
            ('Quote no:', '1338'),
            ('Researcher:', 'First Last (first.last@email.com)')
        )
        with patch_get_project:
            assert self.pr.get_project_info() == exp

    def test_delivered_samples_for_project(self):
        with patch('egcg_core.rest_communication.get_documents') as mock_get:
            self.pr.delivered_samples_for_project
            mock_get.assert_called_once_with('samples', where={'project_id': 'a_project_name', 'delivered': 'yes'})


    def test_get_library_workflow(self):
        with patch_get_sample:
            assert self.pr.get_library_workflow_from_sample('sample:01') is None
            assert self.pr.get_library_workflow_from_sample('human_truseq_nano_sample_1') == 'TruSeq Nano DNA Sample Prep'


    def test_get_report_type(self):
        with patch_get_sample:
            assert self.pr.get_report_type_from_sample('sample:01') == 'non_human'
            self.pr.project_name = 'human_truseq_nano'
            self.pr._samples_from_lims = None
            assert self.pr.get_report_type_from_sample('human_truseq_nano_sample_1') == 'Human'

    def test_update_program_from_csv(self):
        assert len(self.pr.params) == 3
        program_csv = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'source',
            'a_project_name',
            'sample_01',
            'programs.txt'
        )
        self.pr.update_from_program_csv(program_csv)
        exp = {
            'bcbio_version': '1.1',
            'bwa_version': '1.2',
            'gatk_version': '1.3',
            'samblaster_version': '1.4',
            'bcl2fastq_version': '2.17.1.14'
        }
        assert all(self.pr.params[k] == v for k, v in exp.items())

    def test_update_from_project_summary(self):
        assert 'bcbio_version' not in self.pr.params
        assert 'genome_version' not in self.pr.params
        summary_yaml = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'source',
            'a_project_name',
            'sample_01',
            'project-summary.yaml'
        )
        self.pr.update_from_project_summary_yaml(summary_yaml)
        assert self.pr.params['bcbio_version'] == 'bcbio-0.9.4'
        assert self.pr.params['genome_version'] == 'GRCh38 (with alt, decoy and HLA sequences)'

    def test_read_metrics_csv(self):
        exp = {}
        for s in ('1', '2'):
            sample_id = 'sample_' + s
            exp[sample_id] = {
                'Project': 'a_project_name',
                'Sample Id': sample_id,
                'User sample id': sample_id,
                'Read pair sequenced': '1100000000',
                'Yield': '100',
                'Yield Q30': '90',
                'Nb reads in bam': '1000000000',
                'mapping rate': '90.9',
                'properly mapped reads rate': '85',
                'duplicate rate': '20',
                'Mean coverage': '30',
                'Callable bases rate': '90',
                'Delivery folder': '2016-03-08'
            }
        metrics_csv = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'dest',
            'a_project_name',
            'summary_metrics.csv'
        )
        obs = self.pr.read_metrics_csv(metrics_csv)
        assert obs == exp

    @patch(ppath('ProjectReport.get_folder_size'), return_value=1337000000000)
    def test_get_sample_info(self, mocked_project_size):

        with get_patch_delivered_samples(fake_db_samples), patch_get_project:
            print(self.pr.params)
            assert self.pr.get_sample_info() == [
                ('Number of samples:', 2),
                ('Number of samples delivered:', 2),
                ('Total yield (Gb):', '200.00'),
                ('Average yield (Gb):', '100.0'),
                ('Average coverage per sample:', '30.00'),
                ('Total folder size:', '1.34Tb')
            ]
            assert self.pr.params == {
                'project_name': 'a_project_name',
                'adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA',
                'adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
                'bcbio_version': 'bcbio-0.9.4',
                'bcl2fastq_version': '2.17.1.14',
                'bwa_version': '1.2',
                'gatk_version': '1.3',
                'genome_version': 'GRCh38 (with alt, decoy and HLA sequences)',
                'samblaster_version': '1.4'
            }

    def test_get_html_template(self):
        with get_patch_delivered_samples(fake_db_samples), patch_get_sample:
            assert self.pr.get_html_template() == 'truseq_nano_non_human.html'

        with get_patch_delivered_samples([fake_db_sample_hum_nano]), patch_get_sample:
            assert self.pr.get_html_template() == 'truseq_nano.html'

    @patch(ppath('path.getsize'), return_value=1)
    def test_get_folder_size(self, mocked_getsize):
        d = os.path.join(TestProjectManagement.root_path, 'project_report', 'templates')
        obs = self.pr.get_folder_size(d)
        assert obs == 10


def test_project_types():
    os.chdir(TestProjectManagement.root_path)
    projects = ('human_truseq_nano', 'human_pcr_free', 'non_human_truseq_nano', 'non_human_pcr_free')
    samples = (fake_db_sample_hum_nano, fake_db_sample_hum_pcrfree, fake_db_sample_nonhum_nano, fake_db_sample_nonhum_pcrfree)
    for i, sample in enumerate(samples):
        with get_patch_delivered_samples([sample]), patch_get_sample, patch_get_project:
            pr = ProjectReport(projects[i])
            pr.generate_report('html')
