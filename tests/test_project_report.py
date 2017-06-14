import os
from unittest.mock import Mock, patch
from project_report import ProjectReport
from egcg_core.config import cfg
from tests import TestProjectManagement


def ppath(ext):
    return 'project_report.' + ext


class FakeSample:
    def __init__(self, name, udf):
        self.name = name
        self.udf = udf


fake_samples = {
    'a_project_name': [
        FakeSample(
            name='sample:1',
            udf={'Prep Workflow': None, 'Species': 'Thingius thingy'}
        ),
        FakeSample(
            name='sample:2',
            udf={'Prep Workflow': None, 'Species': 'Thingius thingy'}
        )
    ],
    'human_truseq_nano': [
        FakeSample(
            name='human_truseq_nano_sample_1',
            udf={'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Homo sapiens'}
        )
    ],
    'non_human_truseq_nano': [
        FakeSample(
            name='non_human_truseq_nano_sample_1',
            udf={'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Thingius thingy'}
        )
    ],
    'human_pcr_free': [
        FakeSample(
            name='human_pcr_free_sample_1',
            udf={'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Homo sapiens'}
        )
    ],
    'non_human_pcr_free': [
        FakeSample(
            name='non_human_pcr_free_sample_1',
            udf={'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Thingius thingy'}
        )
    ]
}


class FakeLims:
    @staticmethod
    def get_projects(name):
        return [
            Mock(
                udf={
                    'Project Title': 'a_research_title_for_' + name,
                    'Enquiry Number': '1337',
                    'Quote No.': '1338'
                },
                researcher=Mock(
                    first_name='First',
                    last_name='Last',
                    email='first.last@email.com'
                )
            )
        ]

    @staticmethod
    def get_samples(projectname):
        return fake_samples[projectname]


class TestProjectReport(TestProjectManagement):
    def setUp(self):
        cfg.load_config_file(TestProjectManagement.etc_config)
        self.pr = ProjectReport('a_project_name')
        self.pr.lims = FakeLims()
        self.fake_samples = fake_samples['a_project_name']
        os.chdir(TestProjectManagement.root_path)

    def test_get_project_info(self):
        exp = (
            ('Project name:', self.pr.project_name),
            ('Project title:', 'a_research_title_for_a_project_name'),
            ('Enquiry no:', '1337'),
            ('Quote no:', '1338'),
            ('Researcher:', 'First Last (first.last@email.com)')
        )
        assert self.pr.get_project_info() == exp

    def test_samples_for_project(self):
        assert self.pr._samples_for_project is None
        assert self.pr.samples_for_project == self.fake_samples
        assert self.pr._samples_for_project == self.fake_samples

    def test_get_sample(self):
        assert self.pr.get_sample('sample:1') == self.fake_samples[0]

    def test_get_all_sample_names(self):
        assert self.pr.get_all_sample_names() == ['sample:1', 'sample:2']
        assert self.pr.get_all_sample_names(modify_names=True) == ['sample_1', 'sample_2']

    def test_get_library_workflow(self):
        assert self.pr.get_library_workflow_from_sample('sample:1') is None

    def test_get_species(self):
        assert self.pr.get_species_from_sample('sample:1') == 'Thingius thingy'

    def test_update_program_from_csv(self):
        assert len(self.pr.params) == 3
        program_csv = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'source',
            'a_project_name',
            'sample_1',
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
            'sample_1',
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
        assert self.pr.get_html_template() == 'truseq_nano_non_human.html'

    @patch(ppath('path.getsize'), return_value=1)
    def test_get_folder_size(self, mocked_getsize):
        d = os.path.join(TestProjectManagement.root_path, 'project_report', 'templates')
        obs = self.pr.get_folder_size(d)
        assert obs == 10


def test_project_types():
    os.chdir(TestProjectManagement.root_path)
    projects = ('human_truseq_nano', 'human_pcr_free', 'non_human_truseq_nano', 'non_human_pcr_free')
    for p in projects:
        pr = ProjectReport(p)
        pr.lims = FakeLims()
        pr.generate_report('html')
