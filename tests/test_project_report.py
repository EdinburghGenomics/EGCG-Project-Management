import os
from unittest.mock import Mock, patch
from project_report import ProjectReport
from egcg_core.config import cfg
from tests import TestProjectManagement
from collections import OrderedDict
cfg.load_config_file(TestProjectManagement.etc_config)


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
        self.pr = ProjectReport('a_project_name')
        self.pr.lims = FakeLims()
        self.fake_samples = fake_samples['a_project_name']
        os.chdir(TestProjectManagement.root_path)

    @patch(ppath('ProjectReport.get_folder_size'), return_value=1337000000000)
    @patch(ppath('ProjectReport.get_all_sample_names'), return_value=['sample_one', 'sample_two'])
    @patch(ppath('ProjectReport.get_samples_delivered'), return_value=2)
    @patch(ppath('ProjectReport.get_library_workflow'), return_value='TruSeq Nano DNA Sample Prep')
    @patch(ppath('get_species_from_sample'), return_value='Human')
    @patch(ppath('get_genome_version'), return_value='hg38')
    def test_get_project_info(self, mocked_genome, mocked_species, mocked_library_workflow, mocked_delivered_samples, mocked_sample_names, mocked_project_size):
        exp = (
            ('Project name:', self.pr.project_name),
            ('Project title:', 'a_research_title_for_a_project_name'),
            ('Enquiry no:', '1337'),
            ('Quote no:', '1338'),
            ('Number of Samples', 2),
            ('Number of Samples Delivered', 2),
            ('Project Size', '1.34 Terabytes'),
            ('Laboratory Protocol', 'TruSeq Nano DNA Sample Prep'),
            ('Submitted Species', 'Human'),
            ('Genome Used for Mapping', 'hg38')
        )
        assert self.pr.get_project_info() == exp

    def test_samples_for_project(self):
        assert self.pr.samples_for_project_lims == self.fake_samples

    def test_get_sample(self):
        assert self.pr.get_sample('sample:1') == self.fake_samples[0]

    def test_get_all_sample_names(self):
        assert self.pr.get_all_sample_names() == ['sample:1', 'sample:2']
        assert self.pr.get_all_sample_names(modify_names=True) == ['sample_1', 'sample_2']

    def test_get_library_workflow(self):
        assert self.pr.get_library_workflow_from_sample('sample:1') is None

    def test_get_report_type(self):
        assert self.pr.get_report_type_from_sample('sample:1') == 'Thingius thingy'
        self.pr.project_name = 'human_truseq_nano'
        self.pr._lims_samples_for_project = None
        assert self.pr.get_report_type_from_sample('human_truseq_nano_sample_1') == 'Human'

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
    @patch(ppath('ProjectReport.get_project_stats'), return_value=OrderedDict([('Total yield (Gb):', '524.13'),
                                                                               ('Average yield (Gb):', '131.0'),
                                                                               ('Average percent duplicate reads:', 17.380661102525934),
                                                                               ('Average percent mapped reads:', 85.45270355584897),
                                                                               ('Average percent Q30:', 80.32382821869467)]))
    def test_get_sample_info(self, mocked_project_stats, mocked_project_size):
        project_stats = self.pr.get_sample_info()


        assert project_stats == [('Total yield (Gb):', '524.13'),
                               ('Average yield (Gb):', '131.0'),
                               ('Average percent duplicate reads:', 17.380661102525934),
                               ('Average percent mapped reads:', 85.45270355584897),
                               ('Average percent Q30:', 80.32382821869467)]
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
        assert self.pr.get_html_template().get('template_base') == 'truseq_nano_non_human.html'
        self.pr.project_name = 'human_truseq_nano'
        self.pr._lims_samples_for_project = None
        assert self.pr.get_html_template().get('template_base') == 'truseq_nano.html'

    @patch(ppath('path.getsize'), return_value=1)
    def test_get_folder_size(self, mocked_getsize):
        d = os.path.join(TestProjectManagement.root_path, 'project_report', 'templates')
        obs = self.pr.get_folder_size(d)
        assert obs == 9

@patch(ppath('ProjectReport.get_project_stats'), return_value=OrderedDict([('Total yield (Gb):', '524.13'),
                                                                               ('Average yield (Gb):', '131.0'),
                                                                               ('Average percent duplicate reads:', 17.380661102525934),
                                                                               ('Average percent mapped reads:', 85.45270355584897),
                                                                               ('Average percent Q30:', 80.32382821869467)]))
@patch(ppath('ProjectReport.get_folder_size'), return_value=1337000000000)
@patch(ppath('ProjectReport.generate_csv'), return_value=None)
@patch(ppath('ProjectReport.chart_data'), return_value=(None, None, None))
@patch(ppath('ProjectReport.get_project_sample_metrics'), return_value=None)
@patch(ppath('ProjectReport.get_project_info'), return_value=(('Project name:', 'name'),
                                                            ('Project title:', 'a_research_title_for_a_project_name'),
                                                            ('Enquiry no:', '1337'),
                                                            ('Quote no:', '1338'),
                                                            ('Number of Samples', 2),
                                                            ('Number of Samples Delivered', 2),
                                                            ('Project Size', '1.34 Terabytes'),
                                                            ('Laboratory Protocol', 'TruSeq Nano DNA Sample Prep')))
@patch(ppath('ProjectReport.get_all_sample_names'), return_value=['sample_one', 'sample_two'])
@patch(ppath('ProjectReport.get_samples_delivered'), return_value=2)
@patch(ppath('ProjectReport.get_library_workflow'), return_value='TruSeq Nano DNA Sample Prep')
@patch(ppath('ProjectReport.get_species'), return_value=['Human', 'Human', 'Mouse', 'Mouse'])
def test_project_types(mocked_species,
                       mocked_workflow,
                       mocked_delivered,
                       mocked_names,
                       mocked_project_info,
                       mocked_project_metrics,
                       mocked_charts,
                       mocked_csv,
                       mocked_folder_size,
                       mocked_project_stats):
    os.chdir(TestProjectManagement.root_path)
    projects = ('human_truseq_nano', 'human_pcr_free', 'non_human_truseq_nano', 'non_human_pcr_free')
    for p in projects:
        pr = ProjectReport(p)
        pr.lims = FakeLims()
        pr.generate_report('html')
