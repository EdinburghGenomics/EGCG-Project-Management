from os.path import join
from unittest.mock import Mock
from project_report import ProjectReport
from egcg_core.config import cfg
from tests import TestProjectManagement
cfg.load_config_file(TestProjectManagement.etc_config)


class FakeSample:
    def __init__(self, name, udf):
        self.name = name
        self.udf = udf

fake_samples = [
    FakeSample(
        name='sample:1',
        udf={
            'Prep Workflow': 'a_workflow',
            'Species': 'Thingius thingy'
        }
    ),
    FakeSample(
        name='sample:2',
        udf={
            'Prep Workflow': 'a_workflow',
            'Species': 'Thingius thingy'
        }
    )
]


class FakeLims:
    @staticmethod
    def get_projects(name):
        return [
            Mock(
                udf={
                    'Project Title': name,
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
        return fake_samples


class TestProjectReport(TestProjectManagement):
    def setUp(self):
        self.pr = ProjectReport('a_project_name')
        self.pr.lims = FakeLims()

    def test_get_project_info(self):
        exp = (
            ('Project name:', self.pr.project_name),
            ('Project title:', 'a_project_name'),
            ('Enquiry no:', '1337'),
            ('Quote no:', '1338'),
            ('Researcher:', 'First Last (first.last@email.com)')
        )
        assert self.pr.get_project_info() == exp

    def test_samples_for_project(self):
        assert self.pr._samples_for_project is None
        assert self.pr.samples_for_project == fake_samples
        assert self.pr._samples_for_project == fake_samples

    def test_get_sample(self):
        assert self.pr.get_sample('sample:1') == fake_samples[0]

    def test_get_all_sample_names(self):
        assert self.pr.get_all_sample_names() == ['sample:1', 'sample:2']
        assert self.pr.get_all_sample_names(modify_names=True) == ['sample_1', 'sample_2']

    def test_get_library_workflow(self):
        assert self.pr.get_library_workflow_from_sample('sample:1') == 'a_workflow'

    def test_get_species(self):
        assert self.pr.get_species_from_sample('sample:1') == 'Thingius thingy'

    def test_update_program_from_csv(self):
        assert len(self.pr.params) == 3
        program_csv = join(
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
        summary_yaml = join(
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
        metrics_csv = join(
            TestProjectManagement.assets_path,
            'project_report',
            'dest',
            'a_project_name',
            'summary_metrics.csv'
        )
        obs = self.pr.read_metrics_csv(metrics_csv)
        assert obs == exp

    def test_get_sample_info(self):
        pass

    def test_get_html_template(self):
        pass

    def test_generate_report(self):
        pass

    def test_get_html_content(self):
        pass

    def test_get_folder_size(self):
        dest_dir = join(TestProjectManagement.assets_path, 'project_report', 'dest')
        obs = self.pr.get_folder_size(dest_dir)
        assert obs == 573
