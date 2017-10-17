import os
from collections import Counter
from random import randint
from unittest.mock import Mock, PropertyMock, patch
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
            udf={'Prep Workflow': None, 'Species': 'Thingius thingy', 'Genome Version': 'hg38, hg37'}
        ),
        FakeSample(
            name='sample:2',
            udf={'Prep Workflow': None, 'Species': 'Thingius thingy', 'Genome Version': 'hg38, hg37'}
        ),
        FakeSample(
            name='sample:3',
            udf={'Prep Workflow': None, 'Species': 'Thingius thingy', 'Genome Version': 'hg38, hg37'}
        ),
        FakeSample(
            name='sample:4',
            udf={'Prep Workflow': None, 'Species': 'Thingius thingy', 'Genome Version': 'hg38, hg37'}
        )
    ],
    'human_truseq_nano': [
        FakeSample(
            name='human_truseq_nano_sample_1',
            udf={'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Homo sapiens', 'Genome Version': 'hg38'}
        )
    ],
    'non_human_truseq_nano': [
        FakeSample(
            name='non_human_truseq_nano_sample_1',
            udf={'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Thingius thingy', 'Genome Version': 'hg38'}
        )
    ],
    'human_pcr_free': [
        FakeSample(
            name='human_pcr_free_sample_1',
            udf={'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Homo sapiens', 'Genome Version': 'hg38'}
        )
    ],
    'non_human_pcr_free': [
        FakeSample(
            name='non_human_pcr_free_sample_1',
            udf={'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Thingius thingy', 'Genome Version': 'hg38'}
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


rest_api_sample1 = {'tiles_filtered': [],
                    'clean_pc_q30': 80.66940576163829,
                    'mapped_reads': 837830805,
                    'clean_yield_q30': 0.893051514,
                    'clean_yield_in_gb': 1.107051063,
                    'called_gender': 'unknown',
                    'bam_file_reads': 849852870,
                    'properly_mapped_reads': 813246360,
                    'user_sample_id': 'test_10015AT_1',
                    'clean_pc_q30_r1': 89.52611100665973,
                    'pc_mapped_reads': 98.58539455188284,
                    'sample_id': '10015AT0001',
                    'clean_pc_q30_r2': 71.8072031228874,
                    'duplicate_reads': 98921148,
                    'species_name': 'Homo sapiens',
                    'expected_yield_q30': 1.0,
                    'pc_properly_mapped_reads': 95.69260618017327,
                    'pc_duplicate_reads': 11.639796898020712,
                    'pc_pass_filter': 100.0,
                    'project_id': '10015AT',
                    'gender_match': 'unknown',
                    'sample_contamination': {'freemix': 0.0, 'ti_tv_ratio': 1.95, 'het_hom_ratio': 0.07},
                    'coverage': {'bases_at_coverage': {'bases_at_15X': 300}}, 'mean': 21, 'evenness': 15}

rest_api_sample2 = {'tiles_filtered': [],
                    'clean_pc_q30': 80.52789488784828,
                    'mapped_reads': 914871303,
                    'clean_yield_q30': 0.953095261,
                    'clean_yield_in_gb': 1.183559141,
                    'called_gender': 'unknown',
                    'bam_file_reads': 930580648,
                    'properly_mapped_reads': 894575183,
                    'user_sample_id': 'test_10015AT_4',
                    'clean_pc_q30_r1': 89.09414824127175,
                    'pc_mapped_reads': 98.31187710234869,
                    'sample_id': '10015AT0004',
                    'clean_pc_q30_r2': 71.95898669434135,
                    'duplicate_reads': 124298931,
                    'species_name': 'Homo sapiens',
                    'expected_yield_q30': 1.0,
                    'pc_properly_mapped_reads': 96.13086033140891,
                    'pc_duplicate_reads': 13.357136887291041,
                    'pc_pass_filter': 100.0,
                    'project_id': '10015AT',
                    'gender_match': 'unknown',
                    'sample_contamination': {'freemix': 0.0, 'ti_tv_ratio': 1.95, 'het_hom_ratio': 0.07},
                    'coverage': {'bases_at_coverage': {'bases_at_15X': 310}}, 'mean': 20, 'evenness': 10}

test_sample_yield_metrics = {'samples': [], 'clean_yield': [], 'clean_yield_Q30': []}
for i in range(1,5):
    test_sample_yield_metrics['samples'].append('TestSample%s' % i)
    clean_yield_val = randint(100, 150)
    clean_yield_q30_val = clean_yield_val - randint(10,30)
    test_sample_yield_metrics['clean_yield'].append(clean_yield_val)
    test_sample_yield_metrics['clean_yield_Q30'].append(clean_yield_q30_val)

test_pc_statistics = {'pc_duplicate_reads': [], 'pc_properly_mapped_reads': [], 'pc_pass_filter': [], 'samples': []}
for i in range(1,5):
    test_pc_statistics['samples'].append('TestSample%s' % i)
    test_pc_statistics['pc_duplicate_reads'].append(randint(10,30))
    test_pc_statistics['pc_properly_mapped_reads'].append(randint(80,100))
    test_pc_statistics['pc_pass_filter'].append(randint(90,100))


mocked_get_folder_size = patch(ppath('ProjectReport.get_folder_size'), return_value=1337000000000)
mocked_get_all_sample_names = patch(ppath('ProjectReport.get_all_sample_names'), return_value=['sample:1', 'sample:2'])
mocked_get_samples_delivered = patch(ppath('ProjectReport.get_samples_delivered'), return_value=2)
mocked_get_folder_size = patch(ppath('ProjectReport.get_folder_size'), return_value=1337000000000)
mocked_get_library_workflow = patch(ppath('ProjectReport.get_library_workflow'), return_value='TruSeq Nano DNA Sample Prep')
mocked_get_species_from_sample = patch(ppath('get_species_from_sample'), return_value='Human')
mocked_get_genome_version = patch(ppath('get_genome_version'), side_effect='hg38')
mocked_csv = patch(ppath('ProjectReport.csv_file'), return_value='/path/to/csv/project_report.csv')
mocked_samples_for_project_restapi = patch(ppath('ProjectReport.samples_for_project_restapi'), new_callable=PropertyMock(return_value=[rest_api_sample1, rest_api_sample2]))
mocked_calculate_project_statistics = patch(ppath('ProjectReport.calculate_project_statistsics'), return_value=OrderedDict([('Total yield (Gb):', '524.13'),
                                                                               ('Average yield (Gb):', '131.0'),
                                                                               ('Average percent duplicate reads:', 17.380661102525934),
                                                                               ('Average percent mapped reads:', 85.45270355584897),
                                                                               ('Average percent Q30:', 80.32382821869467)]))



class TestProjectReport(TestProjectManagement):
    def setUp(self):
        cfg.load_config_file(TestProjectManagement.etc_config)
        self.pr = ProjectReport('a_project_name')
        self.pr.lims = FakeLims()
        self.fake_samples = fake_samples['a_project_name']
        os.chdir(TestProjectManagement.root_path)

    @mocked_get_folder_size
    @mocked_get_all_sample_names
    @mocked_get_samples_delivered
    @mocked_get_library_workflow
    def test_get_project_info(self, mocked_library_workflow, mocked_delivered_samples, mocked_sample_names, mocked_project_size):
        exp = (('Project name:', 'a_project_name'),
               ('Project title:', 'a_research_title_for_a_project_name'),
               ('Enquiry no:', '1337'),
               ('Quote no:', '1338'),
               ('Number of Samples', 2),
               ('Number of Samples Delivered', 2),
               ('Project Size', '1.34 Terabytes'),
               ('Laboratory Protocol', 'TruSeq Nano DNA Sample Prep'),
               ('Submitted Species', 'Thingius thingy'),
               ('Genome Used for Mapping', 'hg38, hg37'))
        assert self.pr.get_project_info() == exp

    def test_get_list_of_sample_fields(self):
        samples = [rest_api_sample1, rest_api_sample2]
        assert Counter(self.pr.get_list_of_sample_fields(samples, 'evenness')) == Counter([10, 15])
        assert Counter(self.pr.get_list_of_sample_fields(samples, 'coverage', subfields=['bases_at_coverage', 'bases_at_15X'])) == Counter([300, 310])

    def test_samples_for_project(self):
        assert self.pr.samples_for_project_lims == self.fake_samples

    def test_get_sample(self):
        assert self.pr.get_sample('sample:1') == self.fake_samples[0]

    def test_get_all_sample_names(self):
        assert self.pr.get_all_sample_names() == ['sample:1', 'sample:2','sample:3', 'sample:4']
        assert self.pr.get_all_sample_names(modify_names=True) == ['sample_1', 'sample_2', 'sample_3', 'sample_4']

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

    @mocked_get_folder_size
    @mocked_calculate_project_statistics
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
        assert self.pr.get_html_template().get('template_base') == 'report_base.html'

    @patch(ppath('path.getsize'), return_value=1)
    def test_get_folder_size(self, mocked_getsize):
        d = os.path.join(TestProjectManagement.root_path, 'project_report', 'templates')
        obs = self.pr.get_folder_size(d)
        assert obs == 7

@mocked_csv
@mocked_samples_for_project_restapi
@patch(ppath('ProjectReport.get_sample_yield_metrics'), return_value=test_sample_yield_metrics)
@patch(ppath('ProjectReport.get_pc_statistics'), return_value=test_pc_statistics)
def test_project_types(mock_qc_plot,
                       mock_yield_plot,
                       mock_samples_for_project,
                       mocked_csv):
    os.chdir(TestProjectManagement.root_path)
    projects = ('human_truseq_nano', 'human_pcr_free', 'non_human_truseq_nano', 'non_human_pcr_free')
    for p in projects:
        pr = ProjectReport(p)
        pr.lims = FakeLims()
        pr.generate_report('pdf')

@mocked_csv
@mocked_samples_for_project_restapi
@patch(ppath('ProjectReport.get_sample_yield_metrics'), return_value=test_sample_yield_metrics)
@patch(ppath('ProjectReport.get_pc_statistics'), return_value=test_pc_statistics)
def test_run_report(mock1, mock2, mock3, mock4):
    pr = ProjectReport('a_project_name')
    pr.lims = FakeLims()
    pr.generate_report('pdf')
