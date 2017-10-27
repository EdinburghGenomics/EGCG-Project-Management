import copy
import glob
import os
from collections import Counter
from itertools import cycle
from random import randint
from unittest.mock import Mock, PropertyMock, patch
import shutil

import collections

from project_report import ProjectReport
from egcg_core.config import cfg
from tests import TestProjectManagement
from collections import OrderedDict
cfg.load_config_file(TestProjectManagement.etc_config)

nb_samples = 5

def ppath(ext):
    return 'project_report.' + ext


class FakeSample:
    def __init__(self, name, udf):
        self.name = name
        self.udf = udf

fake_sample_templates = {
    'a_project_name': {
        'name':'sample:',
        'udf': {'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Thingius thingy', 'Genome Version': 'hg38'}
    },
    'human_truseq_nano': {
        'name':'human_truseq_nano_sample_',
        'udf': {'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Homo sapiens', 'Genome Version': 'hg38'}
    },
    'non_human_truseq_nano': {
        'name':'non_human_truseq_nano_sample_',
        'udf':{'Prep Workflow': 'TruSeq Nano DNA Sample Prep', 'Species': 'Thingius thingy'}
    },
    'human_pcr_free': {
        'name': 'human_truseq_pcrfree_sample_',
        'udf': {'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Homo sapiens', 'Genome Version': cycle(['hg38', 'hg19'])}
    },
    'non_human_pcr_free': {
        'name': 'non_human_truseq_pcrfree_sample_',
        'udf':{'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep', 'Species': 'Thingius thingy'}
    },
}

fake_samples = {}


def _resolve_next(o):
    if not isinstance(o, str) and isinstance(o, collections.Iterable):
        return next(o)
    return o

for project in fake_sample_templates:
    fake_samples[project] = []
    for i in range(1, nb_samples+1):
        template = fake_sample_templates[project]
        fake_samples[project].append(FakeSample(
            name=template['name'] + str(i),
            udf=dict( [(k, _resolve_next(template['udf'][k])) for k in template['udf']] )
        ))


class FakeLims:
    @staticmethod
    def get_projects(name):
        return [Mock(
            udf={'Project Title': 'a_research_title_for_' + name, 'Enquiry Number': '1337', 'Quote No.': '1338'},
            researcher=Mock(first_name='First', last_name='Last', email='first.last@email.com')
        )]

    @staticmethod
    def get_samples(projectname):
        return fake_samples[projectname]


rest_api_sample1 = {
    'clean_pc_q30': 80.66940576163829,
    'mapped_reads': 837830805,
    'clean_yield_q30': 0.893051514,
    'clean_yield_in_gb': 1.107051063,
    'properly_mapped_reads': 813246360,
    'user_sample_id': 'test_10015AT_1',
    'pc_mapped_reads': 98.58539455188284,
    'sample_id': '10015AT0001',
    'duplicate_reads': 98921148,
    'species_name': 'Homo sapiens',
    'expected_yield_q30': 1.0,
    'pc_properly_mapped_reads': 95.69260618017327,
    'pc_duplicate_reads': 11.639796898020712,
    'pc_pass_filter': 100.0,
    'project_id': '10015AT',
    'sample_contamination': {'freemix': 0.0, 'ti_tv_ratio': 1.95, 'het_hom_ratio': 0.07},
    'coverage': {'bases_at_coverage': {'bases_at_15X': 300}, 'mean': 21, 'evenness': 15}
}

rest_api_sample2 = {
    'clean_pc_q30': 80.52789488784828,
    'mapped_reads': 914871303,
    'clean_yield_q30': 0.953095261,
    'clean_yield_in_gb': 1.183559141,
    'properly_mapped_reads': 894575183,
    'user_sample_id': 'test_10015AT_2',
    'pc_mapped_reads': 98.31187710234869,
    'sample_id': '10015AT0002',
    'duplicate_reads': 124298931,
    'species_name': 'Homo sapiens',
    'expected_yield_q30': 1.0,
    'pc_properly_mapped_reads': 96.13086033140891,
    'pc_duplicate_reads': 13.357136887291041,
    'pc_pass_filter': 100.0,
    'project_id': '10015AT',
    'sample_contamination': {'freemix': 0.0, 'ti_tv_ratio': 1.95, 'het_hom_ratio': 0.07},
    'coverage': {'bases_at_coverage': {'bases_at_15X': 310}, 'mean': 20, 'evenness': 10}
}

rest_api_sample3 = {
    'clean_pc_q30': 80.66940576163829,
    'mapped_reads': 837830805,
    'clean_yield_q30': 0.893051514,
    'clean_yield_in_gb': 1.107051063,
    'properly_mapped_reads': 813246360,
    'user_sample_id': 'test_10015AT_3',
    'pc_mapped_reads': 98.58539455188284,
    'sample_id': '10015AT0003',
    'duplicate_reads': 98921148,
    'species_name': 'Homo sapiens',
    'expected_yield_q30': 1.0,
    'pc_properly_mapped_reads': 95.69260618017327,
    'pc_duplicate_reads': 11.639796898020712,
    'pc_pass_filter': 100.0,
    'project_id': '10015AT',
    'sample_contamination': {'freemix': 0.0, 'ti_tv_ratio': 1.95, 'het_hom_ratio': 0.07},
    'coverage': {'bases_at_coverage': {'bases_at_15X': 300}, 'mean': 21, 'evenness': 15}
}

rest_api_sample4 = {
    'clean_pc_q30': 80.52789488784828,
    'mapped_reads': 914871303,
    'clean_yield_q30': 0.953095261,
    'clean_yield_in_gb': 1.183559141,
    'properly_mapped_reads': 894575183,
    'user_sample_id': 'test_10015AT_4',
    'pc_mapped_reads': 98.31187710234869,
    'sample_id': '10015AT0004',
    'duplicate_reads': 124298931,
    'species_name': 'Homo sapiens',
    'expected_yield_q30': 1.0,
    'pc_properly_mapped_reads': 96.13086033140891,
    'pc_duplicate_reads': 13.357136887291041,
    'pc_pass_filter': 100.0,
    'project_id': '10015AT',
    'sample_contamination': {'freemix': 0.0, 'ti_tv_ratio': 1.95, 'het_hom_ratio': 0.07},
    'coverage': {'bases_at_coverage': {'bases_at_15X': 310}, 'mean': 20, 'evenness': 10}
}
fake_rest_api_samples_template = cycle([rest_api_sample1, rest_api_sample2, rest_api_sample3, rest_api_sample4])

fake_rest_api_samples={}
for project in fake_samples:
    fake_rest_api_samples[project] = []
    for sample in fake_samples[project]:
        t = copy.copy(next(fake_rest_api_samples_template))
        t['sample_id'] = sample.name
        t['project_id'] = project
        fake_rest_api_samples[project].append(t)


test_sample_yield_metrics = {'samples': [], 'clean_yield': [], 'clean_yield_Q30': []}
for i in range(1, nb_samples+1):
    test_sample_yield_metrics['samples'].append('TestSample%s' % i)
    clean_yield_val = randint(100, 150)
    clean_yield_q30_val = clean_yield_val - randint(10,30)
    test_sample_yield_metrics['clean_yield'].append(clean_yield_val)
    test_sample_yield_metrics['clean_yield_Q30'].append(clean_yield_q30_val)

test_pc_statistics = {'pc_duplicate_reads': [], 'pc_mapped_reads': [], 'samples': []}
for i in range(1, nb_samples+1):
    test_pc_statistics['samples'].append('TestSample%s' % i)
    test_pc_statistics['pc_duplicate_reads'].append(randint(10,30))
    test_pc_statistics['pc_mapped_reads'].append(randint(80,100))


mocked_get_folder_size = patch(ppath('ProjectReport.get_folder_size'), return_value=1337000000000)
mocked_get_library_workflow = patch(ppath('ProjectReport.get_library_workflow'), return_value='TruSeq Nano DNA Sample Prep')
mocked_get_species_from_sample = patch(ppath('get_species_from_sample'), return_value='Human')
mocked_get_genome_version = patch(ppath('get_genome_version'), side_effect=cycle(['hg38, hg19']))
mocked_csv = patch(ppath('ProjectReport.write_csv_file'), return_value='/path/to/csv/project_report.csv')


def get_patch_sample_restapi(project_name):
    path = ppath('ProjectReport.samples_for_project_restapi')
    return patch(path, new_callable=PropertyMock(return_value=fake_rest_api_samples[project_name]))


mocked_calculate_project_statistics = patch(ppath('ProjectReport.calculate_project_statistsics'), return_value=OrderedDict([
    ('Total yield (Gb):', '524.13'),
    ('Average yield (Gb):', '131.0'),
    ('Average percent duplicate reads:', 17.380661102525934),
    ('Average percent mapped reads:', 85.45270355584897),
    ('Average percent Q30:', 80.32382821869467)
]))
mocked_sample_yield_metrics = patch(ppath('ProjectReport.get_sample_yield_metrics'), return_value=test_sample_yield_metrics)
mocked_pc_statistics = patch(ppath('ProjectReport.get_pc_statistics'), return_value=test_pc_statistics)
mocked_get_species_found = patch(ppath('ProjectReport.get_species_found'), side_effect=cycle(['Homo sapiens', 'Homo sapiens', 'Homo sapiens', 'Gallus gallus']))
mocked_get_library_workflow_from_sample = patch(ppath('ProjectReport.get_library_workflow_from_sample'), return_value='Nano')


class TestProjectReport(TestProjectManagement):
    def setUp(self):
        cfg.load_config_file(self.etc_config)
        self.pr = ProjectReport('a_project_name')
        self.pr.lims = FakeLims()
        self.fake_samples = fake_samples['a_project_name']
        os.chdir(self.root_path)
        self.source_dir = os.path.join(self.assets_path, 'project_report', 'source')

        #clean up previous reports
        project_report_pdfs = glob.glob(os.path.join(self.assets_path, 'project_report', 'dest', '*', '*.pdf'))
        for pdf in project_report_pdfs:
            os.remove(pdf)

        # create the source folders
        for project in fake_samples:
            prj_dir = os.path.join(self.source_dir, project)
            os.makedirs(prj_dir, exist_ok=True)
            for sample in fake_samples[project]:
                smp_dir = os.path.join(prj_dir, sample.name.replace(':', '_'))
                os.makedirs(smp_dir, exist_ok=True)
                with open(os.path.join(smp_dir, 'programs.txt'), 'w') as open_file:
                    open_file.write('bcbio,1.1\nbwa,1.2\ngatk,1.3\nsamblaster,1.4\n')
                with open(os.path.join(smp_dir, 'project-summary.yaml'), 'w') as open_file:
                    open_file.write('samples:\n- dirs:\n    galaxy: path/to/bcbio/bcbio-0.9.4/galaxy\n  genome_build: hg38\n')

    def tearDown(self):
        # delete the source folders
        for project in fake_samples:
            shutil.rmtree(os.path.join(self.source_dir, project))

    @mocked_get_genome_version
    @mocked_get_folder_size
    @patch(ppath('ProjectReport.get_samples_delivered'), return_value=4)
    def test_get_project_info(self, mocked_get_sample_delivered, mocked_folder_size, mocked_genome_version):
        exp = (('Project name', 'a_project_name'),
               ('Project title', 'a_research_title_for_a_project_name'),
               ('Enquiry no', '1337'),
               ('Quote no', '1338'),
               ('Number of samples', len(fake_samples['a_project_name'])),
               ('Number of samples delivered', 4),
               ('Project size', '1.34 terabytes'),
               ('Laboratory protocol', 'TruSeq Nano DNA Sample Prep'),
               ('Submitted species', 'Thingius thingy'),
               ('Genome version', 'hg38, hg19'))
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
        names = [s.name for  s in fake_samples['a_project_name']]
        assert self.pr.get_all_sample_names() == names
        assert self.pr.get_all_sample_names(modify_names=True) == [n.replace(':', '_') for n in names]

    def test_get_library_workflow(self):
        assert self.pr.get_library_workflow_from_sample('sample:1') == 'TruSeq Nano DNA Sample Prep'

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
        assert obs == 8

    @mocked_csv
    @mocked_sample_yield_metrics
    @mocked_pc_statistics
    def test_project_types(self, mocked_pc_statistics,
                           mocked_sample_yield_metrics,
                           mocked_csv):
        os.chdir(TestProjectManagement.root_path)
        projects = ('human_truseq_nano', 'human_pcr_free', 'non_human_truseq_nano', 'non_human_pcr_free')
        # projects = ('human_truseq_nano',)

        for p in projects:
            with mocked_get_genome_version, mocked_get_species_found, get_patch_sample_restapi(p):
                pr = ProjectReport(p)
                pr.lims = FakeLims()
                pr.generate_report('pdf')
            report = os.path.join(self.assets_path, 'project_report', 'dest', p, 'project_%s_report.pdf' % p)
            assert os.path.isfile(report)

