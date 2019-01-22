import os
import glob
import shutil
import datetime
import collections
from itertools import cycle
from random import randint, random
from unittest.mock import Mock, PropertyMock, patch

from egcg_core.util import query_dict

from project_report.project_information import ProjectReportInformation
from project_report.project_report_latex import ProjectReportLatex
from tests import TestProjectManagement, NamedMock

nb_samples = 50


def ppath(ext):
    return 'project_report.project_information.' + ext


class FakeArtifact:
    def __init__(self, sample):
        self.samples = [sample]


class FakeSample:
    def __init__(self, name, udf):
        self.name = name
        self.udf = udf


fake_sample_templates = {
    'a_project_name': {
        'name': 'sample_',
        'udf': {
            'Prep Workflow': 'TruSeq Nano DNA Sample Prep',
            'Species': 'Thingius thingy',
            'Genome Version': 'hg38',
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': 120,
            'Coverage (X)': 30
        }
    },
    'hmix999': {
        'name': 'HS_mix_',
        'udf': {
            'Prep Workflow': cycle(['TruSeq Nano DNA Sample Prep', 'TruSeq PCR-Free DNA Sample Prep']),
            'Species': 'Homo sapiens',
            'User Prepared Library': cycle([None, None, 'Yes']),
            'Genome Version': 'hg38',
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': cycle([120, 60]),
            'Coverage (X)': cycle([30, 15])
        }
    },
    'nhtn999': {
        'name': 'non_HS_nano_',
        'udf': {
            'Prep Workflow': 'TruSeq Nano DNA Sample Prep',
            'Species': 'Thingius thingy',
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': 120,
            'Coverage (X)': 30,
            'Analysis Type': 'Variant Calling gatk'
        }
    },
    'hpf999': {
        'name': 'HS_pcrfree_',
        'udf': {
            'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep',
            'Species': 'Homo sapiens',
            'Genome Version': cycle(['hg38', 'hg19']),
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': 120,
            'Coverage (X)': 30
        }
    },
    'nhpf999': {
        'name': 'non_HS_pcrfree_',
        'udf': {
            'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep',
            'Species': 'Thingius thingy',
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': 120,
            'Coverage (X)': 30
        }
    },
    'uhtn999': {
        'name': 'notsent_HS_nano_',
        'udf': {
            'Prep Workflow': cycle(['TruSeq Nano DNA Sample Prep', None, None]),
            'Species': 'Homo sapiens',
            'Genome Version': 'hg38',
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': 120,
            'Coverage (X)': 30
        }
    },
    'upl999': {
        'name': 'UPL_HS_',
        'udf': {
            'Prep Workflow': 'TruSeq Nano DNA Sample Prep',  # This sis sometime set and will be ignored
            'User Prepared Library': 'Yes',
            'Species': 'Homo sapiens',
            'Genome Version': 'hg38',
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': 120,
            'Coverage (X)': 30
        }
    }
}

fake_samples = {}


def _resolve_next(o):
    if not isinstance(o, str) and isinstance(o, collections.Iterable):
        return next(o)
    return o


for project in fake_sample_templates:
    fake_samples[project] = []
    for i in range(1, nb_samples + 1):
        template = fake_sample_templates[project]
        fake_samples[project].append(FakeSample(
            name=template['name'] + str(i),
            udf={k: _resolve_next(template['udf'][k]) for k in template['udf']}
        ))

d = datetime.datetime.strptime('2018-01-10', '%Y-%m-%d').date()

fake_process_templates = {
    'a_project_name': {'nb_processes': 1, 'date': d, 'finished': 'Yes', 'NC': 'NC12: Description of major issue'},
    'hmix999': {'nb_processes': 3, 'date': d, 'finished': 'Yes', 'NC': cycle(['NA', 'NA', 'NC25: Description minor issue', 'NA'])},
    'nhtn999': {'nb_processes': 2, 'date': d, 'finished': 'Yes', 'NC': cycle(['NA', 'NC25: Major issue'])},
    'hpf999': {'nb_processes': 1, 'date': d, 'finished': 'Yes', 'NC': 'NA'},
    'nhpf999': {'nb_processes': 1, 'date': d, 'finished': 'Yes', 'NC': 'NC85: All samples were bad quality.'},
    'uhtn999': {'nb_processes': 1, 'date': d, 'finished': 'No', 'NC': 'NA'},
    'upl999':  {'nb_processes': 1, 'date': d, 'finished': 'Yes', 'NC': 'NA'}
}

fake_processes = {}

for project in fake_process_templates:
    fake_processes[project] = []
    template = fake_process_templates[project]
    # Mimic undelivered sample when their library UDF is not set
    sample_to_deliver = [s for s in fake_samples.get(project) if s.udf.get('Prep Workflow') is not None]
    sample_sets = [sample_to_deliver[i::template.get('nb_processes')] for i in
                   range(template.get('nb_processes'))]
    for i, sample_set in enumerate(sample_sets):
        finished = 'No'
        date_run = template.get('date') + datetime.timedelta(days=i)
        if i + 1 == len(sample_sets):
            finished = template.get('finished')
        fake_processes[project].append(Mock(
            all_inputs=Mock(return_value=[FakeArtifact(sample) for sample in sample_set if sample]),
            date_run=date_run.strftime('%Y-%m-%d'),
            id=i + 1,
            udf={
                'Is this the final data release for the project?': finished,
                'Non-Conformances': _resolve_next(template.get('NC'))
            }
        ))


class FakeLims:
    fake_lims_researcher = NamedMock(
        name='First Last', first_name='First', last_name='Last', email='first.last@email.com',
        lab=NamedMock(name='Awesome lab')
    )

    def get_projects(self, name):
        return [Mock(
            udf={
                'Project Title': 'a_research_title_for_' + name,
                'Enquiry Number': '1337',
                'Quote No.': '1338',
                'Number of Quoted Samples': nb_samples,
                'Shipment Address Line 1': 'Institute of Awesomeness',
                'Shipment Address Line 2': '213 high street',
                'Shipment Address Line 3': '-',
                'Shipment Address Line 4': '-',
                'Shipment Address Line 5': '-'
            },
            researcher=self.fake_lims_researcher
        )]

    @staticmethod
    def get_samples(projectname):
        return fake_samples[projectname]

    @staticmethod
    def get_processes(type, projectname):
        if type == 'Data Release Trigger EG 1.0 ST':
            return fake_processes[projectname]


rest_api_sample1 = {
    'clean_pc_q30': 80.66940576163829,
    'mapped_reads': 837830805,
    'clean_yield_q30': 0.893051514,
    'clean_yield_in_gb': 124.107051063,
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
    'coverage': {'bases_at_coverage': {'bases_at_15X': 300}, 'mean': 29, 'evenness': 15}
}

fake_rest_api_samples = {}
for project in fake_samples:
    fake_rest_api_samples[project] = []
    for sample in fake_samples[project]:
        # Mimic undelivered sample when their library UDF is not set
        if sample.udf.get('Prep Workflow') is not None:
            req_yield = sample.udf.get('Required Yield (Gb)')
            req_cov = sample.udf.get('Coverage (X)')
            clean_yield_in_gb = randint(int(req_yield * .9), int(req_yield * 1.5))
            fake_rest_api_samples[project].append({
                'sample_id': sample.name,
                # Add variable padding to see the effect of long user sample names
                'user_sample_id': '_' * randint(0,15) + 'user_' + sample.name,
                'project_id': project,
                'species_name': sample.udf['Species'],
                'aggregated': {
                    'clean_yield_in_gb': clean_yield_in_gb,
                    'clean_pc_q30': clean_yield_in_gb * .8,
                    'pc_duplicate_reads': round(random() * 25, 1),
                    'pc_mapped_reads': round(95 + random() * 5, 1),
                    'run_ids': ['date_machine_number_flowcell1', 'date_machine_number_flowcell1'],
                },
                'coverage': {'mean': randint(int(req_cov * .9), int(req_cov * 1.5))}
            })

mocked_get_folder_size = patch(ppath('ProjectReportInformation.get_folder_size'), return_value=1337000000000)
mocked_sample_status = patch(ppath('ProjectReportInformation.sample_status'), return_value={'started_date': '2017-08-02T11:25:14.659000'})


def get_patch_sample_restapi(project_name):
    path = ppath('ProjectReportInformation.samples_for_project_restapi')
    return patch(path, new_callable=PropertyMock(return_value=fake_rest_api_samples[project_name]))


class TestProjectReport(TestProjectManagement):
    def setUp(self):
        self.fake_samples = fake_samples['a_project_name']
        self.source_dir = os.path.join(self.assets_path, 'project_report', 'source')
        self.working_dir = os.path.join(self.assets_path, 'project_report', 'work')
        os.makedirs(self.working_dir, exist_ok=True)
        self.dest_dir = os.path.join(self.assets_path, 'project_report', 'dest')

        self.pr = ProjectReportInformation('a_project_name')
        self.pr.lims = FakeLims()

        # create the source and dest folders
        for project in fake_samples:
            prj_dir = os.path.join(self.source_dir, project)
            dest_dir = os.path.join(self.dest_dir, project)
            os.makedirs(dest_dir, exist_ok=True)
            os.makedirs(prj_dir, exist_ok=True)
            for sample in fake_samples[project]:
                smp_dir = os.path.join(prj_dir, sample.name.replace(':', '_'))
                os.makedirs(smp_dir, exist_ok=True)
                if sample.udf['Species'] == 'Homo sapiens':
                    with open(os.path.join(smp_dir, 'programs.txt'), 'w') as open_file:
                        open_file.write('bcbio,1.1\nbwa,1.2\ngatk,1.3\nsamblaster,1.4\n')
                    with open(os.path.join(smp_dir, 'project-summary.yaml'), 'w') as open_file:
                        open_file.write(
                            'samples:\n- dirs:\n    galaxy: path/to/bcbio/bcbio-0.9.4/galaxy\n  genome_build: hg38\n')
                else:
                    with open(os.path.join(smp_dir, 'program_versions.yaml'), 'w') as open_file:
                        open_file.write('biobambam_sortmapdup: 2\nbwa: 1.2\ngatk: v1.3\nbcl2fastq: 2.1\nsamtools: 0.3')

        self.run_ids = set()
        for project in fake_rest_api_samples:
            for sample in fake_rest_api_samples[project]:
                for run_id in query_dict(sample, 'aggregated.run_ids'):
                    self.run_ids.add(run_id)
        for run_id in self.run_ids:
            run_dir = os.path.join(self.source_dir, run_id)
            os.makedirs(run_dir, exist_ok=True)
            with open(os.path.join(run_dir, 'program_versions.yaml'), 'w') as open_file:
                open_file.write('bcl2fastq: v2.17.1.14\n')

    def tearDown(self):
        # delete the source folders
        for project in fake_samples:
            shutil.rmtree(os.path.join(self.source_dir, project))
        shutil.rmtree(self.working_dir)


class TestProjectReportInformation(TestProjectReport):

    def test_customer_name(self):
        assert self.pr.customer_name == 'Awesome lab'
        # Remove the lab name from the researcher
        self.pr.lims.fake_lims_researcher = NamedMock(name='Firstname Lastname', lab=NamedMock(name=''))
        # Remove the cached project
        del self.pr.__dict__['project']
        assert self.pr.customer_name == 'Firstname Lastname'

    @mocked_get_folder_size
    def test_get_project_info(self, mocked_folder_size):
        exp = (('Project name', 'a_project_name'),
               ('Project title', 'a_research_title_for_a_project_name'),
               ('Enquiry no', '1337'),
               ('Quote no', '1338'),
               ('Customer name', 'Awesome lab'),
               ('Customer address', 'Institute of Awesomeness\n213 high street'),
               ('Number of samples', len(fake_samples['a_project_name'])),
               ('Number of samples delivered', nb_samples),
               ('Date samples received', 'Detailed in appendix I'),
               ('Total download size', '1.22 terabytes'),
               ('Laboratory protocol', 'Illumina TruSeq Nano library'),
               ('Submitted species', 'Thingius thingy'),
               ('Genome version', 'GRCh38 (with alt, decoy and HLA sequences)'))

        with get_patch_sample_restapi('a_project_name'):
            self.pr.store_sample_info()
            print(self.pr.get_project_info())
            assert self.pr.get_project_info() == exp
        mocked_folder_size.assert_called_with('tests/assets/project_report/dest/a_project_name')

    def test_samples_for_project(self):
        assert self.pr.samples_for_project_lims == self.fake_samples

    def test_get_sample(self):
        assert self.pr.get_lims_sample('sample_1') == self.fake_samples[0]

    def test_get_library_workflow(self):
        assert self.pr.get_library_workflow_from_sample('sample_1') == 'Illumina TruSeq Nano library'

    def test_get_report_type(self):
        assert self.pr.get_species_from_sample('sample_1') == 'Thingius thingy'
        self.pr.project_name = 'hmix999'
        del self.pr.__dict__['samples_for_project_lims']
        assert self.pr.get_species_from_sample('HS_mix_1') == 'Human'

    def test_update_program_from_csv(self):
        assert len(self.pr.params) == 3
        program_csv = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'source',
            'hmix999',
            'HS_mix_1',
            'programs.txt'
        )
        self.pr.update_from_program_csv(program_csv)
        exp = {
            'bcbio_version': '1.1',
            'bwa_version': '1.2',
            'gatk_version': '1.3',
            'samblaster_version': '1.4',
        }
        assert all(self.pr.params[k] == v for k, v in exp.items())

    def test_update_from_project_summary(self):
        assert 'bcbio_version' not in self.pr.params
        assert 'genome_version' not in self.pr.params
        summary_yaml = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'source',
            'hmix999',
            'HS_mix_1',
            'project-summary.yaml'
        )
        assert self.pr.get_from_project_summary_yaml(summary_yaml) == ('bcbio-0.9.4', 'hg38')

    @mocked_get_folder_size
    def test_get_sample_info(self, mocked_project_size):
        with get_patch_sample_restapi('a_project_name'):
            self.pr.store_sample_info()

        assert self.pr.params == {
            'bcl2fastq_version': 2.1,
            'adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA',
            'adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
            'bwa_version': 1.2,
            'biobambam_sortmapdup_version': 2,
            'bcl2fastq_version': 'v2.17.1.14',
            'biobambam_or_samblaster': 'biobambam',
            'biobambam_or_samblaster_version': 2,
            'project_name': 'a_project_name',
            'gatk_version': 'v1.3',
            'samtools_version': 0.3,
            'genome_version': 'GRCh38 (with alt, decoy and HLA sequences)',
            'species_submitted': 'Thingius thingy'
        }

    @patch(ppath('path.getsize'), return_value=1)
    def test_get_folder_size(self, mocked_getsize):
        d = os.path.join(self.source_dir, 'hmix999')
        assert self.pr.get_folder_size(d) == 126
        assert mocked_getsize.call_count == 126

        d = os.path.join(TestProjectManagement.root_path, 'etc')
        assert self.pr.get_folder_size(d) == 12
        assert mocked_getsize.call_count == 138

    def test_abbreviate_species(self):
        assert self.pr.abbreviate_species('Homo sapiens') == 'Hs'
        assert self.pr.abbreviate_species('Gallus gallus') == 'Gg'
        assert self.pr.abbreviate_species('Hyper snake') == 'Hsn'  # Avoid confusion with previous abbrev

        # Further call uses the cash
        assert self.pr.abbreviate_species('Homo sapiens') == 'Hs'

        # After cache is reset
        self.pr.species_abbreviation = {}
        # No confusion possible anymore
        assert self.pr.abbreviate_species('Hyper snake') == 'Hs'

        assert self.pr.abbreviate_species(None) is None


mocked_sample_status_latex = patch('project_report.project_information.ProjectReportInformation.sample_status',
                             return_value={'started_date': '2017-08-02T11:25:14.659000'})


def get_patch_sample_restapi_latex(project_name):
    path = 'project_report.project_information.ProjectReportInformation.samples_for_project_restapi'
    return patch(path, new_callable=PropertyMock(return_value=fake_rest_api_samples[project_name]))


class TestProjectReportLatex(TestProjectReport):
    def setUp(self):
        self.fake_samples = fake_samples['a_project_name']
        self.source_dir = os.path.join(self.assets_path, 'project_report', 'source')
        self.working_dir = os.path.join(self.assets_path, 'project_report', 'work')
        os.makedirs(self.working_dir, exist_ok=True)
        self.dest_dir = os.path.join(self.assets_path, 'project_report', 'dest')

        # Clean up previous reports
        project_reports = glob.glob(os.path.join(self.assets_path, 'project_report', 'dest', '*', '*.pdf'))
        project_report_texs = glob.glob(os.path.join(self.assets_path, 'project_report', 'dest', '*', '*.tex'))
        for f in project_reports + project_report_texs:
            os.remove(f)
        super().setUp()

    @mocked_sample_status_latex
    def test_project_types(self, mocked_sample_status):
        projects = ('hmix999', 'nhtn999', 'hpf999', 'nhpf999', 'uhtn999', 'upl999')

        for p in projects:
            with get_patch_sample_restapi_latex(p):
                report = ProjectReportLatex(p, self.working_dir)
                report.project_information.lims = FakeLims()
                tex_file = report.generate_tex()
                assert os.path.isfile(tex_file)
                # Uncomment to generate the pdf files (it requires latex to be installed locally)
                # pdf_file = report.generate_pdf()
                # assert os.path.isfile(pdf_file)
