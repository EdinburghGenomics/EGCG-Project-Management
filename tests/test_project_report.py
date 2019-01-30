import os
import glob
import pprint
import shutil
import datetime
import collections
from itertools import cycle
from random import randint, random
from unittest.mock import Mock, PropertyMock, patch

from egcg_core.util import query_dict

from project_report import utils
from project_report.project_information import ProjectReportInformation
from project_report.project_report_latex import ProjectReportLatex
from project_report.utils import get_folder_size
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
    'mix999': {
        'name': 'S_mix_',
        'udf': {
            'Prep Workflow': cycle(['TruSeq Nano DNA Sample Prep', 'TruSeq PCR-Free DNA Sample Prep']),
            'Species': cycle(['Homo sapiens', 'Canis lupus familiaris']),
            'User Prepared Library': cycle([None, None, 'Yes']),
            'Genome Version': cycle(['hg38', 'canfam3.1']),
            'Total DNA (ng)': 3000,
            'Required Yield (Gb)': cycle([120, 60]),
            'Coverage (X)': cycle([30, 15]),
            'Analysis Type': 'Variant Calling gatk'
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


def _resolve_next(o):
    if not isinstance(o, str) and isinstance(o, collections.Iterable):
        return next(o)
    return o


fake_lims_samples = {}
fake_rest_samples = {}
fake_rest_sample_status = {}
fake_rest_sample_info = {}

for project in fake_sample_templates:
    fake_lims_samples[project] = []
    fake_rest_samples[project] = []
    fake_rest_sample_status[project] = []
    fake_rest_sample_info[project] = []
    for i in range(1, nb_samples + 1):
        template = fake_sample_templates[project]
        sample_id = template['name'] + str(i)
        udf = {k: _resolve_next(template['udf'][k]) for k in template['udf']}
        # lims sample
        fake_lims_samples[project].append(FakeSample(
            name=sample_id,
            udf=udf
        ))
        # rest sample info
        sample_info = dict((str(k), str(v)) for k, v in udf.items())
        sample_info['sample_id'] = sample_id
        fake_rest_sample_info[project].append(sample_info)

        # rest sample data
        if udf.get('Prep Workflow') is not None:
            req_yield = udf.get('Required Yield (Gb)')
            req_cov = udf.get('Coverage (X)')
            clean_yield_in_gb = randint(int(req_yield * .9), int(req_yield * 1.5))
            if sample_info['Species'] == 'Homo sapiens':
                pipeline = 'bcbio'
            elif udf.get('Analysis Type') == 'Variant Calling gatk':
                pipeline = 'variant_calling'
            else:
                pipeline = 'qc'
            nb_char = randint(0, 25)
            rest_sample_data = {
                'sample_id': sample_id,
                # Add variable padding to see the effect of long user sample names
                'user_sample_id': str(nb_char) + '_' * nb_char + '_user_' + sample_id,
                'project_id': project,
                'species_name': udf.get('Species'),
                'aggregated': {
                    'clean_yield_in_gb': clean_yield_in_gb,
                    'clean_pc_q30': clean_yield_in_gb * .8,
                    'pc_duplicate_reads': round(random() * 25, 1),
                    'pc_mapped_reads': round(95 + random() * 5, 1),
                    'run_ids': ['date_machine_number_flowcell1', 'date_machine_number_flowcell1'],
                    'most_recent_proc': {'pipeline_used': {'name': pipeline}}
                },
                'coverage': {'mean': randint(int(req_cov * .9), int(req_cov * 1.5))}
            }
            fake_rest_samples[project].append(rest_sample_data)

        # rest sample status
        rest_sample_status = {
            'sample_id': sample_id,
            'started_date': '2017-08-02T11:25:14.659000'
        }
        fake_rest_sample_status[project].append(rest_sample_status)

d = datetime.datetime.strptime('2018-01-10', '%Y-%m-%d').date()

fake_process_templates = {
    'a_project_name': {'nb_processes': 1, 'date': d, 'finished': 'Yes', 'NC': 'NC12: Description of major issue'},
    'mix999': {'nb_processes': 3, 'date': d, 'finished': 'Yes', 'NC': cycle(['NA', 'NA', 'NC25: Description minor issue', 'NA'])},
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
    sample_to_deliver = [s for s in fake_lims_samples.get(project) if s.udf.get('Prep Workflow') is not None]
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

    # @staticmethod
    # def get_samples(projectname):
    #     return fake_lims_samples[projectname]

    @staticmethod
    def get_processes(type, projectname):
        if type == 'Data Release Trigger EG 1.0 ST':
            return fake_processes[projectname]


mocked_get_folder_size = patch('project_report.project_information.get_folder_size', return_value=1337000000000)


def fake_get_documents(*args, **kwargs):
    if 'samples' in args:
        return fake_rest_samples.get(kwargs.get('where').get('project_id'))
    elif 'lims/status/sample_info' in args:
        return fake_rest_sample_info.get(kwargs.get('match').get('project_id'))
    elif 'lims/status/sample_status' in args:
        return fake_rest_sample_status.get(kwargs.get('match').get('project_id'))
    else:
        raise KeyError(str(args))


mocked_get_documents = patch('project_report.project_information.get_documents', side_effect=fake_get_documents)


class TestProjectReport(TestProjectManagement):
    def setUp(self):
        self.fake_samples = fake_lims_samples['a_project_name']
        self.source_dir = os.path.join(self.assets_path, 'project_report', 'source')
        self.working_dir = os.path.join(self.assets_path, 'project_report', 'work')
        os.makedirs(self.working_dir, exist_ok=True)
        self.dest_dir = os.path.join(self.assets_path, 'project_report', 'dest')

        self.pr = ProjectReportInformation('a_project_name')
        self.pr.lims = FakeLims()

        # create the source and dest folders
        for project in fake_lims_samples:
            prj_dir = os.path.join(self.source_dir, project)
            dest_dir = os.path.join(self.dest_dir, project)
            os.makedirs(dest_dir, exist_ok=True)
            os.makedirs(prj_dir, exist_ok=True)
            for sample in fake_lims_samples[project]:
                smp_dir = os.path.join(prj_dir, sample.name.replace(':', '_'))
                os.makedirs(smp_dir, exist_ok=True)
                if sample.udf['Species'] == 'Homo sapiens':
                    with open(os.path.join(smp_dir, 'programs.txt'), 'w') as open_file:
                        open_file.write('bcbio,1.1\nbwa,1.2\ngatk,1.3\nsamblaster,1.4\nsamtools,1.5\n')
                    with open(os.path.join(smp_dir, 'project-summary.yaml'), 'w') as open_file:
                        open_file.write(
                            'samples:\n- dirs:\n    galaxy: path/to/bcbio/bcbio-0.9.4/galaxy\n  genome_build: hg38\n')
                else:
                    with open(os.path.join(smp_dir, 'program_versions.yaml'), 'w') as open_file:
                        open_file.write('biobambam_sortmapdup: 2\nbwa: 1.2\ngatk: v1.3\nbcl2fastq: 2.1\nsamtools: 0.3')

        self.run_ids = set()
        for project in fake_rest_samples:
            for sample in fake_rest_samples[project]:
                for run_id in query_dict(sample, 'aggregated.run_ids'):
                    self.run_ids.add(run_id)
        for run_id in self.run_ids:
            run_dir = os.path.join(self.source_dir, run_id)
            os.makedirs(run_dir, exist_ok=True)
            with open(os.path.join(run_dir, 'program_versions.yaml'), 'w') as open_file:
                open_file.write('bcl2fastq: v2.17.1.14\n')

    def tearDown(self):
        # delete the source folders
        for project in fake_lims_samples:
            shutil.rmtree(os.path.join(self.source_dir, project))
        shutil.rmtree(self.working_dir)


class TestProjectReportInformation(TestProjectReport):

    def test_customer_name(self):
        assert self.pr.customer_name == 'Awesome lab'
        # Remove the lab name from the researcher
        self.pr.lims.fake_lims_researcher = NamedMock(name='Firstname Lastname', lab=NamedMock(name=''))
        # Remove the cached project
        del self.pr.__dict__['lims_project']
        assert self.pr.customer_name == 'Firstname Lastname'

    @mocked_get_documents
    def test_get_library_workflow(self, mock_get_docs):
        assert self.pr.get_library_workflow_from_sample('sample_1') == 'Illumina TruSeq Nano library'

    @mocked_get_documents
    def test_get_report_type(self, mock_get_docs):
        assert self.pr.get_species_from_sample('sample_1') == 'Thingius thingy'
        self.pr.project_name = 'hpf999'
        self.pr.__dict__['_sample_info'] = {}
        del self.pr.__dict__['sample_status_for_project']
        del self.pr.__dict__['sample_info_for_project']
        del self.pr.__dict__['sample_data_for_project']
        assert self.pr.get_species_from_sample('HS_pcrfree_1') == 'Homo sapiens'

    def test_read_program_csv(self):
        program_csv = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'source',
            'hpf999',
            'HS_pcrfree_1',
            'programs.txt'
        )

        exp = {
            'bcbio_version': '1.1',
            'bwa_version': '1.2',
            'gatk_version': '1.3',
            'samblaster_version': '1.4',
            'samtools_version': '1.5'
        }
        assert self.pr._read_program_csv(program_csv) == exp

    def test_update_from_project_summary(self):
        summary_yaml = os.path.join(
            TestProjectManagement.assets_path,
            'project_report',
            'source',
            'hpf999',
            'HS_pcrfree_1',
            'project-summary.yaml'
        )
        assert self.pr._read_project_summary_yaml(summary_yaml) == ('bcbio-0.9.4', 'hg38')

    @mocked_get_documents
    def test_get_bioinformatics_params_for_analysis(self, mock_get_docs):
        analysis_types = self.pr.get_project_analysis_types()
        assert len(analysis_types) == 1
        parameters = self.pr.get_bioinformatics_params_for_analysis(analysis_types[0])
        assert parameters == {
            'bcl2fastq_version': 'v2.17.1.14',
            'adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA',
            'adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT',
            'bwa_version': '1.2',
            'biobambam_sortmapdup_version': '2',
            'biobambam_or_samblaster': 'biobambam',
            'biobambam_or_samblaster_version': '2',
            'gatk_version': 'v1.3',
            'samtools_version': '0.3',
            'genome_version': 'hg38',
            'species_submitted': 'Thingius thingy'
        }

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


class TestProjectReportLatex(TestProjectReport):
    def setUp(self):
        self.source_dir = os.path.join(self.assets_path, 'project_report', 'source')
        self.working_dir = os.path.join(self.assets_path, 'project_report', 'work')
        os.makedirs(self.working_dir, exist_ok=True)
        self.dest_dir = os.path.join(self.assets_path, 'project_report', 'dest')

        self.report = ProjectReportLatex('a_project_name', self.working_dir)
        self.report.pi.lims = FakeLims()
        # Clean up previous reports
        project_reports = glob.glob(os.path.join(self.assets_path, 'project_report', 'dest', '*', '*.pdf'))
        project_report_texs = glob.glob(os.path.join(self.assets_path, 'project_report', 'dest', '*', '*.tex'))
        for f in project_reports + project_report_texs:
            os.remove(f)
        super().setUp()

    def test_limit_cell_width(self):
        rows = [
            ['short', 'short', 'short', 'short', 'short'],
            ['short', 'short', 'looooooooooooooooong', 'short', 'short'],
        ]
        assert self.report._limit_cell_width(rows, cell_widths={1: 15}) == rows
        new_rows = [
            ['short', 'short', 'short', 'short', 'short'],
            ['short', 'short', 'loooooooooooooo\nooong', 'short', 'short']
        ]
        assert self.report._limit_cell_width(rows, cell_widths={2: 15}) == new_rows

    @mocked_get_documents
    def test_project_types(self, mocked_get_docs):
        projects = ('mix999', 'nhtn999', 'hpf999', 'nhpf999', 'uhtn999', 'upl999')

        for p in projects:
            report = ProjectReportLatex(p, self.working_dir)
            report.pi.lims = FakeLims()
            tex_file = report.generate_tex()
            assert os.path.isfile(tex_file)
            # Uncomment to generate the pdf files (it requires latex to be installed locally)
            # pdf_file = report.generate_pdf()
            # assert os.path.isfile(pdf_file)

    @mocked_get_folder_size
    @mocked_get_documents
    def test_get_project_info(self, mocked_get_documents, mocked_folder_size):
        exp = (('Project name', 'a_project_name'),
               ('Project title', 'a_research_title_for_a_project_name'),
               ('Enquiry no', '1337'),
               ('Quote no', '1338'),
               ('Customer name', 'Awesome lab'),
               ('Customer address', 'Institute of Awesomeness\n213 high street'),
               ('Number of samples', len(fake_lims_samples['a_project_name'])),
               ('Number of samples delivered', nb_samples),
               ('Date samples received', 'Detailed in appendix I'),
               ('Total download size', '1.22 terabytes'),
               ('Laboratory protocol', 'Illumina TruSeq Nano library'),
               ('Submitted species', 'Thingius thingy'),
               ('Genome version', 'hg38'))
        assert self.report.get_project_info() == exp
        mocked_folder_size.assert_called_with('tests/assets/project_report/dest/a_project_name')


class TestProjectReportUtils(TestProjectManagement):

    @patch('os.path.getsize', return_value=1)
    def test_get_folder_size(self, mocked_getsize):
        d = os.path.join(TestProjectManagement.root_path, 'etc')
        assert utils.get_folder_size(d) == 12
        assert mocked_getsize.call_count == 12

    def test_parse_date(self):
        assert utils.parse_date('2017-08-02T11:25:14.659000') == '02 Aug 17'

    def test_min_mean_max(self):
        values = [10, 15, 26, 32, 18, 31, 18, 19, 25, 36]
        assert utils.min_mean_max(values) == 'min: 10.0, avg: 23.0, max: 36.0'
