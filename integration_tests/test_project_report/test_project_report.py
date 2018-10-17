import os
import re
from hashlib import md5
from unittest.mock import Mock, patch
from integration_tests import IntegrationTest, NamedMock
from egcg_core import rest_communication
from egcg_core.config import cfg
from project_report import client
work_dir = os.path.dirname(__file__)


class FakeLims:
    sample_udf_template = {'Total DNA (ng)': 3000, 'Required Yield (Gb)': 120, 'Coverage (X)': 30}

    def __init__(self, data):
        self.data = data
        self._samples_per_project = {}

    def get_samples(self, projectname):
        project = self.data[projectname]
        if projectname not in self._samples_per_project:
            self._samples_per_project[projectname] = [
                NamedMock(
                    s['rest_data']['sample_id'],
                    udf=dict(self.sample_udf_template, **project['sample_udfs'])
                ) for s in project['samples']
            ]
        return self._samples_per_project[projectname]

    @staticmethod
    def get_projects(name):
        return [
            Mock(
                udf={
                    'Project Title': 'a_research_title_for_' + name,
                    'Enquiry Number': '1337',
                    'Quote No.': '1338',
                    'Number of Quoted Samples': 8,
                    'Shipment Address Line 1': 'Institute of Awesomeness',
                    'Shipment Address Line 2': '213 high street',
                    'Shipment Address Line 3': '-',
                    'Shipment Address Line 4': '-',
                    'Shipment Address Line 5': '-'
                },
                researcher=NamedMock(name='First Last', first_name='First', last_name='Last',
                                     email='first.last@email.com', lab=NamedMock(name='Awesome lab'))
            )
        ]

    def get_processes(self, process_type, projectname):
        if process_type == 'Data Release Trigger EG 1.0 ST':
            nb_process = 2
            samples = self.get_samples(projectname)
            sample_sets = [samples[i::nb_process] for i in range(nb_process)]
            return [
                Mock(
                    all_inputs=Mock(return_value=[Mock(samples=[sample]) for sample in sample_sets[i] if sample]),
                    date_run='2018-01-10',
                    id=i + 1,
                    udf={
                        'Is this the final data release for the project?': 'No',
                        'Non-Conformances': ''
                    }
                ) for i in range(nb_process)
            ]


class TestProjectReport(IntegrationTest):
    delivery_source = os.path.join(work_dir, 'delivery_source')
    delivery_dest = os.path.join(work_dir, 'delivery_dest')
    patches = (
        patch('project_report.client.load_config'),
        patch('project_report.ProjectReport.sample_status', return_value={'started_date': '2018-02-08T12:26:01.893000'})
    )

    run_element_template = {'run_id': 'a_run', 'barcode': 'ATGC', 'library_id': 'a_library', 'useable': 'yes'}
    sample_template = {
        'coverage': {'mean': 37, 'evenness': 15, 'bases_at_coverage': {'bases_at_15X': 300}},
        'required_yield_q30': 1.0,
        'sample_contamination': {'ti_tv_ratio': 1.95, 'freemix': 0.0, 'het_hom_ratio': 0.07},
        'delivered': 'yes'
    }

    projects = {
        'htn999': {
            'samples': [],
            'sample_udfs': {
                'Prep Workflow': 'TruSeq Nano DNA Sample Prep',
                'Analysis Type': None,
                'Species': 'Homo sapiens',
                'Genome Version': 'hg38'
            }
        },
        'nhtn999': {
            'samples': [],
            'sample_udfs': {
                'Prep Workflow': 'TruSeq Nano DNA Sample Prep',
                'Analysis Type': 'Variant Calling gatk',
                'Species': 'Thingius thingy',
                'Genome Version': 'Tthi1'
            }
        },
        'hpf999': {
            'samples': [],
            'sample_udfs': {
                'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep',
                'Analysis Type': None,
                'Species': 'Homo sapiens',
                'Genome Version': 'hg38'
            }
        },
        'nhpf999': {
            'samples': [],
            'sample_udfs': {
                'Prep Workflow': 'TruSeq PCR-Free DNA Sample Prep',
                'Analysis Type': None,
                'Species': 'Thingius thingy',
                'Genome Version': 'Tthi1'
            }
        }
    }

    @classmethod
    def setUpClass(cls):
        cfg.content = {
            'delivery': {
                'source': cls.delivery_source,
                'dest': cls.delivery_dest,
                'signature_name': 'He-Man',
                'signature_role': 'Prince'
            }
        }

        i = 0
        for project_id in sorted(cls.projects):
            i += 100
            sample_ids = []
            for s in range(1, 9):
                i += 1

                sample_id = '%s_sample_%s' % (project_id, s)
                sample_ids.append(sample_id)
                run_element_id = 'a_run_%s_%s-barcode' % (s, project_id)

                run_element = {
                    'run_element_id': run_element_id,
                    'lane': s,
                    'sample_id': sample_id,
                    'project_id': project_id,
                    'passing_filter_reads': 84900000 + i,
                    'total_reads': 85000000 + i,
                    'clean_bases_r1': 25000000000 + i,
                    'clean_bases_r2': 25000010000 + i,
                    'clean_q30_bases_r1': 25000000000 + i,
                    'clean_q30_bases_r2': 25000010000 + i
                }
                run_element.update(cls.run_element_template)

                sample = {
                    'project_id': project_id,
                    'sample_id': sample_id,
                    'run_elements': [run_element_id],
                    'mapped_reads': 837830805 + i,
                    'properly_mapped_reads': 799000000 + i,
                    'bam_file_reads': 800000000 + i,
                    'duplicate_reads': 784000000 + i
                }
                sample.update(cls.sample_template)

                data = {'rest_data': sample, 'run_element': run_element}
                cls.projects[project_id]['samples'].append(data)

    def setUp(self):
        super().setUp()

        # can't have this in cls.patches because we need to construct the fake lims first
        self.patched_lims = patch('project_report.connection', return_value=FakeLims(self.projects))
        self.patched_lims.start()

        for project_id, data in self.projects.items():
            for s in data['samples']:
                rest_communication.post_entry('run_elements', s['run_element'])
                rest_communication.post_entry('samples', s['rest_data'])

                # TODO: refactor duplicate code from unit tests
                sample_dir = os.path.join(self.delivery_source, project_id, s['rest_data']['sample_id'])
                os.makedirs(sample_dir, exist_ok=True)
                if data['sample_udfs']['Species'] == 'Homo sapiens':
                    with open(os.path.join(sample_dir, 'programs.txt'), 'w') as open_file:
                        open_file.write('bcbio,1.1\nbwa,1.2\ngatk,1.3\nsamblaster,1.4\n')
                    with open(os.path.join(sample_dir, 'project-summary.yaml'), 'w') as open_file:
                        open_file.write(
                            'samples:\n- dirs:\n    galaxy: path/to/bcbio/bcbio-0.9.4/galaxy\n  genome_build: hg38\n')
                else:
                    with open(os.path.join(sample_dir, 'program_versions.yaml'), 'w') as open_file:
                        open_file.write('biobambam_sortmapdup: 2\nbwa: 1.2\ngatk: v1.3\nbcl2fastq: 2.1\nsamtools: 0.3')

            rest_communication.post_entry(
                'projects',
                {'project_id': project_id, 'samples': [s['rest_data']['sample_id'] for s in data['samples']]}
            )

            os.makedirs(os.path.join(self.delivery_dest, project_id), exist_ok=True)

    def tearDown(self):
        super().tearDown()
        self.patched_lims.stop()

    @staticmethod
    def _check_md5(html_report):
        m = md5()
        empty_line = re.compile(r' +\n')
        with open(html_report, 'r') as f:
            for line in f:
                if not empty_line.match(line) and 'file://' not in line:
                    m.update(line.encode())

        return m.hexdigest()

    def test_reports(self):
        test_success = True
        exp_md5s = {
            'htn999': 'dbf41471fa5fe2367daab6eb5876e151',
            'nhtn999': 'b05e63e1e9929bfc4d7412cb6d73d99f',
            'hpf999': 'c9cd8f802601e69730c7c38b9e5bc013',
            'nhpf999': 'fa74f5eb9d1e8eba0b16ac23d38a5ab9'
        }
        for k, v in exp_md5s.items():
            client.main(['-p', k, '-o', 'html', '-w', work_dir])
            obs_md5 = self._check_md5(os.path.join(self.delivery_dest, k, 'project_%s_report.html' % k))
            if obs_md5 != v:
                print('md5 mismatch for %s: expected %s, got %s' % (k, v, obs_md5))
                test_success = False

        assert test_success
