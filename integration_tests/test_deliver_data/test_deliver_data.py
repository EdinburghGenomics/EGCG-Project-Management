import os
import gzip
import hashlib
from shutil import rmtree
from unittest.mock import Mock, patch, PropertyMock
from bin.deliver_reviewed_data import release_trigger_lims_step_name
from integration_tests import IntegrationTest, integration_cfg, NamedMock
from egcg_core import rest_communication, util
from egcg_core.config import cfg
from bin import deliver_reviewed_data

work_dir = os.path.dirname(__file__)

fake_samples = {
    'split_human_sample': {
        'authorised': True,
        'lims/samples': {
            'Delivery': 'split',
            'Total DNA(ng)': 2000
        },
        'lims/status/sample_status': {
            'library_type': 'nano',
            'started_date': '2017-08-02T11:25:14.659000'
        },
        'output_files': deliver_reviewed_data.hs_files,
        'api': {
            'bam_file_reads': 1, 'useable': 'yes', 'species_name': 'Homo sapiens',
            'required_yield': 120000000000, 'required_coverage': 30
        },
        'run_elements': [
            {
                'run_id': 'a_run', 'lane': 1, 'barcode': 'ATGC', 'useable': 'yes', 'clean_reads': 1,
                'clean_bases_r1': 65500000000, 'clean_q30_bases_r1': 60500000000, 'clean_bases_r2': 65500000000,
                'clean_q30_bases_r2': 60000000000
            },
            {'lane': 1, 'barcode': 'CGTA', 'useable': 'no'}
        ]
    },
    'delivered_sample': {
        'lims/samples': {
            'Total DNA(ng)': 2000
        },
        'lims/status/sample_status': {
            'library_type': 'nano',
            'started_date': '2017-08-02T11:25:14.659000'
        },
        'output_files': deliver_reviewed_data.hs_files,
        'api': {
            'bam_file_reads': 1, 'useable': 'yes', 'delivered': 'yes', 'species_name': 'Homo sapiens',
            'required_yield': 120000000000, 'required_coverage': 30, 'coverage': {'mean': 31}
        },
        'run_elements': [
            {
                'run_id': 'a_run', 'lane': 2, 'barcode': 'ATGC', 'useable': 'yes', 'clean_reads': 1,
                'clean_bases_r1': 65500000000, 'clean_q30_bases_r1': 60500000000, 'clean_bases_r2': 65500000000,
                'clean_q30_bases_r2': 60000000000
            },
            {'lane': 2, 'barcode': 'CGTA', 'useable': 'no'}
        ]
    },
    'unusable_sample': {
        'lims/samples': {
            'Total DNA(ng)': 2000
        },
        'lims/status/sample_status': {
            'library_type': 'nano',
            'started_date': '2017-08-02T11:25:14.659000'
        },
        'output_files': deliver_reviewed_data.hs_files,
        'api': {
            'bam_file_reads': 1, 'useable': 'no', 'species_name': 'Homo sapiens',
            'required_yield': 120000000000, 'required_coverage': 30, 'coverage': {'mean': 31}
        },
        'run_elements': [
            {
                'run_id': 'a_run', 'lane': 3, 'barcode': 'ATGC', 'useable': 'yes', 'clean_reads': 1,
                'clean_bases_r1': 65500000000, 'clean_q30_bases_r1': 60500000000, 'clean_bases_r2': 65500000000,
                'clean_q30_bases_r2': 60000000000
            },
            {'lane': 3, 'barcode': 'CGTA', 'useable': 'no'}
        ]
    },
    'merged_non_human_sample': {
        'authorised': True,
        'lims/samples': {
            'Delivery': 'merged',
            'Total DNA(ng)': 2000
        },
        'lims/status/sample_status': {
            'library_type': 'nano',
            'started_date': '2017-08-02T11:25:14.659000'
        },
        'output_files': deliver_reviewed_data.other_files,
        'api': {
            'bam_file_reads': 1, 'useable': 'yes', 'species_name': 'Thingius thingy',
            'required_yield': 120000000000, 'required_coverage': 30, 'coverage': {'mean': 31}
        },
        'run_elements': [
            {
                'run_id': 'a_run', 'lane': 4, 'barcode': 'ATGC', 'useable': 'yes', 'clean_reads': 1,
                'clean_bases_r1': 65500000000, 'clean_q30_bases_r1': 60500000000, 'clean_bases_r2': 65500000000,
                'clean_q30_bases_r2': 60000000000
            },
            {'lane': 4, 'barcode': 'CGTA', 'useable': 'no'}
        ]
    },
    'fluidx_non_human_var_calling_sample': {
        'authorised': True,
        'lims/samples': {
            '2D Barcode': 'a_fluidx_barcode',
            'Delivery': 'split',
            'Total DNA(ng)': 2000,
            'Analysis Type': 'Variant Calling'
        },
        'lims/status/sample_status': {
            'library_type': 'nano',
            'started_date': '2017-08-02T11:25:14.659000'
        },
        'output_files': deliver_reviewed_data.variant_calling_files,
        'api': {
            'bam_file_reads': 1, 'useable': 'yes', 'species_name': 'Thingius thingy',
            'required_yield': 120000000000, 'required_coverage': 30, 'coverage': {'mean': 31}
        },
        'run_elements': [
            {
                'run_id': 'a_run', 'lane': 5, 'barcode': 'ATGC', 'useable': 'yes', 'clean_reads': 1,
                'clean_bases_r1': 65500000000, 'clean_q30_bases_r1': 60500000000, 'clean_bases_r2': 65500000000,
                'clean_q30_bases_r2': 60000000000
            },
            {'lane': 5, 'barcode': 'CGTA', 'useable': 'no'}
        ]
    }
}
# Store the get_document function so it is still accessible after it's been patched
get_doc = rest_communication.get_document


def fake_get_document(*args, **kwargs):
    if args[0] in ('samples', 'run_elements'):
        return get_doc(*args, **kwargs)
    else:
        # for lims enpoint still need to get mocked results
        return fake_samples.get(list(kwargs.values())[0].get('sample_id'), {}). get(args[0])


class TestDelivery(IntegrationTest):
    processed_run_dir = os.path.join(work_dir, 'processed_runs')
    processed_projects_dir = os.path.join(work_dir, 'processed_projects')
    delivered_projects_dir = os.path.join(work_dir, 'delivered_projects')
    artifacts = [Mock(samples=[NamedMock(name=sample)]) for sample in fake_samples if fake_samples[sample].get('authorised', False)]
    fake_process = Mock(
        type=NamedMock(name=release_trigger_lims_step_name),
        all_inputs=Mock(return_value=artifacts)
    )
    patches = (
        patch('bin.deliver_reviewed_data.rest_communication.get_document', side_effect=fake_get_document),
        patch('bin.deliver_reviewed_data.load_config'),
        patch('bin.deliver_reviewed_data.DataDelivery.process', new=PropertyMock(return_value=fake_process)),
        patch('bin.deliver_reviewed_data.clarity.get_queue_uri', return_value='a_queue_uri'),
        patch('bin.deliver_reviewed_data.clarity.route_samples_to_delivery_workflow'),
        patch('bin.deliver_reviewed_data.ProjectReport')  # TODO: run the project report once it can take mixed projects
    )

    @classmethod
    def setUpClass(cls):
        cfg.content = {
            'tools': {
                'md5sum': integration_cfg['md5sum'],
                'fastqc': integration_cfg['fastqc']
            },
            'sample': {
                'input_dir': cls.processed_run_dir
            },
            'delivery': {
                'dest': cls.delivered_projects_dir,
                'source': cls.processed_projects_dir,
                'clarity_workflow_name': 'a_workflow',
                'email_notification': {}  # TODO: avoid KeyError in delivery email
            }
        }

        for s in fake_samples:
            sample_dir = os.path.join(cls.processed_projects_dir, 'a_project', s)
            if not os.path.isdir(sample_dir):
                os.makedirs(sample_dir)
                for basename in fake_samples[s]['output_files']:
                    fp = os.path.join(sample_dir, basename.format(ext_sample_id='uid_' + s))
                    cls._seed_file(fp, md5=True)

            fastq_dir = os.path.join(cls.processed_run_dir, 'a_run', 'a_project', s)
            if not os.path.isdir(fastq_dir):
                os.makedirs(fastq_dir)
                lane = fake_samples[s]['run_elements'][0]['lane']
                for i in (1, 2):
                    file_base = 'uid_%s_S%s_L00%s_R%s_001' % (s, lane, lane, i)
                    cls._seed_file(os.path.join(fastq_dir, file_base + '.fastq.gz'), md5=True)
                    cls._seed_file(os.path.join(fastq_dir, file_base + '_fastqc.html'))
                    cls._seed_file(os.path.join(fastq_dir, file_base + '_fastqc.zip'))

    def setUp(self):
        super().setUp()

        os.makedirs(self.delivered_projects_dir, exist_ok=True)
        for d in os.listdir(self.delivered_projects_dir):
            rmtree(os.path.join(self.delivered_projects_dir, d))

        for s in fake_samples:
            rest_communication.post_entry(
                'analysis_driver_procs',
                {'proc_id': 'sample_%s_x' % s, 'dataset_type': 'sample', 'dataset_name': s, 'status': 'finished'}
            )
            for r in fake_samples[s]['run_elements']:
                run_element = dict(
                    r,
                    run_element_id='a_run_%s_%s' % (r['lane'], r['barcode']),
                    run_id='a_run',
                    library_id='a_library',
                    sample_id=s,
                    project_id='a_project'
                )
                rest_communication.post_entry('run_elements', run_element)

            sample = dict(
                fake_samples[s]['api'],
                sample_id=s,
                project_id='a_project',
                user_sample_id='uid_' + s,
                run_elements=['a_run_%s_%s' % (r['lane'], r['barcode']) for r in fake_samples[s]['run_elements']], 
                analysis_driver_procs=['sample_%s_x' % s]
            )
            rest_communication.post_entry('samples', sample)

    @staticmethod
    def _run_main(args=None):
        argv = ['--process_id', 'a_process', '--noemail', '--work_dir', work_dir]
        if args:
            argv += args

        deliver_reviewed_data.main(argv)

    @staticmethod
    def _seed_file(fp, md5=False):
        content = 'Some data for %s\n' % fp
        if fp.endswith('.gz'):
            with gzip.open(fp, 'w') as f:
                f.write(content.encode())
        else:
            with open(fp, 'w') as f:
                f.write(content)

        if md5:
            with open(fp + '.md5', 'w') as g:
                m = hashlib.md5()
                m.update(content.encode())
                g.write('%s  %s\n' % (m.hexdigest(), fp))

    def _check_files_pre_delivery(self):
        missing_files = []
        for s in fake_samples:
            user_sample_id = 'uid_' + s

            sample_dir = os.path.join(self.processed_projects_dir, 'a_project', s)
            for basename in fake_samples[s]['output_files']:
                f = os.path.join(sample_dir, basename.format(ext_sample_id=user_sample_id))
                if not os.path.isfile(f):
                    missing_files.append(f)
                
            for r in fake_samples[s]['run_elements']:
                fastq_dir = os.path.join(self.processed_run_dir, 'a_run', 'a_project', s)
                for i in (1, 2):
                    f = os.path.join(
                        fastq_dir,
                        '%s_S%s_L00%s_R%s_001.fastq.gz' % (user_sample_id, r['lane'], r['lane'], i)
                    )
                    if not os.path.isfile(f):
                        missing_files.append(f)

        assert not missing_files, 'Found missing files: %s' % missing_files

    @staticmethod
    def _check_api_pre_delivery():
        assert rest_communication.get_document('samples', where={'sample_id': 'delivered_sample'})['delivered'] == 'yes'
        for sample_id in ('split_human', 'unusable', 'merged_non_human', 'fluidx_non_human_var_calling'):
            assert rest_communication.get_document('samples', where={'sample_id': sample_id + '_sample'})['delivered'] == 'no'

    @staticmethod
    def _check_api_post_delivery():
        assert rest_communication.get_document('samples', where={'sample_id': 'unusable_sample'})['delivered'] == 'no'
        for sample_id in ('split_human', 'delivered', 'merged_non_human', 'fluidx_non_human_var_calling'):
            assert rest_communication.get_document('samples', where={'sample_id': sample_id + '_sample'})['delivered'] == 'yes'

    @staticmethod
    def _check_files(obs_dir, exp_basenames):
        obs = os.listdir(obs_dir)
        assert obs, 'No files found in %s' % obs_dir
        if sorted(obs) != sorted(exp_basenames):
            print('File check in %s failed' % obs_dir)
            print('Missing files: %s' % [f for f in exp_basenames if f not in obs])
            print('Unexpected_files: %s' % [f for f in obs if f not in exp_basenames])
            raise AssertionError

    def test_deliver(self):
        self._check_files_pre_delivery()
        self._check_api_pre_delivery()

        self._run_main()

        delivery_dir = util.find_file(self.delivered_projects_dir, 'a_project', '????-??-??')

        self._check_files(
            os.path.join(delivery_dir, 'a_fluidx_barcode'),
            ['raw_data'] + [
                'uid_fluidx_non_human_var_calling_sample.' + e
                for e in ('bam', 'bam.md5', 'bam.bai', 'bam.bai.md5', 'g.vcf.gz',
                          'g.vcf.gz.md5', 'g.vcf.gz.tbi', 'g.vcf.gz.tbi.md5')
            ]
        )
        self._check_files(
            os.path.join(delivery_dir, 'a_fluidx_barcode', 'raw_data'),
            [
                'a_run_5_ATGC_' + e
                for e in ('R1.fastq.gz', 'R1.fastq.gz.md5', 'R1_fastqc.html', 'R1_fastqc.zip',
                          'R2.fastq.gz', 'R2.fastq.gz.md5', 'R2_fastqc.html', 'R2_fastqc.zip')
            ]
        )

        self._check_files(
            os.path.join(delivery_dir, 'split_human_sample'),
            ['raw_data'] + [
                'uid_split_human_sample.' + e
                for e in ('bam', 'bam.md5', 'bam.bai', 'bam.bai.md5', 'g.vcf.gz', 'g.vcf.gz.md5', 'g.vcf.gz.tbi',
                          'g.vcf.gz.tbi.md5', 'vcf.gz', 'vcf.gz.md5', 'vcf.gz.tbi', 'vcf.gz.tbi.md5')
            ]
        )
        self._check_files(
            os.path.join(delivery_dir, 'split_human_sample', 'raw_data'),
            [
                'a_run_1_ATGC_' + e
                for e in ('R1.fastq.gz', 'R1.fastq.gz.md5', 'R1_fastqc.html', 'R1_fastqc.zip',
                          'R2.fastq.gz', 'R2.fastq.gz.md5', 'R2_fastqc.html', 'R2_fastqc.zip')
            ]
        )

        self._check_files(
            os.path.join(delivery_dir, 'merged_non_human_sample'),
            [
                'uid_merged_non_human_sample_' + e
                for e in ('R1.fastq.gz', 'R1.fastq.gz.md5', 'R1_fastqc.html', 'R1_fastqc.zip',
                          'R2.fastq.gz', 'R2.fastq.gz.md5', 'R2_fastqc.html', 'R2_fastqc.zip')
            ]
        )

        assert not util.find_file(delivery_dir, 'unusable_sample')
        assert not util.find_file(delivery_dir, 'delivered_sample')

        self._check_api_post_delivery()

    def test_dry_run(self):
        self._check_files_pre_delivery()
        self._check_api_pre_delivery()

        self._run_main(['--dry_run'])

        # nothing should have happened
        self._check_files_pre_delivery()
        self._check_api_pre_delivery()
