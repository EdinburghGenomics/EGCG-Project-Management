import hashlib
import os
from unittest.mock import patch, Mock
import shutil
import datetime

from egcg_core.exceptions import EGCGError
from egcg_core.config import cfg

from tests import TestProjectManagement
from bin.deliver_reviewed_data import DataDelivery

sample1 = {
            'sample_id': 'deliverable_sample',
            'project_id': 'test_project',
            'user_sample_id': 'user_s_id',
            'useable': 'yes',
            'analysis_driver_procs': [
                {
                    'proc_id': 'most_recent_proc',
                    '_created': '06_03_2016_12:00:00',
                    'status': 'finished'
                },
                {
                    'proc_id': 'old_recent_proc',
                    '_created': '01_02_2016_12:00:00',
                    'status': 'aborted'
                }
            ],
            'bam_file_reads': 1,
            'run_elements': [
                {
                    'run_element_id': 'run_el_id1',
                    'clean_reads': 15,
                    'run_id': 'run1',
                    'project_id': 'test_project',
                    'sample_id': 'deliverable_sample',
                    'lane': 1,
                    'useable': 'yes'
                }
            ]
        }
sample2 = {
            'sample_id': 'deliverable_sample2',
            'project_id': 'test_project',
            'user_sample_id': 'user_s_id2',
            'useable': 'yes',
            'analysis_driver_procs': [
                {
                    'proc_id': 'most_recent_proc',
                    '_created': '06_03_2016_12:00:00',
                    'status': 'finished'
                },
                {
                    'proc_id': 'old_recent_proc',
                    '_created': '01_02_2016_12:00:00',
                    'status': 'aborted'
                }
            ],
            'bam_file_reads': 1,
            'run_elements': [
                {
                    'run_element_id': 'run_el_id2',
                    'clean_reads': 15,
                    'run_id': 'run1',
                    'project_id': 'test_project',
                    'sample_id': 'deliverable_sample2',
                    'lane': 2,
                    'useable': 'yes'
                },
                {
                    'run_element_id': 'run_el_id3',
                    'clean_reads': 15,
                    'run_id': 'run1',
                    'project_id': 'test_project',
                    'sample_id': 'deliverable_sample2',
                    'lane': 3,
                    'useable': 'yes'
                }
            ]
        }

def ppath(*parts):
    return 'egcg_core.' + '.'.join(parts)

patched_deliverable_project1 = patch(
    ppath('rest_communication.get_documents'),
    return_value=[
        sample1
    ]
)
patched_deliverable_project2 = patch(
    ppath('rest_communication.get_documents'),
    return_value=[
        sample2
    ]
)
patched_error_project = patch(
    ppath('rest_communication.get_documents'),
    return_value=[
        {
            'sample_id': 'deliverable_sample',
            'project_id': 'test_project',
            'useable': 'yes',
            'analysis_driver_procs': [
                {
                    'proc_id': 'most_recent_proc',
                    '_created': '06_03_2016_12:00:00',
                    'status': 'aborted'
                },
                {
                    'proc_id': 'old_recent_proc',
                    '_created': '01_02_2016_12:00:00',
                    'status': 'finished'
                }
            ]
        }
    ]
)

patched_get_species = patch(
    ppath('clarity.get_species_from_sample'),
    return_value = 'Homo sapiens'
)


class TestDataDelivery(TestProjectManagement):

    def __init__(self, *args, **kwargs):
        super(TestDataDelivery, self).__init__(*args, **kwargs)
        cfg.load_config_file(os.path.join(os.path.dirname(self.root_path), 'etc', 'example_data_delivery.yaml'))
        os.chdir(os.path.dirname(self.root_path))
        self.assets_delivery = os.path.join(self.assets_path, 'data_delivery')
        analysis_files = [
            '{ext_sample_id}.bam', '{ext_sample_id}.bam.bai', '{ext_sample_id}.bam.bai.md5', '{ext_sample_id}.bam.md5',
            '{ext_sample_id}.g.vcf.gz', '{ext_sample_id}.g.vcf.gz.md5', '{ext_sample_id}.g.vcf.gz.tbi',
            '{ext_sample_id}.g.vcf.gz.tbi.md5', '{ext_sample_id}.vcf.gz', '{ext_sample_id}.vcf.gz.md5',
            '{ext_sample_id}.vcf.gz.tbi', '{ext_sample_id}.vcf.gz.tbi.md5'
        ]
        raw_data_files = [
            '{ext_sample_id}_R1.fastq.gz', '{ext_sample_id}_R1.fastq.gz.md5', '{ext_sample_id}_R1_fastqc.html',
            '{ext_sample_id}_R1_fastqc.zip', '{ext_sample_id}_R2.fastq.gz', '{ext_sample_id}_R2.fastq.gz.md5',
            '{ext_sample_id}_R2_fastqc.html', '{ext_sample_id}_R2_fastqc.zip'
        ]
        self.final_files_merged = self._format_list( analysis_files + raw_data_files,  ext_sample_id='user_s_id')
        self.final_files_merged2 = self._format_list( analysis_files,  ext_sample_id='user_s_id2')
        self.final_files_split = self._format_list( analysis_files + ['raw_data'], ext_sample_id='user_s_id')

        self.dest_dir = cfg.query('delivery_dest')


    def _format_list(self, list_, **kwargs):
        return [v.format(**kwargs) for v in list_]


    def setUp(self):
        os.makedirs(self.dest_dir, exist_ok=True)
        self.delivery_dry = DataDelivery(dry_run=True, work_dir=os.path.join(self.assets_delivery, 'staging'), no_cleanup=True)
        self.delivery_real = DataDelivery(dry_run=False, work_dir=os.path.join(self.assets_delivery, 'staging'))
        self._create_run_elements(sample1.get('run_elements') + sample2.get('run_elements'))

    def tearDown(self):
        if os.path.exists(self.delivery_dry.staging_dir):
            shutil.rmtree(self.delivery_dry.staging_dir)
        for d in os.listdir(self.dest_dir):
            shutil.rmtree(os.path.join(self.dest_dir,d))
        self._remove_run_elements(sample1.get('run_elements') + sample2.get('run_elements'))
        pass

    def _touch(self, f):
        return open(f, 'w').close()

    def _md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        with open(fname + '.md5', "w") as f:
            f.write(hash_md5.hexdigest() + "  " + fname)

    def _remove_run_elements(self, list_run_elements):
        sample_dirs = set()
        for run_element in list_run_elements:
            sample_dirs.add(os.path.join(self.assets_delivery,'runs', run_element.get('run_id')))
        for s in sample_dirs:
            shutil.rmtree(s)

    def _create_run_elements(self, list_run_elements):
        for run_element in list_run_elements:
            sample_dir = os.path.join(self.assets_delivery,
                                      'runs',
                                      run_element.get('run_id'),
                                      run_element.get('project_id'),
                                      run_element.get('sample_id'))
            os.makedirs(sample_dir, exist_ok=True)
            for t in [
                'S1_L00%s_R1.fastq.gz', 'S1_L00%s_R2.fastq.gz',
                'S1_L00%s_R1_fastqc.html', 'S1_L00%s_R2_fastqc.html',
                'S1_L00%s_R1_fastqc.zip', 'S1_L00%s_R2_fastqc.zip'
            ]:
                self._touch(os.path.join(sample_dir, t%run_element.get('lane')))
                self._md5(os.path.join(sample_dir, t%run_element.get('lane')))

    def create_analysed_sample_file(self):
        pass


    def test_get_deliverable_projects_samples(self):
        with patched_deliverable_project1 as mocked_get_doc:
            project_to_samples = self.delivery_dry.get_deliverable_projects_samples(project_id='test_project')
            assert list(project_to_samples) == ['test_project']
            assert list([sample.get('sample_id') for samples in project_to_samples.values() for sample in samples]) == ['deliverable_sample']

        with patched_error_project as mocked_get_doc:
            self.assertRaises(EGCGError, self.delivery_dry.get_deliverable_projects_samples)

    def test_summarise_metrics_per_sample(self):
        with patched_deliverable_project1 as mocked_get_doc:
            self.delivery_dry.get_deliverable_projects_samples(project_id='test_project')
            expected_header = ['Project', 'Sample Id', 'User sample id', 'Read pair sequenced',
                               'Yield', 'Yield Q30', 'Nb reads in bam', 'mapping rate', 'properly mapped reads rate',
                               'duplicate rate', 'Mean coverage', 'Delivery folder']
            expected_lines = [
                'test_project\tdeliverable_sample\tuser_s_id\t15\t0.0\t0.0\t1\t0.0\t0.0\t0.0\t0\tdate_delivery'
            ]
            with patch(ppath('clarity.get_species_from_sample'), return_value='Homo sapiens'):
                header, lines = self.delivery_dry.summarise_metrics_per_sample(
                    project_id='test_project',
                    delivery_folder='date_delivery'
                )
                assert header == expected_header
                assert lines == expected_lines

    #def test_generate_md5_summary(self):
    #    self.delivery_dry.generate_md5_summary()


    def test_deliver_data_merged(self):
        with patched_deliverable_project1 as mocked_get_doc , \
                patched_get_species as mocked_get_species,\
                patch(ppath('clarity','get_sample'), return_value=Mock(udf={'Delivery':'merged'})):
            self.delivery_dry.deliver_data(project_id='test_project')
            assert os.listdir(self.delivery_dry.staging_dir) == ['deliverable_sample']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry.staging_dir, 'deliverable_sample')))
            assert sorted(list_files) == sorted(self.final_files_merged)

    def test_deliver_data_merged_concat(self):
        with patched_deliverable_project2 as mocked_get_doc , \
                patched_get_species as mocked_get_species,\
                patch(ppath('clarity','get_sample'), return_value=Mock(udf={'Delivery':'merged'})):
            self.delivery_dry.deliver_data(project_id='test_project')
            assert os.listdir(self.delivery_dry.staging_dir) == ['deliverable_sample2']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry.staging_dir, 'deliverable_sample2')))
            assert sorted(list_files) == sorted(self.final_files_merged2)
            assert len(self.delivery_dry.all_commands_for_cluster) == 2

    def test_deliver_data_split(self):
        with patched_deliverable_project1, patched_get_species,\
                patch(ppath('clarity','get_sample'), return_value=Mock(udf={'Delivery':'split'})):
            self.delivery_dry.deliver_data(project_id='test_project')
            assert os.listdir(self.delivery_dry.staging_dir) == ['deliverable_sample']
            list_files = sorted(os.listdir(os.path.join(self.delivery_dry.staging_dir, 'deliverable_sample')))
            assert sorted(list_files) == sorted(self.final_files_split)

    def test_deliver_data_merged_real(self):
        with patched_deliverable_project1 as mocked_get_doc , \
                patched_get_species as mocked_get_species,\
                patch(ppath('clarity','get_sample'), return_value=Mock(udf={'Delivery':'merged'})),\
                patch.object(DataDelivery, 'run_aggregate_commands', side_effect=print_args),\
                patch('bin.deliver_reviewed_data.DataDelivery.mark_samples_as_released') :
            self.delivery_real.deliver_data(project_id='test_project')
            assert os.listdir(self.dest_dir) == ['test_project']
            today = datetime.date.today().isoformat()
            assert sorted(os.listdir(os.path.join(self.dest_dir, 'test_project'))) == sorted([today, 'all_md5sums.txt', 'summary_metrics.csv'])
            assert os.listdir(os.path.join(self.dest_dir, 'test_project', today)) == ['deliverable_sample']
            list_files = sorted(os.listdir(os.path.join(self.dest_dir, 'test_project', today, 'deliverable_sample')))
            assert sorted(list_files) == sorted(self.final_files_merged)

    def test_deliver_data_split_real(self):
        with patched_deliverable_project1 as mocked_get_doc , \
                patched_get_species as mocked_get_species,\
                patch(ppath('clarity','get_sample'), return_value=Mock(udf={'Delivery':'split'})),\
                patch.object(DataDelivery, 'run_aggregate_commands', side_effect=print_args),\
                patch('bin.deliver_reviewed_data.DataDelivery.mark_samples_as_released') :
            self.delivery_real.deliver_data(project_id='test_project')
            assert os.listdir(self.dest_dir) == ['test_project']
            today = datetime.date.today().isoformat()
            assert sorted(os.listdir(os.path.join(self.dest_dir, 'test_project'))) == sorted([today, 'all_md5sums.txt', 'summary_metrics.csv'])
            assert os.listdir(os.path.join(self.dest_dir, 'test_project', today)) == ['deliverable_sample']
            list_files = sorted(os.listdir(os.path.join(self.dest_dir, 'test_project', today, 'deliverable_sample')))
            assert sorted(list_files) == sorted(self.final_files_split)



def print_args(*args, **kwargs):
    pass