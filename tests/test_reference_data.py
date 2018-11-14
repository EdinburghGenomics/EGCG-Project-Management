from unittest.mock import Mock, patch, PropertyMock
from bin import reference_data
from bin.reference_data import Downloader
from tests import TestProjectManagement


class TestDownloader(TestProjectManagement):

    def setUp(self):
        with patch('bin.reference_data.ncbi.get_species_name', return_value='A species'):
            self.downloader = reference_data.Downloader('A species', 'a_genome')

        self.downloader.__dict__['_logger'] = Mock()

    @patch('builtins.input', return_value=None)
    @patch('os.path.isfile', side_effect=[False, True, False])
    @patch('subprocess.check_call')
    @patch('bin.reference_data.Downloader.run_background')
    @patch('egcg_core.util.find_file', return_value=None)
    @patch('egcg_core.util.find_files', return_value=[])
    def test_prepare_data(self, m_find_files, m_find_file, m_run, m_check_call, m_isfile, m_input):
        self.downloader.reference_fasta = 'a_genome.dna.toplevel.fa'
        self.downloader.reference_variation = 'a_species.vcf.gz'
        m_input.side_effect = [
            'a_species.vcf.gz'
        ]

        self.downloader.prepare_data()

        m_check_call.assert_any_call('gzip -dc a_species.vcf.gz > tmp.vcf', shell=True),
        m_check_call.assert_any_call('path/to/bgzip -c tmp.vcf > a_species.vcf.gz', shell=True)
        assert m_check_call.call_count == 2

        m_run.assert_any_call('path/to/samtools faidx a_genome.dna.toplevel.fa', 'faidx.log'),
        m_run.assert_any_call(
            'path/to/picard -Xmx20G CreateSequenceDictionary R=a_genome.dna.toplevel.fa O=a_genome.dna.toplevel.dict',
            'create_sequence_dict.log'),
        m_run.assert_any_call('path/to/bwa index a_genome.dna.toplevel.fa', 'bwa_index.log'),
        m_run.assert_any_call('path/to/tabix -p vcf a_species.vcf.gz', 'tabix.log'),
        assert m_run.call_count == 4

    @patch('bin.reference_data.Downloader.run_background')
    def test_validate_data(self, m_run):
        self.downloader.reference_fasta = 'a_genome.dna.toplevel.fa'
        self.downloader.reference_variation = 'a_species.vcf.gz'
        self.downloader.procs['faidx'] = Mock()
        self.downloader.procs['CreateSequenceDictionary'] = Mock()

        self.downloader.validate_data()

        m_run.assert_called_once_with(
            'java -Xmx20G -jar path/to/gatk -T ValidateVariants -V a_species.vcf.gz -R a_genome.dna.toplevel.fa -warnOnErrors',
            'a_species.vcf.gz.validate_variants.log')

        self.downloader.procs['faidx'].wait.assert_called_once_with()
        self.downloader.procs['CreateSequenceDictionary'].wait.assert_called_once_with()
        assert 'CreateSequenceDictionary' in self.downloader.procs
        assert self.downloader.procs['CreateSequenceDictionary'] is not None

    @patch('builtins.input', return_value=None)
    @patch('bin.reference_data.Downloader.check_stdout', return_value='tool v1.0')
    @patch('bin.reference_data.Downloader.check_stderr', return_value='tool v1.2\ntool v1.1')
    @patch.object(Downloader, 'data_source', new_callable=PropertyMock(return_value='a source'))
    def test_prepare_metadata(self, mock_source,  mocked_stderr, mocked_stdout, mock_input):
        mock_input.side_effect = [
            3,    # chromosome_count
            100,  # genome_size
            99    # goldenpath
        ]

        exp_tools_used = {
            'picard': 'tool v1.2\ntool v1.1',
            'tabix': 'v1.2',
            'bwa': 'v1.1',
            'bgzip': 'v1.1',
            'samtools': 'v1.0',
            'gatk': 'tool v1.0'
        }
        assert len(exp_tools_used) == len(self.downloader.tools)
        self.downloader.reference_fasta = 'path/to/fasta_file.fa'
        self.downloader.reference_variation = 'path/to/vcf_file.vcf.gz'
        self.downloader.prepare_metadata()
        assert self.downloader.payload == {
            'tools_used': exp_tools_used,
            'data_source': 'a source',
            'data_files': {
                'fasta': 'A_species/a_genome/fasta_file.fa',
                'variation': 'A_species/a_genome/vcf_file.vcf.gz'
            },
            'chromosome_count': 3,
            'genome_size': 100,
            'goldenpath': 99
        }

    @patch('requests.get', return_value=Mock(json=Mock(return_value=[{'id': 'a_taxid'}]), status_code=200))
    @patch('bin.reference_data.Downloader.now', return_value='now')
    def test_finish_metadata(self, mnow, mget):
        assert self.downloader.payload == {}

        with patch('builtins.input', side_effect=['project1,project2', 'Some comments']):
            self.downloader.finish_metadata()
        assert self.downloader.payload == {
            'assembly_name': 'a_genome',
            'species': 'A species',
            'date_added': 'now',
            'project_whitelist': ['project1', 'project2'],
            'comments': 'Some comments',
            'analyses_supported': ['qc']
        }

        # reset
        self.downloader.payload = {}
        self.downloader.reference_variation = 'a_vcf_file.vcf.gz'
        with patch('builtins.input', side_effect=['project1,project2', 'Some comments']):
            self.downloader.finish_metadata()
        assert self.downloader.payload == {
            'assembly_name': 'a_genome',
            'species': 'A species',
            'date_added': 'now',
            'project_whitelist': ['project1', 'project2'],
            'comments': 'Some comments',
            'analyses_supported': ['qc', 'variant_calling']
        }

        # reset
        self.downloader.payload = {}
        self.downloader.ftp_species = 'homo_sapiens'
        with patch('builtins.input', side_effect=['project1,project2', 'Some comments']):
            self.downloader.finish_metadata()
        assert self.downloader.payload == {
            'assembly_name': 'a_genome',
            'species': 'A species',
            'date_added': 'now',
            'project_whitelist': ['project1', 'project2'],
            'comments': 'Some comments',
            'analyses_supported': ['qc', 'variant_calling', 'bcbio']
        }

    @patch('bin.reference_data.ncbi._fetch_from_cache', return_value=('dummy', 'a_taxid', 'dummy', 'dummy'))
    @patch('egcg_core.rest_communication.get_document')
    @patch('egcg_core.rest_communication.post_entry')
    @patch('egcg_core.rest_communication.patch_entry')
    @patch('egcg_core.rest_communication.post_or_patch')
    def test_upload_to_rest_api(self, mpostpatch, mpatch, mpost, mgetdoc, mget):
        self.downloader.payload = {'stuff to upload': 'value', 'genome_size': '1300000'}

        with patch('builtins.input', return_value='y'):
            self.downloader.upload_to_rest_api()
        mpostpatch.assert_called_with(
            'genomes',
            [{'stuff to upload': 'value', 'genome_size': '1300000'}],
            id_field='assembly_name'
        )
        mpatch.assert_called_with(
            'species',
            {'genomes': ['a_genome'], 'default_version': 'a_genome'},
            'name',
            'A species',
            update_lists=True
        )
        # with patch('builtins.input', return_value='y'):
        #     self.downloader.upload_to_rest_api()

        mgetdoc.return_value = None
        with patch('builtins.input', side_effect=['', 'y']):
            self.downloader.upload_to_rest_api()
        mpost.assert_called_with(
            'species',
            {
                'name': 'A species', 'genomes': ['a_genome'],
                'default_version': 'a_genome', 'taxid': 'a_taxid',
                'approximate_genome_size': 1.3
             },
        )


class TestEnsemblDownloader(TestProjectManagement):
    def setUp(self):
        with patch('bin.reference_data.ncbi.get_species_name', return_value='A species'):
            self.downloader = reference_data.EnsemblDownloader('A species', 'a_genome')
        self.downloader.__dict__['ftp'] = Mock()
        self.downloader.__dict__['_logger'] = Mock()

    def test_all_ensembl_releases(self):
        self.downloader.ftp.nlst.return_value = ['README', 'release-1', 'release-10', 'release-2']
        assert self.downloader.all_ensembl_releases == ['release-10', 'release-2', 'release-1']

    def test_latest_genome_version(self):
        self.downloader.__dict__['all_ensembl_releases'] = ['a_release']
        self.downloader.ftp.nlst = Mock(
            return_value=[
                'A_species.a_genome.dna.nonchromosomal.fa.gz',
                'A_species.a_genome.dna.toplevel.fa.gz',
                'A_species.a_genome.dna_rm.chromosome.1.fa.gz'
            ]
        )
        assert self.downloader.latest_genome_version() == 'a_genome'
        self.downloader.ftp.nlst.assert_called_with('a_release/fasta/a_species/dna')

    def test_ensembl_base_url(self):
        self.downloader.__dict__['all_ensembl_releases'] = ['release-2', 'release-1']
        self.downloader.ftp.nlst = Mock(side_effect=[
                [
                    'A_species.a_new_genome.dna.nonchromosomal.fa.gz',
                    'A_species.a_new_genome.dna.toplevel.fa.gz',
                    'A_species.a_new_genome.dna_rm.chromosome.1.fa.gz'
                ],
                [   # Second entry has the right genome
                    'A_species.a_genome.dna.nonchromosomal.fa.gz',
                    'A_species.a_genome.dna.toplevel.fa.gz',
                    'A_species.a_genome.dna_rm.chromosome.1.fa.gz'
                ]
            ]
        )
        assert self.downloader.ensembl_base_url == 'release-1'

    @patch.object(reference_data.EnsemblDownloader, 'download_file')
    @patch('subprocess.check_call')
    @patch('egcg_core.util.find_file', side_effect=['fasta.dna.toplevel.fa.gz', 'fasta.dna.toplevel.fa',
                                                    'a_vcf_file.vcf.gz'])
    def test_download_data(self, mocked_find, mocked_check_call,  mocked_download):
        files = [
            'release-1/fasta/a_species/A_species.a_genome.dna.toplevel.fa.gz',
            'release-1/variation/vcf/A_species.vcf.gz',
            'release-1/variation/vcf/A_species.vcf.gz.tbi'
        ]
        self.downloader.ftp.nlst = Mock(
            side_effect=[
                ['dna'],
                files[0:1],
                files[1:]
            ]
        )
        self.downloader.__dict__['ensembl_base_url'] = 'release-1'
        self.downloader.download_data()
        for f in files:
            mocked_download.assert_any_call(f)
        mocked_find.assert_any_call('path/to/reference_data/A_species/a_genome', '*dna.toplevel.fa.gz')
        mocked_find.assert_any_call('path/to/reference_data/A_species/a_genome', '*dna.toplevel.fa')
        mocked_find.assert_any_call('path/to/reference_data/A_species/a_genome', '*.vcf.gz')
        mocked_check_call.assert_called_with(['gzip', '-d', 'fasta.dna.toplevel.fa.gz'])
        assert self.downloader.reference_fasta == 'fasta.dna.toplevel.fa'
        assert self.downloader.reference_variation == 'a_vcf_file.vcf.gz'

    @patch('requests.get')
    @patch.object(Downloader, 'prepare_metadata')
    def test_prepare_metadata(self, parent_prepare_metadata, mocked_get):
        mocked_get.return_value.json.return_value = {
            'karyotype': [1, 2, 3],
            'base_pairs': 100,
            'golden_path': 99,
            'assembly_name': 'a_genome.1'
        }
        self.downloader.prepare_metadata()
        assert self.downloader.payload == {
            'chromosome_count': 3,
            'genome_size': 100,
            'goldenpath': 99
        }
        parent_prepare_metadata.assert_called_with()


class TestEnsemblGenomeDownloader(TestProjectManagement):
    def setUp(self):
        with patch('bin.reference_data.ncbi.get_species_name', return_value='A species'):
            self.downloader = reference_data.EnsemblGenomeDownloader('A species', 'a_genome')
        self.downloader.__dict__['ftp'] = Mock()
        self.downloader.__dict__['_logger'] = Mock()

    def test_ensembl_base_url(self):
        self.downloader.__dict__['all_ensembl_releases'] = ['release-2', 'release-1']
        self.downloader.ftp.nlst = Mock(side_effect=[
            [],  # release-2 bacteria
            [],  # release-2 fungi
            [],  # release-2 metazoa
            [    # release-2 plants
                'A_species.a_genome.dna.nonchromosomal.fa.gz',
                'A_species.a_genome.dna.toplevel.fa.gz',
                'A_species.a_genome.dna_rm.chromosome.1.fa.gz'
            ]
        ]
        )
        assert self.downloader.ensembl_base_url == 'release-2/plants'

    def test_latest_genome_version(self):
        self.downloader.__dict__['all_ensembl_releases'] = ['a_release']
        self.downloader.ftp.nlst = Mock(
            side_effect=[
                [],  # release-2 bacteria
                [],  # release-2 fungi
                [],  # release-2 metazoa
                [    # release-2 plants
                    'A_species.a_genome.dna.nonchromosomal.fa.gz',
                    'A_species.a_genome.dna.toplevel.fa.gz',
                    'A_species.a_genome.dna_rm.chromosome.1.fa.gz'
                ]
            ]
        )
        assert self.downloader.latest_genome_version() == 'a_genome'
        self.downloader.ftp.nlst.assert_any_call('a_release/bacteria/fasta/a_species/dna')
        self.downloader.ftp.nlst.assert_any_call('a_release/fungi/fasta/a_species/dna')
        self.downloader.ftp.nlst.assert_any_call('a_release/metazoa/fasta/a_species/dna')
        self.downloader.ftp.nlst.assert_any_call('a_release/plants/fasta/a_species/dna')


class TestManualDownload(TestProjectManagement):
    def setUp(self):
        with patch('bin.reference_data.ncbi.get_species_name', return_value='A species'):
            self.downloader = reference_data.ManualDownload('A species', 'a_genome')
        self.downloader.__dict__['_logger'] = Mock()

    @patch('builtins.input', return_value='genome website')
    def test_data_source(self, m_input):
        assert self.downloader.data_source == 'genome website'

    @patch('os.path.isfile', return_value=True)
    @patch('bin.reference_data.copyfile')
    @patch('subprocess.check_call')
    def test_download_data(self, m_check_call, m_copyfile, m_isfile):
        list_answers = ['fasta_file.fa.gz', 'vcf_file.vcf.gz']
        with patch('builtins.input', side_effect=list_answers):
            self.downloader.download_data()
            m_copyfile.assert_any_call('fasta_file.fa.gz', 'path/to/reference_data/A_species/a_genome/fasta_file.fa.gz')
            m_copyfile.assert_any_call('vcf_file.vcf.gz', 'path/to/reference_data/A_species/a_genome/vcf_file.vcf.gz')
            m_check_call.assert_called_once_with(['gzip', '-d', 'path/to/reference_data/A_species/a_genome/fasta_file.fa.gz'])
