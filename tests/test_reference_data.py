from unittest.mock import Mock, patch
from bin import reference_data
from tests import TestProjectManagement


class TestDownloader(TestProjectManagement):
    def setUp(self):
        self.downloader = reference_data.Downloader('A species', 'a_genome')
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

    @patch.object(reference_data.Downloader, 'download_file')
    def test_download_data(self, mocked_download):
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
        self.downloader.__dict__['ensembl_release'] = 'release-1'
        self.downloader.download_data()
        for f in files:
            mocked_download.assert_any_call(f)

    @patch('builtins.input', return_value=None)
    @patch('os.path.isfile', side_effect=[False, True, False])
    @patch('subprocess.check_call')
    @patch('bin.reference_data.Downloader.run_background')
    @patch('egcg_core.util.find_file')
    @patch('egcg_core.util.find_files', return_value=[])
    def test_prepare_data(self, m_find_files, m_find_file, m_run, m_check_call, m_isfile, m_input):
        m_find_file.side_effect = [
            'a_genome.dna.toplevel.fa.gz',
            'a_genome.dna.toplevel.fa',
            None,  # no fa.fai, so run faidx
            None,  # no .dict, so run picard
            None,  # no .bwt, so run bwa index
            'a_species.vcf.gz'
        ]

        self.downloader.prepare_data()

        m_check_call.assert_any_call(['gzip', '-d', 'a_genome.dna.toplevel.fa.gz']),
        m_check_call.assert_any_call('gzip -dc a_species.vcf.gz > tmp.vcf', shell=True),
        m_check_call.assert_any_call('path/to/bgzip -c tmp.vcf > a_species.vcf.gz', shell=True)
        assert m_check_call.call_count == 3

        m_run.assert_any_call('path/to/samtools faidx a_genome.dna.toplevel.fa', 'faidx.log'),
        m_run.assert_any_call('path/to/picard CreateSequenceDictionary R=a_genome.dna.toplevel.fa O=a_genome.dna.toplevel.dict', 'create_sequence_dict.log'),
        m_run.assert_any_call('path/to/bwa index a_genome.dna.toplevel.fa', 'bwa_index.log'),
        m_run.assert_any_call('path/to/tabix -p vcf a_species.vcf.gz', 'tabix.log'),
        m_run.assert_any_call('java -jar path/to/gatk -T ValidateVariants -V a_species.vcf.gz -R a_genome.dna.toplevel.fa -warnOnErrors', 'a_species.vcf.gz.validate_variants.log')
        assert m_run.call_count == 5

    @patch('bin.reference_data.Downloader.check_stdout', return_value='tool v1.0')
    @patch('bin.reference_data.Downloader.check_stderr', return_value='tool v1.2\ntool v1.1')
    @patch('requests.get')
    def test_prepare_ensembl_metadata(self, mocked_get, mocked_stderr, mocked_stdout):
        mocked_get.return_value.json.return_value = {
            'karyotype': [1, 2, 3],
            'base_pairs': 100,
            'golden_path': 99,
            'assembly_name': 'a_genome.1'
        }
        self.downloader.__dict__['ensembl_release'] = 'release-1'
        exp_tools_used = {
            'picard': 'tool v1.2\ntool v1.1',
            'tabix': 'v1.2',
            'bwa': 'v1.1',
            'bgzip': 'v1.1',
            'samtools': 'v1.0',
            'gatk': 'tool v1.0'
        }
        assert len(exp_tools_used) == len(self.downloader.tools)
        obs = self.downloader.prepare_ensembl_metadata({'some': 'data_files'})
        assert obs == {
            'data_files': {'some': 'data_files'},
            'tools_used': exp_tools_used,
            'data_source': 'ftp://ftp.ensembl.org/pub/release-1',
            'chromosome_count': 3,
            'genome_size': 100,
            'goldenpath': 99
        }

    @patch('builtins.input', side_effect=[3, 100, 99, 'a_data_source', 'this:v0.1,that:v0.2'])
    def test_prepare_manual_metadata(self, mocked_input):
        assert self.downloader.prepare_manual_metadata({'some': 'data_files'}) == {
            'data_files': {'some': 'data_files'},
            'chromosome_count': 3,
            'genome_size': 100,
            'goldenpath': 99,
            'data_source': 'a_data_source',
            'tools_used': {
                'this': 'v0.1',
                'that': 'v0.2'
            }
        }

    @patch('requests.get', return_value=Mock(json=Mock(return_value=[{'id': 'a_taxid'}]), status_code=200))
    @patch('egcg_core.rest_communication.get_document')
    @patch('egcg_core.rest_communication.post_entry')
    @patch('egcg_core.rest_communication.patch_entry')
    @patch('egcg_core.rest_communication.post_or_patch')
    @patch('egcg_core.util.find_files')
    @patch('bin.reference_data.Downloader.now', return_value='now')
    @patch('builtins.input', side_effect=['project1,project2', 'Some comments', 'y', 'project1,project2', 'Some comments'])
    def test_finish_metadata(self, minput, mnow, m_find, mpostpatch, mpatch, mpost, mgetdoc, mget):
        self.downloader.finish_metadata({})
        mpostpatch.assert_called_with(
            'genomes',
            {
                'assembly_name': 'a_genome',
                'species': 'A species',
                'date_added': 'now',
                'project_whitelist': ['project1', 'project2'],
                'comments': 'Some comments',
                'analyses_supported': ['qc', 'variant_calling']
            },
            id_field='assembly_name'
        )
        mpatch.assert_called_with(
            'species',
            {'genomes': ['a_genome'], 'default_version': 'a_genome'},
            'name',
            'A species',
            update_lists=True
        )

        mgetdoc.return_value = None
        self.downloader.finish_metadata({})
        mpost.assert_called_with(
            'species',
            {'name': 'A species', 'genomes': ['a_genome'], 'default_version': 'a_genome', 'taxid': 'a_taxid'},
        )
