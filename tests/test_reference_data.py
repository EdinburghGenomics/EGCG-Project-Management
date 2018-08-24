from unittest.mock import Mock, patch
from bin import reference_data_download
from tests import TestProjectManagement


def fake_check_output(argv):
    if 'java' in argv:  # gatk
        return b'v1.0'
    else:
        return b'samtools v1.1'


class TestDownloader(TestProjectManagement):
    def setUp(self):
        self.downloader = reference_data_download.Downloader('A species', 'a_genome')
        self.downloader.__dict__['ftp'] = Mock()
        self.downloader.__dict__['_logger'] = Mock()

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

    @patch.object(reference_data_download.Downloader, 'download_file')
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

    @patch('os.path.isfile', return_value=True)
    @patch('subprocess.Popen')
    @patch('bin.reference_data_download.local_execute')
    @patch('egcg_core.util.find_file')
    @patch('egcg_core.util.find_files')
    def test_prepare_data(self, mocked_find_files, mocked_find_file, mocked_execute, mocked_popen, mocked_isfile):
        mocked_find_files.side_effect = [
            ['a_genome.dna.toplevel.fa.gz'],
            [],  # pre-gzip
            [],  # fa.fai
            ['a_species.vcf.gz']
        ]
        mocked_find_file.side_effect = [
            'a_genome.dna.toplevel.fa',  # post-gzip
            None  # dict file
        ]
        self.downloader.prepare_data()

        exp_cmds = (
            'gzip -d a_genome.dna.toplevel.fa.gz',
            'path/to/samtools faidx a_genome.dna.toplevel.fa',
            'path/to/picard CreateSequenceDictionary R=a_genome.dna.toplevel.fa O=a_genome.dna.toplevel.dict'
        )
        for cmd in exp_cmds:
            mocked_execute.assert_any_call(cmd)

        mocked_popen.assert_called_with(
            'java -jar path/to/gatk -T ValidateVariants -V a_species.vcf.gz -R a_genome.dna.toplevel.fa -warnOnErrors '
            '> a_species.vcf.gz.validate_variants.log 2>&1',
            shell=True
        )

    @patch('subprocess.check_output', new=fake_check_output)
    @patch('subprocess.Popen', return_value=Mock(communicate=Mock(return_value=(None, b'v1.2'))))
    @patch('requests.get', return_value=Mock(json=Mock(return_value={'karyotype': [1, 2, 3], 'base_pairs': 100, 'assembly_name': 'a_genome.1'})))
    def test_prepare_ensembl_metadata(self, mocked_requests, mocked_popen):
        self.downloader.__dict__['ensembl_release'] = 'release-1'
        assert self.downloader.prepare_ensembl_metadata() == {
            'tools_used': {'picard': 'v1.2', 'samtools': 'v1.1', 'gatk': 'v1.0'},
            'data_source': 'ftp://ftp.ensembl.org/pub/release-1',
            'chromosome_count': 3,
            'genome_size': 100
        }

    @patch('builtins.input', side_effect=[3, 100, 'a_data_source', 'this:v0.1,that:v0.2'])
    def test_prepare_manual_metadata(self, mocked_input):
        assert self.downloader.prepare_manual_metadata() == {
            'chromosome_count': 3,
            'genome_size': 100,
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
    @patch('bin.reference_data_download.now', return_value='now')
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
