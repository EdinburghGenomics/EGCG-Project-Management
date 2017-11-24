from bin import reference_data
from unittest.mock import Mock, MagicMock, patch
from tests import TestProjectManagement

ppath = 'bin.reference_data.'


class TestReferenceData(TestProjectManagement):
    fake_esummary = {
        'result': {
            'uids': ['11337', '11338'],
            '11337': {'uid': '11337', 'assemblyname': 'tThi_1.337', 'speciesname': 'Thingius thingy',
                      'organism': 'Thingius thingy (some kind of flappy swimmy thing)', 'speciestaxid': '1337',
                      'seqreleasedate': 'then', 'ftppath_genbank': 'an_ftp_site/tThi_1.338/'},
            '11338': {'uid': '11338', 'assemblyname': 'tThi_1.338', 'speciesname': 'Thingius thingy',
                      'speciestaxid': '1337', 'seqreleasedate': 'now', 'ftppath_genbank': None}
        }
    }
    fake_mlsds = [
        {'thing_11337': None},
        {'VCF': None, '.': None, '..': None}
    ]

    @classmethod
    def setUpClass(cls):
        reference_data.cfg.load_config_file(cls.etc_config)

    @patch(ppath + 'requests.get')
    def test_query_ncbi(self, mocked_get):
        reference_data._query_ncbi('an_eutil', 'a_db', this='that', other='another')
        mocked_get.assert_called_with(
            'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/an_eutil.fcgi',
            {'db': 'a_db', 'retmode': 'JSON', 'version': '2.0', 'this': 'that', 'other': 'another'}
        )

    @patch(ppath + '_query_ncbi', return_value={'esearchresult': {'count': 2, 'idlist': [1337]}})
    def test_list_ids(self, mocked_query):
        assert reference_data.list_ids('a_db', 'Thingius thingy') == [1337]
        mocked_query.assert_called_with('esearch', 'a_db', term='Thingius thingy', retmax=20)

    @patch(ppath + 'list_ids', return_value=['11337', '11338'])
    @patch(ppath + '_query_ncbi', return_value=fake_esummary)
    @patch('builtins.print')
    def test_list_reference_genomes(self, mocked_print, mocked_query, mocked_list_ids):
        reference_data.list_reference_genomes('Thingius thingy')
        mocked_list_ids.assert_called_with('assembly', 'Thingius thingy')
        mocked_query.assert_called_with('esummary', 'assembly', id='11337,11338')
        for s in ('11337 tThi_1.337, species Thingius thingy (1337), released then, ftp=an_ftp_site/tThi_1.338/',
                  '11338 tThi_1.338, species Thingius thingy (1337), released now, ftp=(no ftp available)'):
            mocked_print.assert_any_call(s)

    @patch(ppath + 'yaml')
    @patch('builtins.open', return_value=MagicMock())
    def test_record_reference_data(self, mocked_open, mocked_yaml):
        with patch(ppath + '_now', return_value='now'), patch(ppath + 'os.makedirs'):
            reference_data.record_reference_data('Thingius thingy', 'tThi_1.337', {'some': 'metadata'})

        mocked_open.assert_called_with('path/to/reference_data/Thingius_thingy/metadata.yaml', 'w')
        mocked_file = mocked_open().__enter__()
        mocked_file.write.assert_any_call('# metadata for Thingius thingy, last modified now\n')
        mocked_yaml.safe_dump.assert_called_with(
            {'tThi_1.337': {'date_downloaded': 'now', 'some': 'metadata'}},
            mocked_file, indent=4, default_flow_style=False
        )

    @patch(ppath + '_query_ncbi', return_value=fake_esummary)
    @patch(ppath + 'record_reference_data')
    def test_get_reference_genome(self, mocked_record, mocked_query):
        reference_data.get_reference_genome('11337')
        mocked_query.assert_called_with('esummary', 'assembly', id='11337')
        mocked_record.assert_called_with(
            'Thingius thingy',
            'tThi_1.337',
            {'uid': '11337', 'assemblyname': 'tThi_1.337',
             'organism': 'Thingius thingy (some kind of flappy swimmy thing)', 'speciestaxid': '1337',
             'seqreleasedate': 'then', 'ftppath_genbank': 'an_ftp_site/tThi_1.338/'}
        )

    @patch(ppath + 'record_reference_data')
    @patch('ftplib.FTP', return_value=Mock(mlsd=Mock(side_effect=fake_mlsds)))
    def test_get_dbsnp(self, mocked_ftp, mocked_record):
        with patch(ppath + '_query_ncbi', return_value={'result': {'11337': {'scientificname': 'Thingius thingy'}}}):
            reference_data.get_dbsnp('11337')
        mocked_ftp().mlsd.assert_any_call('snp/organisms/thing_11337')
        mocked_record.assert_called_with(
            'Thingius thingy',
            'dbsnp',
            {'ftp': 'ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/thing_11337/VCF', 'taxid': '11337'}
        )
