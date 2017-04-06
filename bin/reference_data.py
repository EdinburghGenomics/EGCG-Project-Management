import requests
import argparse
import ftplib
from egcg_core.app_logging import logging_default as log_cfg

log_cfg.add_stdout_handler()
app_logger = log_cfg.get_logger('genome_downloader')


def _query_ncbi(eutil, db, **query_args):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/' + eutil + '.fcgi'
    params = {'db': db, 'retmode': 'JSON', 'version': '2.0'}
    params.update(query_args)
    return requests.get(url, params)


def list_ids(db, search_term, retmax=20):
    ids = _query_ncbi('esearch', db, term=search_term, retmax=retmax).json()['esearchresult']
    if int(ids['count']) > retmax:
        app_logger.warning('More than %s results found - try a higher retmax, or narrow the search term', retmax)

    app_logger.info('Found %s ids in %s database for %s', ids['count'], db, search_term)
    return ids['idlist']


def list_reference_genomes(search_term):
    genome_ids = list_ids('assembly', search_term)

    genome_summaries = _query_ncbi('esummary', 'assembly', id=','.join(genome_ids)).json()['result']
    genome_summaries.pop('uids')
    for v in genome_summaries.values():
        print(
            '{name}, species {species} ({taxid}), released {date}, ftp={ftp_path}'.format(
                name=v['assemblyname'], species=v['speciesname'], taxid=v['speciestaxid'],
                date=v['seqreleasedate'], ftp_path=v.get('ftppath_genbank') or '(no ftp available)'
            )
        )


def summarise_dbsnp(taxid):
    ftp = ftplib.FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login()
    wd = 'snp/organisms/'

    dname = None
    dbsnp_entries = dict(ftp.mlsd(wd))
    for d in dbsnp_entries:
        if d.endswith(taxid):
            dname = d
            break

    ls = dict(ftp.mlsd(wd + dname))
    if 'VCF' in ls:
        vcf_path = 'ftp://' + ftp.host + '/' + wd + dname + '/VCF'
        app_logger.info('Found dbsnp vcfs for %s at %s', dname, vcf_path)
    else:
        app_logger.warning('No vcfs found for %s', dname)

    ftp.quit()


def main(argv=None):
    a = argparse.ArgumentParser()
    a.add_argument('action', choices=('info',))
    a.add_argument('type', choices=('genome', 'dbsnp'))
    a.add_argument('--search_term')

    args = a.parse_args(argv)

    if args.action == 'info' and args.type == 'genome':
        list_reference_genomes(args.search_term)

    elif args.action == 'info' and args.type == 'dbsnp':
        ids = list_ids('taxonomy', args.search_term)
        if len(ids) != 1:
            app_logger.error('%s taxids found for %s - try narrowing the search term', len(ids), args.search_term)
            return 1

        summarise_dbsnp(ids[0])


if __name__ == '__main__':
    main(['info', 'genome', '--search_term', 'Bos taurus'])
    main(['info', 'dbsnp', '--search_term', 'Bos taurus'])
