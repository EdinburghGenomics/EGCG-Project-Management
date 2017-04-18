import os
import requests
import argparse
import ftplib
import yaml
from datetime import datetime
from egcg_core.config import cfg
from egcg_core.app_logging import logging_default as log_cfg
from config import load_config

load_config()
log_cfg.add_stdout_handler()
app_logger = log_cfg.get_logger('genome_downloader')


def _now():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')


def _query_ncbi(eutil, db, **query_args):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/' + eutil + '.fcgi'
    params = {'db': db, 'retmode': 'JSON', 'version': '2.0'}
    params.update(query_args)
    return requests.get(url, params).json()


def list_ids(db, search_term, retmax=20):
    ids = _query_ncbi('esearch', db, term=search_term, retmax=retmax)['esearchresult']
    if int(ids['count']) > retmax:
        app_logger.warning('More than %s results found - try a higher retmax, or narrow the search term', retmax)

    app_logger.info('Found %s ids in %s database for %s', ids['count'], db, search_term)
    return ids['idlist']


def list_reference_genomes(search_term):
    genome_ids = [str(i) for i in list_ids('assembly', search_term)]
    genome_summaries = _query_ncbi('esummary', 'assembly', id=','.join(genome_ids))['result']
    genome_summaries.pop('uids')
    for v in genome_summaries.values():
        print(
            '{id} {name}, species {species} ({taxid}), released {date}, ftp={ftp_path}'.format(
                id=v['uid'], name=v['assemblyname'], species=v['speciesname'], taxid=v['speciestaxid'],
                date=v['seqreleasedate'], ftp_path=v.get('ftppath_genbank') or '(no ftp available)'
            )
        )


def record_reference_data(species, assembly, metadata):
    """
    Write metadata to a file in reference_data/species/metadata.yaml.
    :param str species:
    :param str assembly: The genome assembly to write to, or 'dbsnp'
    :param dict metadata:
    """
    md_dir = os.path.join(cfg['genome_downloader']['base_dir'], species.replace(' ', '_'))
    os.makedirs(md_dir, exist_ok=True)
    md_file = os.path.join(md_dir, 'metadata.yaml')

    content = {}
    if os.path.isfile(md_file):
        with open(md_file, 'r') as f:
            content = yaml.safe_load(f)

    if assembly in content:
        app_logger.warning('%s already in metadata file. Remove this to re-run', assembly)
        return 0

    now = _now()
    metadata['date_downloaded'] = now
    content[assembly] = metadata

    with open(md_file, 'w') as f:
        f.write('# metadata for %s, last modified %s\n' % (species, now))
        yaml.safe_dump(content, f, indent=4, default_flow_style=False)


def get_reference_genome(uid):
    data = _query_ncbi('esummary', 'assembly', id=uid)['result'][uid]
    app_logger.info('Download Fasta files from %s, merge and index with bwa and bowtie', data['ftppath_genbank'])

    keys = ('organism', 'assemblyname', 'seqreleasedate', 'speciestaxid', 'uid', 'ftppath_genbank')
    record_reference_data(
        data['speciesname'],
        data['assemblyname'],
        {k: data[k] for k in keys}
    )


def list_taxids(search_term):
    ids = list_ids('taxonomy', search_term)
    app_logger.info('Found %s taxids for search term %s: %s', len(ids), search_term, ids)


def get_dbsnp(taxid):
    host = 'ftp.ncbi.nlm.nih.gov'
    ftp = ftplib.FTP(host)
    ftp.login()
    wd = 'snp/organisms/'

    dname = None
    dbsnp_entries = dict(ftp.mlsd(wd))
    for d in dbsnp_entries:
        if d.endswith(taxid):
            dname = d
            break

    ls = dict(ftp.mlsd(wd + dname))
    ftp.quit()
    if 'VCF' not in ls:
        app_logger.warning('No vcfs found for %s', dname)
        return 1

    vcf_path = 'ftp://' + host + '/' + wd + dname + '/VCF'
    app_logger.info('Found dbsnp vcfs for %s at %s', dname, vcf_path)

    esummary = _query_ncbi('esummary', 'taxonomy', id=taxid)['result'][taxid]
    record_reference_data(esummary['scientificname'], 'dbsnp', {'ftp': vcf_path, 'taxid': taxid})


def main(argv=None):
    a = argparse.ArgumentParser()
    a.add_argument('action', choices=('list', 'info'))
    a.add_argument('type', choices=('genome', 'dbsnp'))
    a.add_argument('--search_term', required=True)

    args = a.parse_args(argv)

    if args.action == 'list' and args.type == 'genome':
        list_reference_genomes(args.search_term)

    elif args.action == 'list' and args.type == 'dbsnp':
        list_taxids(args.search_term)

    elif args.action == 'info' and args.type == 'genome':
        get_reference_genome(args.search_term)

    elif args.action == 'info' and args.type == 'dbsnp':
        get_dbsnp(args.search_term)

    else:
        return 1
