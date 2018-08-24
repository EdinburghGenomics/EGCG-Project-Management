import os
import ftplib
import requests
import argparse
import subprocess
from datetime import datetime
from cached_property import cached_property
from egcg_core import util, rest_communication
from egcg_core.config import cfg
from egcg_core.app_logging import AppLogger, logging_default
from egcg_core.executor import local_execute
from config import load_config


logging_default.set_log_level(10)
logging_default.add_stdout_handler()


class DownloadError(Exception):
    pass


def now():
    return datetime.utcnow().strftime('%d_%m_%Y_%H:%M:%S')


class Downloader(AppLogger):
    def __init__(self, species, genome_version=None, upload=True, download=True):
        self.species = species
        self.ftp_species = species.lower().replace(' ', '_')
        self.species_dir = self.ftp_species[0].upper() + self.ftp_species[1:]
        self.genome_version = genome_version or self.latest_genome_version()
        self.upload = upload
        self.download = download

        self.data_dir = os.path.join(
            cfg['genome_downloader']['base_dir'],
            self.species_dir,
            self.genome_version
        )
        self.tools = {t: cfg['genome_downloader'][t] for t in ('picard', 'gatk', 'samtools')}
        self.info('Data dir: %s', self.data_dir)

    def run(self):
        if self.ensembl_release:  # and not self.manual:
            if self.download:
                self.download_data()
                self.prepare_data()

            payload = self.prepare_ensembl_metadata()
        else:
            self.info('Running metadata upload only')
            assert self.genome_version
            payload = self.prepare_manual_metadata()

        self.finish_metadata(payload)

    @cached_property
    def ftp(self):
        ftp = ftplib.FTP('ftp.ensembl.org')
        ftp.login()
        ftp.cwd('pub')
        return ftp

    @cached_property
    def all_ensembl_releases(self):
        releases = [
            d for d in self.ftp.nlst()  # Ensembl doesn't support mlsd for some reason
            if d.startswith('release-')
        ]
        releases.reverse()
        return releases

    @cached_property
    def ensembl_release(self):
        for release in self.all_ensembl_releases:
            ls = self.ftp.nlst('%s/fasta/%s/dna' % (release, self.ftp_species))
            top_level_fastas = [f for f in ls if f.endswith('dna.toplevel.fa.gz')]
            if top_level_fastas and self.genome_version in top_level_fastas[0]:
                return release

        self.info('Could not find any Ensembl releases for ' + self.genome_version)

    def latest_genome_version(self):
        ls = self.ftp.nlst('%s/fasta/%s/dna' % (self.all_ensembl_releases[0], self.ftp_species))
        if not ls:
            raise DownloadError('Could not find %s in latest Ensembl release' % self.ftp_species)

        ext = '.dna.toplevel.fa.gz'
        top_level_fasta = [f for f in ls if f.endswith(ext)][0]
        return '.'.join(top_level_fasta.split('.')[1:]).replace(ext, '')

    def download_data(self):
        self.info('Downloading reference genome')
        base_dir = '%s/fasta/%s' % (self.ensembl_release, self.ftp_species)

        ls = self.ftp.nlst(base_dir)
        if 'dna_index' in ls:
            ls = self.ftp.nlst(os.path.join(base_dir, 'dna_index'))
        else:
            ls = self.ftp.nlst(os.path.join(base_dir, 'dna'))

        files_to_download = [f for f in ls if 'dna.toplevel' in f]
        self.info('Reference genome: found %i files, downloading %i', len(ls), len(files_to_download))
        for f in files_to_download:
            self.download_file(f)

        ls = self.ftp.nlst('%s/variation/vcf/%s' % (self.ensembl_release, self.ftp_species))
        files_to_download = [f for f in ls if '%s.vcf.gz' % self.ftp_species in f.lower()]
        if not files_to_download:
            i = input('%i files found. Enter a basename for a single VCF to use, or nothing to continue without '
                      'variants. ' % len(ls))
            if i:
                files_to_download = [f for f in ls if i in f]  # this should include the index file
            else:
                files_to_download = []

        self.info('Variation: found %i files, downloading %i', len(ls), len(files_to_download))
        for f in files_to_download:
            self.download_file(f)

    def download_file(self, fp):
        local_file_path = os.path.join(self.data_dir, os.path.basename(fp))
        dir_name = os.path.dirname(local_file_path)
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)

        with open(local_file_path, 'wb') as f:
            self.ftp.retrbinary('RETR ' + fp, f.write)

    def prepare_data(self):
        self.info('Preparing downloaded data')
        fa_gzs = util.find_files(self.data_dir, '*dna.toplevel.fa.gz')
        if len(fa_gzs) != 1:
            raise DownloadError('%s fa.gz files found' % len(fa_gzs))

        fastas = util.find_files(self.data_dir, '*dna.toplevel.fa')
        if fastas:
            raise DownloadError('Unexpected .fa files found: %s' % fastas)

        fasta_gz = fa_gzs[0]
        local_execute('gzip -d %s' % fasta_gz).join()
        fasta = util.find_file(self.data_dir, '*dna.toplevel.fa')
        if not util.find_files(self.data_dir, '*dna.toplevel.fa.gz.fai'):
            local_execute(self.tools['samtools'] + ' faidx %s' % fasta).join()

        dict_file = os.path.splitext(fasta)[0] + '.dict'
        if not util.find_file(dict_file):
            local_execute(self.tools['picard'] + ' CreateSequenceDictionary R=%s O=%s' % (fasta, dict_file)).join()

        vcfs = util.find_files(self.data_dir, '*.vcf.gz')
        if len(vcfs) == 1:
            vcf = vcfs[0]
        else:
            vcf = None

        if not vcf:
            self.info('Finishing with fasta reference genome only')
        else:
            assert os.path.isfile(vcf)
            validation_log = '%s.validate_variants.log' % vcf
            p = subprocess.Popen(
                'java -jar %s -T ValidateVariants -V %s -R %s -warnOnErrors > %s 2>&1' % (
                    self.tools['gatk'], vcf, fasta, validation_log
                ),
                shell=True
            )
            exit_status = p.wait()
            self.info('Validation completed with exit status %s - see log at %s', exit_status, validation_log)

    def prepare_ensembl_metadata(self):
        payload = {
            'tools_used': {
                'picard': subprocess.Popen(  # picard --version gives exit status 1, so need to use Popen
                    [self.tools['picard'], 'CreateSequenceDictionary', '--version'],
                    stderr=subprocess.PIPE
                ).communicate()[1].decode().strip(),
                'samtools': subprocess.check_output(
                    [self.tools['samtools'], '--version']
                ).decode().split('\n')[0].split(' ')[1],
                'gatk': subprocess.check_output(['java', '-jar', self.tools['gatk'], '--version']).decode().strip()
            },
            'data_source': 'ftp://ftp.ensembl.org/pub/' + self.ensembl_release
        }
        assembly_data = requests.get(
            'http://rest.ensembl.org/info/assembly/%s' % self.species,
            params={'content-type': 'application/json'}
        ).json()
        if self.genome_version in assembly_data['assembly_name']:
            payload['chromosome_count'] = len(assembly_data['karyotype'])
            payload['genome_size'] = assembly_data['base_pairs']
        else:
            self.info('Assembly not found in Ensembl Rest API')
            for field in ('chromosome_count', 'genome_size'):
                data = input('Enter a value to use for %s. ' % field)
                if data:
                    payload[field] = int(data)

        return payload

    @staticmethod
    def prepare_manual_metadata():
        payload = {}
        for field in ('chromosome_count', 'genome_size'):
            value = input('Enter a value to use for %s. ' % field)
            if value:
                payload[field] = int(value)

        data_source = input('Enter a value to use for data_source')
        if data_source:
            payload['data_source'] = data_source

        payload['tools_used'] = {}
        tools = input("Enter tools used to index/validate data in the format 'tool_1:version,tool_2:version...' ")
        for t in tools.split(','):
            tool, version = t.split(':')
            payload['tools_used'][tool] = version

        return payload

    def finish_metadata(self, payload):
        payload.update(
            assembly_name=self.genome_version,
            species=self.species,
            date_added=now()
        )

        project_whitelist = input('Enter a comma-separated list of projects to whitelist for this genome. ')
        if project_whitelist:
            payload['project_whitelist'] = project_whitelist.split(',')

        comments = input('Enter any comments about the genome. ')
        if comments:
            payload['comments'] = comments

        analyses = ['qc']
        if util.find_files(self.data_dir, '*.vcf.gz'):
            analyses.append('variant_calling')
            if self.ftp_species == 'homo_sapiens':  # human
                analyses.append('bcbio')

        payload['analyses_supported'] = analyses

        if not self.upload:
            print(payload)
            return

        rest_communication.post_or_patch('genomes', payload, id_field='assembly_name')

        species = rest_communication.get_document('species', where={'name': self.species})
        if species:
            species_payload = {'genomes': [self.genome_version]}
            if input("Enter 'y' if the current genome version should be set as the default for this species. ") == 'y':
                species_payload['default_version'] = self.genome_version

            rest_communication.patch_entry(
                'species',
                species_payload,
                'name',
                self.species,
                update_lists=True
            )
        else:
            r = requests.get(
                'http://rest.ensembl.org/taxonomy/name/%s' % self.species,
                params={'content-type': 'application/json'}
            )
            taxonomy_data = r.json()
            if r.status_code == 200 and len(taxonomy_data) == 1:
                taxid = taxonomy_data[0]['id']
            else:
                taxid = input('%i taxons found for %s - enter the taxid of this species. ' % (len(taxonomy_data), self.species))

            rest_communication.post_entry(
                'species',
                {
                    'name': self.species,
                    'genomes': [self.genome_version],
                    'default_version': self.genome_version,
                    'taxid': taxid
                }
            )


def main():
    a = argparse.ArgumentParser()
    a.add_argument(
        'species',
        help="Species name as used by Ensembl's FTP site, with underscores and lower case, e.g. 'felis_catus'"
    )
    a.add_argument('--genome_version', default=None)
    a.add_argument('--no_upload', dest='upload', action='store_false', help='Turn off the metadata upload')
    a.add_argument('--no_download', dest='download', action='store_false', help='Turn off the data download')
    args = a.parse_args()
    load_config()
    d = Downloader(args.species, args.genome_version, args.upload, args.download)
    d.run()


if __name__ == '__main__':
    main()


# TODO: goldenpath
