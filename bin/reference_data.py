import os
import ftplib
import logging
import requests
import argparse
import subprocess
from datetime import datetime
from shutil import copyfile
from cached_property import cached_property
from egcg_core import util, rest_communication, ncbi
from egcg_core.app_logging import AppLogger, logging_default
from egcg_core.config import cfg
from egcg_core.exceptions import EGCGError
from config import load_config


class DownloadError(Exception):
    pass


class Downloader(AppLogger):
    def __init__(self, species, genome_version=None, upload=True):
        """
        The abstract Downloader class.
        It retrieve the official species' scientific name from NCBI and
        ensure the species exists before probing starting the actual download.
        """
        scientific_name = ncbi.get_species_name(species)
        if not scientific_name:
            raise EGCGError('Species %s could not be resolved in NCBI please check the spelling.', species)
        self.species = scientific_name
        self.ftp_species = self.species.lower().replace(' ', '_')
        self.genome_version = genome_version or self.latest_genome_version()
        self.upload = upload

        self.rel_data_dir = os.path.join(self.ftp_species[0].upper() + self.ftp_species[1:], self.genome_version)
        self.abs_data_dir = os.path.join(cfg['reference_data']['base_dir'], self.rel_data_dir)

        self.reference_fasta = None
        self.reference_variation = None

        self.tools = {}
        for toolname in ('picard', 'gatk', 'samtools', 'tabix', 'bgzip', 'bwa'):
            tool = cfg.query('tools', toolname)
            if isinstance(tool, str):  # only one tool definition
                self.tools[toolname] = tool
            else:
                self.tools[toolname] = cfg['reference_data'][toolname]

        self.payload = {}
        self.procs = {}
        self.info('Data dir: %s', self.abs_data_dir)
        os.makedirs(self.abs_data_dir, exist_ok=True)

    def latest_genome_version(self):
        return None

    def run(self):
        self.download_data()
        self.prepare_data()
        self.prepare_metadata()
        self.validate_data()
        self.finish_metadata()

        for p in self.procs.values():
            exit_status = p.wait()
            if exit_status != 0:
                self.error('Error during execution of %s: exit status is %s', p.args, p.wait())
            else:
                self.info("Completed cmd '%s' with exit status %s", p.args, p.wait())

        self.upload_to_rest_api()

    def download_data(self):
        """
        Download data retrieve the reference fasta file and the vcf file.
        It should provide the fasta file uncompressed but the vcf file compressed with gzip.
        """
        raise NotImplementedError

    def prepare_data(self):
        """Prepare the reference data by indexing the fasta and vcf files"""
        self.info('Indexing reference genome data')

        if not util.find_file(self.reference_fasta + '.fai'):
            if os.path.isfile(self.reference_fasta + '.gz.fai'):
                os.rename(self.reference_fasta + '.gz.fai', self.reference_fasta + '.fai')
            else:
                self.procs['faidx'] = self.run_background(
                    '%s faidx %s' % (self.tools['samtools'], self.reference_fasta), 'faidx.log'
                )

        dict_file = os.path.splitext(self.reference_fasta)[0] + '.dict'
        if not util.find_file(dict_file):
            self.procs['CreateSequenceDictionary'] = self.run_background(
                '%s -Xmx20G CreateSequenceDictionary R=%s O=%s' % (self.tools['picard'], self.reference_fasta, dict_file),
                'create_sequence_dict.log'
            )

        if not util.find_file(self.reference_fasta, '.bwt'):
            self.procs['bwa index'] = self.run_background(self.tools['bwa'] + ' index ' + self.reference_fasta,
                                                          'bwa_index.log')

        if not self.reference_variation:
            self.info('Finishing with fasta reference genome only')
        else:
            assert os.path.isfile(self.reference_variation)
            tbi = self.reference_variation + '.tbi'
            if not os.path.isfile(tbi):
                self.info('.tbi file not found - bgzipping VCF and indexing.')
                subprocess.check_call('gzip -dc %s > tmp.vcf' % self.reference_variation, shell=True)
                subprocess.check_call('%s -c tmp.vcf > %s' % (self.tools['bgzip'], self.reference_variation),
                                      shell=True)
                tabix = self.run_background('%s -p vcf %s' % (self.tools['tabix'], self.reference_variation),
                                            'tabix.log')
                tabix.wait()
                self.procs['tabix'] = tabix

    def validate_data(self):
        """
        Validate that the reference data conforms to some expectations such as:
          - The vcf file ran through GATK ValidateVariants without error.
        """
        if self.reference_variation:
            if 'faidx' in self.procs:
                self.procs['faidx'].wait()
            self.procs['CreateSequenceDictionary'].wait()

            self.procs['ValidateVariants'] = self.run_background(
                'java -Xmx20G -jar %s -T ValidateVariants -V %s -R %s -warnOnErrors' % (
                    self.tools['gatk'], self.reference_variation, self.reference_fasta),
                '%s.validate_variants.log' % self.reference_variation
            )

    def prepare_metadata(self):
        """Initial preparation of the genome metadata that will be uploaded to the REST API."""
        self.payload.update({
            'tools_used': {
                'picard': self.check_stderr([self.tools['picard'], 'CreateSequenceDictionary', '--version']),
                'tabix': self.check_stderr([self.tools['tabix']]).split('\n')[0].split(' ')[1],
                'bwa': self.check_stderr([self.tools['bwa']]).split('\n')[1].split(' ')[1],
                'bgzip': self.check_stderr([self.tools['bgzip']]).split('\n')[1].split(' ')[1],
                'samtools': self.check_stdout([self.tools['samtools'], '--version']).split('\n')[0].split(' ')[1],
                'gatk': self.check_stdout(['java', '-jar', self.tools['gatk'], '--version'])
            },
            'data_source': self.data_source,
            'data_files': {
                'fasta': os.path.join(self.rel_data_dir, os.path.basename(self.reference_fasta)),
            }
        })
        if self.reference_variation:
            self.payload['data_files']['variation'] = os.path.join(
                self.rel_data_dir, os.path.basename(self.reference_variation)
            )

        if 'faidx' in self.procs:
            self.procs['faidx'].wait()
        defaults = {}
        if os.path.exists(self.reference_fasta + '.fai'):
            genome_size, chromosome_count = self.genome_param_from_fai_file(self.reference_fasta + '.fai')
            defaults = {
                'genome_size': genome_size,
                'chromosome_count': chromosome_count,
                'goldenpath': genome_size
            }
        for field in ('chromosome_count', 'genome_size', 'goldenpath'):
            if field not in self.payload or self.payload[field] is None:
                msg = 'Enter a value to use for %s.' % field
                if field in defaults:
                    msg += ' (%s)' % defaults[field]
                value = input(msg) or defaults.get(field)
                if value:
                    self.payload[field] = int(value)

    def finish_metadata(self):
        """Finalise the genome metadata that will be uploaded to the REST API."""
        self.payload.update(
            assembly_name=self.genome_version,
            species=self.species,
            date_added=self.now()
        )

        project_whitelist = input('Enter a comma-separated list of projects to whitelist for this genome. ')
        if project_whitelist:
            self.payload['project_whitelist'] = project_whitelist.split(',')

        comments = input('Enter any comments about the genome. ')
        if comments:
            self.payload['comments'] = comments

        analyses = ['qc']
        if self.reference_variation:
            analyses.append('variant_calling')
            if self.ftp_species == 'homo_sapiens':  # human
                analyses.append('bcbio')

        self.payload['analyses_supported'] = analyses

    def upload_to_rest_api(self):
        """Upload the genome and optionally the species metadata."""
        if not self.upload:
            print(self.payload)
            return

        rest_communication.post_or_patch('genomes', [self.payload], id_field='assembly_name')

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
                update_lists=['genomes']
            )
        else:
            genome_size = float(int(self.payload.get('genome_size')) / 1000000)
            genome_size = input(
                "Enter species genome size (in Mb) to use for yield calculation. (default: %.0f) " % genome_size
            ) or genome_size

            # FIXME: Probably should expose the taxid in EGCG-Core so we do not have to access the private methods
            info = ncbi._fetch_from_cache(self.species)
            if info:
                q, taxid, scientific_name, common_name = info
            else:
                taxid, scientific_name, common_name = ncbi._fetch_from_eutils(self.species)

            rest_communication.post_entry(
                'species',
                {
                    'name': self.species,
                    'genomes': [self.genome_version],
                    'default_version': self.genome_version,
                    'taxid': taxid,
                    'approximate_genome_size': float(genome_size)
                }
            )

    @staticmethod
    def genome_param_from_fai_file(fai_file):
        """Read in the fai file and extract:
          - the number of entry --> number of chromosome
          - the sum of each entry's size (second column) --> genome size"""
        genome_size = 0
        nb_chromosome = 0
        with open(fai_file) as open_file:
            for line in open_file:
                sp_line = line.strip().split()
                genome_size += int(sp_line[1])
                nb_chromosome += 1
        return genome_size, nb_chromosome

    @property
    def data_source(self):
        raise NotImplementedError

    @staticmethod
    def check_stdout(argv):
        return subprocess.check_output(argv).decode().strip()

    @staticmethod
    def check_stderr(argv):
        """Capture output from tabix and picard --version commands, which print to stderr and exit with status 1."""
        p = subprocess.Popen(argv, stderr=subprocess.PIPE)
        out, err = p.communicate()
        return err.decode().strip()

    def run_background(self, cmd, log_file):
        return subprocess.Popen('%s > %s 2>&1' % (cmd, os.path.join(self.abs_data_dir, log_file)), shell=True)

    @staticmethod
    def now():
        return datetime.utcnow().strftime('%d_%m_%Y_%H:%M:%S')


class EnsemblDownloader(Downloader):
    ftp_site = 'ftp.ensembl.org'
    rest_site = 'http://rest.ensembl.org'

    def variation_url(self, base_url):
        return '%s/variation/vcf/%s' % (base_url, self.ftp_species)

    @cached_property
    def ftp(self):
        ftp = ftplib.FTP(self.ftp_site)
        ftp.login()
        ftp.cwd('pub')
        return ftp

    @cached_property
    def all_ensembl_releases(self):
        releases = []
        for d in self.ftp.nlst():  # Ensembl doesn't support mlsd for some reason
            if d.startswith('release-'):
                releases.append(int(d.split('-')[1]))

        releases.sort(reverse=True)
        return ['release-%s' % r for r in releases]

    @cached_property
    def ensembl_base_url(self):
        for release in self.all_ensembl_releases:
            ls = self.ftp.nlst('%s/fasta/%s/dna' % (release, self.ftp_species))
            top_level_fastas = [f for f in ls if f.endswith('dna.toplevel.fa.gz')]
            if top_level_fastas and self.genome_version in top_level_fastas[0]:
                self.info('Found %s in Ensembl %s', self.genome_version, release)
                return release

        raise DownloadError('Could not find any Ensembl releases for ' + self.genome_version)

    def latest_genome_version(self):
        ls = self.ftp.nlst('%s/fasta/%s/dna' % (self.all_ensembl_releases[0], self.ftp_species))
        if not ls:
            raise DownloadError('Could not find %s in latest Ensembl release' % self.ftp_species)

        ext = '.dna.toplevel.fa.gz'
        top_level_fasta = [f for f in ls if f.endswith(ext)][0]
        return '.'.join(top_level_fasta.split('.')[1:]).replace(ext, '')

    def download_data(self):
        """
        Download reference and variation file from Ensembl and decompress if necessary.
        """
        self.info('Downloading reference genome')
        base_dir = '%s/fasta/%s' % (self.ensembl_base_url, self.ftp_species)

        ls = self.ftp.nlst(base_dir)
        if os.path.join(base_dir, 'dna_index') in ls:
            ls = self.ftp.nlst(os.path.join(base_dir, 'dna_index'))
        else:
            ls = self.ftp.nlst(os.path.join(base_dir, 'dna'))

        files_to_download = [f for f in ls if 'dna.toplevel' in f]
        self.info('Reference genome: found %i files, downloading %i', len(ls), len(files_to_download))
        for f in files_to_download:
            self.download_file(f)

        # TODO: Add support multiple chromosome files if toplevel does not exist
        fa_gz = util.find_file(self.abs_data_dir, '*dna.toplevel.fa.gz')
        if not fa_gz:
            raise DownloadError('No fasta file found')

        subprocess.check_call(['gzip', '-d', fa_gz])
        self.reference_fasta = util.find_file(self.abs_data_dir, '*dna.toplevel.fa')

        ls = self.ftp.nlst(self.variation_url(self.ensembl_base_url))
        files_to_download = [f for f in ls if '%s.vcf.gz' % self.ftp_species in f.lower()]
        if not files_to_download:
            i = input('%i files found. Enter a basename for a single VCF to use, or nothing to continue without '
                      'variants. ' % len(ls))
            if i:
                files_to_download = [f for f in ls if i in f]  # this should include the index file

        self.info('Variation: found %i files, downloading %i', len(ls), len(files_to_download))
        for f in files_to_download:
            self.download_file(f)

        self.reference_variation = util.find_file(self.abs_data_dir, '*.vcf.gz') or input(
            'Could not identify a vcf.gz file to use - enter one here. ')

    def prepare_metadata(self):
        assembly_data = requests.get(
            '%s/info/assembly/%s' % (self.rest_site, self.species),
            params={'content-type': 'application/json'}
        ).json()

        if 'assembly_name' in assembly_data and self.genome_version in assembly_data['assembly_name'].replace(' ', '_'):
            self.payload['chromosome_count'] = len(assembly_data.get('karyotype'))
            self.payload['genome_size'] = assembly_data.get('base_pairs')
            self.payload['goldenpath'] = assembly_data.get('golden_path')
        else:
            self.info('Assembly not found in Ensembl Rest API')
        # Run parent function after so metadata is not requested from user if available in ensembl
        super().prepare_metadata()

    @property
    def data_source(self):
        return 'ftp://%s/pub/%s' % (self.ftp_site, self.ensembl_base_url)

    def download_file(self, fp):
        local_file_path = os.path.join(self.abs_data_dir, os.path.basename(fp))
        dir_name = os.path.dirname(local_file_path)
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)

        with open(local_file_path, 'wb') as f:
            self.ftp.retrbinary('RETR ' + fp, f.write)


class EnsemblGenomeDownloader(EnsemblDownloader):
    ftp_site = 'ftp.ensemblgenomes.org'
    rest_site = 'http://rest.ensemblgenomes.org'
    sub_sites = ['bacteria', 'fungi', 'metazoa', 'plants', 'protists']

    def variation_url(self, base_url):
        return '%s/vcf/%s' % (base_url, self.ftp_species)

    @cached_property
    def ensembl_base_url(self):
        for release in self.all_ensembl_releases:
            for site in self.sub_sites:
                ls = self.ftp.nlst('%s/%s/fasta/%s/dna' % (release, site, self.ftp_species))
                top_level_fastas = [f for f in ls if f.endswith('dna.toplevel.fa.gz')]
                if top_level_fastas and self.genome_version in top_level_fastas[0]:
                    self.info('Found %s in EnsemblGenomes %s', self.genome_version, release + '/' + site)
                    return release + '/' + site

        raise DownloadError('Could not find any Ensembl releases for ' + self.genome_version)

    def latest_genome_version(self):
        ls = None
        for site in self.sub_sites:
            ls = self.ftp.nlst('%s/%s/fasta/%s/dna' % (self.all_ensembl_releases[0], site, self.ftp_species))
            if ls:
                break
        if not ls:
            raise DownloadError('Could not find %s in latest Ensembl release' % self.ftp_species)

        ext = '.dna.toplevel.fa.gz'
        top_level_fasta = [f for f in ls if f.endswith(ext)][0]
        return '.'.join(top_level_fasta.split('.')[1:]).replace(ext, '')


class ManualDownload(Downloader):
    def latest_genome_version(self):
        raise DownloadError('Manual download needs the genome_version to be provided from the command line.')

    @cached_property
    def data_source(self):
        data_source = input('Enter a value to use for data_source. ')
        return data_source

    def download_data(self):
        self.info('Downloading reference genome')
        fa_gz = input('Provide a fasta file to use.')
        if not fa_gz:
            raise DownloadError('No fasta file found')
        elif not os.path.isfile(fa_gz):
            raise DownloadError('fasta file provided %s was not found' % fa_gz)

        if not os.path.abspath(os.path.dirname(fa_gz)) == self.abs_data_dir:
            self.info('Copy %s to %s', os.path.basename(fa_gz), self.abs_data_dir)
            new_path = os.path.join(self.abs_data_dir, os.path.basename(fa_gz))
            copyfile(fa_gz, new_path)
            fa_gz = new_path

        self.reference_fasta = fa_gz
        if fa_gz.endswith('.gz'):
            subprocess.check_call(['gzip', '-d', fa_gz])
            self.reference_fasta = fa_gz[:-3]

        vcf = input('Could not identify a vcf.gz file to use - enter one here.')
        if vcf and not os.path.isfile(vcf):
            raise DownloadError('vcf file provided %s was not found' % vcf)
        if vcf:
            if not os.path.abspath(os.path.dirname(vcf)) == self.abs_data_dir:
                self.info('Copy %s to %s', os.path.basename(vcf), self.abs_data_dir)
                new_path = os.path.join(self.abs_data_dir, os.path.basename(vcf))
                copyfile(vcf, new_path)
                vcf = new_path
            self.reference_variation = vcf


def main():
    a = argparse.ArgumentParser()
    a.add_argument(
        'species',
        help='Species name as used by NCBI. If there are spaces in the species name, it should be quoted'
    )
    a.add_argument('--genome_version', default=None)
    a.add_argument('--no_upload', dest='upload', action='store_false', help='Turn off the metadata upload')
    a.add_argument('--manual', action='store_true', help='Run manual metadata upload only, even if present in Ensembl')
    a.add_argument('--debug', action='store_true', help='Show debug statement in the output')
    args = a.parse_args()
    load_config()

    logging_default.add_stdout_handler()
    if args.debug:
        logging_default.set_log_level(logging.DEBUG)
    if args.manual:
        d = ManualDownload(args.species, args.genome_version, args.upload)
        d.run()
    else:
        for downloader in EnsemblDownloader, EnsemblGenomeDownloader:
            try:
                d = downloader(args.species, args.genome_version, args.upload)
                d.run()
                # Some species are in both the Ensembl and EnsemblGenome ftp site
                # break when one is successful
                break
            except DownloadError as e:
                print(str(e))


if __name__ == '__main__':
    main()
