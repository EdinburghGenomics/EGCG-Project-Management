import os
import csv
from collections import defaultdict

import yaml
import datetime
from cached_property import cached_property
from os import path

from egcg_core.app_logging import AppLogger
from egcg_core.util import find_file, query_dict
from egcg_core.clarity import connection
from egcg_core.rest_communication import get_documents
from egcg_core.exceptions import EGCGError

from config import cfg
from project_report.utils import get_folder_size


class ProjectReportInformation(AppLogger):
    workflow_alias = {
        'TruSeq Nano DNA Sample Prep': 'Illumina TruSeq Nano library',
        'TruSeq PCR-Free DNA Sample Prep': 'Illumina TruSeq PCR-Free library',
        'TruSeq PCR-Free Sample Prep': 'Illumina TruSeq PCR-Free library',
        'TruSeq DNA PCR-Free Sample Prep': 'Illumina TruSeq PCR-Free library'
    }
    library_abbreviation = {
        'User Prepared Library': 'UPL',
        'Illumina TruSeq Nano library': 'Nano',
        'Illumina TruSeq PCR-Free library': 'PCRfree'
    }
    analysis_abbreviation = {
        'bcbio': 'bcbio',
        'variant_calling': 'variant',
        'qc': 'basic qc'
    }
    analysis_description = {
        'bcbio': 'GATK3 based variant call for human',
        'variant_calling': 'GATK based variant call',
        'qc': 'Alignment based quality control'
    }

    species_abbreviation = {}

    def __init__(self, project_name):
        self.project_name = project_name
        self.run_folders = cfg['sample']['input_dir']
        self.project_source = path.join(cfg['delivery']['source'], project_name)
        self.project_delivery = path.join(cfg['delivery']['dest'], project_name)
        self.lims = connection()
        self.adapter1 = 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA'
        self.adapter2 = 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT'
        self._sample_info = {}

    @cached_property
    def lims_project(self):
        return self.lims.get_projects(name=self.project_name)[0]

    @cached_property
    def sample_status_for_project(self):
        return get_documents('lims/status/sample_status',
                             match={'project_id': self.project_name, "project_status": "all"})

    @cached_property
    def sample_info_for_project(self):
        return get_documents('lims/status/sample_info',
                             match={'project_id': self.project_name, "project_status": "all"})

    @cached_property
    def sample_data_for_project(self):
        samples = get_documents('samples', where={'project_id': self.project_name, 'delivered': 'yes'}, all_pages=True)
        if not samples:
            raise EGCGError('No samples found for project %s' % self.project_name)
        return samples

    # Private functions to parse and extract sample data
    def _get_genome_version(self, sample_name):
        species = self.get_species_from_sample(sample_name)
        genome_version = query_dict(self.sample_info(sample_name), 'info.Genome Version')
        if not genome_version and species:
            self.warning('Resolve genome version for sample %s from config file', sample_name)
            return cfg.query('species', species, 'default')
        return genome_version

    @staticmethod
    def _read_program_csv(program_csv):
        all_programs = {}
        if program_csv and path.exists(program_csv):
            with open(program_csv) as open_prog:
                for row in csv.reader(open_prog):
                    all_programs[row[0] + '_version'] = row[1]
        return all_programs

    @staticmethod
    def _read_project_summary_yaml(summary_yaml):
        with open(summary_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
        sample_yaml = full_yaml['samples'][0]
        return path.basename(path.dirname(sample_yaml['dirs']['galaxy'])), sample_yaml['genome_build']

    def _get_bcl2fastq_version(self, run_ids):
        bcl2fastq_versions = set()
        for run_id in run_ids:
            prog_vers_yaml = os.path.join(self.run_folders, run_id, 'program_versions.yaml')
            bcl2fastq_version = None
            if os.path.isfile(prog_vers_yaml):
                with open(prog_vers_yaml, 'r') as open_file:
                    full_yaml = yaml.safe_load(open_file)
                    if 'bcl2fastq' in full_yaml and full_yaml.get('bcl2fastq'):
                        bcl2fastq_version = full_yaml.get('bcl2fastq')
            if bcl2fastq_version:
                bcl2fastq_versions.add(bcl2fastq_version)
            else:
                raise ValueError('Run %s has no bcl2fastq version available in %s' % (run_id, prog_vers_yaml))
        return ', '.join(bcl2fastq_versions)

    @staticmethod
    def _read_program_version_yaml(prog_vers_yaml):
        program_version = {}
        with open(prog_vers_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
            for p in ['bwa', 'gatk', 'samtools', 'samblaster', 'biobambam_sortmapdup']:
                if p in full_yaml:
                    program_version[p + '_version'] = full_yaml.get(p)
        return program_version

    def _get_version(self, sample_name):
        versions = {}
        genome_version = None
        species = self.get_species_from_sample(sample_name)
        sample_source = path.join(self.project_source, sample_name)
        if species == 'Homo sapiens':
            program_csv = find_file(sample_source, 'programs.txt')
            versions.update(self._read_program_csv(program_csv))
            summary_yaml = find_file(sample_source, 'project-summary.yaml')
            if summary_yaml:
                bcbio_version, genome_version = self._read_project_summary_yaml(summary_yaml)
                versions['bcbio_version'] = bcbio_version
        else:
            program_yaml = find_file(sample_source, 'program_versions.yaml')
            versions.update(self._read_program_version_yaml(program_yaml))
        versions['bcl2fastq_version'] = self._get_bcl2fastq_version(
            query_dict(self.sample_info(sample_name), 'data.aggregated.run_ids')
        )
        if 'biobambam_sortmapdup_version' in versions:
            versions['biobambam_or_samblaster'] = 'biobambam'
            versions['biobambam_or_samblaster_version'] = versions.get('biobambam_sortmapdup_version')
        else:
            versions['biobambam_or_samblaster'] = 'samblaster'
            versions['biobambam_or_samblaster_version'] = versions.get('samblaster_version')
        if not genome_version:
            genome_version = self._get_genome_version(sample_name)
        versions['genome_version'] = genome_version
        return versions

    # Sample spcecific functions
    def sample_info(self, sample_id):
        if sample_id not in self._sample_info:
            sample_dict = {}
            sample_data = [s for s in self.sample_data_for_project if s.get('sample_id') == sample_id]
            if sample_data:
                sample_dict['data'] = sample_data[0]
            else:
                return None
            self._sample_info[sample_id] = sample_dict

            sample_status = [s for s in self.sample_status_for_project if s.get('sample_id') == sample_id]
            if sample_status:
                sample_dict['status'] = sample_status[0]
            sample_info = [s for s in self.sample_info_for_project if s.get('sample_id') == sample_id]
            if sample_info:
                sample_dict['info'] = sample_info[0]

            sample_dict['versions'] = self._get_version(sample_id)
        return self._sample_info.get(sample_id)

    def get_fluidx_barcode(self, sample_name):
        return query_dict(self.sample_info(sample_name), 'info.2D Barcode')

    def get_analysis_type_from_sample(self, sample_name):
        return query_dict(self.sample_info(sample_name), 'data.aggregated.most_recent_proc.pipeline_used.name')

    def get_library_workflow_from_sample(self, sample_name):
        if query_dict(self.sample_info(sample_name), 'info.User Prepared Library') == 'Yes':
            return 'User Prepared Library'
        else:
            return self.workflow_alias.get(query_dict(self.sample_info(sample_name), 'info.Prep Workflow'))

    def get_species_from_sample(self, sample_name):
        return query_dict(self.sample_info(sample_name), 'data.species_name')

    def get_required_yield(self, sample_name):
        return int(query_dict(self.sample_info(sample_name), 'info.Required Yield (Gb)'))

    def get_quoted_coverage(self, sample_name):
        return int(query_dict(self.sample_info(sample_name), 'info.Coverage (X)'))

    def get_genome_version(self, sample_name):
        return query_dict(self.sample_info(sample_name), 'versions.genome_version')

    def get_started_date_from_sample(self, sample_name):
        return query_dict(self.sample_info(sample_name), 'status.started_date')

    def get_user_sample_id(self, sample_name):
        return query_dict(self.sample_info(sample_name), 'data.user_sample_id')

    def get_yield_in_gb(self, sample_name):
        return round(query_dict(self.sample_info(sample_name), 'data.aggregated.clean_yield_in_gb'), 1)

    def get_pc_q30(self, sample_name):
        return round(query_dict(self.sample_info(sample_name), 'data.aggregated.clean_pc_q30'), 1)

    def get_average_coverage(self, sample_name):
        return round(query_dict(self.sample_info(sample_name), 'data.coverage.mean'), 1)

    # Project level functions
    @property
    def sample_names_delivered(self):
        return [sample.get('sample_id') for sample in self.sample_data_for_project]

    def _aggregate_per_project(self, func):
        return sorted(set(func(s) for s in self.sample_names_delivered))

    def get_project_species(self):
        return self._aggregate_per_project(self.get_species_from_sample)

    def get_project_library_workflows(self):
        return self._aggregate_per_project(self.get_library_workflow_from_sample)

    def get_project_analysis_types(self):
        return self._aggregate_per_project(self.get_analysis_type_from_sample)

    def get_project_genome_version(self):
        return self._aggregate_per_project(self.get_genome_version)

    def project_size_in_terabytes(self):
        project_size = get_folder_size(self.project_delivery)
        return project_size / 1099511627776.0

    @property
    def project_title(self):
        return self.lims_project.udf.get('Project Title', '')

    @property
    def quote_number(self):
        return self.lims_project.udf.get('Quote No.', '')

    @property
    def enquiry_number(self):
        return self.lims_project.udf.get('Enquiry Number', '')

    @property
    def number_quoted_samples(self):
        return self.lims_project.udf.get('Number of Quoted Samples', '')

    @property
    def customer_name(self):
        name = self.lims_project.researcher.lab.name
        if not name:
            name = self.lims_project.researcher.name
        return name

    @property
    def customer_address_lines(self):
        address_keys = ('Shipment Address Line 1', 'Shipment Address Line 2', 'Shipment Address Line 3',
                        'Shipment Address Line 4', 'Shipment Address Line 5')
        return [
            self.lims_project.udf.get(k)
            for k in address_keys
            if self.lims_project.udf.get(k) and self.lims_project.udf.get(k) != '-'
        ]

    @cached_property
    def authorisations(self):
        processes_from_projects = self.lims.get_processes(type='Data Release Trigger EG 1.0 ST',
                                                          projectname=self.project_name)
        release_data = []
        for i, process in enumerate(processes_from_projects):
            sample_names = [a.samples[0].name for a in process.all_inputs(resolve=True)]
            version = 'v' + str(i + 1)
            if process.udf.get('Is this the final data release for the project?', 'No') == 'Yes':
                version += '-final'
            ncs = process.udf.get('Non-Conformances', '')
            if ncs.lower() in ['na', 'n/a']:
                ncs = ''
            release_data.append({
                'samples': sample_names,
                'version': version,
                'name': cfg.query('delivery', 'signature_name'),
                'role': cfg.query('delivery', 'signature_role'),
                'date': datetime.datetime.strptime(process.date_run, '%Y-%m-%d').strftime('%d %b %y'),
                'id': process.id,
                'NCs': ncs
            })
        return release_data

    @property
    def report_version(self):
        return self.authorisations[-1].get('version')

    def get_bioinformatics_params_for_analysis(self, analysis_type):
        """
        Aggregate the program version for sample of a specific type.
        :param analysis_type: the analysis type requested
        :return: a dictionary containing program/genome versions
        """
        params_for_analysis_tmp = defaultdict(set)
        for sample in self.sample_names_delivered:
            if self.get_analysis_type_from_sample(sample) == analysis_type:
                for k, v in self.sample_info(sample).get('versions').items():
                    params_for_analysis_tmp[k].add(v)
                params_for_analysis_tmp['species_submitted'].add(self.get_species_from_sample(sample))
                params_for_analysis_tmp['adapter1'].add(self.adapter1)
                params_for_analysis_tmp['adapter2'].add(self.adapter2)
        params_for_analysis = {}
        for k in params_for_analysis_tmp:
            params_for_analysis[k] = ', '.join([str(s) for s in params_for_analysis_tmp.get(k)])
        return params_for_analysis

    def get_format_delivered(self):
        formats_delivered = set()
        for analysis_type in self.get_project_analysis_types():
            if analysis_type and analysis_type in ['bcbio', 'variant_calling']:
                formats_delivered.update(['fastq', 'bam', 'vcf'])
            else:
                formats_delivered.add('fastq')
        return formats_delivered

    def abbreviate_species(self, species, nchar=1):
        if species and species not in self.species_abbreviation:
            sp_species = species.split()
            abbreviation = species.split()[0][:1].upper()
            if len(sp_species) > 1:
                abbreviation += ''.join([e[:nchar].lower() for e in species.split()[1:]])
            if abbreviation in self.species_abbreviation.values():
                abbreviation = self.abbreviate_species(species, nchar + 1)
            self.species_abbreviation[species] = abbreviation
        return self.species_abbreviation.get(species)
