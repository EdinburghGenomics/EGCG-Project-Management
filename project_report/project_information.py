import os
import csv

import matplotlib
import yaml
import datetime
from cached_property import cached_property
from os import path, listdir

from egcg_core.app_logging import AppLogger
from egcg_core.util import find_file, query_dict
from egcg_core.clarity import connection
from egcg_core.rest_communication import get_documents, get_document
from egcg_core.exceptions import EGCGError
import pandas as pd
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.collections as mpcollections
from config import cfg

species_alias = {'Homo sapiens': 'Human', 'Human': 'Human'}


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
    species_abbreviation = {}

    def __init__(self, project_name):
        self.project_name = project_name
        self.run_folders = cfg['sample']['input_dir']
        self.project_source = path.join(cfg['delivery']['source'], project_name)
        self.project_delivery = path.join(cfg['delivery']['dest'], project_name)
        self.lims = connection()
        self.params = {
            'project_name': project_name,
            'adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA',
            'adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT'
        }

    @cached_property
    def project(self):
        return self.lims.get_projects(name=self.project_name)[0]

    @staticmethod
    def sample_status(sample_id):
        return get_document('lims/status/sample_status', match={'sample_id': sample_id, "project_status": "all"})

    @cached_property
    def samples_for_project_lims(self):
        return self.lims.get_samples(projectname=self.project_name)

    @cached_property
    def samples_for_project_restapi(self):
        samples = get_documents('samples', where={'project_id': self.project_name, 'delivered': 'yes'}, all_pages=True)
        if not samples:
            raise EGCGError('No samples found for project %s' % self.project_name)
        return samples

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

    @property
    def sample_names_delivered(self):
        return [sample.get('sample_id') for sample in self.samples_for_project_restapi]

    def get_lims_sample(self, sample_name):
        samples = [s for s in self.samples_for_project_lims if s.name == sample_name]
        if len(samples) == 1:
            return samples[0]
        raise ValueError('%s samples found for %s' % (len(samples), sample_name))

    def get_fluidx_barcode(self, sample_name):
        return self.get_lims_sample(sample_name).udf.get('2D Barcode')

    def get_analysis_type_from_sample(self, sample_name):
        return self.get_lims_sample(sample_name).udf.get('Analysis Type')

    def get_library_workflow_from_sample(self, sample_name):
        if self.get_lims_sample(sample_name).udf.get('User Prepared Library') == 'Yes':
            return 'User Prepared Library'
        else:
            return self.workflow_alias.get(self.get_lims_sample(sample_name).udf.get('Prep Workflow'))

    def get_species_from_sample(self, sample_name):
        s = self.get_lims_sample(sample_name).udf.get('Species')
        return species_alias.get(s, s)

    def get_sample_total_dna(self, sample_name):
        return self.get_lims_sample(sample_name).udf.get('Total DNA (ng)')

    def get_required_yield(self, sample_name):
        return self.get_lims_sample(sample_name).udf.get('Required Yield (Gb)')

    def get_quoted_coverage(self, sample_name):
        return self.get_lims_sample(sample_name).udf.get('Coverage (X)')

    def get_genome_version(self, sample_name):
        s = self.get_lims_sample(sample_name)
        species = self.get_species_from_sample(sample_name)
        genome_version = s.udf.get('Genome Version', None)
        if not genome_version and species:
            return cfg.query('species', species, 'default')
        return genome_version

    def get_species(self):
        return set(self.get_species_from_sample(s) for s in self.sample_names_delivered)

    def get_library_workflows(self):
        library_workflows = set()
        for sample in self.sample_names_delivered:
            library_workflows.add(self.get_library_workflow_from_sample(sample))
        unknown_libraries = library_workflows.difference(set(self.library_abbreviation))
        if len(unknown_libraries):
            raise ValueError('%s unknown library preparation: %s' % (len(unknown_libraries), unknown_libraries))
        return sorted(library_workflows)

    def get_analysis_type(self):
        analysis_types = set()
        for sample in self.sample_names_delivered:
            analysis_types.add(self.get_analysis_type_from_sample(sample))
        return analysis_types.pop()

    def project_size_in_terabytes(self):
        project_size = self.get_folder_size(self.project_delivery)
        return project_size / 1099511627776.0

    @staticmethod
    def parse_date(date):
        if not date:
            return 'NA'
        return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f').strftime('%d %b %y')

    @staticmethod
    def calculate_mean(values):
        return sum(values) / max(len(values), 1)

    @property
    def project_title(self):
        return self.project.udf.get('Project Title', '')

    @property
    def quote_number(self):
        return self.project.udf.get('Quote No.', '')

    @property
    def enquiry_number(self):
        return self.project.udf.get('Enquiry Number', '')

    @property
    def number_quoted_samples(self):
        return self.project.udf.get('Number of Quoted Samples', '')

    @property
    def customer_name(self):
        name = self.project.researcher.lab.name
        if not name:
            name = self.project.researcher.name
        return name

    @property
    def customer_address_lines(self):
        address_keys = ('Shipment Address Line 1', 'Shipment Address Line 2', 'Shipment Address Line 3',
                        'Shipment Address Line 4', 'Shipment Address Line 5')
        return [
            self.project.udf.get(k)
            for k in address_keys
            if self.project.udf.get(k) and self.project.udf.get(k) != '-'
        ]

    def update_from_program_csv(self, program_csv):
        all_programs = {}
        if program_csv and path.exists(program_csv):
            with open(program_csv) as open_prog:
                for row in csv.reader(open_prog):
                    all_programs[row[0]] = row[1]
        # TODO: change the hardcoded version of bcl2fastq
        #all_programs['bcl2fastq'] = '2.17.1.14'
        for p in ['bcl2fastq', 'bcbio', 'bwa', 'gatk', 'samblaster']:
            if p in all_programs:
                self.params[p + '_version'] = all_programs.get(p)

    @staticmethod
    def get_from_project_summary_yaml(summary_yaml):
        with open(summary_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
        sample_yaml = full_yaml['samples'][0]
        return path.basename(path.dirname(sample_yaml['dirs']['galaxy'])), sample_yaml['genome_build']

    def get_bcl2fastq_version(self, run_ids):
        bcl2fastq_versions = set()
        for run_id in run_ids:
            prog_vers_yaml = os.path.join(self.run_folders, run_id, 'program_versions.yaml')
            with open(prog_vers_yaml, 'r') as open_file:
                full_yaml = yaml.safe_load(open_file)
                if 'bcl2fastq' in full_yaml and full_yaml.get('bcl2fastq'):
                    bcl2fastq_versions.add(full_yaml.get('bcl2fastq'))
                else:
                    self.warning('Run %s has no bcl2fastq version: default to v2.17.1.14', run_id)
                    bcl2fastq_versions.add('v2.17.1.14')
        return ', '.join(bcl2fastq_versions)

    def update_from_program_version_yaml(self, prog_vers_yaml):
        with open(prog_vers_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
            for p in ['bwa', 'gatk', 'samtools', 'samblaster', 'biobambam_sortmapdup']:
                if p in full_yaml:
                    self.params[p + '_version'] = full_yaml.get(p)

    def get_project_info(self):
        self.store_sample_info()
        species_submitted = set()
        library_workflows = self.get_library_workflows()
        for sample in self.sample_names_delivered:
            species = self.get_species_from_sample(sample)
            species_submitted.add(species)
        project_info = (
            ('Project name', self.project_name),
            ('Project title', self.project_title),
            ('Enquiry no', self.enquiry_number),
            ('Quote no', self.quote_number),
            ('Customer name', self.customer_name),
            ('Customer address', '\n'.join(self.customer_address_lines)),
            ('Number of samples', self.number_quoted_samples),
            ('Number of samples delivered', len(self.samples_for_project_restapi)),
            ('Date samples received', 'Detailed in appendix I'),
            ('Total download size', '%.2f terabytes' % self.project_size_in_terabytes()),
            ('Laboratory protocol', ', '.join(library_workflows)),
            ('Submitted species', ', '.join(list(species_submitted))),
            ('Genome version', self.params['genome_version'])
        )

        return project_info

    @staticmethod
    def min_mean_max(list_values):
        if list_values:
            return 'min: %.1f, avg: %.1f, max: %.1f' % (
                min(list_values),
                ProjectReportInformation.calculate_mean(list_values),
                max(list_values)
            )
        else:
            return 'min: 0, mean: 0, max: 0'

    def calculate_project_statistsics(self):
        samples = self.samples_for_project_restapi
        sample_data_mapping = {
            'Yield per sample (Gb)': 'aggregated.clean_yield_in_gb',
            'Coverage per sample': 'coverage.mean',
            '% Duplicate reads': 'aggregated.pc_duplicate_reads',
            '% Reads mapped': 'aggregated.pc_mapped_reads',
            '% Q30': 'aggregated.clean_pc_q30',
        }
        headers = ['Yield per sample (Gb)', '% Q30', 'Coverage per sample', '% Reads mapped', '% Duplicate reads']
        project_stats = []
        for field in headers:
            project_stats.append((field, self.min_mean_max(
                [query_dict(sample, sample_data_mapping[field]) for sample in samples]
            )))
        return project_stats

    def store_sample_info(self):
        genome_versions = set()
        species_submitted = set()
        for sample_data in self.samples_for_project_restapi:
            sample = sample_data.get('sample_id')
            species = self.get_species_from_sample(sample)
            species_submitted.add(species)
            genome_version = None
            sample_source = path.join(self.project_source, sample)
            if self.get_species_from_sample(sample) == 'Human':
                program_csv = find_file(sample_source, 'programs.txt')
                self.update_from_program_csv(program_csv)
                summary_yaml = find_file(sample_source, 'project-summary.yaml')
                if summary_yaml:
                    bcbio_version, genome_version = self.get_from_project_summary_yaml(summary_yaml)
                    self.params['bcbio_version'] = bcbio_version
            else:
                program_yaml = find_file(sample_source, 'program_versions.yaml')
                self.update_from_program_version_yaml(program_yaml)

            if not genome_version:
                self.warning('Resolve genome version for sample %s from config file', sample)
                genome_version = self.get_genome_version(sample)

            if genome_version == 'hg38':
                genome_version = 'GRCh38 (with alt, decoy and HLA sequences)'
            genome_versions.add(genome_version)

            self.params['bcl2fastq_version'] = self.get_bcl2fastq_version(
                query_dict(sample_data, 'aggregated.run_ids')
            )

        if 'biobambam_sortmapdup_version' in self.params:
            self.params['biobambam_or_samblaster'] = 'biobambam'
            self.params['biobambam_or_samblaster_version'] = self.params['biobambam_sortmapdup_version']
        else:
            self.params['biobambam_or_samblaster'] = 'samblaster'
            self.params['biobambam_or_samblaster_version'] = self.params['samblaster_version']

        self.params['genome_version'] = ', '.join(genome_versions)
        self.params['species_submitted'] = ', '.join(species_submitted)

    def get_sample_yield_coverage_metrics(self):
        req_to_metrics = {}
        for sample in self.samples_for_project_restapi:
            req = (self.get_required_yield(sample.get('sample_id')), self.get_quoted_coverage(sample.get('sample_id')))
            if req not in req_to_metrics:
                req_to_metrics[req] = {'samples': [], 'clean_yield': [], 'coverage': []}
            all_yield_metrics = [sample.get('sample_id'),
                                 query_dict(sample, 'aggregated.clean_yield_in_gb'),
                                 query_dict(sample, 'coverage.mean')]
            if None not in all_yield_metrics:
                req_to_metrics[req]['samples'].append(all_yield_metrics[0])
                req_to_metrics[req]['clean_yield'].append(all_yield_metrics[1])
                req_to_metrics[req]['coverage'].append(all_yield_metrics[2])
        return req_to_metrics

    def get_authorization(self):
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

    def get_sample_data_in_tables(self, authorisations):
        tables = {}
        header = [
            'User ID', 'Internal ID', 'Date received', 'Date reviewed', 'Species', 'Library prep.'
        ]

        def find_sample_release_date_in_auth(sample):
            return [auth.get('date') for auth in authorisations if sample in auth.get('samples')]

        rows = []

        library_descriptions = set()
        species_abbreviations = set()
        for sample in self.samples_for_project_restapi:
            date_reviewed = find_sample_release_date_in_auth(sample.get('sample_id'))
            internal_sample_name = self.get_fluidx_barcode(sample.get('sample_id')) or sample.get('sample_id')
            library = self.get_library_workflow_from_sample(sample.get('sample_id'))
            library_descriptions.add('%s: %s' % (self.library_abbreviation.get(library), library))
            species = sample.get('species_name')
            species_abbreviations.add('%s: %s' %(self.abbreviate_species(species), species))
            row = [
                sample.get('user_sample_id', 'None'),
                internal_sample_name,
                self.parse_date(self.sample_status(sample.get('sample_id')).get('started_date')),
                ', '.join(date_reviewed),
                self.abbreviate_species(species),
                self.library_abbreviation.get(library)
            ]

            rows.append(row)
        tables['appendix I'] = {
            'header': header, 'rows': rows,
            'footer': [', '.join(sorted(library_descriptions)), ', '.join(sorted(species_abbreviations))]
        }
        header = [
            'User ID', 'Yield quoted (Gb)', 'Yield provided (Gb)', '% Q30 > 75%', 'Quoted coverage', 'Provided coverage'
        ]

        rows = []
        for sample in self.samples_for_project_restapi:
            row = [
                sample.get('user_sample_id', 'None'),
                self.get_required_yield(sample.get('sample_id')),
                round(query_dict(sample, 'aggregated.clean_yield_in_gb'), 1),
                round(query_dict(sample, 'aggregated.clean_pc_q30'), 1),
                self.get_quoted_coverage(sample.get('sample_id')),
                round(query_dict(sample, 'coverage.mean'), 1)
            ]

            rows.append(row)
        tables['appendix II'] = {'header': header, 'rows': rows}

        return tables

    def get_library_prep_analysis_types_and_format(self):
        species = self.get_species()
        try:
            analysis_type = self.get_analysis_type()
        except ValueError as e:
            if len(species) != 1 or species.pop() is not 'Human':
                raise e
        library_preparations = self.get_library_workflows()
        if len(species) == 1 and species.pop() == 'Human':
            bioinfo_analysis_types = ['bioinformatics_analysis_bcbio']
            formats_delivered = ['fastq', 'bam', 'vcf']
        elif analysis_type and analysis_type in ['Variant Calling', 'Variant Calling gatk']:
            bioinfo_analysis_types = ['bioinformatics_analysis']
            formats_delivered = ['fastq', 'bam', 'vcf']
        else:
            bioinfo_analysis_types = ['bioinformatics_qc']
            formats_delivered = ['fastq']

        return library_preparations, bioinfo_analysis_types, formats_delivered

    @classmethod
    def get_folder_size(cls, folder):
        total_size = path.getsize(folder)
        for item in listdir(folder):
            itempath = path.join(folder, item)
            if path.isfile(itempath):
                total_size += path.getsize(itempath)
            elif path.isdir(itempath):
                total_size += cls.get_folder_size(itempath)
        return total_size


def yield_vs_coverage_plot(project_information, working_dir):
    req_to_metrics = project_information.get_sample_yield_coverage_metrics()
    list_plots = []
    for req in req_to_metrics:
        df = pd.DataFrame(req_to_metrics[req])
        req_yield, req_cov = req
        max_x = max(df['clean_yield']) + .1 * max(df['clean_yield'])
        max_y = max(df['coverage']) + .1 * max(df['coverage'])
        min_x = min(df['clean_yield']) - .1 * max(df['clean_yield'])
        min_y = min(df['coverage']) - .1 * max(df['coverage'])

        min_x = min((min_x, req_yield - .1 * req_yield))
        min_y = min((min_y, req_cov - .1 * req_cov))

        plt.figure(figsize=(10, 5))
        df.plot(kind='scatter', x='clean_yield', y='coverage')

        plt.xlim(min_x, max_x)
        plt.ylim(min_y, max_y)
        plt.xlabel('Delivered yield (Gb)')
        plt.ylabel('Coverage (X)')

        xrange1 = [(0, req_yield)]
        xrange2 = [(req_yield, max_x)]
        yrange1 = (0, req_cov)
        yrange2 = (req_cov, max_y)

        c1 = mpcollections.BrokenBarHCollection(xrange1, yrange1, facecolor='red', alpha=0.2)
        c2 = mpcollections.BrokenBarHCollection(xrange1, yrange2, facecolor='yellow', alpha=0.2)
        c3 = mpcollections.BrokenBarHCollection(xrange2, yrange1, facecolor='yellow', alpha=0.2)
        c4 = mpcollections.BrokenBarHCollection(xrange2, yrange2, facecolor='green', alpha=0.2)

        ax = plt.gca()
        ax.add_collection(c1)
        ax.add_collection(c2)
        ax.add_collection(c3)
        ax.add_collection(c4)

        plot_outfile = path.join(working_dir, 'yield%s_cov%s_plot.png' % (req_yield, req_cov))
        plt.savefig(plot_outfile, bbox_inches='tight', pad_inches=0.2)
        list_plots.append({
            'nb_sample': len(df),
            'req_yield': req_yield,
            'req_cov': req_cov,
            'file': os.path.abspath(plot_outfile)
        })
    return list_plots
