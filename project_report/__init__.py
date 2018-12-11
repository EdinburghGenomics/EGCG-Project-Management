import os
import csv
import yaml
import datetime
import matplotlib
from cached_property import cached_property
from os import path, listdir
from jinja2 import Environment, FileSystemLoader
from egcg_core.util import find_file, query_dict
from egcg_core.clarity import connection
from egcg_core.app_logging import logging_default as log_cfg
from egcg_core.rest_communication import get_documents, get_document
from egcg_core.exceptions import EGCGError
import pandas as pd
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.collections as mpcollections

from config import cfg

app_logger = log_cfg.get_logger(__name__)
log_cfg.get_logger('weasyprint', 40)

try:
    from weasyprint import HTML
    from weasyprint.fonts import FontConfiguration
except ImportError:
    HTML = None
    FontConfiguration = None

species_alias = {'Homo sapiens': 'Human', 'Human': 'Human'}


class ProjectReport:
    workflow_alias = {
        'TruSeq Nano DNA Sample Prep': 'TruSeq Nano',
        'TruSeq PCR-Free DNA Sample Prep': 'TruSeq PCR-Free',
        'TruSeq PCR-Free Sample Prep': 'TruSeq PCR-Free',
        'TruSeq DNA PCR-Free Sample Prep': 'TruSeq PCR-Free'
    }

    sample_qc_alias = {'TruSeq Nano': 'sample_qc_nano', 'TruSeq PCR-free': 'sample_qc_pcrfree'}

    def __init__(self, project_name, working_dir=None):
        self.project_name = project_name
        self.working_dir = working_dir or os.getcwd()
        self.project_source = path.join(cfg['delivery']['source'], project_name)
        self.project_delivery = path.join(cfg['delivery']['dest'], project_name)
        self.lims = connection()
        self.params = {
            'project_name': project_name,
            'adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA',
            'adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT'
        }
        self.font_config = FontConfiguration()

    @property
    def eglogo_path(self):
        return 'file://' + os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'EG_logo_blackonwhite_300dpi.png'
        )

    @cached_property
    def project(self):
        return self.lims.get_projects(name=self.project_name)[0]

    @staticmethod
    def sample_status(sample_id):
        return get_document('lims/status/sample_status', match={'sample_id': sample_id})

    @cached_property
    def samples_for_project_lims(self):
        return self.lims.get_samples(projectname=self.project_name)

    @cached_property
    def samples_for_project_restapi(self):
        samples = get_documents('samples', where={'project_id': self.project_name, 'delivered': 'yes'}, all_pages=True)
        if not samples:
            raise EGCGError('No samples found for project %s' % self.project_name)
        return samples

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

    def get_species(self, samples):
        return set(self.get_species_from_sample(s) for s in samples)

    def get_library_workflows(self, samples):
        library_workflows = set()
        for sample in samples:
            library_workflows.add(self.get_library_workflow_from_sample(sample))
        unknown_libraries = library_workflows.difference({'TruSeq Nano', 'TruSeq PCR-Free'})
        if len(unknown_libraries):
            raise ValueError('%s unknown library preparation: %s' % (len(unknown_libraries), unknown_libraries))
        return sorted(library_workflows)

    def get_analysis_type(self, samples):
        analysis_types = set()
        for sample in samples:
            analysis_types.add(self.get_analysis_type_from_sample(sample))
        if len(analysis_types) != 1:
            raise ValueError('%s analysis types used for this project: %s' % (len(analysis_types), analysis_types))
        return analysis_types.pop()

    def project_size_in_terabytes(self):
        project_size = self.get_folder_size(self.project_delivery)
        return project_size / 1099511627776.0

    @staticmethod
    def parse_date(date):
        if not date:
            return 'NA'
        return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d')

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
        all_programs['bcl2fastq'] = '2.17.1.14'
        for p in ['bcl2fastq', 'bcbio', 'bwa', 'gatk', 'samblaster']:
            if p in all_programs:
                self.params[p + '_version'] = all_programs.get(p)

    @staticmethod
    def get_from_project_summary_yaml(summary_yaml):
        with open(summary_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
        sample_yaml = full_yaml['samples'][0]
        return path.basename(path.dirname(sample_yaml['dirs']['galaxy'])), sample_yaml['genome_build']

    def update_from_program_version_yaml(self, prog_vers_yaml):
        with open(prog_vers_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
            for p in ['bcl2fastq', 'bwa', 'gatk', 'samtools', 'samblaster', 'biobambam_sortmapdup']:
                if p in full_yaml:
                    self.params[p + '_version'] = full_yaml.get(p)

    def get_project_info(self):
        species_submitted = set()
        library_workflows = self.get_library_workflows(self.sample_names_delivered)
        for sample in self.sample_names_delivered:
            species = self.get_species_from_sample(sample)
            species_submitted.add(species)
        project_info = (
            ('Project name', self.project_name),
            ('Project title', self.project_title),
            ('Enquiry no', self.enquiry_number),
            ('Quote no', self.quote_number),
            ('Customer name', self.customer_name),
            ('Customer address', self.customer_address_lines),
            ('Number of samples', self.number_quoted_samples),
            ('Number of samples delivered', len(self.samples_for_project_restapi)),
            ('Date samples received', 'Detailed in appendix I'),
            ('Project size', '%.2f terabytes' % self.project_size_in_terabytes()),
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
                ProjectReport.calculate_mean(list_values),
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
        for sample in set(self.sample_names_delivered):
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
                genome_version = self.get_genome_version(sample)

            if genome_version == 'hg38':
                genome_version = 'GRCh38 (with alt, decoy and HLA sequences)'
            genome_versions.add(genome_version)
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

    def yield_vs_coverage_plot(self):
        req_to_metrics = self.get_sample_yield_coverage_metrics()
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

            plot_outfile = path.join(self.working_dir, 'yield%s_cov%s_plot.png' % (req_yield, req_cov))
            plt.savefig(plot_outfile, bbox_inches='tight', pad_inches=0.2)
            list_plots.append({
                'nb_sample': len(df),
                'req_yield': req_yield,
                'req_cov': req_cov,
                'file': 'file://' + os.path.abspath(plot_outfile)
            })
        self.params['yield_cov_chart'] = list_plots

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
                'date': process.date_run,
                'id': process.id,
                'NCs': ncs
            })
        return release_data

    def get_html_template(self):
        template = {'template_base': 'report_base.html',
                    'glossary': [],
                    'charts_template': ['yield_cov_chart']}

        species = self.get_species(self.sample_names_delivered)
        try:
            analysis_type = self.get_analysis_type(self.sample_names_delivered)
        except ValueError as e:
            if len(species) != 1 or species.pop() is not 'Human':
                raise e
        library_workflows = self.get_library_workflows(self.sample_names_delivered)
        if len(species) == 1 and species.pop() == 'Human':
            bioinfo_template = ['bioinformatics_analysis_bcbio']
            formats_template = ['fastq', 'bam', 'vcf']
        elif analysis_type and analysis_type in ['Variant Calling', 'Variant Calling gatk']:
            bioinfo_template = ['bioinformatics_analysis']
            formats_template = ['fastq', 'bam', 'vcf']
        else:
            bioinfo_template = ['bioinformatics_qc']
            formats_template = ['fastq']
        template['bioinformatics_template'] = bioinfo_template
        template['formats_template'] = formats_template
        template['laboratory_template'] = library_workflows + ['sequencing']
        return template

    def generate_report(self, output_format):
        project_file = path.join(self.project_delivery, 'project_%s_report.%s' % (self.project_name, output_format))
        if not HTML:
            raise ImportError('Could not import WeasyPrint - PDF output not available')
        else:
            report_render, pages, full_html = self.get_html_content()
        if output_format == 'html':
            open(project_file, 'w').write(full_html)
        elif HTML:
            report_render.copy(pages).write_pdf(project_file)

    def get_csv_data(self, authorisations):
        header = [
            'Internal ID', 'External ID', 'Date reviewed', 'DNA QC (>1000 ng)', 'Date received',
            'Species', 'Workflow', 'Yield quoted (Gb)', 'Yield provided (Gb)', '% Q30 > 75%',
            'Quoted coverage', 'Provided coverage'
        ]

        def find_sample_release_date_in_auth(sample):
            return [auth.get('date') for auth in authorisations if sample in auth.get('samples')]

        rows = []
        for sample in self.samples_for_project_restapi:
            date_reviewed = find_sample_release_date_in_auth(sample.get('sample_id'))
            internal_sample_name = self.get_fluidx_barcode(sample.get('sample_id')) or sample.get('sample_id')
            row = [
                internal_sample_name,
                sample.get('user_sample_id', 'None'),
                ', '.join(date_reviewed),
                round(self.get_sample_total_dna(sample.get('sample_id')), 1),
                self.parse_date(self.sample_status(sample.get('sample_id')).get('started_date')),
                sample.get('species_name'),
                self.get_library_workflow_from_sample(sample.get('sample_id')),
                self.get_required_yield(sample.get('sample_id')),
                round(query_dict(sample, 'aggregated.clean_yield_in_gb'), 1),
                round(query_dict(sample, 'aggregated.clean_pc_q30'), 1),
                self.get_quoted_coverage(sample.get('sample_id')),
                round(query_dict(sample, 'coverage.mean'), 1)
            ]

            rows.append(row)
        return header, rows

    def get_html_content(self):
        self.yield_vs_coverage_plot()

        self.params['csv_path'] = 'summary_metrics.csv'
        template_dir = path.join(path.dirname(path.abspath(__file__)), 'templates')
        env = Environment(loader=FileSystemLoader(template_dir))
        project_templates = self.get_html_template()
        template1 = env.get_template(project_templates.get('template_base'))
        template2 = env.get_template('csv_base.html')
        self.store_sample_info()
        authorisations = self.get_authorization()
        report = template1.render(
            eglogo=self.eglogo_path,
            authorisations=authorisations,
            project_info=self.get_project_info(),
            project_stats=self.calculate_project_statistsics(),
            project_templates=project_templates,
            params=self.params
        )

        csv_table_headers, csv_table_rows = self.get_csv_data(authorisations)
        appendices = template2.render(
            authorisations=authorisations,
            report_csv_headers=csv_table_headers,
            report_csv_rows=csv_table_rows,
            csv_path=self.params['csv_path'],
            project_id=self.params['project_name'],
        )
        combined_report_html = (report + appendices)
        report_html = HTML(string=report)
        appendices_html = HTML(string=appendices)
        report_render = report_html.render(font_config=self.font_config)
        appendices_render = appendices_html.render(font_config=self.font_config)
        pages = []
        for doc in report_render, appendices_render:
            for p in doc.pages:
                pages.append(p)
        return report_render, pages, combined_report_html

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
