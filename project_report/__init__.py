import re
import os
import csv
import yaml
import matplotlib
matplotlib.use('Agg')
import pandas as pd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from os import path, listdir
from collections import OrderedDict
from jinja2 import Environment, FileSystemLoader
from egcg_core.util import find_file
from egcg_core.clarity import connection, get_genome_version
from egcg_core.app_logging import logging_default as log_cfg
from config import cfg
from egcg_core.rest_communication import get_documents
from egcg_core.exceptions import EGCGError

app_logger = log_cfg.get_logger(__name__)
log_cfg.get_logger('weasyprint', 40)

try:
    from weasyprint import HTML
except ImportError:
    HTML = None

species_alias = {
    'Homo sapiens': 'Human',
    'Human': 'Human'
}

class ProjectReport:
    _lims_samples_for_project = None
    _database_samples_for_project = None
    _project = None

    def __init__(self, project_name):
        self.project_name = project_name
        self.project_source = path.join(cfg.query('sample', 'delivery_source'), project_name)
        self.project_delivery = path.join(cfg.query('sample', 'delivery_dest'), project_name)
        self.lims = connection()
        self.params = {
            'project_name': project_name,
            'adapter1': 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCA',
            'adapter2': 'AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT'
        }

    def facility_and_customer_details(self):
        facility_customer_info = {}
        facility_customer_info['egcg address'] = '\n'.join(['Edinburgh Genomics',
                                                            'The Roslin Institute',
                                                            'The University of Edinburgh',
                                                            'Easter Bush Campus',
                                                            'EH25 9RG'])

    @property
    def project(self):
        if self._project is None:
            self._project = self.lims.get_projects(name=self.project_name)[0]
        return self._project

    @property
    def samples_for_project_lims(self):
        if self._lims_samples_for_project is None:
            self._lims_samples_for_project = self.lims.get_samples(projectname=self.project_name)
        return self._lims_samples_for_project

    @property
    def samples_for_project_restapi(self):
        if self._database_samples_for_project is None:
            self._database_samples_for_project = get_documents('aggregate/samples', match={"project_id": self.project_name}, paginate=False)
            if not self._database_samples_for_project:
                raise EGCGError('No samples found for project %s' % (self.project_name))
        return self._database_samples_for_project

    def get_sample(self, sample_name):
        samples = [s for s in self.samples_for_project_lims if s.name == sample_name]
        if len(samples) == 1:
            return samples[0]
        raise ValueError('%s samples found for %s' % (len(samples), sample_name))

    def get_all_sample_names(self, modify_names=False):
        if modify_names:
            return [re.sub(r'[: ]', '_', s.name) for s in self.samples_for_project_lims]
        return [s.name for s in self.samples_for_project_lims]

    def get_samples_delivered(self):
        sample_yields = [s.get('clean_yield_in_gb') for s in self.samples_for_project_restapi if s.get('clean_yield_in_gb')]
        samples_in_project = len(sample_yields)
        return samples_in_project

    def get_library_workflow_from_sample(self, sample_name):
        return self.get_sample(sample_name).udf.get('Prep Workflow')

    def get_report_type_from_sample(self, sample_name):
        s = self.get_sample(sample_name).udf.get('Species')
        return species_alias.get(s, s)

    def get_species(self, samples):
        species = set()
        for sample in samples:
            species.add(self.get_report_type_from_sample(sample))
        return species

    def get_species_found(self, sample):
        sample_contamination = sample.get('species_contamination', {}).get('contaminant_unique_mapped')
        if sample_contamination:
            species_found = [s for s in sample_contamination if sample_contamination[s] > 500]
            return species_found
        return None

    def get_library_workflow(self, samples):
        library_workflow = set()
        for sample in samples:
            library_workflow.add(self.get_library_workflow_from_sample(sample))
        if len(library_workflow) != 1:
            raise ValueError('%s workflows used for this project: %s' % (len(library_workflow), library_workflow))
        library_workflow = library_workflow.pop()
        return library_workflow

    def project_size_in_terabytes(self):
        project_size = self.get_folder_size(self.project_delivery)
        return (project_size/1000000000000.0)

    def calculate_mean(self, values):
        return (sum(values)/max(len(values), 1))

    @property
    def project_title(self):
        return self.project.udf.get('Project Title', '')

    @property
    def quote_number(self):
        return self.project.udf.get('Quote No.', '')

    @property
    def enquiry_number(self):
        return self.project.udf.get('Enquiry Number', '')


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

    def update_from_project_summary_yaml(self, summary_yaml):
        with open(summary_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
        sample_yaml = full_yaml['samples'][0]
        self.params['bcbio_version'] = path.basename(path.dirname(sample_yaml['dirs']['galaxy']))
        if sample_yaml['genome_build'] == 'hg38':
            self.params['genome_version'] = 'GRCh38 (with alt, decoy and HLA sequences)'

    @staticmethod
    def read_metrics_csv(metrics_csv):
        samples_to_info = {}
        with open(metrics_csv) as open_metrics:
            reader = csv.DictReader(open_metrics, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                samples_to_info[row['Sample Id']] = row
        return samples_to_info

    def get_project_info(self):
        sample_names = self.get_all_sample_names()
        genome_versions = set()
        species_submitted = set()
        library_workflow = self.get_library_workflow(self.get_all_sample_names())
        self.params['library_workflow'] = library_workflow

        for sample in sample_names:
            lims_sample = self.get_sample(sample)
            species = lims_sample.udf.get('Species')
            genome_version = get_genome_version(sample, species=species)
            species_submitted.add(species)
            genome_versions.add(genome_version)

        project_info = (
            ('Project name', self.project_name),
            ('Project title', self.project_title),
            ('Enquiry no', self.enquiry_number),
            ('Quote no', self.quote_number),
            ('Number of samples', len(sample_names)),
            ('Number of samples delivered', self.get_samples_delivered()),
            ('Project size', '%.2f terabytes' % self.project_size_in_terabytes()),
            ('Laboratory protocol', library_workflow),
            ('Submitted species', ', '.join(list(species_submitted))),
            ('Genome version', ', '.join(list(genome_versions)))
                        )
        return project_info


    def get_list_of_sample_fields(self, samples, field, subfields=None):
        if subfields:
            sample_fields = [s.get(field, {}) for s in samples if s.get(field)]
            for f in subfields:
                sample_fields = [s.get(f, {}) for s in sample_fields]
            return [s for s in sample_fields if s]
        sample_fields = [s.get(field) for s in samples if s.get(field)]
        return sample_fields

    def gather_project_data(self):
        samples = self.samples_for_project_restapi
        project_sample_data = {'clean_yield':
                             {'key': 'clean_yield_in_gb',
                              'subfields': None},
                         'coverage_per_sample':
                             {'key': 'coverage',
                              'subfields': ['mean']},
                         'pc_duplicate_reads':
                             {'key': 'pc_duplicate_reads',
                              'subfields': None},
                         'evenness':
                             {'key': 'evenness',
                              'subfields': None},
                         'freemix':
                             {'key': 'sample_contamination',
                              'subfields': 'freemix'},
                         'pc_properly_mapped_reads':
                             {'key': 'pc_properly_mapped_reads',
                              'subfields': None},
                         'clean_pc_q30':
                             {'key': 'clean_pc_q30',
                              'subfields': None},
                         'mean_bases_covered_at_15X':
                             {'key': 'coverage',
                              'subfields': ['bases_at_coverage', 'bases_at_15X']}}

        for field in project_sample_data:
            project_sample_data[field]['values'] = self.get_list_of_sample_fields(samples,
                                                                                  project_sample_data[field]['key'],
                                                                                  subfields=project_sample_data[field]['subfields'])
        return project_sample_data

    def calculate_project_statistsics(self):
        p = self.gather_project_data()
        project_stats = OrderedDict()
        project_stats['Total yield (Gb)'] = '%.2f' % sum(p['clean_yield']['values'])
        project_stats['Mean yield per sample (Gb)'] = '%.1f' % (self.calculate_mean(p['clean_yield']['values']))
        project_stats['Mean coverage per sample'] = '%.2f' % (self.calculate_mean(p['coverage_per_sample']['values']))
        project_stats['Mean % duplicate reads'] = (round(self.calculate_mean(p['pc_duplicate_reads']['values']), 2))
        project_stats['Mean evenness'] = (round(self.calculate_mean(p['evenness']['values']), 2))
        project_stats['Maximum freemix value'] = round(max(p['freemix']['values'], default=0), 2)
        project_stats['Mean % Reads Mapped to Reference Genome'] = round(self.calculate_mean(p['pc_properly_mapped_reads']['values']), 2)
        project_stats['Mean % Q30'] = (round(self.calculate_mean(p['clean_pc_q30']['values']), 2))
        project_stats['Mean bases covered at 15X'] = (round(self.calculate_mean(p['mean_bases_covered_at_15X']['values']), 2))
        return project_stats

    def get_sample_info(self):
        modified_samples = self.get_all_sample_names(modify_names=True)
        for sample in set(modified_samples):
            sample_source = path.join(self.project_source, sample)
            program_csv = find_file(sample_source, 'programs.txt')
            if not program_csv:
                program_csv = find_file(sample_source, '.qc', 'programs.txt')
            self.update_from_program_csv(program_csv)
            summary_yaml = find_file(sample_source, 'project-summary.yaml')
            if not summary_yaml:
                summary_yaml = find_file(sample_source, '.qc', 'project-summary.yaml')
            if summary_yaml:
                self.update_from_project_summary_yaml(summary_yaml)
        get_project_stats = self.calculate_project_statistsics()
        project_stats = []
        for stat in get_project_stats:
            if get_project_stats[stat]:
                project_stats.append((stat, get_project_stats[stat]))
        return project_stats

    def get_sample_yield_metrics(self):
        yield_metrics = {'samples': [], 'clean_yield': [], 'clean_yield_Q30': []}
        for sample in self.samples_for_project_restapi:

            all_yield_metrics = [sample.get('sample_id'),
                                 sample.get('clean_yield_in_gb'),
                                 sample.get('clean_yield_q30')]
            if not None in all_yield_metrics:
                yield_metrics['samples'].append(all_yield_metrics[0])
                yield_metrics['clean_yield'].append(all_yield_metrics[1])
                yield_metrics['clean_yield_Q30'].append(all_yield_metrics[2])
        return yield_metrics

    def get_pc_statistics(self):
        pc_statistics = {'pc_duplicate_reads': [], 'pc_properly_mapped_reads': [], 'pc_pass_filter': [], 'samples': []}
        for sample in self.samples_for_project_restapi:
            all_pc_statistics = [sample.get('pc_duplicate_reads'),
                                 sample.get('pc_properly_mapped_reads'),
                                 sample.get('pc_pass_filter'),
                                 sample.get('sample_id')]
            if not None in all_pc_statistics:
                pc_statistics['pc_duplicate_reads'].append(all_pc_statistics[0])
                pc_statistics['pc_properly_mapped_reads'].append(all_pc_statistics[1])
                pc_statistics['pc_pass_filter'].append(all_pc_statistics[2])
                pc_statistics['samples'].append(all_pc_statistics[3])
        return pc_statistics








    def yield_plot(self, sample_labels=False):
        yield_plot_outfile = path.join(self.project_source, 'yield_plot.png')
        sample_yields = self.get_sample_yield_metrics()
        df = pd.DataFrame(sample_yields)
        indices = np.arange(len(df))
        plt.figure(figsize=(10, 5))
        if sample_labels:
            plt.xticks([i for i in range(len(df))], list((df['samples'])), rotation=-80)
        else:
            plt.xticks([])
        plt.xlim([-1, max(indices) + 1])
        plt.bar(indices, df['clean_yield'], width=0.8, align='center', color='gainsboro')
        plt.bar(indices, df['clean_yield_Q30'], width=0.2, align='center', color='lightskyblue')
        plt.ylabel('Gigabases')
        blue_patch = mpatches.Patch(color='gainsboro', label='Yield (Gb)')
        green_patch = mpatches.Patch(color='lightskyblue', label='Yield Q30 (Gb)')
        lgd = plt.legend(handles=[blue_patch, green_patch], loc='upper center', bbox_to_anchor=(0.5, 1.25))
        plt.savefig(yield_plot_outfile, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.2)
        yield_plot_outfile = 'file://' + os.path.abspath(yield_plot_outfile)
        self.params['yield_chart'] = yield_plot_outfile










    def qc_plot(self, sample_labels=False):
        qc_plot_outfile = path.join(self.project_source, 'qc_plot.png')
        pc_statistics = self.get_pc_statistics()
        df = pd.DataFrame(pc_statistics)
        indices = np.arange(len(df))
        plt.figure(figsize=(10, 5))
        if sample_labels:
            plt.xticks([i for i in range(len(df))], list((df['samples'])), rotation=-80)
        else:
            plt.xticks([])
        plt.xlim([-1, max(indices) + 1])
        plt.bar(indices, df['pc_properly_mapped_reads'], width=1, align='center', color='gainsboro')
        plt.bar(indices, df['pc_duplicate_reads'], width=0.4, align='center', color='mediumaquamarine')
        blue_patch = mpatches.Patch(color='gainsboro', label='% Paired Reads Aligned to Reference Genome')
        green_patch = mpatches.Patch(color='mediumaquamarine', label='% Duplicate Reads')
        lgd = plt.legend(handles=[blue_patch, green_patch], loc='upper center', bbox_to_anchor=(0.5, 1.25))
        plt.ylabel('% of Reads')
        plt.savefig(qc_plot_outfile, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.2)
        qc_plot_outfile = 'file://' + os.path.abspath(qc_plot_outfile)
        self.params['mapping_duplicates_chart'] = qc_plot_outfile


    def method_fields(self):
        fields = {'sample_qc': {'title': 'Sample QC',
                                'headings': ['Method', 'QC', 'Critical equipment', 'Pass criteria'],
                                'rows': [('Sample picogreen', 'gDNA quantified against Lambda DNA standards', 'Hamilton robot', '> 1000ng'),
                                         ('Fragment analyzer QC', 'Quality of gDNA determined', 'Fragment analyzer', 'Quality score > 5'),
                                         ('gDNA QC Review Process', 'N/A', 'N/A', 'N/A')]},
                  'library_prep': {'title': 'Library preparation',
                                   'headings': ['Method', 'Purpose', 'Critical equipment'],
                                   'rows': [('Sequencing plate preparation', 'Samples normalised to fall within 5-40ng/ul', 'Hamilton robot'),
                                            ('Nano DNA', 'Libraries prepared using Illumina SeqLab %s' % (self.params['library_workflow']), 'Hamilton, Covaris LE220, Gemini Spectramax XP, Hybex incubators, BioRad C1000/S1000 thermal cycler')]},
                  'library_qc': {'title': 'Library QC',
                                 'headings': ['Method', 'QC', 'Critical equipment', 'Pass criteria'],
                                 'rows': [('Library QC as part of Nano DNA', 'Insert size evaluated', 'Caliper GX Touch', 'Fragment sizes fall between 530bp and 730bp'),
                                          ('Library QC as part of Nano DNA', 'Library concentration calculated', 'Roche Lightcycler', 'Concentration between 5.5nM and 40nM')]},
                  'sequencing': {'title': 'Sequencing',
                                 'headings': ['Method', 'Steps', 'Critical equipment'],
                                 'rows': [('Clustering and sequencing of libraries as part of %s' % (self.params['library_workflow']), 'Clustering', 'cBot2'),
                                          ('Clustering and Sequencing of libraries as part of %s' % (self.params['library_workflow']), 'Sequencing', 'HiSeqX')]},
                  'bioinformatics': {'title': 'Bioinformatics analysis',
                                     'headings': ['Method', 'Software', 'Version'],
                                     'rows': [('Demultiplexing', 'bcl2fastq', self.params['bcl2fastq_version']),
                                      ('Alignment', 'bwa mem', self.params['bwa_version']),
                                      ('Mark duplicates', 'samblaster', self.params['samblaster_version']),
                                      ('Indel realignment', 'GATK IndelRealigner', self.params['gatk_version']),
                                      ('Base recalibration', 'GATK BaseRecalibrator', self.params['gatk_version']),
                                      ('Genotype likelihood calculation', 'GATK HaplotypeCaller', self.params['gatk_version'])]}
                  }
        return fields

    def get_html_template(self):
        template = {'template_base': 'report_base.html',
                    'bioinformatics_template': ['bioinformatics_table'],
                    'formats_template': ['fastq', 'bam', 'vcf'],
                    'charts_template': ['yield_chart', 'mapping_duplicates_chart'],
                    'laboratory_template': ['sample_qc_table', 'library_prep_table', 'library_qc_table', 'sequencing_table']}
        return template

    def generate_report(self, output_format):
        project_file = path.join(self.project_delivery, 'project_%s_report.%s' % (self.project_name, output_format))
        report_render, pages, full_html = self.get_html_content()
        if output_format == 'html':
            open(project_file, 'w').write(full_html)
        else:
            report_render.copy(pages).write_pdf(project_file)

    def get_csv_data(self):
        header = ['Internal ID',
                    'External ID',
                    'Species found',
                    'Workflow',
                    'Clean yield (Gb)',
                    'Clean %Q30',
                    '% Duplicate reads',
                    '% Properly mapped reads',
                    '% Pass filter',
                    'Median coverage']
        rows = []
        for sample in self.samples_for_project_restapi:
            row = [
                sample.get('sample_id'),
                sample.get('user_sample_id', 'None'),
                self.get_species_found(sample),
                self.get_library_workflow_from_sample(sample.get('sample_id', 'None')),
                round(sample.get('clean_yield_in_gb', 'None'), 2),
                round(sample.get('clean_pc_q30', 'None'), 2),
                round(sample.get('pc_duplicate_reads', 'None'), 2),
                round(sample.get('pc_properly_mapped_reads', 'None'), 2),
                sample.get('pc_pass_filter', 'None'),
                sample.get('median_coverage', 'None')]

            rows.append(row)
        return (header, rows)

    def write_csv_file(self):
        csv_file = path.join(self.project_delivery, 'project_data.csv')
        header, rows = self.get_csv_data()
        with open(csv_file, 'w') as outfile:
            writer = csv.writer(outfile, delimiter='\t')
            writer.writerow(header)
            for row in rows:
                writer.writerow(row)
        return csv_file

    def get_html_content(self):
        sample_labels = False
        if not self.get_all_sample_names():
            raise EGCGError('No samples found for project %s ' % (self.project_name))
        if len(self.get_all_sample_names()) < 35:
            sample_labels = True
        self.params['csv_path'] = self.write_csv_file()
        template_dir = path.join(path.dirname(path.abspath(__file__)), 'templates')
        env = Environment(loader=FileSystemLoader(template_dir))
        project_templates = self.get_html_template()
        template1 = env.get_template(project_templates.get('template_base'))
        template2 = env.get_template('csv_base.html')
        self.yield_plot(sample_labels=sample_labels)
        self.qc_plot(sample_labels=sample_labels)
        report = template1.render(
                                   project_info=self.get_project_info(),
                                   project_stats=self.get_sample_info(),
                                   project_templates=project_templates,
                                   params=self.params,
                                   quote_number=self.quote_number,
                                   method_fields=self.method_fields()
                                    )

        csv_table_headers, csv_table_rows = self.get_csv_data()
        csv = template2.render(report_csv_headers=csv_table_headers,
                               report_csv_rows=csv_table_rows)
        combined_report_html = (report + csv)
        report_html = HTML(string=report)
        csv_html = HTML(string=csv)
        report_render = report_html.render()
        csv_render = csv_html.render()
        pages = []
        for doc in report_render, csv_render:
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
