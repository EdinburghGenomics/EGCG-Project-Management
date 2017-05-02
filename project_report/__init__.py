import re
import csv
import yaml
import matplotlib
matplotlib.use('Agg')
import pandas as pd
import seaborn as sns
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from os import path, listdir
from collections import OrderedDict
from jinja2 import Environment, FileSystemLoader
from egcg_core.util import find_file
from egcg_core.clarity import connection
from egcg_core.app_logging import logging_default as log_cfg
from config import cfg
from egcg_core.rest_communication import get_documents
from egcg_core.exceptions import EGCGError

app_logger = log_cfg.get_logger(__name__)
log_cfg.get_logger('weasyprint', 40)

try:
    from weasyprint import HTML
except ImportError:
    app_logger.info('WeasyPrint is not installed. PDF output will be unavailable')
    HTML = None

species_alias = {
    'Homo sapiens': 'Human',
    'Human': 'Human'
}


class ProjectReport:
    _lims_samples_for_project = None
    _database_samples_for_project = None
    template_alias = {
        'TruSeq Nano DNA Sample Prep': 'truseq_nano',
        None: 'truseq_nano',
        'TruSeq PCR-Free DNA Sample Prep': 'truseq_pcrfree',
        'TruSeq PCR-Free Sample Prep': 'truseq_pcrfree',
        'TruSeq DNA PCR-Free Sample Prep': 'truseq_pcrfree'
    }

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

    def get_project_info(self):
        project = self.lims.get_projects(name=self.project_name)[0]
        number_of_samples = len(self.get_all_sample_names(modify_names=True))
        project_size = self.get_folder_size(self.project_delivery)
        samples_in_project = self.get_samples_delivered()
        return (
            ('Project name:', self.project_name),
            ('Project title:', project.udf.get('Project Title', '')),
            ('Enquiry no:', project.udf.get('Enquiry Number', '')),
            ('Quote no:', project.udf.get('Quote No.', '')),
            ('Number of Samples', number_of_samples),
            ('Number of Samples Delivered', samples_in_project),
            ('Project Size', '%.2f Terabytes' % (project_size/1000000000000.0)),
            ('Laboratory Protocol', self.get_library_workflow(self.get_all_sample_names()))
        )

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


    def get_project_stats(self):
        sample_yields = [s.get('clean_yield_in_gb') for s in self._database_samples_for_project if s.get('clean_yield_in_gb')]
        coverage_per_sample = [s.get('coverage', {}).get('mean') for s in self._database_samples_for_project if s.get('coverage')]
        pc_duplicate_reads = [s.get('pc_duplicate_reads') for s in self._database_samples_for_project if s.get('pc_duplicate_reads')]
        evenness = [s.get('evenness') for s in self._database_samples_for_project if s.get('evenness')]
        freemix = [s.get('freemix') for s in self._database_samples_for_project if s.get('freemix')]
        pc_properly_mapped_reads = [s.get('pc_properly_mapped_reads') for s in self._database_samples_for_project if s.get('pc_properly_mapped_reads')]
        clean_pc_q30 = [s.get('clean_pc_q30') for s in self._database_samples_for_project if s.get('clean_pc_q30')]
        mean_bases_covered_at_15X = [s.get('coverage_statistics', {})
                                         .get('bases_at_coverage', {})
                                         .get('bases_at_15X')
                                     for s in self._database_samples_for_project if s.get('coverage_statistics')]

        project_stats = OrderedDict()

        if sample_yields:
            project_stats['Total yield (Gb):'] = '%.2f' % sum(sample_yields)
            project_stats['Mean yield (Gb):'] = '%.1f' % (sum(sample_yields)/max(len(sample_yields), 1))
        if coverage_per_sample:
            project_stats['Mean coverage per sample:'] = '%.2f' % (sum(coverage_per_sample)/max(len(coverage_per_sample), 1))
        if pc_duplicate_reads:
            project_stats['Mean % duplicate reads:'] = round(sum(pc_duplicate_reads)/len(pc_duplicate_reads), 2)
        if evenness:
            project_stats['Mean evenness:'] = round(sum(evenness)/len(evenness), 2)
        if freemix:
            project_stats['Maximum freemix value:'] = round(max(freemix), 2)
        if pc_properly_mapped_reads:
            project_stats['Mean % Reads Mapped to Reference Genome:'] = round(sum(pc_properly_mapped_reads)/len(pc_properly_mapped_reads), 2)
        if clean_pc_q30:
            project_stats['Mean % Q30:'] = round(sum(clean_pc_q30)/len(clean_pc_q30), 2)
        if mean_bases_covered_at_15X:
            project_stats['Mean bases covered at 15X:'] = round(sum(mean_bases_covered_at_15X)/len(mean_bases_covered_at_15X), 2)

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
        get_project_stats = self.get_project_stats()
        project_stats = []
        for stat in get_project_stats:
            if get_project_stats[stat]:
                project_stats.append((stat, get_project_stats[stat]))
        return project_stats

    def contamination_chart_data(self):
        sample_contamination = {'number_mapped_to_nonfocal': [], 'number_mapped_to_focal': []}
        for sample in self._database_samples_for_project:
            c = sample.get('species_contamination')
            focal_species = sample.get('species_name')
            number_mapped_to_focal = c.get('total_reads_mapped') * (100 - (c.get('percent_unmapped_focal') - c.get('percent_unmapped'))) / 100
            contaminant_unique_mapped = c.get('contaminant_unique_mapped', {})
            number_mapped_to_nonfocal = 0
            for species in contaminant_unique_mapped:
                if not species == focal_species:
                    number_mapped_to_nonfocal += contaminant_unique_mapped[species]
            sample_contamination['number_mapped_to_nonfocal'].append(number_mapped_to_nonfocal)
            sample_contamination['number_mapped_to_focal'].append(number_mapped_to_focal)
        return sample_contamination

    def get_bamfile_reads_for_project_samples(self):
        bamfile_reads = {}
        for sample in self._database_samples_for_project:
            s = sample.get('sample_id')
            b = sample.get('bam_file_reads')
            bamfile_reads[s] = b
        return bamfile_reads

    def get_sample_yield_metrics(self):
        yield_metrics = {'samples': [], 'clean_yield': [], 'clean_yield_Q30': []}
        for sample in self._database_samples_for_project:
            sample_id = sample.get('sample_id')
            clean_yield_in_gb = sample.get('clean_yield_in_gb')
            clean_yield_q30 = sample.get('clean_yield_q30')
            if not None in [sample_id, clean_yield_in_gb, clean_yield_q30]:
                all_yield_metrics = [sample_id, clean_yield_in_gb, clean_yield_q30]
                yield_metrics['samples'].append(all_yield_metrics[0])
                yield_metrics['clean_yield'].append(all_yield_metrics[1])
                yield_metrics['clean_yield_Q30'].append(all_yield_metrics[2])
        return yield_metrics

    def get_pc_statistics(self):
        pc_statistics = {'pc_duplicate_reads': [], 'pc_properly_mapped_reads': [], 'pc_pass_filter': []}
        for sample in self._database_samples_for_project:
            pc_duplicate_reads = sample.get('pc_duplicate_reads')
            pc_properly_mapped_reads = sample.get('pc_properly_mapped_reads')
            pc_pass_filter = sample.get('pc_pass_filter')
            if not None in [pc_duplicate_reads, pc_properly_mapped_reads, pc_pass_filter]:
                all_pc_statistics = [pc_duplicate_reads, pc_properly_mapped_reads, pc_pass_filter]
                pc_statistics['pc_duplicate_reads'].append(all_pc_statistics[0])
                pc_statistics['pc_properly_mapped_reads'].append(all_pc_statistics[1])
                pc_statistics['pc_pass_filter'].append(all_pc_statistics[2])
        return pc_statistics

    def chart_data(self):
        bam_reads_plot_outfile = path.join(self.project_source, 'bam_reads_plot.png')
        per_sample_bamfile_reads = self.get_bamfile_reads_for_project_samples()
        bamfile_reads_list = list(per_sample_bamfile_reads.values())
        bamfile_reads_list = [i for i in bamfile_reads_list if i]
        if len(bamfile_reads_list) > 1:
            plt.figure(figsize=(10, 5))
            plt.hist(bamfile_reads_list, bins=(range(min(bamfile_reads_list), max(bamfile_reads_list), 100000000)))
            plt.ylabel('Number of Samples')
            plt.xlabel('BAM File Reads')
            plt.savefig(bam_reads_plot_outfile)

        yield_plot_outfile = path.join(self.project_source, 'yield_plot.png')
        sample_yields = self.get_sample_yield_metrics()
        df = pd.DataFrame(sample_yields)
        indices = np.arange(len(df))
        plt.figure(figsize=(10, 5))
        plt.xticks([])
        plt.xlim([-1, max(indices) + 1])
        plt.bar(indices, df['clean_yield'], width=0.8, align='center', color='gainsboro')
        plt.bar(indices, df['clean_yield_Q30'], width=0.2, align='center', color='lightskyblue')
        plt.ylabel('Gigabases')
        plt.xticks([])
        blue_patch = mpatches.Patch(color='gainsboro', label='Yield (Gb)')
        green_patch = mpatches.Patch(color='lightskyblue', label='Yield Q30 (Gb)')
        lgd = plt.legend(handles=[blue_patch, green_patch], loc=9, bbox_to_anchor=(0.5,-0.02))
        plt.savefig(yield_plot_outfile, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=1)

        qc_plot_outfile = path.join(self.project_source, 'qc_plot.png')
        pc_statistics = self.get_pc_statistics()
        df = pd.DataFrame(pc_statistics)
        indices = np.arange(len(df))
        plt.figure(figsize=(10, 5))
        plt.xticks([])
        plt.xlim([-1, max(indices) + 1])
        plt.bar(indices, df['pc_properly_mapped_reads'], width=1, align='center', color='gainsboro')
        plt.bar(indices, df['pc_duplicate_reads'], width=0.4, align='center', color='mediumaquamarine')
        blue_patch = mpatches.Patch(color='gainsboro', label='% Paired Reads Aligned to Reference Genome')
        green_patch = mpatches.Patch(color='mediumaquamarine', label='% Duplicate Reads')
        lgd = plt.legend(handles=[blue_patch, green_patch], loc=9, bbox_to_anchor=(0.5,-0.02))
        plt.ylabel('% of Reads')
        plt.savefig(qc_plot_outfile, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=1)
        bam_reads_plot_outfile = 'file://' + bam_reads_plot_outfile
        yield_plot_outfile = 'file://' + yield_plot_outfile
        qc_plot_outfile = 'file://' + qc_plot_outfile
        return bam_reads_plot_outfile, yield_plot_outfile, qc_plot_outfile

    def get_project_sample_metrics(self):
        project_sample_metrics = {'median_coverage': [],
                                  'clean_pc_q30': [],
                                  'pc_properly_mapped_reads': [],
                                  'clean_yield_in_gb': []}
        for sample in self._database_samples_for_project:
            project_sample_metrics['median_coverage'].append(sample.get('median_coverage'))
            project_sample_metrics['clean_pc_q30'].append(sample.get('clean_pc_q30'))
            project_sample_metrics['pc_properly_mapped_reads'].append(sample.get('pc_properly_mapped_reads'))
            project_sample_metrics['clean_yield_in_gb'].append(sample.get('clean_yield_in_gb'))
        for metric in project_sample_metrics:
            if all(i is None for i in project_sample_metrics[metric]):
               project_sample_metrics[metric] = [0 for i in project_sample_metrics[metric]]
            else:
                project_sample_metrics[metric] = [i for i in project_sample_metrics[metric] if i]
            project_sample_metrics[metric] = [min(project_sample_metrics[metric]), max(project_sample_metrics[metric])]
        return project_sample_metrics

    def get_species(self, samples):
        species = set()
        for sample in samples:
            species.add(self.get_report_type_from_sample(sample))
        return species

    def get_library_workflow(self, samples):
        library_workflow = set()
        for sample in samples:
            library_workflow.add(self.get_library_workflow_from_sample(sample))
        if len(library_workflow) != 1:
            raise ValueError('%s workflows used for this project: %s' % (len(library_workflow), library_workflow))
        library_workflow = library_workflow.pop()
        return library_workflow

    def get_html_template(self):
        template_base = self.template_alias[self.get_library_workflow(self.get_all_sample_names(modify_names=False))]
        template = {'template_base': None, 'bioinformatics_template': None, 'formats_template': None}
        species = self.get_species(self.get_all_sample_names())
        if not species:
            raise ValueError('No species found for this project')
        elif len(species) == 1 and list(species)[0] == 'Human':
            template['template_base'] = template_base + '.html'
            template['bioinformatics_template'] = ['human_bioinf']
            template['formats_template'] = ['fastq', 'bam', 'vcf']
        elif 'Sheep' in (list(species)):
            template['template_base'] = template_base + '_non_human.html'
            template['bioinformatics_template'] = ['non_human_bioinf', 'bos_taurus_bioinf']
            template['formats_template'] = ['fastq']
        else:
            template['template_base'] = template_base + '_non_human.html'
            template['bioinformatics_template'] = ['non_human_bioinf']
            template['formats_template'] = ['fastq']
        return template

    def generate_report(self, output_format):
        project_file = path.join(self.project_delivery, 'project_%s_report.%s' % (self.project_name, output_format))
        self.generate_csv()
        h = self.get_html_content()
        if output_format == 'html':
            open(project_file, 'w').write(h)
        else:
            HTML(string=h).write_pdf(project_file)

    def generate_csv(self):
        csv_file = path.join(self.project_delivery, 'project_data.csv')

        with open(csv_file, 'w') as outfile:
            writer = csv.writer(outfile, delimiter='\t')
            writer.writerow(['Internal ID',
                            'External ID',
                            'Species',
                            'Workflow',
                            'Clean Yield (Gb)',
                            'Clean %Q30',
                            '% Duplicate Reads',
                            '% Properly Mapped Reads',
                            '% Pass Filter',
                            'Median Coverage'])

            for sample in self.samples_for_project_restapi:
                sample_from_lims = self.get_sample(sample.get('sample_id'))
                writer.writerow([sample.get('sample_id', 'None'),
                                 sample.get('user_sample_id', 'None'),
                                 sample_from_lims.udf.get('Species'),
                                 self.get_library_workflow_from_sample(sample.get('sample_id', 'None')),
                                 sample.get('clean_yield_in_gb', 'None'),
                                 sample.get('clean_pc_q30', 'None'),
                                 sample.get('pc_duplicate_reads', 'None'),
                                 sample.get('pc_properly_mapped_reads', 'None'),
                                 sample.get('pc_pass_filter', 'None'),
                                 sample.get('median_coverage', 'None')])

    def get_html_content(self):
        template_dir = path.join(path.dirname(path.abspath(__file__)), 'templates')
        env = Environment(loader=FileSystemLoader(template_dir))
        project_templates = self.get_html_template()
        template = env.get_template(project_templates.get('template_base'))
        project_stats = self.get_sample_info()
        bam_reads_plot_outfile, yield_plot_outfile, qc_plot_outfile = self.chart_data()
        project_sample_metrics = self.get_project_sample_metrics()
        return template.render(project_stats=project_stats,
                               project_info=self.get_project_info(),
                               bam_reads_plot_outfile=bam_reads_plot_outfile,
                               yield_plot_outfile=yield_plot_outfile,
                               qc_plot_outfile=qc_plot_outfile,
                               project_templates=project_templates,
                               project_sample_metrics=project_sample_metrics,
                               **self.params)

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
