import re
import csv
import yaml
from os import path, listdir
from jinja2 import Environment, FileSystemLoader
from egcg_core.util import find_file
from egcg_core.clarity import connection
from egcg_core.app_logging import logging_default as log_cfg
from config import cfg
from egcg_core.rest_communication import get_documents

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
    _samples_for_project = None
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
        return (
            ('Project name:', self.project_name),
            ('Project title:', project.udf.get('Project Title', '')),
            ('Enquiry no:', project.udf.get('Enquiry Number', '')),
            ('Quote no:', project.udf.get('Quote No.', '')),
            ('Researcher:', '%s %s (%s)' % (project.researcher.first_name,
                                            project.researcher.last_name,
                                            project.researcher.email))
        )

    @property
    def samples_for_project(self):
        if self._samples_for_project is None:
            self._samples_for_project = self.lims.get_samples(projectname=self.project_name)
        return self._samples_for_project

    def get_sample(self, sample_name):
        samples = [s for s in self.samples_for_project if s.name == sample_name]
        if len(samples) == 1:
            return samples[0]
        raise ValueError('%s samples found for %s' % (len(samples), sample_name))

    def get_all_sample_names(self, modify_names=False):
        if modify_names:
            return [re.sub(r'[: ]', '_', s.name) for s in self.samples_for_project]
        return [s.name for s in self.samples_for_project]

    def get_library_workflow_from_sample(self, sample_name):
        return self.get_sample(sample_name).udf.get('Prep Workflow')

    def get_species_from_sample(self, sample_name):
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

    def per_project_sample_basic_stats(self):
        samples_for_project = get_documents('aggregate/samples', where={'project_id': self.project_name})
        sample_yields = [s.get('clean_yield_in_gb') for s in samples_for_project]
        coverage_per_sample = [s.get('coverage', {}).get('mean') for s in samples_for_project]
        samples_in_project = len(sample_yields)
        return sample_yields, samples_in_project, coverage_per_sample

    def per_project_sample_qc_stats(self):
        samples_for_project = get_documents('aggregate/samples', where={'project_id': self.project_name})
        pc_duplicate_reads = [s.get('pc_duplicate_reads') for s in samples_for_project if s.get('pc_duplicate_reads')]
        evenness = [s.get('evenness') for s in samples_for_project if s.get('evenness')]
        freemix = [s.get('freemix') for s in samples_for_project if s.get('freemix')]
        pc_properly_mapped_reads = [s.get('pc_properly_mapped_reads') for s in samples_for_project if s.get('pc_properly_mapped_reads')]
        clean_pc_q30 = [s.get('clean_pc_q30') for s in samples_for_project if s.get('clean_pc_q30')]
        mean_bases_covered_at_15X = [s.get('coverage_statistics', {})
                                         .get('bases_at_coverage', {})
                                         .get('bases_at_15X')
                                     for s in samples_for_project if s.get('coverage_statistics')]

        average_pc_duplicates = None
        average_evenness = None
        max_freemix = None
        average_pc_properly_mapped_reads = None
        average_clean_pc_q30 = None
        average_mean_bases_covered_at_15X = None

        if pc_duplicate_reads:
            average_pc_duplicates = sum(pc_duplicate_reads)/len(pc_duplicate_reads)
        if evenness:
            average_evenness = sum(evenness)/len(evenness)
        if freemix:
            max_freemix = max(freemix)
        if pc_properly_mapped_reads:
            average_pc_properly_mapped_reads = sum(pc_properly_mapped_reads)/len(pc_properly_mapped_reads)
        if clean_pc_q30:
            average_clean_pc_q30 = sum(clean_pc_q30)/len(clean_pc_q30)
        if mean_bases_covered_at_15X:
            average_mean_bases_covered_at_15X = sum(mean_bases_covered_at_15X)/len(mean_bases_covered_at_15X)

        return average_pc_duplicates, \
               average_evenness, \
               max_freemix, \
               average_pc_properly_mapped_reads, \
               average_clean_pc_q30, \
               average_mean_bases_covered_at_15X

    def get_sample_info(self):

        # basic statisticss
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

        yields, samples_delivered, coverage = self.per_project_sample_basic_stats()

        basic_stats_results = [
            ('Number of samples:', len(modified_samples)),
            ('Number of samples delivered:', samples_delivered),
            ('Total yield (Gb):', '%.2f' % sum(yields)),
            ('Average yield (Gb):', '%.1f' % (sum(yields)/max(len(yields), 1)))
        ]

        basic_stats_results.append(('Average coverage per sample:', '%.2f' % (sum(coverage)/max(len(coverage), 1))))

        project_size = self.get_folder_size(self.project_delivery)
        basic_stats_results.append(('Total folder size:', '%.2fTb' % (project_size/1000000000000.0)))

        # QC
        pc_duplicate_reads, \
        evenness, \
        freemix, \
        pc_properly_mapped_reads, \
        clean_pc_q30, \
        mean_bases_covered_at_15X = self.per_project_sample_qc_stats()
        qc_results = [('Average percent duplicate reads: %s' % (pc_duplicate_reads)),
                      ('Average evenness: %s' % (evenness)),
                      ('Maximum freemix value : %s' % (freemix)),
                      ('Average percent mapped reads : %s' % (pc_properly_mapped_reads)),
                      ('Average percent Q30 : %s' % (clean_pc_q30)),
                      ('Average bases covered at 15X: %s' % (pc_duplicate_reads))]

        return basic_stats_results, qc_results


    def get_html_template(self):
        samples = self.get_all_sample_names(modify_names=False)
        library_workflow = set()
        species = set()
        for sample in samples:
            library_workflow.add(self.get_library_workflow_from_sample(sample))
            species.add(self.get_species_from_sample(sample))

        if len(library_workflow) != 1:
            raise ValueError('%s workflows used for this project: %s' % (len(library_workflow), library_workflow))
        library_workflow = library_workflow.pop()

        if len(species) != 1:
            raise ValueError('%s species used for this project: %s' % (len(species), species))
        species = species.pop()

        template_base = self.template_alias[library_workflow]
        if species == 'Human':
            return template_base + '.html'
        return template_base + '_non_human.html'

    def generate_report(self, output_format):
        project_file = path.join(self.project_delivery, 'project_%s_report.%s' % (self.project_name, output_format))
        h = self.get_html_content()
        if output_format == 'html':
            open(project_file, 'w').write(h)
        else:
            HTML(string=h).write_pdf(project_file)

    def get_html_content(self):
        template_dir = path.join(path.dirname(path.abspath(__file__)), 'templates')
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(self.get_html_template())
        basic_stats_results, qc_results = self.get_sample_info()
        return template.render(basic_stats_results=basic_stats_results, qc_results=qc_results, project_info=self.get_project_info(), **self.params)

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
