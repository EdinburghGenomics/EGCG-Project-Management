import sys
import re
import csv
import yaml
import logging
from os import path, listdir
from argparse import ArgumentParser
from weasyprint import HTML
from jinja2 import Environment, FileSystemLoader
from egcg_core.util import find_file
from egcg_core.clarity import connection
from config import cfg, load_config

app_logger = logging.getLogger(__name__)

species_alias = {
    'Homo sapiens': 'Human',
    'Human': 'Human'
}


class ProjectReport:
    _samples_for_project = None
    template_alias = {
        'TruSeq Nano DNA Sample Prep': 'truseq_nano_template',
        None: 'truseq_nano_template',
        'TruSeq PCR-Free DNA Sample Prep': 'truseq_pcrfree_template',
        'TruSeq PCR-Free Sample Prep': 'truseq_pcrfree_template'
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
        if path.exists(program_csv):
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

        samples_delivered = self.read_metrics_csv(path.join(self.project_delivery, 'summary_metrics.csv'))
        yields = [float(samples_delivered[s]['Yield']) for s in samples_delivered]
        results = [
            ('Number of samples:', len(modified_samples)),
            ('Number of samples delivered:', len(samples_delivered)),
            ('Total yield Gb:', '%.2f' % sum(yields)),
            ('Average yield Gb:', '%.1f' % (sum(yields)/max(len(yields), 1)))
        ]

        try:
            coverage = [float(samples_delivered[s]['Mean coverage']) for s in samples_delivered]
            results.append(('Average coverage per samples:', '%.2f' % (sum(coverage)/max(len(coverage), 1))))
        except KeyError:
            app_logger.warning('Not adding mean coverage')

        project_size = self.get_folder_size(self.project_delivery)
        results.append(('Total folder size:', '%.2fTb' % (project_size/1000000000000.0)))
        return results

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

    def generate_report(self):
        project_file = path.join(self.project_delivery, 'project_%s_report.pdf' % self.project_name)
        h = HTML(string=self.get_html_content())
        h.write_pdf(project_file)

    def get_html_content(self):
        template_dir = path.join(path.dirname(path.abspath(__file__)), 'templates')
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(self.get_html_template())
        return template.render(results=self.get_sample_info(), project_info=self.get_project_info(), **self.params)

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


def main():
    load_config()
    args = _parse_args()
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.DEBUG)
    logging.getLogger('xhtml2pdf').addHandler(handler)
    app_logger.addHandler(handler)
    pr = ProjectReport(args.project_name)
    pr.generate_report()


def _parse_args():
    a = ArgumentParser()
    a.add_argument(
        '-p',
        '--project_name',
        dest='project_name',
        type=str,
        help='The name of the project for which a report should be generated.'
    )
    return a.parse_args()


if __name__ == '__main__':
    main()
