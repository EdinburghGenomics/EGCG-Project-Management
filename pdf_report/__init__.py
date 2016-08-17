#!/usr/bin/env python
from argparse import ArgumentParser
import csv
from cStringIO import StringIO
from genologics.lims import Lims
import re
import logging
import sys
import os
from xhtml2pdf import pisa
from jinja2 import Environment, FileSystemLoader
from report_generation.config import EnvConfiguration
import yaml

__author__ = 'tcezard'


app_logger = logging.getLogger(__name__)
species_alias = {
    'Homo sapiens': 'Human',
    'Human': 'Human'
}
def get_pdf(html_string):
    pdf = StringIO()
    html_string = html_string.encode('utf-8')
    pisa.CreatePDF(StringIO(html_string), pdf)
    return pdf

cfg = EnvConfiguration()

def getFolderSize(folder):
    total_size = os.path.getsize(folder)
    for item in os.listdir(folder):
        itempath = os.path.join(folder, item)
        if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            total_size += getFolderSize(itempath)
    return total_size

class ProjectReport:

    def __init__(self, project_name):
        self.project_name = project_name
        self.project_source = os.path.join(cfg.query('sample','delivery_source'), project_name)
        self.project_delivery = os.path.join(cfg.query('sample','delivery_dest'), project_name)
        self.lims=Lims(**cfg.get('clarity'))
        self.params = {'project_name':project_name}
        self.results = {}
        self.fill_sample_names_from_lims()
        self.samples_delivered = self.read_metrics_csv(os.path.join(self.project_delivery, 'summary_metrics.csv'))
        self.get_sample_param()
        self.fill_project_information_from_lims()

    def fill_project_information_from_lims(self):
        project = self.lims.get_projects(name=self.project_name)[0]
        self.project_info = {}
        self.project_info['project_name']=['Project name:',self.project_name]
        self.project_info['project_title']=['Project title:', project.udf.get('Project Title', '')]
        self.project_info['enquiry'] = ['Enquiry no:', project.udf.get('Enquiry Number', '')]
        self.project_info['quote'] = ['Quote no:', project.udf.get('Quote No.', '')]
        self.project_info['researcher'] = ['Researcher:','%s %s (%s)'%(project.researcher.first_name,
                                                                       project.researcher.last_name,
                                                                       project.researcher.email)]
        self.project_order = ['project_name', 'project_title', 'enquiry', 'quote', 'researcher']


    def fill_sample_names_from_lims(self):
        samples = self.lims.get_samples(projectname=self.project_name)
        self.samples = [s.name for s in samples]
        self.modified_samples = [re.sub(r'[: ]','_', s.name) for s in samples]


    def get_library_workflow_from_sample(self, sample_name):
        samples = self.lims.get_samples(projectname=self.project_name, name=sample_name)
        if len(samples) == 1:
            return samples[0].udf.get('Prep Workflow')
        else:
            app_logger.error('%s samples found for sample name %s'%sample_name)

    def get_species_from_sample(self, sample_name):
        samples = self.lims.get_samples(projectname=self.project_name, name=sample_name)
        if len(samples) == 1:
            s = samples[0].udf.get('Species')
            return species_alias.get(s, s)
        else:
            app_logger.error('%s samples found for sample name %s'%sample_name)

    def parse_program_csv(self, program_csv):
        all_programs = {}
        if os.path.exists(program_csv):
            with open(program_csv) as open_prog:
                for row in csv.reader(open_prog):
                    all_programs[row[0]]=row[1]
        #TODO: change the hardcoded version of bcl2fastq
        all_programs['bcl2fastq'] = '2.17.1.14'
        for p in ['bcl2fastq','bcbio', 'bwa', 'gatk', 'samblaster']:
            if p in all_programs:
                self.params[p + '_version']=all_programs.get(p)


    def parse_project_summary_yaml(self, summary_yaml):
        with open(summary_yaml, 'r') as open_file:
            full_yaml = yaml.safe_load(open_file)
        sample_yaml=full_yaml['samples'][0]
        path_to_bcbio = os.path.basename(os.path.dirname(sample_yaml['dirs']['galaxy']))
        self.params['bcbio_version'] = path_to_bcbio.split('/')[-2]
        if sample_yaml['genome_build'] == 'hg38':
            self.params['genome_version'] = 'GRCh38 (with alt, decoy and HLA sequences)'

    def read_metrics_csv(self, metrics_csv):
        samples_to_info={}
        with open(metrics_csv) as open_metrics:
            reader = csv.DictReader(open_metrics, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in reader:
                samples_to_info[row['Sample Id']] = row
        return samples_to_info

    def get_sample_param(self):
        self.fill_sample_names_from_lims()
        project_size = 0
        library_workflows=set()
        species = set()
        for sample in self.samples:
            library_workflow = self.get_library_workflow_from_sample(sample)
            library_workflows.add(library_workflow)
            species.add(self.get_species_from_sample(sample))
        if len(library_workflows) == 1 :
            self.library_workflow = library_workflows.pop()
        else:
            app_logger.error('More than one workfkow used in project %s: %s'%(self.project_name, ', '.join(library_workflows)))

        if len(species) == 1 :
            self.species = species.pop()
        else:
            app_logger.error('More than one species used in project %s: %s'%(self.project_name, ', '.join(species)))


        if self.library_workflow in ['TruSeq Nano DNA Sample Prep', None] :
            self.template = 'truseq_nano_template'
        elif self.library_workflow in ['TruSeq PCR-Free DNA Sample Prep', 'TruSeq PCR-Free Sample Prep'] :
            self.template = 'truseq_pcrfree_template'
        else:
            app_logger.error('Unknown library workflow %s for project %s'%(self.library_workflow, self.project_name))
            return None

        if self.species == 'Human':
            self.template += '.html'
        else:
            self.template += '_non_human.html'

        self.params['adapter1'] = "AGATCGGAAGAGCACACGTCTGAACTCCAGTCA"
        self.params['adapter2'] = "AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT"

        project_size = getFolderSize(self.project_delivery)
        for sample in set(self.modified_samples):
            sample_source=os.path.join(self.project_source, sample)
            if os.path.exists(sample_source):
                program_csv = os.path.join(sample_source, 'programs.txt')
                if not os.path.exists(program_csv):
                    program_csv = os.path.join(sample_source, '.qc', 'programs.txt')
                self.parse_program_csv(program_csv)
                summary_yaml = os.path.join(sample_source, 'project-summary.yaml')
                if not os.path.exists(summary_yaml):
                    summary_yaml = os.path.join(sample_source, '.qc', 'project-summary.yaml')
                if os.path.exists(summary_yaml):
                    self.parse_project_summary_yaml(summary_yaml)

        self.results['project_size']=['Total folder size:','%.2fTb'%(project_size/1000000000000.0)]
        self.results['nb_sample']=['Number of sample:', len(self.samples)]
        self.results['nb_sample_delivered']=['Number of sample delivered:',len(self.samples_delivered)]
        yields = [float(self.samples_delivered[s]['Yield']) for s in self.samples_delivered]
        self.results['yield']=['Total yield Gb:','%.2f'%sum(yields)]
        self.results['mean_yield']=['Average yield Gb:','%.1f'%(sum(yields)/max(len(yields), 1))]

        try:
            coverage = [float(self.samples_delivered[s]['Mean coverage']) for s in self.samples_delivered]
            self.results['coverage']=['Average coverage per samples:','%.2f'%(sum(coverage)/max(len(coverage), 1))]
            self.results_order=['nb_sample','nb_sample_delivered', 'yield', 'mean_yield', 'coverage', 'project_size']
        except KeyError:
            self.results_order=['nb_sample','nb_sample_delivered', 'yield', 'mean_yield', 'project_size']



    def generate_report(self):
        template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates'))
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(self.template)
        output = template.render(results_order=self.results_order, results=self.results,
                                 project_info=self.project_info, project_order=self.project_order,
                                 **self.params)
        pdf = get_pdf(output)
        project_file = os.path.join(self.project_delivery, 'project_%s_report.pdf'%self.project_name)
        with open(project_file, 'w') as open_pdf:
            open_pdf.write(pdf.getvalue())


def main():
    #Setup options
    argparser=_prepare_argparser()
    args = argparser.parse_args()
    logging.StreamHandler()
    handler = logging.StreamHandler(stream=sys.stdout, )
    handler.setLevel(logging.DEBUG)
    logging.getLogger('xhtml2pdf').addHandler(handler)
    app_logger.addHandler(handler)
    pr = ProjectReport(args.project_name)
    pr.generate_report()

def _prepare_argparser():
    """Prepare optparser object. New arguments will be added in this
    function first.
    """
    description = """Simple script that parse bcbio outputs and generate a wiki table"""

    argparser = ArgumentParser(description=description)
    argparser.add_argument("-p", "--project_name", dest="project_name", type=str,
                           help="The name of the project for which a report should be generated.")


    return argparser


if __name__=="__main__":
    main()
