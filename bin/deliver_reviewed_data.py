import os
import sys
import csv
import shutil
import argparse
import datetime
import logging
import traceback
from collections import defaultdict
from os.path import basename, join, dirname
from cached_property import cached_property
from egcg_core import executor, rest_communication, clarity
from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import cfg
from egcg_core.constants import ELEMENT_NB_READS_CLEANED, ELEMENT_RUN_NAME, ELEMENT_PROJECT_ID, ELEMENT_LANE, \
    ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_SAMPLE_EXTERNAL_ID, ELEMENT_RUN_ELEMENT_ID, ELEMENT_USEABLE
from egcg_core.exceptions import EGCGError
from egcg_core.notifications.email import send_html_email
from egcg_core.util import find_files, find_fastqs, query_dict
from pyclarity_lims.entities import Process

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from project_report import ProjectReport
from config import load_config

hs_files = [
    '{ext_sample_id}.g.vcf.gz',
    '{ext_sample_id}.g.vcf.gz.tbi',
    '{ext_sample_id}.vcf.gz',
    '{ext_sample_id}.vcf.gz.tbi',
    '{ext_sample_id}.bam',
    '{ext_sample_id}.bam.bai'
]

variant_calling_files = [
    '{ext_sample_id}.g.vcf.gz',
    '{ext_sample_id}.g.vcf.gz.tbi',
    '{ext_sample_id}.bam',
    '{ext_sample_id}.bam.bai'
]

other_files = []

email_template = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'delivery_email_template.html'
)

release_trigger_lims_step_name = 'Data Release Trigger EG 1.0 ST'


def _execute(*commands, **kwargs):
    exit_status = executor.execute(*commands, **kwargs).join()
    if exit_status != 0:
        raise EGCGError('Commands %s exited with status %s' % (commands, exit_status))


def _now():
    return datetime.datetime.utcnow().strftime('%d_%m_%Y_%H:%M:%S')


class DataDelivery(AppLogger):
    def __init__(self, dry_run, work_dir, process_id, no_cleanup=False, email=True):
        self.process_id = process_id
        self.dry_run = dry_run
        self.work_dir = work_dir
        self.no_cleanup = no_cleanup
        self.email = email
        self.all_commands_for_cluster = []
        self.postponed_register = []
        self.all_samples_dict = {}
        self.sample2species = {}
        self.sample2analysis_type = {}
        self.samples2files = defaultdict(list)
        self.staging_dir = os.path.join(self.work_dir, 'data_delivery_' + _now())
        self.delivery_dest = cfg['delivery']['dest']
        self.delivery_source = cfg['delivery']['source']

    @cached_property
    def today(self):
        return datetime.date.today().isoformat()

    @staticmethod
    def parse_date(date):
        if not date:
            return 'NA'
        return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d')

    @staticmethod
    def library_alias(library_type):
        return {'nano': 'TruSeq Nano', 'pcrfree': 'TruSeq PCR-Free'}.get(library_type, 'NA')

    @cached_property
    def process(self):
        return Process(clarity.connection(), id=self.process_id)

    @staticmethod
    def get_sample_data(sample_name):
        return {
            'data': rest_communication.get_document('samples', where={'sample_id': sample_name}),
            'udfs': rest_communication.get_document('lims/samples', match={'sample_id': sample_name}),
            'status': rest_communication.get_document('lims/status/sample_status', match={'sample_id': sample_name}),
            'run_elements': rest_communication.get_documents('run_elements', where={'sample_id': sample_name}),
        }

    def already_delivered_samples(self, project_id):
        return dict(
            (sample['sample_id'], self.get_sample_data(sample['sample_id']))
            for sample in rest_communication.get_documents('samples', where={'project_id': project_id, 'delivered': 'yes'})
        )

    @cached_property
    def deliverable_samples(self):
        """Retrieve the names of samples that went through the authorisation step. Then get the data associated."""
        if self.process.type.name != release_trigger_lims_step_name:
            raise ValueError('Process %s is not of the type ' + release_trigger_lims_step_name)
        sample_names = [a.samples[0].name for a in self.process.all_inputs(resolve=True)]
        project_to_samples = defaultdict(list)
        samples = [self.get_sample_data(sample) for sample in sample_names]
        for sample in samples:
            project_to_samples[query_dict(sample, 'data.project_id')].append(sample.get('data'))
            self.all_samples_dict[query_dict(sample, 'data.sample_id')] = sample
        return project_to_samples

    def stage_data(self, sample):
        # test sample has arrived in FluidX tube
        sample_id = sample.get(ELEMENT_SAMPLE_INTERNAL_ID)
        fluidx_barcode = self.all_samples_dict[sample_id]['udfs'].get('2D Barcode')

        # Create staging_directory
        if fluidx_barcode:
            sample_dir = os.path.join(self.staging_dir, fluidx_barcode)
        else:
            sample_dir = os.path.join(self.staging_dir, sample_id)
        os.makedirs(sample_dir, exist_ok=True)

        # Find the fastq files
        self._stage_fastq_files(sample, sample_dir)

        # Find the analysed files
        self._stage_analysed_files(sample, sample_dir)
        return sample_dir

    def _stage_fastq_files(self, sample, sample_dir):
        sample_id = sample.get(ELEMENT_SAMPLE_INTERNAL_ID)
        delivery_type = self.all_samples_dict[sample_id]['udfs'].get('Delivery', 'merged')
        original_fastq_files = self._get_fastq_files_for_sample(sample_id)
        external_sample_id = sample.get(ELEMENT_SAMPLE_EXTERNAL_ID)

        if delivery_type == 'merged':
            if len(original_fastq_files) == 1:
                r1, r2 = list(original_fastq_files.values())[0]
                self._link_run_element_files(sample_id, r1, r2, sample_dir, external_sample_id)
            else:
                r1_files = [r1 for r1, r2 in original_fastq_files.values()]
                r2_files = [r2 for r1, r2 in original_fastq_files.values()]
                self._on_cluster_concat_file_to_sample(sample_id, r1_files, sample_dir, rename=external_sample_id + '_R1.fastq.gz')
                self._on_cluster_concat_file_to_sample(sample_id, r2_files, sample_dir, rename=external_sample_id + '_R2.fastq.gz')
        else:
            fastq_folder = os.path.join(sample_dir, 'raw_data')
            os.makedirs(fastq_folder)
            for run_element_id in original_fastq_files:
                r1, r2 = original_fastq_files.get(run_element_id)
                self._link_run_element_files(sample_id, r1, r2, fastq_folder, run_element_id)

    def _link_run_element_files(self, sample_id, r1, r2, fastq_folder, rename):
        fastq1 = self._link_file_to_sample_folder(r1, fastq_folder, rename=rename + '_R1.fastq.gz')
        fastq2 = self._link_file_to_sample_folder(r2, fastq_folder, rename=rename + '_R2.fastq.gz')
        md5_fastq1 = self._link_file_to_sample_folder(r1 + '.md5', fastq_folder, rename=rename + '_R1.fastq.gz.md5')
        md5_fastq2 = self._link_file_to_sample_folder(r2 + '.md5', fastq_folder, rename=rename + '_R2.fastq.gz.md5')

        self.register_file(sample_id, fastq1, md5_fastq1)
        self.register_file(sample_id, fastq2, md5_fastq2)

        self._link_file_to_sample_folder(r1.replace('.fastq.gz', '_fastqc.html'), fastq_folder, rename=rename + '_R1_fastqc.html')
        self._link_file_to_sample_folder(r2.replace('.fastq.gz', '_fastqc.html'), fastq_folder, rename=rename + '_R2_fastqc.html')
        self._link_file_to_sample_folder(r1.replace('.fastq.gz', '_fastqc.zip'), fastq_folder, rename=rename + '_R1_fastqc.zip')
        self._link_file_to_sample_folder(r2.replace('.fastq.gz', '_fastqc.zip'), fastq_folder, rename=rename + '_R2_fastqc.zip')

    def _stage_analysed_files(self, sample, sample_dir):
        sample_id = sample[ELEMENT_SAMPLE_INTERNAL_ID]
        external_sample_id = sample[ELEMENT_SAMPLE_EXTERNAL_ID]
        project_id = sample[ELEMENT_PROJECT_ID]
        origin_sample_dir = os.path.join(self.delivery_source, project_id, sample_id)

        if not os.path.isdir(origin_sample_dir):
            raise EGCGError('Directory for sample %s in project %s does not exist' % (sample_id, project_id))

        files_to_move = self.get_analysis_files(sample_name=sample_id, external_sample_name=external_sample_id)
        for f in files_to_move:
            origin_file = os.path.join(origin_sample_dir, f)
            if not os.path.isfile(origin_file):
                raise EGCGError('File %s for sample %s does not exist' % (f, sample))
            data_file = self._link_file_to_sample_folder(origin_file, sample_dir)
            origin_md5_file = os.path.join(origin_sample_dir, f + '.md5')
            if not os.path.isfile(origin_md5_file):
                raise EGCGError('File %s for sample %s does not exist' % (f, sample))

            md5_file = self._link_file_to_sample_folder(origin_md5_file, sample_dir)
            self.register_file(sample_id, data_file, md5_file)

    def register_file(self, sample_id, data_file, md5_file):
        with open(md5_file) as open_file:
            md5, file_path = open_file.readline().strip().split()
        rel_path = os.path.relpath(data_file, start=self.staging_dir)
        file_size = os.stat(data_file).st_size
        self.samples2files[sample_id].append({'file_path': rel_path, 'md5': md5, 'size': file_size})

    def update_registered_files(self, sample_id, delivery_folder):
        for file_dict in self.samples2files.get(sample_id):
            file_dict['file_path'] = join(basename(dirname(delivery_folder)), basename(delivery_folder), file_dict['file_path'])

    def register_postponed_files(self):
        for tuple_val in self.postponed_register:
            self.register_file(*tuple_val)

    def _sample_metrics(self, sample_data, delivery_folder):
        return [
            query_dict(sample_data, 'data.project_id'),
            query_dict(sample_data, 'data.sample_id'),
            query_dict(sample_data, 'data.user_sample_id'),
            query_dict(sample_data, 'data.species_name'),
            self.library_alias(query_dict(sample_data, 'status.library_type')),
            self.parse_date(query_dict(sample_data, 'status.started_date')),
            query_dict(sample_data, 'udfs.Total DNA(ng)'),
            query_dict(sample_data, 'data.aggregated.clean_reads'),
            query_dict(sample_data, 'data.required_yield') / 1000000000,
            query_dict(sample_data, 'data.aggregated.yield_in_gb'),
            query_dict(sample_data, 'data.aggregated.yield_q30_in_gb'),
            query_dict(sample_data, 'data.aggregated.pc_q30'),
            query_dict(sample_data, 'data.aggregated.pc_mapped_reads'),
            query_dict(sample_data, 'data.aggregated.pc_duplicate_reads'),
            query_dict(sample_data, 'data.required_coverage'),
            query_dict(sample_data, 'data.coverage.mean'),
            os.path.basename(delivery_folder)
        ]

    def summarise_metrics_per_sample(self, project_id, delivery_folder):
        headers = ['Project', 'Sample Id', 'User sample id', 'Species', 'Library type', 'Received date', 'DNA QC (ng)',
                   'Number of Read pair', 'Target Yield', 'Yield', 'Yield Q30', '%Q30', 'Mapped reads rate',
                   'Duplicate rate', 'Target Coverage', 'Mean coverage', 'Delivery folder']
        lines = []
        for sample_id, sample_data in self.all_samples_dict.items():
            if query_dict(sample_data, 'data.project_id') == project_id:
                res = self._sample_metrics(sample_data, delivery_folder)
                lines.append('\t'.join(str(r) for r in res))
        return headers, lines

    @staticmethod
    def _link_file_to_sample_folder(file_to_link, sample_folder, rename=None):
        if rename is None:
            rename = os.path.basename(file_to_link)
        link_file = os.path.join(sample_folder, rename)
        command = 'ln %s %s' % (file_to_link, link_file)
        _execute(command, env='local')
        return link_file

    def _on_cluster_concat_file_to_sample(self, sample_id, files, sample_folder, rename):
        command = 'cat {files} > {fq}; {md5sum} {fq} > {fq}.md5; {fastqc} --nogroup -q {fq}'.format(
            files=' '.join(files),
            fq=os.path.join(sample_folder, rename),
            md5sum=cfg.query('tools', 'md5sum', ret_default='md5sum'),
            fastqc=cfg['tools']['fastqc']
        )
        self.postponed_register.append(
            (sample_id, os.path.join(sample_folder, rename), os.path.join(sample_folder, rename) + '.md5')
        )
        self.all_commands_for_cluster.append(command)

    def get_sample_species(self, sample_name):
        return self.all_samples_dict[sample_name]['data']['species_name']

    def get_analysis_type(self, sample_name):
        return self.all_samples_dict[sample_name]['udfs'].get('Analysis Type')

    def get_analysis_files(self, sample_name, external_sample_name):
        species = self.get_sample_species(sample_name)
        analysis_type = self.get_analysis_type(sample_name)
        if species is None:
            raise EGCGError('No species information found in the LIMS for ' + sample_name)
        elif species == 'Homo sapiens':
            files = hs_files
        elif analysis_type in ['Variant Calling', 'Variant Calling gatk']:
            files = variant_calling_files
        else:
            files = other_files
        final_list = []
        for f in files:
            final_list.append(f.format(ext_sample_id=external_sample_name))
        return final_list

    def _get_fastq_files_for_sample(self, sample_id):
        fastq_files = {}
        # TODO: make sure that the list of run elements is the same as the one that was QC.
        for run_element in self.all_samples_dict[sample_id]['run_elements']:
            if run_element.get(ELEMENT_USEABLE) == 'yes' and int(run_element.get(ELEMENT_NB_READS_CLEANED, 0)) > 0:
                local_fastq_dir = os.path.join(cfg['input_dir'], run_element[ELEMENT_RUN_NAME])
                fastqs = find_fastqs(local_fastq_dir, run_element[ELEMENT_PROJECT_ID],
                                     run_element[ELEMENT_SAMPLE_INTERNAL_ID], run_element[ELEMENT_LANE])
                if fastqs:
                    fastq_files[run_element.get(ELEMENT_RUN_ELEMENT_ID)] = tuple(sorted(fastqs))
                else:
                    raise EGCGError(
                        'No Fastq files found for %s, %s, %s, %s' % (
                            local_fastq_dir, run_element[ELEMENT_PROJECT_ID],
                            run_element[ELEMENT_SAMPLE_INTERNAL_ID], run_element[ELEMENT_LANE]
                        )
                    )
        return fastq_files

    def mark_samples_as_released(self, samples):
        for sample_id in samples:
            payload = {
                'delivered': 'yes',
                'files_delivered': self.samples2files.get(sample_id, []),
                'delivery_date': _now()
            }
            rest_communication.patch_entry(
                'samples', payload=payload, id_field='sample_id', element_id=sample_id, update_lists=['files_delivered']
            )
        clarity.route_samples_to_delivery_workflow(samples)

    def write_metrics_file(self, project, delivery_folder):
        lines = []
        summary_metrics_file = os.path.join(self.delivery_dest, project, 'summary_metrics.csv')
        if os.path.isfile(summary_metrics_file):
            # file already there so grab the sample ids and delivery folder and regenerate the rest
            with open(summary_metrics_file, 'r') as open_file:
                reader = csv.DictReader(open_file, delimiter='\t')
                already_delivered_data = self.already_delivered_samples(project)
                for l in reader:
                    print(l.get('Delivery folder'))
                    res = self._sample_metrics(already_delivered_data[l.get('Sample Id')], l.get('Delivery folder'))
                    lines.append('\t'.join(str(r) for r in res))

        header, new_lines = self.summarise_metrics_per_sample(project, delivery_folder)
        lines += new_lines
        with open(summary_metrics_file, 'w') as open_file:
            open_file.write('\t'.join(header) + '\n')
            open_file.write('\n'.join(lines) + '\n')

    def run_aggregate_commands(self):
        if self.all_commands_for_cluster:
            self.debug('Concatenating fastqs for %s samples', len(self.all_commands_for_cluster))
            _execute(
                *self.all_commands_for_cluster,
                job_name='concat_delivery',
                working_dir=self.staging_dir,
                cpus=1,
                mem=2,
                log_commands=False
            )

    def generate_md5_summary(self, project, batch_folder):
        all_md5_files = find_files(batch_folder, '*', '*.md5') + find_files(batch_folder, '*', 'raw_data', '*.md5')
        md5_summary = []
        for md5_file in all_md5_files:
            with open(md5_file) as open_file:
                md5, file_path = open_file.readline().strip().split()
            file_name = os.path.basename(md5_file)[:-len('.md5')]
            batch_name = os.path.basename(batch_folder)
            prefix, suffix = md5_file[:-len('.md5')].split(batch_name)
            with open(md5_file, 'w') as open_file:
                open_file.write('%s  %s' % (md5, file_name))
            md5_summary.append('%s  %s' % (md5, batch_name + suffix))
        all_md5_files = os.path.join(self.delivery_dest, project, 'all_md5sums.txt')
        with open(all_md5_files, 'a') as open_file:
            open_file.write('\n'.join(md5_summary) + '\n')

    def cleanup(self):
        if self.no_cleanup:
            return
        if os.path.exists(self.staging_dir):
            shutil.rmtree(self.staging_dir)
            self.debug('Cleaned up staging dir %s', self.staging_dir)

    def deliver_data(self):
        project_to_delivery_folder = {}
        sample2stagedirectory = {}
        for project in self.deliverable_samples:
            for sample in self.deliverable_samples.get(project):
                stage_directory = self.stage_data(sample)
                sample2stagedirectory[sample.get(ELEMENT_SAMPLE_INTERNAL_ID)] = stage_directory

        if self.dry_run:
            print('Will Execute ')
            print('\n'.join(self.all_commands_for_cluster))
            print('Will move')
            for project in self.deliverable_samples:
                batch_delivery_folder = os.path.join(self.delivery_dest, project, self.today)
                for sample in self.deliverable_samples.get(project):
                    print('%s --> %s' % (sample2stagedirectory.get(sample[ELEMENT_SAMPLE_INTERNAL_ID]),
                                         batch_delivery_folder))
                header, lines = self.summarise_metrics_per_sample(project, batch_delivery_folder)
                print('\t'.join(header))
                print('\n'.join(lines))
        else:
            # run the command on the cluster and register the output
            self.run_aggregate_commands()
            self.register_postponed_files()
            for project in self.deliverable_samples:
                # Create the batch directory
                batch_delivery_folder = os.path.join(self.delivery_dest, project, self.today)
                os.makedirs(batch_delivery_folder, exist_ok=True)
                # Move all the staged sample directory
                project_to_delivery_folder[project] = batch_delivery_folder
                for sample in self.deliverable_samples.get(project):
                    shutil.move(
                        sample2stagedirectory.get(sample[ELEMENT_SAMPLE_INTERNAL_ID]),
                        batch_delivery_folder
                    )
                    self.update_registered_files(sample[ELEMENT_SAMPLE_INTERNAL_ID], batch_delivery_folder)
                self.write_metrics_file(project, batch_delivery_folder)
                self.generate_md5_summary(project, batch_delivery_folder)
            self.mark_samples_as_released(list(sample2stagedirectory))

        # Generate project report
        project_to_reports = {}
        for project in self.deliverable_samples:
            project_report = join(self.delivery_dest, project, 'project_%s_report.pdf' % project)
            try:
                if not self.dry_run:
                    pr = ProjectReport(project, self.staging_dir)
                    pr.generate_report('pdf')
            except Exception as e:
                self.critical('Project report generation for %s failed: %s' % (project, e))
                etype, value, tb = sys.exc_info()
                if tb:
                    stacktrace = ''.join(traceback.format_exception(etype, value, tb))
                    self.info('Stacktrace below:\n' + stacktrace)
                project_report = None
            if project_report and os.path.exists(project_report):
                project_to_reports[project] = project_report

        # Send email confirmation with attachments
        self.send_reports(self.deliverable_samples, project_to_reports)
        self.cleanup()

    def send_reports(self, project_to_samples, project_to_reports):
        email_cfg = cfg.query('delivery', 'email_notification')
        if email_cfg and set(email_cfg) != {'mailhost', 'port', 'sender', 'recipients'}:
            self.error('Invalid email config: will not sent email')
            return

        for project_id in project_to_samples:
            species_list = [self.get_sample_species(sample[ELEMENT_SAMPLE_INTERNAL_ID]) for sample in
                            project_to_samples[project_id]]

            subject = '%s: %s WGS Data Release' % (project_id, ', '.join(sorted(set(species_list))))
            params = self.get_email_data(project_id, project_to_samples[project_id])
            params.update(email_cfg)
            if self.dry_run:
                subject = 'Dry run: ' + subject
            self.info('Send email for project %s', project_id)
            if self.email:
                send_html_email(
                    subject=subject,
                    attachments=project_to_reports.values(),
                    email_template=email_template,
                    **params
                )

    def get_email_data(self, project_id, samples):
        return {
            'release_batch': self.today,
            'num_samples': len(samples),
            'project_id': project_id,
            'delivery_queue': clarity.get_queue_uri(
                cfg['delivery']['clarity_workflow_name'],
                cfg.query('delivery', 'clarity_stage_name', ret_default=None)
            )
        }


def resolve_process_id(process_id):
    # Take the end of the url if it is a url
    process_id = process_id.split('/')[-1]
    if not process_id.startswith('24-'):
        process_id = '24-' + process_id
    return process_id


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--dry_run', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--no_cleanup', action='store_true')
    p.add_argument('--noemail', dest='email', action='store_false')
    p.add_argument('--work_dir', type=str, required=True)
    p.add_argument('--process_id', type=str)
    args = p.parse_args(argv)

    load_config()
    log_cfg.set_log_level(logging.INFO)
    log_cfg.add_stdout_handler()

    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    cfg.merge(cfg['sample'])
    process_id = resolve_process_id(args.process_id)
    dd = DataDelivery(args.dry_run, args.work_dir, process_id=process_id, no_cleanup=args.no_cleanup, email=args.email)
    dd.deliver_data()


if __name__ == '__main__':
    main()
