import argparse
import datetime
import logging
import os
import sys
from collections import defaultdict

import shutil
from egcg_core import executor
from egcg_core import rest_communication, clarity
from egcg_core.app_logging import AppLogger, logging_default as log_cfg
from egcg_core.config import EnvConfiguration
from egcg_core.exceptions import EGCGError
from egcg_core.util import find_fastqs
from egcg_core.constants import ELEMENT_NB_READS_CLEANED, ELEMENT_RUN_NAME, ELEMENT_PROJECT_ID, ELEMENT_LANE, \
    ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_SAMPLE_EXTERNAL_ID




cfg = EnvConfiguration(
    [
        os.getenv('EGCGCONFIG'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'example_data_delivery.yaml')
    ]
)



hs_list_files = [
    '{ext_sample_id}.g.vcf.gz',
    '{ext_sample_id}.g.vcf.gz.tbi',
    '{ext_sample_id}.vcf.gz',
    '{ext_sample_id}.vcf.gz.tbi',
    '{ext_sample_id}.bam',
    '{ext_sample_id}.bam.bai'
]

other_list_files = []


class DataDelivery(AppLogger):
    def __init__(self, dry_run, work_dir):
        self.all_commands_for_cluster = []
        self.dry_run = dry_run
        self.all_samples_values = []
        self.sample2species = {}
        self.work_dir = work_dir
        today = datetime.date.today().isoformat()
        self.staging_dir = os.path.join(self.work_dir, 'data_delivery_' + today)

    def get_deliverable_projects_samples(self, project_id=None, sample_id=None):
        project_to_samples = defaultdict(list)
        # Get sample that have been review from REST API but not marked as delivered
        where_clause = {"useable": "yes"}
        if project_id:
            where_clause["project_id"] = project_id
        if sample_id:
            where_clause["sample_id"] = sample_id
        # These samples are useable but could have been delivered already so need to check
        samples = rest_communication.get_documents(
                'samples',
                depaginate=True,
                embedded={"analysis_driver_procs": 1, "run_elements": 1},
                where=where_clause
        )
        for sample in samples:
            processes = sample.get('analysis_driver_procs', [{}])
            processes.sort(
                key=lambda x: datetime.datetime.strptime(x.get('_created', '01_01_1970_00:00:00'), '%d_%m_%Y_%H:%M:%S'))
            if not processes[-1].get('status', 'new') == 'finished':
                raise EGCGError("Reviewed sample %s not marked as finished" % sample.get('sample_id'))
            if sample.get('delivered', 'no') == 'no':
                project_to_samples[sample.get('project_id')].append(sample)
                self.all_samples_values.append(sample)
        return project_to_samples

    def stage_data(self, project, sample):
        # Create staging_directory
        today = datetime.date.today().isoformat()
        sample_dir = os.path.join(self.staging_dir, project, today, sample.get(ELEMENT_SAMPLE_INTERNAL_ID))
        os.makedirs(sample_dir, exist_ok=True)

        # Find the fastq files
        self._stage_fastq_files(sample, sample_dir)

        # Find the analysed files
        self._stage_analysed_files(sample, sample_dir)
        return sample_dir

    def _stage_fastq_files(self, sample, sample_dir):
        delivery_type = 'merged'
        original_fastq_files = self._get_fastq_file_for_sample(sample)
        external_sample_id = sample.get(ELEMENT_SAMPLE_EXTERNAL_ID)

        if delivery_type == 'merged':
            if len(original_fastq_files) == 1:
                r1_file, r2_file = original_fastq_files[0]
                self._link_file_to_sample_folder(r1_file, sample_dir, rename=external_sample_id + '_R1.fastq.gz')
                self._link_file_to_sample_folder(r2_file, sample_dir, rename=external_sample_id + '_R2.fastq.gz')
                self._link_file_to_sample_folder(r1_file + '.md5', sample_dir,
                                                 rename=external_sample_id + '_R1.fastq.gz.md5')
                self._link_file_to_sample_folder(r2_file + '.md5', sample_dir,
                                                 rename=external_sample_id + '_R2.fastq.gz.md5')
                # retrieve fastqc files
            else:
                r1_files = [r1 for r1, r2 in original_fastq_files]
                r2_files = [r2 for r1, r2 in original_fastq_files]
                self._on_cluster_concat_file_to_sample(r1_files, sample_dir, rename=external_sample_id + '_R1.fastq.gz')
                self._on_cluster_concat_file_to_sample(r2_files, sample_dir, rename=external_sample_id + '_R2.fastq.gz')
        else:
            fastq_folder = os.path.join(sample_dir, 'raw_data')
            os.makedirs(fastq_folder)
            for r1, r2 in original_fastq_files:
                self._link_file_to_sample_folder(r1, fastq_folder, rename=external_sample_id + '_R1.fastq.gz')
                self._link_file_to_sample_folder(r2, fastq_folder, rename=external_sample_id + '_R2.fastq.gz')
                self._link_file_to_sample_folder(r1 + '.md5', fastq_folder, rename=external_sample_id + '_R1.fastq.gz')
                self._link_file_to_sample_folder(r2 + '.md5', fastq_folder, rename=external_sample_id + '_R2.fastq.gz')
                self._link_file_to_sample_folder(r1.replace('.fastq.gz', '_fastqc.html'), fastq_folder,
                                                 rename=external_sample_id + '_R1_fastqc.html')
                self._link_file_to_sample_folder(r2.replace('.fastq.gz', '_fastqc.html'), fastq_folder,
                                                 rename=external_sample_id + '_R2_fastqc.html')

    def _stage_analysed_files(self, sample, sample_dir):
        sample_id = sample.get(ELEMENT_SAMPLE_INTERNAL_ID)
        external_sample_id = sample.get(ELEMENT_SAMPLE_EXTERNAL_ID)

        project_id = sample.get(ELEMENT_PROJECT_ID)
        origin_sample_dir = os.path.join(cfg.query('delivery_source'), project_id, sample_id)

        if not os.path.isdir(origin_sample_dir):
            raise EGCGError('Directory for sample %s in project %s does not exist' % (sample_id, project_id))
        list_of_file_to_move = self.get_list_of_file_to_move(sample_name=sample_id, external_sample_name=external_sample_id)
        for file_to_move in list_of_file_to_move:
            origin_file = os.path.join(origin_sample_dir, file_to_move)
            if not os.path.isfile(origin_file):
                raise EGCGError('File %s for sample %s does not exist' % (file_to_move, sample))
            self._link_file_to_sample_folder(origin_file, sample_dir)

    def summarise_metrics_per_sample(self, project_id, delivery_folder):
        headers = ['Project', 'Sample Id', 'User sample id', 'Read pair sequenced', 'Yield', 'Yield Q30',
                   'Nb reads in bam', 'mapping rate', 'properly mapped reads rate', 'duplicate rate',
                   'Mean coverage', 'Callable bases rate', 'Delivery folder']
        headers_not_human = ['Project', 'Sample Id', 'User sample id', 'Read pair sequenced', 'Yield', 'Yield Q30',
                             'Delivery folder']
        lines = []
        for sample in self.all_samples_values:
            # TODO: Aggregation is done here until we can do the filtering on the REST API
            if sample.get('project_id') == project_id:
                res = [sample.get('project_id'), sample.get('sample_id'), sample.get('user_sample_id')]
                clean_reads = sum([int(e.get('clean_reads', '0')) for e in sample.get('run_elements') if
                                   e.get('useable') == 'yes'])
                clean_bases_r1 = sum([int(e.get('clean_bases_r1', '0')) for e in sample.get('run_elements') if
                                      e.get('useable') == 'yes'])
                clean_bases_r2 = sum([int(e.get('clean_bases_r2', '0')) for e in sample.get('run_elements') if
                                      e.get('useable') == 'yes'])
                clean_q30_bases_r1 = sum(int(e.get('clean_q30_bases_r1', '0')) for e in sample.get('run_elements') if
                                         e.get('useable') == 'yes')
                clean_q30_bases_r2 = sum(int(e.get('clean_q30_bases_r2', '0')) for e in sample.get('run_elements') if
                                         e.get('useable') == 'yes')
                res.append(str(clean_reads))
                res.append(str((clean_bases_r1 + clean_bases_r2) / 1000000000))
                res.append(str((clean_q30_bases_r1 + clean_q30_bases_r2) / 1000000000))
                theoritical_cov = (clean_bases_r1 + clean_bases_r2) / 3200000000.0
                if self.get_sample_species(sample.get('sample_id')) == 'Homo sapiens':
                    tr = sample.get('bam_file_reads', 0)
                    mr = sample.get('mapped_reads', 0)
                    dr = sample.get('duplicate_reads', 0)
                    pmr = sample.get('properly_mapped_reads', 0)
                    if not tr:
                        raise EGCGError('Sample %s has no total number of reads' % sample.get('sample_id'))
                    res.append(str(tr))
                    res.append(str(float(mr) / float(tr) * 100))
                    res.append(str(float(pmr) / float(tr) * 100))
                    res.append(str(float(dr) / float(tr) * 100))
                    theoritical_cov = theoritical_cov * (float(mr) / float(tr)) * (1 - (float(dr) / float(tr)))
                    # res.append(str(theoritical_cov))
                    res.append(str(sample.get('median_coverage', 0)))
                    res.append(str(sample.get('pc_callable', 0) * 100))
                else:
                    headers = headers_not_human
                res.append(os.path.basename(delivery_folder))
                lines.append('\t'.join(res))
        return headers, lines

    def _execute(command, **kwargs):
        exit_status = executor.execute([command], **kwargs).join()
        if exit_status != 0:
            raise EGCGError('command %s exited with status %s' % (command, exit_status))

    def _link_file_to_sample_folder(self, file_to_link, sample_folder, rename=None):
        if rename is None:
            rename = os.path.basename(file_to_link)
        command = 'ln %s %s' % (file_to_link, os.path.join(sample_folder, rename))
        self._execute(command, env='local')

    def _on_cluster_concat_file_to_sample(self, list_files, sample_folder, rename):
        command = 'cat %s > %s' % (' '.join(list_files), os.path.join(sample_folder, rename))
        command += '; md5sum %s > %s' % (
        os.path.join(sample_folder, rename), os.path.join(sample_folder, rename) + '.md5')
        command += '; fastqc %s ' % (os.path.join(sample_folder, rename))
        self.all_commands_for_cluster.append(command)

    def get_sample_species(self, sample_name):
        if sample_name not in self.sample2species:
            self.sample2species[sample_name] = clarity.get_species_from_sample(sample_name)
        return self.sample2species.get(sample_name)

    def get_list_of_file_to_move(self, sample_name, external_sample_name):
        species = self.get_sample_species(sample_name)
        if species is None:
            raise EGCGError('No species information found in the LIMS for ' + sample_name)
        elif species == 'Homo sapiens':
            list_of_file = hs_list_files
        else:
            list_of_file = other_list_files
        final_list = []
        for f in list_of_file:
            final_list.append(f.format(ext_sample_id=external_sample_name))
            final_list.append(f.format(ext_sample_id=external_sample_name) + '.md5')
        return final_list

    def _get_fastq_file_for_sample(self, sample):
        fastqs_files = {}
        for run_element in sample.get('run_elements'):
            if int(run_element.get(ELEMENT_NB_READS_CLEANED, 0)) > 0:
                local_fastq_dir = os.path.join(cfg['input_dir'], run_element.get(ELEMENT_RUN_NAME), 'fastq')
                fastqs = find_fastqs(local_fastq_dir, run_element.get(ELEMENT_PROJECT_ID),
                                     sample.get(ELEMENT_SAMPLE_INTERNAL_ID), run_element.get(ELEMENT_LANE))
                fastqs_files[run_element.id] = tuple(sorted(fastqs))
        return fastqs_files

    def mark_samples_as_released(self, samples):
        for sample_name in samples:
            rest_communication.patch_entry('samples', payload={'delivered': 'yes'}, id_field='sample_id',
                                           element_id=sample_name)
        clarity.route_samples_to_delivery_workflow(samples)

    def mark_only(self, project_id=None, sample_id=None):
        project_to_samples = self.get_deliverable_projects_samples(project_id, sample_id)
        all_samples = []
        for project in project_to_samples:
            samples = project_to_samples.get(project)
            for sample in samples:
                all_samples.append(sample)
        if self.dry_run:
            self.info('Mark %s samples as delivered' % len(all_samples))
        else:
            self.mark_samples_as_released(all_samples)

    def write_metrics_file(self, project, delivery_folder):
        delivery_dest = cfg.query('delivery_dest')
        header, lines = self.summarise_metrics_per_sample(project, delivery_folder)
        summary_metrics_file = os.path.join(delivery_dest, project, 'summary_metrics.csv')
        if os.path.isfile(summary_metrics_file):
            with open(summary_metrics_file, 'a') as open_file:
                open_file.write('\n'.join(lines) + '\n')
        else:
            with open(summary_metrics_file, 'w') as open_file:
                open_file.write('\t'.join(header) + '\n')
                open_file.write('\n'.join(lines) + '\n')

    def run_aggregate_commands(self):
        exit_status = executor.execute(self.all_commands_for_cluster)
        if exit_status != 0 :
            raise EGCGError('Aggregation command failed with status %s'%exit_status)


    def deliver_data(self, project_id=None, sample_id=None):
        delivery_dest = cfg.query('delivery_dest')
        project_to_samples = self.get_deliverable_projects_samples(project_id, sample_id)
        project_to_delivery_folder = {}
        sample2stagedirectory={}
        for project in project_to_samples:
            for sample in project_to_samples.get(project):
                stage_directory = self.stage_data(project, sample)
                sample2stagedirectory[sample] = stage_directory

        if not self.dry_run:
            self.run_aggregate_commands()

        if self.dry_run:
            pass
        else:
            for project in project_to_samples:
                # Create the batch directory
                today = datetime.date.today().isoformat()
                batch_delivery_folder = os.path.join(delivery_dest, project, today)
                os.makedirs(batch_delivery_folder)
                # move all the staged sample directory
                project_to_delivery_folder[project] = batch_delivery_folder
                for sample in project_to_samples.get(project):
                    shutil.move(sample2stagedirectory.get(sample), batch_delivery_folder)
                self.write_metrics_file(project, batch_delivery_folder)
            self.mark_samples_as_released(list(sample2stagedirectory))

        # TODO: Generate project report


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry_run', action='store_true')
    p.add_argument('--debug', action='store_true')
    p.add_argument('--work_dir', type=str, required=True)
    p.add_argument('--mark_only', action='store_true')
    p.add_argument('--project_id', type=str)
    p.add_argument('--sample_id', type=str)
    args = p.parse_args()

    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)
        log_cfg.add_handler(logging.StreamHandler(stream=sys.stdout))

    cfg.merge(cfg['sample'])
    dd = DataDelivery(args.dry_run, args.work_dir)
    if args.mark_only:
        dd.mark_only(project_id=args.project_id, sample_id=args.sample_id)
    else:
        dd.deliver_data(project_id=args.project_id, sample_id=args.sample_id)


if __name__ == '__main__':
    main()
