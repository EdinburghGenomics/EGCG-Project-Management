from datetime import datetime
from os import listdir
from os.path import join, isdir
from egcg_core import rest_communication, util, clarity
from egcg_core.constants import ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_PROJECT_ID, ELEMENT_RUN_NAME, ELEMENT_LANE
from egcg_core.config import cfg
from data_deletion import Deleter


class ProcessedSample:

    def __init__(self, sample_data):
        self.sample_data = sample_data
        self._release_date = None

    @property
    def release_date(self):
        if not self._release_date:
            self._release_date = clarity.get_sample_release_date(self.sample_id)
        return self._release_date

    @property
    def sample_id(self):
        return self.sample_data['sample_id']

    @property
    def project_id(self):
        return self.sample_data['project_id']

    @property
    def external_sample_id(self):
        return self.sample_data['external_sample_id']

    def _find_fastqs_for_run_element(self, run_element):
        return util.find_fastqs(
            join(self.raw_data_dir, run_element[ELEMENT_RUN_NAME], 'fastq'),
            run_element[ELEMENT_PROJECT_ID],
            run_element[ELEMENT_SAMPLE_INTERNAL_ID],
            lane=run_element[ELEMENT_LANE]
        )

    def _raw_data_files(self):
        all_fastqs = []
        run_elements = rest_communication.get_documents(
            'run_elements', quiet=True, where={ELEMENT_SAMPLE_INTERNAL_ID: self.sample_id}
        )
        for e in run_elements:
            assert e[ELEMENT_SAMPLE_INTERNAL_ID] == self.sample_id
            fastqs = self._find_fastqs_for_run_element(e)
            if fastqs:
                all_fastqs.extend(fastqs)
            else:
                self.warning(
                    'Found %s fastq files for run %s lane %s sample %s',
                    len(fastqs),
                    e[ELEMENT_RUN_NAME],
                    e[ELEMENT_LANE]
                )
        return  all_fastqs

    def _released_data_folder(self, project_id, sample_id):
        release_folders = util.find_files(self.delivered_data_dir, project_id, '*', sample_id)
        if len(release_folders) != 1:
            self.warning(
                'Found %s deletable directories for sample %s: %s',
                len(release_folders),
                sample_id,
                release_folders
            )
        else:
            return release_folders[0]

    def _processed_data_folder(self):
        files_to_delete = []
        files_to_search = [
            '{ext_s_id}_R1.fastq.gz', '{ext_s_id}_R2.fastq.gz',
            '{ext_s_id}.bam', '{ext_s_id}.bam.bai',
            '{ext_s_id}.vcf.gz', '{ext_s_id}.vcf.gz.tbi',
            '{ext_s_id}.g.vcf.gz', '{ext_s_id}.g.vcf.gz.tbi'
        ]
        for f in files_to_search:
            file_to_delete = util.find_file(
                self.processed_data_dir,
                self.project_id, self.sample_id,
                f.format(ext_s_id=self.external_sample_id)
            )
            if file_to_delete:
                files_to_delete.append(file_to_delete)
        return files_to_delete


class DeliveredDataDeleter(Deleter):

    def __init__(self, work_dir, dry_run=False, deletion_limit=None, manual_delete=None, sample_ids=None):
        super().__init__(work_dir, dry_run, deletion_limit)
        self.raw_data_dir = cfg['data_deletion']['fastqs']
        self.processed_data_dir = cfg['data_deletion']['processed_data']
        self.delivered_data_dir = cfg['data_deletion']['delivered_data']

        if manual_delete is None:
            manual_delete = []
        self.list_samples = manual_delete
        self.limit_samples = sample_ids

    def deletable_samples(self):
        manual_samples = []
        if self.list_samples:
            manual_samples = rest_communication.get_documents(
                'aggregate/samples',
                quiet=True,
                match={'$or': [{'sample_id': s} for s in self.list_samples]}
            )
            manual_samples = [ProcessedSample(s) for s in manual_samples]
        return sorted(manual_samples + self._auto_deletable_samples(), key=lambda e: e.sample_data['sample_id'])

    def _auto_deletable_samples(self):
        samples = []
        sample_records = rest_communication.get_documents(
            'aggregate/samples',
            quiet=True,
            match={'$or': [{'proc_status': 'finished'}, {'proc_status': 'aborted'}]}
        )
        for r in sample_records:
            s = ProcessedSample(r)
            if s.release_date and self._old_enough_for_deletion(s.release_date):
                samples.append(s)
        return samples

    def mark_sample_as_deleted(self, sample_id, proc_id):
        if not proc_id:
            self.warning('No pipeline process found for ' + sample_id)
        else:
            rest_communication.patch_entry(
                'analysis_driver_procs',
                {'status': 'deleted'},
                'proc_id', proc_id
            )

    def setup_samples_for_deletion(self, sample_records):
        for s in sample_records:
            self._setup_sample_for_deletion(s['project_id'], s['sample_id'])

    @classmethod
    def _old_enough_for_deletion(cls, date_run, age_threshold=90):
        year, month, day = date_run.split('-')
        age = cls._now() - datetime(int(year), int(month), int(day))
        return age.days > age_threshold

    def _setup_sample_for_deletion(self, project_id, sample_id):
        release_folder = self._released_data_folder(project_id, sample_id)
        if release_folder:
            deletable_data = join(self.deletion_dir, project_id)
        #    self._execute('mkdir -p ' + deletable_data)
        #    self._execute('mv %s %s' % (release_folder, deletable_data))

    def _try_delete_empty_dir(self, d):
        if isdir(d) and not listdir(d):
            self._execute('rm -r ' + d)

    def _cleanup_empty_dirs(self, project_id):
        project_folder = join(self.data_dir, project_id)
        for release_dir in listdir(project_folder):
            self._try_delete_empty_dir(join(project_folder, release_dir))
        self._try_delete_empty_dir(project_folder)

    def delete_data(self):
        deletable_samples = self.deletable_samples()
        if self.limit_samples:
            deletable_samples = [s for s in deletable_samples if s.sample_data['sample_id'] in self.limit_samples]

        sample_ids = [e[ELEMENT_SAMPLE_INTERNAL_ID] for e in deletable_samples]
        self.debug('Found %s samples for deletion: %s' % (len(deletable_samples), sample_ids))

        if self.dry_run or not deletable_samples:
            return 0

        #self.setup_samples_for_deletion(deletable_samples)
        #samples_to_delete = []
        #for p in listdir(self.deletion_dir):
        #    samples_to_delete.extend(listdir(join(self.deletion_dir, p)))
        #self._compare_lists(samples_to_delete, sample_ids)

        #for s in deletable_samples:
        #    self.mark_sample_as_deleted(s['sample_id'], s.get('most_recent_proc', {}).get('proc_id'))

        #self.delete_dir(self.deletion_dir)
        #for p in set([e[ELEMENT_PROJECT_ID] for e in deletable_samples]):
        #    self._cleanup_empty_dirs(p)
