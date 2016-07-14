from os import listdir
from os.path import join, basename

from egcg_core import rest_communication, clarity, util
from egcg_core.constants import ELEMENT_DELIVERED, ELEMENT_FASTQS_DELETED, ELEMENT_USEABLE,\
    ELEMENT_PROJECT_ID, ELEMENT_RUN_NAME, ELEMENT_SAMPLE_INTERNAL_ID, ELEMENT_LANE
from egcg_core.config import cfg
from data_deletion import Deleter


class FastqDeleter(Deleter):
    def __init__(self, work_dir, dry_run=False, deletion_limit=None, project_id=None):
        super().__init__(work_dir, dry_run, deletion_limit)
        self.data_dir = cfg['fastqs']
        self._samples_released_in_lims = None
        self._samples_released_in_app = None
        self.project_id = project_id

    @property
    def samples_released_in_lims(self):
        if self._samples_released_in_lims is None:
            self._samples_released_in_lims = clarity.get_released_samples()
        return set(self._samples_released_in_lims)

    @property
    def samples_released_in_app(self):
        if self._samples_released_in_app is None:
            where = {ELEMENT_DELIVERED: 'yes', ELEMENT_USEABLE: 'yes', ELEMENT_FASTQS_DELETED: 'no'}
            if self.project_id:
                where[ELEMENT_PROJECT_ID] = self.project_id
            self._samples_released_in_app = rest_communication.get_documents(
                'samples',
                quiet=True,
                where=where,  # TODO: do we want useable only?
                projection={ELEMENT_SAMPLE_INTERNAL_ID: 1},
                depaginate=True
            )
        return [s[ELEMENT_SAMPLE_INTERNAL_ID] for s in self._samples_released_in_app]

    def find_fastqs_for_run_element(self, run_element):
        return util.find_fastqs(
            join(self.data_dir, run_element[ELEMENT_RUN_NAME], 'fastq'),
            run_element[ELEMENT_PROJECT_ID],
            run_element[ELEMENT_SAMPLE_INTERNAL_ID],
            lane=run_element[ELEMENT_LANE]
        )

    def deletable_samples(self):
        return sorted(self.samples_released_in_lims & set(self.samples_released_in_app))[:self.deletion_limit]

    def setup_deletion_records(self):
        deletable_samples = self.deletable_samples()
        deletion_records = []

        n_samples = 0
        n_run_elements = 0
        n_fastqs = 0

        for s in deletable_samples:
            n_samples += 1
            run_elements = rest_communication.get_documents(
                'run_elements', quiet=True, where={ELEMENT_SAMPLE_INTERNAL_ID: s}
            )
            for e in run_elements:
                assert e[ELEMENT_SAMPLE_INTERNAL_ID] == s
                fastqs = self.find_fastqs_for_run_element(e)
                if fastqs:
                    n_run_elements += 1
                    n_fastqs += len(fastqs)
                    deletion_records.append(_FastqDeletionRecord(e, fastqs))

        self.debug(
            'Found %s deletable fastqs from %s run elements in %s samples: %s' % (
                n_fastqs, n_run_elements, n_samples, deletion_records
            )
        )
        return deletion_records

    def setup_fastqs_for_deletion(self, deletion_records):
        all_fastqs = []
        for r in deletion_records:
            all_fastqs.extend(self._setup_record_for_deletion(r))

        comparisons = (
            (listdir(self.deletion_dir), [r.run_id for r in deletion_records]),
            (util.find_files(self.deletion_dir, '*', 'fastq', '*'), [r.project_id for r in deletion_records]),
            (util.find_files(self.deletion_dir, '*', 'fastq', '*', '*'), [r.sample_id for r in deletion_records]),
            (util.find_all_fastqs(self.deletion_dir), all_fastqs)
        )
        for expected, observed in comparisons:
            self._compare_lists(set([basename(f) for f in observed]),
                                set([basename(f) for f in expected]))

    def _setup_record_for_deletion(self, del_record):
        deletion_sub_dir = join(
            self.deletion_dir,
            del_record.run_id,
            'fastq',
            del_record.project_id,
            del_record.sample_id
        )
        self._execute('mkdir -p ' + deletion_sub_dir)
        self._execute('mv %s %s' % (' '.join(del_record.fastqs), deletion_sub_dir))
        observed = [basename(f) for f in listdir(deletion_sub_dir)]
        assert all([basename(f) in observed for f in del_record.fastqs])
        return del_record.fastqs

    def mark_sample_as_deleted(self, sample_id):
        self.debug('Updating dataset status for ' + sample_id)
        if not self.dry_run:
            rest_communication.patch_entry(
                'samples', {ELEMENT_FASTQS_DELETED: 'yes'}, ELEMENT_SAMPLE_INTERNAL_ID, sample_id
            )

    def delete_data(self):
        deletion_records = self.setup_deletion_records()
        if self.dry_run or not deletion_records:
            return 0

        self.setup_fastqs_for_deletion(deletion_records)
        for s in set([r.sample_id for r in deletion_records]):
            self.mark_sample_as_deleted(s)

        self.delete_dir(self.deletion_dir)


class _FastqDeletionRecord:
    def __init__(self, run_element, fastqs):
        self.run_element = run_element
        self.fastqs = fastqs
        self.run_id = run_element[ELEMENT_RUN_NAME]
        self.sample_id = run_element[ELEMENT_SAMPLE_INTERNAL_ID]
        self.project_id = run_element[ELEMENT_PROJECT_ID]
        self.lane = run_element[ELEMENT_LANE]

    def __repr__(self):
        return '%s(%s/%s/%s/%s)' % (
            self.__class__.__name__,
            self.run_id,
            self.project_id,
            self.sample_id,
            [basename(f) for f in self.fastqs]
        )
