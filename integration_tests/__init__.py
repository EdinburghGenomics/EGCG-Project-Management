import os
from time import sleep
from shutil import rmtree
from unittest.mock import Mock
from collections import defaultdict
from egcg_core import rest_communication, archive_management


class NamedMock(Mock):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name


def setup_delivered_samples(processed_dir, delivered_dir, fastq_dir):
    for d in (processed_dir, delivered_dir, fastq_dir):
        if os.path.isdir(d):
            rmtree(d)

    all_files = defaultdict(list)
    for i in range(1, 4):
        sample_id = 'sample_' + str(i)
        ext_sample_id = 'ext_' + sample_id
        sample_dir = os.path.join(processed_dir, 'a_project', sample_id)
        delivered_dir = os.path.join(delivered_dir, 'a_project', 'a_delivery_date', sample_id)
        sample_fastq_dir = os.path.join(fastq_dir, 'a_run', 'a_project', sample_id)

        os.makedirs(sample_dir)
        os.makedirs(sample_fastq_dir)
        os.makedirs(delivered_dir)

        rest_communication.post_entry(
            'samples',
            {'sample_id': sample_id, 'user_sample_id': ext_sample_id, 'project_id': 'a_project'}
        )
        rest_communication.post_entry(
            'run_elements',
            {'run_element_id': 'a_run_%s_ATGC' % i, 'run_id': 'a_run', 'lane': i, 'barcode': 'ATGC',
             'project_id': 'a_project', 'sample_id': sample_id, 'library_id': 'a_library'}
        )

        for ext in ('.bam', '.vcf.gz'):
            f = os.path.join(sample_dir, ext_sample_id + ext)
            all_files[sample_id].append(f)

        for r in ('1', '2'):
            f = os.path.join(sample_fastq_dir, 'L00%s_R%s.fastq.gz' % (i, r))
            all_files[sample_id].append(f)

        for f in all_files[sample_id]:
            open(f, 'w').close()
            os.link(f, os.path.join(delivered_dir, os.path.basename(f)))
            archive_management.register_for_archiving(f)

    for sample_id in ('sample_1', 'sample_2', 'sample_3'):
        for f in all_files[sample_id]:
            while not archive_management.is_archived(f):
                sleep(10)

    return all_files
