from unittest.mock import patch
from datetime import datetime


finished_proc = {'status': 'finished'}
aborted_proc = {'status': 'aborted'}
running_proc = {'status': 'running'}


# none of these should be deletable
fake_run_elements_no_procs = [
    {'run_element_id': 'unreviewed_run_element', 'run_id': 'unreviewed_run', 'review_statuses': ['not reviewed']},
    {'run_element_id': 'passed_run_element', 'run_id': 'passed_run', 'review_statuses': ['pass']},
    {'run_element_id': 'failed_run_element', 'run_id': 'failed_run', 'review_statuses': ['fail']}
]


fake_run_elements_procs_running = [
    {
        # not deletable
        'run_element_id': 'unreviewed_run_element',
        'run_id': 'unreviewed_run',
        'review_statuses': ['not reviewed'],
        'most_recent_proc': running_proc
    },
    {
        # not deletable
        'run_element_id': 'partially_unreviewed_run_element',
        'run_id': 'partially_unreviewed_run',
        'review_statuses': ['not reviewed', 'pass'],
        'most_recent_proc': running_proc
    },
    {
        # not deletable
        'run_element_id': 'passed_run_element',
        'run_id': 'passed_run',
        'review_statuses': ['pass'],
        'most_recent_proc': running_proc
    },
    {
        # not deletable
        'run_element_id': 'failed_run_element',
        'run_id': 'failed_run',
        'review_statuses': ['fail'],
        'most_recent_proc': running_proc
    }
]


fake_run_elements_procs_complete = [
    {
        # not deletable
        'run_element_id': 'unreviewed_run_element',
        'run_id': 'unreviewed_run',
        'review_statuses': ['not reviewed', 'pass'],
        'most_recent_proc': finished_proc
    },
    {
        # deletable
        'run_element_id': 'passed_run_element',
        'run_id': 'passed_run',
        'review_statuses': ['pass'],
        'most_recent_proc': finished_proc
    },
    {
        # deletable
        'run_element_id': 'failed_run_element',
        'run_id': 'failed_run',
        'review_statuses': ['fail'],
        'most_recent_proc': aborted_proc
    }

]

patch_get = 'egcg_core.rest_communication.get_documents'
patched_patch_entry = patch('data_deletion.raw_data.rest_communication.patch_entry')
patched_deletable_runs = patch(
    'data_deletion.raw_data.RawDataDeleter.deletable_runs',
    return_value=[{'run_id': 'deletable_run', 'most_recent_proc': {'proc_id': 'most_recent_proc'}}]
)
patched_now = patch('data_deletion.Deleter._now', return_value=datetime(2000, 12, 31))



patched_clarity_get_samples = patch(
    'data_deletion.fastq.clarity.get_released_samples', return_value=['deletable_sample', 'deletable_sample_2']
)

patched_deletable_samples = patch(
    'data_deletion.fastq.rest_communication.get_documents',
    return_value=[{'sample_id': 'deletable_sample'}]
)
