from bin import report_runs
from pytest import raises
from unittest.mock import patch, call


@patch('egcg_core.rest_communication.get_documents')
def test_run_status_data(mocked_get_docs):
    report_runs.cache['run_status_data'] = {}
    fake_rest_data = [{'run_id': 1, 'some': 'data'}, {'run_id': 2, 'some': 'data'}]
    mocked_get_docs.return_value = fake_rest_data

    obs = report_runs.run_status_data(2)
    assert obs == fake_rest_data[1]
    assert report_runs.cache['run_status_data'] == {1: fake_rest_data[0], 2: fake_rest_data[1]}
    mocked_get_docs.assert_called_with('lims/status/run_status')
    assert mocked_get_docs.call_count == 1

    obs = report_runs.run_status_data(1)
    assert obs == fake_rest_data[0]
    assert mocked_get_docs.call_count == 1  # not called again


@patch('egcg_core.rest_communication.get_documents')
def test_run_element_data(mocked_get_docs):
    report_runs.cache['run_elements_data'] = {}

    mocked_get_docs.return_value = 'some data'
    obs = report_runs.run_elements_data('a_run')
    assert obs == 'some data'
    assert report_runs.cache['run_elements_data'] == {'a_run': 'some data'}
    mocked_get_docs.assert_called_with('run_elements', where={'run_id': 'a_run'})
    assert mocked_get_docs.call_count == 1

    obs = report_runs.run_elements_data('a_run')
    assert obs == 'some data'
    assert mocked_get_docs.call_count == 1  # not called again


@patch('egcg_core.rest_communication.get_document')
def test_sample_data(mocked_get_docs):
    report_runs.cache['sample_data'] = {}

    mocked_get_docs.return_value = 'some data'
    obs = report_runs.sample_data('a_sample')
    assert obs == 'some data'
    assert report_runs.cache['sample_data'] == {'a_sample': 'some data'}
    mocked_get_docs.assert_called_with('samples', where={'sample_id': 'a_sample'})
    assert mocked_get_docs.call_count == 1

    obs = report_runs.sample_data('a_sample')
    assert obs == 'some data'
    assert mocked_get_docs.call_count == 1  # not called again


@patch('bin.report_runs.logger')
def test_get_run_success(mocked_logger):
    report_runs.cache['run_elements_data'] = {
        'a_run': [
            {'lane': 1, 'reviewed': 'pass'},
            {'lane': 2, 'reviewed': 'fail', 'review_comments': 'Failed due to things'},
            {'lane': 3, 'reviewed': 'fail', 'review_comments': 'Failed due to thungs'},
        ]
    }

    assert report_runs.get_run_success('a_run') == {
        'name': 'a_run',
        'failed_lanes': 2,
        'details': ['lane 2: things', 'lane 3: thungs']
    }
    assert mocked_logger.mock_calls == [
        call.info('a_run: 2 lanes failed:'),
        call.info('lane 2: things'),
        call.info('lane 3: thungs')
    ]
    report_runs.cache['run_elements_data']['a_run'].append(
        {'lane': 1, 'reviewed': 'fail', 'review_comments': 'this will break stuff'}
    )

    with raises(ValueError) as e:
        report_runs.get_run_success('a_run')

    assert str(e.value) == 'More than one review status for lane 1 in run a_run'


@patch('bin.report_runs.send_html_email')
@patch('bin.report_runs.get_run_success', return_value={'name': 'successful_run', 'failed_lanes': 0, 'details': []})
@patch('bin.report_runs.today', return_value='today')
def test_report_runs(mocked_today, mocked_run_success, mocked_email):
    report_runs.cfg.content = {'run_report': {'email_notification': {}}}
    report_runs.cache['run_status_data'] = {
        'a_run': {
            'run_status': 'RunCompleted',
            'sample_ids': ['passing', 'no_data', 'poor_yield', 'poor_coverage', 'poor_yield_and_coverage']
        },
        'errored_run': {
            'run_status': 'RunErrored',
            'sample_ids': ['passing', 'no_data', 'poor_yield', 'poor_coverage', 'poor_yield_and_coverage']
        }
    }

    report_runs.cache['sample_data'] = {
        'passing': {'aggregated': {'clean_pc_q30': 75, 'clean_yield_in_gb': 2, 'from_run_elements': {'mean_coverage': 4}}},
        'no_data': {'aggregated': {}},
        'poor_yield': {'aggregated': {'clean_pc_q30': 75, 'clean_yield_in_gb': 1, 'from_run_elements': {'mean_coverage': 4}}},
        'poor_coverage': {'aggregated': {'clean_pc_q30': 75, 'clean_yield_in_gb': 2, 'from_run_elements': {'mean_coverage': 3}}},
        'poor_yield_and_coverage': {'aggregated': {'clean_pc_q30': 75, 'clean_yield_in_gb': 1, 'from_run_elements': {'mean_coverage': 3}}},
    }

    for s in report_runs.cache['sample_data'].values():
        s['required_yield'] = 2000000000
        s['required_coverage'] = 4

    report_runs.report_runs(['a_run', 'errored_run'])
    mocked_email.assert_any_call(
        subject='Run report today',
        email_template=report_runs.email_template_report,
        runs=[
            {'name': 'successful_run', 'failed_lanes': 0, 'details': []},
            {'name': 'errored_run', 'failed_lanes': 8, 'details': ['RunErrored']}
        ]
    )

    exp_failing_samples = [
        {'id': 'no_data', 'reason': 'No data'},
        {'id': 'poor_yield_and_coverage', 'reason': 'Not enough data: yield (1.0 < 2) and coverage (3 < 4)'}
    ]

    mocked_email.assert_any_call(
        subject='Sequencing repeats today',
        email_template=report_runs.email_template_repeats,
        runs=[
            {'name': 'a_run', 'repeat_count': 2, 'repeats': exp_failing_samples},
            {'name': 'errored_run', 'repeat_count': 2, 'repeats': exp_failing_samples}
        ]
    )
