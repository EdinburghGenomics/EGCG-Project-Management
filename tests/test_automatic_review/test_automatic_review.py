from bin import automatic_review as ar
from tests.test_automatic_review import fake_data
from unittest.mock import patch

ppath = 'bin.automatic_review.'


def test_run_metrics():
    cfg = ar.review_thresholds.get('run')
    for lane in fake_data.run_elements_by_lane_pass:
        assert ar.get_failing_metrics(lane, cfg) == []
    for lane in fake_data.run_elements_by_lane_fail:
        assert len(ar.get_failing_metrics(lane, cfg))


@patch(ppath + 'clarity.get_species_from_sample', return_value='Homo sapiens')
@patch(ppath + 'clarity.get_expected_yield_for_sample', return_value=95000000000)
def test_sample_metrics(mock_yield, mock_species):
    sample_cfg = ar.sample_config(fake_data.samples_fail, 'Homo sapiens')
    assert ar.get_failing_metrics(fake_data.samples_fail, sample_cfg) == [
        'clean_yield_in_gb', 'clean_yield_q30', 'provided_gender'
    ]

    sample_cfg = ar.sample_config(fake_data.samples_pass, 'Homo sapiens')
    assert ar.get_failing_metrics(fake_data.samples_pass, sample_cfg) == []

    sample_cfg = ar.sample_config(fake_data.samples_no_genotype, 'Bos taurus')
    assert ar.get_failing_metrics(fake_data.samples_no_genotype, sample_cfg) == []


@patch(ppath + 'clarity.get_expected_yield_for_sample', return_value=120000000000)
def test_sample_config(mock_yield):
    assert ar.sample_config(fake_data.samples_no_genotype, 'Homo sapiens') == {
        'clean_yield_in_gb': {'comparison': '>', 'value': 160},
        'pc_duplicate_reads': {'comparison': '<', 'value': 35},
        'pc_mapped_reads': {'comparison': '>', 'value': 90},
        'median_coverage': {'comparison': '>', 'value': 40},
        'clean_yield_q30': {'comparison': '>', 'value': 120},
        'provided_gender': {'value': {'key': 'called_gender', 'fallback': 'unknown'}, 'comparison': 'agreeswith'}
    }

    assert ar.sample_config(fake_data.samples_pass, 'Homo sapiens') == {
        'clean_yield_in_gb': {'comparison': '>', 'value': 160},
        'pc_duplicate_reads': {'comparison': '<', 'value': 35},
        'pc_mapped_reads': {'comparison': '>', 'value': 90},
        'median_coverage': {'comparison': '>', 'value': 40},
        'clean_yield_q30': {'comparison': '>', 'value': 120},
        'provided_gender': {'value': {'key': 'called_gender', 'fallback': 'unknown'}, 'comparison': 'agreeswith'},
        'genotype_validation.no_call_seq': {'comparison': '<', 'value': 10},
        'genotype_validation.mismatching_snps': {'comparison': '<', 'value': 5}
    }


@patch(ppath + 'rest_communication.patch_entries')
@patch(ppath + 'clarity.get_expected_yield_for_sample', return_value=95000000000)
def test_automatic_sample_review(mocked_yield, mocked_patch):
    sample_cfg = ar.sample_config(fake_data.samples_pass, 'Homo sapiens')
    r = ar.SampleReviewer(fake_data.samples_pass, sample_cfg, 'Homo sapiens')
    r.patch_entry()
    mocked_patch.assert_called_with(
        'samples',
        payload={'reviewed': 'pass'},
        where={'sample_id': 'LP1251551__B_12'}
    )
    mocked_patch.reset_mock()

    sample_cfg = ar.sample_config(fake_data.samples_fail, 'Homo sapiens')
    r = ar.SampleReviewer(fake_data.samples_fail, sample_cfg, 'Homo sapiens')
    r.patch_entry()
    mocked_patch.assert_called_with(
        'samples',
        payload={
            'reviewed': 'fail',
            ar.ELEMENT_REVIEW_COMMENTS: 'failed due to clean_yield_in_gb, clean_yield_q30, provided_gender'
        },
        where={'sample_id': 'LP1251551__C_04'}
    )
    mocked_patch.reset_mock()

    sample_cfg = ar.sample_config(fake_data.samples_no_genotype, 'Homo sapiens')
    r = ar.SampleReviewer(fake_data.samples_no_genotype, sample_cfg, 'Homo sapiens')
    r.patch_entry()
    mocked_patch.assert_called_with(
        'samples',
        payload={'reviewed': 'genotype missing'},
        where={'sample_id': 'LP1251551__B_12'}
    )
    mocked_patch.reset_mock()

    sample_cfg = ar.sample_config(fake_data.samples_non_human, 'Bos taurus')
    r = ar.SampleReviewer(fake_data.samples_non_human, sample_cfg, 'Bos taurus')
    r.patch_entry()
    mocked_patch.assert_called_with(
        'samples',
        payload={'reviewed': 'pass'},
        where={'sample_id': 'LP1251551__B_12'}
    )


@patch(ppath + 'rest_communication.patch_entries')
def test_automatic_run_review(mocked_patch):
    with patch('bin.automatic_review.rest_communication.get_documents', return_value=fake_data.run_elements_by_lane_pass):
        r = ar.RunReviewer('test')
        r.patch_entry()
        mocked_patch.assert_called_with(
            'run_elements',
            payload={'reviewed': 'pass'},
            where={'run_id': 'test', 'lane': 5}
        )
        mocked_patch.reset_mock()

    with patch(ppath + 'rest_communication.get_documents', return_value=fake_data.run_elements_by_lane_fail):
        r = ar.RunReviewer('test')
        r.patch_entry()
        mocked_patch.assert_called_with(
            'run_elements',
            payload={'reviewed': 'fail', 'review_comments': 'failed due to pc_pass_filter, yield_in_gb'},
            where={'run_id': 'test', 'lane': 1}
        )
