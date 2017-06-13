passing_lane = {
    'run_id': 'a_run', 'pc_q30': 83.01, 'lane_number': 1, 'pc_pass_filter': 75.27, 'yield_in_gb': 140.29
}

failing_lanes = [
    {'run_id': 'a_run', 'pc_q30': 77.14, 'pc_pass_filter': 39.87, 'yield_in_gb': 74.30, 'lane_number': 2},
    {'run_id': 'a_run', 'pc_q30': 74.97, 'pc_pass_filter': 40.17, 'yield_in_gb': 74.86, 'lane_number': 3}
]


failing_sample = {
    'pc_mapped_reads': 97.18,
    'provided_gender': 'female',
    'clean_yield_in_gb': 114.31,
    'sample_id': 'LP1251551__C_04',
    'called_gender': 'male',
    'genotype_validation': {'mismatching_snps': 3, 'no_call_chip': 1, 'no_call_seq': 0, 'matching_snps': 28},
    'pc_duplicate_reads': 16.20,
    'median_coverage': 30.156,
    'clean_yield_q30': 89.33
}


passing_sample = {
    'sample_id': 'LP1251551__B_12',
    'genotype_validation': {'mismatching_snps': 1, 'no_call_seq': 0, 'no_call_chip': 3, 'matching_snps': 28},
    'provided_gender': 'female',
    'clean_yield_in_gb': 120.17,
    'clean_yield_q30': 95.82,
    'called_gender': 'female',
    'median_coverage': 30.34,
    'pc_mapped_reads': 95.62,
    'pc_duplicate_reads': 20
}


sample_no_genotype = {
    'sample_id': 'LP1251551__B_12',
    # 'genotype_validation': None,
    'provided_gender': 'female',
    'clean_yield_in_gb': 120.17,
    'clean_yield_q30': 95.82,
    'called_gender': 'female',
    'median_coverage': 30.34,
    'pc_mapped_reads': 95.62,
    'pc_duplicate_reads': 20
}

non_human_sample = {
    'sample_id': 'LP1251551__B_12',
    'clean_yield_in_gb': 120.17,
    'clean_yield_q30': 95.82,
    'median_coverage': 30.34,
    'pc_mapped_reads': 95.62,
    'pc_duplicate_reads': 20
}
