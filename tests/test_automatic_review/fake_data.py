run_elements_by_lane_pass = [
    {'pc_q30': 83.86, 'lane_number': 7, 'pc_pass_filter': 75.71, 'yield_in_gb': 141.10},
    {'pc_q30': 83.01, 'lane_number': 1, 'pc_pass_filter': 75.27, 'yield_in_gb': 140.29},
    {'pc_q30': 81.42, 'lane_number': 2, 'pc_pass_filter': 69.93, 'yield_in_gb': 130.33},
    {'pc_q30': 82.29, 'lane_number': 4, 'pc_pass_filter': 75.54, 'yield_in_gb': 140.78},
    {'pc_q30': 82.29, 'lane_number': 3, 'pc_pass_filter': 73.73, 'yield_in_gb': 137.40},
    {'pc_q30': 83.28, 'lane_number': 6, 'pc_pass_filter': 77.19, 'yield_in_gb': 143.85},
    {'pc_q30': 85.08, 'lane_number': 8, 'pc_pass_filter': 81.57, 'yield_in_gb': 152.01},
    {'pc_q30': 83.27, 'lane_number': 5, 'pc_pass_filter': 77.09, 'yield_in_gb': 143.67}
]

run_elements_by_lane_fail = [
    {'pc_q30': 71.61, 'pc_pass_filter': 39.46, 'yield_in_gb': 73.55, 'lane_number': 4},
    {'pc_q30': 74.97, 'pc_pass_filter': 40.17, 'yield_in_gb': 74.86, 'lane_number': 2},
    {'pc_q30': 74.37, 'pc_pass_filter': 39.41, 'yield_in_gb': 73.44, 'lane_number': 3},
    {'pc_q30': 70.71, 'pc_pass_filter': 38.36, 'yield_in_gb': 71.50, 'lane_number': 6},
    {'pc_q30': 63.71, 'pc_pass_filter': 17.99, 'yield_in_gb': 33.52, 'lane_number': 7},
    {'pc_q30': 70.56, 'pc_pass_filter': 36.97, 'yield_in_gb': 68.91, 'lane_number': 5},
    {'pc_q30': 73.65, 'pc_pass_filter': 38.85, 'yield_in_gb': 72.41, 'lane_number': 8},
    {'pc_q30': 77.14, 'pc_pass_filter': 39.87, 'yield_in_gb': 74.30, 'lane_number': 1}
]


samples_fail = {
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


samples_pass = {
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


samples_no_genotype = {
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

samples_non_human = {
    'sample_id': 'LP1251551__B_12',
    'clean_yield_in_gb': 120.17,
    'clean_yield_q30': 95.82,
    'median_coverage': 30.34,
    'pc_mapped_reads': 95.62,
    'pc_duplicate_reads': 20
}
