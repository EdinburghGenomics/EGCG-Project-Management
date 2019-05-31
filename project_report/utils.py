import datetime
import os
from math import ceil

import pandas as pd
import matplotlib
from egcg_core.util import query_dict

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.collections as mpcollections


def calculate_mean(values):
    return sum(values) / max(len(values), 1)


def min_mean_max(list_values):
    if list_values:
        return 'min: %.1f, avg: %.1f, max: %.1f' % (
            min(list_values),
            calculate_mean(list_values),
            max(list_values)
        )
    else:
        return 'min: 0, mean: 0, max: 0'


def parse_date(date):
    if not date:
        return 'NA'
    return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f').strftime('%d %b %y')


def get_folder_size(folder):
    """
    Get the size of folder provided
    :param folder: The folder to be sized
    :return: the size of the folder
    """
    total_size = os.path.getsize(folder)
    for item in os.listdir(folder):
        itempath = os.path.join(folder, item)
        if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            total_size += get_folder_size(itempath)
    return total_size


def yield_vs_coverage_plot(project_information, working_dir):
    # Extract the data required from project information
    req_to_metrics = {}
    for sample in project_information.sample_names_delivered:
        req = (project_information.get_required_yield(sample), project_information.get_quoted_coverage(sample))
        if req not in req_to_metrics:
            req_to_metrics[req] = {'samples': [], 'clean_yield': [], 'coverage': []}
        all_yield_metrics = [
            sample,
            query_dict(project_information.sample_info(sample), 'data.aggregated.clean_yield_in_gb'),
            query_dict(project_information.sample_info(sample), 'data.coverage.mean')]
        if None not in all_yield_metrics:
            req_to_metrics[req]['samples'].append(all_yield_metrics[0])
            req_to_metrics[req]['clean_yield'].append(all_yield_metrics[1])
            req_to_metrics[req]['coverage'].append(all_yield_metrics[2])

    # generate plots
    list_plots = []
    for req in req_to_metrics:
        df = pd.DataFrame(req_to_metrics[req])
        req_yield, req_cov = req
        max_x = max(df['clean_yield']) + .1 * max(df['clean_yield'])
        max_y = max(df['coverage']) + .1 * max(df['coverage'])
        min_x = min(df['clean_yield']) - .1 * max(df['clean_yield'])
        min_y = min(df['coverage']) - .1 * max(df['coverage'])

        min_x = min((min_x, req_yield - .1 * req_yield))
        min_y = min((min_y, req_cov - .1 * req_cov))

        plt.figure(figsize=(10, 5))
        df.plot(kind='scatter', x='clean_yield', y='coverage')

        plt.xlim(min_x, max_x)
        plt.ylim(min_y, max_y)
        plt.xlabel('Delivered yield (Gb)')
        plt.ylabel('Coverage (X)')

        xrange1 = [(0, req_yield)]
        xrange2 = [(req_yield, max_x)]
        yrange1 = (0, req_cov)
        yrange2 = (req_cov, max_y)

        c1 = mpcollections.BrokenBarHCollection(xrange1, yrange1, facecolor='red', alpha=0.2)
        c2 = mpcollections.BrokenBarHCollection(xrange1, yrange2, facecolor='yellow', alpha=0.2)
        c3 = mpcollections.BrokenBarHCollection(xrange2, yrange1, facecolor='yellow', alpha=0.2)
        c4 = mpcollections.BrokenBarHCollection(xrange2, yrange2, facecolor='green', alpha=0.2)

        ax = plt.gca()
        ax.add_collection(c1)
        ax.add_collection(c2)
        ax.add_collection(c3)
        ax.add_collection(c4)

        plot_outfile = os.path.join(working_dir, 'yield%s_cov%s_plot.png' % (req_yield, req_cov))
        plt.savefig(plot_outfile, bbox_inches='tight', pad_inches=0.2)
        list_plots.append({
            'nb_sample': len(df),
            'req_yield': req_yield,
            'req_cov': req_cov,
            'file': os.path.abspath(plot_outfile)
        })
    return list_plots


def format_list_as_enumeration(l):
    if len(l) > 1:
        return ', '.join([str(s) for s in l[:-1]]) + ' and ' + str(l[-1])
    else:
        return str(l[0])


def calculate_text_size(text):
    """
    Simplistic function that calculate a size of a text based on the number and type of characters.
    It uses an averaged size for lower/upper-case, digits, spaces and underscores.
    It also assumes that the font is using latex "scriptsize" text size.
    """
    text_size = 0
    for c in str(text):
        if c.islower():
            text_size += 13
        elif c.isupper() or c.isdigit() or c == '_':
            text_size += 20
        elif c == ' ':
            text_size += 7
        else:
            text_size += 12
    return text_size/100


def estimate_columns_definition(rows, column_types, minimums=None, extend_to=None, separator=' '):
    """
    Provided with the rows to of the tables and the latex type for the column. this estimates and provides the best fit
    columns definition. It assumes that the font is using latex "scriptsize" text size.
    :param rows: List of lists containing all the text to display in the columns. It assumes that all cell text is to
                be displayed.
    :param column_types: The latex (tabu) types of column to be used in the column definition returned
    :param minimums: The minimum value to be used for the column sizes (in cm)
    :param extend_to: The total size of the sum of column it should be extended to
    :param separator: the character used to separate the column in the definition. Usually space ( ) or pipe (|)
    :return: The column definition to be used in the tabu type tables
    """
    # Starts with minimum column size
    if not minimums:
        column_sizes = [0] * len(rows[0])
    else:
        column_sizes = minimums.copy()
    assert len(column_sizes) == len(column_types), "Inconsistent number of column in columns type and column size"

    # Calculate the minimum column sizes based on the text provided in the rows
    for row in rows:
        for i, cell in enumerate(row):
            c = calculate_text_size(cell)
            if c > column_sizes[i]:
                column_sizes[i] = c
    # Extend the minimum column sizes to match the extended_to parameter
    if extend_to:
        remaining = extend_to - sum(column_sizes)
        if remaining > 0:
            for i in range(len(column_sizes)):
                column_sizes[i] += remaining / len(column_sizes)

    # round up at the first decimal point to have readable values
    # (This will increase the column size to beyond extend_to)
    for i in range(len(column_sizes)):
        column_sizes[i] = ceil(column_sizes[i] * 10) / 10.0
    # Write column def
    column_def = []
    for i, t in enumerate(column_types):
        if 'X' in t:
            column_def.append(t)
        elif 'p' in t:
            column_def.append('p{%scm}' % column_sizes[i])
        elif 'R' in t:
            column_def.append('R{%scm}' % column_sizes[i])
    return separator.join(column_def)
