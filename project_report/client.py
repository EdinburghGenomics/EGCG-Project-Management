from argparse import ArgumentParser
from config import load_config
from egcg_core.app_logging import logging_default as log_cfg
from project_report import ProjectReport


def main():
    load_config()
    args = _parse_args()
    log_level = 10 if args.debug else 20
    log_cfg.add_stdout_handler(log_level)
    pr = ProjectReport(args.project_name)
    pr.generate_report(args.output_format)


def _parse_args():
    a = ArgumentParser()
    a.add_argument('-p', '--project_name', dest='project_name', type=str,
                   help='The name of the project to generate a report for')
    a.add_argument('-o', '--output_format', type=str, choices=('html', 'pdf'), default='pdf')
    a.add_argument('-d', '--debug', action='store_true')
    return a.parse_args()
