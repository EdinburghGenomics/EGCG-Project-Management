from argparse import ArgumentParser
from config import load_config
from egcg_core.app_logging import logging_default as log_cfg
from project_report.project_report_latex import ProjectReportLatex


def main(argv=None):
    load_config()
    args = _parse_args(argv)
    log_level = 10 if args.debug else 20
    log_cfg.add_stdout_handler(log_level)
    pr = ProjectReportLatex(args.project_name, args.working_dir)
    if args.output_format == 'tex':
        output_file = pr.generate_tex()
    elif args.output_format == 'pdf':
        output_file = pr.generate_pdf()
    print("Output file generated in {f} ".format(f=output_file))


def _parse_args(argv):
    a = ArgumentParser()
    a.add_argument('-p', '--project_name', dest='project_name', type=str,
                   help='The name of the project to generate a report for')
    a.add_argument('-o', '--output_format', type=str, choices=('tex', 'pdf'), default='pdf')
    a.add_argument('-w', '--working_dir', type=str, help='directory where temporary files will be created')
    a.add_argument('-d', '--debug', action='store_true')
    return a.parse_args(argv)
