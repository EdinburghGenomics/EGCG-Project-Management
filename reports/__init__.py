from os import path
from egcg_core.app_logging import logging_default as log_cfg, AppLogger

try:
    from weasyprint import HTML
except ImportError:
    weasyprint_logger = log_cfg.get_logger('weasyprint', 40)
    weasyprint_logger.info('WeasyPrint is not installed. PDF output will be unavailable')
    HTML = None




class Report(AppLogger):

    def __init__(self, output_dir, report_file_name):
        self.output_dir = output_dir
        self.report_file_name = report_file_name


    def generate_report(self, output_format):
        report_file = path.join(self.output_dir, self.report_file_name+'.'+output_format)
        h = self.get_html_content()
        if output_format == 'html':
            open(report_file, 'w').write(h)
        else:
            HTML(string=h).write_pdf(report_file)

    def get_html_content(self):
        pass

