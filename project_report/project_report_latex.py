import os
import re

import yaml
from pylatex import Document, Section, Subsection, Package, PageStyle, Head, MiniPage, StandAloneGraphic, Foot, \
    NewPage, HugeText, Tabu, Subsubsection, FootnoteText, LineBreak, NoEscape, LongTabu, Hyperref, Marker, \
    MultiColumn, MediumText, LargeText
from pylatex.base_classes import Environment, ContainerCommand
from pylatex.section import Paragraph
from pylatex.utils import italic, bold
from project_report.project_information import yield_vs_coverage_plot, ProjectReportInformation


# Load all source texts from yaml.
_report_text_yaml_file = os.path.join(os.path.dirname(__file__), 'report_texts.yaml')
with open(_report_text_yaml_file) as open_file:
    report_text = yaml.load(open_file)

EG_logo_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'EG_logo_blackonwhite_300dpi.png')
UoE_EG_logo_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'UoE_EG_logo.png')
Uni_logo_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'UoE_Stacked_Logo_CMYK_v1_160215.png')


def add_text(doc, t):
    """
    Split the provided text to escape latex commands and then add to the container
    """
    current_pos = 0
    for m in re.finditer(r'latex::(.+?)::', t):
        doc.append(t[current_pos: m.start()])
        doc.append(NoEscape(' ' + m.group(1) + ' '))
        current_pos = m.end()
    doc.append(t[current_pos:])
    return doc


class LatexSection(Environment):
    """This class is generic and allow the creation of any section like

    .. code-block:: latex

        \begin{name}
            Some content
        \end{name}

    The name is provided to the constructor. No additional package will be added to the list of packages.
    """
    def __init__(self, name, **kwargs):
        self._latex_name = name
        super().__init__(**kwargs)


class HRef(ContainerCommand):
    """A class that represents an hyperlink to a web address."""

    _repr_attributes_mapping = {
        'marker': 'options',
        'text': 'arguments',
    }

    packages = [Package('hyperref')]

    def __init__(self, url, text=None):
        """
        Args
        ----
        url: str
            The url to use.
        text:
            The text that will be shown as a link to the url. Use the url if not set
        """

        self.url = url
        if text is None:
            text = url
        super().__init__(arguments=NoEscape(url), data=text)


class ProjectReportLatex:

    def __init__(self, project_name, working_dir):
        self.project_information = ProjectReportInformation(project_name)
        self.working_dir = working_dir
        self.output_dir = self.project_information.project_delivery
        self.report_file_path = None
        self.doc = None

    @staticmethod
    def _limit_cell_width(rows, cell_widths):
        """
        Limit the size of the text in the cells of specific columns.
        When a cell has more characters than the limit, insert a new line.
        It can only insert one new line.
        :param rows: all rows of the table.
        :param cell_widths: a dict where the key is the column number (starting from 0) and
                            the value is the max number of character on one line
        :return: new rows modified if the character limit was reached.
        """
        new_rows = []
        for row in rows:
            new_row = []
            new_rows.append(new_row)
            for i, cell in enumerate(row):
                if i in cell_widths and len(str(cell)) > cell_widths.get(i):
                    new_row.append(
                        str(cell)[:cell_widths[i]] + '\n' + str(cell)[cell_widths.get(i):]
                    )
                else:
                    new_row.append(cell)
        return new_rows

    @staticmethod
    def create_vertical_table(container, header, rows, column_def=None, footer=None):
        def add_footer_rows(foot):
            if not isinstance(foot, list):
                foot = [foot]
            for f in foot:
                data_table.add_row((MultiColumn(ncol - 1, align='l', data=f), ''))

        ncol = len(header)
        if not column_def:
            column_def = ' '.join(['X[r]'] * ncol)
        # Using the tabu package -> http://mirrors.ibiblio.org/CTAN/macros/latex/contrib/tabu/tabu.pdf
        with container.create(LongTabu(column_def, width=ncol)) as data_table:
            data_table.add_hline()
            data_table.add_row(header, mapper=bold)
            data_table.add_hline()
            data_table.end_table_header()
            # Footer contains the next page notice
            data_table.add_hline()
            if footer:
                add_footer_rows(footer)
            data_table.add_row((MultiColumn(ncol, align='r', data='Continued on Next Page'),))
            data_table.add_hline()
            data_table.end_table_footer()
            # Last footer does not have the next page row
            if footer:
                add_footer_rows(footer)
            data_table.end_table_last_footer()
            for r in rows:
                data_table.add_row(r)
            data_table.add_hline()

    @staticmethod
    def create_horizontal_table(container, rows):
        """ Meant to be used for only two columns where the header is the first column"""
        # Convert cell containing lists into multilines cells
        converted_rows = []
        for row in rows:
            converted_row = []
            for cell in row:
                if isinstance(cell, list):
                    converted_row.append('\n'.join(cell))
                else:
                    converted_row.append(cell)
            converted_rows.append(converted_row)

        column_def = r'>{\bfseries}lX[l]'
        with container.create(Tabu(column_def, row_height=1.6)) as data_table:
            for r in converted_rows:
                data_table.add_row(r)

    @staticmethod
    def first_pages_style():
        # Generating first page style
        page = PageStyle("firstpage")

        # Address in small print in footer
        with page.create(Foot("C")) as footer:
            footer.append(FootnoteText(report_text.get('eg_post_address')))

        return page

    @staticmethod
    def all_pages_style(title):
        # Generating report page style
        page = PageStyle("allpages")

        # EG logo in header
        with page.create(Head("L")) as header_left:
            with header_left.create(MiniPage(pos='c', align='l')) as logo_wrapper:
                logo_wrapper.append(HRef(
                    url=report_text.get('eg_web_link'),
                    text=StandAloneGraphic(image_options="height=40px", filename=Uni_logo_file)
                ))

        # UoE logo in header
        with page.create(Head("R")) as right_header:
            with right_header.create(MiniPage(pos='c', align='r')) as logo_wrapper:
                logo_wrapper.append(HRef(
                    url=report_text.get('UoE_web_link'),
                    text=StandAloneGraphic(image_options="height=50px", filename=EG_logo_file)
                ))

        # Document revision in footer
        with page.create(Foot("L")) as footer:
            footer.append(FootnoteText(report_text.get('project_report_version')))

        # Page number in footer
        with page.create(Foot("C")) as footer:
            footer.append(NoEscape(r'Page \thepage\ of \pageref*{LastPage}'))

        # Document title in footer
        with page.create(Foot("R")) as footer:
            footer.append(FootnoteText(title))

        return page

    def create_authorisation_section(self, authorisations):
        header = ['Version', 'Release date', '# Samples', 'Released by', 'Signature id']
        # Columns 1, 2, 3 and 5 are fixed width and 4 is variable
        columns = '|p{1.5cm}|p{2.5cm}|p{2cm}|X|p{2.5cm}|'
        rows = [[
            str(authorisation.get('version')),
            str(authorisation.get('date')),
            str(len(authorisation.get('samples'))),
            '%s (%s)' % (authorisation.get('name'), authorisation.get('role')),
            str(authorisation.get('id'))
        ] for authorisation in authorisations]
        self.create_vertical_table(self.doc, header, rows, columns)

        msg = report_text.get('releases_signatures_desc').format(
            number_of_batches=len(authorisations),
            batches='batch' if len(authorisations) == 1 else 'batches'
        )
        self.doc.append(msg + ' ')
        self.doc.append(Hyperref(Marker('Appendix I. Per sample metadata', prefix='sec'), 'Appendix I'))
        self.doc.append(NoEscape('.\n'))

    def create_project_description_section(self, project_infos):
        with self.doc.create(Section('Project description', numbering=True)):
            self.create_horizontal_table(self.doc, project_infos)
            with self.doc.create(Paragraph('')):
                self.doc.append('For detailed per-sample information, please see ')
                self.doc.append(Hyperref(Marker('Appendix I. Per sample metadata', prefix='sec'), 'Appendix I'))
                self.doc.append(NoEscape('.\n'))

            self.doc.append(NewPage())

    def create_method_section(self, library_preparation_types, bioinfo_analysis_types, bioinformatic_parameters):
        with self.doc.create(Section('Methods', numbering=True)):
            add_text(self.doc, report_text.get('method_header'))
            for library_prep_type in library_preparation_types:
                if library_prep_type == 'Illumina TruSeq Nano library':
                    library_prep = report_text.get('library_preparation_nano')
                    library_qc = report_text.get('library_qc_nano')
                elif library_prep_type == 'Illumina TruSeq PCR-Free library':
                    library_prep = report_text.get('library_preparation_pcr_free')
                    library_qc = report_text.get('library_qc_pcr_free')
                elif library_prep_type == 'User Prepared Library':
                    library_prep = None
                    library_qc = report_text.get('library_qc_nano')
                else:
                    raise ValueError('Unsuported library preparation type: %s' % library_prep_type)

                with self.doc.create(Subsection(library_prep_type, numbering=True)):
                    # Skip the sample QC and Library preparation description if UPL
                    if library_prep:
                        with self.doc.create(Subsubsection(
                                'Sample QC', label=library_prep_type + '_Sample_QC', numbering=True
                        )):
                            add_text(self.doc, report_text.get('sample_qc'))
                        with self.doc.create(Subsubsection(
                                'Library Preparation', label=library_prep_type + '_Library_Preparation', numbering=True
                        )):
                            add_text(self.doc, library_prep)
                    else:  # User prepared library
                        add_text(self.doc, report_text.get('user_prepared_libraries'))
                    with self.doc.create(Subsubsection(
                            'Library QC', label=library_prep_type + '_Library_QC', numbering=True
                    )):
                        add_text(self.doc, library_qc)

            with self.doc.create(Subsection('Sequencing', numbering=True)):
                add_text(self.doc, report_text.get('sequencing'))

            for bioinfo_analysis_type in bioinfo_analysis_types:
                if bioinfo_analysis_type is 'bioinformatics_qc':
                    with self.doc.create(Subsection('Bioinformatics QC', numbering=True)):
                        add_text(self.doc, report_text.get('bioinformatics_qc').format(**bioinformatic_parameters))
                if bioinfo_analysis_type is 'bioinformatics_analysis_bcbio':
                    with self.doc.create(Subsection('Bioinformatics Analysis for Human samples', numbering=True)):
                        add_text(self.doc, report_text.get('bioinformatics_analysis_bcbio').format(**bioinformatic_parameters))
                if bioinfo_analysis_type is 'bioinformatics_analysis':
                    with self.doc.create(Subsection('Bioinformatics Analysis', numbering=True)):
                        add_text(self.doc, report_text.get('bioinformatics_analysis').format(**bioinformatic_parameters))
                self.doc.append(NewPage())

    def create_results_section(self, result_summary, charts_info):
        with self.doc.create(Section('Results', numbering=True)):
            self.create_horizontal_table(self.doc, result_summary)
            self.doc.append('For detailed per-sample information, please see ')
            self.doc.append(Hyperref(Marker('Appendix II. Per Sample Results', prefix='sec'), 'Appendix II'))
            self.doc.append(NoEscape('.\n'))

            with self.doc.create(Subsection('Yield and Coverage', numbering=True)):
                add_text(self.doc, report_text.get('yield_and_coverage'))
                self.doc.append(LineBreak())

                for chart_dict in charts_info:
                    with self.doc.create(MiniPage()) as chart_mp:
                        with chart_mp.create(LatexSection('center')) as chart_wrapper:
                            chart_wrapper.append(
                                StandAloneGraphic(image_options=r'width=.8\textwidth', filename=chart_dict['file'])
                            )
                            chart_wrapper.append(LineBreak())
                        chart_mp.append(italic(report_text.get('yield_and_coverage_chart').format(**chart_dict)))
                    self.doc.append(NoEscape('\n\n'))
        self.doc.append(NewPage())

    def create_file_format_section(self, formats_delivered):
        with self.doc.create(Section('Format of the Files Delivered', numbering=True)):
            for format_delivered in formats_delivered:
                if format_delivered == 'fastq':
                    with self.doc.create(Subsection('Fastq format', numbering=True)):
                        add_text(self.doc, report_text.get('fastq_format'))
                        self.doc.append(NoEscape('\n'))
                        self.doc.append('More detail about the format in ')
                        self.doc.append(HRef(url=NoEscape(report_text.get('fastq_link')), text='Fastq specification'))
                if format_delivered == 'bam':
                    with self.doc.create(Subsection('BAM format', numbering=True)):
                        add_text(self.doc, report_text.get('bam_format'))
                        self.doc.append(NoEscape('\n'))
                        self.doc.append('More detail about the format in ')
                        self.doc.append(HRef(url=NoEscape(report_text.get('bam_link')), text='BAM specification'))
                if format_delivered == 'vcf':
                    with self.doc.create(Subsection('VCF format', numbering=True)):
                        add_text(self.doc, report_text.get('vcf_format'))
                        self.doc.append(NoEscape('\n'))
                        self.doc.append('More detail about the format in ')
                        self.doc.append(HRef(url=report_text.get('vcf_link'), text='VCF specification'))

        self.doc.append(NewPage())

    def create_formal_statement_section(self, project_name, authorisations):
        with self.doc.create(Section('Deviations, Additions and Exclusions', numbering=True)):
            for authorisation in authorisations:
                if 'NCs' in authorisation and authorisation.get('NCs'):
                    title = '{project} {version}: {date}'.format(
                        project=project_name, version=authorisation.get('version'), date=authorisation.get('date')
                    )
                    with self.doc.create(Subsection(title, numbering=True)):
                        self.doc.append(authorisation.get('NCs'))
        with self.doc.create(Section('Declaration of Compliance', numbering=True)):
            add_text(self.doc, report_text.get('formal_statement'))
        with self.doc.create(Section('Confidentiality and privacy', numbering=True)):
            add_text(self.doc, report_text.get('privacy_notice') + '\n')
            self.doc.append(HRef(url=NoEscape(report_text.get('privacy_notice_link'))))

    def create_appendix_tables(self, appendix_tables):
        with self.doc.create(Section('Appendix I. Per sample metadata', numbering=True)):
            add_text(self.doc, report_text.get('appendix_description'))
            self.doc.append(LineBreak())

            with self.doc.create(LatexSection('scriptsize',)) as small_section:
                self.create_vertical_table(
                    small_section,
                    appendix_tables['appendix I']['header'],
                    self._limit_cell_width(appendix_tables['appendix I']['rows'], {0: 40}),
                    # Set the column width to fix width for all but first column
                    column_def='X[l] p{2.5cm} p{2cm} p{2cm} p{1.7cm} p{2cm}',
                    footer=appendix_tables['appendix I']['footer']
                )
        self.doc.append(NewPage())

        with self.doc.create(Section('Appendix II. Per Sample Results', numbering=True)):
            add_text(self.doc, report_text.get('appendix_description'))
            self.doc.append(LineBreak())

            with self.doc.create(LatexSection('scriptsize',)) as small_section:
                self.create_vertical_table(
                    small_section,
                    appendix_tables['appendix II']['header'],
                    self._limit_cell_width(appendix_tables['appendix II']['rows'], {0: 40}),
                    # Set the column width to fix width for all but first column
                    # R: is the newly defined column type in the preamble
                    'X[l] R{2cm} R{2.2cm} R{1.9cm} R{2cm} R{2cm}'
                )

    def front_page(self, project_name, report_version, authorisations):
        with self.doc.create(MiniPage(height='5cm', pos='c', align='c')) as logo_wrapper:
            logo_wrapper.append(HRef(
                url='https://genomics.ed.ac.uk',
                text=StandAloneGraphic(image_options="height=80px", filename=UoE_EG_logo_file)
            ))
        self.doc.append(LineBreak())
        with self.doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
            title_wrapper.append(HugeText('Whole genome sequencing report'))
        self.doc.append(LineBreak())
        with self.doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
            title_wrapper.append(LargeText('Project: ' + project_name))
        self.doc.append(LineBreak())
        with self.doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
            title_wrapper.append(MediumText('Report version: ' + report_version))
        self.doc.append(LineBreak())

        self.create_authorisation_section(authorisations)
        self.doc.append(NewPage())

    def populate_document(self):
        # Get information from the project_information object
        project_name = self.project_information.project_name
        authorisations = self.project_information.get_authorization()
        project_info = self.project_information.get_project_info()

        library_prep_type, bioinfo_analysis_types, format_delivered = self.project_information.get_library_prep_analysis_types_and_format()
        bioinformatic_parameters = self.project_information.params
        result_summary = self.project_information.calculate_project_statistics()
        appendix_tables = self.project_information.get_sample_data_in_tables(authorisations)
        charts_info = yield_vs_coverage_plot(self.project_information, self.working_dir)
        last_auth = authorisations[-1]

        document_title = 'Project {name} Report {version}'.format(
            name=self.project_information.project_name, version=last_auth.get('version')
        )

        # Add the required packages
        self.doc.packages.append(Package('roboto', 'sfdefault'))  # Add roboto as the default Font
        self.doc.packages.append(Package('array'))  # Array package https://ctan.org/pkg/array?lang=en
        self.doc.packages.append(Package('hyperref', ['colorlinks=true', 'linkcolor=blue', 'urlcolor=blue']))
        # SI units package https://ctan.org/pkg/siunitx?lang=en
        self.doc.packages.append(Package('siunitx', NoEscape('per-mode=symbol')))

        self.doc.preamble.append(self.first_pages_style())  # Create the footer and header for first page
        # Create the footer and header for rest of the pages
        self.doc.preamble.append(self.all_pages_style(document_title))
        # New column type that aligned on the right and allow to specify fixed column width
        # See https://bit.ly/2RMlZgS
        self.doc.preamble.append(NoEscape(
            r'\newcolumntype{R}[1]{>{\raggedleft\let\newline\\\arraybackslash\hspace{0pt}}p{#1}}'
        ))
        self.doc.change_document_style('firstpage')
        # First page of the document
        self.front_page(project_name, last_auth.get('version'), authorisations)

        # Main document
        self.doc.change_document_style('allpages')

        # Table of content
        self.doc.append(NoEscape('\n'.join([
            '{',
            '\hypersetup{linkcolor=black}',  # Ensure the table of content's links are black
            r'\setcounter{tocdepth}{2}',
            r'\tableofcontents',
            '}'
        ])))

        self.doc.append(NewPage())

        # Subsequent sections
        self.create_project_description_section(project_info)
        self.create_method_section(library_prep_type, bioinfo_analysis_types, bioinformatic_parameters)
        self.create_results_section(result_summary, charts_info)
        self.create_file_format_section(format_delivered)
        self.create_formal_statement_section(project_name, authorisations)

        self.doc.append(NoEscape(r'\noindent\makebox[\linewidth]{\rule{\linewidth}{0.4pt}}'))
        with self.doc.create(LatexSection('center')) as center_sec:
            center_sec.append('End of ' + document_title)
        self.doc.append(NewPage())

        # Appendices
        self.create_appendix_tables(appendix_tables)

    def generate_document(self):
        # Get information from the project_information object
        authorisations = self.project_information.get_authorization()
        last_auth = authorisations[-1]

        document_title = 'Project {name} Report {version}'.format(
            name=self.project_information.project_name, version=last_auth.get('version')
        )

        # Prepare the document geometry
        geometry_options = [
            'headheight=66pt',  # TODO: transfer the header space to pagestyle if possible
            'margin=1.5cm',
            'bottom=1cm',
            'top=1cm',
            'includeheadfoot',
            'a4paper'
        ]

        self.report_file_path = os.path.join(self.output_dir, document_title.replace(' ', '_'))

        # Create the standard document with 12pt font size https://en.wikibooks.org/wiki/LaTeX/Fonts#Sizing_text
        return Document(
            self.report_file_path,
            document_options=['12pt'],
            geometry_options=geometry_options,
            lmodern=False,  # Do not use latin modern font
            indent=False  # Remove the default paragraph indentation throughout the document
        )

    def generate_tex(self):
        self.doc = self.generate_document()
        self.populate_document()
        self.doc.generate_tex()
        return self.report_file_path + '.tex'

    def generate_pdf(self):
        self.doc = self.generate_document()
        self.populate_document()
        self.doc.generate_pdf(clean_tex=True, silent=True)
        return self.report_file_path + '.pdf'
