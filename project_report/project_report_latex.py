import os

import yaml
from egcg_core.util import query_dict
from pylatex import Document, Section, Subsection, Package, PageStyle, Head, MiniPage, StandAloneGraphic, Foot, \
    NewPage, HugeText, Tabu, Subsubsection, FootnoteText, LineBreak, NoEscape, LongTabu, Hyperref, Marker, \
    MultiColumn, MediumText, LargeText
from pylatex.section import Paragraph
from pylatex.utils import italic, bold
from project_report.project_information import ProjectReportInformation
from project_report.utils import yield_vs_coverage_plot, parse_date, min_mean_max, estimate_columns_definition

# Load all source texts from yaml.
from project_report.pylatex_ext import HRef, LatexSection, add_text

_report_text_yaml_file = os.path.join(os.path.dirname(__file__), 'report_texts.yaml')
with open(_report_text_yaml_file) as open_file:
    report_text = yaml.load(open_file)

EG_logo_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'EG_logo_blackonwhite_300dpi.png')
UoE_EG_logo_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'UoE_EG_logo.png')
Uni_logo_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etc', 'UoE_Stacked_Logo_CMYK_v1_160215.png')


class ProjectReportLatex:

    def __init__(self, project_name, working_dir):
        self.pi = ProjectReportInformation(project_name)
        self.working_dir = working_dir
        self.output_dir = self.pi.project_delivery
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
        """
        Create a table with the specified header at the top in the provided container.
        The table is created using the tabu package. http://mirrors.ibiblio.org/CTAN/macros/latex/contrib/tabu/tabu.pdf
        The header will be formatted as bold.
        :param container: The container where the table will be added.
        :param header: A list containing the column header.
        :param rows: A list of list containing each cell's content.
        :param column_def: a list of string describing each column.
        :param footer: An optional list of footer lines.
        """
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

    def get_project_info(self):
        project_info = (
            ('Project name', self.pi.project_name),
            ('Project title', self.pi.project_title),
            ('Enquiry no', self.pi.enquiry_number),
            ('Quote no', self.pi.quote_number),
            ('Customer name', self.pi.customer_name),
            ('Customer address', '\n'.join(self.pi.customer_address_lines)),
            ('Number of samples', self.pi.number_quoted_samples),
            ('Number of samples delivered', len(self.pi.sample_names_delivered)),
            ('Date samples received', 'Detailed in appendix I'),
            ('Total download size', '%.2f terabytes' % self.pi.project_size_in_terabytes()),
            ('Laboratory protocol', ', '.join(self.pi.get_project_library_workflows())),
            ('Submitted species', ', '.join(self.pi.get_project_species())),
            ('Genome version', ', '.join(self.pi.get_project_genome_version()))
        )

        return project_info

    def format_result_summary(self):
        sample_data_mapping = {
            'Yield per sample (Gb)': 'data.aggregated.clean_yield_in_gb',
            'Coverage per sample': 'data.coverage.mean',
            '% Duplicate reads': 'data.aggregated.pc_duplicate_reads',
            '% Reads mapped': 'data.aggregated.pc_mapped_reads',
            '% Q30': 'data.aggregated.clean_pc_q30',
        }
        headers = ['Yield per sample (Gb)', '% Q30', 'Coverage per sample', '% Reads mapped', '% Duplicate reads']
        project_stats = []
        for field in headers:
            project_stats.append((field, min_mean_max(
                [query_dict(self.pi.sample_info(sample_id), sample_data_mapping[field])
                 for sample_id in self.pi.sample_names_delivered]
            )))
        return project_stats

    @staticmethod
    def format_table_footer_line(definitions, superscript):
        """
        Take a footer rows as list of tuples. The tuple have two elements.
        The first element will be formatted as bold.

        :param superscript: superscripted text that appear at the beginning of the line
        :param definitions: lists of tuples containing the definitions to be added on that line
        :return: list of latex formatted rows
        """
        formatted_latex = []
        if superscript:
            formatted_latex.append(NoEscape(r'\textsuperscript{%s}' % superscript))
        for def_element in definitions:
            formatted_latex.append(bold(def_element[0]))
            formatted_latex.append(': ' + def_element[1])
            formatted_latex.append(', ')  # Separate the different entries with comma
        return formatted_latex[:-1]  # Remove the last comma

    def get_sample_data_in_tables(self):
        authorisations = self.pi.authorisations
        tables = {}
        header = [
            'User ID', 'Internal ID', 'Received', 'Reviewed',
            NoEscape(r'Species\textsuperscript{1}'),  # reference to the footer
            NoEscape(r'Library prep.\textsuperscript{2}'),
            NoEscape(r'Analysis\textsuperscript{3}')
        ]

        def find_sample_release_date_in_auth(sample):
            return [auth.get('date') for auth in authorisations if sample in auth.get('samples')]

        rows = []

        library_descriptions = set()
        species_descriptions = set()
        analysis_descriptions = set()
        for sample in self.pi.sample_names_delivered:
            date_reviewed = find_sample_release_date_in_auth(sample)
            internal_sample_name = self.pi.get_fluidx_barcode(sample) or sample
            library = self.pi.get_library_workflow_from_sample(sample)
            library_descriptions.add((self.pi.library_abbreviation.get(library), library))
            species = self.pi.get_species_from_sample(sample)
            species_descriptions.add((self.pi.abbreviate_species(species), species))
            analysis = self.pi.get_analysis_type_from_sample(sample)
            analysis_descriptions.add((self.pi.analysis_abbreviation.get(analysis),
                                       self.pi.analysis_description.get(analysis)))
            row = [
                self.pi.get_user_sample_id(sample),
                internal_sample_name,
                parse_date(self.pi.get_started_date_from_sample(sample)),
                ', '.join(date_reviewed),
                self.pi.abbreviate_species(species),
                self.pi.library_abbreviation.get(library),
                self.pi.analysis_abbreviation.get(analysis)
            ]

            rows.append(row)
        # Latex specific column definition
        # Set the column width to fix width for all but first column
        column_types = ['X[l]', 'p', 'p', 'p', 'p', 'p', 'p']
        column_def = estimate_columns_definition(
            rows, column_types,
            minimums=[2.0, 2.0, 1.5, 1.5, 1.0, 1.5, 1.5],
            extend_to=16)

        tables['appendix I'] = {
            'header': header, 'column_def': column_def, 'rows': rows,
            'footer': [
                self.format_table_footer_line(sorted(library_descriptions), 1),
                self.format_table_footer_line(sorted(species_descriptions), 2),
                self.format_table_footer_line(sorted(analysis_descriptions), 3)
            ]
        }
        header = [
            'User ID', 'Yield quoted (Gb)', 'Yield provided (Gb)', '% Q30 > 75%', 'Quoted coverage', 'Provided coverage'
        ]

        rows = []
        for sample in self.pi.sample_names_delivered:
            row = [
                self.pi.get_user_sample_id(sample),
                self.pi.get_required_yield(sample),
                self.pi.get_yield_in_gb(sample),
                self.pi.get_pc_q30(sample),
                self.pi.get_quoted_coverage(sample),
                self.pi.get_average_coverage(sample)
            ]

            rows.append(row)
        # Set the column width to fix width for all but first column
        # R: is the newly defined column type in the document preamble
        column_types = ['X[l]', 'R', 'R', 'R', 'R', 'R']
        column_def = estimate_columns_definition(rows, column_types, minimums=[2.0, 2.0, 2.0, 1.7, 2, 2], extend_to=16)
        tables['appendix II'] = {'header': header, 'rows': rows, 'column_def': column_def}

        return tables

    # Page style function
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
                    url=report_text.get('UoE_web_link'),
                    text=StandAloneGraphic(image_options="height=40px", filename=Uni_logo_file)
                ))

        # UoE logo in header
        with page.create(Head("R")) as right_header:
            with right_header.create(MiniPage(pos='c', align='r')) as logo_wrapper:
                logo_wrapper.append(HRef(
                    url=report_text.get('eg_web_link'),
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

    def create_authorisation_section(self):
        authorisations = self.pi.authorisations
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

        self.doc.append(report_text.get('releases_signatures_desc') + ' ')
        self.doc.append(Hyperref(Marker('Appendix I. Per sample metadata', prefix='sec'), 'Appendix I'))
        self.doc.append(NoEscape('.\n'))

    def create_project_description_section(self):
        with self.doc.create(Section('Project description', numbering=True)):
            self.create_horizontal_table(self.doc, self.get_project_info())
            with self.doc.create(Paragraph('')):
                self.doc.append('For detailed per-sample information, please see ')
                self.doc.append(Hyperref(Marker('Appendix I. Per sample metadata', prefix='sec'), 'Appendix I'))
                self.doc.append(NoEscape('.\n'))

            self.doc.append(NewPage())

    def create_method_section(self):
        with self.doc.create(Section('Methods', numbering=True)):
            add_text(self.doc, report_text.get('method_header'))
            for library_prep_type in self.pi.get_project_library_workflows():
                if library_prep_type == 'Illumina TruSeq Nano library':
                    library_prep = report_text.get('library_preparation_nano')
                    library_qc = report_text.get('library_qc_nano')
                elif library_prep_type == 'Illumina TruSeq PCR-Free library':
                    library_prep = report_text.get('library_preparation_pcr_free')
                    library_qc = report_text.get('library_qc_pcr_free')
                elif library_prep_type == 'Roche KAPA PCR-Free library':
                    library_prep = report_text['library_preparation_kapa_pcr_free']
                    library_qc = report_text['library_qc_kapa_pcr_free']
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

            bioinf_reports = {
                'qc': ('Bioinformatics QC', 'bioinformatics_qc'),
                'bcbio': ('Bioinformatics Analysis for Human samples', 'bioinformatics_analysis_bcbio'),
                'variant_calling': ('Bioinformatics Analysis', 'bioinformatics_analysis'),
                'variant_calling_gatk4': ('Bioinformatics Analysis with GATK4', 'bioinformatics_analysis_gatk4'),
                'human_variant_calling_gatk4': ('Bioinformatics Analysis with GATK4 for Human samples', 'bioinformatics_analysis_gatk4'),
                'qc_gatk4': ('Bioinformatics QC with GATK4', 'bioinformatics_qc')
            }

            for bioinfo_analysis_type in self.pi.get_project_analysis_types():
                bioinfo_version = self.pi.get_bioinformatics_params_for_analysis(bioinfo_analysis_type)
                subsection, paragraph = bioinf_reports[bioinfo_analysis_type]
                with self.doc.create(Subsection(subsection, numbering=True)):
                    add_text(self.doc, report_text[paragraph].format(**bioinfo_version))

            if self.pi.has_rapid_samples():
                with self.doc.create(Subsection('Rapid Bioinformatics Analysis', numbering=True)):
                    add_text(self.doc, report_text['rapid_analysis'])

            self.doc.append(NewPage())

    def create_results_section(self):
        result_summary = self.format_result_summary()
        charts_info = yield_vs_coverage_plot(self.pi, self.working_dir)
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

    def create_file_format_section(self):
        with self.doc.create(Section('Format of the Files Delivered', numbering=True)):
            formats_delivered = self.pi.get_format_delivered()
            for file_format in ['fastq', 'bam', 'vcf']:
                if file_format in formats_delivered:
                    with self.doc.create(Subsection('%s format' % file_format.upper(), numbering=True)):
                        add_text(self.doc, report_text.get('%s_format' % file_format))
                        self.doc.append(NoEscape('\n'))
                        self.doc.append('More detail about the format in ')
                        self.doc.append(HRef(
                            url=NoEscape(report_text.get('%s_link' % file_format)),
                            text='%s specification' % file_format.upper()
                        ))

        self.doc.append(NewPage())

    def create_formal_statement_section(self):
        with self.doc.create(Section('Deviations, Additions and Exclusions', numbering=True)):
            for authorisation in self.pi.authorisations:
                if 'NCs' in authorisation and authorisation.get('NCs'):
                    title = '{project} {version}: {date}'.format(
                        project=self.pi.project_name,
                        version=authorisation.get('version'),
                        date=authorisation.get('date')
                    )
                    with self.doc.create(Subsection(title, numbering=True)):
                        self.doc.append(authorisation.get('NCs'))
        with self.doc.create(Section('Declaration of Compliance', numbering=True)):
            add_text(self.doc, report_text.get('formal_statement'))
        with self.doc.create(Section('Confidentiality and privacy', numbering=True)):
            add_text(self.doc, report_text.get('privacy_notice') + '\n')
            self.doc.append(HRef(url=NoEscape(report_text.get('privacy_notice_link'))))

    def create_appendix_tables(self):
        appendix_tables = self.get_sample_data_in_tables()
        with self.doc.create(Section('Appendix I. Per sample metadata', numbering=True)):
            add_text(self.doc, report_text.get('appendix_description'))
            self.doc.append(LineBreak())

            with self.doc.create(LatexSection('scriptsize',)) as small_section:
                self.create_vertical_table(
                    small_section,
                    appendix_tables['appendix I']['header'],
                    self._limit_cell_width(appendix_tables['appendix I']['rows'], {0: 35}),
                    column_def=appendix_tables['appendix I']['column_def'],
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
                    column_def=appendix_tables['appendix II']['column_def']
                )

    def front_page(self):
        with self.doc.create(MiniPage(height='5cm', pos='c', align='c')) as logo_wrapper:
            logo_wrapper.append(HRef(
                url=report_text.get('eg_web_link'),
                text=StandAloneGraphic(image_options="height=80px", filename=UoE_EG_logo_file)
            ))
        self.doc.append(LineBreak())
        with self.doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
            title_wrapper.append(HugeText('Whole genome sequencing report'))
        self.doc.append(LineBreak())
        with self.doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
            title_wrapper.append(LargeText('Project: ' + self.pi.project_name))
        self.doc.append(LineBreak())
        with self.doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
            title_wrapper.append(MediumText('Report version: ' + self.pi.report_version))
        self.doc.append(LineBreak())

        self.create_authorisation_section()
        self.doc.append(NewPage())

    def populate_document(self):
        document_title = 'Project {name} Report {version}'.format(
            name=self.pi.project_name, version=self.pi.report_version
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
        self.front_page()

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
        self.create_project_description_section()
        self.create_method_section()
        self.create_results_section()
        self.create_file_format_section()
        self.create_formal_statement_section()

        self.doc.append(NoEscape(r'\noindent\makebox[\linewidth]{\rule{\linewidth}{0.4pt}}'))
        with self.doc.create(LatexSection('center')) as center_sec:
            center_sec.append('End of ' + document_title)
        self.doc.append(NewPage())

        # Appendices
        self.create_appendix_tables()

    def generate_document(self):
        document_title = 'Project {name} Report {version}'.format(
            name=self.pi.project_name, version=self.pi.report_version
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
