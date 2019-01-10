import os
import re

import yaml
from pylatex import Document, Section, Subsection, Package, PageStyle, Head, MiniPage, StandAloneGraphic, Foot, \
    NewPage, HugeText, Tabu, Subsubsection, FootnoteText, LineBreak, Command, NoEscape, LongTabu, Hyperref, Marker, \
    MultiColumn, MediumText, LargeText
from pylatex.base_classes import Environment, CommandBase
from pylatex.section import Paragraph
from pylatex.utils import italic, bold

from project_report.project_information import yield_vs_coverage_plot


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
    for m in re.finditer('latex::(.+)::latex', t):
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


class HRef(CommandBase):
    """A class that represents an hyperlink to a web address."""

    _repr_attributes_mapping = {
        'marker': 'options',
        'text': 'arguments',
    }

    packages = [Package('hyperref')]

    def __init__(self, url, text):
        """
        Args
        ----
        url: str
            The url to use.
        text: str
            The text that will be shown as a link
            to the url.
        """

        self.url = url
        super().__init__(arguments=NoEscape(url), extra_arguments=text)


def create_vertical_table(container, header, rows, column_def=None, footer=False):
    ncol = len(header)
    if not column_def:
        column_def = ' '.join(['X[r]'] * ncol)
    with container.create(LongTabu(column_def, width=ncol)) as data_table:
        data_table.add_hline()
        data_table.add_row(header, mapper=bold)
        data_table.add_hline()
        data_table.end_table_header()
        # Footer contains the next page notice
        data_table.add_hline()
        data_table.add_row((MultiColumn(ncol, align='r',data='Continued on Next Page'),))
        data_table.add_hline()
        data_table.end_table_footer()
        # Last footer does not contain anything
        data_table.end_table_last_footer()

        for r in rows:
            data_table.add_row(r)
        data_table.add_hline()


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


def create_authorisation_section(doc, authorisations):
    header = ['Version', 'Release date', '# Samples', 'Released by', 'Signature id']
    columns = '|p{1.5cm}|p{2.5cm}|p{2cm}|X|p{2.5cm}|'
    rows = [[
        str(authorisation.get('version')),
        str(authorisation.get('date')),
        str(len(authorisation.get('samples'))),
        '%s (%s)' % (authorisation.get('name'), authorisation.get('role')),
        str(authorisation.get('id'))
    ] for authorisation in authorisations]
    create_vertical_table(doc, header, rows, columns)

    msg = report_text.get('releases_signatures_desc').format(
        number_of_batches=len(authorisations),
        batches='batch' if len(authorisations) == 1 else 'batches'
    )
    doc.append(msg + ' ')
    doc.append(Hyperref(Marker('Appendix I. Per sample metadata', prefix='sec'), 'Appendix I'))
    doc.append(NoEscape('.\n'))


def create_project_description_section(doc, project_infos):
    with doc.create(Section('Project description', numbering=True)):
        create_horizontal_table(doc, project_infos)
        with doc.create(Paragraph('')):
            doc.append('For detailed per-sample information, please see ')
            doc.append(Hyperref(Marker('Appendix I. Per sample metadata', prefix='sec'), 'Appendix I'))
            doc.append(NoEscape('.\n'))

    doc.append(NewPage())


def create_method_section(doc, library_preparation_types, bioinfo_analysis_types, bioinformatic_parameters):
    with doc.create(Section('Methods', numbering=True)):
        add_text(doc, report_text.get('method_header'))
        for library_prep_type in library_preparation_types:
            if library_prep_type == 'TruSeq Nano':
                library_prep = report_text.get('library_preparation_nano')
                library_qc = report_text.get('library_qc_nano')
            elif library_prep_type == 'TruSeq PCR-Free':
                library_prep = report_text.get('library_preparation_pcr_free')
                library_qc = report_text.get('library_qc_pcr_free')
            elif library_prep_type == 'User Prepared Library':
                library_prep = None
                library_qc = report_text.get('library_qc_nano')
            else:
                raise ValueError('Unsuported library preparation type: %s' % library_prep_type)

            with doc.create(Subsection(library_prep_type, numbering=True)):
                # Skip the sample QC and Library preparation description if UPL
                if library_prep:
                    with doc.create(Subsubsection('Sample QC', label=library_prep_type + '_Sample_QC', numbering=True)):
                        add_text(doc, report_text.get('sample_qc'))
                    with doc.create(Subsubsection('Library Preparation', label=library_prep_type + '_Library_Preparation',
                                                  numbering=True)):
                        add_text(doc, library_prep)
                with doc.create(Subsubsection('Library QC', label=library_prep_type + '_Library_QC', numbering=True)):
                    add_text(doc, library_qc)

        with doc.create(Subsection('Sequencing', numbering=True)):
            add_text(doc, report_text.get('sequencing'))

        for bioinfo_analysis_type in bioinfo_analysis_types:
            if bioinfo_analysis_type is 'bioinformatics_qc':
                with doc.create(Subsection('Bioinformatics QC', numbering=True)):
                    add_text(doc, report_text.get('bioinformatics_qc').format(**bioinformatic_parameters))
            if bioinfo_analysis_type is 'bioinformatics_analysis_bcbio':
                with doc.create(Subsection('Bioinformatics Analysis for Human samples', numbering=True)):
                    add_text(doc, report_text.get('bioinformatics_analysis_bcbio').format(**bioinformatic_parameters))
            if bioinfo_analysis_type is 'bioinformatics_analysis':
                with doc.create(Subsection('Bioinformatics Analysis', numbering=True)):
                    add_text(doc, report_text.get('bioinformatics_analysis').format(**bioinformatic_parameters))
    doc.append(NewPage())


def create_results_section(doc, result_summary, charts_info):
    with doc.create(Section('Results', numbering=True)):
        create_horizontal_table(doc, result_summary)
        doc.append('For detailed per-sample information, please see ')
        doc.append(Hyperref(Marker('Appendix II. Per Sample Results', prefix='sec'), 'Appendix II'))
        doc.append(NoEscape('.\n'))

        with doc.create(Subsection('Yield and Coverage', numbering=True)):
            add_text(doc, report_text.get('yield_and_coverage'))
            doc.append(LineBreak())

            for chart_dict in charts_info:
                with doc.create(MiniPage()) as chart_mp:
                    with chart_mp.create(LatexSection('center')) as chart_wrapper:
                        chart_wrapper.append(
                            StandAloneGraphic(image_options=r'width=.8\textwidth', filename=chart_dict['file'])
                        )
                        chart_wrapper.append(LineBreak())
                    chart_mp.append(italic(report_text.get('yield_and_coverage_chart').format(**chart_dict)))
                doc.append(NoEscape('\n\n'))
    doc.append(NewPage())


def create_file_format_section(doc, formats_delivered):
    with doc.create(Section('Format of the Files Delivered', numbering=True)):
        for format_delivered in formats_delivered:
            if format_delivered == 'fastq':
                with doc.create(Subsection('Fastq format', numbering=True)):
                    add_text(doc, report_text.get('fastq_format'))
                    doc.append(NoEscape('\n'))
                    doc.append('More detail about the format in ')
                    doc.append(HRef(url=NoEscape(report_text.get('fastq_link')), text='Fastq specification'))
            if format_delivered == 'bam':
                with doc.create(Subsection('BAM format', numbering=True)):
                    add_text(doc, report_text.get('bam_format'))
                    doc.append(NoEscape('\n'))
                    doc.append('More detail about the format in ')
                    doc.append(HRef(url=NoEscape(report_text.get('bam_link')), text='BAM specification'))
            if format_delivered == 'vcf':
                with doc.create(Subsection('VCF format', numbering=True)):
                    add_text(doc, report_text.get('vcf_format'))
                    doc.append(NoEscape('\n'))
                    doc.append('More detail about the format in ')
                    doc.append(HRef(url=report_text.get('vcf_link'), text='VCF specification'))

    doc.append(NewPage())


def create_formal_statement_section(doc, project_name, authorisations):
    with doc.create(Section('Deviations, Additions and Exclusions', numbering=True)):
        for authorisation in authorisations:
            if 'NCs' in authorisation:
                title = '{project} {version}: {date}'.format(
                    project=project_name, version=authorisation.get('version'), date=authorisation.get('date')
                )
                doc.create(Subsubsection(title, numbering=True))
                doc.append(authorisation.get('NCs'))
    with doc.create(Section('Declaration of Compliance', numbering=True)):
        add_text(doc, report_text.get('formal_statement'))


def create_appendix_table(doc, appendix_tables):
    with doc.create(Section('Appendix I. Per sample metadata', numbering=True)):
        add_text(doc, report_text.get('appendix_description'))
        doc.append(LineBreak())

        with doc.create(LatexSection('scriptsize',)) as small_section:
            create_vertical_table(
                small_section,
                appendix_tables['appendix I']['header'],
                appendix_tables['appendix I']['rows']
            )
    with doc.create(Section('Appendix II. Per Sample Results', numbering=True)):
        add_text(doc, report_text.get('appendix_description'))
        doc.append(LineBreak())

        with doc.create(LatexSection('scriptsize',)) as small_section:
            create_vertical_table(
                small_section,
                appendix_tables['appendix II']['header'],
                appendix_tables['appendix II']['rows']
            )


def first_pages_style():
    # Generating first page style
    page = PageStyle("firstpage")

    # Address in small print in footer
    address = 'Edinburgh Genomics Clinical, The Roslin Institute Easter Bush, Midlothian EH25 9RG Scotland, UK'
    with page.create(Foot("C")) as footer:
        footer.append(FootnoteText(address))

    return page


def all_pages_style(title):
    # Generating report page style
    page = PageStyle("allpages")

    # EG logo in header
    with page.create(Head("L")) as header_left:
        with header_left.create(MiniPage(pos='c', align='l')) as logo_wrapper:

            logo_wrapper.append(HRef(
                url='https://genomics.ed.ac.uk',
                text=StandAloneGraphic(image_options="height=40px", filename=Uni_logo_file)
            ))

    # UoE logo in header
    with page.create(Head("R")) as right_header:
        with right_header.create(MiniPage(pos='c', align='r')) as logo_wrapper:

            logo_wrapper.append(HRef(
                url='https://www.ed.ac.uk/',
                text=StandAloneGraphic(image_options="height=50px", filename=EG_logo_file)
            ))

    # Document revision in footer
    with page.create(Foot("L")) as footer:
        footer.append(FootnoteText("EGC-BTP-1 rev. 2"))

    # Page number in footer
    with page.create(Foot("C")) as footer:
        footer.append(NoEscape(r'Page \thepage\ of \pageref*{LastPage}'))

    # Document title in footer
    with page.create(Foot("R")) as footer:
        footer.append(FootnoteText(title))

    return page


def front_page(doc, project_name, report_version, authorisations):
    with doc.create(MiniPage(height='5cm', pos='c', align='c')) as logo_wrapper:
        logo_wrapper.append(HRef(
            url='https://genomics.ed.ac.uk',
            text=StandAloneGraphic(image_options="height=80px", filename=UoE_EG_logo_file)
        ))
    doc.append(LineBreak())
    with doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
        title_wrapper.append(HugeText('Whole genome sequencing report'))
    doc.append(LineBreak())
    with doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
        title_wrapper.append(LargeText('Project: ' + project_name))
    doc.append(LineBreak())
    with doc.create(MiniPage(height='3cm', pos='c', align='c')) as title_wrapper:
        title_wrapper.append(MediumText('Report version: ' + report_version))
    doc.append(LineBreak())

    create_authorisation_section(doc, authorisations)
    doc.append(NewPage())


def generate_document(project_information, working_dir, output_dir):
    # Get information from the project_information object
    project_name = project_information.project_name
    authorisations = project_information.get_authorization()
    project_info = project_information.get_project_info()

    library_prep_type, bioinfo_analysis_types, format_delivered = project_information.get_library_prep_analysis_types_and_format()
    bioinformatic_parameters = project_information.params
    result_summary = project_information.calculate_project_statistsics()
    appendix_tables = project_information.get_sample_data_in_tables(authorisations)
    charts_info = yield_vs_coverage_plot(project_information, working_dir)
    last_auth = authorisations[-1]

    document_title = 'Project {name} Report {version}'.format(
        name=project_information.project_name, version=last_auth.get('version')
    )

    # Prepare the document geometry
    geometry_options = {
        'headheight': '66pt',  # TODO: transfer the header space to pagestyle
        "margin": "1.5cm",
        "bottom": "1cm",
        "top": "1cm",
        "includeheadfoot": True,
        "a4paper": True
    }

    report_file = os.path.join(output_dir, document_title.replace(' ', '_'))
    # Create the standard document with 12pt font size https://en.wikibooks.org/wiki/LaTeX/Fonts#Sizing_text
    doc = Document(
        report_file,
        document_options=['12pt'],
        geometry_options=geometry_options,
        lmodern=False,  # Do not use latin modern font
        indent=False  # Remove the default paragraph indentation throughout the document
    )
    # Add the required packages
    doc.packages.append(Package('roboto', 'sfdefault'))  # Add roboto as the default Font
    doc.packages.append(Package('array'))  # Array package https://ctan.org/pkg/array?lang=en
    doc.packages.append(Package('hyperref', ['colorlinks=true', 'linkcolor=blue', 'urlcolor=blue']))

    doc.preamble.append(first_pages_style())  # Create the footer and header for first page
    doc.preamble.append(all_pages_style(document_title))  # Create the footer and header for all pages

    doc.change_document_style('firstpage')
    # First page of the document
    front_page(doc, project_name, last_auth.get('version'), authorisations)

    doc.change_document_style('allpages')
    #
    doc.append(NoEscape('{\n\hypersetup{linkcolor=black}\n' + r'\tableofcontents' + '\n}'))

    doc.append(NewPage())

    # Subsequent sections
    create_project_description_section(doc, project_info)
    create_method_section(doc, library_prep_type, bioinfo_analysis_types, bioinformatic_parameters)
    create_results_section(doc, result_summary, charts_info)
    create_file_format_section(doc, format_delivered)
    create_formal_statement_section(doc, project_name, authorisations)

    doc.append(NoEscape(r'\noindent\makebox[\linewidth]{\rule{\linewidth}{0.4pt}}'))
    with doc.create(LatexSection('center')) as center_sec:
        center_sec.append('End of ' + document_title)
    doc.append(NewPage())

    # Appendix
    create_appendix_table(doc, appendix_tables)

    doc.generate_pdf(clean_tex=False, silent=True)

