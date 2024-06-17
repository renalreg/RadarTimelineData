import inspect
import os
import random
from typing import Any

import docx
import polars as pl
import xlsxwriter
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.enum.text import (
    WD_COLOR_INDEX,
    WD_ALIGN_PARAGRAPH,
)
from docx.oxml import parse_xml, OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt, RGBColor, Inches, Cm, Mm
from docx.text.paragraph import Paragraph
from docx.text.run import Run
from loguru import logger
from openpyxl.utils import get_column_letter

from radar_timeline_data.audit_writer.stylesheet import (
    stylesheet,
    table_styles,
    page_color,
)


class AuditWriter:
    """
    A class to manage the creation and writing of audit documents and spreadsheets.

    This class is responsible for initializing an audit document and spreadsheet,
    adding headings and paragraphs to the document, and managing the creation of
    worksheets within the spreadsheet.

    Attributes:
    - directory (str): The directory where the output files will be saved.
    - filename (str): The name of the audit file.
    - document (Document): An instance of the Document class for creating the audit document.
    - important_High (int): A counter for high importance audit entries.
    - important_Low (int): A counter for low importance audit entries.
    - info (dict): A dictionary to store additional information related to the audit.
    - wb (Workbook): An instance of the Workbook class for creating the audit spreadsheet.
    - current_worksheet (Worksheet): The current worksheet being written to in the spreadsheet.
    - worksheets (dict): A dictionary of worksheets within the spreadsheet.
    """

    def __init__(
        self,
        directory: str,
        filename: str,
        document_title: str,
        include_excel: bool = True,
        include_breakdown: bool = True,
        include_logger: bool = True,
    ):
        """
        Initializes an AuditWriter object.

        Parameters:
        - directory (str): The directory where the output files will be saved.
        - filename (str): The name of the audit file.
        - include_excel (bool): Whether to allow excel files to be generated
        - include_breakdown (bool): Whether to include top info section in doc contain info such as warnings
        """

        # file init
        self.directory = directory
        self.filename = filename
        self.document = Document()
        # Access the first section of the document
        section = self.document.sections[0]

        # Set page size to A4
        section.page_height = Mm(297)
        section.page_width = Mm(210)

        self.add_watermark()

        # set style
        self.stylesheet = stylesheet
        self.__style()

        # add heading
        para = self.document.add_heading(f"Audit {document_title}", 0)
        self.add_paragraph_border(para, ["bottom"])
        # add process sub heading
        para = self.document.add_paragraph("Proccess", style="Heading 1")
        self.add_paragraph_border(para, ["bottom"])
        # for top breakdown
        self.__include_breakdown = include_breakdown
        if include_breakdown:
            self.important_High = 0
            self.important_Low = 0
            self.info = {}

        # for excel
        self.__include_excel = include_excel
        if include_excel:
            self.wb = xlsxwriter.Workbook(
                os.path.join(self.directory, f"{self.filename}.xlsx")
            )
            self.current_worksheet = None
            self.worksheets = {}

        # select logger object
        self.__logger = logger if include_logger else StubObject()

    def __style(self):
        """
        Applies the styles from the stylesheet to the document.
        """
        for style_name, style_attributes in self.stylesheet.items():
            if style_name in self.document.styles:
                style = self.document.styles[style_name]
            elif style_name in ["Symbol"]:
                style = self.document.styles.add_style(
                    style_name, docx.enum.style.WD_STYLE_TYPE.CHARACTER
                )
            else:
                style = self.document.styles.add_style(
                    style_name, docx.enum.style.WD_STYLE_TYPE.PARAGRAPH
                )

            font = style.font
            if style_name == "Title" or "Heading" in style_name:
                style.element.rPr.rFonts.set(
                    qn("w:asciiTheme"), style_attributes.get("font", "Times New Roman")
                )
            else:
                font.name = style_attributes.get("font", "Times New Roman")
            font.size = style_attributes.get("size", Pt(12))
            font.bold = style_attributes.get("bold", False)
            font.color.rgb = style_attributes.get("color", RGBColor(0, 0, 0))

            if style_name not in ["Symbol"]:
                paragraph_format = style.paragraph_format
                paragraph_format.alignment = style_attributes.get(
                    "alignment", WD_ALIGN_PARAGRAPH.LEFT
                )

    def add_watermark(self, watermark_path: str = None):
        if not watermark_path:
            script_dir = os.path.dirname(__file__)
            # Construct the full path to the file
            print(script_dir)
            watermark_path = os.path.join(script_dir, "img/watermark.png")
            print(watermark_path)

        # Open the document footer for editing
        section = self.document.sections[0]
        footer = section.footer

        # Create a paragraph and add image to it
        paragraph = (
            footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
        )
        run = paragraph.add_run()
        run.add_picture(watermark_path, width=Inches(1))  # Adjust width as necessary

        # Center align the paragraph
        paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER

        # Set the image as behind text
        shape = run._element.getparent()
        shape._inline = None  # Make the shape floating
        shape.wrap = True
        shape.width = Inches(1)  # Adjust width as necessary
        shape.height = Inches(1)  # Adjust height as necessary

    def add_change(self, description: str, changes: list[Any]):
        # description of the change

        total_length = 0
        for change in changes:
            if isinstance(change, list):
                for item in change:
                    if isinstance(item, str):
                        total_length += len(item)
                    else:
                        return
            elif isinstance(change, str):
                total_length += len(change)
            elif isinstance(change, pl.DataFrame):
                total_length += sum(len(col) for col in change.columns)
            else:
                return

        self.document.add_paragraph(f"{description}  ")
        para = self.document.add_paragraph()
        # self.__set_paragraph_spacing(para, 0, 0)

        if total_length < 60:
            for index, change in enumerate(changes):
                para.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
                if index != 0:
                    run = para.add_run(" \u2192 ")
                    run.style = self.document.styles["Symbol"]
                if isinstance(change, pl.DataFrame):
                    para.add_run(f"{str(change.columns)} ")
                else:
                    para.add_run(f"{str(change)} ")
        else:
            self.add_change_table(changes)
            self.document.add_paragraph("\n")
        # log change
        self.__logger.info(description)

    def add_hyperlink(self, paragraph, url, text):
        """
        Adds a hyperlink to the audit document.

        Parameters:
        - paragraph: The paragraph to which the hyperlink will be added.
        - url (str): The URL of the hyperlink.
        - text (str): The text to be displayed for the hyperlink.
        """

        part = paragraph.part
        r_id = part.relate_to(
            url, docx.opc.constants.RELATIONSHIP_TYPE.HYPERLINK, is_external=True
        )
        hyperlink = docx.oxml.shared.OxmlElement("w:hyperlink")
        hyperlink.set(docx.oxml.shared.qn("r:id"), r_id)

        new_run = docx.oxml.shared.OxmlElement("w:r")
        rPr = docx.oxml.shared.OxmlElement("w:rPr")
        new_run.append(rPr)
        new_run.text = text.replace("_", " ")
        hyperlink.append(new_run)

        r = paragraph.add_run()
        r._r.append(hyperlink)

        link = self.stylesheet.get("Link")
        color = link.get("color") if link is not None else None
        r.font.color.rgb = (
            self.stylesheet.get("Link").get("color") if color else RGBColor(255, 0, 0)
        )
        r.font.underline = True

        return hyperlink

    def add_important(self, text: str, severity: bool):
        """
        Adds important information to the audit document.

        Parameters:
        - text (str): The text of the important information.
        - severity (bool): The severity level of the information (True for high severity, False for low severity).
        """
        paragraph = self.document.add_paragraph()
        self.__set_paragraph_spacing(paragraph, 0, 0)
        run = paragraph.add_run("\u26A0")
        run.font.highlight_color = WD_COLOR_INDEX.GRAY_25
        self.__format_run(run, Pt(16), (204, 51, 0)) if severity else self.__format_run(
            run, Pt(16), (255, 204, 0)
        )
        run = paragraph.add_run(text)
        run.font.highlight_color = WD_COLOR_INDEX.GRAY_25
        if severity:
            self.important_High += 1
            self.__logger.critical(text)
        else:
            self.important_Low += 1
            self.__logger.warning(text)

    def add_info(self, key: str, value: str | tuple[str, str]):
        """
        Adds information to the audit writer.

        Args:
            key (str): The key of the information to add.
            value (str): The value of the information to add.

        Returns:
            None
        """
        if type(value) is tuple:
            if key in self.info:
                self.info[key][value[0]] = value[1]
            else:
                self.info[key] = {value[0]: value[1]}
        else:
            self.info[key] = value
        self.__logger.info(f"{key} : {value}")

    def add_table(self, text: str, table: pl.DataFrame, table_name: str):
        """
        Adds a table to the xlsx document and creates a link in the current document.

        Parameters:
        - text (str): Text description for the table.
        - table (pl.DataFrame): The DataFrame to be added as a table.
        - table_name (str): The name of the table must not contain spaces.
        """
        if self.__include_excel:
            para = self.document.add_paragraph(f"{text} \u2192 ")
            # self.__set_paragraph_spacing(para, 0, 0)
            table_name = table_name.strip()
            self.add_hyperlink(
                para,
                f"{self.filename}.xlsx#{self.current_worksheet}!{get_column_letter(self.worksheets[self.current_worksheet] + 1)}1",
                table_name,
            )
            # Define a list of available table styles

            # Select a random style
            random_style = random.choice(table_styles)
            table.write_excel(
                workbook=self.wb,
                worksheet=self.current_worksheet,
                table_name=table_name,
                table_style=random_style,
                position=(0, self.worksheets[self.current_worksheet]),
                include_header=True,
            )
            self.worksheets[self.current_worksheet] += len(table.columns) + 1
            call = inspect.getframeinfo(inspect.currentframe().f_back)
            self.__logger.info(
                f"{call.function}:{call.lineno} {text} \n {table_name} created "
                f"at file path {self.filename}.xlsx#{self.current_worksheet}!"
                f"{get_column_letter(self.worksheets[self.current_worksheet] + 1)}1"
            )

    def add_table_snippets(self, table: pl.DataFrame):
        """
        Adds a Python-docx table with column names and types, as well as some rows, based on a polars DataFrame.

        Args:
            table (polars.DataFrame): The DataFrame containing the data.

        Returns:
            None
        """
        cols = table.columns
        doc_tbl = self.document.add_table(rows=1, cols=len(cols))
        doc_tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
        doc_tbl.style = "Table Grid"
        doc_tbl.autofit = True
        hdr_cells = doc_tbl.rows[0].cells
        for index, (name, data_type) in enumerate(zip(cols, table.dtypes)):
            hdr_cells[index].text = name + "\n" + str(data_type)

    def add_change_table(self, changes: list[Any]):
        if not 0 < len(changes) < 8:
            return
        doc_tbl = self.document.add_table(rows=1, cols=((len(changes) * 2) - 1))
        doc_tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
        doc_tbl.style = "Table Grid"
        doc_tbl.autofit = True

        tblBorders = parse_xml(
            r'<w:tblBorders xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            r'<w:top w:val="nil"/><w:left w:val="nil"/><w:bottom w:val="nil"/><w:right w:val="nil"/>'
            r'<w:insideH w:val="nil"/><w:insideV w:val="nil"/>'
            r"</w:tblBorders>"
        )
        doc_tbl._tbl.tblPr.append(tblBorders)
        hdr_cells = doc_tbl.rows[0].cells

        for index, change in enumerate(changes):
            max_length = 1
            if index != 0:  # For every iteration apart from the first
                arrow_cell = hdr_cells[((index * 2) - 1)]
                arrow_cell.width = Cm(1.0)
                arrow_cell.text = "\u2192"
                arrow_cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

                for paragraph in arrow_cell.paragraphs:
                    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    for run in paragraph.runs:
                        run.style = "Symbol"

            cell = hdr_cells[(index * 2) if index != 0 else 0]
            cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            if isinstance(change, str):
                cell.text = change
                max_length = len(change)
            elif isinstance(change, list) and all(
                isinstance(item, str) for item in change
            ):
                cell.text = "\n".join(change)
                max_length = len(max(change, key=len))
            elif isinstance(change, pl.DataFrame):
                temp = change.columns
                cell.text = "\n".join(temp)
                max_length = len(max(temp, key=len))

            cell.width = Cm(min(round(0.42 * max_length), 5))  # Default larger width

    def add_text(self, text: str, style: str | None = None):
        """
        Adds text to the audit document.

        Parameters:
        - text (str): The text to be added.
        """

        para = self.document.add_paragraph(text)

        if style:
            para.style = style
            if "Heading" in style:
                self.add_paragraph_border(para, ["bottom"])

        self.__logger.info(text)
        # para.paragraph_format.left_indent = Inches(0.25)

    def add_top_breakdown(self):
        """
        Adds a breakdown of important information at the top of the audit document.
        """
        if not self.__include_breakdown:
            return

        paragraph = self.document.paragraphs[1].insert_paragraph_before()
        run = paragraph.add_run("\u26A0")
        self.__format_run(run, Pt(16), (255, 204, 0))
        paragraph.add_run(f" {str(self.important_Low)} Warnings raised \n")

        run = paragraph.add_run("\u26A0")
        self.__format_run(run, Pt(16), (204, 51, 0))
        paragraph.add_run(f" {str(self.important_High)} Issues raised \n")

        for element in self.info:
            if type(self.info[element]) is dict:
                paragraph.add_run(f"{element}:\n")
                for key, value in self.info[element].items():
                    run = paragraph.add_run(f"\t• {key}: {value}\n")
            else:
                paragraph.add_run(f"{element}: {self.info[element]}\n")

        paragraph.insert_paragraph_before("breakdown", style="Heading 1")

    @staticmethod
    def __format_run(run: Run, font_size: int, color_rgb: tuple):
        """
        Formats the given run with specified font size and color.
        """
        run.font.size = font_size
        run.font.color.rgb = RGBColor(*color_rgb)

    @staticmethod
    def __set_paragraph_spacing(
        paragraph: Paragraph, space_before: int, space_after: int
    ):
        """
        Sets the spacing for the given paragraph.
        """
        paragraph.paragraph_format.space_before = space_before
        paragraph.paragraph_format.space_after = space_after

    @staticmethod
    def add_paragraph_border(paragraph, border_override: list[str] = None):
        # Get the XML of the paragraph
        p = paragraph._element

        # Create a new element for paragraph borders
        pPr = p.find(qn("w:pPr"))
        if pPr is None:
            pPr = OxmlElement("w:pPr")
            p.insert(0, pPr)

        # Create the border element
        pBdr = OxmlElement("w:pBdr")

        # Define each side of the border
        for border_position in (
            border_override if border_override else ["top", "bottom"]
        ):
            border = OxmlElement(f"w:{border_position}")
            border.set(qn("w:val"), "single")
            border.set(qn("w:sz"), "10")  # Size of the border
            border.set(qn("w:space"), "1")
            border.set(qn("w:color"), "000000")  # Black color
            pBdr.append(border)

        # Append the border element to the paragraph properties
        pPr.append(pBdr)

    def set_page_color(self):
        """
        Set the background color of all pages in a Word document.

        Parameters:
        - doc: Document object from python-docx.
        - color: RGBColor object representing the color.
        """
        # Iterate through all sections in the document
        shd = OxmlElement("w:background")
        # Add attributes to the xml element
        shd.set(qn("w:color"), page_color)
        # Add background element at the start of Document.xml using below
        self.document.element.insert(0, shd)
        background_shp = OxmlElement(
            "w:displayBackgroundShape"
        )  # Setting to use my background
        self.document.settings.element.insert(0, background_shp)  # Apply setting

    def commit_audit(self):
        """
        Commits the audit by adding the top breakdown, closing the workbook, and saving the document.
        """
        self.add_top_breakdown()
        self.set_page_color()

        self.wb.close()
        self.document.save(os.path.join(self.directory, f"{self.filename}.docx"))

    def set_ws(self, worksheet_name: str):
        """
        sets a worksheet to the audit file. can be used to change worksheets

        Args:
            worksheet_name (str): The name of the worksheet.
        """
        if worksheet_name not in self.worksheets:
            self.worksheets[worksheet_name] = 0
        self.current_worksheet = worksheet_name


class StubObject:
    def __init__(self):
        self.info = []

    def __getattr__(self, name):
        return self._stub_callable

    def __setattr__(self, name, value):
        if name == "info" and not hasattr(self, "info"):
            object.__setattr__(self, "info", [])
        else:
            pass

    def _stub_callable(self, *args, **kwargs):
        return self
