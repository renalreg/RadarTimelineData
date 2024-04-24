import os
from typing import Any, List

from docx import Document
from docx.enum.dml import MSO_THEME_COLOR_INDEX
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import (
    WD_COLOR_INDEX,
    WD_ALIGN_PARAGRAPH,
    WD_TAB_ALIGNMENT,
    WD_TAB_LEADER,
)
from docx.oxml import parse_xml
from docx.oxml.ns import qn
from docx.shared import Pt, RGBColor, Inches
import docx
import polars as pl
import xlsxwriter
from docx.text.paragraph import Paragraph
from docx.text.run import Run
from openpyxl.utils import get_column_letter


class AuditWriter:
    def __init__(self, directory: str, filename: str):
        """
        Initializes an AuditWriter object.

        Parameters:
        - directory (str): The directory where the output files will be saved.
        - filename (str): The name of the audit file.
        """
        self.directory = directory
        self.document = Document()
        self.__style()

        self.document.add_heading("Audit " + filename, 0)
        self.document.add_paragraph("Proccess", style="Heading 1")

        self.filename = filename
        self.important_High = 0
        self.important_Low = 0
        self.info = {}

        self.wb = xlsxwriter.Workbook(
            os.path.join(self.directory, self.filename + ".xlsx")
        )
        self.current_worksheet = None
        self.worksheets = {}

    def add_change(self, description: str, old: Any, new: Any):
        """
        Adds a change description along with old and new data representations to the document.

        Args:
            description (str): Description of the change.
            old (Any): Old data representation.
            new (Any): New data representation.

        Returns:
            None
        """
        para = self.document.add_paragraph(description)
        para.style = "List Bullet"
        if isinstance(old, pl.DataFrame) and isinstance(new, pl.DataFrame):
            self.add_table_snippets(old)
            para = self.document.add_paragraph()
            para.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run = para.add_run("\u21A7")
            run.font.size = Pt(20)
            self.add_table_snippets(new)

        elif isinstance(old, list) and isinstance(new, list):
            para = self.document.add_paragraph()
            para.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
            para.add_run(str(old) + "\n")
            run = para.add_run("\u21A7 \n")
            run.font.size = Pt(20)
            para.add_run(str(new) + "\n")

    def add_hyperlink(self, paragraph, url, text, color):
        """
        Adds a hyperlink to the audit document.

        Parameters:
        - paragraph: The paragraph to which the hyperlink will be added.
        - url (str): The URL of the hyperlink.
        - text (str): The text to be displayed for the hyperlink.
        - color: Color of the hyperlink.
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
        new_run.text = text
        hyperlink.append(new_run)
        r = paragraph.add_run()
        r._r.append(hyperlink)
        r.font.color.theme_color = MSO_THEME_COLOR_INDEX.HYPERLINK
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
        else:
            self.important_Low += 1

    def add_info(self, key: str, value: str):
        self.info[key] = value

    def add_table(self, text: str, table: pl.DataFrame, table_name: str):
        """
        Adds a table to the xlsx document and creates a link in the current document.

        Parameters:
        - text (str): Text description for the table.
        - table (pl.DataFrame): The DataFrame to be added as a table.
        - table_name (str): The name of the table must not contain spaces.
        """
        table_name = table_name.strip()
        para = self.document.add_paragraph(text + " ")
        para.style = "List Bullet"
        self.add_hyperlink(
            para,
            self.directory
            + "/"
            + self.filename
            + ".xlsx"
            + r"#"
            + self.current_worksheet
            + r"!"
            + (get_column_letter(self.worksheets[self.current_worksheet] + 1))
            + "1",
            table_name,
            None,
        )
        table.write_excel(
            workbook=self.wb,
            worksheet=self.current_worksheet,
            table_name=table_name,
            position=(0, self.worksheets[self.current_worksheet]),
        )
        self.worksheets[self.current_worksheet] += len(table.columns) + 1

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

    def add_text(self, text: str, bulletpoint: bool = False):
        """
        Adds text to the audit document.

        Parameters:
        - text (str): The text to be added.
        """

        para = self.document.add_paragraph(text)
        self.__set_paragraph_spacing(para, 0, 0)

        if bulletpoint:
            para.style = "List Bullet 2"

        else:
            para.style = "List Bullet"

            # para.paragraph_format.left_indent = Inches(0.25)
            # para.paragraph_format.right_indent = Inches(0.25)

    def __style(self):
        styles = self.document.styles
        paragraph_styles = [s for s in styles if s.type == WD_STYLE_TYPE.PARAGRAPH]
        for style in paragraph_styles:
            style.font.name = "Cascadia Code"
        for i in range(1, 9):
            h = styles["Heading %d" % i]
            rFonts = h.element.rPr.rFonts
            rFonts.set(qn("w:asciiTheme"), "Cascadia Code")
        h = styles["Title"]
        rFonts = h.element.rPr.rFonts
        rFonts.set(qn("w:asciiTheme"), "Cascadia Code")

    def add_top_breakdown(self):
        """
        Adds a breakdown of important information at the top of the audit document.
        """
        paragraph = self.document.paragraphs[1].insert_paragraph_before()
        run = paragraph.add_run("\u26A0")
        self.__format_run(run, Pt(16), (255, 204, 0))
        paragraph.add_run(" " + str(self.important_Low) + " Warnings raised ")
        self.__set_paragraph_spacing(paragraph, 0, 0)

        paragraph = paragraph.insert_paragraph_before()
        self.__set_paragraph_spacing(paragraph, 0, 0)
        run = paragraph.add_run("\u26A0")
        self.__format_run(run, Pt(16), (204, 51, 0))
        paragraph.add_run(" " + str(self.important_High) + " Issues raised ")
        self.__set_paragraph_spacing(paragraph, 0, 0)

        for info in reversed(self.info):
            paragraph = paragraph.insert_paragraph_before()
            paragraph.add_run(info + " : " + self.info[info])
            self.__set_paragraph_spacing(paragraph, 0, 0)

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

    def commit_audit(self):
        """
        Commits the audit by adding the top breakdown, closing the workbook, and saving the document.
        """
        self.add_top_breakdown()
        self.wb.close()
        self.document.save(os.path.join(self.directory, self.filename + ".docx"))

    def set_ws(self, worksheet_name: str):
        """
        sets a worksheet to the audit file. can be used to change worksheets

        Parameters:
        - worksheet_name (str): The name of the worksheet.
        """
        if worksheet_name in self.worksheets:
            self.current_worksheet = worksheet_name

        else:
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
