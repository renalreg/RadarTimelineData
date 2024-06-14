from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_COLOR_INDEX
from docx.shared import Pt, RGBColor

body_font = "Cascadia Code"
title_font = "Cascadia Code"
heading_font = "Cascadia Code"
text_color = RGBColor(14, 1, 36)
heading_color = RGBColor(14, 1, 36)
primary_color = RGBColor(237, 25, 32)
secondary_color = RGBColor(119, 213, 244)
accent_color = RGBColor(136, 79, 241)
page_color = "FFEAE2"
# Define the stylesheet
stylesheet = {
    "Title": {
        "font": body_font,
        "size": Pt(26),
        "bold": True,
        "alignment": WD_ALIGN_PARAGRAPH.CENTER,
        "color": text_color,
    },
    "Heading 1": {
        "font": body_font,
        "size": Pt(20),
        "bold": True,
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": heading_color,
    },
    "Heading 2": {
        "font": body_font,
        "size": Pt(16),
        "bold": True,
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": heading_color,
    },
    "Heading 3": {
        "font": body_font,
        "size": Pt(14),
        "bold": False,
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": heading_color,
    },
    "Heading 4": {
        "font": body_font,
        "size": Pt(12),
        "bold": False,
        "italic": True,
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": heading_color,
    },
    "Normal": {
        "font": body_font,
        "size": Pt(10),
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": text_color,
    },
    "List Bullet": {
        "font": body_font,
        "size": Pt(10),
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": text_color,
    },
    "Link": {
        "font": body_font,
        "size": Pt(10),
        "color": accent_color,
    },
    "Symbol": {
        "font": body_font,
        "size": Pt(20),
        "color": secondary_color,
    },
    "Quote": {
        "font": body_font,
        "size": Pt(12),
        "italic": True,
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": text_color,
    },
    "Caption": {
        "font": body_font,
        "size": Pt(10),
        "italic": True,
        "alignment": WD_ALIGN_PARAGRAPH.CENTER,
        "color": text_color,
    },
    "Code": {
        "font": body_font,
        "size": Pt(10),
        "alignment": WD_ALIGN_PARAGRAPH.LEFT,
        "color": RGBColor(42, 161, 152),  # Example color for code
        "highlight_color": WD_COLOR_INDEX.GRAY_50,
    },
}
# default word table styles
table_styles = [
    "Table Style Medium 1",
    "Table Style Medium 2",
    "Table Style Medium 3",
    "Table Style Medium 4",
    "Table Style Medium 5",
    "Table Style Medium 6",
    "Table Style Medium 7",
    "Table Style Medium 8",
    "Table Style Medium 9",
    "Table Style Light 1",
    "Table Style Light 2",
    "Table Style Light 3",
    "Table Style Light 4",
    "Table Style Light 5",
    "Table Style Light 6",
    "Table Style Light 7",
    "Table Style Light 8",
    "Table Style Light 9",
    "Table Style Light 10",
    "Table Style Light 11",
    "Table Style Dark 1",
    "Table Style Dark 2",
    "Table Style Dark 3",
    "Table Style Dark 4",
    "Table Style Dark 5",
    "Table Style Dark 6",
    "Table Style Dark 7",
    "Table Style Dark 8",
    "Table Style Dark 9",
    "Table Style Dark 10",
]
