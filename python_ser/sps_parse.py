import re
import polars as pl
from itertools import chain
import argparse
import sys

COMBOS = [
    ("VARIABLE LABELS\n", r"^record type \"(\w*)\""),
    ("VALUE LABELS\n", None),
    (".\n", r"^\s+/?(\w+)\s$"),
]


class DataList:
    def __init__(self, rectype, name, start, end, fmt=None):
        self.rectype = rectype
        self.name = name
        self.start = start
        self.end = end
        self.fmt = fmt
        self.label = None
        self.valuelabels = []

    @classmethod
    def parse(cls, line, rectype):
        regex = r"\s+(\w+)\s+(\d+)-*(\d+)?\s(\(\w\))?"
        res = re.finditer(regex, line)
        new = []
        for m in res:
            new.append(
                cls(
                    rectype,
                    m.group(1),
                    m.group(2),
                    m.group(3),
                    m.group(4).strip("()") if m.group(4) else None,
                )
            )
        return new

    def __repr__(self) -> str:
        return f"DataList(Rectype: {self.rectype}, Name: {self.name}, Start: {self.start}, End: {self.end}, Fmt: {self.fmt}, Label: {self.label}, ValueLabels: {self.valuelabels})"

    def to_df(self):
        head_line = [
            (
                self.rectype,
                self.name.lower(),
                str(self.start),
                str(int(self.end) - int(self.start) + 1) if self.end else "1",
                self.fmt if self.fmt else "",
                "",
                self.label,
                "",
                self.label,
                "",
            )
        ]

        return pl.DataFrame(
            head_line + self.valuelabels if self.valuelabels else head_line,
            orient="row",
        )


class VarLabel:
    def __init__(self, name, label):
        self.name = name
        self.label = label

    @classmethod
    def parse(cls, line, leader):
        regex = r"\s+(\w+)\s+(\'|\")(.+)(\'|\")\s"
        m = re.match(regex, line)
        if m:
            return cls(m.group(1), m.group(3))

    def __repr__(self) -> str:
        return f"VarLabel(Name: {self.name}, Label: {self.label})"


class ValueLabel:
    def __init__(self, parent, value, label) -> None:
        self.parent = parent.rstrip("_f")
        self.value = value
        self.label = label

    @classmethod
    def parse(cls, line, leader):
        regex = r"\s+(\w+)\s+(\'|\")(.+)(\'|\")\s"
        m = re.match(regex, line)
        if m and leader != ".":
            return cls(leader, m.group(1), m.group(3))

    def __repr__(self) -> str:
        return f"ValueLabel(Parent: {self.parent}, Value: {self.value}, Label: {self.label})"

    def to_row(self):
        return ("", "", "", "", "", self.value, "", self.label, "", self.label)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="sps_parse.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="This utility parses an SPSS syntax file and writes its content as a tab-delimited data dictionary.",
        epilog="""
The parser expects a syntax file with three basic sections: DATA LIST,
VARIABLE LABELS, and VALUE LABELS. Optionally, there can be multiple
DATA LIST commands, each for a different RECORD TYPE. Additional requirements
are described in this schematic overview of the expected file structure.
    record type 'X'.                         * Optional.
    data list
      * Each variable on a separate line.
    .                                        * Terminators on own line.
    record type 'Y'.                         * Optional.
    data list
      * Each variable on a separate line.
    .
    variable labels
      * Each variable on a separate line.
      * Use single-quotes or double-quotes for labels.
    .
    value labels
      * Each variable on a separate line, preceded by a forward slash.
      * Each value-label pair on a separate line.
      * Use single-quotes or double-quotes for labels.
    .
The parser ignores the following:
    - lines before the RECORD TYPE or DATA LIST commands
    - lines after the command terminator for VALUE LABELS
    - leading and trailing white space on all lines
    - blank lines""",
    )
    parser.add_argument("filename", help="SPSS syntax file")
    parser.add_argument(
        "-stat_trans",
        action="store_true",
        help="Depricated. Do not need to include in order to files created by Stat Transfer.",
    )
    parser.add_argument(
        "-debug_ST",
        action="store_true",
        help="Depricated. File does not deliniate between Stat Transfer files and non Stat Transfer files.",
    )
    return parser


def main():
    args = build_parser().parse_args()
    file_secs = build_file_sections(args.filename, COMBOS)
    zipper(file_secs)
    df = build_df(file_secs["DataList"])
    print(df.write_csv(file=None, separator="\t"), file=sys.stdout)


def build_file_sections(filename, combos):
    try:
        with open(filename, "r") as f:
            file_secs = {
                "DataList": parse_file_sections(f, DataList, combos[0]),
                "VarLabels": parse_file_sections(f, VarLabel, combos[1]),
                "ValueLabels": parse_file_sections(f, ValueLabel, combos[2]),
            }
        return file_secs
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Unable to process file. Are you sure this is the .sps file you're looking for?"
        )


def parse_file_sections(file, cur, combo):
    ender, sec_lead = combo
    vals = []
    leader = "."
    for line in file:
        if line.upper() == ender:
            if cur == DataList:
                return list(chain.from_iterable(vals))
            else:
                return vals
        if sec_lead:
            if m := re.search(sec_lead, line):
                leader = m.group(1)
        v = cur.parse(line, leader)
        if v:
            vals.append(v)
    return vals


def zipper(file_secs):
    data_list, var_labels, value_labels = (
        file_secs["DataList"],
        file_secs["VarLabels"],
        file_secs["ValueLabels"],
    )
    var_l = {v.name: v.label for v in var_labels}
    value_l = {v.parent: [] for v in value_labels}
    [(value_l[v.parent]).append(v.to_row()) for v in value_labels]
    for d in data_list:
        d.label = var_l[d.name] if d.name in var_l else ""
        d.valuelabels = value_l[d.name] if d.name in value_l else None


def build_df(dl):
    df = pl.concat([d.to_df() for d in dl])
    df.columns = [
        "RecordType",
        "Var",
        "Col",
        "Wid",
        "Frm",
        "Value",
        "VarLabel",
        "ValueLabel",
        "VarLabelOrig",
        "ValueLabelOrig",
    ]
    empty_cols_1 = pl.DataFrame(
        {
            "Freq": [],
            "Sel": [],
            "Notes": [],
            "Svar": [],
            "ValueSvar": [],
        },
        schema={
            "Freq": pl.Utf8,
            "Sel": pl.Utf8,
            "Notes": pl.Utf8,
            "Svar": pl.Utf8,
            "ValueSvar": pl.Utf8,
        },
    )
    df2 = df.select(
        pl.col("VarLabelOrig").alias("VarLabelSvar"),
        pl.col("ValueLabelOrig").alias("ValueLabelSvar"),
    )
    empty_cols_2 = pl.DataFrame(
        {
            "UnivSvar": [],
            "NoRec": [],
            "NonTab": [],
            "Hide": [],
            "Decim": [],
            "String": [],
            "CommP": [],
            "CodeTy": [],
            "DDoc1": [],
            "Dtag1": [],
            "JDoc1": [],
            "JTag1": [],
            "DDoc2": [],
            "DTag2": [],
            "JDoc2": [],
            "JTag2": [],
        },
        schema={
            "UnivSvar": pl.Utf8,
            "NoRec": pl.Utf8,
            "NonTab": pl.Utf8,
            "Hide": pl.Utf8,
            "Decim": pl.Utf8,
            "String": pl.Utf8,
            "CommP": pl.Utf8,
            "CodeTy": pl.Utf8,
            "DDoc1": pl.Utf8,
            "Dtag1": pl.Utf8,
            "JDoc1": pl.Utf8,
            "JTag1": pl.Utf8,
            "DDoc2": pl.Utf8,
            "DTag2": pl.Utf8,
            "JDoc2": pl.Utf8,
            "JTag2": pl.Utf8,
        },
    )
    return pl.concat([df, empty_cols_1, df2, empty_cols_2], how="horizontal")


if __name__ == "__main__":
    main()
