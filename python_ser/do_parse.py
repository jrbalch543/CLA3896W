import re
import polars as pl
from itertools import chain
import argparse
import sys


class DataList:
    def __init__(self, rectype, name, start, end, fmt=None):
        self.rectype = rectype
        self.name = name
        self.start = start
        self.end = end
        self.fmt = fmt
        self.label = None
        self.valuelabels = []

    def __repr__(self) -> str:
        return f"DataList(Rectype: {self.rectype}, Name: {self.name}, Start: {self.start}, End: {self.end}, Fmt: {self.fmt}, Label: {self.label}, ValueLabels: {self.valuelabels})"

    def set_rt(self, new_rt):
        self.rectype = new_rt

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

    def set_fmt(self, new_fmt):
        self.fmt = new_fmt


class VarLabel:
    def __init__(self, name, label):
        self.name = name
        self.label = label

    def __repr__(self) -> str:
        return f"VarLabel(Name: {self.name}, Label: {self.label})"


class ValueLabel:
    def __init__(self, parent, value, label) -> None:
        self.parent = parent
        self.value = value
        self.label = label

    def set_parent(self, new_par):
        self.parent = new_par

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


def build_file_sections(filename):
    data_list = []
    rectype_chunk = []
    var_labels = {}
    val_labels = {}
    with open(filename, "r") as f:
        while True:
            line = f.readline()
            if line:
                if m := re.match(r"\s+(\w+)\s+(\w+)\s+(\d+)-(\d+)\s+", line):
                    if m.group(1) == "str":
                        rectype_chunk.append(
                            DataList(".", m.group(2), m.group(3), m.group(4), "a")
                        )
                    else:
                        rectype_chunk.append(
                            DataList(".", m.group(2), m.group(3), m.group(4), None)
                        )
                elif m := re.match(r"drop if rectype != `\"(.)\"", line):
                    set_rectypes = [dl.set_rt(m.group(1)) for dl in rectype_chunk]
                    data_list.extend(rectype_chunk)
                    rectype_chunk = []
                elif m := re.match(r"replace (\w+)\s+=\s+(\w+)\s+\/\s(\w+)", line):
                    indices = [
                        i
                        for i in range(len(data_list))
                        if data_list[i].name == m.group(1)
                    ]
                    for idx in indices:
                        data_list[idx].set_fmt(str(m.group(3).count("0")))
                elif m := re.match(r"label var (\w+)\s+`\"(.+)\"", line):
                    var_labels[m.group(1)] = m.group(2)
                elif m := re.match(r"label define (\w+)\s+(-*\d+)\s+`\"(.+)\"", line):
                    if m.group(1) not in val_labels:
                        val_labels[(m.group(1))] = [(m.group(2), m.group(3))]
                    else:
                        val_labels[m.group(1)].append((m.group(2), m.group(3)))
                elif m := re.match(r"label values (\w+)\s+(\w+)", line):
                    val_labels[m.group(1)] = val_labels.pop(m.group(2))
                    val_labels[m.group(1)] = list(
                        map(
                            lambda vl: ValueLabel(m.group(1), vl[0], vl[1]).to_row(),
                            val_labels[m.group(1)],
                        )
                    )
            else:
                if len(rectype_chunk) > 0:
                    data_list.extend(rectype_chunk)
                break
    return {"DataList": data_list, "VarLabels": var_labels, "ValueLabels": val_labels}


def zipper(file_secs):
    for dl in file_secs["DataList"]:
        try:
            dl.label = file_secs["VarLabels"][dl.name]
        except KeyError as e:
            print(f"!!! LABEL NOT FOUND FOR {dl.name} !!!")
            raise e
        try:
            dl.valuelabels.extend(file_secs["ValueLabels"][dl.name])
        except KeyError:
            pass


def main():
    parser = build_parser()
    args = parser.parse_args()
    file_sections = build_file_sections(args.filename)
    zipper(file_sections)
    df = build_df(file_sections["DataList"])
    print(df.write_csv(file=None, separator="\t"), file=sys.stdout)


if __name__ == "__main__":
    main()
