import argparse
from pathlib import Path
from .do_parse import main as do_parse
from .sas_parse import main as sas_parse
from .sps_parse import main as sps_parse


def build_parser():
    parser = argparse.ArgumentParser(
        prog="statfile_parse.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
-----------------------
SPSS/SAS/Stata Syntax File Parser
-----------------------

This utility parses a syntax file from one of SPSS, SAS, or Stata packages and writes 
its content as a tab-delimited data dictionary.

Command-line arguments:
    syntax file ( file needs file extension of .sas .sps or .do )

Output:
    written to standard output (typically, the screen)

Options:
    -help        Display this help message.
    -stat_trans  Use when processing .sps files created by Stat Transer.
                 With this option in effect, the .sps file will be
                 pre-processed, converting it into the format described below
                 so that it can then be parsed.
    -debug_ST    Just run the -stat_trans pre-processing step.

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
    parser.add_argument("filename", help=".sps, .sas, or .do file")
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
    decide_which(args)


def decide_which(args):
    suffix = Path(args.filename).suffix
    if suffix == ".do":
        do_parse()
    elif suffix == ".sas":
        sas_parse()
    elif suffix == ".sps":
        sps_parse()
    else:
        print(
            f"ERROR: File did not end in one of .do, .sas, or .sps. File supplied is of type {suffix}."
        )


if __name__ == "__main__":
    main()
