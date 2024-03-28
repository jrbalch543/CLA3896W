from ipums.metadata import IPUMS
from xml.dom import minidom
from glob import glob
import argparse


def make_parser():
    """Allow user CMD Line args"""
    parser = argparse.ArgumentParser(
        description="""
    ---------------------
    Validate XML metadata
    ---------------------
    This program reports whether any metadata files fail to parse as valid XML.
    It can be run in two modes:
        - Project mode: Supply one or more project names as 
                        command-line arguments (usa, ipumsi, etc).
        - File mode:    Supply XML file names directly at the command line.\n""",
        epilog="""
    If there are any XML errors, the results are written to the screen unless 
    you redirect the output (recommended). If you do redirect the output, 
    only the program's status is written to the screen.""",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument("-f", "--fileMode", action="store_true", help="File mode.")
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Quiet mode: do not print project to screen.",
    )
    parser.add_argument(
        "-sq",
        "--smartQuotes",
        action="store_true",
        help="Smart Quotes: Also check file for smart quotes, smart apostrophes, etc.",
    )
    parser.add_argument(
        "filenameOrProject",
        nargs="*",
        help="Enter files or projects directly from command line.",
    )
    return parser


def try_to_parse_files(directory, file_list, args):
    """Attempt to parse files in file list from given directory or CMD Line"""
    n = 0
    bad_file_list = []
    good_file_list = []
    if not args.quiet:
        print(
            "     Checking {xml} files: {directory}".format(
                xml=len(file_list), directory=directory
            )
        )
    for file in file_list:
        n += 1
        if not args.quiet:
            print("     " + str(n) + ". " + str(file))
        try:
            if args.smartQuotes:
                check_for_smart_quotes(file)
            f = minidom.parse(str(file))
            good_file_list.append(str(file))
        except:
            print("Error parsing file: {}".format(str(file)))
            bad_file_list.append(file)
    if len(bad_file_list) > 0 and (not args.quiet):
        bad_file_num = 1
        print("Could not open from: " + str(directory))
        for file in bad_file_list:
            print("     {num}. {f}".format(num=bad_file_num, f=str(file)))
            bad_file_num += 1
    print("Finished Parsing Files.")
    return good_file_list


def check_for_smart_quotes(
    f,
):
    """Check for smart quotes in xml file (smart quotes, aposostrophes, em-dashes, and en-dashes)"""
    mistakes = [
        b"\u2026",
        b"\u2013",
        b"\u2014",
        b"\u2018",
        b"\u2019",
        b"\u201B",
        b"\u201C",
        b"\u201D",
        b"\u201F",
    ]  ## Binary values for Elipses, apost, quote, quote, en-dash, em-dash. These characters do not exist in ASCII, so binary values are used.
    bad_files = False
    try:
        with open(f, "rb") as fh:
            line_num = 0
            for line in fh:
                line_num += 1
                if any([x in line for x in mistakes]):
                    print("Contains invalid character on line " + str(line_num))
                    bad_files = True
                    break
    except:
        print("Could not open file for reading: " + f)
        bad_files = True
    return bad_files


def args_parser(direc, parser, args):
    """Run parser and script functionality"""
    if args.filenameOrProject != []:  ## If CMD line arg given
        if args.fileMode:
            direc.append(
                try_to_parse_files("from command line", args.filenameOrProject, args)
            )
        else:
            for p in args.filenameOrProject:
                try:
                    ipums_p = IPUMS(p)
                    direc.append(
                        try_to_parse_files(
                            str(ipums_p), glob(ipums_p.project.path + "/**/*.xml"), args
                        )
                    )
                except:
                    continue
    else:
        parser.print_help()
    return direc


def main():
    direc = []
    parser = make_parser()
    args = parser.parse_args()
    run = args_parser(direc, parser, args)


if __name__ == "__main__":
    main()
