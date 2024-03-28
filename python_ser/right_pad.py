import argparse


def build_parser():
    parser = argparse.ArgumentParser(
        prog="Right_pad.py",
        description="""This program right-pads a data file so that all lines have a minimum length, ignoring newlines. In the process, newlines are converted to Unix newlines. Lines longer than the minimum are not truncated.""",
    )
    parser.add_argument("data_file", help="(use '-' if supplied via a pipe)")

    gen = parser.add_argument_group("Options (general)")
    gen.add_argument("--len", metavar="N", type=int, help="Specify the desired length.")
    gen.add_argument(
        "-p",
        metavar="X",
        help="Specify the padding character (default = space).",
        default=" ",
    )

    rt = parser.add_argument_group("Options (for data files that use record types)")
    rt.add_argument("--rt", nargs="*", help="Specify record type values.")
    rt.add_argument(
        "--rt_len", nargs="*", type=int, help="Desired lengths for each record type."
    )
    rt.add_argument(
        "-s",
        nargs="?",
        type=int,
        help="Specify the start column for the record type (1 indexed).",
    )
    rt.add_argument(
        "-w", nargs="?", type=int, help="Specify the width for the record type."
    )
    gen.add_argument(
        "-d",
        action="store_true",
        help="Debug. Print output to screen rather than to original file.",
    )

    return parser.parse_args()


def pad_line(line, padder, leng):
    if len(line) > leng:
        raise ValueError(f"Given line length is less than existing line lengths")
    try:
        line = line.replace("\n", "").ljust(leng, padder)
        return line
    except TypeError:
        raise TypeError(f"Padding character {padder} must be one character.")


def rec_type_padder(line, args):
    try:
        asserter(args)
    except AssertionError as e:
        print("!!! ERROR FOUND PARSING COMMAND LINE ARGUMENTS !!!")
        raise e
    try:
        slic = args.s - 1
        rt = line[slic : slic + args.w]
        i = args.rt.index(rt)
        line = pad_line(line, args.p, args.rt_len[i])
        return line
    except ValueError as e:
        if "is not in list" in str(e):
            raise ValueError(
                f"Could not find any matching rectype from given {args.rt} as {rt}"
            )
        raise e
    except TypeError as e:
        raise e


def asserter(args):
    try:
        assert args.rt and args.rt_len
    except AssertionError:
        raise AssertionError("Need to supply both record types and lengths")
    try:
        assert len(args.rt) == len(args.rt_len)
    except AssertionError:
        raise AssertionError("Need to supply equal numbers of record types and lengths")
    try:
        assert args.s != None and args.w != None
    except AssertionError:
        raise AssertionError(
            "Need to specify both start column and width for record types"
        )


def read_data_file(args):
    output_lines = []
    try:
        with open(args.data_file, "r") as f:
            while True:
                line = f.readline()
                if line:
                    if args.rt == None:
                        try:
                            line = pad_line(line, args.p, args.len)
                        except TypeError as e:
                            raise e
                        except ValueError as e:
                            raise e
                    else:
                        line = rec_type_padder(line, args)
                    output_lines.append(line)
                else:
                    break
        return output_lines
    except FileNotFoundError:
        raise FileNotFoundError(f"Unable to find {args.data_file}")


def main():
    args = build_parser()
    try:
        output_lines = read_data_file(args)
        if not args.d:
            with open(args.data_file, "w") as f:
                for line in output_lines:
                    f.write(line + "\n")
        else:
            for line in output_lines:
                print(line.rstrip("\n"))
    except FileNotFoundError as e:
        raise e


if __name__ == "__main__":
    main()
