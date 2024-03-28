import argparse
import os
import sys
import warnings
from typing import List


def build_parser():
    parser = argparse.ArgumentParser(
        prog="command_maker.py",
        description="""This program can be used to generate and execute repetitive commands. The first
argument defines the command, which should contain one or more wildcards (the
default wildcard is two asterisks; see option -w). For each subsequent argument 
supplied to the program, the wildcards will be replaced with the argument and 
the command will be generated. By default, the program simply prints the 
commands; to run them, use option -r. For a more powerful set of wildcards,
see the -sp option.""",
        epilog="""Examples:

# Copy some files.
command_maker.py -c 'cp data**.txt test/**.dat' 1 2 3

    cp data1.txt test/1.dat
    cp data2.txt test/2.dat
    cp data3.txt test/3.dat

# Rename some files (demonstrates the -sp and -aro options).
command_maker.py --sp       -c 'mv %s.txt %05d.dat' a 1 b 2 c 3
command_maker.py --sp --aro -c 'mv %s.txt %05d.dat' a b c 1 2 3

    mv a.txt 00001.dat
    mv b.txt 00002.dat
    mv c.txt 00003.dat""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    inpt = parser.add_mutually_exclusive_group(required=True)
    inpt.add_argument("--command", "-c", nargs="+", help="command with wildcards")
    inpt.add_argument(
        "-f",
        dest="FILE",
        default=None,
        help="Supply the command-with-wildcards via a file rather than on command line.",
    )

    parser.add_argument("values", nargs="+", help="items to generate commands")
    parser.add_argument(
        "--wildcard",
        "-w",
        dest="X",
        default="**",
        help="Set the wildcard marker to X (default = **).",
    )
    parser.add_argument(
        "-r", action="store_true", help="Run the commands rather than printing them."
    )
    parser.add_argument(
        "-n",
        action="store_true",
        help="Do not attach a newline to the command (applicable when printing).",
    )
    parser.add_argument(
        "--sp",
        action="store_true",
        help="""Process the command through Perl's sprintf() function. In this case, the command should contains sprintf() markers 
                        (\%%s, \%%d, etc.) rather than a generic wildcard. For information on the sprintf() function, run this: 'perldoc -f sprintf'.""",
    )
    parser.add_argument(
        "--aro",
        action="store_true",
        help="Reorder the arguments (useful with the -sp option; see examples).",
    )

    parser.add_argument("-d", action="store_true", help="Debug. Set to off by default.")

    return parser


def cmd_builder(args: argparse.Namespace) -> List[str]:
    """Build commands. Decides whether or not to sprintf or proceed normally."""
    process_potential_file(args)
    if args.sp:
        try:
            res = sprintf(args)
        except ValueError as e:
            print("!!! ERROR PROCESSING COMMANDS !!!")
            raise e
    else:
        res = normal(args)
    return res


def process_potential_file(args: argparse.Namespace) -> None:
    """Processes if there is a file, moves those commands to command and shoves the "command" to values"""
    if args.FILE:
        args.values = list(args.command) + args.values
        with open(args.FILE, "r") as f:
            args.command = [cmd.strip() for cmd in f.readlines()]
    else:
        args.command = [args.command]


def sprintf(args: argparse.Namespace) -> List[str]:
    """Uses format-percents as a way to do wildcards. Also looks if using --aro."""
    final_list = []
    for i, val in enumerate(args.values):
        try:
            if val.count(".") == 1:
                args.values[i] = float(val)
            else:
                args.values[i] = int(val)
        except ValueError:
            pass
    for cmd in args.command:
        size = cmd.count("%")
        if args.aro:
            aro(args, size, cmd, final_list)
        else:
            i = 0
            k = 1
            while k * size < len(args.values):
                try:
                    final_list.append(cmd % tuple(args.values[i : k * size]))
                    i = k * size
                    k += 1
                except TypeError as e:
                    raise ValueError(
                        f"Incorrect value types given. Types do not match.\nCommand:{cmd} \nValues:{tuple(args.values[i : k * size])}"
                    )
            try:
                final_list.append(cmd % tuple(args.values[i:]))
            except TypeError as e:
                raise ValueError(
                    f"Too many arguments given: {args.values}",
                )
    return final_list


def aro(args: argparse.Namespace, size: int, cmd: str, final_list: List[str]) -> None:
    """Does some nifty value reordering to fit wildcards"""
    if size != 0 and len(args.values) % size == 0:

        def split_list(alist, wanted_parts):
            length = len(alist)
            return [
                alist[i * length // wanted_parts : (i + 1) * length // wanted_parts]
                for i in range(wanted_parts)
            ]

        parts = split_list(args.values, size)
        parts = list(zip(*parts))
        # print(parts)
        for part in parts:
            try:
                final_list.append(cmd % part)
            except TypeError as e:
                raise ValueError(
                    f"Incorrect value types given. Types do not match.\nCommand:{cmd} \nValues:{part}"
                )
    else:
        raise ValueError(
            f"""Invalid number of arguments given: 
{len(args.values)} values given to fill {size} spots. Does not divide evenly.
{args.values}"""
        )


def normal(args: argparse.Namespace) -> List[str]:
    """Adds values to replace wildcards in commands"""
    final_list = []
    for cmd in args.command:
        if cmd.count(args.X) == 0:
            warnings.warn(UserWarning(f"!!WARNING: No wildcards found in {cmd}!!"))
        for val in args.values:
            exe = cmd.replace(args.X, val)
            final_list.append(exe)
    return final_list


def determine_final(args: argparse.Namespace, final_list: List[str]) -> None:
    """Run, print without newline, and print with newline"""
    if args.r:
        for cmd in final_list:
            os.system(cmd)
    elif args.n:
        for cmd in final_list:
            print(cmd, end=" ")
        print()
    else:
        for cmd in final_list:
            print(cmd)


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.d:
        sys.tracebacklimit = 0
    try:
        out = cmd_builder(args)
        determine_final(args, out)
    except Exception as e:
        raise e


if __name__ == "__main__":
    main()


# #     parser.add_argument("-p", dest="'PERL'", default=False, help="""Define some Perl code to process the items. Each item will be passed to the code as \$_ in an eval() call.
# # For example, -p 's/\\n\$_//' would remove trailing newlines from the items.""")
