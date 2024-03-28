from pathlib import Path
from argparse import ArgumentParser


def make_parser():
    parser = ArgumentParser(
        prog="gzdo.py",
        description="""
---------------------------------------------
Replace 'infix' with 'gzinfix' in a 'do' file
---------------------------------------------
""",
    )
    parser.add_argument("filename", help="filename or filepath to a 'do' file")
    return parser


def rewrite(filename):
    assert Path(filename).suffix == ".do", ValueError(
        f"{filename} is not a STATA '.do' file. Cannot process {Path(filename).suffix}."
    )
    with open(file=filename, mode="r") as f:
        content = f.readlines()
    content = [line.replace("infix", "gzinfix") for line in content]
    with open(file=Path(".") / filename, mode="w") as f:
        for line in content:
            f.write(line)


def main():
    parser = make_parser()
    args = parser.parse_args()
    rewrite(args.filename)


if __name__ == "__main__":
    main()
