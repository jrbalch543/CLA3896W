import pandas as pd
import subprocess
import sys
import os


def main():
    args = sys.argv
    cmd_args = ["/pkg/ipums/programming/bin/_cross_quick_rust_"] + args[1:]
    out = subprocess.run(cmd_args, capture_output=True)
    if "-h" in cmd_args or out.stdout != b"":
        print(out.stdout.decode("utf-8"))
        exit()
    if out.stderr != b"":
        print(out.stderr.decode("utf-8"))
        exit()
    df = pd.read_csv("crosstab_output.csv")
    print(
        pd.crosstab(
            df["column_0"],
            df["column_1"],
            values=df["Frequency"],
            aggfunc="sum",
            margins=True,
            margins_name="*TOT*",
        )
        .fillna(0)
        .convert_dtypes()
    )
    os.remove("crosstab_output.csv")


if __name__ == "__main__":
    main()
