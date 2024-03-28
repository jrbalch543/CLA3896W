import argparse
import os
import re
import getpass
from collections import OrderedDict
from ipums.metadata import IPUMS


def make_and_check_go_data(godata_file, match_str):
    """Check if Go file exists, checks formatting, and populates dictionary for writing later"""
    direc = {}
    try:
        with open(godata_file, "r") as go:
            for line in go:
                match = re.search(match_str, line)
                if match:
                    key = match.group(1)
                    path = match.group(2)
                    direc[key] = path
                else:
                    print("Bad line in go data file:\n" + str(line))
    except:
        print("No GoData File")
        with open(godata_file, "w") as go:
            print("File Created")
            make_and_check_go_data(godata_file, match_str)
    return direc


def show_go_paths(godata_file):
    """Reads Go file for certain methods later"""
    lines = []
    try:
        with open(godata_file, "r") as go:
            if os.stat(godata_file).st_size == 0:
                print(
                    "\tFile previously cleared. No GO paths currently exist. Try -add to add a new GO path."
                )
            for line in go:
                print(line)
                lines.append(line)
    except:
        print("Failed to open file for reading: " + str(godata))
    return lines


def write_go_paths(godata_file, go_paths):
    """Writes Go file from go_paths given"""
    try:
        go_paths = OrderedDict(sorted(go_paths.items()))
        with open(godata_file, "w") as go:
            for key in go_paths.keys():
                go.write("\t" + str(key) + "\t" + str(go_paths[key]) + " \n")
        return True
    except:
        print("Failed to open file for writing: " + str(godata_file))
        return False


def clear_go(godata_file, go_paths, parser, args):
    """Method re-writes Go file, all arguments exists in this in order to not cause errors later."""
    decision = input("Enter 'yes' to clear all paths: ")
    run_clear_go(godata_file, decision)


def run_clear_go(godata_file, decision):
    """Clears Go file based on decision input"""
    acceptable = [
        "yes",
        "Yes",
        "Y",
        "y",
        "clear",
    ]
    if decision in acceptable:
        write_go_paths(godata_file, {})
        print("Go File Cleared")
        return True
    else:
        print("Go File Not Cleared")
        return False


def add_go(godata_file, go_paths, parser, args):
    """Adds a key to Go file"""
    if os.path.exists(args.PATH):
        go_paths[args.KEY] = args.PATH
        write_go_paths(godata_file, go_paths)
        print("Go Key-Path Added")
        return go_paths
    else:
        print("Invalid Path")
        parser.print_help()
        return None


def ren_go(godata_file, go_paths, parser, args):
    """Renames a Go path"""
    try:
        go_paths[args.NEWKEY] = go_paths[args.OLDKEY]
        del go_paths[args.OLDKEY]
        write_go_paths(godata_file, go_paths)
        print(
            "Go Key " + str(args.OLDKEY) + " Renamed. \nNew Name: " + str(args.NEWKEY)
        )
        return go_paths
    except:
        print("Invalid Key")
        return None


def del_go(godata_file, go_paths, parser, args):
    """Delete a path from Go file"""
    try:
        del go_paths[args.KEY]
        write_go_paths(godata_file, go_paths)
        print("Go Path Deleted")
        return go_paths
    except:
        print("Invalid Key")
        return None


def ws_go(godata_file, go_paths, parser, args):
    """Add a workspace path to Go file"""
    try:
        projAndName = args.WORKSPACE.split("/")
        project = projAndName[0]
        name = projAndName[1]
        proj = IPUMS(project)
        proj_path = proj.project.path
        full_path = os.path.join("~", proj_path, "workspaces", name)
        if os.path.exists(full_path):
            go_paths[project + "/" + name] = full_path
            write_go_paths(godata_file, go_paths)
            print("Successfully added " + project + "/" + name + " to Go File")
            return go_paths
        else:
            print("Invalid Input: Try PROJECT/WORKSPACE_NAME")
            parser.print_help()
            return None
    except:
        print("Invalid Input: Try PROJECT/WORKSPACE_NAME")
        parser.print_help()
        return None


def go_go(godata_file, go_paths, parser, args):
    """Either show all Go paths or show specific paths based on user input"""
    keys = args.KEY[0]
    output = []
    if keys != []:
        badkeys = []
        for key in keys:
            if key in go_paths.keys():
                print(go_paths[key])
                output.append(go_paths[key])
            else:
                print("Invalid Key: " + key)
                badkeys.append(key)
        if len(badkeys) > 0:
            print("\nInvalid Keys: " + str(badkeys))
            parser.print_help()
    else:
        print("Showing paths...")
        output = show_go_paths(godata_file)
    return output


def run_parser(file_name):
    """Run argParse to allow for user input"""
    parser = argparse.ArgumentParser(
        description="""
This script is an improved version of the Unix pushd(), popd(), and dirs() commands.  It creates a data file in the user's home directory to store paths as key-value pairs:\n
                KEY     /some/path
                KEY     /some/other/path
                etc.\n
The user can then `cd` to those paths, using the assigned KEY.\n
                go KEY\n
The script is designed to be run within the context of a Unix `cd` command.  In other words, whatever go.py prints to STDOUT becomes the argument supplied to `cd`.')""",
        epilog="""
To get starting using this utility, you need to modify your shell configuration file:
                # If your shell is tcsh (the default), add this line to .chsrc.user.
                alias go 'cd  `/pkg/ipums/programming/perl_ipums/go.py \!:*`'
                # If your shell is bash.
                function go {
                    cd `/pkg/ipums/programming/perl_ipums/go.py \$\@`
                }
The script supplies limited support for the special Unix directory conventions.  The following simple items work when supplying a PATH to the script, but more complex specifications  do not:
                .
                ..
                ~""",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    subparser = parser.add_subparsers(
        title="commands",
        dest="command",
        metavar="Keys: \t\t Args: \t\t Function:",
        required=True,
    )

    add = subparser.add_parser("add", help="KEY, PATH \t add (or replace) a path.")
    add.set_defaults(func=add_go, godata_file=file_name, parser=parser)
    add.add_argument("KEY", type=str, help="Key to add")
    add.add_argument("PATH", type=str, help="Path of new key")

    delete = subparser.add_parser("del", help="KEY \t\t Delete a path.")
    delete.set_defaults(func=del_go, godata_file=file_name, parser=parser)
    delete.add_argument("KEY", type=str, help="Key to delete")

    ren = subparser.add_parser("ren", help="OLDKEY, NEWKEY \t Change a key for a path.")
    ren.set_defaults(func=ren_go, godata_file=file_name, parser=parser)
    ren.add_argument("OLDKEY", type=str, help="Key to be renamed")
    ren.add_argument("NEWKEY", type=str, help="New name of renamed key")

    clear = subparser.add_parser("clear", help=" \t\t Clear all paths.")
    clear.set_defaults(func=clear_go, godata_file=file_name, parser=parser)

    ws = subparser.add_parser("ws", help="[PROJECT/]NAME \t Go to an IPUMS workspace.")
    ws.set_defaults(func=ws_go, godata_file=file_name, parser=parser)
    ws.add_argument(
        "WORKSPACE",
        type=str,
        help="IPUMS Project and Workspace inside\nFormat: PROJECT/NAME",
    )

    go = subparser.add_parser(
        "go",
        help="KEY \t\t Go to a path if KEY provided. Show the paths if KEY not provided.",
    )
    go.set_defaults(func=go_go, godata_file=file_name, parser=parser)
    go.add_argument("KEY", nargs="*", action="append", help="Will load path from key.")

    return parser


def main():
    """Main function invokes all other functions"""
    user = getpass.getuser()
    home = os.path.expanduser("~")
    godata = str(home) + "/go_" + str(user) + ".dat"
    match_str = "^\s*(.+)\t(.+)\s+$"
    parser = run_parser(godata)
    args = parser.parse_args()
    direc = make_and_check_go_data(godata, match_str)
    args.func(godata_file=godata, go_paths=direc, parser=parser, args=args)


if __name__ == "__main__":
    main()
