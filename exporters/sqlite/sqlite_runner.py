from .sqlite_metadata_dumper import SqliteMetadataDumper
import sys


class SqliteRunner(object):
    """This object is a wrapper and used by the top-level sqlite_metadata_dumper.py"""

    def __init__(self, helper):
        self.helper = helper
        self.dumper = SqliteMetadataDumper(
            helper.product,
            db_file=helper.db_file,
            verbose=helper.verbose,
            wipe=helper.force,
        )

    def __print_now(self, msg):
        """__print_now prints to stdout immediately, used for progress."""
        sys.stdout.write(msg)
        sys.stdout.flush()

    def trans_tables_table(self):
        self.__print_now(f"Making fresh database of {str(self.helper.project)}\n")
        self.dumper.create_fresh_database()

    def var_table(self):
        self.dumper.create_variables_table()

    def cf_tables(self):
        self.__print_now("Creating variables table.\n")
        self.var_table()
        self.__print_now("Creating all control file tables.\n")
        self.dumper.create_all_cf_tables()

    def cf_table(self):
        self.__print_now(
            f"Creating {str(self.helper.control_file)} table (control file data)\n"
        )
        if self.helper.control_file == "variables":
            self.var_table()
        else:
            self.dumper.create_cf_table(self.helper.control_file)

    def run(self):
        self.__print_now("Initializing connection to database...")
        print(self.helper.db_file, self.helper.project)
        if self.helper.force:
            self.trans_tables_table()
            self.cf_tables()
        elif self.helper.all:
            self.cf_tables()
        elif self.helper.cf:
            self.cf_table()
        self.__print_now("DONE!\n")
