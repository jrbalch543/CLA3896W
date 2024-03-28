from datetime import datetime
from pathlib import Path
import sqlite3
import os
import apsw
from ipums.metadata.exporters.sqlite.tables.project_table import ProjectTable


class SqliteConnectionManager(object):
    """Class to manage connection with a sqlite database.
    Args:
        db_path(str): path to a sqlite3 database file.
    """

    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.apsw_con = None

    def connect_via_sqlite(self):
        sqlite_con = sqlite3.connect(str(self.db_path))
        # make sure file has group write privileges
        try:
            os.chmod(str(self.db_path), 0o664)
        except PermissionError:
            # Ticket 14799
            # only owner can chmod, so just skip it if PermissionError thrown
            pass
        return sqlite_con

    def connect_via_apsw(self):
        if self.apsw_con is None:
            self.apsw_con = apsw.Connection(str(self.db_path))
        # make sure file has group write privileges
        try:
            os.chmod(str(self.db_path), 0o664)
        except PermissionError:
            # Ticket 14799
            # only owner can chmod, so just skip it if PermissionError thrown
            pass

    def _dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def query(self, q):
        """Execute a query to the metadata db and return a list of dicts."""
        with self.connect_via_sqlite() as con:
            cur = con.cursor()
            cur.row_factory = self._dict_factory
            cur.execute(q)
            return cur.fetchall()

    def check_project_match(self, proj_name):
        """Verify the DB matches the project name argument."""
        if not self.db_path.exists():
            return True
        project_table = ProjectTable()
        with self.connect_via_sqlite() as con:
            if project_table.exists(project_table.name, con):
                ret = self.query("SELECT project from project")
                db_proj = ret[0]["project"]
                if db_proj.upper() != proj_name.upper():
                    raise KeyError(
                        "DB project does not match command argument ("
                        + "DB: "
                        + db_proj
                        + ", arg: "
                        + proj_name
                        + ")"
                    )
            else:
                project_table.create_in_db(con)
                con.executemany(
                    project_table.sql_cmd_to_insert(),
                    [(proj_name.upper(), datetime.now())],
                )
        return True

    def _execute_transactions(self, sql_list):
        self.connect_via_apsw()
        con = self.apsw_con.cursor()
        con.execute("BEGIN")
        for sql in sql_list:
            try:
                con.execute(sql)
            except:
                print("Failed to execute this SQL:", sql)
                raise
        con.execute("COMMIT")
        con.close()
        self.apsw_con = None

    def _executemany_transaction(self, insert_string, info):
        self.connect_via_apsw()
        con = self.apsw_con.cursor()
        con.execute("BEGIN")
        con.executemany(insert_string, info)
        con.execute("COMMIT")
        con.close()
        self.apsw_con = None
