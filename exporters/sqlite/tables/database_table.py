import abc
from .column import Column
from difflib import Differ
import textwrap


class DatabaseTable(abc.ABC):
    """
    A single table in the sqlite database.
    """

    FILE_SUFFIX = ".xml"

    def __init__(self, name, description, *columns, indexes={}, primary_keys=None):
        """
        Args:
            name (str): The table's name.
            description (str): Short phrase describing what's in the table.
            columns (Column):  Columns in the table.
            indexes (dict):  Indexes to create for the database.  Each key
                             is the name of a index.  For a single index,
                             the value is the variable's name (str).  For a
                             composite key, the value is a sequence of two
                             two or more variable names.
        """
        self.name = name
        self.description = description
        assert len(columns) > 0
        for column in columns:
            assert isinstance(column, Column)
        self.columns = columns
        self.indexes = {}
        for name, columns in indexes.items():
            self._define_index(name, columns)
        self._define_primary_key(primary_keys)

    def _define_primary_key(self, primary_keys):
        if primary_keys is None:
            self.primary_key = None
        elif isinstance(primary_keys, list):
            key_str = ", ".join(primary_keys)
            self.primary_key = f"PRIMARY KEY ({key_str})"
        else:
            raise KeyError(
                f"{primary_keys} of {self.name} needs to be a list: {type(primary_keys)}"
            )

    def _define_index(self, name, columns):
        """
        Args:
            name (str):  The index's name.
            columns (str or seq):  If a single index, the name of column
                                   to index.  If a composite index, the
                                   names of two or more columns.
        """
        if isinstance(columns, str):
            columns = [columns]
        self.indexes[name] = [self[col_name] for col_name in columns]

    def __getitem__(self, col_name):
        for column in self.columns:
            if column.name == col_name:
                return column
        raise KeyError(f'no {col_name} column in database table "{self.name}"')

    def sql_cmd_to_drop(self):
        return f"DROP TABLE IF EXISTS {self.name}"

    def sql_cmd_to_create(self):
        """Return the SQL command to create the table in a database."""
        sql_cmd = f"CREATE TABLE {self.name}("
        last_column = self.columns[-1]
        for column in self.columns:
            sql_cmd += f"{column.name} {column.data_type}"
            if column == last_column:
                if self.primary_key is not None:
                    sql_cmd += f", {self.primary_key})"
                else:
                    sql_cmd += ")"
            else:
                sql_cmd += ", "
        return sql_cmd

    def sql_cmd_to_insert(self, or_replace=False, name_placeholders=False):
        """
        Return the SQL command to insert (or replace) the table.

        Args:
            or_replace (bool):  Include "or REPLACE" in the command.
            name_placeholders (bool):  Use ":name" notation in the command
                                       instead of question marks ("?").
        """
        if or_replace:
            or_replace = " or REPLACE"
        else:
            or_replace = ""
        if name_placeholders:
            placeholders = [f":{col}" for col in self.columns]
        else:
            placeholders = ["?" for col in self.columns]
        sql_cmd = (
            f"INSERT {or_replace} into {self.name}("
            + ", ".join([col.name for col in self.columns])
            + ") VALUES("
            + ", ".join(placeholders)
            + ")"
        )
        return sql_cmd

    def sql_cmd_to_create_index(self, name, columns):
        """
        Args:
            name (str):
            columns (seq of Column):
        """
        col_list = ", ".join([col.name for col in columns])
        sql_cmd = f"CREATE INDEX {name} ON {self.name} ({col_list})"
        return sql_cmd

    def create_in_db(self, db_conn):
        """
        Create the table in a database.

        Args:
            db_conn: The connection to the database.
        """
        with db_conn:
            db_conn.execute(self.sql_cmd_to_create())
            for idx_name, columns in self.indexes.items():
                db_conn.execute(self.sql_cmd_to_create_index(idx_name, columns))

    def drop_from_db(self, db_conn):
        """
        Drop table from a database.
        Args:
            db_conn: The connection to the database.
        """
        with db_conn:
            db_conn.execute(self.sql_cmd_to_drop())

    @staticmethod
    def exists(table_name, db_conn):
        """
        Check if the given table exists in the connected database.

        Args:
            db_conn: The connection to the database.
            table_name: The name of the table to check for in the database.
        """
        table_exists = (
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        if db_conn.execute(table_exists).fetchone():
            return True
        else:
            return False

    def check_schema_mismatch(self, db_conn):
        """Check if current schema (from db connection) matches new schema
        (this table object definition).
        This is important for knowing when a table needs to be fully recreated
        as SQLite doesn't have many schema migration capabilities.

        Args:
            db_conn (sqlite3.Connection): Database connection

        Raises:
            SchemaError: current and new schemas do not match.
        """
        d = Differ()
        cur_schema = db_conn.query(
            f"SELECT sql FROM sqlite_schema WHERE name = '{self.name}'"
        )[0]["sql"]
        cur_schema = textwrap.fill(cur_schema, 55)
        print(cur_schema)
        new_schema = textwrap.fill(self.sql_cmd_to_create(), 55)
        diff = "\n".join(d.compare(cur_schema.split("\n"), new_schema.split("\n")))
        if cur_schema.casefold() != new_schema.casefold():
            raise SchemaError(f"Schema statements do not match \n{diff}")

    def invalid_values(self, validation_key, validation_table, db_conn):
        """Return invalid values based on a given column, which represents a foreign key to the given validation table.

        Args:
            validation_key (str): name of column to use as validation key
            validation_table (str): An IPUMS Metadata Database table name
            db_conn (sqlite3.Connection): Database connection

        Returns:
            Set[str]: Values from the validation key column not found in validation table
        """
        query = (
            f"SELECT {validation_key} FROM {self.name} "
            f"WHERE {validation_key} NOT IN "
            f"(SELECT {validation_key} FROM {validation_table})"
        )
        with db_conn:
            invalid_rows = db_conn.execute(query)
        unique_invalid_values = set([row[0] for row in invalid_rows.fetchall()])
        return unique_invalid_values


class SchemaError(BaseException):
    def __init__(self, message):
        super().__init__(message)
