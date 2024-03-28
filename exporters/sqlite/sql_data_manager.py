from collections import defaultdict


class SqlDataManager(object):
    """Class to manage the juggling of SQL statements in order to cache them."""

    def __init__(self):
        self.cached_table_rowdata = defaultdict(lambda: [])
        self.cached_table_delete_statements = defaultdict(lambda: [])
