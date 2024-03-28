class Column(object):
    """
    A column in a database table.
    """

    def __init__(self, name, data_type):
        """
        Args:
            name (str): The column's name (e.g., "xml" or "date_created").
            data_type (str):  The column's data type; examples:
                                  "VARCHAR(255) UNIQUE"
                                  "BLOB"
                                  "str"
        """
        self.name = name
        self.data_type = data_type

    def __str__(self):
        return self.name
