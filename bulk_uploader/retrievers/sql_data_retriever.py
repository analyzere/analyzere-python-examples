import pyodbc
import pandas as pd

class SQLDataRetriever:
    """
    Retrieves layer definitions and loss data from SQL queries and loads
    them into Pandas DataFrames for downstream processing.
    """
    @classmethod
    def add_parser_arguments(cls, parser):
        pass

    def ensure_connection(self):
        if self.connection is None:
            self.connection = pyodbc.connect(
                driver=self.config.sql.driver,
                server=self.config.sql.server,
                database=self.config.sql.database,
                uid=self.config.sql.username,
                pwd=self.config.sql.password,
                encrypt="no"
            )
        return self.connection

    def get_layers(self):
        connection = self.ensure_connection()
        layer_query = self.config.sql.layers_query
        return pd.read_sql_query(layer_query, con=connection)

    def get_losses(self):
        connection = self.ensure_connection()
        losses_query = self.config.sql.losses_query
        return pd.read_sql_query(losses_query, con=connection)

    @property
    def loss_type(self):
        return self.config.sql.loss_type

    def __init__(self, args, config):
        self.config = config
        self.connection = None
