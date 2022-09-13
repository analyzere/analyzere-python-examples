import configparser
import logging
from collections import namedtuple
from pathlib import Path

LOG = logging.getLogger()

class ConfigFile:
    """
    This class parses the sections of config file and returns a namedtuple object that 
    represents the section and its members. 
    """
    def get_server_details(self):
        ServerDetails = namedtuple('ServerDetails',
                              list(dict(self.config.items('server')).keys()))
        server_details = ServerDetails._make(list(dict(self.config.items('server')).values()))

        return server_details

    def get_defaults(self):
        Defaults = namedtuple('Defaults',
                              list(dict(self.config.items('defaults')).keys()))
        defaults = Defaults._make(list(dict(self.config.items('defaults')).values()))

        return defaults

    def get_layer_term_columns(self):
        BulkLayerTermColumns = namedtuple('BulkLayerTermColumns',
                                          list(dict(self.config.items('bulk_layer_terms')).keys()))
        bulk_layer_term_columns = BulkLayerTermColumns._make(
                                  list(dict(self.config.items('bulk_layer_terms')).values()))

        return bulk_layer_term_columns

    def get_loss_set_columns(self):
        BulkLossSetColumns = namedtuple('BulkLossSetColumns',
                                        list(dict(self.config.items('bulk_loss_set')).keys()))
        bulk_loss_set_columns = BulkLossSetColumns._make(
                                list(dict(self.config.items('bulk_loss_set')).values()))

        return bulk_loss_set_columns

    def get_ARe_loss_set_columns(self):
        AReLossSetColumns = namedtuple('AReLossSetColumns',
                                        list(dict(self.config.items('ARe_loss_columns')).keys()))
        are_loss_set_columns = AReLossSetColumns._make(
                               list(dict(self.config.items('ARe_loss_columns')).values()))

        return are_loss_set_columns

    def get_sql_config(self):
        sql_config = namedtuple('SQLConfig',
                                list(dict(self.config.items('SQL')).keys()))
        sql_config_values = sql_config._make(
                               list(dict(self.config.items('SQL')).values()))

        return sql_config_values

    def __init__(self, filename):
        if not Path(filename).is_file():
            LOG.error(f"Configuration file {filename} does not exist or is not a file.")
            raise ValueError(f"Configuration file not found.")
        self.config = configparser.ConfigParser()
        self.config.read(filename)
        self.sections = self.config.sections()
