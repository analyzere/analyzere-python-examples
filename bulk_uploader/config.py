import configparser
import logging
from collections import namedtuple
from pathlib import Path

LOG = logging.getLogger()


class ConfigFile:
    """
    This class provides properties-style access to the sections and
    configuration values in the configuration file.
    """

    def __getattribute__(self, attr):
        """
        Overloading the attribute accessor to intercept configuration
        section names and return their values as a namedtuple.
        """
        # Avoid infinite recursion by skipping over private members
        if not attr.startswith("_") and attr in self._config:
            # Check if the given config section has already been
            # instantiated and cached. If so, return the cached value.
            if attr in self._cache:
                return self._cache[attr]
            else:
                # Otherwise, create a namedtuple of the configuration value
                # keys and instantiate the tuple with the values.
                T = namedtuple(attr, [k for k, _ in self._config.items(attr)])
                t = T._make([v for _, v in self._config.items(attr)])
                self._cache[attr] = t
                return t
        else:
            # If a private member or an attribute that does not refer to a
            # configuration section, then call the default implementation.
            return super().__getattribute__(attr)

    def __init__(self, filename):
        if not Path(filename).is_file():
            LOG.error(
                f"Configuration file {filename} does not exist or is not a file."
            )
            raise ValueError(f"Configuration file not found.")
        self._config = configparser.ConfigParser()
        self._config.read(filename)
        self._cache = {}
