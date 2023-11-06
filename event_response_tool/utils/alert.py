import sys
import logging

logger = logging.getLogger()

RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[0;33m"
BLACK = "\033[1;30m"


class Alert:
    @staticmethod
    def exception(msg):
        logger.exception(f"{msg}")
        sys.tracebacklimit = 0
        code = "ERROR: "
        print(f"{RED}{code}{str(msg)}")
        sys.exit()

    @staticmethod
    def error(msg):
        logger.error(f"{msg}")
        sys.tracebacklimit = 0
        code = "ERROR: "
        print(f"{RED}{code}{str(msg)}")
        sys.exit()

    @staticmethod
    def warning(msg):
        code = "WARNING: "
        print(f"{YELLOW}{code}{msg}")
        logger.warning(f"{msg}")

    @staticmethod
    def info(msg, success=False):
        print(f"{GREEN}{msg}") if success else print(f"{msg}")
        logger.info(f"{msg}")

    @staticmethod
    def debug(msg):
        logger.debug(f"{msg}")
