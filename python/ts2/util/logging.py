"""
Helper methods for consistent logging
"""
import logging
import sys

import ts2.settings as settings

class Logger(object):

    _singleton = None

    @staticmethod
    def getInstance():
        if not Logger._singleton:
            logging.basicConfig(filename=settings.LOG_FILE, level=logging.DEBUG)
            Logger._singleton = logging.getInstance(__name__)
            Logger.configure(Logger._singleton)
        return Logger._singleton

    @staticmethod
    def configure(logger):
        """
        Set the log message format, threshold, etc here...
        """
        pass

def logDebug(self, msg):
    log = Logger.getInstance()
    log.debug(msg)

def logInfo(self, msg):
    log = Logger.getInstance()
    log.info(msg)

def logWarning(self, msg):
    log = Logger.getInstance()
    log.warning(msg)

def logError(self, msg):
    log = Logger.getInstance()
    log.error(msg)
