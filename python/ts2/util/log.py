"""
Helper methods for consistent logging
"""
import logging
import sys

import ts2.settings as settings

from threading import Lock

class Logger(object):

    _singleton = None
    log_lock = Lock()

    @staticmethod
    def getInstance():
        if not Logger._singleton:
            logging.basicConfig(filename=settings.LOG_FILE, level=logging.DEBUG)
            Logger._singleton = logging.getLogger(__name__)
            Logger.configure(Logger._singleton)
        return Logger._singleton

    @staticmethod
    def configure(logger):
        """
        Set the log message format, threshold, etc here...
        """
        pass

def lock_method(log_func):
    def log(msg):
        Logger.log_lock.acquire()
        try:
            log_func(msg)
        except Exception:
            pass
        Logger.log_lock.release()
    return log

@lock_method
def debugLog(msg):
    log = Logger.getInstance()
    log.debug(msg)

@lock_method
def infoLog(msg):
    log = Logger.getInstance()
    log.info(msg)

@lock_method
def warningLog(msg):
    log = Logger.getInstance()
    log.warning(msg)

@lock_method
def errorLog(msg):
    log = Logger.getInstance()
    log.error(msg)
