from threading import Thread
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler, FileCreatedEvent, FileModifiedEvent
from Queue import Queue
import json
import time
from abc import abstractmethod

from ts2.util.logging import logDebug, logError, logInfo, logWarning
import ts2.settings as settings


class FinishedFileNotifier(RegexMatchingEventHandler, Thread):
    """
    Handles events from the watchdog Observer. When a file hasn't been modified for a given duration, the
    FinishedFileNotifier will send all subscribers an update of the form (root, path, idx), where root
    is the root directory, path is the full path to the completed file, and idx is the file's time index.
    """

    def __init__(self, root, subscribers, regexes, name_parser):
        RegexMatchingEventHandler.__init__(self, regexes)
        Thread.__init__(self)
        self.mod_time = settings.MOD_TIME
        self.root = root
        self.subscribers = subscribers
        self.name_parser = name_parser

        # The event dictionary stores last modification times for all files under root (recursively)
        # A dict is better than an implementation that involves sorting because the sorting would have to take place
        # for every on_created or on_modified call. A single linear scan every mod_time is faster.
        self._event_dict = {}

        # File completion notifications are inserted into this queue, which is read by the DB manager thread
        self._queue = Queue()

        self._observer = Observer()
        self._observer.schedule(self, self.root, recursive=True)

        self._stopped = False

    def stop(self):
        self._stopped = True
        self._observer.stop()

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent):
            self._event_dict[event.src_path] = time.time()

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent):
            self._event_dict[event.src_path] = time.time()

    def _generate_notifications(self):
        cur_time = time.time()
        for (file, t) in self._event_dict.items():
            if (cur_time - t) > self.mod_time:
                notification = (self.root, file, ETLConfiguration.get_time_index(file, self.name_parser))
                self._queue.put(notification)
                del self._event[file]

    def get_notifications(self):
        notifications = []
        while not self._queue.empty():
            notifications.append(self._queue.get())
        return notifications

    def run(self):
        self._observer.start()
        while not self._stopped:
            self._generate_notifications()


class ETLConfiguration(object):
    """
    An ETLConfiguration dictates how files being written into a set of directories should be loaded and inserted
    into a synchronization system (currently an HBase table). In order to do this, the following parameters are
    necessary:
        1) The names of the directories to monitor
        2) The regexes that match those files to be ingested
        3) How a file name should be translated into a time key for synchronization
    """

    GROUP_NAME = "name"

    def __init__(self, config={}):
        self.config = config

    def add_dir(self, dir, regexes, name_parser):
        """
        :param dir: The directory to monitor for new files
        :param regexes: The regexes with which to check for matching files
        :param name_parsers: A regex containing a match group named 'name', which after a file match will contain the
            time index.
        """
        self.config[dir] = (regexes, name_parser)

    def get_configs(self):
        for dir in self.config:
            (regexes, name_parser) = self.config[dir]
            yield (dir, regexes, name_parser)

    @staticmethod
    def get_time_index(fname, name_parser):
        match = name_parser.search(fname)
        if not match:
            logWarning("Cannot parse time index out of filename: %s" % fname)
            return
        return match.group(ETLConfiguration.GROUP_NAME)

    @staticmethod
    def load_from_json(json_file):
        with open(json_file, 'r') as f:
            conf = json.load(f)
            dir_configs = conf['dirs']
            etl_configuration = ETLConfiguration()
            for dir_obj in dir_configs:
                dir = dir_obj['dir']
                name = dir['name']
                regexes = dir['regexes']
                parsers = dir['parsers']
                etl_configuration.add_dir(name, regexes, parsers)
            return etl_configuration
        return None


class FileLoadManager(Thread):
    """
    The FileLoadManager monitors a list of directories containing time-indexed files (files that when sorted
    lexicographically will be time-ordered) for new files, and hands those new files off to an Synchronizer
    which synchronizes the file contents according to the files' time indices.
    """

    def __init__(self, etl_conf, synchronizer):
        """
        Constructs a FinishedFileNotifier for each directory in dirs_and_regexes

        :param dirs_and_regexes: A dictionary of (dir, [regex1, regex2...]) pairs
        """
        self.synchronizer = synchronizer

        def make_notifier(dir, regexes, name_parser):
            return FinishedFileNotifier(dir, [self], regexes, name_parser)
        self._notifiers = [make_notifier(dir, regexes, name_parser) for (dir, regexes, name_parser) in etl_conf.get_configs()]


        self._stopped = False

    def start(self):
        """
        Starts the FinishedFileNotifiers, then begin checking each of their notification queues for file
        completion notifications.
        """
        self.synchronizer.initialize()
        while not self._stopped:
            for nid, notifier in enumerate(self._notifiers):
                ns = notifier.get_notifications()
                # Buffer the files in memory
                loaded = map(lambda (root, path, idx): (root, idx, open(path, 'rb').read()), ns)
                self.synchronizer.synchronize(nid, loaded)

    def stop(self):
        """
        Stops the FinishedFileNotifiers
        """
        for notifier in self._notifiers:
            notifier.stop()
        self.synchronizer.terminate()


class Synchronizer(object):
    """
    Takes multiple lists of unordered byte arrays and sequences them in time. Can issue notifications when the
    same time index has been encountered in each byte array list.

    Abstract base class
    """

    @abstractmethod
    def synchronize(self, sequence_id, data_list):
        """

        :param sequence_id:
        :param data_list:
        :return:
        """
        pass
