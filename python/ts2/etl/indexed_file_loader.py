from threading import Thread
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler, FileCreatedEvent, FileModifiedEvent
from Queue import Queue
import json
import time
from abc import abstractmethod
import re

from ts2.util.log import debugLog, errorLog, infoLog, warningLog
import ts2.settings as settings


class FinishedFileNotifier(Thread):
    """
    Launches an watchdog Observer which looks for file creation/modifications in a given set of directories and
    then passes notifications corresponding to those events to a subscriber.
    """

    class FileModHandler(RegexMatchingEventHandler):
        """
        Handles events from the watchdog Observer. When a file hasn't been modified for a given duration, the
        FileModHandler will insert an event into its queue corresponding to that file.
        """

        def __init__(self, regexes, event_dict):
            RegexMatchingEventHandler.__init__(self, regexes, ignore_directories=True)
            self._event_dict = event_dict

        def _process(self, event):
            t = time.time()
            debugLog("Inserting %s with time %s into event_dict" % (event.src_path, str(t)))
            self._event_dict[event.src_path] = t

        def on_created(self, event):
            self._process(event)

        def on_modified(self, event):
            self._process(event)


    def __init__(self, root, id, subscribers, regexes, name_parser):
        Thread.__init__(self)
        self.mod_time = settings.MOD_TIME
        self.root = root
        self.id = id
        self.subscribers = subscribers
        # For now, only support one regex
        self.name_parser = name_parser[0]
        self.regexes = ['.*']

        # The event dictionary stores last modification times for all files under root (recursively)
        # A dict is better than an implementation that involves sorting because the sorting would have to take place
        # for every on_created or on_modified call. A single linear scan every mod_time is faster.
        self._event_dict = {}

        # File completion notifications are inserted into this queue, which is read by the DB manager thread
        self._queue = Queue()

        self._observer = None

        self._stopped = False

    def stop(self):
        self._stopped = True
        self._observer.stop()


    def _generate_notifications(self):
        cur_time = time.time()
        for (file, t) in self._event_dict.items():
            debugLog("Checking file %s with time %s" % (file, str(t)))
            if (cur_time - t) > self.mod_time:
                notification = (self.root, self.id, file, ETLConfiguration.get_time_index(file, self.name_parser))
                debugLog("Inserting %s into notification queue" % str(notification))
                self._queue.put(notification)
                del self._event_dict[file]

    def get_notifications(self):
        notifications = []
        while not self._queue.empty():
            notifications.append(self._queue.get())
        debugLog("In get_notifications, notifications: %s" % str(notifications))
        return notifications

    def run(self):
        self._observer = Observer()
        self._observer.schedule(FinishedFileNotifier.FileModHandler(self.regexes, self._event_dict),
                                self.root, recursive=True)
        debugLog("Starting file observer on: %s with regexes %s" % (self.root, self.regexes))
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

    def add_dir(self, dir, id, regexes, name_parser):
        """
        :param dir: The directory to monitor for new files
        :param regexes: The regexes with which to check for matching files
        :param name_parsers: A regex containing a match group named 'name', which after a file match will contain the
            time index.
        """
        self.config[dir] = (id, regexes, name_parser)

    def get_configs(self):
        for dir in self.config:
            (id, regexes, name_parser) = self.config[dir]
            yield (dir, id, regexes, name_parser)

    @staticmethod
    def get_time_index(fname, name_parser):
        debugLog("Getting time index from %s with parser %s" % (fname, name_parser))
        match = re.search(name_parser, fname)
        if not match:
            warningLog("Cannot parse time index out of filename: %s" % fname)
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
                id = dir['id']
                regexes = dir['regexes']
                parsers = dir['parsers']
                etl_configuration.add_dir(name, id, regexes, parsers)
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
        Thread.__init__(self)

        self.synchronizer = synchronizer

        def make_notifier(dir, id, regexes, name_parser):
            return FinishedFileNotifier(dir, id, [self], regexes, name_parser)
        self._notifiers = []
        for (dir, id, regexes, name_parser) in etl_conf.get_configs():
            self._notifiers.append(make_notifier(dir, id, regexes, name_parser))

        self._stopped = False

        # Keep track of the minimum filename-based time-index that we've encountered
        self.min_idx = 0

    def _notification_to_record(self, dataset_id, idx, path):
        data = open(path, 'rb').read()
        int_idx = int(idx)
        if int_idx < self.min_idx:
            self.min_idx = int_idx
        normalized_idx = int_idx - self.min_idx
        return (dataset_id, str(normalized_idx), data)

    def run(self):
        """
        Starts the FinishedFileNotifiers, then begin checking each of their notification queues for file
        completion notifications.
        """
        self.synchronizer.initialize()
        for notifier in self._notifiers:
            notifier.start()
        while not self._stopped:
            for nid, notifier in enumerate(self._notifiers):
                ns = notifier.get_notifications()
                # Buffer the files in memory, discard the root and use the ID from now on
                record = map(lambda (root, id, path, idx): self._notification_to_record(id, idx, path), ns)
                self.synchronizer.synchronize(nid, record)

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
    def set_sequence_names(self, names):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    @abstractmethod
    def synchronize(self, sequence_id, data_list):
        """

        :param sequence_id:
        :param data_list:
        :return:
        """
        pass
