import os
import shutil
from threading import Thread
from multiprocessing import Process
import time

class Copier(Thread):
    """
    The Copier copies all ordered files in one directory tree into another directory, with an optional
    delay after each copy.
    """

    def __init__(self, dirs, delay=0):
        Thread.__init__(self)
        self.dirs = dirs
        self.processes = []
        self.delay = delay

    def copy_dir(self, src, dst):
        fs = []
        for (root, dir, files) in os.walk(src):
            for f in files:
                fs.append(os.path.join(root, f))
        for f in sorted(fs):
            shutil.copy(f, dst)
            time.sleep(self.delay)

    def start(self):
        for src, dst in self.dirs:
            t = Process(target=self.copy_dir, args=(src, dst))
            self.processes.append(t)
            t.start()

    def stop(self):
        for proc in self.processes:
            proc.terminate()
