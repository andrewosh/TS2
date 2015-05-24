import os
import shutil
from threading import Thread
from multiprocessing import Process

class Copier(Thread):
    """
    The Copier copies all ordered files in one directory tree into another directory, with an optional
    delay after each copy.
    """

    def __init__(self, dirs):
        Thread.__init__(self)
        self.dirs = dirs

    def copy_dir(self, src, dst):
        fs = []
        for (root, dir, file) in os.walk(src):
            fs.append(os.path.join(root, file))
        for f in sorted(fs):
            shutil.copy(f, dst)

    def start(self):
        self.threads = []
        for src, dst in self.dirs:
            t = Process(target=self.copy_dir, args=(src, dst))
            self.threads.append(t)
            t.start()

    def stop(self):
        for thread in self.threads:
            thread.stop()
