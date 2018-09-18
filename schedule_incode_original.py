#!/usr/bin/python
import os
import socket
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import ec_incode


class IncodeHandler(FileSystemEventHandler):
    def __init__(self):
        self.sleep = 60 * 60
        self.incode_path = r"C:\\dev\\incode"
        self.incode_file = r"TMP_PC2HOST.TMP"
        if socket.gethostname() == "gis-development":
            self.workspace = r"C:\\Users\\sde\\AppData\\Roaming\\ESRI\\" \
                             r"Desktop10.3\\ArcCatalog\\dev.cityofelcampo.org.sde"
        elif socket.gethostname() == "gis":
            self.workspace = r"C:\\Users\\spowell\\AppData\\Roaming\\ESRI\Desktop10.3\\" \
                             r"ArcCatalog\\powellcattle.com.sde"
        else:
            self.workspace = \
                r"C:\\Users\\spowell\\AppData\\Roaming\\ESRI\\" \
                r"Desktop10.3\\ArcCatalog\\black-charolais.com.sde"

        self.init_time = os.path.getmtime(self.getFilePath())
        print("__init__ complete")

    def getFilePath(self):
        return os.path.join(self.incode_path, self.incode_file)

    def on_modified(self, event):
        current_time = os.path.getmtime(self.getFilePath())
        if current_time != self.init_time:
            self.init_time = current_time
            ec_incode.load_incode_readings(self.getFilePath(), self.workspace)


if __name__ == "__main__":
    handler = IncodeHandler()
    observer = Observer()
    observer.schedule(handler, path=handler.incode_path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(handler.sleep)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()