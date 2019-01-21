from datetime import datetime
from ftplib import FTP
import zipfile
import os
import logging
import inspect
import openlocationcode



code = openlocationcode.encode(29,-96)
print(code)