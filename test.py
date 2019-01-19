from datetime import datetime
from ftplib import FTP
import zipfile
import os
import logging
import inspect

import arcpy

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="parcel_updates.log",
                    filemode="w",
                    level=logging.ERROR,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

arcpy.env.workspace = r"C:\Users\spowell\AppData\Roaming\ESRI\Desktop10.6\ArcCatalog\localhost.sde"

datasets = arcpy.ListDatasets("Boundary", r"Feature")
assert(datasets)


feature_classes = arcpy.ListFeatureClasses("county_parcel", "All", "ec.sde.Boundary")
assert(feature_classes)