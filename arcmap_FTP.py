#!/usr/bin/python
import ftplib
import arcpy



site = arcpy.GetParameterAsText(0)
user = arcpy.GetParameterAsText(1)
password = arcpy.GetParameterAsText(2)
directory = arcpy.GetParameterAsText(3)
file_name = arcpy.GetParameterAsText(4)
arcpy.AddMessage("Site name is " + site)





# site = r"ftp.bisenterprises.com"
# user = r"whartongis"
# password = r"Wharton-1"
# directory = r"/Wharton GIS"
# file_name = r"WhartonCADData.zip"
#
arcpy.AddMessage("Accessing FTP site for %s download" % file_name)
ftp = ftplib.FTP(site)
#ftp.login(user, password)

#ftp.cwd(directory)
#file = open(file_name, "wb")
#arcpy.AddMessage("Downloading %s from FTP site" % file_name)
#ftp.retrbinary("RETR %s" % file_name, (file.write))
#file.close()
#ftp.quit()


#ec_util.get_ftp_file(site, user, password, directory, file_name)
# current_path = os.path.dirname(os.path.abspath(__file__))
# ec_util.unzip_CAD(current_path, file_name)
