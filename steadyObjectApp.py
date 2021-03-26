#!/usr/bin/python3
#    Air stagnation indices over the Euro-Mediterranean region
#    Copyright (C) 2020  Carlos Ordóñez García
#    Copyright (C) 2020  José Manuel Garrido Pérez
#    Copyright (C) 2020  Javier Ruano Ruano
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
# import Download_Dataset
from ASI_Download import ASI_Download
import numpy as np
from ASI_Datase_RACKt import ASI
import logging
import os
import os
import glob

def print_license():
    print("     Copyright (C) 2020  Carlos Ordóñez García\n \
    Copyright (C) 2020  José Manuel Garrido Pérez\n \
    Copyright (C) 2020  Javier Ruano Ruano\n \
    Air stagnation indices over the Euro-Mediterranean region\n\n \
    This program is free software: you can redistribute it and/or modify\n \
    it under the terms of the GNU General Public License as published by\n \
    the Free Software Foundation, either version 3 of the License, or\n \
    (at your option) any later version.\n\n \
    This program is distributed in the hope that it will be useful,\n \
    but WITHOUT ANY WARRANTY; without even the implied warranty of\n \
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n \
    GNU General Public License for more details.\n\n \
    You should have received a copy of the GNU General Public License\n \
    along with this program.  If not, see <https://www.gnu.org/licenses/>.\n")

def limpia_temporales():
    # se podría mirar desde config y usar glob con append o algo.
    try:

        fileList = glob.glob('/DataLake/FicherosActualizacion/*nc')
        # Iterate over the list of filepaths & remove each file.
        for filePath in fileList:
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)
    except:
        pass
    try:
        fileList = glob.glob('/DataLake/temporales/*nc')
        # Iterate over the list of filepaths & remove each file.
        for filePath in fileList:
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)

    except:
        pass
    try:
        fileList = glob.glob('/DataLake/FicherosActualizacion/months_ml/*nc')
        # Iterate over the list of filepaths & remove each file.
        for filePath in fileList:
            try:
                os.remove(filePath)
            except:
                print("Error while deleting file : ", filePath)
    except:
        pass

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--init", type=str, choices=['all', 'horton', 'huang', 'wang'], help="create netcdf for ASI")
parser.add_argument("-u", "--update", type=str, choices=['all', 'horton', 'huang', 'wang'],
                    help="update netcdf for ASI")
parser.add_argument("-m", "--maps", type=str, choices=['all', 'horton', 'huang', 'wang'], help="create Maps for ASI")
parser.add_argument("-d", "--daemon", type=str, choices=['all'], help="used by Cron to update")
args = parser.parse_args()
answer = []

print_license()

if args.init is None and args.update is None and args.maps is None and args.daemon is None:
    parser.error("Not options")
if args.init and args.update:
    parser.error("Only one options is allowed")
if (args.init):
    new_dataset = 'create'
elif (args.update):
    new_dataset = 'update'
elif (args.maps):
    if (args.maps == 'all' or args.maps == 'horton'):
        data = ASI('horton', 'django')
        data.generate_Maps()
    if (args.maps == 'all' or args.maps == 'wang'):
        data = ASI('wang', 'django')
        data.generate_Maps()
    if (args.maps == 'all' or args.maps == 'huang'):
        data = ASI('huang', 'django')
        data.generate_Maps()
    new_dataset = 'maps'

if args.init:

    # opción suprimida, requiere mucha descarga de información
    # answer = Download_Dataset.create('HortonWang')

    if (args.init == 'all' or args.init == 'horton'):
        data = ASI('horton', 'django')
        data.Read_netcdf(False, True)
        data.Horton_generation()

    if (args.init == 'all' or args.init == 'wang'):
        data = ASI('wang', 'django')
        data.Read_netcdf(False, True)
        data.Wang_generation()
    if (args.init == 'all' or args.init == 'huang'):
        data = ASI('huang', 'django')
        data.Read_netcdf(False, True)
        data.Huang_generation()
    command = args.init

if args.update:

    logging.basicConfig(filename='steadyUpdate.log', filemode='w',
                        format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    if (args.update == 'all' or args.update == 'horton'):
        DatasetDownload = ASI_Download('horton', 'update')
        answer = DatasetDownload.execute()

        if (np.sum("FALSE" == answer) == 0):
            print("actualizando")
            if (np.sum('ERROR' == answer) == 0):
                try:
                    data = ASI('horton', 'update')
                    data.Read_netcdf(True, True)
                    os.system("/usr/sbin/apachectl -k stop")
                    
                    data.Horton_generation(True)
                    
                    print("### procesado horton ###")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream2/stream/data/")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream/stream/data/")
                    os.system("/usr/sbin/apachectl -k start")
                except:
                    os.system("/usr/sbin/apachectl -k start")
                    print("### error procesando horton ###")
                    logging.error('error procesando horton')
                    os.system("/usr/sbin/apachectl -k start")
            else:
                print("### error descargando datos horton ###")
                logging.error('error descargando datos horton')
                os.system("/usr/sbin/apachectl -k start")
        else:
            logging.info('horton actualizado')
    

    if (args.update == 'all' or args.update == 'wang'):

        DatasetDownload = ASI_Download('wang', 'update')
        answer = DatasetDownload.execute()

        if (np.sum('FALSE' == answer) == 0):
            print("actualizando")
            if (np.sum('ERROR' == answer) == 0):
                try:
                    data = ASI('wang', 'update')
                    data.Read_netcdf(True, True)
                    print("parando servidor")
                    os.system("systemctl stop apache2")
                    print("actualizar")
                    data.Wang_generation(True)
                    print("FIN")
                    
                    print("### procesado wang ###")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream2/stream/data/")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream/stream/data/")
                    os.system("/usr/sbin/apachectl -k start")
                except:
                    print("### error procesando wang ###")
                    logging.error('error procesando wang')
                    os.system("/usr/sbin/apachectl -k start")
            else:
                print("### error descargando datos wang ###")
                logging.error('error descargando datos wang')
                os.system("/usr/sbin/apachectl -k start")
        else:
            logging.info('wang actualizado')
        

    if (args.update == 'all' or args.update == 'huang'):

        DatasetDownload = ASI_Download('huang', 'update')
        answer = DatasetDownload.execute()

        if (np.sum('FALSE' == answer) == 0):
            if (np.sum('ERROR' == answer) == 0):
                try:
                    data = ASI('huang', 'update')
                    data.Read_netcdf(True, True)
                    os.system("/usr/sbin/apachectl -k stop")
                    data.Huang_generation(True)
    
                    print("### procesado huang ###")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream2/stream/data/")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream/stream/data/")
                    os.system("/usr/sbin/apachectl -k start")
                except:
                    print("### error procesando huan ###")
                    logging.error('error procesando huang')
                    os.system("/usr/sbin/apachectl -k start")
            else:
                print("### error descargando datos huang ###")
                logging.error('error descargando datos huang')
        else:
            logging.info('huang actualizado')
    

if args.maps:
    command = args.maps

if args.daemon:
    #os.system("/usr/bin/ps aux | /usr/bin/grep python3 | /usr/bin/awk '{print $2}' | /usr/bin/xargs /usr/bin/kill -9")
    import os
    try:
        limpia_temporales()

        # Get a list of all the file paths that ends with .txt from in specified directory

        logging.basicConfig(filename='steadyObjectApp.py.log', filemode='w',
                            format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        DatasetDownload = ASI_Download('horton', 'update')
        answer = DatasetDownload.execute()
        if (np.sum('FALSE' == answer) == 0):
            if (np.sum('ERROR' == answer) == 0):
                try:
                    data = ASI('horton', 'update')
                    data.Read_netcdf(True, True)
                    os.system("/usr/sbin/apachectl -k stop")
                    fileList = glob.glob('/var/www/stream/stream/static/data/images/*png')
                    # Iterate over the list of filepaths & remove each file.
                    for filePath in fileList:
                        try:
                            os.remove(filePath)
                        except:
                            print("Error while deleting file : ", filePath)
                    data.Horton_generation(True)
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream2/stream/data/")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream/stream/data/")

                    os.system("/usr/sbin/apachectl -k start")
                    print("### procesado horton ###")
                except:
                    print("### error procesando horton ###")
                    logging.error('error procesando horton')
                    os.system("/usr/sbin/apachectl -k start")
            else:
                print("### error descargando datos horton ###")
                logging.error('error descargando datos horton')
                os.system("/usr/sbin/apachectl -k start")
        else:
            logging.info('horton actualizado')
        #os.system("/usr/bin/ps aux | /usr/bin/grep python3 | /usr/bin/awk '{print $2}' | /usr/bin/xargs /usr/bin/kill -9")
        limpia_temporales()
        DatasetDownload = ASI_Download('wang', 'update')
        answer = DatasetDownload.execute()

        if (np.sum('FALSE' == answer) == 0):
            if (np.sum('ERROR' == answer) == 0):
                try:
                    data = ASI('wang', 'update')
                    data.Read_netcdf(True, True)
                    os.system("/usr/sbin/apachectl -k stop")
                    data.Wang_generation(True)
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream2/stream/data/")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream/stream/data/")

                    os.system("/usr/sbin/apachectl -k start")
                    print("### procesado wang ###")
                except:
                    print("### error procesando wang ###")
                    logging.error('error procesando wang')

                    os.system("/usr/sbin/apachectl -k start")
            else:
                print("### error descargando datos wang ###")
                logging.error('error descargando datos wang')
                os.system("/usr/sbin/apachectl -k start")
        else:
            logging.info('wang actualizado')
        #os.system("/usr/bin/ps aux | /usr/bin/grep python3 | /usr/bin/awk '{print $2}' | /usr/bin/xargs /usr/bin/kill -9")
        limpia_temporales()
        DatasetDownload = ASI_Download('huang', 'update')
        answer = DatasetDownload.execute()
        if (np.sum('FALSE' == answer) == 0):
            if (np.sum('ERROR' == answer) == 0):
                try:
                    data = ASI('huang', 'update')
                    data.Read_netcdf(True, True)
                    os.system("/usr/sbin/apachectl -k stop")
                    data.Huang_generation(True)

                    print("### procesado huang ###")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream2/stream/data/")
                    os.system("/usr/bin/cp /DataLake/temporales/*nc /var/www/stream/stream/data/")
                    os.system("/usr/sbin/apachectl -k start")
                except:
                    os.system("/usr/sbin/apachectl -k start")
                    print("### error procesando huang ###")
                    logging.error('error procesando huang')
                    os.system("/usr/sbin/apachectl -k start")
            else:
                print("### error descargando datos huang ###")
                logging.error('error descargando datos huang')

        else:
            logging.info('huang actualizado')
        os.system("/usr/bin/ps aux | /usr/bin/grep python3 | grep -v steadyObjectApp.py |/usr/bin/awk '{print $2}' | /usr/bin/xargs /usr/bin/kill -9")
    except:

        try:
            os.system("/usr/bin/ps aux | /usr/bin/grep python3 | grep -v steadyObjectApp.py | /usr/bin/awk '{print $2}' | /usr/bin/xargs /usr/bin/kill -9")

            
            os.system("systemctl restart apache2")
        except:

            
            os.system("systemctl start apache2")
        try:

            
            os.system("systemctl start apache2")
        except:
            pass

try:
    #sleep(15)
    os.system("systemctl restart apache2")
except:
    #sleep(15)
    os.system("systemctl start apache2")
try:
    #sleep(15)
    os.system("systemctl start apache2")
except:
    pass
