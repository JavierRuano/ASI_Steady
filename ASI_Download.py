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

import cdsapi
import calendar
import datetime
import multiprocessing
import ASI_Datase_RACKt as ASI
import ntplib
import pandas as pd
from time import strptime
from calendar import monthrange
import time
import pandas as pd
import numpy as np
from datetime import datetime



class ASI_Download(ASI.ASI):
    variables = {
        'horton': [
            'u_component_of_wind',
            'v_component_of_wind',
            '10m_u_component_of_wind',
            '10m_v_component_of_wind',
            'total_precipitation',
# se añade para que tarde menos tiempo luego. Wang
#            'boundary_layer_height'
        ],
        'wang': [
            '10m_u_component_of_wind',
            '10m_v_component_of_wind',
            'total_precipitation',
            'boundary_layer_height'],
        'hortonwang': [
            'u_component_of_wind',
            'v_component_of_wind',
            '10m_u_component_of_wind',
            '10m_v_component_of_wind',
            'total_precipitation',
            'boundary_layer_height'],

        'huang': [
            'total_precipitation',
            'boundary_layer_height',
            'convective_inhibition',
            'convective_available_potential_energy'
        ],

    }
    def __init__(self, typi, commandi):
        super().__init__(typi, commandi)

    def getSuperClass(self):
        return super().getInstance()
    def era5_request(self,levels, rangeDateTime):
        try:
            c = cdsapi.Client()
            dating = rangeDateTime.split('/TO/')
            # '131.128',
            config = super().getConfig()


            import os, sys
            if (super().getCommand() == 'create'):
                path = config['daily_directory']['indir']  + 'months_ml/'
            else:
                path = config['daily_directory']['tmpdir'] + 'months_ml/'
            try:
                os.mkdir(path, 0o755)
            except:
                pass

            name = 'wind_ml' + levels + '_time_' + dating[0] + '_' + dating[1] + '.nc'


            c.retrieve('reanalysis-era5-complete', {
                'class': 'ea',
                'expver': '1',
                'stream': 'oper',
                'type': 'an',
                'param': ['131.128', '132.128'],
                'levtype': 'ml',
                'area': [75., -20., 24.75, 40.],
                # North, West, South, East. Default: global
                'levelist': levels,
                'grid': [0.75, 0.75],
                # Latitude/longitude grid: east-west (longitude) and north-south resolution (latitude). Default: 0.25 x 0.25
                'date': rangeDateTime,
                'time': '00/06/12/18',
                'format': 'netcdf'
            }, path + name)
            return "OK"
        except:
            return "ERROR"

    def generateUpdateDate(self):
        indices_maps = {'wang': 'ASI_Wang_2017', 'horton': 'ASI_Horton_2012', 'huang': 'ASI_Huang_2018'}

        timeNow = super().get_timestamp()
        month_limit = strptime(timeNow.split(' ')[1], '%b').tm_mon
        year_limit = int(timeNow.split(' ')[4])
        if (month_limit < 5):
            end_year = year_limit - 1
            end_month = 12 - abs((month_limit) - 2)
        else:
            end_year = year_limit
            end_month = month_limit - 3

        if (len(str(end_month)) == 1):
            end_month = "0" + str(end_month)

        super().read_Matrix_Django(super().getConfig()['daily_directory']['persistentDataset'])
        dayStart = pd.DatetimeIndex([super().getXarray()[indices_maps[str(super().GetASIName())]].time.values[
                                         len(super().getXarray()[indices_maps[str(super().GetASIName())]].time.values) - 1]])
        if (len(str(dayStart.month[0])) == 1):
            mesStart = "0" + str(dayStart.month[0])
        else:
            mesStart = str(dayStart.month[0])

        if (len(str(dayStart.day[0])) == 1):
            diaStart = "0" + str(dayStart.day[0])
        else:
            diaStart = str(dayStart.day[0])

        dataStart = str(dayStart.year[0]) + "-" + mesStart + "-" + diaStart
        dataEnd = str(end_year) + "-" + str(end_month) + "-" + str(monthrange(end_year, int(end_month))[1])

        return str(dataStart) + "/TO/" + str(dataEnd), dataStart, dataEnd, dayStart.month

    def raw_variables_to(self,i):
        try:
            config = super().getConfig()

            if (super().getCommand() == 'create'):
                filedestination = config['daily_directory']['indir']
            else:
                filedestination = config['daily_directory']['tmpdir']


            c = cdsapi.Client()
            # 'boundary_layer_height',
            timeHours = ['00:00', '06:00', '12:00', '18:00'] if (
                        i[1] not in ['convective_inhibition', 'convective_available_potential_energy']) else '12:00'
            call_Copernico = [{'product_type': 'reanalysis',
                               'format': 'netcdf',
                               'grid': ['0.75', '0.75'],
                               'area': ['75.', '-20.', '24.75', '40.']}]

            call_Copernico[0]['variable'] = i[1]
            call_Copernico[0]['time'] = timeHours



            datestime = [d.strftime('%Y-%m-%d') for d in pd.date_range(
                str(datetime.strptime(i[0].split('/')[0], "%Y-%m-%d").day) + "-" + str(
                    datetime.strptime(i[0].split('/')[0], "%Y-%m-%d").month) + "-" +
                str(datetime.strptime(i[0].split('/')[0], "%Y-%m-%d").year),
                str(datetime.strptime(i[0].split('/')[2], "%Y-%m-%d").day) + "-" + str(
                    datetime.strptime(i[0].split('/')[2], "%Y-%m-%d").month) + "-" +
                str(datetime.strptime(i[0].split('/')[2], "%Y-%m-%d").year))]

            call_Copernico[0]['date'] = datestime
            BoolNotSurface = (('component_of_wind' in i[1]) and (('10' not in i[1])))
            ResourceEra5 = 'reanalysis-era5-pressure-levels' if BoolNotSurface else 'reanalysis-era5-single-levels'

            if (BoolNotSurface):
                call_Copernico[0]['pressure_level'] = '500'

            c.retrieve(ResourceEra5, call_Copernico[0],
                       filedestination + 'dataset_Global_' + i[1] + "_" + i[0].split('/')[0] + "_" + i[0].split('/')[
                           2] + '.nc')

            return "OK"
        except:
            return "ERROR"

    def retrieve_era5_levels(self):
        c = cdsapi.Client()


        levelup = "91/92/93/94/95/96/97/98/99/100/101/102/103/104/105/106/107/108/109/110/111/112/113/114/115/116/117/118/119/120/121/122/123/124/125/126/127/128/129/130/131/132/133/134/135/136/137"
        p = multiprocessing.Pool(47)
        #
        import pandas as pd
        import datetime
        datestime = [[d.strftime('%Y-%m')] for d in pd.date_range('1-1979', str(
            (datetime.date.today() - datetime.timedelta(days=92)).month) + "-" + str(
            (datetime.date.today() - datetime.timedelta(days=92)).year), freq='M')]

        timeDate = self.generateUpdateDate()[0]
        base_year = timeDate.split('-')[0]
        if (super().getCommand() == 'create'):
            array = timeDate.split('/')
            array[0] = '1979-1-1'
            # array[2]='-12-31'
            timeDate = "/".join(array)

        func_pivot = self.era5_request
        p = multiprocessing.Pool(37)
        answer=p.starmap(func_pivot, [[x, timeDate] for x in levelup.split('/')])
        return answer
    def dataDownloading(self):
        ASI = super().GetASIName()
        MAX_WORKERS = 12
        timeDate = self.generateUpdateDate()[0]

        if (super().getCommand()=='create'):
            array = timeDate.split('/')
            array[0] = '1979-1-1'
            # array[2]='-12-31'
            timeDate = "/".join(array)

        timeplusvariable = [[timeDate, y] for y in self.variables[str(ASI)]]
        func_pivot = self.raw_variables_to
        p = multiprocessing.Pool(MAX_WORKERS)
        # OJO cambiar lo de las doce de la mañana BLH

        answers = p.map(func_pivot, timeplusvariable)
        # answers=p.map(raw_variables,timeplusvariable)

        if (np.sum([y == 'OK' for y in answers]) == len(self.variables[str(ASI)])):
            return "OK"
        else:
            return "ERR"

    def execute(self):
        ASI=super().GetASIName()
        
            # print(generateUpdateDate(str(ASI).replace('HortonWang','Horton').lower())[1])
        start_date = datetime.strptime(self.generateUpdateDate()[1],'%Y-%m-%d')

        timeNow = datetime.strptime(super().get_timestamp(), '%a %b %d %H:%M:%S %Y')
        num_months = (timeNow.year - start_date.year) * 12 + (timeNow.month - start_date.month)
        print(start_date)
        print(num_months)
        if (num_months < 4):
            print("Ya actualizado")
            return "FALSE"

        answer = self.dataDownloading()
        if ASI != 'huang' and num_months > 3:
            return answer



        if (answer == "OK"):
            dateMark = self.generateUpdateDate()[0]
            answer=self.retrieve_era5_levels()
            # dataDownloading(2050, variables)
            return answer
        else:
            return "ERROR"


#x=ASI_Download('horton','update')
#z=ASI_Download('huang','update')
#y=x.getSuperClass()
#del(x)
#print(y.GetASIName())
