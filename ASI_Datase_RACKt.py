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


import configparser
import xarray as xr
import numpy as np
import netCDF4
import os
import numpy as np
import datetime
from cartopy import config
import cartopy           as cart
import cartopy.crs       as ccrs
import matplotlib.pyplot as plt
import calendar
import pandas as pd



class ASI:
    def __init__(self, typi, commandi):
        """ Contructor de la clase ASI

        Leera la configuración de un fichero config.ini.
        Establece el modo de operación de la clase y el tipo
        de índice de estancamiento.
        Crea una variable llamada xarray donde se almacenan algunas de
        las variables que son necesarias para calcular el índice de
        estancamiento.

        Parámetros:
        typi -- tipo de índice de estancamiento
        commandi -- modo de operación


        """
        self.config = configparser.ConfigParser()
        self.config.optionxform = str
        self.config.read('/DataLake/utils/config.ini')

        self.timestamp = self.get_timestamp()
        self.units = self.config['daily_units']
        self.description = self.config['daily_description']
        self.typi = typi
        # commandi serÃ­a create, update o maps
        self.conquer = commandi
        self.xarray = xr.Dataset()


    def GetASIName(self):
        return self.typi
    def getConfig(self):
        return self.config
    def getXarray(self):
        return self.xarray
    def getCommand(self):
        return self.conquer
    def getInstance(self):
        return self
    def module_Wind(self,Wind,ml):
        """ Crea el módulo de un DataArray
            La operación será

            xr.ufuncs.sqrt(xr.ufuncs.square(Module_Wind_u) + xr.ufuncs.square(Module_Wind_v))

            En el caso que ml sea True, también se creará una media del día, es decir,
            la función producirá el módulo diario.

            resample(time='1d').reduce(np.mean)

            En el caso de Huang no se hace puesto se requiere comparar las cuatro horas
            disponible 00:00, 06:00, 12:00, 18:00 para usar sólo la que mayor ventilación
            produzca.

            Parámetros:
            Wind -- DataArray del que se producirá el módulo
            ml -- Si la operación proviene de Huang, es decir,
                  son model levels su valor será True en otro
                  caso será False.




            """


        Wind_v = Wind[ ('v', 'v10')['v10' in Wind.data_vars]]
        Wind_u = Wind[('u', 'u10')['u10' in Wind.data_vars]]

        time_Index = Wind_u.time.sel(time=datetime.time(12)).values
        latitude_Index = Wind_u.latitude.values
        longitude_Index = Wind_u.longitude.values
        # level_Index=Wind_u.level
        if(ml==False):

            Module_Wind_u =  Wind_u.resample(time='1d').reduce(np.mean)
            Module_Wind_v = Wind_v.resample(time='1d').reduce(np.mean)
            #    np.add.reduceat(Wind_u.values, np.arange(0, len(np.arange(len(Wind_u.time.values))), 4)) / 4
            #Module_Wind_v = np.add.reduceat(Wind_v.values, np.arange(0, len(np.arange(len(Wind_v.time.values))), 4)) / 4
            WindDatarray=xr.DataArray(xr.ufuncs.sqrt(xr.ufuncs.square(Module_Wind_u) + xr.ufuncs.square(Module_Wind_v)),
                                        coords=[time_Index, latitude_Index, longitude_Index],
                                        dims=['time', 'latitude', 'longitude'])
        if(ml==True):
            time_Index = Wind_u.time.values
            latitude_Index = Wind_u.latitude.values
            longitude_Index = Wind_u.longitude.values
            WindDatarray = xr.DataArray((xr.ufuncs.sqrt(xr.ufuncs.square(Wind_v), xr.ufuncs.square(Wind_u))).values,
                                        coords=[time_Index, latitude_Index, longitude_Index],
                                        dims=['time', 'latitude', 'longitude'])




        return WindDatarray



    def module_Precipitation(self):
        """
        realiza una operación de acumulación diario de precipitación, no tiene variable de
        entrada puesto usa la variable self.xarray.tp que se creó desde Read_netcdf, y es
        asignada a la variable self.xarray creada en el constructor.

        self.xarray.tp.resample(time='1d').reduce(np.sum).fillna(0).compute().values * 1000

        Se multiplica por 1000 puesto que las unidades son mm.


        :return:
        """

        import numpy as np
        time_Index = self.xarray.tp.time.sel(time=datetime.time(12)).values
        longitude_Index = self.xarray.tp.longitude.values
        latitude_Index = self.xarray.tp.latitude.values
        return xr.DataArray(self.xarray.tp.resample(time='1d').reduce(np.sum).fillna(0).compute().values * 1000,
                                                coords=[time_Index, latitude_Index, longitude_Index],
                                                dims=['time', 'latitude', 'longitude'])



    def ventilation_reduce_dimension(self):

        """
            Reduce la dimensión de la ventilación eligiendo la máxima ventilación para un día
            de entre las cuatro horas disponibles, 00:00; 06:00, 12:00, 18:00.
            Esta función es llamada desde Huang.

        """

        return self.xarray.ventilation.resample(time='1d').reduce(np.max)
    def blh_reduce_dimension(self):
        """
        Reduce la dimensión de blh puesto que en la generación de Horton y Wang sólo se
        usa las 12:00 mientras que en la generación de Huang usa 00:00, 06:00, 12:00 ,
        18:00.
        Es una medida que se debe usar puesto que en los Dataset, el indice de tiempo será
        global y aunque sólo usemos las 12:00, habrá presencia de nan para compatibilizar
        con el resto de variables
        """
        latitude_Index = self.xarrayblh.blh.latitude.values

        longitude_Index = self.xarrayblh.blh.longitude.values
        time_Index = self.xarrayblh.blh.sel(time=datetime.time(12)).time.values
        return xr.DataArray(self.xarrayblh.blh.sel(time=datetime.time(12)).values,
                                                coords=[time_Index, latitude_Index, longitude_Index],
                                                dims=['time', 'latitude', 'longitude'])

    def generate_Mask_Ocean(self, NETCDF_FILES_FOLDER):
        """
        Usa la temperatura temperatura de la superficie en el oceáno  (sea surface temperature field (sst)), para crear una
        máscara de tierra/agua, la temperatura en la zona terrestres será nan.


        :param self:
        :param NETCDF_FILES_FOLDER: Carpeta donde se almacenan los ficheros netcdf
        :return:
        """
        ds = xr.open_mfdataset(NETCDF_FILES_FOLDER + 'SurfaceTemp.nc',combine='by_coords')

        mask_ocean = 2 * np.ones((ds.dims['latitude'], ds.dims['longitude'])) * np.isfinite(ds.sst.isel(time=0))
        mask_land = 1 * np.ones((ds.dims['latitude'], ds.dims['longitude'])) * np.isnan(ds.sst.isel(time=0))
        mask_array = mask_ocean + mask_land

        self.xarray.coords['mask'] = (('latitude', 'longitude'), mask_array)

    def get_ASI_Netcdf(self, NETCDF_FILES_FOLDER, start, end, lat_uno, lat_dos, lng_uno, lng_dos):
        """
        filtra un indice de estancamiento según sus coordenadas y según el espacio de tiempo,
        es util para la función del web server que permite descargar los datos.
        Es importante destacar que usa el diccionarío:
        {'wang': 'ASI_Wang_2017', 'horton': 'ASI_Horton_2012', 'huang': 'ASI_Huang_2018'}


        :param self:
        :param NETCDF_FILES_FOLDER:
        :param start: fecha inicial
        :param end: fecha final
        :param lat_uno:
        :param lat_dos:
        :param lng_uno:
        :param lng_dos:
        :return:
        """

        indices_maps = {'wang': 'ASI_Wang_2017', 'horton': 'ASI_Horton_2012', 'huang': 'ASI_Huang_2018'}
        self.read_Matrix_Django(NETCDF_FILES_FOLDER)
        self.generate_Mask_Ocean(NETCDF_FILES_FOLDER)
        return self.xarray[indices_maps[str(self.typi)]].where(self.xarray.mask == 1).where(
            (self.xarray[indices_maps[str(self.typi)]].latitude >= lat_uno) & (
                        self.xarray[indices_maps[str(self.typi)]].latitude <= lat_dos) & (
                    self.xarray[indices_maps[str(self.typi)]].longitude >= lng_uno) & (
                    self.xarray[indices_maps[str(self.typi)]].longitude <= lng_dos), drop=True).sel(
            time=slice(start, end))

    def read_Matrix_Django(self,NETCDF_FILES_FOLDER):
        """
        Leerá los indices de estancamiento de los ficheros netcdf almacenados.
        Los almacena en la variable xarray, está función es usada para producir los ficheros
        CSV para mostrar en la página web.
        La función usará el atributo self.typi para identificar el tipo de indice
        de estancamiento que se desea extraer. Dicho atributo fue asignado en el constructor
        de la clase ASI.

        :param self:
        :param NETCDF_FILES_FOLDER: Directorio donde están almacenados los netcdf almacenados.
        :return:
        """

        if (self.typi == 'horton'):
            self.xarray = xr.open_mfdataset(NETCDF_FILES_FOLDER + 'HORTON_MAP.nc',combine='by_coords')
        if (self.typi == 'wang'):
            self.xarray = xr.open_mfdataset(NETCDF_FILES_FOLDER + 'WANG_MATRIX.nc',combine='by_coords')
        if (self.typi == 'huang'):
            self.xarray = xr.open_mfdataset(NETCDF_FILES_FOLDER + 'HUANG_MAP.nc',combine='by_coords')
    def read_Matrix_parameters_Django(self,NETCDF_FILES_FOLDER):
        """
        Leerá las variables usadas para indices de estancamiento de los ficheros netcdf
        almacenados.
        Los almacena en la variable xarray, está función es usada para producir los ficheros
        CSV para mostrar en la página web.
        La función usará el atributo self.typi para identificar el tipo de indice
        de estancamiento que se desea extraer. Dicho atributo fue asignado en el constructor
        de la clase ASI.

        :param self:
        :param NETCDF_FILES_FOLDER: Carpeta donde se almacenan los ficheros Netcdf
        :return:
        """

        if (self.typi == 'horton'):
            self.xarray = xr.open_mfdataset(NETCDF_FILES_FOLDER + 'HORTON.nc',combine='by_coords')
        if (self.typi == 'wang'):
            self.xarray = xr.open_mfdataset(NETCDF_FILES_FOLDER + 'WANG.nc',combine='by_coords')
        if (self.typi == 'huang'):
            self.xarray = xr.open_mfdataset(NETCDF_FILES_FOLDER + 'HUANG.nc',combine='by_coords')

    def get_Photo_ASI(self, NETCDF_FILES_FOLDER, IMAGE_FILES_FOLDER, start, end, lat_uno, lat_dos, lng_uno, lng_dos):
        """
        Produce imágenes filtradas por las variables en entradas, y generarán 5 imágenes de
        teniendo cada una de las imágenes un máximo diferente.
        Al igual que las demás funciones los parámetros asignados provienen del servidor web,
        en particular del formulario.
        Está función tiene algunas partes procedentes del script de Jose Garrido,
        lon_corrected y lat_corrected han sido muy útiles para encajar los pixeles de color
        en el mapa.


        :param self:
        :param NETCDF_FILES_FOLDER:
        :param IMAGE_FILES_FOLDER:
        :param start:
        :param end:
        :param lat_uno:
        :param lat_dos:
        :param lng_uno:
        :param lng_dos:
        :return:
        """
        #### desde django me tendrÃ¡ que enviar los NETCDF_FILES_FOLDER

        self.read_Matrix_Django(NETCDF_FILES_FOLDER)
        # read_Matrix_Django()
        self.generate_Mask_Ocean(NETCDF_FILES_FOLDER)
        #### desde django me tendrÃ¡ que enviar los NETCDF_FILES_FOLDER
        indices_maps = {'wang': 'ASI_Wang_2017', 'horton': 'ASI_Horton_2012', 'huang': 'ASI_Huang_2018'}

        dataImg = (self.xarray[indices_maps[str(self.typi)]].where(self.xarray.mask == 1).sel(
            time=slice(start, end)).sum(dim=('time')) * 100) / len(
            self.xarray[indices_maps[str(self.typi)]].sel(time=slice(start, end)).coords['time'].values)

        # dataImg=self.xarray[indices_maps[str(self.typi)]].where(self.xarray[indices_maps[str(self.typi)]] == 1).sel(time=slice(start, end)).sum(dim=('time'))
        min_lon = -20
        max_lon = 40
        min_lat = 24.75
        max_lat = 75
        bbox = [min_lon, max_lon, min_lat, max_lat]

        def discrete_cmap(N, base_cmap=None):
            """Create an N-bin discrete colormap from the specified input map"""
            import cartopy           as cart
            import cartopy.crs       as ccrs
            import matplotlib.pyplot as plt
            # Note that if base_cmap is a string or None, you can simply do
            #    return plt.cm.get_cmap(base_cmap, N)
            # The following works for string, None, or a colormap instance:


            base = plt.cm.get_cmap(base_cmap)
            color_list = base(np.linspace(0, 1, N))
            cmap_name = base.name + str(N)
            return base.from_list(cmap_name, color_list, N)

        # ccrs.Mercator()

        def make_map(bbox, projection=ccrs.Mercator()):
            fig, ax = plt.subplots(figsize=(9, 13),
                                   subplot_kw=dict(projection=projection))
            ax.set_extent(bbox)
            ax.coastlines(resolution='50m')
            return fig, ax


        fig, ax = make_map(bbox=bbox)
        import random

        hashin = random.getrandbits(128)

        # Cambiado a petición de Carlos para ajustarlo al artículo de Jose Garrido
        for valor in np.arange(0, 5):
            saturation = 100
            if (valor == 0):
                saturation = 100
            if (valor == 1):
                saturation = 90
            if (valor == 2):
                saturation = 80
            if (valor == 3):
                saturation = 70
            if (valor == 4):
                saturation = 60

            dataImg = (self.xarray[indices_maps[str(self.typi)]].where(self.xarray.mask == 1).sel(
                time=slice(start, end)).sum(dim=('time')) * 100) / len(
                self.xarray[indices_maps[str(self.typi)]].sel(time=slice(start, end)).coords['time'].values)

            lon_corrected = np.copy(dataImg['longitude'])
            lat_corrected = np.copy(dataImg['latitude'])
            delta_lon = lon_corrected[1] - lon_corrected[0]
            delta_lat = lat_corrected[1] - lat_corrected[0]
            for xxx in range(0, len(lon_corrected)):
                lon_corrected[xxx] = lon_corrected[xxx] - delta_lon / 2.
            for yyy in range(0, len(lat_corrected)):
                lat_corrected[yyy] = lat_corrected[yyy] - delta_lat / 2.

                # map projection

            ##### OJO SE PODRÏA DEJAR cmap para modificar el color de la imágen
            cs = ax.pcolormesh(lon_corrected, lat_corrected, dataImg.values, cmap='jet', vmax=saturation,
                               transform=ccrs.PlateCarree())



            fig.savefig(IMAGE_FILES_FOLDER + str(hashin) + str(valor) + '.png', transparent=True)


        return str(hashin)

    def refresh_Graphics(self, NETCDF_FILES_FOLDER, start, end, lat_uno, lat_dos, lng_uno, lng_dos):
        """
        Esta función es la que consulta el fichero netcdf de índices de estancamiento para
        producir un fichero CSV donde se indica el tanto por ciento de días estancados,
        comprendidos entre start y end.

        :param self:
        :param NETCDF_FILES_FOLDER:
        :param start:
        :param end:
        :param lat_uno:
        :param lat_dos:
        :param lng_uno:
        :param lng_dos:
        :return:
        """
        #### desde django me tendrÃ¡ que enviar los NETCDF_FILES_FOLDER

        self.read_Matrix_Django(NETCDF_FILES_FOLDER)
        # read_Matrix_Django()
        self.generate_Mask_Ocean(NETCDF_FILES_FOLDER)
        #### desde django me tendrÃ¡ que enviar los NETCDF_FILES_FOLDER
        indices_maps = {'wang': 'ASI_Wang_2017', 'horton': 'ASI_Horton_2012', 'huang': 'ASI_Huang_2018'}



        saveFile = self.xarray[indices_maps[str(self.typi)]].where(self.xarray.mask == 1).where(
            (self.xarray[indices_maps[str(self.typi)]].latitude >= lat_uno) & (
                        self.xarray[indices_maps[str(self.typi)]].latitude <= lat_dos) & (
                    self.xarray[indices_maps[str(self.typi)]].longitude >= lng_uno) & (
                    self.xarray[indices_maps[str(self.typi)]].longitude <= lng_dos), drop=True).sel(
            time=slice(start, end))


        CSVFile = saveFile.mean(dim=['latitude', 'longitude']).to_dataframe()

        CSVFile['Date'] = saveFile.time.values
        CSVFile['Date'] = pd.to_datetime(CSVFile.Date)
        CSVFile['Date'] = CSVFile['Date'].dt.strftime('%Y-%m-%d')

        CSVFile[self.typi] = CSVFile[indices_maps[self.typi]].apply(lambda x: round(x * 100, 2))

        # CSVFile['TILA']=CSVFile['ASI_Horton_2012'].apply(lambda x: str(round(round(x*100,2)+3,2))+";"+str(round(round(x*100,2)+5,2)) +"; "+str(round(round(x*100,2)+6,2))).str.replace(" ","")
        CSVFile = CSVFile.drop(indices_maps[self.typi], axis=1)

        # CSVFile['HIERBALUISA']=CSVFile['ASI_Horton_2012'].apply(lambda x: str(round(round(x*100,2)+3,2))+";"+str(round(round(x*100,2)+5,2)) +"; "+str(round(round(x*100,2)+6,2))).str.replace(" ","")
        CSVFile = CSVFile.set_index('Date')
        CSVFile.rename(columns={self.typi: '% of stagnant area'}, inplace=True)
        # CSVFile.to_csv(output, sep=',')
        return CSVFile

    def bbox_Netcdf(self, start, end, lat_uno, lat_dos, lng_uno, lng_dos, variable):
        """
        filtra xarray acorde las variable de entrada y cacula la media en
        la dimensión latitud y la longitud, quedando una Time Series.
        :param self:
        :param start:
        :param end:
        :param lat_uno:
        :param lat_dos:
        :param lng_uno:
        :param lng_dos:
        :param variable:
        :return:
        """
        saveFile = self.xarray
        # print(saveFile[variable].where(saveFile.mask == 1))
        return saveFile[variable].where(saveFile.mask == 1).where(
            (saveFile[variable].latitude >= lat_uno) & (saveFile[variable].latitude <= lat_dos),
            drop=True).where(
            (saveFile[variable].longitude >= lng_uno) & (saveFile[variable].longitude <= lng_dos),
            drop=True).sel(time=slice(start, end)).mean(dim=['latitude', 'longitude']).to_dataframe()

    def refresh_Graphics_CSV(self, NETCDF_FILES_FOLDER, start, end, lat_uno, lat_dos, lng_uno, lng_dos):
        #### desde django me tendrÃ¡ que enviar los NETCDF_FILES_FOLDER
        self.read_Matrix_parameters_Django(NETCDF_FILES_FOLDER)
        # read_Matrix_Django()
        self.generate_Mask_Ocean(NETCDF_FILES_FOLDER)

        if (self.typi == 'horton'):
            variables = ['Wind_Surface', 'Wind', 'Precipitation']
        if (self.typi == 'wang'):
            variables = ['Wind_Surface', 'blh', 'Precipitation']

        if (self.typi == 'huang'):
            ### wind_ml
            variables = ['ventilation', 'Precipitation']
        # dataframeCSV=saveFile['Wind_Surface'].where(saveFile.mask==1).where((saveFile['Wind_Surface'].latitude>=lat_uno) & (saveFile['Wind_Surface'].latitude<=lat_dos) & (saveFile['Wind_Surface'].longitude>=lng_uno) & (saveFile['Wind_Surface'].longitude<=lng_dos),drop=True).sel(time=slice(start,end)).mean(dim=['latitude','longitude']).to_dataframe()
        #### Requisito crear el Dataset de xarray
        # dataframeCSV=xr.Dataset(coords=self.xarray.coords)
        dataframeCSV = self.bbox_Netcdf(start, end, lat_uno, lat_dos, lng_uno, lng_dos, variables[0])

        for i in variables[1:]:

            dataframeCSV[str(i)] = self.bbox_Netcdf(start, end, lat_uno, lat_dos, lng_uno, lng_dos, i)

        #### Algo parecido para manipular el dataset.
        dataframeCSV = dataframeCSV.reset_index()
        dataframeCSV.rename(columns={'time': 'Date'}, inplace=True)
        dataframeCSV['Date'] = pd.to_datetime(dataframeCSV.Date)
        dataframeCSV['Date'] = dataframeCSV['Date'].dt.strftime('%Y-%m-%d')
        if (self.typi == 'huang'):
            try:
                dataframeCSV['Precipitation'] = dataframeCSV['Precipitation'].apply(
                    lambda x: str(round(float(x) * 1000, 2)))
            except:
                dataframeCSV['Precipitation'] = dataframeCSV['Precipitation'].apply(
                    lambda x: str(round(float(x), 2)))
        try:
            dataframeCSV['Precipitation'] = dataframeCSV['Precipitation'].apply(lambda x: str(round(float(x), 2)))

        except:
            dataframeCSV['Precipitation'] = dataframeCSV['tp'].apply(
                lambda x: str(round(float(x), 2)))
        for i in variables:
            if (i == 'Wind_Surface' or i == 'ventilation'):
                chain_ini = ""
            else:

                chain_ini = "0,"

            if (i == 'blh'):

                dataframeCSV['blh'] = dataframeCSV['blh'].apply(lambda x: float(x))
                if (self.typi == 'wang'):
                    dataframeCSV['blh'] = dataframeCSV['blh'] / 1000

                # dataframeCSV['blh']=dataframeCSV[dataframeCSV['blh']!=0.0].apply(lambda x: float(x)/1000)
            dataframeCSV[str(i)] = dataframeCSV[str(i)].apply(lambda x: chain_ini + str(round(float(x), 2)))

        ####
        #  Se debe hacer un diccionario dataframeCSV.rename(columns={'Wind': '500hPa Wind (m/s)'}, inplace=True)
        #   dataframeCSV.rename(columns={'Wind_Surface': '10m Wind (m/s)'}, inplace=True)
        #    dataframeCSV.rename(columns={'Precipitation': 'Precipitation * 1000'}, inplace=True)

        if (self.typi == 'horton'):
            variables = ['Wind_Surface', 'Wind', 'Precipitation']
            dataframeCSV.rename(columns={'Wind': '500hPa Wind (m/s)'}, inplace=True)
            dataframeCSV.rename(columns={'Wind_Surface': '10m Wind (m/s)'}, inplace=True)
            dataframeCSV.rename(columns={'Precipitation': 'Precipitation (mm)'}, inplace=True)

        if (self.typi == 'wang'):
            variables = ['Wind_Surface', 'blh', 'Precipitation']
            dataframeCSV.rename(columns={'blh': 'blh km'}, inplace=True)
            dataframeCSV.rename(columns={'Wind_Surface': '10m Wind (m/s)'}, inplace=True)
            dataframeCSV.rename(columns={'Precipitation': 'Precipitation (mm)'}, inplace=True)

        if (self.typi == 'huang'):
            # variables = ['CAPE', 'CIN', 'Ventilation', 'blh', 'Precipitation']
            variables = ['Precipitation', 'ventilation']
            # dataframeCSV.rename(columns={'blh': 'blh (m)'}, inplace=True)
            dataframeCSV.rename(columns={'Precipitation': 'Precipitation (mm) * 1000'}, inplace=True)
            dataframeCSV.rename(columns={'ventilation': 'Ventilation (m/s)'}, inplace=True)
            # dataframeCSV.rename(columns={'CIN': 'CIN (J/kg)'}, inplace=True)
            # dataframeCSV.rename(columns={'CAPE': 'CAPE (J/kg)'}, inplace=True)

        dataframeCSV = dataframeCSV.set_index('Date')
        return dataframeCSV

    def printDescription(self):
        print(self.timestamp)


    def Horton_generation(self, update=False):
        """
        Función que calcula el indice de estancamiento de Horton,
        :param self:
        :param update:
        :return:
        """

        wind_dataset=self.xarray.u.to_dataset(name='u').merge(self.xarray.v.to_dataset(name='v'))


        wind_dataarray=self.module_Wind(wind_dataset,False)
        time_Index=wind_dataarray.time.values
        latitude_Index=wind_dataarray.latitude.values
        longitude_Index=wind_dataarray.longitude.values

        Rain_Array=self.module_Precipitation()

        datasetSurface=self.xarray.u10.to_dataset(name='u10').merge(self.xarray.v10.to_dataset(name='v10'))
        Wind_Surface_Array=self.module_Wind(datasetSurface,False)

        Rain_Array['time']=Wind_Surface_Array['time']

        ds = xr.Dataset({'Wind':wind_dataarray,
                         'Wind_Surface':  Wind_Surface_Array,
                         'Precipitation': Rain_Array,

                         },
                        coords={'time': time_Index,
                                'lat': latitude_Index,
                                'lon': longitude_Index

                                })
        ds['lat'].attrs = {
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'}
        ds['lon'].attrs = {
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'}


        ASI = xr.ufuncs.logical_and(wind_dataarray < 13., xr.ufuncs.logical_and(Wind_Surface_Array < 3.2, Rain_Array < 1.))


        ASI['latitude'].attrs = {
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'}
        ASI['longitude'].attrs = {
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'}
        ASI['time'].attrs = {
            'standard_name': 'time',
            'long_name': 'time  ',
            }

        ASI = ASI.assign_attrs(
            { 'long_name': 'air_stagnation_index'})
        #ASI = self.Add_Description_dataset(ASI)
        if (update == False):
            self.Add_Description_dataset(ASI.to_dataset(name='ASI_Horton_2012')).to_netcdf(self.config['daily_directory']['outdir']+'HORTON_MAP.nc',
                                                                                           encoding={'latitude': {'_FillValue': None}, 'longitude': {'_FillValue': None}},mode='w')
            self.Add_Description_dataset(ds).to_netcdf(self.config['daily_directory']['outdir']+'HORTON.nc',
                                                       encoding={'lat': {'_FillValue': None}, 'lon': {'_FillValue': None}},mode='w')
        else:

            old_ds = xr.open_mfdataset(self.config['daily_directory']['persistentDataset']+'HORTON.nc',combine='by_coords')
            #### Recover netcdf and merge with the another
            new_ds = old_ds.combine_first(ds).compute()
            print("uno")
            self.xarray.close()
            old_ds.close()
            self.Add_Description_dataset(new_ds).to_netcdf(self.config['daily_directory']['pruebadir']+'HORTON.nc',
                                                           encoding={'lat': {'_FillValue': None}, 'lon': {'_FillValue': None}},mode='w')
            print("dos")
            old_ds = xr.open_mfdataset(self.config['daily_directory']['persistentDataset']+'HORTON_MAP.nc',combine='by_coords')

            new_ds = old_ds.combine_first(ASI.to_dataset(name='ASI_Horton_2012')).compute()
            old_ds.close()
            self.xarray.close()
            print("tres")
            self.Add_Description_dataset(new_ds).to_netcdf(self.config['daily_directory']['pruebadir']+'HORTON_MAP.nc',
                                                           encoding={'latitude': {'_FillValue': None}, 'longitude': {'_FillValue': None}},mode='w')

    def Huang_generation(self, update=False):
        """
        Método que calcula el índice de estancamiento Huang.
        Una de las formas que agilizan este calculo este eliminar la dimensión de nivel,
        por ello se descargan los ficheros del valor u y v por cada nivel en un fichero
        distinto, y se va leyendo y usandolo para calcular la ventilación.
        Pero no habrá más de un nivel cargado en memoria.

        :param self:
        :param update:
        :return:
        """

        # variables=['CAPE','CIN','Ventilation','blh','Precipitation']


        # Diferente porque al ser muchos ficheros es mejor leer los niveles iterativamente.
        if (update == False):

            directory = self.config['daily_directory']['indir']+'months_ml/'
        else:
            directory=self.config['daily_directory']['tmpdir']+'months_ml/'

        BH=self.xarrayblh['blh'].compute()

        flag_ventilation = False
        levels = {}

        for i in open('/DataLake/utils/model-layer', 'r').read().split('\n'):
            if (len(i.split(' ')) < 2):
                break
            levels[float(i.split(' ')[0])] = float(i.split(' ')[1])
        layer_meters = [10] + [i[0] - i[1] for i in zip(list(levels.values())[-2::-1], list(levels.values())[:-47:-1])]
        layer_meters.reverse()
        #directory = 'H:\\Trabajo\\Notebooks\\raw_data\\'
        
        
        count=0

        for value in levels.keys():


            if(update==False):
                Wind = xr.open_mfdataset(directory + 'wind_ml' + str(value).replace('.0', '') + '*.nc',combine='by_coords').sel(time=slice('1979-1-1', '2019-12-31'))
            else:
                Wind = xr.open_mfdataset(directory + 'wind_ml' + str(value).replace('.0', '') + '*.nc',combine='by_coords')

            wind_module=self.module_Wind(Wind,True).compute()
            wind_mod=wind_module.compute()
            wind_mod['time']=BH['time']
            #BH['time']=self.xarray_aux['cin'].time.values
            if (count > 0):
                ventilation += wind_mod.values * abs( BH.values - levels[value]) * (levels[value] < BH.values) * (BH.values < levels[value - 1])
                ventilation += wind_mod.values * abs(layer_meters[count]) * (levels[value] < BH.values) * (BH.values > levels[value - 1])

            if(count==0):
                ventilation=xr.DataArray(coords=[wind_mod['time'].values, wind_mod.latitude.values, wind_mod.longitude.values],
                                    dims=['time', 'latitude', 'longitude']).fillna(0)
            count += 1
        self.xarray_aux['blh'] = self.blh_reduce_dimension().compute()

        Precipitation=self.module_Precipitation().compute()
        Precipitation['time']=self.xarray_aux.time.values
        self.xarray['ventilation']=ventilation
        Ventilation=self.ventilation_reduce_dimension().compute()
        Ventilation['time']=self.xarray_aux.time.values

        # tp time cambia todo el dataset, por lo que por seguridad se deja en otro xarray.
        backup=self.xarray_aux

        backup['Precipitation']=Precipitation

        backup['ventilation']=Ventilation
        self.xarray=self.xarray.drop('tp')

        #self.xarray['tp']['time']=self.xarray['ventilation']['time']
        self.xarray = backup

        self.xarray['cin']['time']=self.xarray['ventilation']['time']
        self.xarray['cape']['time']=self.xarray['ventilation']['time']
        self.xarray['Precipitation']['time']=self.xarray['ventilation']['time']

        ASI_HUANG=xr.ufuncs.logical_and(  ~(xr.ufuncs.logical_and(np.sum(self.xarray['cin'].fillna(0).values < 50), self.xarray['cape'].fillna(0).values > 100)),xr.ufuncs.logical_and(self.xarray.ventilation.fillna(0).values < 6000, self.xarray['Precipitation'].values < 0.001))

        ASI_HUANG=xr.DataArray(ASI_HUANG, coords=[self.xarray.ventilation.sortby('time').time.values, self.xarray.ventilation.latitude.values, self.xarray.ventilation.longitude.values],
                                  dims=['time', 'latitude', 'longitude'])

        ASI_HUANG['latitude'].attrs = {
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'}
        ASI_HUANG['longitude'].attrs = {
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'}

        self.xarray['latitude'].attrs = {
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'}
        self.xarray['longitude'].attrs = {
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'}


              # ASI = self.Add_Description_dataset(ASI)
        if (update == False):

            self.Add_Description_dataset(ASI_HUANG.to_dataset(name='ASI_Huang_2018')).to_netcdf(
                self.config['daily_directory']['outdir'] + 'HUANG_MAP.nc',
                encoding={'latitude': {'_FillValue': None}, 'longitude': {'_FillValue': None}},mode='w')

            self.xarray.drop('cin').drop('cape').to_netcdf(self.config['daily_directory']['outdir'] + 'HUANG.nc',
                                                       encoding={'latitude': {'_FillValue': None},
                                                                 'longitude': {'_FillValue': None}},mode='w')
        else:

            old_ds = xr.open_mfdataset(self.config['daily_directory']['persistentDataset'] + 'HUANG.nc',combine='by_coords')
            #### Recover netcdf and merge with the another
            new_ds = self.xarray.drop('cin').drop('cape').combine_first(old_ds).compute()
            old_ds.close()
            self.xarray.close()
            new_ds.to_netcdf(self.config['daily_directory']['pruebadir'] + 'HUANG.nc',
                                                           encoding={'latitude': {'_FillValue': None},
                                                                     'longitude': {'_FillValue': None}},mode='w')

            old_ds = xr.open_mfdataset(self.config['daily_directory']['persistentDataset'] + 'HUANG_MAP.nc',combine='by_coords')
            new_ds = ASI_HUANG.to_dataset(name='ASI_Huang_2018').combine_first(old_ds).compute()
            old_ds.close()
            self.xarray.close()
            self.Add_Description_dataset(new_ds).to_netcdf(self.config['daily_directory']['pruebadir'] + 'HUANG_MAP.nc',
                                                           encoding={'latitude': {'_FillValue': None},
                                                                     'longitude': {'_FillValue': None}},mode='w')
        # print(self.xarray['ventilation'])

    def Wang_generation(self, update=False):
        """

        :param self:
        :param update:
        :return:
        """

        import itertools

        Boundary_Layer_Hei = self.blh_reduce_dimension()
        Rain_Array=self.module_Precipitation().compute()
        datasetSurface=self.xarray.u10.to_dataset(name='u10').merge(self.xarray.v10.to_dataset(name='v10'))
        Wind_Surface_Array=self.module_Wind(datasetSurface,False)




        time_Index = Wind_Surface_Array.time.values
        latitude_Index = Wind_Surface_Array.latitude.values
        longitude_Index = Wind_Surface_Array.longitude.values

        ds = xr.Dataset({'blh': Boundary_Layer_Hei,
                         'Wind_Surface': self.module_Wind(datasetSurface,False).compute(),
                         'Precipitation': Rain_Array

                         },
                        coords={'time': time_Index,
                                'lat': latitude_Index,
                                'lon': longitude_Index

                                }).compute()


        ds['lat'].attrs = {
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'}
        ds['lon'].attrs = {
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'}

        print("Control")
        print(ds['Wind_Surface'].values.max())
        import pandas as pd

        dates = pd.DatetimeIndex(Wind_Surface_Array.time.values)

        Index = pd.DataFrame(Wind_Surface_Array.time.values, columns=['date'])



        def define_period(season):

            if (season in [3, 4, 5]):

                return 1

            elif (season in [6, 7, 8]):

                return 2

            elif (season in [9, 10, 11]):

                return 3

            else:

                return 0

        count = 0

        operationSeason = Index['date'].astype('datetime64').dt.month.apply(define_period).values

        def s1(b):

            return np.multiply(np.multiply(3.57, 1000), np.exp(-3.35 * b)) + 0.352

        def s2(b):

            return np.multiply(np.multiply(7.66, 10), np.exp(-2.12 * b)) + 0.443

        def s3(b):

            return np.multiply(np.multiply(1.88, 10000), np.exp(-5.15 * b)) + 0.440

        def s4(b):

            return np.multiply(0.759, np.exp(-0.6 * b)) + 0.264

        function = {

            "1": s1,

            "2": s2,

            "3": s3,

            "0": s4

        }

        count = 0

        for i in operationSeason:
            Wind_Surface_Array[count] = function[str(i)](Wind_Surface_Array[count])

            count += 1






        Wind_Surface_Array['time']=Boundary_Layer_Hei['time']
        Rain_Array['time']=Boundary_Layer_Hei['time']
        WANG_MATRIX = xr.ufuncs.logical_and(Rain_Array < 0.001,
                                            Boundary_Layer_Hei.compute() / 1000 < Wind_Surface_Array)
        WANG_MATRIX['latitude'].attrs = {
            'standard_name': 'latitude',
            'long_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'}
        WANG_MATRIX['longitude'].attrs = {
            'standard_name': 'longitude',
            'long_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'}
        print("actualiza")

        if (update == False):
            self.Add_Description_dataset(WANG_MATRIX.to_dataset(name='ASI_Wang_2017')).to_netcdf(
                self.config['daily_directory']['outdir']+'WANG_MATRIX.nc',
                encoding={'latitude': {'_FillValue': None}, 'longitude': {'_FillValue': None}},mode='w')
            ds.to_netcdf(self.config['daily_directory']['outdir']+'WANG.nc',
                         encoding={'lat': {'_FillValue': None}, 'lon': {'_FillValue': None}},mode='w')

        else:
            print("actualiza OK")
            old_ds = xr.open_mfdataset(self.config['daily_directory']['persistentDataset']+'WANG.nc',combine='by_coords')
            #### Recover netcdf and merge with the another
            
            new_ds = old_ds.combine_first(ds).compute()
            self.xarray.close()
            old_ds.close()
            new_ds.to_netcdf(self.config['daily_directory']['pruebadir']+'WANG.nc',
                             encoding={'lat': {'_FillValue': None}, 'lon': {'_FillValue': None}}, mode='w')
            print("actualiza OK OK")
            old_ds = xr.open_mfdataset(self.config['daily_directory']['persistentDataset']+'WANG_MATRIX.nc',combine='by_coords')
            new_ds = old_ds.combine_first(WANG_MATRIX.to_dataset(name='ASI_Wang_2017')).compute()
            print(new_ds)
            old_ds.close()
            
            
            print("actualiza OK OK OK")
            # self.Add_Description_dataset(ASI)
            #self.Add_Description_dataset(
            #self.config['daily_directory']['pruebadir']
            self.Add_Description_dataset(new_ds).to_netcdf(self.config['daily_directory']['pruebadir']+ 'WANG_MATRIX.nc', mode='w')
            print("OK OK OK OK")


    def get_timestamp(self):
        """

        :param self:
        :return:
        """
        import time
        import ntplib
        from time import ctime, strptime
        c = ntplib.NTPClient()
        try:
            response = c.request('europe.pool.ntp.org', version=3)
        except:
            try:
                time.time.sleep(1)
                response = c.request('1.europe.pool.ntp.org', version=3)
            except:
                try:
                    time.sleep(1)
                    response = c.request('2.europe.pool.ntp.org', version=3)
                except:
                    time.sleep(1)
                    response = c.request('3.europe.pool.ntp.org', version=3)

        # response = c.request('europe.pool.ntp.org', version=3)
        timer = time.strftime('%a %b %d %H:%M:%S %Y', strptime(ctime(response.tx_time)))

        return timer

    def Add_Description(self):

        self.xarray = self.xarray.assign_attrs(
            {'Description': "Matrix ASI according to HORTON 2012 " + str(self.xarray['time'][0].values)[0:4],
             'Department': self.config['Department']['Department'],
             'Authors': self.config['Author']['Author'],
             'Conventions': self.config['Conventions']['Convention'],
             'History': self.timestamp})

    def Add_Description_dataset(self, dataset_stream):
        """


        :param self:
        :param dataset_stream:
        :return:
        """
        dataset_stream.attrs['Description']= "Air stagnation index (ASI) following " +self.config['Researcher'][str(self.typi)] + ". Stagnant = 1, non-stagnant = 0."
        dataset_stream.attrs['Department']= self.config['Department']['Department']
        dataset_stream.attrs['Authors']= self.config['Author']['Author']
        dataset_stream.attrs['Reference']=self.config['References'][str(self.typi)]
        dataset_stream.attrs['Data_source']=self.config['Source']['Data_source']
        dataset_stream.attrs['Conventions'] = 'CF-1.6'
        #dataset_stream.attrs['Conventions']= self.config['Conventions']['Convention']

        dataset_stream.attrs['History']= self.timestamp
        try:
            dataset_stream['time'].attrs['standard_name']= 'time'
            dataset_stream['time'].attrs['long_name'] = 'time'
            #dataset_stream['time'].attrs['units'] = 'dimensionless'
            dataset_stream['latitude'].attrs['standard_name']='latitude'
            dataset_stream['latitude'].attrs['long_name'] ='latitude'
            dataset_stream['latitude'].attrs['units']= 'degrees_north'
            dataset_stream['longitude'].attrs['axis'] = 'Y'
            dataset_stream['longitude'].attrs['standard_name']= 'longitude'
            dataset_stream['longitude'].attrs['long_name'] = 'longitude'
            dataset_stream['longitude'].attrs['units']='degrees_east'
            dataset_stream['longitude'].attrs['axis']='X'
        except:
            pass
        try:
            ##dataset_stream['mask'].attrs['units']='dimensionless'
            dataset_stream['mask'].attrs['long_name']="land_sea_mak"
            dataset_stream.attrs['Conventions'] = 'CF-1.6';
            dataset_stream.attrs['featureType'] = "timeseries";
            dataset_stream.attrs['cdm_data_type'] = "Timeseries";
            dataset_stream.attrs['title'] = "Air stagnation index";
            dataset_stream.attrs['institution'] = "UCM";
        except:
            dataset_stream.attrs['Conventions'] = 'CF-1.6';
            pass
        try:
            indices_maps = {'wang': 'ASI_Wang_2017', 'horton': 'ASI_Horton_2012', 'huang': 'ASI_Huang_2018',
                            'all': 'lambda'}

            dataset_stream[indices_maps[self.typi]]=dataset_stream[indices_maps[self.typi]].assign_attrs({'long_name':'air_stagnation_index'})
        except:
            # Para otro tipo de variables
            pass
        return dataset_stream

        #dataset_stream.attrs['Description']= "Air stagnation index (ASI) following " +self.config['Researcher'][str(self.typi)] + ". Stagnant = 1, non-stagnant = 0."
        #dataset_stream.attrs['Department']= self.config['Department']['Department']
        #dataset_stream.attrs['Authors']= self.config['Author']['Author']
        #dataset_stream.attrs['Reference']=self.config['References'][str(self.typi)]
        #dataset_stream.attrs['Data_source']=self.config['Source']['Data_source']
        #dataset_stream.attrs['Conventions']= self.config['Conventions']['Convention']
        #dataset_stream.attrs['History']= self.timestamp
        #dataset_stream['time'].attrs['standard_name']= 'time'
        #dataset_stream['time'].attrs['long_name'] = 'time'

        #return dataset_stream
    """
    Lectura de los ficheros Netcdf.
    
    """
    def Read_netcdf(self, update, classic):
        #### Version Update ####

        if (update == False):

            Static_Folder = self.config['daily_directory']['indir']
        else:
            Static_Folder=self.config['daily_directory']['tmpdir']
            pass
        if (self.typi == 'horton'):

            netcdf_temp = xr.open_mfdataset(Static_Folder + '*10m_u_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp = netcdf_temp.p0005.combine_first(netcdf_temp.p0001)
                netcdf_temp = netcdf_temp.rename('u10')

            netcdf_temp1 = xr.open_mfdataset(Static_Folder + '*10m_v_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp1 = netcdf_temp1.p0005.combine_first(netcdf_temp1.p0001)
                netcdf_temp1 = netcdf_temp1.rename('v10')

            netcdf_temp2 = xr.open_mfdataset(Static_Folder + '*l_u_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp2 = netcdf_temp2.p0005.combine_first(netcdf_temp2.p0001)
                netcdf_temp2 = netcdf_temp2.rename('u')
            netcdf_temp3 = xr.open_mfdataset(Static_Folder + '*l_v_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp3 = netcdf_temp3.p0005.combine_first(netcdf_temp3.p0001)
                netcdf_temp3 = netcdf_temp3.rename('v')
            netcdf_temp4 = xr.open_mfdataset(Static_Folder + '*total_precipitation*',combine='by_coords')
            if (classic == False):
                netcdf_temp4 = netcdf_temp4.p0005.combine_first(netcdf_temp4.p0001)
                netcdf_temp4 = netcdf_temp.rename('tp')
                 # self.xarray = netcdf_temp.to_dataset().merge(netcdf_temp1.to_dataset()).merge(netcdf_temp2.to_dataset()).merge(netcdf_temp3.to_dataset()).merge(netcdf_temp4.to_dataset())
            self.xarray = netcdf_temp.merge(netcdf_temp1).merge(netcdf_temp2).merge(netcdf_temp3).merge(
                    netcdf_temp4)

        if (self.typi == 'wang'):

            netcdf_temp = xr.open_mfdataset(Static_Folder + '*10m_u_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp = netcdf_temp.p0005.combine_first(netcdf_temp.p0001)
                netcdf_temp = netcdf_temp.rename('u10')


            netcdf_temp1 = xr.open_mfdataset(Static_Folder + '*10m_v_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp1 = netcdf_temp1.p0005.combine_first(netcdf_temp1.p0001)
                netcdf_temp1 = netcdf_temp1.rename('v10')


            netcdf_temp2 = xr.open_mfdataset(Static_Folder + '*total_precipitation*',combine='by_coords')

            if (classic == False):
                netcdf_temp1 = netcdf_temp2.p0005.combine_first(netcdf_temp2.p0001)
                netcdf_temp1 = netcdf_temp2.rename('tp')



            netcdf_temp3 = xr.open_mfdataset(Static_Folder + '*boundary_layer_height*nc',combine='by_coords')
            if (classic == False):
                netcdf_temp3 = netcdf_temp3.p0005.combine_first(netcdf_temp3.p0001)
                netcdf_temp3 = netcdf_temp3.rename('blh')


            #.merge(netcdf_temp3)
            self.xarrayblh=netcdf_temp3
            self.xarray = netcdf_temp.merge( netcdf_temp1.merge(netcdf_temp2))
        if (self.typi == 'huang'):
            # variables=['CAPE','CIN','Ventilation','blh','Precipitation']
            # self.config['daily_directory']['indir']
            #directory = '/media/tape/AlmacenDataset/'
            netcdf_temp = xr.open_mfdataset(Static_Folder + '*convective_available_potential_energy*',combine='by_coords')
            if (classic == False):
                netcdf_temp = netcdf_temp.p0005.combine_first(netcdf_temp.p0001)
                netcdf_temp = netcdf_temp.rename('cape')
            netcdf_temp1 = xr.open_mfdataset(Static_Folder + '*convective_inhibition*',combine='by_coords')
            if (classic == False):
                netcdf_temp1 = netcdf_temp1.p0005.combine_first(netcdf_temp1.p0001)
                netcdf_temp1 = netcdf_temp1.rename('cin')
            netcdf_temp2 = xr.open_mfdataset(Static_Folder + '*total_precipitation*',combine='by_coords')
            if (classic == False):
                netcdf_temp2 = netcdf_temp2.p0005.combine_first(netcdf_temp2.p0001)
                netcdf_temp2 = netcdf_temp2.rename('tp')

            #netcdf_temp3 = xr.open_mfdataset(Static_Folder + '4hourswithmario.nc')
            #if (classic == False):
            #    netcdf_temp3 = netcdf_temp3.p0005.combine_first(netcdf_temp3.p0001)
            #    netcdf_temp3 = netcdf_temp3.rename('blh')
            #desdoblar información
            netcdf_temp4 = xr.open_mfdataset(Static_Folder + '*boundary_layer_height*nc',combine='by_coords')
            if (classic == False):
                netcdf_temp4 = netcdf_temp4.p0005.combine_first(netcdf_temp4.p0001)
                netcdf_temp4 = netcdf_temp4.rename('blh')
            self.xarrayblh=netcdf_temp4
            self.xarray = netcdf_temp2
            self.xarray_aux=netcdf_temp.merge(netcdf_temp1)
            #.merge(netcdf_temp4))

        if (self.typi == 'all'):
            netcdf_temp = xr.open_mfdataset(Static_Folder + '*10m_u_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp = netcdf_temp.p0005.combine_first(netcdf_temp.p0001)
                netcdf_temp = netcdf_temp.rename('u10')
            netcdf_temp1 = xr.open_mfdataset(Static_Folder+ '*10m_v_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp1 = netcdf_temp1.p0005.combine_first(netcdf_temp1.p0001)
                netcdf_temp1 = netcdf_temp1.rename('v10')
            netcdf_temp2 = xr.open_mfdataset(Static_Folder + '*l_u_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp2 = netcdf_temp2.p0005.combine_first(netcdf_temp2.p0001)
                netcdf_temp2 = netcdf_temp2.rename('u')
            netcdf_temp3 = xr.open_mfdataset(Static_Folder + '*l_v_component_of_wind*',combine='by_coords')
            if (classic == False):
                netcdf_temp3 = netcdf_temp3.p0005.combine_first(netcdf_temp3.p0001)
                netcdf_temp3 = netcdf_temp3.rename('v')
            netcdf_temp4 = xr.open_mfdataset(Static_Folder + '*total_precipitation*',combine='by_coords')
            if (classic == False):
                netcdf_temp4 = netcdf_temp4.p0005.combine_first(netcdf_temp4.p0001)
                netcdf_temp4 = netcdf_temp.rename('tp')
            netcdf_temp5 = xr.open_mfdataset(Static_Folder + '*boundary_layer_height*',combine='by_coords')
            if (classic == False):
                netcdf_temp5 = netcdf_temp3.p0005.combine_first(netcdf_temp5.p0001)
                netcdf_temp5 = netcdf_temp5.rename('blh')


            self.xarray = netcdf_temp.merge(netcdf_temp1.merge(
                    netcdf_temp2.merge(netcdf_temp3).merge(
                        netcdf_temp4.merge(netcdf_temp5))))



    def generate_Maps(self):
        """

        :param self:
        :param ASI:
        :return:
        """

        def discrete_cmap(N, base_cmap=None):
            """Create an N-bin discrete colormap from the specified input map"""

            # Note that if base_cmap is a string or None, you can simply do
            #    return plt.cm.get_cmap(base_cmap, N)
            # The following works for string, None, or a colormap instance:

            base = plt.cm.get_cmap(base_cmap)
            color_list = base(np.linspace(0, 1, N))
            cmap_name = base.name + str(N)
            return base.from_list(cmap_name, color_list, N)

        def plot_colormesh_2(path_out, lon, lat, data0, title, units, no_discrete_bins):
            '''
            Plot a color mesh of a given field
            '''

            lon_corrected = np.copy(lon)
            lat_corrected = np.copy(lat)
            delta_lon = lon_corrected[1] - lon_corrected[0]
            delta_lat = lat_corrected[1] - lat_corrected[0]
            for xxx in range(0, len(lon_corrected)):
                lon_corrected[xxx] = lon_corrected[xxx] - delta_lon / 2.
            for yyy in range(0, len(lat_corrected)):
                lat_corrected[yyy] = lat_corrected[yyy] - delta_lat / 2.

            plt.figure(figsize=(8, 10))
            # map projection
            ax = plt.axes(projection=ccrs.PlateCarree())

            cmap = discrete_cmap(no_discrete_bins, 'rainbow')
            cmap.set_bad('w', 1.)
            cont = ax.pcolormesh(lon_corrected, lat_corrected, data0, vmin=0, vmax=100,
                                 transform=ccrs.PlateCarree(),
                                 cmap=cmap)

            # Set plotting area and draw coastlines
            # (Check aspect of plot for whole globe if it doesn't look good)
            # ax.set_global()
            # ax.set_extent([-15, 45, 25, 80])
            ax.set_extent([-13, 33.5, 33.5, 71])
            # ax.set_extent([-60, 60, 19.5, 85.5])
            # ax.add_feature(cart.feature.COASTLINE)
            land_10m = cart.feature.NaturalEarthFeature('physical', 'ocean', '50m')
            ax.add_feature(land_10m, zorder=100, color='w', edgecolor='k', facecolor='w')
            ax.add_feature(cart.feature.BORDERS, linestyle='-', alpha=.5)
            # ax.coastlines(resolution='50m', color='black', linewidth=1)

            # ------------------------------------------------------------
            # Colour bar and title
            cbar = plt.colorbar(cont, orientation='horizontal', pad=0.02)
            cbar.ax.tick_params(labelsize=15)
            cbar.set_label(units, fontsize=22)
            plt.title(title, fontsize=22)



            plt.savefig(path_out, dpi=300, format='png', bbox_inches='tight')

            # Close & clear the figures to save memory
            plt.close()
            plt.clf()




        import pandas
        import pandas as pd

         # Get structure of variables (including their attributes & values)
        NETCDF_FILES_FOLDER='/var/www/stream/stream/data/'
        self.read_Matrix_Django('/var/www/stream/stream/data/')
        # read_Matrix_Django()
        self.generate_Mask_Ocean('/var/www/stream/stream/data/')
        #### desde django me tendrÃ¡ que enviar los NETCDF_FILES_FOLDER

        indices_maps = {'wang': 'ASI_Wang_2017', 'horton': 'ASI_Horton_2012', 'huang': 'ASI_Huang_2018'}


        #saveFile = self.xarray[indices_maps[str(self.typi)]].where(self.xarray.mask == 1)


        lon_var = self.xarray.variables['longitude']
        lat_var = self.xarray.variables['latitude']
        time_var = self.xarray.variables['time']
        variable_var = self.xarray.variables[indices_maps[str(self.typi)]].where(self.xarray.mask == 1)

        lon_var = lon_var[:]
        lat_var = lat_var[:]
        variable_var = variable_var[:]
        for option_year in np.arange(1979, 2021, 1):
            for option_month in np.arange(1, 13):
                time_list_datetime = pd.DatetimeIndex(time_var[:].values)
                # time_list_date      = time_list_datetime.date()
                time_list_year = time_list_datetime.year
                time_list_month = time_list_datetime.month

                boolean_work = (time_list_year == option_year) * (time_list_month == option_month)
                array_plot = np.divide(np.sum(variable_var[boolean_work, :, :], axis=0),
                                       float(np.sum(boolean_work))) * 100.

                plot_colormesh_2(self.config['daily_directory']['maps_outdir'] + str(option_year) + "_" + str(
                    option_month) + '_' + str(indices_maps[str(self.typi)])  + '.png', lon_var, lat_var,
                                 array_plot,
                                 str(indices_maps[str(self.typi)]) + ' (' + calendar.month_name[option_month] + ' ' + str(option_year) + ')',
                                 '%', 10)



#    def execute(self):
#        print(" TYPI " + str(self.typi))
#        print(" TYPI " + str(self.conquer))
#        if (self.conquer == 'create'):
#            print("Se estÃ¡ creando")
#            data.Read_netcdf(False, False)

#            print(self.xarray)
#        elif (self.conquer == 'update'):
#            print("Se estÃ¡ actualizando")
            # data.Read_netcdf(True,False)
            # data.Updated_matrix()
            # data.Merge()
#        elif (self.conquer == 'maps'):




#            print("Se estÃ¡ generando mapas" + indices_maps[self.typi])

            # data.Updated_matrix()
#        elif (self.conquer == 'django'):
#            print("-------------------")


#data = ASI('horton', 'django')
#data.Read_netcdf(False, True)
#print("---- Avanza ----")
#data.Horton_generation()
# data.Huang_generation()
#print("Inicia")
#data = ASI('wang', 'django')
#data.Read_netcdf(False, True)
#print("---- Avanza ----")
#data.Wang_generation()

#data2 = ASI('huang', 'django')
#data2.Read_netcdf(False,True)
#data.Read_netcdf(False,True)
#data2.Huang_generation()
############## ------------------------- #################
#data.Huang_generation()
# data.read_Matrix_parameters_Django('/pool/pool4/steady/')
# data.refresh_Graphics_CSV('/pool/pool4/steady/',"1999-11-20","2001-3-20", -40,100,-24,100)
# data.refresh_Graphics('/pool/pool4/steady/',"1999-11-20","2001-3-20", -40,100,-24,100)
# data.execute()

# data.Merge()
# print("OK")
# data.printDescription()

# data.Updated_matrix()
# data.Merge()
# data.Wang_generation()

# data.generate_Maps('ASI_Wang_2017')
