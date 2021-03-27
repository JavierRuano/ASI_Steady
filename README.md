# ASI_Steady
Python program using xarray for calculating Air stagnation indices over the Euro-Mediterranean region
http://steady-ucm.org for details


Welcome to the ASI_Steady wiki! http://147.96.21.169/

![image](https://user-images.githubusercontent.com/34353851/112707260-55ccc600-8eaa-11eb-9088-98c382bb3d87.png)

This project is the code from website http://steady-ucm.org


UML

## Diagram
![image](https://user-images.githubusercontent.com/34353851/112707154-6df01580-8ea9-11eb-8eb5-122914afef1c.png)

## Data download
![image](https://user-images.githubusercontent.com/34353851/112707179-a2fc6800-8ea9-11eb-8040-4adcc17d8a68.png)

### 1
![image](https://user-images.githubusercontent.com/34353851/112707189-c9ba9e80-8ea9-11eb-9710-384fef53bc3c.png)

### 2
![image](https://user-images.githubusercontent.com/34353851/112707217-0eded080-8eaa-11eb-994c-ff96b6eba1cf.png)

## Class Diagram

![image](https://user-images.githubusercontent.com/34353851/112707230-23bb6400-8eaa-11eb-92dd-983bb9a319d7.png)

Las tres  funciones principales que se usarán en el cálculo de los índices de estancamiento son, conforme a la ilustración 1:
1.	def module_Wind(self,Wind,ml)
2.	def module_Precipitation(self)
3.	def ventilation_reduce_dimension(self)

La lectura de los ficheros netcdf se realiza mediante la función:
1.	def Read_netcdf(self, update, classic):


Los tres índices se calcularán mediante las funciones:
1.	def Horton_generation(self, update=False):
2.	def Wang_generation(self, update=False):
3.	def Huang_generation(self, update=False):

Funciones necesarias para la escritura de ficheros netcdf:
1.	def Add_Description_dataset(self, dataset_stream):

2.	def get_timestamp(self):
	
Una vez ya creados los índices de estancamiento, se pueden leer mediante las siguientes funciones:
def refresh_Graphics(self, NETCDF_FILES_FOLDER, start, end, lat_uno, lat_dos, lng_uno, lng_dos):

def refresh_Graphics_CSV(self, NETCDF_FILES_FOLDER, start, end, lat_uno, lat_dos, lng_uno, lng_dos):

## Server Diagram
### 1
![image](https://user-images.githubusercontent.com/34353851/112707327-cb389680-8eaa-11eb-8226-d5c4b756b4f8.png)

### 2
![image](https://user-images.githubusercontent.com/34353851/112707343-e99e9200-8eaa-11eb-8207-9bbf83d3b793.png)

### 3
![image](https://user-images.githubusercontent.com/34353851/112707354-01761600-8eab-11eb-8aea-1cf8e06ce9c3.png)



https://www.gnu.org/licenses/gpl-3.0.html
