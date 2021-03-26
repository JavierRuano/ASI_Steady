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

from django.shortcuts import render
from bootstrap_datepicker_plus import DatePickerInput
from .models import Indice
from io import BytesIO
import polls.ASI_Datase_RACKt as AS
import csv
import os
from django.http import HttpResponse, HttpResponseNotFound

import numpy as np
import glob
import xarray as xr
import numpy as np
import branca.colormap as cm
import matplotlib
from io import StringIO
# Create your views here.
from django.http import HttpResponse
from django.template import loader
from django.shortcuts import render, redirect, render_to_response
from django.template.loader import render_to_string
from django.http import HttpResponse
from django.template.loader import get_template
from django.template.context import RequestContext
import pandas as pd

from .models import IndexForm
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
#import cmocean as cm
from cmocean import cm
import cmocean
import cartopy.crs as ccrs
from cartopy.io import shapereader
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import matplotlib.pyplot as plt
from bootstrap_datepicker_plus import DateTimePickerInput
from django.views import generic
import json
from django.views.decorators.csrf import csrf_exempt
import os
from stream.set_folders import NETCDF_FILES_FOLDER, IMAGE_FILES_FOLDER
### Add to send to ASI_DATASET
ASI_TYPE={'A':'horton','B':'wang','C':'huang'}


from subprocess import check_output
from django.http import HttpResponse
from django.template import loader

@csrf_exempt
def ASI_refresh(request):
    np.seterr(divide='ignore', invalid='ignore')
    if request.method == 'POST':


        if(float(request.POST['lat2'])<=float(request.POST['lat1'])):
            lat_uno=float(request.POST['lat2'])
            lat_dos=float(request.POST['lat1'])
        else:
            lat_dos=float(request.POST['lat2'])
            lat_uno=float(request.POST['lat1'])

        if(float(request.POST['lng2'])<=float(request.POST['lng1'])):
            lng_uno=float(request.POST['lng2'])
            lng_dos=float(request.POST['lng1'])
        else:
            lng_dos=float(request.POST['lng2'])
            lng_uno=float(request.POST['lng1'])
        start=datetime.strptime(request.POST['date1'], '%d/%m/%Y').strftime("%Y-%m-%d")
        end=datetime.strptime(request.POST['date2'], '%d/%m/%Y').strftime("%Y-%m-%d")
        print("------ 0 ----")
        data=AS.ASI(ASI_TYPE[request.POST['ASI']],'django')
        print("------ 1 ----")

        output = StringIO()

        answer=data.refresh_Graphics(NETCDF_FILES_FOLDER,start,end,lat_uno,lat_dos, lng_uno,lng_dos)

        answer.to_csv(output,sep=',')

        print(answer)
        val=output.getvalue()
        output.close()
        #response = HttpResp
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="precipita.csv"'
        response.write(val)
       # dataframeCSV.to_csv(path_or_buf=response,sep=',')
        return response



@csrf_exempt
def ASI_Components_refresh(request):

    if request.method == 'POST':


        if(float(request.POST['lat2'])<=float(request.POST['lat1'])):
            lat_uno=float(request.POST['lat2'])
            lat_dos=float(request.POST['lat1'])
        else:
            lat_dos=float(request.POST['lat2'])
            lat_uno=float(request.POST['lat1'])

        if(float(request.POST['lng2'])<=float(request.POST['lng1'])):
            lng_uno=float(request.POST['lng2'])
            lng_dos=float(request.POST['lng1'])
        else:
            lng_dos=float(request.POST['lng2'])
            lng_uno=float(request.POST['lng1'])


        start=datetime.strptime(request.POST['date1'], '%d/%m/%Y').strftime("%Y-%m-%d")
        end=datetime.strptime(request.POST['date2'], '%d/%m/%Y').strftime("%Y-%m-%d")
        output = StringIO()

        data=AS.ASI(ASI_TYPE[request.POST['ASI']],'django')

        answer=data.refresh_Graphics_CSV(NETCDF_FILES_FOLDER,start,end,lat_uno,lat_dos, lng_uno,lng_dos)
        answer.to_csv(output,sep=',')
        print(answer)
        val=output.getvalue()
        output.close()
        #response = HttpResp
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="precipita.csv"'
        response.write(val)
       # dataframeCSV.to_csv(path_or_buf=response,sep=',')
        return response


def make_map(projection=ccrs.PlateCarree()):
    fig, ax = plt.subplots(figsize=(9, 13),
                           subplot_kw=dict(projection=projection))
    gl = ax.gridlines(draw_labels=True)
    gl.xlabels_top = gl.ylabels_right = False
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    return fig, ax


def index(request):


    form=IndexForm(request.POST)
    context={'conditional':False, 'mapa':None,'form':form,'x1':24.75,'x2':75,'x3':-20,'x4':40}

    return render(request,'polls/indexResponsive.html',context)
    #return render(request,'polls/index.html',context)



def MapProgres(request):
    # if this is a POST request we need to process the form data

    if request.method == 'POST':
        print(request.POST)
        print("Download" in request.POST)
        # create a form instance and populate it with data from the request:
        form = Indice(request.POST)
        # check whether it's valid:
        print("hola", form)
        if("Image" in request.POST):
            print("GO 2")
            context={}

            start=datetime.strptime(request.POST['start_date'], '%d/%m/%Y').strftime("%Y-%m-%d")
            end=datetime.strptime(request.POST['end_date'], '%d/%m/%Y').strftime("%Y-%m-%d")
            if(request.POST['elo']<request.POST['slo']):
                lat_uno=float(request.POST['elo'])
                lat_dos=float(request.POST['slo'])
            else:
                lat_dos=float(request.POST['elo'])
                lat_uno=float(request.POST['slo'])

            if(request.POST['el']<request.POST['sl']):
                lng_uno=float(request.POST['el'])
                lng_dos=float(request.POST['sl'])
            else:
                lng_dos=float(request.POST['el'])
                lng_uno=float(request.POST['sl'])
            data=AS.ASI(ASI_TYPE[request.POST['ASI']],'django')
            print("------ 1 ----")


            hashin=data.get_Photo_ASI(NETCDF_FILES_FOLDER,IMAGE_FILES_FOLDER ,start,end,lat_uno,lat_dos, lng_uno,lng_dos)
#Segunda parte
            answer=data.refresh_Graphics(NETCDF_FILES_FOLDER,start,end,lat_uno,lat_dos, lng_uno,lng_dos)


            #saveFile=ASIPercent
            #saveFile=saveFile[indices_maps[str(request.POST['ASI'])]].where((saveFile[indices_maps[str(request.POST['ASI'])]].latitude>=lat_uno) & (saveFile[indices_maps[str(request.POST['ASI'])]].latitude<=lat_dos) & (saveFile[indices_maps[str(request.POST['ASI'])]].longitude>=lng_uno) & (saveFile[indices_maps[str(request.POST['ASI'])]].longitude<=lng_dos),drop=True).sel(time=slice(start,end))



            try:
                #str(hashin)
                context={'conditional':True,'form': IndexForm(request.POST),'hashfile':str(hashin),'start_graph_year':start,'end_graph_year':end,'x1':lat_uno,'x2':lat_dos,'x3':lng_uno,'x4':lng_dos,'ASI':request.POST['ASI']}
            except:
                context={'form': IndexForm(request.POST)}
#            return render(request,'polls/LeaflefOverlay.hmtl',context)
#            return render(request,'polls/LeafletJavascrit.html',context)
                context={}
            #return render(request,'polls/ind.html',context)
            print("enviado-->")
            print(context)
            return render(request,'polls/indexResponsive.html',context)
            #print(request.POST)
            #return render(request, 'polls/name.html', {'form': form,'ASI':datetime.strptime(str(request.POST['year']+"-"+request.POST['Month']+"-"+"1"),'%Y-%m-%d')})

        else:
            if('Download' in request.POST):
                if(request.POST['elo']<=request.POST['slo']):
                    lat_uno=float(request.POST['elo'])
                    lat_dos=float(request.POST['slo'])
                else:
                    lat_dos=float(request.POST['elo'])
                    lat_uno=float(request.POST['slo'])

                if(request.POST['el']<=request.POST['sl']):
                    lng_uno=float(request.POST['el'])
                    lng_dos=float(request.POST['sl'])
                else:
                    lng_dos=float(request.POST['el'])
                    lng_uno=float(request.POST['sl'])

                #saveFile=xr.open_mfdataset(NETCDF_FILES_FOLDER +'horton_2012_2October2019.nc')
                start=datetime.strptime(request.POST['start_date'], '%d/%m/%Y').strftime("%Y-%m-%d")
                end=datetime.strptime(request.POST['end_date'], '%d/%m/%Y').strftime("%Y-%m-%d")

                data = AS.ASI(ASI_TYPE[request.POST['ASI']], 'django')
                ETCDF_FILES_FOLDER='/var/www/stream/stream/'
                saveFile=data.get_ASI_Netcdf(NETCDF_FILES_FOLDER,start,end,lat_uno,lat_dos, lng_uno,lng_dos)
                
                #saveFile=saveFile['ASI_HORTON_2012'].where((saveFile['ASI_HORTON_2012'].latitude>=lat_uno) & (saveFile['ASI_HORTON_2012'].latitude<=lat_dos) & (saveFile['ASI_HORTON_2012'].longitude>=lng_uno) & (saveFile['ASI_HORTON_2012'].longitude<=lng_dos),drop=True).sel(time=slice(start,end)).compute()
                saveFile.to_netcdf(NETCDF_FILES_FOLDER +'envio.nc')

                data = check_output(["cat",NETCDF_FILES_FOLDER +"envio.nc"])
                #data = check_output(["pwd"])

                os.remove(NETCDF_FILES_FOLDER +'envio.nc')

                response = HttpResponse(BytesIO(data),content_type='application/octet-stream')

                response['Content-Disposition'] = 'attachment; filename=streamUCM.nc'
                response.write(data)
                #print('C:\\Users\\strea\\source\\repos\\stream\\stream\\data\\netcdfDownload/streamUCM.nc')
                return response

      




