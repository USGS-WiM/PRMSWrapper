#------------------------------------------------------------------------------
#----- ServiceAgent.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2014 WiM - USGS

#    authors:  Jeremy K. Newson USGS Wisconsin Internet Mapping
# 
#   purpose:  Data retrieval code for Iowa StreamEst PRMS model
#             Loads TS data
#          
#discussion:  
#

#region "Comments"
#07.18.2010 jkn - Created
#endregion

#region "Imports"
import glob, sys
import csv
import requests
import datetime
import string
import traceback
from vos.TimeSeries import * 
import logging
import re

from datetime import date, timedelta
#endregion

class ServiceAgentBase(object):
    """ """
    #region Constructor
    def __init__(self,baseurl):
        self.BaseUrl = baseurl
    #endregion

    #region Methods
    def Execute(self, resource):
        try:
            url = self.BaseUrl + resource
            response = requests.get(url)
            res = re.split('\n',response.text.strip())
            return res 
        except requests.exceptions as e:
             if hasattr(e, 'reason'):
                self.__sm__("Error:, failed to reach a server " + e.reason.strerror, 1.54, 'ERROR')
                return ""

             elif hasattr(e, 'code'):
                self.__sm__("Error: server couldn't fullfill request " + e.code, 1.58, 'ERROR')
                return ''
        except:
            tb = traceback.format_exc()            
            self.__sm__("url exception failed " + resource + ' ' + tb, 1.60, 'ERROR')
            return ""    
    
    def indexMatching(self, seq, condition):
        for i,x in enumerate(seq):
            if condition(x):
                return i
        return -1

    def __sm__(self, msg, id, type = ''):
        print(type +' ' + str(id) + ' ' + msg)

        if type in ('ERROR'): logging.error(str(id) +' ' + msg)
        else : logging.info(str(id) + ' ' + msg)
        
    #endregion
#end class


class MesonetServiceAgent(ServiceAgentBase):
    """ """
   
    #region Constructor
    def __init__(self):
        ServiceAgentBase.__init__(self,"http://mesonet.agron.iastate.edu/")
    #endregion
    #region Methods
    def GetClimateSeries(self, prmsstationID, startDate, endDate):
        ts = {}
        tsdate = None
        tshigh = None
        tslow = None
        tsprecip = None   
        dayindex = -999
        highindex = -999
        lowindex = -999
        precipindex =-999
        try:   
            daterange = lambda d1, d2: (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))    
                         
            resource = "request/coop/dl.php?network={0}&station[]={1}&year1={2}&month1={3}&day1={4}&year2={5}&month2={6}&day2={7}"
            resource = resource + "&vars[]=high&vars[]=low&vars[]=precip&what=view&delim=comma&gis=no"
             
            resource =resource.format(self.__getNetwork__(prmsstationID),self.__getStationMessonetStationID__(prmsstationID),
                                      startDate.strftime('%Y'),startDate.strftime('%m'),startDate.strftime('%d'),
                                      endDate.strftime('%Y'),endDate.strftime('%m'),endDate.strftime('%d'))

            res = map(lambda s: s.strip().split(","), self.Execute(resource) ) 
            headers = res[0]
            if "day" in headers: dayindex = headers.index("day")
            if "high" in headers: highindex = headers.index("high")
            if "low" in headers: lowindex = headers.index("low")
            if "precip" in headers: precipindex = headers.index("precip")
            #remove header from results (easier to work with)
            res.pop(0)
            for d in daterange(startDate, endDate):
                try:
                    dt = d.strftime('%Y')+"/"+d.strftime('%m')+"/"+d.strftime('%d')
                    values = None
                    
                    for x in range(len(res)):
                        if dt in res[x]:
                            values = res[x]
                            #once found remove
                            res.pop(x)
                            break
                    
                    if values[dayindex] == d.strftime('%Y')+"/"+d.strftime('%m')+"/"+d.strftime('%d'):
                        tsdate = values[dayindex]
                    else: raise Exception("wrong Date")

                    if highindex != -999 or values[highindex] != " ":
                        tshigh =values[highindex]
                    else: tshigh = '-999'

                    if lowindex != -999 or values[lowindex] != " ":
                        tslow =values[lowindex]
                    else: tslow = '-999'

                    if precipindex != -999 or values[precipindex] != " ":
                        tsprecip =values[precipindex]
                    else: tsprecip = '-999'                                
                        
                    #next i
                except:
                    tb = traceback.format_exc()
                    tsdate = d.strftime('%Y')+"/"+d.strftime('%m')+"/"+d.strftime('%d')
                    tshigh = '-999'
                    tslow = '-999'
                    tsprecip = '-999' 

                ts[tsdate] = Climatets(prmsstationID, tsdate, tshigh, tslow, tsprecip)
            #next row            
        except:
            tb = traceback.format_exc()
            self.__sm__(prmsstationID +"climate failed "  + tb, 1.150, 'ERROR')

        return ts
    
    def GetPETSeries(self, stationID, startDate, endDate):
        ts={}
        tsdate = None
        tsSolRad = None
        tsPET = None

        daterange = lambda d1, d2: (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))    

        for d in daterange(startDate, endDate):
            resource = "http://mesonet.agron.iastate.edu/agclimate/hist/worker.php?timeType=daily&sts[]=A135879"
            resource = resource + "&vars[]=c80&vars[]=c70&startYear={0}&startMonth={1}&startDay={2}&endYear={0}&endMonth={1}&endDay={2}&delim=comma&lf=dos"

            resource =resource.format(stationID,d.strftime('%Y'),d.strftime('%m'),d.strftime('%d')) 
            res = self.Execute(resource)

            try:
                headers = res[5].strip().split(",")
                values = res[6].strip().split(",")

                for i in range(len(headers)):
                    header = headers[i].lower()
                    if header == "valid":
                        if values[i] == year+"-"+month+"-"+day:
                             tsdate = values[i]
                        else:
                             raise Exception("wrong Date")
                    elif header == "solar rad":
                        if values[i] != " ":
                            tsSolRad = values[i]
                        else:
                            tsSolRad ='-999'
                    elif header == "potential et":
                        if values[i] != " ":
                            tsPET = values[i]
                        else:
                            tsPET = '-999'
           
                # next i
            except:
                tb = traceback.format_exc()
                tsdate = d.strftime('%Y')+"/"+d.strftime('%m')+"/"+d.strftime('%d')
                tsSolRad = '-999'
                tsPET = '-999'
                 
            ts[tsdate] = PET(stationID, tsdate, tsSolRad,tsPET)
        #next d
        return ts
    #endregion

    #region Helper Methods
    def __getNetwork__(self, stationid):
        
        stcode = self.__getStateCode__(stationid)
        return stcode+"CLIMATE"

    def __getStationMessonetStationID__(self, stationid):

        stcode = self.__getStateCode__(stationid)
        id = stationid[2:6]
        return stcode+id
    
    def __getStateCode__(self, stationid):
        stcode = stationid[0:2]
       
        if(stcode == '13'): return "IA"
        elif(stcode == '11'): return "IL"
        elif(stcode == '47'): return "WI"
        elif(stcode == '21'): return "MN"
        elif(stcode == '23'): return "MO"
        else: 
            self.__sm__("invalid stcode " + stcode + ' ', 1.223, 'ERROR')
            raise BaseException("stcode invalid")
    #endregion

class USGSServiceAgent(ServiceAgentBase):
    """ """
   
    #region Constructor
    def __init__(self):
        ServiceAgentBase.__init__(self,"http://waterdata.usgs.gov/nwis/")
    #endregion
    #region Methods
    
    def GetDischargeDVSeries(self, usgsstationID, startDate, endDate):
        ts = {}
        tsdate = None
        Q = None
        try:   
            daterange = lambda d1, d2: (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))    
             #http://waterdata.usgs.gov/nwis/dv?cb_00060=on&format=rdb&site_no=05481650&referred_module=sw&period=&begin_date=2016-02-08&end_date=2016-02-09            
            resource = "dv?cb_00060=on&format=rdb&site_no={0}&referred_module=sw&period=&begin_date={1}&end_date={2}"
             
            resource =resource.format(usgsstationID,startDate,endDate)
        
            result2 = self.__removeHeader__(self.Execute(resource))

            res = map(lambda s: s.strip().split("\t"), result2 ) 
            headers = res[0]
            if "datetime" in headers: dayindex = headers.index("datetime")
            Qindex = self.indexMatching(headers, lambda x:x.endswith('_00060_00003'))
            
            #remove header from results (easier to work with)
            res.pop(0)
            for d in daterange(startDate, endDate):
                try:
                    dt = d.strftime('%Y')+"-"+d.strftime('%m')+"-"+d.strftime('%d')
                    values = None
                    
                    for x in range(len(res)):
                        if dt in res[x]:
                            values = res[x]
                            #once found remove
                            res.pop(x)
                            break
                    
                    if values[dayindex] == d.strftime('%Y')+"-"+d.strftime('%m')+"-"+d.strftime('%d'):
                        tsdate = d.strftime('%Y')+"/"+d.strftime('%m')+"/"+d.strftime('%d')
                    else: raise Exception("wrong Date")

                    if Qindex < 0 or values[Qindex] != " ":
                        Q =values[Qindex]
                    else: Q = '-999'                               
                        
                    #next i
                except:
                    tb = traceback.format_exc()
                    tsdate = d.strftime('%Y')+"/"+d.strftime('%m')+"/"+d.strftime('%d')
                    Q = '-999'

                ts[tsdate] = StreamGagets(usgsstationID, tsdate, Q)
            #next row            
        except:
            tb = traceback.format_exc()
            self.__sm__(usgsstationID +"read usgs file failed "  + tb, 2.294, 'ERROR')

        return ts

    #endregion

    #region Helper Methods
    def __removeHeader__(self, itemList):
        try:
            newList =[]
            for line in itemList:
                if not line.startswith('#'):
                    newList.append(line)
            
            return newList
        except:
            tb = traceback.format_exc()
            self.__sm__(usgsstationID +"remove header failed "  + tb, 1.321, 'ERROR')
    #endregion               



