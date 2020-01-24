#------------------------------------------------------------------------------
#----- PRMSWrapper.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2014 WiM - USGS

#    authors:  Jeremy K. Newson USGS Wisconsin Internet Mapping
# 
#   purpose:  wraps and updates Iowa StreamEst PRMS models
#          
#discussion:  
#

#region "Comments"
#07.18.2010 jkn - Created
#endregion

#region "Imports"
import glob, sys
import csv
import string
import datetime
from datetime import datetime as dt
import traceback
import os
from datetime import date, timedelta

import subprocess

import vos.serviceAgents.ServiceAgent

import array
import arcpy

import logging
#endregion

class PRMSWrapper(object):
    """   """
   #region "Constructor"
    def __init__(self,startDate, endDate, PRMSdirectory, overwrite = False):

        self.__StartDate__ = startDate
        self.__EndDate__ = endDate
        self.__Directory__ = PRMSdirectory
        self.__DoReplace__ = overwrite

        logdir = os.path.join(PRMSdirectory, 'prmswrapper.log')
        logging.basicConfig(filename=logdir, format ='%(asctime)s %(message)s')

        self.__sm__("-+-+-+-+-+-+-+-+-+ NEW RUN -+-+-+-+-+-+-+-+-+", 0.82)
        self.__sm__("Execute Date: " + str(datetime.date.today()), 0.82)
        self.__sm__("Start Date: " + str(self.__StartDate__) +" End Date: " +str(self.__EndDate__), 0.82)
        self.__sm__("Overwrite: " + str(self.__DoReplace__), 0.82)
        self.__sm__("-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+", 0.82)

        self.__Models__ = self.__readFile__(self.__getFile__('model'))
        
   #endregion

    #region Methods
    def Load(self, model):
        isOK = False
        try:
            isOK = self.__updateDataFile__(model)
            if(not isOK):return isOK
            isOK = self.__updateControlFile__(model)
            if(not isOK):return isOK

            return isOK
        except:
            self.__sm__("load exception thrown " + model, 0.67, 'ERROR')
            return False

    def Execute(self, model):
        try:
            appPath = self.__getFile__('app')
            app = os.path.basename(appPath)
            workingDir = self.__getSubFolder__('appWorkDirectory')
            controlFile = self.__getFile__('control')  
            
            self.__sm__("executing " + model + 'prms model', 0.151)  
            self.__executeModel__([app, controlFile.format(model)], appPath, workingDir.format(model))                
            return True
        except:
            tb = traceback.format_exc()
            self.__sm__("execution exception thrown " + model + ' ' + tb, 0.82, 'ERROR')
            
            return False
    
    def UpdateReachTables(self, model):
        try:
            self.__updateNseg__(self.__getFile__('nseg').format(model),self.__getFile__('reachTable').format(model))          
                                    
            self.__sm__("finished updating segment file " + model, 0.95)  
            return True
        except:
            tb = traceback.format_exc()                        
            self.__sm__("reach table exception thrown " + model + ' ' + tb, 0.99, 'ERROR')  
            return False
    
    def Run(self):
        try:
            self.__sm__("prmsWrap running", 0.98)
            for model in self.__Models__:  
                self.__runModel__(model)
            #next model
        except:
            tb = traceback.format_exc()
            self.__sm__("prmsWrap error caught " + tb, 0.104, 'ERROR')
       
    def RunAsync(self):
        threadList = []
        for model in self.__Models__:  
            t =Thread(target = self.__runModel__, args =[model])
            threadList.append(t)
        #next model  
           
        #start threads
        map(lambda t: t.start(), threadList)
        #Join threads
        map(lambda t: t.Join(), threadList)           
    #endregion

    #region Helper Methods
    def __runModel__(self, model):
        self.Load(model)
        self.Execute(model) 
        self.UpdateReachTables(model)

    def __updateDataFile__(self, model):
        
            self.__sm__('updating ' + model + ' data file', 0.133)
            #get stations files to update
            rstationCount = 0
            cstationCount = 0
            try:                
                #check to datafile's last entry is ok to proceed
                lastRow = self.__getFileTail__(self.__getFile__('data').format(model)).split(' ')[0:3]
                dfNextDate = datetime.date(int(lastRow[0]),int(lastRow[1]),int(lastRow[2]))+ datetime.timedelta(days=1)
                if (dfNextDate > self.__EndDate__): 
                    self.__sm__("data file end date (" + str(dfNextDate) +") exceeds end date " + str(self.__EndDate__) + "Exiting data file update", 0.134)
                    return False

                if (dfNextDate != self.__StartDate__ and dfNextDate <= self.__EndDate__):
                    self.__StartDate__ = dfNextDate
                    self.__sm__("mismatch of start time and datafile last row. Changing start time " + str(self.__StartDate__), 0.134)

                usgsAgent = vos.serviceAgents.ServiceAgent.USGSServiceAgent()
                runoff =[]                
                self.__sm__("getting " + model + "'s runoff stations", 0.136)
                for station in self.__readFile__(self.__getFile__("runoff").format(model)):
                    runoff.append(usgsAgent.GetDischargeDVSeries(station, self.__StartDate__, self.__EndDate__))
                    rstationCount += 1
                #next station

                sa = vos.serviceAgents.ServiceAgent.MesonetServiceAgent()
                climate = []
                self.__sm__("getting " + model + "'s climate stations", 0.156)
                for station in self.__readFile__(self.__getFile__("climate").format(model)):
                    climate.append(sa.GetClimateSeries(station, self.__StartDate__, self.__EndDate__))
                    cstationCount += 1
                #next station

                self.__appendData__(model, runoff, climate, rstationCount,cstationCount)

                return True
            except:
                tb = traceback.format_exc()
                self.__sm__("update Data File Error " + model + ' ' + tb, 0.151, 'ERROR')
                return False
    
    def __getFileTail__(self, file, tailcount =1):
        f = self.__readFile__(file)
        return f[tailcount*-1]
            
    def __updateControlFile__(self, model):
        """ updates control file """
        self.__sm__("updateing " + model + ' control file ', 0.153)
        f = self.__getFile__('control').format(model)
        cntlFile = self.__readFile__(f)

        try:
            yr = self.__EndDate__.strftime('%Y')
            mnth = self.__EndDate__.strftime('%m')
            dy = self.__EndDate__.strftime('%d')
            #find control files end date index
            enddateIndex = cntlFile.index('end_time')
            cntlFile[enddateIndex + 3] = yr
            cntlFile[enddateIndex + 4] = mnth
            cntlFile[enddateIndex + 5] = dy

            self.__writeToFile__(f, cntlFile)

            return True
        except:
            tb = traceback.format_exc()
            self.__sm__("failed to update control file " + model + ' ' + tb, 0.173, 'ERROR')
            return False    

    def __readFile__(self, file):
        f = None
        try:
            if (not os.path.isfile(file)):
                self.__sm__(file +" does not exist. If this is an error, check path.", 0.178)
                return []
            f = open(file, 'r')
            return map(lambda s: s.strip(), f.readlines())
        except:
            tb = traceback.format_exc()
            self.__sm__("file " + file + ' failed' + tb, 0.180, 'ERROR')
        finally:
            if not f == None:
                if not f.closed :
                    f.close();

    def __appendLineToFile__(self, file, content):
        f = None
        try:
            f = open(file, "a")
            f.write(string.lower(content + '\n'))
        except:
            self.__sm__("file  " + file + ' Failed to write', 0.189, 'ERROR')

        finally:
            if not f == None or not f.closed :
                f.close();

    def __writeToFile__(self, file, content):
        f = None
        try:
            f = open(file, "w")
            f.writelines(map(lambda x:x+'\n', content))
        except:
            self.__sm__("file " + file + ' failed to write', 0.201, 'ERROR')
        finally:
            if not f == None or not f.closed :
                f.close();
    
    def __getSubFolder__(self, fileType):
        path =""
        if fileType in ('model','reachTable'): path ="prms3.0.5_win"
        elif fileType in ('control'): path ="prms3.0.5_win\\projects\\{0}\\control"
        elif fileType in ('data','params','climate','runoff'): path ="prms3.0.5_win\\projects\\{0}\\input"
        elif fileType in ('nseg'): path ="prms3.0.5_win\\projects\\{0}\\output"
        elif fileType in ('app'): path ="prms3.0.5_win\\bin"
        elif fileType in ('appWorkDirectory'): path ="prms3.0.5_win\\projects\\{0}"

        else: return ""
                
        return os.path.join(self.__Directory__,path)

    def __getFile__(self, fileType):
        file =""
        if fileType in ('model'): file = "models.txt"
        elif fileType in ('climate'): file ="{0}.weather"
        elif fileType in ('runoff'): file ="{0}.runoff"
        elif fileType in ('control'): file ="{0}.control"
        elif fileType in ('data'): file ="{0}.data"
        elif fileType in ('params'): file ="{0}.params"
        elif fileType in ('nseg'): file ="animation.out.nsegment"
        elif fileType in ('reachTable'): file = "reachseg.gdb\\{0}"
        elif fileType in ('app'): file = "prms.exe"
        else: file = ""     
        
        path = self.__getSubFolder__(fileType)
        return os.path.join(path,file)

    def __appendData__(self, model, runoffDataToAppend, climateDatatoAppend, runoffStationCount, climateStationCount):
        try:
            daterange = lambda d1, d2: (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))         
            #http://stackoverflow.com/questions/9304408/how-to-add-an-integer-to-each-element-in-a-list
            for d in daterange(self.__StartDate__, self.__EndDate__):
                runofftempList = self.__dataList__('Discharge',d,runoffDataToAppend)
                hightempList = self.__dataList__('TempHigh', d, climateDatatoAppend)
                lowtempList = self.__dataList__('TempLow', d, climateDatatoAppend)
                precipList = self.__dataList__('Precip', d, climateDatatoAppend)

                if len(runofftempList) != runoffStationCount: runofftempList =['-999.0']* runoffStationCount
                if len(hightempList) != climateStationCount : hightempList =['-999.0']*climateStationCount
                if len(lowtempList) != climateStationCount : lowtempList =['-999.0']*climateStationCount
                if len(precipList) != climateStationCount : precipList =['-999.0']*climateStationCount
                            
                line = ' '.join(runofftempList+precipList + hightempList + lowtempList)
                line = d.strftime('%Y') + " " + d.strftime('%m') + " " + d.strftime('%d') + ' 0 0 0 ' + line

                self.__appendLineToFile__(self.__getFile__('data').format(model), line)

            return True
        except:
            tb = traceback.format_exc()
            self.__sm__("update Data File Error " + model + ' ' + tb, 0.256, 'ERROR')
            return False

    def __dataList__(self, property, d, dataList):
        list=[]
        key = d.strftime('%Y')+"/"+d.strftime('%m')+"/"+d.strftime('%d')
        for dl in dataList:
            list.append(getattr(dl[key],property))

        return list
    
    def __updateNseg__(self, segfile, segTable):
        
        rows = None
        can_continue = False
        try:
            yr = self.__StartDate__.strftime('%Y')
            mnth = self.__StartDate__.strftime('%m')
            dy = self.__StartDate__.strftime('%d')
            substring = yr+'-'+mnth+'-'+dy+':00:00:00'
            
            if self.__DoReplace__:
                can_continue = True
                self.__sm__("deleting all rows", 0.275)  
                arcpy.TruncateTable_management(segTable)
                

            with open(segfile, 'r') as file:
                self.__sm__("rebuilding table", 0.278) 
                rows = arcpy.da.InsertCursor(segTable, ("nsegment","segment_cf","tstamp_str","tstamp"))
                for _ in range(9):
                    next(file)
                for r in file:
                    if not can_continue:
                        if not substring in r:
                            continue
                        else:
                            can_continue = True
                    rowValues = r.split()
                    tstampValues = rowValues[0].split(':')[0].split('-')

                    yr = int(tstampValues[0])
                    mo = int(tstampValues[1])
                    da = int(tstampValues[2])
    
                    tstamp_str = str(mo)+'/'+str(da)+'/'+str(yr)
                    nsegment = rowValues[1]
                    segment_cf = rowValues[2]
    
                    dt = datetime.datetime(yr,mo,da)
    
                    rows.insertRow((nsegment,segment_cf,tstamp_str,dt))
                #next r
            self.__sm__("finished rebuilding table", 0.298) 
        except:
            tb = traceback.format_exc()
            self.__sm__("seg file error thrown " + tb, 0.300, 'ERROR')             
            return False
        finally:
            #Delete cursor object
            del rows

    def __index_containing_substring__(self, the_list, substring):
        for i, s in enumerate(the_list):
            if substring in s:
                  return i
        return -1
            
    def __executeModel__(self, cmd, exe, workDir):
        try:
            process = subprocess.Popen(cmd, executable = exe, cwd = workDir)
            process.wait()
        except:
            tb = traceback.format_exc()
            self.__sm__("failed to execute model" + tb,0.338,'ERROR') 
          
    def __sm__(self, msg, id, type = ''):
        print(type +' ' + str(id) + ' ' + msg)

        if type in ('ERROR'): logging.error(str(id) +' ' + msg)
        else : logging.info(str(id) + ' ' + msg)
    #endregion
    
##-------1---------2---------3---------4---------5---------6---------7---------8
##       Main
##-------+---------+---------+---------+---------+---------+---------+---------+

# Date time setup for use when updating the data files daily.
# now is set to the current date, yesterday is set to the previous day
class Main(object):

    def __init__(self):
        self.run()
    
    def run(self):
        today = datetime.date.today()
        yesterday= today - datetime.timedelta(days=1)

        startDate = yesterday #datetime.date(2016,05,13)
        endDate = yesterday

        prmswrap =  PRMSWrapper(startDate, endDate,r"D:\Projects\Data\IowaStreamEst\PRMS",False)

        prmswrap.Run()

# specifies that this class can be ran directly
if __name__ == '__main__':
    Main()
