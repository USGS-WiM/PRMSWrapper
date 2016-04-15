#------------------------------------------------------------------------------
#----- Timeseries.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2014 WiM - USGS

#    authors:  Jeremy K. Newson USGS Wisconsin Internet Mapping
# 
#   purpose:  Data retrieval code for Iowa StreamEst PRMS model
#          
#discussion:  
#

#region "Comments"
#07.17.2010 jkn - Created
#endregion

#region "Imports"

#endregion
class ts (object):
    def __init__(self, sid, date):
        self.Date = date
        self.StationID = sid

class Climatets(ts):
    """ Climatets extends ts by including high Temp, low temp, and precip properties  """

    def __init__(self,sid, date, tempHigh, tempLow, precip):
        ts.__init__(self,sid, date)
        self.Precip = precip
        self.TempHigh = tempHigh
        self.TempLow = tempLow

class StreamGagets (ts):
     def __init__(self,sid, date, discharge):
        ts.__init__(self,sid, date)
        self.Discharge = discharge

class PET(ts):
     def __init__(self,sid, date, solarRad, pet):
        ts.__init__(self,sid, date)
        self.SolarRadiation = solarRad
        self.PET = pet
 





