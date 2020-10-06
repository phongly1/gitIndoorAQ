import geopy
import pandas as pd
from geopy.distance import lonlat, distance
from numpy import average
from purpleair.network import SensorList
from purpleair.sensor import Sensor
from datetime import datetime
from datetime import timedelta

def findSensor(sensorName, p):
       print(sensorName)
       df = p.to_dataframe(sensor_filter='all',channel='parent')
       sensLoc = df.loc[df['name'] == sensorName]
       lat = sensLoc.at[sensLoc.index.values[0], 'lat']
       lon = sensLoc.at[sensLoc.index.values[0], 'lon']

       senseID = int(sensLoc.index.values[0])
       se = Sensor(senseID) #gives us a dataframe based on the sensor ID
       cummies = se.parent.get_historical(weeks_to_get=1, thingspeak_field='primary')

       totalRows = len(cummies[['created_at']])
       lonCol = []
       latCol = []
       n = 0
       while n < totalRows:
              lonCol.append(lon)
              latCol.append(lat)
              n = n + 1
       lonCol = pd.DataFrame(lonCol,columns=["lon"], index=cummies.index)
       latCol = pd.DataFrame(latCol,columns=["lat"], index=cummies.index)
       ''' IF YOU ADD MORE COLUMNS, FIX THAT IN PROCEEDING FUNCTION THAT ARE GENERATING NEW DATAFRAMES BY COLUMNS '''
       outcummies = pd.concat([cummies[['created_at']],cummies[['PM2.5_CF_ATM_ug/m3']], cummies[['Temperature_F']], cummies[['Humidity_%']], lonCol[["lon"]], latCol[["lat"]]], axis=1)

       # for each in cummies.columns.values:
       #         print(each)


       # for each in cummies.index.values:
       #        print(, ,  , )

       # avg = sensLoc.at[sensLoc.index.values[0],'10min_avg'] #10 min avg of pm 2.5
       # tim = sensLoc.at[sensLoc.index.values[0], 'created']
       # conc = sensLoc.at[sensLoc.index.values[0],'p_2_5_um'] #instantaneous reading of pm 2.5
       # tmpF = sensLoc.at[sensLoc.index.values[0],'temp_f']
       # hum = sensLoc.at[sensLoc.index.values[0],'humidity']
       # pres = sensLoc.at[sensLoc.index.values[0],'pressure']
       return outcummies

def sensorDictCalcWeight(sensorDataFramesDict, location, bigPappaPeePee = 2.0):


       geoDist = []
       denomSummies = 0.0

       # Calculate the geodesic distance in miles using the parsed long and lat from purpleair.
       for each in sensorDataFramesDict:
              df = pd.DataFrame(sensorDataFramesDict[each])
              newLonLat = (df.at[df.index.values[0], "lon"], df.at[df.index.values[0], "lat"])
              ourLonLat = (location.longitude, location.latitude) # based off of delivered address
              daddyDist = (distance(lonlat(*newLonLat), lonlat(*ourLonLat)).miles)
              denomSummies = denomSummies + (1 / (daddyDist ** bigPappaPeePee))
              geoDist.append(daddyDist)

       weights = []
       wsum = 0 # should sum to 1.0
       for d in geoDist:
              w = (1 / (d ** bigPappaPeePee)) / denomSummies
              wsum = w + wsum
              weights.append(w)

       #if wsum != 1.0:
              #raise Exception("Warning: Weights did not sum to 1.0, wtf???")

       # Prints the long, lat and weights. To be inspected by users
       for each in sensorDataFramesDict:
              df = pd.DataFrame(sensorDataFramesDict[each])
              print(df.at[df.index.values[0], "lon"], df.at[df.index.values[0], "lat"], weights[each])

       for each in sensorDataFramesDict:
              # Extract dataframe from dictionary with index key (something bt 0:6)
              df = pd.DataFrame(sensorDataFramesDict[each])
              # Extract calculated weights using the same index key (parity in indexes bt this dict and arr)
              newWeight = weights[each]

              # define total number of rows of extracted data frame
              totalRows = len(df[['created_at']])
              n = 0
              # initiate what will be the new dataframe column of weigths
              weightCol = []
              while n < totalRows:
                     # repeat the weights val in this arr for as many N's as there are rows
                     weightCol.append(newWeight)
                     n = n + 1

              # convert to fucking dataframe
              weightCol = pd.DataFrame(weightCol, columns=["weight"], index=df.index)

              '''change this if you ever query more data from purple air!'''
              df = pd.concat([df[['created_at', 'PM2.5_CF_ATM_ug/m3', 'Temperature_F', 'Humidity_%', 'lon', 'lat']],
                              weightCol[["weight"]]], axis=1)
              df["weighted"] = df["PM2.5_CF_ATM_ug/m3"] * df["weight"]
              sensorDataFramesDict[each] = df
       return sensorDataFramesDict

def everyThirtyMinutesDaddyHitsMyProstate(sensorDataFramesDict, COLUMN_FIELD = "PM2.5_CF_ATM_ug/m3"):
       dates = []
       for each in sensorDataFramesDict:
              df = sensorDataFramesDict[each]
              for date in df['created_at'].tolist():
                     if date not in dates:
                            dates.append(date)

       newDates = []
       currDate = min(dates)
       while currDate <= (max(dates) + timedelta(minutes=30)):
              newDates.append(currDate)
              currDate = currDate + timedelta(minutes=30)
       print(len(newDates))

       masterDataFrame = pd.DataFrame({"DateTime" : newDates})

       meanTimeLocation = {}
       N = 0
       for each in sensorDataFramesDict:
              df = sensorDataFramesDict[each]
              print(len(df.values))
              df.sort_values(by=['created_at'])
              storedMeans = []

              data25 = []
              currDate = 0
              nextDate = 1
              while nextDate < len(newDates):
                     for index, row in df.iterrows():
                            #print('new while step', currDate, nextDate)
                            #print('new row df step', row)
                            if (row['created_at'] >= newDates[currDate]) and (row['created_at'] < newDates[nextDate]):
                                   data25.append(row[COLUMN_FIELD])
                            else:
                                   if len(data25) > 0:
                                          #print(data25)
                                          storedMeans.append(((newDates[currDate]),average(data25)))
                                          currDate = nextDate
                                          nextDate = nextDate + 1
                                          data25 = []
                                   else:
                                          #print(data25)
                                          storedMeans.append(((newDates[currDate]),None))
                                          currDate = nextDate
                                          nextDate = nextDate + 1
                                          data25 = []
                                   if nextDate == len(newDates):
                                          break
                                   if (row['created_at'] >= newDates[currDate]) and (row['created_at'] < newDates[nextDate]):
                                          data25.append(row[COLUMN_FIELD])

              newLonLatCol = []
              lonlat = (df.at[df.index.values[0], "lon"], df.at[df.index.values[0], "lat"])
              n = 0
              while n < len(storedMeans):
                     newLonLatCol.append(lonlat)
                     n = n + 1

              partA = pd.DataFrame(storedMeans, columns=["DateTime",COLUMN_FIELD])
              partB = pd.DataFrame(newLonLatCol, columns=["lon","lat"])
              partB.index = partA.index
              partAB = pd.DataFrame()
              partAB["DateTime"] = partA["DateTime"]
              partAB[COLUMN_FIELD] = partA[COLUMN_FIELD]
              partAB["lon"] = partB["lon"]
              partAB["lat"] = partB["lat"]

              masterDataFrame = pd.merge(masterDataFrame, partAB, how='inner',left_index=True, right_index=True)
              print(len(storedMeans))
              meanTimeLocation[N] = masterDataFrame
              N = N + 1
              print(masterDataFrame)

address = "5717 Keith Ave, Oakland, CA"
sen = ["Ross@Chabot", "Blaburtings", "61st St. Oakland, 300 block @ Colby","back deck", "Treehouse - Outside", "Rockridge-Temescal", "Boyd Avenue, Oakland"]
#p = SensorList()
locator = geopy.Nominatim(user_agent="myGeocoder")
location = locator.geocode(address)
print("{}: Latitude = {}, Longitude = {}".format(address, location.latitude, location.longitude))


n = 0
sensorDataFramesDict = {}
p = SensorList()
for each in sen:
       sensorDataFramesDict[n] = findSensor(each, p)
       n = n + 1
sensorDataFramesDict = sensorDictCalcWeight(sensorDataFramesDict,location,bigPappaPeePee=.25)
#everyThirtyMinutesDaddyHitsMyProstate(sensorDataFramesDict)




#def grabdata(sensorID):
#       se = Sensor(sensorID)
#       df = se.parent.get_historical()
#s = Sensor(ID[1], parse_location = True)
#print(s)











































































# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# from purpleair.sensor import Sensor

#p = SensorList()  # Initialized 11,220 sensors!
# Other sensor filters include 'outside', 'useful', 'family', and 'no_child'
#df = p.to_dataframe(sensor_filter='all',
 #                   channel='parent')

#rint(df)
