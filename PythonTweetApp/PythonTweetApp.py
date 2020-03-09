
import json
import pandas as pd
import pyodbc
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pandas.io import sql
from sqlalchemy import create_engine
from pandas.io.json import json_normalize

#Declare variables that contains the user credentials to access Twitter API
#You can get your own keys in https://apps.twitter.com/
#--------------------------------------------------------------------------------
aToken =       "58971104-mppPOEdI8ofDFdFso6VHGkAhpSCGprvxGPJGiGtuJ"
aTokenSecret = "0dn4EbV9Tmtw2ZauNcC8mXzVNqRIm3iYwlzofNKgg23ch"
cKey =         "xC9Esilp15bh5LRpW472c5cvt"
cSecret =      "21H6DJydjdjosGPmwyZrHX0QgVXKfpUae04M9DTscRwGjg9lKR"

#Define after how many twitts we do a insert in the data base.
bufferSize = 5

#Defina an array to store the tweets readed from the stream api
twittsBuffer = []

#Define a connectiont to read-write to the Sql server Database 
#engine = create_engine("mssql+pyodbc://sa:master@SQLServer/TwitterDB?driver=SQL+Server+Native+Client+11.0")
engine = create_engine("mssql+pyodbc://sa:master@SQLServer")

#Define a function that receive a twitt by parameter and store it into the twittBuffer variable
#if the twittBuffer reach the buffersize defined lenght then call the function AddTwittsToDB that insert the twitts into
#the twittsBuffer array into the SQL Server database and clean the buffer
#--------------------------------------------------------------------------------
def AddTwittToBuffer(twitt):
    
    global twittsBuffer
    twittsBuffer.append(twitt)
    
    if (len(twittsBuffer) == bufferSize):
        AddTwittsToDB(twittsBuffer)
        twittsBuffer = []
    print(twitt['coordinates'] if twitt['coordinates']!= None else 'no coordinates')
    return 
#This function write the twitts stored in the variable twitBuffer to the SQL Database
#--------------------------------------------------------------------------------
def AddTwittsToDB(twitts):
    tData = {'id': [], 
             'text': [], 
             'screen_name': [], 
             'created_at': [],
             'retweet_count': [], 
             'favorite_count': [],
             'friends_count': [], 
             'followers_count': [], 
             'lang':[], 
             'country':[], 
             'latitude':[], 
             'lontitude':[]}
    
    for t in twitts:
        tData['id'].append(t['id'])
        tData['text'].append(t['text'])
        tData['screen_name'].append(t['user']['screen_name'])
        tData['created_at'].append(t['created_at'])
        tData['retweet_count'].append(t['retweet_count'])
        tData['favorite_count'].append(t['favorite_count'])
        tData['friends_count'].append(t['user']['friends_count'])
        tData['followers_count'].append(t['user']['followers_count'])
        tData['lang'].append(t['lang'])
        if t['place'] != None :
            tData['country'].append(t['place']['country'])
        else :
            tData['country'].append(None)
        
        if t['coordinates'] != None :
            tData['lontitude'].append(t['coordinates']['coordinates'][0])
            tData['latitude'].append(t['coordinates']['coordinates'][1])
        else :
            tData['lontitude'].append(None)
            tData['latitude'].append(None)
    tweets = pd.DataFrame(tData)
    tweets.set_index('id', inplace=True)
    tweets.to_sql("Tweets",engine,None,if_exists='append')
    return True
#--------------------------------------------------------------------------------
#Create a listener class that process received tweets
#On error print status
#--------------------------------------------------------------------------------
class StdOutListener(StreamListener):
    def on_data(self, data):
        t= json.loads(data)
        AddTwittToBuffer(t)
        return True
    def on_error(self, status):
        print(status)
#--------------------------------------------------------------------------------
#Define a main function, the entry point of the program
if __name__ == '__main__':
    #This object handles Twitter authetification and the connection to Twitter Streaming API
    myListener = StdOutListener()
    authenticator = OAuthHandler(cKey, cSecret)
    authenticator.set_access_token(aToken, aTokenSecret)
    stream = Stream(authenticator, myListener)
    #This line filter Twitter Streams to capture data tweets with the included text: 'Microsoft' or 'SolidQ' or 'Visual Studio'
    #stream.filter(track=['PowerBI', 'Tableau', 'Qlikview','Microstrategy','Pyramid Analytics','Business Objects', 'Ibm cognos'])
    stream.filter(track=['Aragua', '#AraguaenLlamas', 'Agua','#sinluz','electricidad','calle', 'oscuridad'])
#--------------------------------------------------------------------------------