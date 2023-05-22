#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# MLB spec code for Getting the MLB Prop data and sending it to: 
# 1. Google Sheets (for Review)
# 2. SQL for Use 

# The intention is to use this as a roadmap for writing a version that is integrated to the Database
# 05/19/2023

# The Google Sheet for the data review is: 
# MLBGetProps

# https://docs.google.com/spreadsheets/d/1MvVT67sxzg2yBhHNMQXeRMxWQ4D4CnieMI9KfVoya-s/edit


# In[ ]:


#Packages
#Time
import time
from datetime import datetime, timedelta, timezone, date
from pytz import timezone as tz
#Set Timezone to Eastern time
ny = tz("America/New_York")
fmt = '%B %d, %Y %H:%M:%S'
timestart = datetime.now(ny)

import sys, os

#UrlLib
import urllib.request
from urllib.request import Request, urlopen
import json 

#Gspread
from gspread import authorize
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

#SQL
from os import environ
from sqlalchemy import create_engine

#Beatiful Soup
from bs4 import BeautifulSoup


#Pandas
from pandas import read_sql,DataFrame, read_html, read_json , merge, concat,to_numeric, set_option, json_normalize



#Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

#SQL
from os import environ
import sqlalchemy
from sqlalchemy import create_engine


import re
import requests

#For Sims:

from df2gspread import df2gspread as d2g
import warnings
import random
import re


# In[ ]:


#Get Dates to use. Will be used to hit the queries for today and tomorrow
#Time shifting is around the MLB schedule
today = datetime.now(ny) - timedelta(hours=5)
tomorrow = datetime.now(ny) + timedelta(hours=19)


# In[ ]:


#defining header to use on URL requests
header= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) ' 
      'AppleWebKit/537.11 (KHTML, like Gecko) '
      'Chrome/23.0.1271.64 Safari/537.11',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
      'Accept-Encoding': 'none',
      'Accept-Language': 'en-US,en;q=0.8',
      'Connection': 'keep-alive'}


# In[ ]:


#Setup Module to print output messages to Slack from Python
# Reference: https://www.august.com.au/blog/how-to-send-slack-alerts-from-google-sheets-apps-script/
# Reference: https://www.youtube.com/watch?v=lEQ68HhpO4g= 
# webhook= https://hooks.slack.com/services/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
#Provide Channel Webhook for:
#mlb_activity_log
webhook= 'https://hooks.slack.com/services/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'

#Setup Sendslack
def send_slack_message(message):
    payload = '{"text":"%s"}' % message
    response = requests.post('https://hooks.slack.com/services/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',
                            data=payload)
    print(message)

def main(argv):
    message=''
    try: opts, args = getopt.getopt(argv,"hm:",["message="])
    
    except getopt.GetoptError:
        print('SlackMessage.py -m <message>')
        sys.exit(2)
    if len(opts)== 0:
        message="No message"
    for opts, arg in opts:
        if opt == '-h':
            print('SlackMessage.py -m <message>')
            sys.exit()
        elif opt in ("-m", "--message"):
            message = arg
    send_slack_message(message)
    if __name__=="__main__":
        main(sys.argv[1:])


# In[ ]:


send_slack_message(':billed_cap:1. MLB Prop Setup started:'+timestart.strftime("%B %d, %Y %H:%M:%S"))


# In[ ]:


#Get MlB Q4 Player ids from SQL
try:
    engine = create_engine('mysql+pymysql://dailyrotodb:QvCPetyGry^201F90!@dailyrotodb.cpvxpuf3txsh.us-east-1.rds.amazonaws.com:3306/dailyrotodb')
    sql1 = '''SELECT * FROM MLB_Player_Q4;'''
    MlBPlayerQ4 = read_sql(sql1, engine)
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:2. Got Q4 Player Ids:'+timenow.strftime("%B %d, %Y %H:%M:%S"))
except:
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:2. Got Q4 Player Ids failed:'+timenow.strftime("%B %d, %Y %H:%M:%S"))


# In[ ]:


#Connect to Debug GSheet
try:
    link='https://api.sportradar.us/oddscomparison-player-props/production/v2/en/players/mappings.json?api_key=gkppjuantd4fnh54b4mnhzbf&start='
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope) 
    gc = authorize(credentials)
    timenow = datetime.now(ny)
    sheet = gc.open('MLBGetProps')
    Goal = sheet.worksheet("Scrape")
    Goal.update('b1', timestart.strftime("%B %d, %Y %H:%M:%S"))
    send_slack_message(':billed_cap:3. Connected to Google Debug Sheet:'+timenow.strftime("%B %d, %Y %H:%M:%S"))
except:
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:3. Failed to Connect to Google Debug Sheet:'+timenow.strftime("%B %d, %Y %H:%M:%S"))


# In[ ]:


#Build our Roster Map from SD and Q4:
try:
    print('Get Rosters from FantasyData')
    data2 = []
    #Get Rosters FantasyData
    with urlopen("https://api.sportsdata.io/v3/mlb/scores/json/Players?key=9080515fbdff4aa095490cadda5ecd30") as url:
        players = json.loads(url.read().decode())
    for player in players:
        row=[ player['PlayerID'], player['FirstName'] + ' '+ player['LastName'] , player['SportRadarPlayerID'], player['MLBAMID'] , player['Team'], player['Position'] ]
        data2.append(row)
    df2=DataFrame(data2)
    df2.columns = ["FantasyDataPlayerID", "Player_Name_FD", "SR_ID", "Player-MLB-ID", "FantasyDTeam", "Position"]
    #Merge
    print('Merge datasets')
    combined= df2.merge(MlBPlayerQ4, on='SR_ID')
    #Let's authorize us to work on that file
    timestart =  datetime.now()
    sheet = gc.open('MLBGetProps')

    #Let's push the map
    Goal = sheet.worksheet("SRMap")
    sheet.values_clear("SRMap!a1:m2000")

    set_with_dataframe(Goal, combined, row=1,col=1)
    timenow = datetime.now(ny)
    Goal.update('n1', timenow.strftime("%B %d, %Y %H:%M:%S"))
    

    #Output the MLB Player map (for actIve players on Rosters) to SQL:
    #Send to SQL
    combined.to_sql('MLBPlayerMap', engine, if_exists='replace')
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:4. Made Player ID map and send it to SQL as MLBPlayerMap:'+timenow.strftime("%B %d, %Y %H:%M:%S"))
except:
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:4. Failed to make Player ID map and send it to SQL as MLBPlayerMap::'+timenow.strftime("%B %d, %Y %H:%M:%S"))


# In[ ]:


#Let's Get the Game Table from SQL and add the Q4 Link for projections
try:
    q4link='https://api.quarter4.io/baseball/v2/events/'
    q4key='/performances?page=1&count=300&api_key=4206d78e-5d13-4981-8b88-0cc70fa8fc2d'
    sql2 = '''SELECT * FROM MLB_Game_IDs;'''
    MlBGames = read_sql(sql2, engine)
    MlBGames_today = MlBGames.loc[MlBGames['Day'] == today.date()] 
    MlBGames_tomorrow = MlBGames.loc[MlBGames['Day'] == tomorrow.date()] 
    MLBGamesAll=concat([MlBGames_today,MlBGames_tomorrow])
    MLBGamesAll['Q4link']=q4link+MLBGamesAll['Q4_ID']+q4key
    MLBGamesAll=MLBGamesAll.reset_index()
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:5.Setup Game Table:'+timenow.strftime("%B %d, %Y %H:%M:%S"))
except:
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:5. Failed to Setup Game Table:'+timenow.strftime("%B %d, %Y %H:%M:%S"))


# In[ ]:


#Print out the Games to the sheet
Goal = sheet.worksheet("MLB_Game_IDs")
sheet.values_clear("MLB_Game_IDs!a1:j2000")
set_with_dataframe(Goal,MlBGames, row=1,col=1)
timenow = datetime.now(ny)
Goal.update('k2', timenow.strftime("%B %d, %Y %H:%M:%S"))


# In[ ]:


#Get Q4 Projections by looping thru the Q4 game projection links and grabbing the appropriate fields
try:
    p=MLBGamesAll.shape[0]
    projs=[]
    for x in range(0,p):
        gamelink=MLBGamesAll['Q4link'][x]
        try:
            req = urllib.request.Request(url= gamelink, headers=header)
            with urlopen(req) as url:
                data = json.loads(url.read().decode())
            players=data['data']
            for player in players:
                #Player Info
                GameID=MLBGamesAll['GameID'][x]
                Day=MLBGamesAll['Day'][x]
                DateTime=MLBGamesAll['DateTime'][x]
                AwayTeam=MLBGamesAll['AwayTeam'][x]
                HomeTeam=MLBGamesAll['HomeTeam'][x]
                SR_GameID=MLBGamesAll['SR_GameID'][x]
                #Q4 Player ID
                Q4PlayerID= player['relationships']['player']['data']['id']
                Q4PlayerID= Q4PlayerID[21:57]
                #Hits Probabilities
                try:
                    Q4_Prob_0_Hits= player['attributes']['statisticsHittingOverallOnbaseHBins'][0]
                    Q4_Prob_0_to_1_Hits= Q4_Prob_0_Hits+player['attributes']['statisticsHittingOverallOnbaseHBins'][1]
                    Q4_Prob_0_to_2_Hits= Q4_Prob_0_to_1_Hits+player['attributes']['statisticsHittingOverallOnbaseHBins'][2]
                except:
                    Q4_Prob_0_Hits= ''
                    Q4_Prob_0_to_1_Hits= ''
                    Q4_Prob_0_to_2_Hits=''
                
                #Singles Probabilities
                try:
                    Q4_Prob_0_Singles= player['attributes']['statisticsHittingOverallOnbaseSBins'][0]
                    Q4_Prob_0_to_1_Singles= Q4_Prob_0_Singles+player['attributes']['statisticsHittingOverallOnbaseSBins'][1]
                except:
                    Q4_Prob_0_Singles= ''
                    Q4_Prob_0_to_1_Singles= ''
                
                #Doubles Probabilities
                try:
                    Q4_Prob_0_Doubles= player['attributes']['statisticsHittingOverallOnbaseDBins'][0]
                    Q4_Prob_0_to_1_Doubles= Q4_Prob_0_Doubles+player['attributes']['statisticsHittingOverallOnbaseDBins'][1]
                except:
                    Q4_Prob_0_Doubles= ''
                    Q4_Prob_0_to_1_Doubles= ''
                
                #Triples Probabilities
                try:
                    Q4_Prob_0_Triples= player['attributes']['statisticsHittingOverallOnbaseTBins'][0]
                    Q4_Prob_0_to_1_Triples= Q4_Prob_0_Triples+player['attributes']['statisticsHittingOverallOnbaseTBins'][1]
                except:
                    Q4_Prob_0_Triples= ''
                    Q4_Prob_0_to_1_Triples= ''
                
                #TotalBases Probabilities
                try:
                    Q4_Prob_0_TotalBases= player['attributes']['statisticsHittingOverallOnbaseTbBins'][0]
                    Q4_Prob_0_to_1_TotalBases= Q4_Prob_0_TotalBases+player['attributes']['statisticsHittingOverallOnbaseTbBins'][1]
                    Q4_Prob_0_to_2_TotalBases= Q4_Prob_0_to_1_TotalBases+player['attributes']['statisticsHittingOverallOnbaseTbBins'][2]
                except:
                    Q4_Prob_0_TotalBases= ''
                    Q4_Prob_0_to_1_TotalBases= ''
                    Q4_Prob_0_to_2_TotalBases= ''
                
                #Runs Probabilities
                try:
                    Q4_Prob_0_Runs= player['attributes']['statisticsHittingOverallRunsTotalBins'][0]
                    Q4_Prob_0_to_1_Runs= Q4_Prob_0_Runs+player['attributes']['statisticsHittingOverallRunsTotalBins'][1]
                except:
                    Q4_Prob_0_Runs= ''
                    Q4_Prob_0_to_1_Runs= ''
                
                #RBI Probabilities
                try:
                    Q4_Prob_0_RBI= player['attributes']['statisticsHittingOverallRbiBins'][0]
                    Q4_Prob_0_to_1_RBI= Q4_Prob_0_RBI+player['attributes']['statisticsHittingOverallRbiBins'][1]
                except:
                    Q4_Prob_0_RBI= ''
                    Q4_Prob_0_to_1_RBI= ''
                

                #Home Run (HR) Probabilities
                try:
                    Q4_Prob_0_HomeRun= player['attributes']['statisticsHittingOverallOnbaseHrBins'][0]
                    Q4_Prob_0_to_1_HomeRun= Q4_Prob_0_HomeRun+player['attributes']['statisticsHittingOverallOnbaseHrBins'][1]
                except:
                    Q4_Prob_0_HomeRun= ''
                    Q4_Prob_0_to_1_HomeRun= ''
                
                #Strikeout(K) Probabilities
                try:
                    Q4_Prob_0_K= player['attributes']['statisticsPitchingOverallOutsKtotalBins'][0]
                    Q4_Prob_0_to_1_K= Q4_Prob_0_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][1]
                    Q4_Prob_0_to_2_K= Q4_Prob_0_to_1_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][2]
                    Q4_Prob_0_to_3_K= Q4_Prob_0_to_2_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][3]
                    Q4_Prob_0_to_4_K= Q4_Prob_0_to_3_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][4]
                    Q4_Prob_0_to_5_K= Q4_Prob_0_to_4_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][5]
                    Q4_Prob_0_to_6_K= Q4_Prob_0_to_5_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][6]
                    Q4_Prob_0_to_7_K= Q4_Prob_0_to_6_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][7]
                    Q4_Prob_0_to_8_K= Q4_Prob_0_to_7_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][8]
                    Q4_Prob_0_to_9_K= Q4_Prob_0_to_8_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][9]
                    Q4_Prob_0_to_10_K= Q4_Prob_0_to_9_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][10]
                    Q4_Prob_0_to_11_K= Q4_Prob_0_to_10_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][11]
                    Q4_Prob_0_to_12_K= Q4_Prob_0_to_11_K + player['attributes']['statisticsPitchingOverallOutsKtotalBins'][12]

                except:
                    Q4_Prob_0_K= ''
                    Q4_Prob_0_to_1_K= ''
                    Q4_Prob_0_to_2_K= ''
                    Q4_Prob_0_to_3_K= ''
                    Q4_Prob_0_to_4_K= ''
                    Q4_Prob_0_to_5_K= ''
                    Q4_Prob_0_to_6_K= ''
                    Q4_Prob_0_to_7_K= ''
                    Q4_Prob_0_to_8_K= ''
                    Q4_Prob_0_to_9_K= ''
                    Q4_Prob_0_to_10_K= ''
                    Q4_Prob_0_to_11_K= ''
                    Q4_Prob_0_to_12_K= ''
                
                row=[gamelink, GameID, Day, DateTime, AwayTeam, HomeTeam, SR_GameID, Q4PlayerID, Q4_Prob_0_Hits, Q4_Prob_0_to_1_Hits, Q4_Prob_0_to_2_Hits, Q4_Prob_0_Singles, Q4_Prob_0_to_1_Singles, Q4_Prob_0_Doubles, Q4_Prob_0_to_1_Doubles, Q4_Prob_0_Triples, Q4_Prob_0_to_1_Triples, Q4_Prob_0_TotalBases, Q4_Prob_0_to_1_TotalBases, Q4_Prob_0_to_2_TotalBases, Q4_Prob_0_Runs, Q4_Prob_0_to_1_Runs, Q4_Prob_0_RBI, Q4_Prob_0_to_1_RBI, Q4_Prob_0_HomeRun, Q4_Prob_0_to_1_HomeRun, Q4_Prob_0_K, Q4_Prob_0_to_1_K, Q4_Prob_0_to_2_K, Q4_Prob_0_to_3_K, Q4_Prob_0_to_4_K, Q4_Prob_0_to_5_K, Q4_Prob_0_to_6_K, Q4_Prob_0_to_7_K, Q4_Prob_0_to_8_K, Q4_Prob_0_to_9_K, Q4_Prob_0_to_10_K, Q4_Prob_0_to_11_K, Q4_Prob_0_to_12_K]
                projs.append(row)
        except:
            dummy=1
    Q4projections=DataFrame(projs)


    Q4projections.columns = ['gamelink', 'GameID', 'Day', 'DateTime', 'AwayTeam', 'HomeTeam', 'SR_GameID', 'Q4_ID', 'Q4_Prob_0_Hits', 'Q4_Prob_0_to_1_Hits', 'Q4_Prob_0_to_2_Hits', 'Q4_Prob_0_Singles', 'Q4_Prob_0_to_1_Singles', 'Q4_Prob_0_Doubles', 'Q4_Prob_0_to_1_Doubles', 'Q4_Prob_0_Triples', 'Q4_Prob_0_to_1_Triples', 'Q4_Prob_0_TotalBases', 'Q4_Prob_0_to_1_TotalBases', 'Q4_Prob_0_to_2_TotalBases', 'Q4_Prob_0_Runs', 'Q4_Prob_0_to_1_Runs', 'Q4_Prob_0_RBI', 'Q4_Prob_0_to_1_RBI', 'Q4_Prob_0_HomeRun', 'Q4_Prob_0_to_1_HomeRun', 'Q4_Prob_0_K', 'Q4_Prob_0_to_1_K', 'Q4_Prob_0_to_2_K', 'Q4_Prob_0_to_3_K', 'Q4_Prob_0_to_4_K', 'Q4_Prob_0_to_5_K', 'Q4_Prob_0_to_6_K', 'Q4_Prob_0_to_7_K', 'Q4_Prob_0_to_8_K', 'Q4_Prob_0_to_9_K', 'Q4_Prob_0_to_10_K', 'Q4_Prob_0_to_11_K', 'Q4_Prob_0_to_12_K']

    Q4projectionsFinal=combined.merge(Q4projections, on='Q4_ID')
    Goal = sheet.worksheet("Q4Proj")
    sheet.values_clear("Q4Proj!a1:bg2000")
    set_with_dataframe(Goal,Q4projectionsFinal, row=1,col=1)
    timenow = datetime.now(ny)
    Goal.update('av2', timenow.strftime("%B %d, %Y %H:%M:%S"))
    send_slack_message(':billed_cap:6. Got Q4 Player Projections:'+timenow.strftime("%B %d, %Y %H:%M:%S"))
except:
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:6. Failed to Get Q4 Player Projections:'+timenow.strftime("%B %d, %Y %H:%M:%S"))


# In[ ]:


# 1. Get MLB Props from SportsData
# 2. Figure out the Bet Name Tag
# 3. Figure out the Implied Odds from the Book
# 4. Add the Q4 projected Odds
# 5. Edge
# 5. Stars
# 6. RecommendedBet
print('Get the props from SportsData' )
linkurl1= 'https://api.sportsdata.io/v3/mlb/odds/json/BettingEventsByDate/'+str(today.date()) +'?key=ab5ccb732f1d48febbdb890d300d656e'
linkurl2= 'https://api.sportsdata.io/v3/mlb/odds/json/BettingEventsByDate/'+str(tomorrow.date()) +'?key=ab5ccb732f1d48febbdb890d300d656e'
links = [linkurl1,linkurl2]

with urllib.request.urlopen("https://api.sportsdata.io/v3/mlb/scores/json/Players?key=9080515fbdff4aa095490cadda5ecd30") as url:
    data = json.loads(url.read().decode())

Rosters = DataFrame(data)
PlayerInfo = Rosters[['PlayerID','Team', 'Position']].copy()
games=[]

for link in links:
    print(link)
    req = urllib.request.Request(url= link, headers=header)
    with urlopen(req) as url:
        data = json.loads(url.read().decode())
    for game in data:
        
        try: #NOTE: This is using a trial key
            row= [game['HomeTeam'],game['AwayTeam'],game['GameID'],game['GameStartTime'],'https://api.sportsdata.io/v3/mlb/odds/json/BettingPlayerPropsByGameID/'+str(game['GameID'])+'?key=ab5ccb732f1d48febbdb890d300d656e']
            games.append(row)
            print('https://api.sportsdata.io/v3/mlb/odds/json/BettingPlayerPropsByGameID/'+str(game['GameID'])+'?key=ab5ccb732f1d48febbdb890d300d656e')
        except:
            row= [game['HomeTeam'],game['AwayTeam'],game['GameID'],game['GameStartTime'],'https://api.sportsdata.io/v3/mlb/odds/json/BettingPlayerPropsByGameID/'+str(game['GameID'])+'?key=ab5ccb732f1d48febbdb890d300d656e']
            games=row
            print('https://api.sportsdata.io/v3/mlb/odds/json/BettingPlayerPropsByGameID/'+str(game['GameID'])+'?key=ab5ccb732f1d48febbdb890d300d656e')

dfgames=DataFrame(games)
dfgames.columns = ["HomeTeam", "AwayTeam","GameID","GameStartTime","GameLink"]
p=dfgames.shape[0]
props=[]
for x in range(0,p):
    gamelink=dfgames['GameLink'][x]
    req = urllib.request.Request(url= gamelink, headers=header)
    with urlopen(req) as url:
        data = json.loads(url.read().decode())
    for prop in data:
        if prop['AnyBetsAvailable'] :
            HomeTeam=dfgames['HomeTeam'][x]
            AwayTeam=dfgames['AwayTeam'][x]
            GameID=dfgames['GameID'][x]
            GameStartTime=dfgames['GameStartTime'][x]
            BettingBetType=prop['BettingBetType']
            BettingPeriodType=prop['BettingPeriodType']
            BettingOutcomes=prop['BettingOutcomes']
            PlayerID=prop['PlayerID']
            PlayerName=prop['PlayerName']
            
            if BettingPeriodType == 'Full-Game':
                for outcome in BettingOutcomes:
                    if outcome['IsAvailable']:
                        BettingOutcomeType= outcome['BettingOutcomeType']
                        Value=outcome['Value']
                        ML= outcome['PayoutAmerican']
                        book= outcome['SportsBook']['Name']
                        bookUrl= outcome['SportsbookUrl']
                        Updated=outcome['Updated']
                        #Implied from the Sportsbook. Using the same formula we typically use for Props/GameBets
                        if ML<0:
                            ImpliedOdds= 1/(1-100/ML)
                        else:
                            ImpliedOdds= 1/(1+ML/100)
                        
                        if book == 'Consensus':
                            dummy=0
                        else:
                            if BettingOutcomeType =="Over": 
                                BetName= BettingOutcomeType + ':'	+ str(Value)+' '+ BettingBetType
                                btype="Over"
                                
                            if BettingOutcomeType =="Yes":
                                BetName= BettingBetType
                                if BettingBetType == 'To Hit A Home Run': BettingBetType = 'Total Home Runs'
                                if BettingBetType == 'To Get A Hit': BettingBetType = 'Total Hits'
                                if BettingBetType == 'To Record an RBI': BettingBetType = 'Total RBIs'
                                if BettingBetType == 'To Hit A Double': BettingBetType = 'Doubles'
                                if BettingBetType == 'To Score A Run': BettingBetType = 'Total Runs'
                                if BettingBetType == 'To Hit A Single': BettingBetType = 'Singles'
                                if BettingBetType == 'To Hit A Triple': BettingBetType = 'Triples'
                                Value=.5 
                                btype="Yes"
                            
                            if BettingOutcomeType =="Under": 
                                btype="Under"
                                BetName= BettingOutcomeType + ':'	+ str(Value)+' '+ BettingBetType
                                
                            #Setup Q4 derived Fields
                            Q4Odds=''
                            Edge=''
                            Stars=''
                            RecommendedBet= 'No'
                            
                            try:
                                #Lookup the relevant Q4 Projections
                                q4lookup1=Q4projectionsFinal.loc[Q4projectionsFinal['GameID']==GameID]
                                q4lookup=q4lookup1.loc[q4lookup1['FantasyDataPlayerID']==PlayerID]
                                #Home Runs
                                if BettingBetType == 'Total Home Runs' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_HomeRun']        
                                if BettingBetType == 'Total Home Runs' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_HomeRun']
                                #Hits
                                if BettingBetType == 'Total Hits' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_Hits']        
                                if BettingBetType == 'Total Hits' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_Hits']
                                #Singles
                                if BettingBetType == 'Singles' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_Singles']        
                                if BettingBetType == 'Singles' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_Singles']
                                #Doubles
                                if BettingBetType == 'Doubles' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_Doubles']        
                                if BettingBetType == 'Doubles' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_Doubles']
                                #Triples
                                if BettingBetType == 'Triples' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_Triples']        
                                if BettingBetType == 'Triples' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_Triples']
                                
                                #Total Bases
                                if BettingBetType == 'Total Bases' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_TotalBases']        
                                if BettingBetType == 'Total Bases' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_TotalBases']
                                
                                #Total Runs
                                if BettingBetType == 'Total Runs' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_Runs']        
                                if BettingBetType == 'Total Runs' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_Runs']
                                
                                #Total RBIs
                                if BettingBetType == 'Total RBIs' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_RBI']        
                                if BettingBetType == 'Total RBIs' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_RBI']

                                #Total Pitching Strikeouts
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == .5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_K']        
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 1.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_1_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 2.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_2_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 3.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_3_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 4.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_4_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 5.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_5_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 6.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_6_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 7.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_7_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 8.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_8_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 9.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_9_K']
                                if BettingBetType == 'Total Pitching Strikeouts' and Value == 10.5:
                                    Q4Odds=q4lookup.iloc[0]['Q4_Prob_0_to_10_K']
                                
                                
                                if  Q4Odds!='' and btype=='Under' :
                                    Edge=Q4Odds-ImpliedOdds
                                    Stars= round(Edge*100,0)/2
                                    RecommendedBet= 'Yes'
                                    if Stars < 0:
                                        Stars= 0
                                        RecommendedBet= 'No'
                                    if Stars > 5:
                                        Stars= 5
                                if  Q4Odds!=''and (btype=='Yes' or btype=='Over'):
                                    Edge=(1-Q4Odds)-ImpliedOdds
                                    Stars= round(Edge*100,0)/2
                                    RecommendedBet= 'Yes'
                                    if Stars < 0:
                                        Stars= 0
                                        RecommendedBet= 'No'
                                    if Stars > 5:
                                        Stars= 5

                                    


                            except:
                                dummy =1
                            row= [HomeTeam, AwayTeam, GameID, GameStartTime, gamelink, book, BettingBetType,BettingPeriodType,PlayerID,PlayerName,Updated,Value,bookUrl, ML, btype, BetName, ImpliedOdds, Q4Odds, Edge,Stars,RecommendedBet]
                            props.append(row)
                            

dfprops=DataFrame(props)


dfprops.columns = ["HomeTeam", "AwayTeam", "GameID", "GameStartTime", "gamelink", "book", "BettingBetType","BettingPeriodType","PlayerID","PlayerName","Updated","Value","bookUrl", "MoneyLine", "BetSelection","BetName", "ImpliedOdds", "Q4OddsUnder", "Edge","Stars","RecommendedBet"]

dfprops2= merge(dfprops,PlayerInfo,how="left",on=["PlayerID"])

SDPropsFinal= dfprops2[["PlayerName","Position","Team", "MoneyLine", "BetSelection","Value", "BettingBetType", "book","PlayerID","Updated","bookUrl","HomeTeam", "AwayTeam", "GameID", "GameStartTime", "gamelink", "BetName","ImpliedOdds", "Q4OddsUnder", "Edge","Stars","RecommendedBet"]].copy()

print('Push the props')
Goal = sheet.worksheet("PropFeedAll")
sheet.values_clear("PropFeedAll!a1:v40000")
set_with_dataframe(Goal, SDPropsFinal, row=1,col=1)
timenow = datetime.now(ny)
Goal.update('x2', timenow.strftime("%B %d, %Y %H:%M:%S"))
    


# In[ ]:


try:   
    #Send Props to SQL
    #Taqble will be called MLBPropsTableAll
    timenow = datetime.now(ny)
    SDPropsFinal['Table_Updated(EST)']=timenow.strftime("%B %d, %Y %H:%M:%S")
    SDPropsFinal.to_sql('MLBPropsTableAll', engine, if_exists='replace')
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:7. Sent Props to SQL (MLBPropsTableAll) Run Complete :'+timenow.strftime("%B %d, %Y %H:%M:%S"))
except:
    timenow = datetime.now(ny)
    send_slack_message(':billed_cap:7. Failed to Send Props to SQL (MLBPropsTableAll) Run Complete :'+timenow.strftime("%B %d, %Y %H:%M:%S"))

