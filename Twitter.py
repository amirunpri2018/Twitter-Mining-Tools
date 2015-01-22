# -*- coding: utf-8 -*-
"""
Created on Wed Jan 21 12:07:08 2015

@author: Gaurav
"""

# Import the necessary methods from tweepy library
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Import modules needed to parse Twitter data
import json
import pandas as pd
import re

#This is a basic listener that just prints received tweets to stdout.
class TweetListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status

def listenToTwitter(self, filters=None):
    
    # Credentials
    self.consumer_key = "mTv3PyWHMPNdlqvSYM05rMjxl"
    self.consumer_secret = "P7Sgr5rZt4V9VJImzvUy6qn4mELzzMfO0rH9ig6D3729oKIxyR"
    self.access_token = "2890181881-L3Kgl5GMHpupldDQukq3OB1AAQSRs5CCbrautYz"
    self.access_token_secret = "k12Dwju3eaNVaMKhx6ejJ69Noe1LemJbbJ11xPmeci0fx"
    self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
    self.auth.set_access_token(self.access_token, self.access_token_secret)

    try:
        stream = Stream(self.auth, TweetListener())
        if filters is not None:
            stream.filter(track=filters)
        else:
            stream.filter(track=['default'])
    except:
        KeyboardInterrupt
#        python processTweet.py > twitterData.txt
        
    
def wordsInText(word, text):
    word = word.lower()
    text = text.lower()
    match = re.search(word, text)
    if match:
        return True
    return False
    
class processTweets:
    
    def __init__(self, path=None,tags=None):
        if path is not None:
            self.tweetDataPath = path
        else:
            self.tweetDataPath = 'C:/Anaconda/Scripts/twitterData.txt'
        if tags is not None:
            self.tags = tags
        else:
            self.keywords = ['default']
        self.tweetData = []
        self.tweetFile = open(self.tweetDataPath, 'r')      
        
    def loadTweets(self):
        for line in self.tweetFile:
            try:
                self.tweetData.append(json.loads(line))
            except:
                continue
        self.tweets = pd.DataFrame()
        self.tweets['text'] = map(lambda tweet: tweet['text'], self.tweetData)
        print "Imported file has a total of %s tweets." % (len(self.tweetData))

    def getCategories(self):
        twitterCategories = list(self.tweetData[0].keys())
        for i in range(len(twitterCategories)):
            print twitterCategories[i]
        return
        
    def addCategory(self, categories=None):
        if (categories and self.tweets) is not None:
            for cat in range(len(categories)):
                try:
                    self.tweets[categories[cat]] = map(lambda tweet: tweet[categories[cat]], self.tweetData)
                    print "Data frame now includes the '%s' category." % (categories[cat])
                except:
                    print "Twitter data does not have a '%s' category." % (categories[cat])
                    KeyError
        return

    def getTextByTags(self):
        tags = self.tags
        for i in range(len(tags)):
            if type(tags[i]) == str:
                try:
                    self.tweets[tags[i]] =self.tweets['text'].apply(lambda tweet: wordsInText(tags[i], tweet))
                    print "%s tweets contain the tag: %s." % (self.tweets[tags[i]].value_counts()[True], tags[i])
                except:
                    print "No tweets contain the tag: %s." % (tags[i])
                    KeyError
        return         
    
tw = processTweets(tags=['GMO'])
tw.loadTweets()
tw.getTextByTags()
