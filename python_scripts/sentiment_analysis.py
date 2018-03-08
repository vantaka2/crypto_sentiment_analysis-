""" This is a function to access a sentiment analysis API (https://market.mashape.com/vivekn/sentiment-3) """
import requests
import pandas as pd
import json

def get_sentiment(text):
    """Api call to get sentiment and confidence of title
    Input:= title
    Output: Dictionary of sentiment & confidence""" 
    a = requests.post(url='https://community-sentiment.p.mashape.com/text/',
                      headers={"X-Mashape-Key": "2Wd5fI5lvemshnqqV2JQd3Ltr453p17JZQijsnwa2sDywxYAsg",
                                 "Content-Type": "application/x-www-form-urlencoded",
                                 "Accept": "application/json"},
                      data = {"txt":text})
    c= json.loads(a.content.decode('utf8'))['result']
    return c