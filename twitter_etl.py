import tweepy
import json
import pandas as pd
import s3fs
from datetime import datetime

def run_twitter_etl():
    access_key =  "1755998915315281920-RNOTrAmbaf0FPoJaxtCFyMRxK67crD"
    access_secret = "MLGuj8JwCoxmTAnn26zkuyo5CdXFOKVGevUA4xVEv2Y0Y"
    consumer_key = "A0XDvJIc1RU1JOpKMH5wxUWhE"
    consumer_secret = "PXjjHDLkjpu2VGJyXPanwwfE9GGC1YAwL4ynrxR80AAVjuutDi"

    # twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                                # 200 is the maximum allowed count
                                count=200,
                                include_rts = False, # remove retweets
                                # Necessary to keep full_text 
                                # otherwise only the first 140 words are extracted
                                tweet_mode = 'extended'
                                )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('elon_musk_twitter.csv')
