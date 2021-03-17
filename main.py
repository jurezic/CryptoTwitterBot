import tweepy
import cryptocompare
import datetime
import urllib.request, json
import pandas as pd
import collections
import csv
from textblob import TextBlob as NLP
from IPython.display import display
CRYPTO_LINK = "https://www.cryptocompare.com/api/data/coinlist/"
NUMBER_OF_TWEETS = 100
NUMBER_OF_FOLLOWERS = 100

def get_twitter_api():
    BEARER_TOKEN = ""
    auth = tweepy.OAuthHandler("", "")
    auth.set_access_token("", "")

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    try:
        api.verify_credentials()
        print("Authentication OK")
        return api
    except:
        print("Error during authentication!")

def json_extract(obj, key):
    arr = []

    def extract(obj, arr, key):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values

def get_cryptos(link):
    with urllib.request.urlopen(link) as url:
        data = json.loads(url.read().decode())
        crypto_symbols = json_extract(data, 'Symbol')
        crypto_names = json_extract(data, 'CoinName')
        cryptos = {(k, v): 0 for k, v in zip(crypto_symbols, crypto_names)}
        return cryptos

def sort_followers(api):
    sorted_followers = {}
    for follower in tweepy.Cursor(api.friends).items(NUMBER_OF_FOLLOWERS):
        sorted_followers[follower.name] = round(follower.followers_count/1000)
    sorted_followers = dict(sorted(sorted_followers.items(), key=lambda item: item[1], reverse=True))
    (pd.DataFrame.from_dict(data=sorted_followers, orient='index').to_csv('followers.csv', header=False))
    return sorted_followers

def get_tweets(api, cryptos, sorted_followers):
    #tweets = []
    for indx, tweet in enumerate(tweepy.Cursor(api.home_timeline).items(NUMBER_OF_TWEETS)):
        crypto_keys = list(cryptos.keys())
        for idx, key in enumerate(crypto_keys):
            if (f" {key[0]} " in tweet.text) or (f" {key[1]} " in tweet.text) or (f"${key[0]}" in tweet.text) or (f"${key[1]}" in tweet.text) or (f"#{key[0]}" in tweet.text) or (f"#{key[1]}" in tweet.text):
                polarity = NLP(tweet.text).sentiment.polarity
                cryptos[key] += polarity*sorted_followers[tweet.user.name]
                #tweets.append(f"*** AT {tweet.created_at} ->{tweet.user.name}<- with polarity={NLP(tweet.text).sentiment} ==={tweet.text}=== ***")
        if indx == NUMBER_OF_TWEETS - 1 :
            last_tweet_time = tweet.created_at
    cryptos = dict(sorted(cryptos.items(), key=lambda item: item[1], reverse=True))
    cryptos = {k: v for k, v in cryptos.items() if v}
    pd.DataFrame.from_dict(data=cryptos, orient='index').to_csv('cryptos.csv', header=False)
    return cryptos, last_tweet_time

def get_mani(cryptos, last_tweet_time):
    for idx, (crypto, significance) in enumerate(cryptos.items()):
        print(f"*{idx}.*  {crypto} {significance} {last_tweet_time}")
    df_mani = pd.DataFrame(cryptos.items(), columns=['Crypto', 'Significance'])
    df_mani['TimeOfTweet'] = last_tweet_time
    time = datetime.datetime.now() - datetime.timedelta(minutes=30)
    df_mani[f'PriceAt {time}'] = 0
    for idx, key in enumerate(list(cryptos.keys())):
        if (json_extract(cryptocompare.get_historical_price(key[0], currency='USD', timestamp=time), 'USD')) is not None:
            df_mani[f'PriceAt {time}'][idx] = json_extract(cryptocompare.get_historical_price(key[0], currency='USD', timestamp=time), 'USD')[0]
        else:
            df_mani[f'PriceAt {time}'][idx] = 0

def check_prices(df_mani):
    time = datetime.datetime.now()
    df_mani[f'PriceAt {time}'] = 0
    for idx, key in enumerate(list(cryptos.keys())):
        if (json_extract(cryptocompare.get_historical_price(key[0], currency='USD', timestamp=time), 'USD')) is not None:
            df_mani[f'PriceAt {time}'][idx] = json_extract(cryptocompare.get_historical_price(key[0], currency='USD', timestamp=time), 'USD')[0]
        else:
            df_mani[f'PriceAt {time}'][idx] = 0
    print(df_mani)
    df.to_csv('Results.csv', index=False)

def main_method():

    api = get_twitter_api()

    #Get list of all cryptocurrencies
    cryptos = get_cryptos(CRYPTO_LINK)

    #First sort followers by count of their followers and associate them weights of signals by that count
    #Do this only once at first because of API limits, otherwise leave the sort followers call commented
    #sorted_followers = sort_followers(api)
    sorted_followers = pd.read_csv('Followers.csv', header=None, index_col=0, squeeze=True).to_dict()
    print(sorted_followers)

    #Get a list of those tweets which contain any of the key words (crypto names or symbols)
    #cryptos = pd.read_csv('cryptos.csv', header=None, index_col=0, squeeze=True).to_dict()
    cryptos, last_tweet_time = get_tweets(api, cryptos, sorted_followers)

    #Money Printer Go Brrr
    df_mani = get_mani(cryptos, last_tweet_time)

    while True:
        check_prices(df_mani)
        sleep(60)

if __name__ == '__main__':
    main_method()
    time = datetime.datetime.now() - datetime.timedelta(minutes=30)
    print(json_extract(cryptocompare.get_historical_price('BTC', currency='USD', timestamp=time), 'USD')[0])
    print(time)