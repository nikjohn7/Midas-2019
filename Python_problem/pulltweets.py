import sys,tweepy,json, jsonlines
from datetime import date
from pathlib import Path
from tweepy.parsers import JSONParser
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
""" author: Nikhil John """
""" last_modified: 23 Mar 2019 """
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# Provide Twitter API access credentials here
my_consumer_key = ''
my_consumer_secret = ''
my_access_key = ''
my_access_secret = ''
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=


class TweetFetcher:
    def __init__(self, consumer_key=my_consumer_key, consumer_secret=my_consumer_secret,
                access_token=my_access_key, access_token_secret=my_access_secret,
                wait_on_rate_limit=True,
                wait_on_rate_limit_notify=True):
        self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.auth.secure = True
        self.auth.set_access_token(access_token, access_token_secret)
        self.__api = tweepy.API(self.auth,
                                wait_on_rate_limit=wait_on_rate_limit,
                                wait_on_rate_limit_notify=wait_on_rate_limit_notify
                                )
        print('Twitter APi initialized')

    def api(self):
        return self.__api

    api = property(fget=api)

    def exists(self,userid):
        """
            Checks if the userid exists on Twitter
        :param userid:
        :return: True if userid exists
        """
        existsuser = False
        try:
            user = self.api.get_user(userid)
            if (user):
                existsuser = True

        except tweepy.TweepError as err:
            print(err)
            return existsuser

    def limit_handled(self, cursor):
        """

        :param cursor:
        :return:
        """
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                sys.time.sleep(1 * 30)


    def getTweets(self,userid,directory='C:/Users/nikhi/PycharmProjects/Midas-2019/data', filename='Midas-tweets.jsonl'):
        """ Gets all the Tweets permitted by the Twitter API for the given userId
        :param userid: The twitter user
        :param directory: The folder for the JSON lines file
        :param filename: filename for all of the tweets available. If filename already exists, todays date is appended to the filename
        :return: None
        """
        # Midas Problem Statement 1
        twitterapi = self.api
        ##-------- Do we have a vald Twitter User handle? ---------
        if not self.exists(userid):
            print("Invalid Twitter User handle or id :" + userid)
            return
        # Get the User object for twitter...
        user = twitterapi.get_user(userid)
        print(" Twitter User Exists:'" + user.screen_name + "'")
        # -------------------------------------------------------------
        filepath = self._prepareForWrite(directory, filename)
        ##-------- Call Twitter API the first time ---------
        """ tweet_mode : Twitter now supports 280 characters per tweet and we enable the api to read longer tweets by setting tweet_mode = extended
        This also requires tweet.full_text instead of tweet.text """
        """exclude_replies: we set this to True to filter out replies to original tweets """
        """ We wrap the tweepy Cursor into a rate-limited Cursor that respects rate_limits on the API"""
        available_tweets = self.limit_handled(tweepy.Cursor(twitterapi.user_timeline, id=userid, tweet_mode='extended', exclude_replies=True).items())
        alltweets = []
        last_tweet_id = 0
        tryAgain = True
       ##-------- Call Twitter API repeatedly until we retrieve no more tweets ---------
        with jsonlines.open(filepath, mode='w') as writer:
            while (tryAgain):
                # oldest will keep track  of the last tweet id retrieved
                oldest = 0
                new_tweets = []
                for tweet in available_tweets:
                    writer.write(tweet._json)
                    new_tweets.append(tweet._json)
                # we have some more tweets in new_tweets
                if (new_tweets):
                    alltweets.extend(new_tweets)
                    oldest = (new_tweets[-1]['id']) - 1
                    #----------------------------------------------------------------------------------------
                    # We need this extra check to deal with the situation when API returns the previous results again
                    # Not expected, but I have observed this behaviour
                    if (oldest != last_tweet_id):
                        last_tweet_id = oldest
                    else:
                        tryAgain = False;
                    #------------------------------------------------------------------------------------------
                    print (".. Getting tweets for %s before %s" % (user.screen_name, oldest))
                    available_tweets = self.limit_handled(
                        tweepy.Cursor(twitterapi.user_timeline, id=userid, tweet_mode='extended',
                                      exclude_replies=True,max_id=oldest).items())
                else:
                    tryAgain = False;

        print("User %s : ...%s tweets downloaded " % (user.screen_name,len(alltweets)))


    def _prepareForWrite(self, directory, filename):
        """
            Ensures that directory and all parsnts are created if they do not exist and appends day to filename if it already exists
        :param directory:
        :param filename:
        :return: filename as pathlib.Path object
        """
        folder = Path(directory)
        folder.mkdir(parents=True, exist_ok=True)
        filepath = folder / filename
        # if the filenme already exists, then we add todays date to the name component
        # Caution : running many times on the same day will overwrite the same file
        if (filepath.is_file()):
            today = str(date.today())
            filename = filepath.stem + '-' + today + filepath.suffix
            filepath = folder / filename
        # -------------------------------------------------------------
        return filepath

    def _prepareForRead(self, directory, filename):
        """
            Checks if directory and file exists and can be read. Returns false otherwise
        :param directory:
        :param filename:
        :return: the absolute path to the file if it exists and None otherwise
        """
        fname = None
        folder = Path(directory)
        filepath = folder / filename
        if (filepath.is_file()):
          fname = filepath.absolute();
        # -------------------------------------------------------------
        return fname

    def parseTweets(self,directory,filename):
        """
            Reads an existing JSON lines file and extracts certain fields
            The other part of your script should be able to parse these JSON lines file to display the following for every tweet
            in a tabular format.
            ● The text of the tweet.
            ● Date and time of the tweet.
            ● The number of favorites/likes.
            ● The number of retweets.
            ● Number of Images present in Tweet. If no image returns None.
        :param directory:
        :param filename:
        :return: the list of tweets
        """
        tweets = []
        fname = self._prepareForRead(directory,filename)
        if (not fname):
            print(" Invalid file %s " % (fname))
            return tweets
        with jsonlines.open(fname) as reader:
            for tweet in reader:
                photos = []
                if ("media" in tweet['entities']):
                    media = tweet['entities']["media"]
                    photos = [(x['display_url']) for x in media if x['type']=="photo"]
                tweets.append(["'" + tweet['full_text'] + "'", str(tweet['created_at']), 
                               tweet['favorite_count'], tweet['retweet_count'], len(photos)])
        return tweets


if __name__ == "__main__":
    a = TweetFetcher()
    directory = 'C:/Users/nikhi/PycharmProjects/Midas-2019/data'
    filename = 'Midas-tweets.jsonl'
    #--------------------------------------------------
    # Code to pull Tweets and store in a file
    #a.getTweets('@IPL',directory,filename)
    #---------------------------------------------------
    # --------------------------------------------------
    # Code to read file and parse Tweets
    tweets = a.parseTweets(directory,filename)
    print("%s " % ('\n').join(["%s " % ('\t').join(map(str,x)) for x in tweets]))
    # --------------------------------------------------