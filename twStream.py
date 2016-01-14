#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'user'
import os
import json
import HTMLParser
import logging
import re
from threading import Timer
import time

# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

from sys import exit
from datetime import datetime

# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#import polyglot
from polyglot.text import Text, Word

import urlparse
import psycopg2
from psycopg2.extras import Json

if "NJSAGENT_APPROOT" in os.environ:
    approot = os.getenv('NJSAGENT_APPROOT', "") + "/logs"
else:
    approot = os.path.dirname(os.path.realpath(__file__))

logfile = approot + "/" + os.path.basename(__file__) + ".log"
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=logfile)
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)
log = logging.getLogger('')

if not "NJSAGENT_APPROOT" in os.environ:
    log.warning("Missing NJSAGENT_APPROOT environment variable")
    exit(1)

conn = ''
cur = ''
countries = {}
sources = {}
terms = {}
recieved = 0
found = 0
lastdata = datetime.now()
inserted = []


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


def report():
    log.info("Recieved " + str(recieved) + " found " + str(found))


def check_inactive():
    diff = datetime.now() - lastdata
    total_seconds = diff.seconds
    if total_seconds > (60 * 15):
        log.warn("Stream idle: Lastdata recieved " + lastdata.strftime("%Y-%m-%d %H:%M:%S.%f") + " now " + datetime.now() + " diff" + str(total_seconds))
        exit(1)


# This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def on_data(self, data):
        global recieved, found, lastdata, inserted
        recieved += 1
        lastdata = datetime.now()
        try:
            tweet = json.loads(data)
            try:
                userid = tweet['user']['id_str']
                if userid in sources:
                    if self.is_valid_content(tweet):
                        if not tweet['id_str'] in inserted:
                            found += 1
                            self.save_tweet(tweet)
            except KeyError, AssertionError:
                pass
        except Exception, e:
            log.exception(str(e))
            log.warning('Bad tweet json' + data)
        return True

    def on_error(self, status):
        log.error(status)
        return False

    def is_valid_content(self, tweet):
        if sources[tweet['user']['id_str']]["filter"] == "false":
            return True
        try:
            h = HTMLParser.HTMLParser()
            t = h.unescape(tweet["text"])
            tokens = Text(t).words
            country = sources[tweet['user']['id_str']]["country"]
            country_terms = countries[country]
        except Exception, e:
            log.warning("Problem tokenizing " + tweet["text"] + " " + str(e))
            return False
        for token in tokens:
            try:
                p = re.compile('^' + re.escape(token), re.IGNORECASE)
            except Exception, e:
                log.warning("Bad regular expression for token " + str(token))
                continue
            for country_term in country_terms:
                m = p.match(country_term)
                if m:
                    return True
        return False

    def save_tweet(self, tweet):
        global inserted
        today = datetime.utcnow().strftime('%Y-%m-%d')
        country = sources[tweet['user']['id_str']]["country"]
        try:
            sql = "INSERT INTO countrydata (date) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM countrydata WHERE date::date = %s)"
            cur.execute(sql, [today, today])
            try:
                sql = "SELECT " + country + " FROM countrydata WHERE date::date = %s FOR UPDATE"
                cur.execute(sql, [today])
                dbdata = cur.fetchone()[0]
                if not "tweets" in dbdata:
                    dbdata["tweets"] = []
                if not self.isindb(dbdata, tweet):
                    try:
                        newdbdata = dbdata
                        newdbdata["tweets"].append(tweet)
                        sql = "UPDATE countrydata SET " + country + " = (%s) WHERE date::date = %s"
                        cur.execute(sql, [Json(newdbdata), today])
                        inserted.append(tweet['id_str'])
                        log.debug("Added tweet from " + tweet["user"]["name"] + " to " + country)
                    except psycopg2.Error as e:
                        log.warning("Cannot update because " + e.pgerror)
                    except Exception as e:
                        print(e)
            except psycopg2.Error as e:
                log.warning("Cannot select date because " + e.pgerror)
        except psycopg2.Error as e:
            log.warning("Cannot insert date because " + e.pgerror)

    def isindb(self, dbdata, tweet):
        for dbtweet in dbdata['tweets']:
            if dbtweet['id_str'] == tweet['id_str']:
                return True
        return False


def main():
    global conn, cur

    if not "TWCOLLECTOR_PGSQL" in os.environ:
        log.warning("Missing TWCOLLECTOR_PGSQL environment variable")
        exit(1)
    if not "TWCOLLECTOR_TWITTER_ACCESS_TOKEN_KEY" in os.environ:
        log.warning("Missing TWCOLLECTOR_TWITTER_ACCESS_TOKEN_KEY environment variable")
        exit(1)
    if not "TWCOLLECTOR_TWITTER_ACCESS_TOKEN_SECRET" in os.environ:
        log.warning("Missing TWCOLLECTOR_TWITTER_ACCESS_TOKEN_SECRET environment variable")
        exit(1)
    if not "TWCOLLECTOR_TWITTER_CONSUMER_KEY" in os.environ:
        log.warning("Missing TWCOLLECTOR_TWITTER_CONSUMER_KEY environment variable")
        exit(1)
    if not "TWCOLLECTOR_TWITTER_CONSUMER_SECRET" in os.environ:
        log.warning("Missing TWCOLLECTOR_TWITTER_CONSUMER_SECRET environment variable")
        exit(1)
    if not "TWCOLLECTOR_SOURCE" in os.environ:
        log.warning("Missing TWCOLLECTOR_SOURCE environment variable")
        exit(1)
    if not "TWCOLLECTOR_MODE" in os.environ:
        log.warning("Missing TWCOLLECTOR_MODE environment variable")
        exit(1)

    pgsql = os.getenv('TWCOLLECTOR_PGSQL', '')
    # Variables that contains the user credentials to access Twitter API
    access_token = os.getenv('TWCOLLECTOR_TWITTER_ACCESS_TOKEN_KEY', '')
    access_token_secret = os.getenv('TWCOLLECTOR_TWITTER_ACCESS_TOKEN_SECRET', '')
    consumer_key = os.getenv('TWCOLLECTOR_TWITTER_CONSUMER_KEY', '')
    consumer_secret = os.getenv('TWCOLLECTOR_TWITTER_CONSUMER_SECRET', '')
    target = os.getenv('TWCOLLECTOR_SOURCE', '')
    mode = os.getenv('TWCOLLECTOR_MODE', '')

    with open(os.path.dirname(os.path.realpath(__file__)) + "/" + target) as json_file:
        json_data = json.load(json_file)

    for country in json_data["tracks"]:
        countries[country["country"]] = [x.strip() for x in country["track"].split(',')]
        trm = [x.strip() for x in country["track"].split(',')]
        for term in trm:
            if term != '':
                terms[term] = term
        for general in json_data["general"]:
            countries[country["country"]].append(general.strip())
            terms[general.strip()] = general.strip()
        for source in country["sources"]:
            sourcedata = {}
            sourcedata["country"] = country["country"].strip()
            if "filter" in source:
                sourcedata["filter"] = source["filter"]
            else:
                print "Invalid filter for source id: " + source["data-user-id"]
                sourcedata["filter"] = "true"
            sources[source["data-user-id"].strip()] = sourcedata

    if mode == "track":
        termslist = terms.keys()
        termslist.sort()
        parameters = ','.join(termslist)
    else:
        accountslist =  sources.keys()
        accountslist.sort()
        parameters = ','.join(accountslist)

    log.info("Started in mode {0} target {1} parameters {2}".format(mode, target, parameters))
    
    try:
        urlparse.uses_netloc.append('postgres')
        url = urlparse.urlparse(pgsql)
        conn = psycopg2.connect(
            "dbname=%s user=%s password=%s host=%s " % (url.path[1:], url.username, url.password, url.hostname))
        conn.autocommit = True
        cur = conn.cursor()
    except  psycopg2.Error as e:
        log.error("I am unable to connect to the database because " + e.pgerror)
        exit(1)

    # This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    # This line filter Twitter Streams to capture data by the keywords
    if mode == "track": 
        stream.filter(track=[x.strip() for x in parameters.split(',')])
    else:
        stream.filter(follow=[x.strip() for x in parameters.split(',')])


if __name__ == '__main__':
    try:
        rt = RepeatedTimer(60 * 10, report)  # it auto-starts, no need of rt.start()
        ia = RepeatedTimer(60, check_inactive)
        main()
    except KeyboardInterrupt:
        logging.shutdown()
        print '\nGoodbye!'
        exit()
    except Exception, e:
        log.exception(str(e))
        logging.shutdown()
    finally:
        rt.stop()  # better in a try/finally block to make sure the program ends!
        ia.stop()
        time.sleep(60)
        log.warn("Exiting")
        exit(1)
