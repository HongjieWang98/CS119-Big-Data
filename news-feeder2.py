#!/usr/bin/env python3
import sys
import feedparser, time, datetime
import pandas as pd
import re
import time, datetime
import pandas as pd
import os
from collections import defaultdict
from pybloom import BloomFilter
from pybloom import ScalableBloomFilter

class Feed:
    name = ''
    url = None
    max_delay = None
    def __init__(self, name, url, max_delay):
        self.name = name
        self.url  = url
        self.max_delay = max_delay

    def __str__(self):
        return '%s: %s' % (self.name, self.url)

    def getHeadline(self):
        d = feedparser.parse (self.url)
        for post in d.entries:
            time.sleep(1.0)
            ret = (datetime.datetime.now().time(), self.name, post.title, post.link)
            yield ret

#
def pre_process(text):
    # lowercase
    text=text.lower()
    # remove tags
    text=re.sub("","",text)
    # remove numbers and dollar amounts
    text = re.sub(r'[0-9\,$]+', '', text)
    # remove special characters and digits
    text=re.sub("(\\d|\\W)+"," ",text)
    # remove short words
    text = ' '.join([word for word in text.split() if len(word) > 3])
    return text

def tolist(text):
    return text.split()

hdlines_df = pd.read_csv('big-data-repo/data/2020-headlines.csv')
hdlines_df.dropna(inplace=True)
hdlines_df.drop(columns=['SNO', 'Website'], inplace=True)
hdlines_df

hdlines_df['text'] = hdlines_df['News']
hdlines_df['text'] = hdlines_df['text'].apply(lambda x:pre_process(x))
hdlines_df['text']
hdlines = hdlines_df['text']

wordsFreq = defaultdict(int)
lines = hdlines.tolist()
for line in lines:
    words = line.split()
    for word in words:
        if len(word) > 3:
            wordsFreq[word] += 1
            
def topn(wordsFreq, n):
    return sorted(wordsFreq.items(), key=lambda k_v: k_v[1], reverse=True)[:n]

understood_words = set([word for word in wordsFreq.keys() if wordsFreq[word] > 1])
print ('collected %d-word newsworthy vocabulary' % len(understood_words))

bf = BloomFilter(capacity=21180*8, error_rate=0.1)
for word in understood_words:
    bf.add(word)         



feeds = (
    Feed('nyt-home', 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml', 1),
    Feed('nyt-sun', 'https://rss.nytimes.com/services/xml/rss/nyt/sunday-review.xml', 1),
    Feed('nyt-hlth', 'https://rss.nytimes.com/services/xml/rss/nyt/Health.xml', 1),
    Feed('nyt-wrld', 'https://www.nytimes.com/section/world/rss.xml', 1),
    Feed('nyt-bsns', 'http://feeds.nytimes.com/nyt/rss/Business', 1),
    Feed('nyt-tech', 'http://feeds.nytimes.com/nyt/rss/Technology', 1),
    Feed('nyt-sprt', 'https://rss.nytimes.com/services/xml/rss/nyt/Sports.xml', 1),
    Feed('nyt-scnc', 'http://www.nytimes.com/services/xml/rss/nyt/Science.xml', 1),
    Feed('nyt-arts', 'https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml', 1),
    Feed('nyt-trvl', 'https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml', 1),
    Feed('nyt-usa',  'http://www.nytimes.com/services/xml/rss/nyt/US.xml', 1),
    Feed('bbc-hlth', 'http://feeds.bbci.co.uk/news/health/rss.xml', 1),
    Feed('bbc-brkn', 'https://bbcbreakingnews.com/feed', 1),
    Feed('bbc-bsns', 'http://feeds.bbci.co.uk/news/business/rss.xml', 1),
    Feed('bbc-pltc', 'http://feeds.bbci.co.uk/news/politics/rss.xml', 1),
    Feed('bbc-educ', 'http://feeds.bbci.co.uk/news/education/rss.xml', 1),
    Feed('bbc-scnc', 'http://feeds.bbci.co.uk/news/science_and_environment/rss.xml', 1),
    Feed('bbc-tech', 'http://feeds.bbci.co.uk/news/technology/rss.xml', 1),
    Feed('bbc-arts', 'http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml', 1),
)

'''
# Used for testing, now commented out
titles = []
for feed in feeds:
    for (tt, name, title, link) in feed.getHeadline():
        words = pre_process(title).split()

        if all([x in understood_words for x in words]):
            # print('i know this: ', title)
            pass
        else:
            print(title)
'''

titles = []
for feed in feeds:
    for (tt, name, title, link) in feed.getHeadline():
        print (title, flush = True)
        #t = pre_process(str(title))
        for i in title.split():
            t = pre_process(str(i))
            if len(t)<=3:
                continue
            if t not in bf:
                print('\r\n Unformiliar word:',t)
                print('Unformiliar title:',title,'\r\n')
                break