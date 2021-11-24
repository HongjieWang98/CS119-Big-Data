# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 21:16:17 2021

@author: 11231
"""

import re
import os
import math
import mmh3
from bitarray import bitarray
from collections import defaultdict
import pandas as pd
from random import shuffle
def solve_text(text):
       text = text.lower()
       text=re.sub("","",text)
       text = re.sub(r'[0-9\,$]+', '', text)
       return text
data_df = pd.read_csv(os.path.dirname(dir_path) + '/data/2020-headlines.csv')
data_df.dropna(inplace=True)
data_df.drop(columns=['SNO', 'Website'], inplace=True)


data_df['text'] = data_df['News']
data_df['text'] = data_df['text'].apply(lambda x:solve_text(x))
datas = data_df['text']

wordsFreq = defaultdict(int)
lines = datas.tolist()
for line in lines:
    words = line.split()
    for word in words:
        if len(word) > 3:
            wordsFreq[word] += 1


words = set([word for word in wordsFreq.keys() if wordsFreq[word] > 1])

size = len(words)
bit_array=bitarray(size)
hash_count = (size/count)*math.log(2)

for word in words:
    digests = []
    for i in range(hash_count):
        print("Hash Result:", mmh3.hash(item, i), "|||||", mmh3.hash(item, i) % size)
        digest = mmh3.hash(item, i) % self.size
        digests.append(digest)
        
        # set the bit True in bit_array
        bit_array[digest] = 1
        print(hash_count)
        print(size)
with open('bloom.txt', 'w') as output_file:
    output_file.write(bloomf.bit_array.to01())

'''
for word in words:
    digests = []
for i in range(hash_count):
    print("Hash Result:", mmh3.hash(item, i), "|||||", mmh3.hash(item, i) % size)
digest = mmh3.hash(item, i) % self.size
digests.append(digest)# set the bit True in bit_array
bit_array[digest] = 1
print(hash_count)
print(size)
with open('bloom.txt', 'w') as output_file:
    output_file.write(bloomf.bit_array.to01())
'''