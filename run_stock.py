
import pandas as pd
from time import sleep
import os

df = pd.read_csv('ipy/symbols.csv')
for i, symbol in enumerate(df['symbol'].values[38:]):
    i+=38
    print('start %d / %d %s' % (i, len(df), symbol))
    
    while 1:
        try:
            exe_res = os.system(" ".join(['scrapy', 'crawl', 'guba_online','-a','symbol=%s' % symbol, '--nolog']))
            if exe_res != 0:
                raise
            #sleep(1)
            break
        except:
            print('%s error, wating for 20 mins' % (symbol))
            sleep(1200)
