
import pandas as pd
from time import sleep
import os
from time import time
df = pd.read_csv('ipy/symbols.csv')
for i, symbol in enumerate(df['symbol'].values[2970:]):
    i+=2970
    print('start %d / %d %s' % (i, len(df), symbol))
    
    while 1:
        try:
            t1 = time()
            exe_res = os.system(" ".join(['scrapy', 'crawl', 'guba_online','-a','symbol=%s' % symbol, '--nolog']))
            # exe_res = os.system(" ".join(['scrapy', 'crawl', 'guba_online','-a','symbol=%s' % symbol]))

            t2 = time()
            # print(t2-t1)
            if exe_res != 0:
                raise
            sleep(1)
            break
        except:
            print('%s error, wating for 20 mins' % (symbol))
            sleep(1200)
