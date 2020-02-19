
import pandas as pd
from time import sleep
import os

#df = pd.read_csv('ipy/symbols.csv')
for symbol in ['cjpl']:
    cnt = 1
    if os.path.exists('log/check_out.txt'):
        with open('log/check_out.txt','r') as f:
            cnt = int(f.readlines()[-1].strip())/10+1
    while 1:
        file_path = 'news/%s_%d.csv' % (symbol,cnt)
        if os.path.exists(file_path):
            os.remove(file_path)
                
        try:
            os.system(" ".join(['scrapy', 'crawl', 'guba_default','-a','symbol=%s' % symbol[:6],'-o', file_path, '-t', 'csv', '--nolog']))
            with open(file_path, 'r') as rf:
                lines = len(rf.readlines())
                with open("log/real_%s_%d.csv" % (symbol, cnt), 'a+') as wf:
                    wf.write('%d' % lines)
            sleep(1)
            print('%s %d %d' % (symbol, cnt, lines))
            cnt += 1
        except:
            print('%s_%d error, wating for 10 mins' % (symbol,cnt))
            sleep(600)
