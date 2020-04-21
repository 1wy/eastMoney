
import pandas as pd
from time import sleep
import os

df = pd.read_csv('ipy/symbols.csv')
for i, symbol in enumerate(df['symbol'].values[:]):
    print('start %d / %d %s' % (i, len(df), symbol))
    cnt = 1
    cnt2 = 1
    cnt3 = 1
    #if os.path.exists('news2/%s/' % symbol):
    #    num_file = len(os.listdir('news2/%s' % symbol))
    #    if num_file > 0:
    #        cnt += num_file-1
    #if os.path.exists('news2-bak/%s/' % symbol):
    #    num_file = len(os.listdir('news2-bak/%s' % symbol))
    #    if num_file > 0:
    #        cnt3 += num_file-1
    #print('start %d / %d %s %d' % (i, len(df), symbol,cnt))
    if os.path.exists('log/checkout_%s.txt' % symbol[:6]):
        with open('log/checkout_%s.txt' % symbol[:6],'r') as f:
            lines = f.readlines()
            cnt2 = int(lines[-1].strip())//10+1
    #print('start %d / %d %s %d %d' % (i, len(df), symbol,cnt,cnt2))
    #if (cnt < cnt2):
    #    print('start %d / %d %s %d %d %d' % (i, len(df), symbol,cnt,cnt2,cnt3))
    #continue
    while 1:
        file_path = 'news2/%s/%s_%d.csv' % (symbol,symbol,cnt)
        if os.path.exists(file_path):
            os.remove(file_path)
                
        try:
            exe_res = os.system(" ".join(['scrapy', 'crawl', 'guba_online','-a','symbol=%s' % symbol[:6],'-o', file_path, '-t', 'csv', '--nolog']))
            if exe_res != 0:
                raise
            with open('log/totpage_%s.txt' % symbol[:6], 'r') as rf:
                totpage = int(rf.readline().strip())
            if totpage < cnt*10+1:
                break
            sleep(1)
            cnt += 1
        except:
            print('%s_%d error, wating for 10 mins' % (symbol,cnt))
            sleep(600)
