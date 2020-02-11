
import pandas as pd
from time import sleep
import os

#df = pd.read_csv('ipy/symbols.csv')
for symbol in ['cjpl']:
    for job in range(50449//20+1):
        file_path = 'news/%s_%d.csv' % (symbol,job)
        if os.path.exists(file_path):
            os.remove(file_path)
        os.system(" ".join(['scrapy', 'crawl', 'guba_default','-a symbol=%s' % symbol[:6],'-a job=%d' % job, '-o', file_path, '-t', 'csv', '--nolog']))
        sleep(3)
