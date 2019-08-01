
import pandas as pd
from time import sleep
import os

df = pd.read_csv('ipy/symbols.csv')
for symbol in df['symbol'].values:
	file_path = 'news/%s.csv' % symbol
	if os.path.exists(file_path):
		os.remove(file_path)
	try:
		os.system(" ".join(['scrapy', 'crawl', 'guba','-a','symbol=%s' % symbol[:6],'-o', file_path, '-t', 'csv', '--nolog']))
		with open(file_path, 'r') as rf:
			lines = len(rf.readlines())
			with open("log/real_%s.csv" % symbol, 'w') as wf:
				wf.write('%d' % lines)
		print('%s %d' % (symbol, lines))
	except:
		print('%s N' % (symbol))
	sleep(5)
