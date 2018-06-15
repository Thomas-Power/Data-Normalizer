"""
Gathers REST data from exchange website.
"""
import requests
import json
import time

timestamp = int(time.time())
currencies = ['XRP', 'ETH', 'TRX', 'VEN', 'BNB', 'OMG', 'ICX', 'SUB', 'WTC', 'ZRX', 'NULS', 'AE', 'ZIL', 'QTUM', 'EOS', 'REP', 'IOTA', 'XVG', 'XLM', 'THETA', 'ENG', 'FUN', 'KNC', 'BNT', 'CMT', 'ELF', 'LRC', 'AION', 'SNT', 'DGD', 'IOST', 'PPT', 'REP', 'GNT']


for i in range (len(currencies)):
	currency_pair = 'BTC-' + currencies[i]
	
	http = 'https://bittrex.com/Api/v2.0/pub/market/GetTicks?'
	ticker = 'marketName=' + currency_pair
	interval = '&tickInterval=oneMin'


	r = requests.get(http + ticker + interval)

	export_dir = './output/' + currency_pair + str(timestamp) + '.json'

	export = open(export_dir, 'a')

	data = r.json()

	json.dump(data, export)
