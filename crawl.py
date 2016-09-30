import os
import logging.handlers
import json


collection = {'mongoexport --host 128.199.112.254 --port 27017 --username stagwasp --password stag.wasp@topica --collection call_logs --db stagwasp --out /home/thang/PycharmProjects/etl_siren/data/call_logs.json',
'mongoexport --host 128.199.112.254 --port 27017 --username stagwasp --password stag.wasp@topica --collection events --db stagwasp --out /home/thang/PycharmProjects/etl_siren/data/events.json',
'mongoexport --host 128.199.112.254 --port 27017 --username stagwasp --password stag.wasp@topica --collection feedbacks --db stagwasp --out /home/thang/PycharmProjects/etl_siren/data/feedbacks.json',
'mongoexport --host 128.199.112.254 --port 27017 --username stagwasp --password stag.wasp@topica --collection ratings --db stagwasp --out /home/thang/PycharmProjects/etl_siren/data/ratings.json',
'mongoexport --host 128.199.112.254 --port 27017 --username stagwasp --password stag.wasp@topica --collection reason_feedbacks --db stagwasp --out /home/thang/PycharmProjects/etl_siren/data/reason_feedbacks.json',
'mongoexport --host 128.199.112.254 --port 27017 --username stagwasp --password stag.wasp@topica --collection timelines --db stagwasp --out /home/thang/PycharmProjects/etl_siren/data/timelines.json'
}


files = {'/home/thang/PycharmProjects/etl_siren/data/call_logs.json',
'/home/thang/PycharmProjects/etl_siren/data/events.json',
'/home/thang/PycharmProjects/etl_siren/data/feedbacks.json',
'/home/thang/PycharmProjects/etl_siren/data/ratings.json',
'/home/thang/PycharmProjects/etl_siren/data/reason_feedbacks.json',
'/home/thang/PycharmProjects/etl_siren/data/timelines.json',
}

def query():	
	for query in collection:
		os.system(query)


def split():
	for f in files:
		if os.path.exists(f):
			if (float(os.path.getsize(f)) / (1024 * 1024)) >= 64:
				os.system("split -l 50000 %s /home/thang/PycharmProjects/etl_siren/data/siren" % f)
				os.remove(f)
		else: 
			print('%s file not exists' % f)


def put_hadoop():	
	os.system('hadoop fs -put /home/thang/etl_siren/data ')
	return 


if __name__ == '__main__':
	query()
	split()
