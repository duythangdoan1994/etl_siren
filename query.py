import pymongo
import json
import os
import glob
import time
import datetime
from bson import json_util


def get_database():
    db_connect = pymongo.MongoClient('128.199.112.254', 27017)
    database = db_connect['stagwasp']
    database.authenticate('stagwasp', 'stag.wasp@topica')
    return database


def getcoll():
    database = get_database()
    collection = database.collection_names(include_system_collections=False)
    return collection


def process_json():
    database = get_database()
    collection = getcoll()
    for i in collection:
        if i == 'feedbacks' or i == 'call_logs':
            if i == 'feedbacks':
                with open("./data/%s.json" % i, "w") as f:
                    cursor = database[i].find({})
                    for document in cursor:
                        document['_id'] = str(document.get('_id'))
                        document['team_id'] = str(document.get('team_id'))
                        document['user_id'] = str(document.get('user_id'))
                        document['reason_feedback_id'] = str(document.get('eason_feedback_id'))
                        document['channel_feedback_id'] = str(document.get('channel_feedback_id'))
                        document['updated_at'] = "{:%d/%m/%Y/%H/%M/%S}".format(document['updated_at'])
                        document['created_at'] = "{:%d/%m/%Y/%H/%M/%S}".format(document['created_at'])
                        document['updated_at'] = int(time.mktime(
                            datetime.datetime.strptime(document['updated_at'], "%d/%m/%Y/%H/%M/%S").timetuple()))
                        document['created_at'] = int(time.mktime(
                            datetime.datetime.strptime(document['created_at'], "%d/%m/%Y/%H/%M/%S").timetuple()))
                        document = json.dumps(document, default=json_util.default)
                        f.write(document + '\n')
                    f.close()
            else:
                with open("./data/%s.json" % i, "w") as f:
                    cursor = database[i].find({})
                    for document in cursor:
                        document.update(document['call'])
                        if 'call' in document:
                            del document['call']
                        document['_id'] = str(document['_id'])
                        document['user_id'] = str(document['user_id'])
                        document['updated_at'] = "{:%d:%m:%Y:%H:%M:%S}".format(document['updated_at'])
                        document['created_at'] = "{:%d:%m:%Y:%H:%M:%S}".format(document['created_at'])
                        document['updated_at'] = int(time.mktime(
                            time.strptime(document['updated_at'], '%d:%m:%Y:%H:%M:%S')))
                        document['created_at'] = int(time.mktime(
                            time.strptime(document['created_at'], '%d:%m:%Y:%H:%M:%S')))
                        document = json.dumps(document, default=json_util.default)
                        f.write(document + '\n')
                    f.close()


def check_file():
    file = glob.glob("./data/*")
    for f in file:
        if os.path.exists(f):
            if (float(os.path.getsize(f)) / (1024 * 1024)) >= 64:
                os.system('split -b60m %s ./data/feedbacks' % f)
                os.system('rm -rf %s' % f)


def put_hadoop():
    os.system('hdfs dfs -rm  -r -f /user/hadoop/edumall/siren/*')
    os.system('hdfs dfs -put ./data/*  /user/hadoop/edumall/siren')


if __name__ == '__main__':
    get_database()
    getcoll()
    process_json()
    check_file()
    put_hadoop()

