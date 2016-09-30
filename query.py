import pymongo
import json
import os
import glob
import time
import datetime
from bson import json_util


def get_database():
    # mongo_uri = "mongodb://128.199.112.254:27017"
    db_connect = pymongo.MongoClient('128.199.112.254', 27017)
    database = db_connect['stagwasp']
    database.authenticate('stagwasp', 'stag.wasp@topica')
    return database


def getcoll():
    database = get_database()
    collection = database.collection_names(include_system_collections=False)
    return collection


# with open("./data/%s.json" % i, 'w') as f:
#     for document in cursor:
#         document = json_util.dumps(document)
#         document = json.dumps(document, default=json_util.default)
#         json.dump(f, document)
def process_json():
    database = get_database()
    collection = getcoll()
    for i in collection:
        if i == 'feedbacks' or i == 'call_logs':
            if i == 'feedbacks':
                with open("./data/%s.json" % i, "w") as f:
                    cursor = database[i].find({})
                    for document in cursor:
                        u = "{:%d/%m/%Y/%H/%M/%S}".format(document['updated_at'])
                        c = "{:%d/%m/%Y/%H/%M/%S}".format(document['updated_at'])
                        document['updated_at'] = time.mktime(
                            datetime.datetime.strptime(u, "%d/%m/%Y/%H/%M/%S").timetuple())
                        document['created_at'] = time.mktime(
                            datetime.datetime.strptime(c, "%d/%m/%Y/%H/%M/%S").timetuple())
                        document = json.dumps(document, default=json_util.default)
                        f.write(document + '\n')
                    f.close()
            else:
                with open("./data/%s.json" % i, "w") as f:
                    cursor = database[i].find({})
                    for document in cursor:
                        u = "{:%d/%m/%Y/%H/%M/%S}".format(document['updated_at'])
                        c = "{:%d/%m/%Y/%H/%M/%S}".format(document['updated_at'])
                        document['updated_at'] = time.mktime(
                            datetime.datetime.strptime(u, "%d/%m/%Y/%H/%M/%S").timetuple())
                        document['created_at'] = time.mktime(
                            datetime.datetime.strptime(c, "%d/%m/%Y/%H/%M/%S").timetuple())
                        document = json_util.dumps(document)
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
    os.system('hdfs dfs -put ./data/*  /user/hadoop/edumall/siren')


if __name__ == '__main__':
    get_database()
    getcoll()
    process_json()
    check_file()
    put_hadoop()

