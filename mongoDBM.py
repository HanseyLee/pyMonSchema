# -*- coding:utf-8 -*-
import os
import logging
import pprint as pt
import multiprocessing

from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from pymongo.errors import ConnectionFailure
from pymongo.errors import CollectionInvalid
from bson.code import Code


MONGO_URI_DEFAULT = 'mongodb://localhost:27017/admin'
URI_CLIENT_DICT = {}


def get_mongo_client(uri=MONGO_URI_DEFAULT, fork=False, **kwargs):
    """Get pymongo.mongo_client.MongoClient instance. One mongodb uri, one client.

    @:param uri: mongodb uri
    @:param fork: for fork-safe in multiprocess case, if fork=True, return a new MongoClient instance, default False.
    @:param kwargs: refer to pymongo.mongo_client.MongoClient kwargs
    """
    if fork:
        return new_mongo_client(uri, **kwargs)
    global URI_CLIENT_DICT
    matched_client = URI_CLIENT_DICT.get(uri)
    if matched_client is None:      # no matched client
        new_client = new_mongo_client(uri, **kwargs)
        if new_client is not None:
            URI_CLIENT_DICT[uri] = new_client
        return new_client
    return matched_client


def new_mongo_client(uri, **kwargs):
    """Create new pymongo.mongo_client.MongoClient instance. DO NOT USE IT DIRECTLY."""

    try:
        client = MongoClient(uri, maxPoolSize=1024, **kwargs)
        client.admin.command('ismaster')    # The ismaster command is cheap and does not require auth.
    except ConnectionFailure:
        logging.error("new_mongo_client(): Server not available, Please check you uri: {}".format(uri))
        return None
    else:
        return client


def get_existing_db(client, db_name):
    """Get existing pymongo.database.Database instance.

    @:param client: pymongo.mongo_client.MongoClient instance
    @:param db_name: database name wanted
    """

    if client is None:
        logging.error('client {} is None'.format(client))
        return None
    try:
        db_available_list = client.list_database_names()
    except PyMongoError as e:
        logging.error('client: {}, db_name: {}, client.list_database_names() error: {}'.
                      format(client, db_name, repr(e)))
    else:
        if db_name not in db_available_list:
            logging.error('client {} has no db named {}'.format(client, db_name))
            return None
    db = client.get_database(db_name)
    return db


def get_existing_coll(db, coll_name):
    """Get existing pymongo.collection.Collection instance.

    @:param client: pymongo.mongo_client.MongoClient instance
    @:param coll_name: collection name wanted
    """

    if db is None:
        logging.error('db {} is None'.format(db))
        return None
    try:
        coll_available_list = db.list_collection_names()
    except PyMongoError as e:
        logging.error('db: {}, coll_name: {}, db.list_collection_names() error: {}'.
                      format(db, coll_name, repr(e)))
    else:
        if coll_name not in coll_available_list:
            logging.error('db {} has no collection named {}'.format(db, coll_name))
            return None
    coll = db.get_collection(coll_name)
    return coll


class Operation:
    """Operation for constructing sequential pipeline. Only used in DBManager.session_pipeline() or transaction_pipeline().

    constructor parameters:
    level: <'client' | 'db' | 'coll'> indicating different operation level, MongoClient, Database, Collection
    operation_name: Literally, the name of operation on specific level
    args: position arguments the operation need. Require the first parameter or a tuple of parameters of the operation.
    kwargs: key word arguments the operation need.

    examples:
    # pymongo.collection.Collection.find(filter, projection, skip=None, limit=None,...)
    Operation('coll', 'find', {'x': 5}) only filter parameter, equivalent to:
    Operation('coll', 'find', args={'x': 5}) or Operation('coll', 'find', kwargs={filter: {'x': 5}})

    Operation('coll', 'find', ({'x': 5},{'_id': 0}) {'limit':100}), equivalent to:
    Operation('coll', 'find', args=({'x': 5},{'_id': 0}, None, {'limit':100}) ), OR
    Operation('coll', 'find', kwargs={'filter':{'x': 5}, 'projection': {'_id': 0},'limit':100})
    """

    def __init__(self, level, operation_name, args=(), kwargs={}, callback=None):
        self.level = level
        self.operation_name = operation_name
        self.args = args
        if kwargs is None:
            self.kwargs = None
        else:
            self.kwargs = kwargs
        self.callback = callback
        self.out = None

    # def __str__(self):
    #     return


class DBManager:
    """A safe and simple pymongo packaging class ensuring existing database and collection.

    Operations:
    MongoClient level operations: https://api.mongodb.com/python/current/api/pymongo/mongo_client.html
    Database level operations: https://api.mongodb.com/python/current/api/pymongo/database.html
    Collection level operations: https://api.mongodb.com/python/current/api/pymongo/collection.html

    examples:
    var dbm = DBManager('mongodb://localhost:27017/admin', 'testDB', 'testCollection')

    # MongoClient(host=['localhost:27019'], document_class=dict, tz_aware=False,
    # connect=True, maxpoolsize=1024)
    print(dbm.client)

    # Database(MongoClient(host=['localhost:27019'], document_class=dict, tz_aware=False,
    # connect=True, maxpoolsize=1024), 'testDB')
    print(dbm.db)

    # Collection(Database(MongoClient(host=['localhost:27019'], document_class=dict,
    # tz_aware=False, connect=True, maxpoolsize=1024), 'testDB'), 'testCollection')
    print(dbm.coll)

    # change db or coll
    dbm.db_name = 'test'
    dbm.coll_nmae = 'test'
    # Collection(Database(MongoClient(host=['localhost:27019'], document_class=dict,
    # tz_aware=False, connect=True, maxpoolsize=1024), 'test'), 'test')
    print(dbm.coll)

    # simple manipulation operation
    dbm.coll.insert_one({'hello': 'world'})
    print(dbm.coll.find_one())   # {'_id': ObjectId('...'), 'hello': 'world'}

    # bulk operation
    from pymongo import InsertOne, DeleteOne, ReplaceOne, ReplaceOne
    dbm.bulk_write([InsertOne({'y':1}), DeleteOne({'x':1}), ReplaceOne({{'w':1}, {'z':1}, upsert=True})])

    # simple managing operation
    dbm.coll.create_index([('hello', pymongo.DESCENDING)], background=True)
    dbm.client.list_database_names()
    dbm.db.list_collection_names()


    # MapReduce
    r"
    mapper = Code('''
    function () {...}
    ''')
    reducer = Code('''
    function (key, value) {...}
    ''')
    rst = dbm.coll.inline_map_reduce(mapper, reducer)
    "

    # causal-consistency session or transaction pipeline operation
    def cursor_callback(cursor):
        return cursor.distinct('hello')
    op_1 = Operation('coll', 'insert_one', {'hello': 'heaven'})
    op_2 = Operation('coll', 'insert_one', {'hello': 'hell'})
    op_3 = Operation('coll', 'insert_one', {'hello': 'god'})
    op_4 = Operation('coll', 'find', kwargs={'limit': 2}, callback=cursor_callback)
    op_5 = Operation('coll', 'find_one', {'hello': 'god'})
    pipeline = [op_1, op_2, op_3, op_4, op_5]
    rst = dbm.transaction_pipeline(pipeline) # only on replica set deployment
    # rst = dbm.session_pipeline(pipeline) # can be standalone, replica set or sharded cluster.
    for op in rst:
        print(op.out)


    # multiprocess
    def func():
        # new process, new client with fork=True parameter.
        dbm2 = DBManager('mongodb://anotherhost/admin', 'test', 'test', fork=True)
        # Do something with db.
        pass
    proc = multiprocessing.Process(target=func)
    proc.start()

    """
    __default_uri = 'mongodb://localhost:27017/admin'
    __default_db_name = 'test'
    __default_coll_name = 'test'

    def __init__(self, uri=__default_uri, db_name=__default_db_name, coll_name=__default_coll_name, **kwargs):
        self.__uri = uri
        self.__db_name = db_name
        self.__coll_name = coll_name
        self.__client = get_mongo_client(uri, **kwargs)
        self.__db = get_existing_db(self.__client, db_name)
        self.__coll = get_existing_coll(self.__db, coll_name)

    def __str__(self):
        return u'uri: {}, db_name: {}, coll_name: {}, id_client: {}, client: {}, db: {}, coll: {}'.format(
            self.uri, self.db_name, self.coll_name, id(self.client), self.client, self.db, self.coll)

    @property
    def uri(self):
        return self.__uri

    @property
    def db_name(self):
        return self.__db_name

    @property
    def coll_name(self):
        return self.__coll_name

    @db_name.setter
    def db_name(self, db_name):
        self.__db_name = db_name
        self.__db = get_existing_db(self.__client, db_name)

    @coll_name.setter
    def coll_name(self, coll_name):
        self.__coll_name = coll_name
        self.__coll = get_existing_coll(self.__db, coll_name)

    @property
    def client(self):
        return self.__client

    @property
    def db(self):
        return self.__db

    @property
    def coll(self):
        # always use the current instance self.__db
        self.__coll = get_existing_coll(self.__db, self.__coll_name)
        return self.__coll

    def create_coll(self, db_name, coll_name):
        """Create new collection with new or existing database"""
        if self.__client is None:
            return None
        try:
            return self.__client.get_database(db_name).create_collection(coll_name)
        except CollectionInvalid:
            logging.error('collection {} already exists in database {}'.format(coll_name, db_name))
            return None

    def session_pipeline(self, pipeline):
        if self.__client is None:
            logging.error('client is None in session_pipeline: {}'.format(self.__client))
            return None
        with self.__client.start_session(causal_consistency=True) as session:
            result = []
            for operation in pipeline:
                try:
                    if operation.level == 'client':
                        target = self.__client
                    elif operation.level == 'db':
                        target = self.__db
                    elif operation.level == 'coll':
                        target = self.__coll

                    operation_name = operation.operation_name
                    args = operation.args
                    kwargs = operation.kwargs
                    operator = getattr(target, operation_name)
                    if type(args) == tuple:
                        ops_rst = operator(*args, session=session, **kwargs)
                    else:
                        ops_rst = operator(args, session=session, **kwargs)

                    if operation.callback is not None:
                        operation.out = operation.callback(ops_rst)
                    else:
                        operation.out = ops_rst

                except Exception as e:
                    logging.error('{} {} Exception, session_pipeline args: {}, kwargs: {}'.format(
                        target, operation, args, kwargs))
                    logging.error('session_pipeline Exception: {}'.format(repr(e)))
                result.append(operation)
            return result

    # https://api.mongodb.com/python/current/api/pymongo/client_session.html#transactions
    def transaction_pipeline(self, pipeline):
        if self.__client is None:
            logging.error('client is None in transaction_pipeline: {}'.format(self.__client))
            return None
        with self.__client.start_session(causal_consistency=True) as session:
            with session.start_transaction():
                result = []
                for operation in pipeline:
                    try:
                        if operation.level == 'client':
                            target = self.__client
                        elif operation.level == 'db':
                            target = self.__db
                        elif operation.level == 'coll':
                            target = self.__coll
                        operation_name = operation.operation_name
                        args = operation.args
                        kwargs = operation.kwargs
                        operator = getattr(target, operation_name)
                        if type(args) == tuple:
                            ops_rst = operator(*args, session=session, **kwargs)
                        else:
                            ops_rst = operator(args, session=session, **kwargs)

                        if operation.callback is not None:
                            operation.out = operation.callback(ops_rst)
                        else:
                            operation.out = ops_rst
                    except Exception as e:
                        logging.error('{} {} Exception, transaction_pipeline args: {}, kwargs: {}'.format(
                            target, operation, args, kwargs))
                        logging.error('transaction_pipeline Exception: {}'.format(repr(e)))
                        raise Exception(repr(e))
                    result.append(operation)
                return result


# if __name__ == '__main__':
#     logging.basicConfig(level=logging.DEBUG,
#                         format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                         datefmt='%a, %d %b %Y %H:%M:%S',
#                         filename='myapp.log',
#                         filemode='w')
