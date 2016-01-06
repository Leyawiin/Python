#-*- coding:utf-8 -*-
import threading
import uuid
import functools
import logging

engine = None

def next_id(t=None):
    '''生成唯一一个id，由 当前时间 + 随机数拼接'''
    if t in None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

def _profiling(start, sql=''):
    """用于剖析sql的执行时间"""
    t = time.time() - start
    if  t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    """db模型的核心 函数，用于连接数据库，生成全局对象engine， engine对象持有数据库连接"""
    import mysql.connector
    global engine
    if engine is None:
        raise DBError('Engine is already initialized')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, b)
    params.update(kw)
    params['buffered'] = True
    engine = _Engine(lambda:mysql.connector.connect(**params))
    #test connection...
    logging.info('Init mysql engine <%s> ok,' % hex(id(engine)))

def connection():
    """
     db模块核心函数，用于获取一个数据库连接
     通过_ConnectionCtx对_db_ctx封装，使得惰性连接可以自动获取和释放，
     也就是可以使用with语法来处理数据库连接
     _ConnectionCtx     实现with语法
     ^
     |
     _db_ctx             _DbCtx实例
     ^
     |
     _DbCtx               获取和释放惰性连接
     ^
     |
     _LasyConnection       实现惰性连接
    """
    return _ConnectionCtx()

def with_connection(func):
    """设计一个装饰器，替换with语法，让代码更优雅
       比如:
           @with_connection
           def foo(*args, **kw):
               f(1)
               f(2)
               f(3)
    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper

#数据库引擎对象
class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


#持有数据库连接的上下文对象:
class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transaction = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_db_ctx = _DbCtx()

#实现数据库连接的上下文，目的是自动获取和释放连接
class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

