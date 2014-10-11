#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
Database operation module. This module is independent whit web module.
'''

import os, re, sys, time, uuid, socket, datetime, functools, threading, logging, collections

from utils import Dict

def next_str(t = None):
    '''
    Return next id as 50-char string.

    Args:
        t: unix timestamp, default to None and using time.time().
    '''
    if t is None:
        t = time.time()

    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

next_id = next_str

def _profiling(start, sql = ''):
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

class DBError(exception):
    pass

class MulticolumnError(DEBrror):
    pass

def _log(s):
    logging.debug(s)

def _dummy_connect():
    '''
    Connect function used for get db connection. This function will be relocated in init(dbn, ...).
    '''
    raise DBError('Database is not initialized. Call init(dbn, ...) first.')

_db_connect = _dummy_connect
_db_convert = '?'

class _LasyConnection(object):
    def __init__(self):
        self.connect = None

    def cursor(self):
        if self.connection is None:
            _log('open connection...')
            self.connection = _db_connect()
        return self.connection.cursor()
    
    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            _log('close connection...')
            connection.close()

class _DbCtx(threading.local):
    '''
    Thread local object that holds connection info
    '''

    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        _log('open lazy connection...')
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()

    def cursor(self):
        '''
        Return cursor
        '''
        return self.connection.cursor()

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
    '''
    _ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most outer connection has effect.
    with connection():
        pass
        with connection():
            pass
    '''
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def _exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    '''
    Return _ConnectionCtx object that can be used by 'with' statement:
    with connection():
        pass
    '''
    return _ConnectionCtx()

def with_connection(func):
    '''
    Decorator for reuse connection.

    @with_connection
    def foo(*args, **kw):
        f1()
        f2()
        f3()

    '''

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper


