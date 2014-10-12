#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
Database operation module. This module is independent whit web module.
'''

import os
import re
import sys
import time
import uuid
import socket
import datetime
import functools
import threading
import logging
import collections

from utils import Dict


def next_str(t=None):
    '''
    Return next id as 50-char string.

    Args:
        t: unix timestamp, default to None and using time.time().
    '''
    if t is None:
        t = time.time()

    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

next_id = next_str


def _profiling(start, sql=''):
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


class _TransactionCtx(object):

    '''
    _TransactionCtx object that can handle transactions.

    with _TransactionCtx();
        pass
    '''

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            # needs open a connection first:
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        _log('begin transaction...' if _db_ctx.transactions == 1 else 'join current transaction...')
        return self
    
    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        _log('commit transaction...')
        try:
            _db_ctx.connection.commit()
            _log('commit ok.')
        except:
            logging.warning('commit faild. try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        _log('manually rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')

def transaction():
    '''
    Create a transaction object so can use with statement:

    with transaction():
        pass

    >>> def update_profile(id,name, rollback):
    ...     u = dict(id = id, name = name, email = '%s@test.ort' % name)
    '''
    return _TransactionCtx()

def with_transaction(func):
    '''
    A decorator that makes function around transaction.
    '''

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper

def _select(sql, first, *args):
    'execute select SQL and return unique result or list result.'
    global _db_ctx, _db_convert
    cursor = None
    if _db_convert != '?':
        sql = sql.replace('?', _db_convert)
    _log('SQL: %s, ARGS: %s' % (sql, args))
    start = time.time()
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()
        _profiling(start, sql)

@with_connection
def select_one(sql, *args):
    '''
    Execute select SQL and expected one result.
    If no result found, return None.
    If multiple result found, the first one returned.
    '''
    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
    '''
    Execute select SQL and expected one int and only one int result.
    '''
    d = _select(sql, True, *args)
    if len(d) != 1:
        raise MulticolumnError('Expect only one column.')
    return d.values()[0]

@with_connection
def select(sql, *args):
    '''
    Execute select SQL and return list or empty list if no result.
    '''
    return _select(sql, False, *args)

@with_connection
def _update(sql, args, post_fn = None):
    global _db_ctx, _db_convert
    cursor = None
    if _db_convert != '?':
        sql = sql.replace('?', _db_convert)
    _log('SQL: %s, ARGS: %s' % (sql, args))
    start = time.time()
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            # no transaction enviroment:
            _log('auto commit')
            _db_ctx.connection.commit()
            post_fn and psot_fn()
        return r
    finally:
        if cursor:
            cursor.close()
        _profiling(start, sql)

def insert(table, **kw):
    '''
    Execute insert SQL.
    '''
    cols, args = zip(*kw.iteritems())
    sql = 'insert into %s (%s) values (%s)' % (table, ','.join(cols), ','.join([_db_convert for i in range(len(cols))]))
    return _update(sql, args)

def update(sql, *args):
    '''
    Execute update SQL.
    '''
    return _update(sql, args)

def update_kw(table, where, *args, **kw):
    '''
    Execute update SQL by table, where, args and kw.
    '''

    if len(kw) == 0:
        raise ValueError('No kw args.')
    sqls = ['update', table, 'set']
    params = []
    updates = []
    for k, v in kw.iteritems():
        updates.append('%s=?' % k)
        params.append(v)
    sqls.append(', '.join(updates))
    sqls.append('where')
    sqls.append(where)
    params.extend(args)
    return update(sql, *params)

