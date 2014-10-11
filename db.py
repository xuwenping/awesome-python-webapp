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

def
