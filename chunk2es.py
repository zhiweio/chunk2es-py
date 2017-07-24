#!/usr/bin/env python
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from datetime import datetime
import elasticsearch
import subprocess
import logging
import shutil
import json
import time
import glob
import sys
import os


reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(level=logging.ERROR)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CACHE_PATH = os.path.join(BASE_DIR, 'cache')
TASK_INFO = os.path.join(BASE_DIR, 'tasks.info')


def _cli_parse(args):
    DOC = '''
    "tool for export data from large file to elasticsearch\n\n"
    '''
    from argparse import ArgumentParser, RawDescriptionHelpFormatter
    import textwrap
    parser = ArgumentParser(
        prog=args[0], formatter_class=RawDescriptionHelpFormatter,
        description=textwrap.dedent(DOC))
    opt = parser.add_argument
    opt('-f', '--input-file', dest='file',
        help='input file, export data to elastics')
    opt('-c', '--config', help='specify config file about elastics')
    cli_args = parser.parse_args(args[1:])
    return cli_args, parser


def quit(func):
    # decorator for quitting elegantly
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            print '\nquit :)'
            sys.exit(0)
    return wrapper


class Cache(object):
    """Cache"""

    def __init__(self, cache_path):
        super(Cache, self).__init__()
        self.pattern = cache_path + '/chunk_*'

    def exists(self):
        if glob.glob(self.pattern):
            return True
        return False

    def get(self):
        if self.exists():
            return glob.glob(self.pattern)


class TaskList(object):
    """task list:
    """

    def __init__(self, task_info):
        super(TaskList, self).__init__()
        self.task_info = task_info

    def exists(self):
        return os.path.exists(self.task_info)

    def create(self):
        if not self.exists():
            obj = {
                'complete': []
            }
            with open(self.task_info, 'wb') as fp:
                json.dump(obj, fp)
            return True
        return False

    def get(self):
        try:
            with open(self.task_info, 'rb') as fp:
                data = json.load(fp)
                return data
        except (IOError, ValueError):
            # tasks.info does not exist
            # no tasks found
            return {"complete": set()}

    def mark_complete(self, input_file):
        '''complement mark:
        success:
        >>> tasklist.mark_complete('test.txt')
        >>> None
        failed:
        >>> tasklist.mark_complete('test.txt')
        >>> TypeError: set(['test.txt']) is not JSON serializable
        '''
        tasks = self.get()
        tasks['complete'].append(input_file)
        with open(self.task_info, 'wb') as fp:
            try:
                json.dump(tasks, fp)
                return None
            except TypeError as e:
                return e


def read_config(config_file):
    with open(config_file, 'rb') as fp:
        conf = json.load(fp)
    hosts = conf['hosts']
    delimiter = conf['delimiter']
    headline = conf['headline']
    ingore = conf['ingore']
    index = conf['_index']
    doc_type = conf['_type']
    es_id = conf['_id']
    if es_id in headline and hosts and delimiter and index and doc_type:
        return {
            'hosts': hosts,
            '_index': index,
            '_type': doc_type,
            '_id': es_id,
            'delimiter': delimiter,
            'headline': headline,
            'ingore': ingore
        }
    else:
        raise ValueError('error config')


@quit
def gen_chunks(huge_file, lines=50000):
    split_command = ['split', '-l',
                     str(lines), '-a', '5', huge_file, 'chunk_']
    try:
        subprocess.check_call(split_command)
    except subprocess.CalledProcessError as e:
        logging.error('!-> split file error: {}'.format(e))
        sys.exit(1)

    chunked_files = glob.glob('chunk_*')
    # move chunks to cache
    if not os.path.isdir(CACHE_PATH):
        os.mkdir(CACHE_PATH)
    for src in chunked_files:
        shutil.move(src, CACHE_PATH)
    return glob.glob(CACHE_PATH + '/chunk_*')


def clean_chunk(chunk):
    '''delete chunk completed from cache dir'''
    try:
        os.remove(chunk)
    except OSError:
        pass


def gen_data(chunk, conf):
    with open(chunk) as f:
        delimiter = conf['delimiter']
        headline = conf['headline']
        ingore = conf['ingore']
        # default headline is the first line of file if not specify
        headline = headline if headline else f.readline().strip().split(delimiter)
        for line in f:
            fields = line.strip().split(delimiter)
            # NOTE: only string fileds
            source = dict(zip(headline, fields))
            es_id = source[conf['_id']]
            if ingore:  # delete ingored field value
                for i in ingore:
                    del source[i]
            yield {
                "_index": conf['_index'],
                "_type": conf['_type'],
                "_id": es_id,
                "_source": source
            }


def sync(es, chunk, conf, stats_only=True, raise_on_exception=False, **kwargs):
    try:
        response = streaming_bulk(es, gen_data(chunk, conf, **kwargs))
        status = (ok for ok in response)
        # emulate standard behavior for failed actions
        # if no failure, return 0
        # otherwise, return num of failures
        return len(filter(lambda x: True if not x else None, status))
    except elasticsearch.exceptions.ConnectionError:
        logging.error('connection error -> sync to ES failed')
        sys.exit(6)


@quit
def running(es, chunks, conf):
    while chunks:
        chunk = chunks.pop()
        failed = sync(es, chunk, conf)
        if failed:
            logging.error('failed - num: {}'.format(failed))
        else:
            clean_chunk(chunk)


if __name__ == '__main__':
    opts, parser = _cli_parse(sys.argv)

    cache = Cache(CACHE_PATH)
    taskinfo = TaskList(TASK_INFO)

    if len(sys.argv) < 3:
        print '!-> few argumets'
        parser.print_usage()
        sys.exit(2)
    if not opts.file and not opts.config:
        print '!-> no input file or not specify config'
        parser.print_usage()
        sys.exit(2)
    try:
        with open(opts.config) as fp:
            conf = json.load(fp)
    except IOError:
        print '!-> cannot read specific config file'
        parser.print_usage()
        sys.exit(3)

    # create a connection to elastics
    es = Elasticsearch(hosts=conf['hosts'])
    if not es.ping():
        print '!-> cannot ping elastics, maybe occur ConnectionError'
        sys.exit(110)

    if cache.exists():
        print 'read cache...'
        chunks = cache.get()
    elif not taskinfo.exists() or opts.file not in taskinfo.get()['complete']:
        taskinfo.create()
        print 'generating file chunks...'
        chunks = gen_chunks(opts.file)
    else:
        print '!-> repeat operation, \"{}\" has synchronized to elastics'.format(opts.file)
        sys.exit(6)

    print 'syncing to elastics...'
    start_time = datetime.now()
    start_timestamp = time.time()
    running(es, chunks, conf)

    print '-> mark \"{}\"" as complete in task.info'.format(opts.file)
    if taskinfo.mark_complete(opts.file):
        # JSON serialize error
        print '!-> mark failed'
    print '---' * 30
    print 'start    at:  {}'.format(start_time.ctime())
    print 'complete at:  {}'.format(datetime.now().ctime())
    print 'spent  time: {}s'.format(round(time.time() - start_timestamp, 2))
