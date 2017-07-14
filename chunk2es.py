#!/usr/bin/env python
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from datetime import datetime
import elasticsearch
import subprocess
import argparse
import textwrap
import logging
import shutil
import codecs
import json
import time
import glob
import sys
import os

reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(level=logging.WARNING)


CACHE_PATH = 'cache'


def init_parser():
    Doc = '''
    '''

    epilog = '''
    '''
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            "tool for export data from file to elasticsearch\n\n" + Doc),
        epilog=epilog)
    parser.add_argument(
        '-f', '--input-file', dest='file', help='read ip addresses from a txt file')
    parser.add_argument('-c', '--config', help='specify config file about ES')
    return parser


def read_config(CONFIG):
    with open(CONFIG, 'rb') as fp:
        conf = json.load(fp)
    hosts = conf['hosts']
    delimiter = conf['delimiter']
    headline = conf['headline']
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
            'headline': headline
        }
    else:
        raise ValueError('error config')


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


class TaskInfo(object):
    """Flag"""

    def __init__(self):
        super(TaskInfo, self).__init__()
        self.task_info = 'tasks.info'

    def exists(self):
        try:
            with open(self.task_info, 'rb'):
                return True
        except IOError:
            return False

    def create(self):
        if not self.exists():
            with open(self.task_info, 'wb') as f:
                f.write(codecs.BOM_UTF8)

    def get(self):
        try:
            with open(self.task_info, 'rb') as f:
                return [line.strip() for line in f]
        except IOError:
            raise IOError('tasks.info does not exists')

    def complete(self, input_file):
        '''complement mark
        '''
        data = self.get()
        if not data:
            data = []
        data.append(input_file)
        with open(self.task_info, 'wb+') as f:
            f.write('\n'.join(data))


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

    def clean(self):
        if self.exists():
            map(os.remove, glob.glob(self.pattern))


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


def sync(es, chunk, conf, stats_only=True, **kwargs):
    response = streaming_bulk(es, gen_data(chunk, conf, **kwargs))
    status = [ok for ok in response]
    return len(filter(lambda x: True if x else None, status))


if __name__ == '__main__':
    parser = init_parser()
    args = parser.parse_args()
    input_file = args.file
    conf = args.config

    if len(sys.argv) < 3:
        print '!-> few argumets'
        parser.print_usage()
        sys.exit(2)
    if not input_file and not conf:
        print '!-> no input file or not specify config'
        parser.print_usage()
        sys.exit(2)
    try:
        with open(conf) as fp:
            conf = json.load(fp)
    except IOError:
        print '!-> cannot read specific config file'
        parser.print_usage()
        sys.exit(3)

    cache = Cache(CACHE_PATH)
    taskinfo = TaskInfo()

    if cache.exists():
        print 'read cache...'
        chunks = cache.get()
    elif not taskinfo.exists() or input_file not in taskinfo.get():
        taskinfo.create()
        print 'generating file chunks...'
        chunks = gen_chunks(input_file)
    else:
        print '!-> duplicate file, \"{}\" has export to ES'.format(input_file)
        sys.exit(6)
    # create a connection to ES
    try:
        es = Elasticsearch(hosts=conf['hosts'])
        es.ping()
    except elasticsearch.exceptions.ConnectionError:
        logging.ERROR(
            '!-> error connection: cannot ping elasticsearch cluster')
        sys.exit(5)

    print 'starting sync to ES...'
    start_time = datetime.now()
    start_timestamp = time.time()
    while chunks:
        chunk = chunks.pop()
        success = sync(es, chunk, conf)
        logging.debug('success - num: {}'.format(success))
        if success:
            clean_chunk(chunk)
    print 'mark \"{}\"" as completion in task.info'.format(input_file)
    taskinfo.complete(input_file)
    print '---' * 30
    print 'start    at:  {}'.format(start_time.ctime())
    print 'complete at:  {}'.format(datetime.now().ctime())
    print 'spent time: {}s'.format(round(time.time() - start_timestamp, 2))
