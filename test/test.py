#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json


def read_config(CONFIG):
    with open(CONFIG, 'rb') as fp:
        conf = json.load(fp)
    hosts = conf['hosts']
    delimiter = conf['delimiter']
    headline = conf['headline']
    index = conf['_index']
    doc_type = conf['_type']
    es_id = conf['_id']
    ignore_fileds = conf['ingore']
    if es_id in headline and hosts and delimiter and index and doc_type:
        return {
            'hosts': hosts,
            '_index': index,
            '_type': doc_type,
            '_id': es_id,
            'delimiter': delimiter,
            'headline': headline,
            'ingore': ignore_fileds
        }
    else:
        raise ValueError('error config')


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


conf = read_config('es.conf')
print gen_data('test.txt', conf).next()
