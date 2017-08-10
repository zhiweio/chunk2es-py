#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import sys


# def _cli_parse(args):
#     DOC = '''
#     "tool for export data from large file to elasticsearch\n\n"
#     '''
#     from argparse import ArgumentParser
#     import argparse
#     import textwrap
#     parser = ArgumentParser(
#         prog=args[0], formatter_class=argparse.RawDescriptionHelpFormatter,
#         description=textwrap.dedent(DOC))
#     opt = parser.add_argument
#     opt('-f', '--input-file', dest='file',
#         help='read ip addresses from a txt file')
#     opt('-c', '--config', help='specify config file about elastics')
#     cli_args = parser.parse_args(args[1:])
#     return cli_args, parser


# opts, parser = _cli_parse(sys.argv)
# print opts.config

# with open('sample00.txt') as f:
#     for i, item in enumerate(f.readline().split('\t')):
#         print i + 1, item
