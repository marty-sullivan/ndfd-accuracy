#!/usr/bin/env python

import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta
from os import path
from utils import GoUtils

parser = ArgumentParser(description='')

parser.add_argument('--start', type=str, default='20151001', help='The valid begin period of an ASOS station')
parser.add_argument('--end', type=str, default='20171031', help='The valid end period of an ASOS station')

parser.add_argument('--profile', type=str, default=None, help='The AWS account profile to use. Only needed if not on an EC2 instance in the desired account with the proper Instance Profile')
parser.add_argument('--region', type=str, default='us-east-1', help='The AWS region to use. Default: us-east-1')
parser.add_argument('--log_level', type=str, default='INFO', help='Log Level for Go Script. Default: INFO')

args = parser.parse_args()
utils = GoUtils(args)









startTime = datetime.strptime(args.start, '%Y%m%d')
endTime = datetime.strptime(args.end, '%Y%m%d')
years = ['2015', '2016', '2017']

utils.retrieve_ghcn_observations(years)
if not path.exists('./data/stations.json'):
    utils.parse_ghcn_stations(startTime, endTime)

utils.parse_ghcn_observations('PRCP', years, startTime, endTime)
