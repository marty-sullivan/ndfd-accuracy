#!/usr/bin/env python

import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta
from os import path
from sys import exit
from utils import GoUtils

parser = ArgumentParser(description='')

parser.add_argument('--start', type=str, default='20151001', help='Valid begin period of a GHCN station')
parser.add_argument('--end', type=str, default='20171031', help='Valid end period of a GHCN station')
parser.add_argument('--states', nargs='+', default=['NY'], help='US States to parse')
parser.add_argument('--elements', nargs='+', default=['PRCP', 'MINT', 'MAXT'], help='Weather elements to parse')
parser.add_argument('--retrieve-observations', action='store_true', help='Retrieve yearly GHCN observations')
parser.add_argument('--retrieve-forecasts', type=str, help='Retrieve NDFD forecast variable')
parser.add_argument('--parse-stations', action='store_true', help='Parse valid GHCN stations in states')
parser.add_argument('--parse-observations', action='store_true', help='Parse observations for elements') 
parser.add_argument('--parse-forecasts', action='store_true', help='Parse forecasts for stations')
parser.add_argument('--analyze', action='store_true', help='Analyze NDFD Forecast Accuracy')
parser.add_argument('--create-shapefiles', action='store_true', help='Create accuracy shapefiles')

parser.add_argument('--profile', type=str, default=None, help='The AWS account profile to use. Only needed if not on an EC2 instance in the desired account with the proper Instance Profile')
parser.add_argument('--region', type=str, default='us-east-1', help='The AWS region to use. Default: us-east-1')
parser.add_argument('--debug', action='store_true', help='Set more verbose logging')

args = parser.parse_args()
utils = GoUtils(args)


startTime = datetime.strptime(args.start, '%Y%m%d')
endTime = datetime.strptime(args.end, '%Y%m%d')

if args.retrieve_observations:
    utils.retrieve_ghcn_observations()

if args.retrieve_forecasts:
    utils.retrieve_ndfd_forecasts()

if args.parse_stations:
    utils.parse_ghcn_stations()

if args.parse_observations:
    utils.parse_ghcn_observations()

if args.parse_forecasts:
    utils.parse_ndfd_forecasts()

if args.analyze:
    utils.analyze_accuracy()

if args.create_shapefiles:
    utils.create_accuracy_shapefiles()
