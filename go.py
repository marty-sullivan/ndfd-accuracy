#!/usr/bin/env python

import logging
from argparse import ArgumentParser
from utils import GoUtils

parser = ArgumentParser(description='Cornell University AWS Login Service Deployment Framework')

parser.add_argument('--profile', type=str, default=None, help='The AWS account profile to use. Only needed if not on an EC2 instance in the desired account with the proper Instance Profile')
parser.add_argument('--region', type=str, default='us-east-1', help='The AWS region to use. Default: us-east-1')
parser.add_argument('--log_level', type=str, default='INFO', help='Log Level for Go Script. Default: INFO')

args = parser.parse_args()
utils = GoUtils(args)

stations = utils.parse_stations()
utils.retrieve_observations(stations, ['2015', '2016', '2017'])

