import logging
import pandas
import re 
import shapefile
import subprocess
import yaml
from boto3.session import Session
from botocore.exceptions import ClientError, WaiterError
from datetime import datetime, timedelta
from os import path, remove
from pyproj import Proj
from sys import stdout
from troposphere.template_generator import TemplateGenerator

class GoUtils:

    def __init__(self, args):
        self.args = args
        self.run_time = datetime.now().strftime('%Y%m%d%H%M%S')
        self.acceptable_stack_states = ['CREATE_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE']
        self.init_logging()
        self.init_aws()

        with open('config.yml', 'r') as frameworkYAML:
            self.config = yaml.load(frameworkYAML)

    def init_aws(self):
        if self.args.profile:
            self.aws = Session(profile_name=self.args.profile, region_name=self.args.region)
        else:
            self.aws = Session(region_name=self.args.region)

        self.cfn = self.aws.resource('cloudformation')
        self.ddb = self.aws.resource('dynamodb')
        self.s3 = self.aws.resource('s3')

    def init_logging(self):
        log_levels = { 'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL }

        self.logger = logging.getLogger()
        self.logger.setLevel(log_levels[self.args.log_level])

        sh = logging.StreamHandler(stdout)
        sh.setLevel(log_levels[self.args.log_level])
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(message)s')
        sh.setFormatter(formatter)
        self.logger.addHandler(sh)

        logging.getLogger('boto').propagate = False
        logging.getLogger('boto3').propagate = False
        logging.getLogger('botocore').propagate = False

    def parse_stations(self):
        colspec = [(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71), (72, 75), (76, 79), (80, 85)]
        names = ['id', 'lat', 'lon', 'elev', 'st', 'name', 'gsn', 'hcncrn', 'wmo']

        data = pandas.read_fwf('ghcnd-stations.txt', colspecs=colspec, names=names)
        p = Proj('+proj=utm +zone=18T, +north +ellps=WGS84 +datum=WGS84 +units=m +no_defs')

        w = shapefile.Writer(shapeType=shapefile.POINTZ)
        w.field('stationId', 'C', size=11)
        w.field('stationName', 'C', size=30)
        w.field('lat', 'N', decimal=4)
        w.field('lon', 'N', decimal=4)
        w.field('elev', 'N', decimal=1)

        count = 0
        for i, row in data.iterrows():
            if row['st'] == 'NY':
                count += 1
                x, y = p(row['lon'], row['lat'])
                w.point(x, y, row['elev'])
                w.record(row['id'], row['name'], row['lat'], row['lon'], row['elev'])

        self.logger.info('{Count} stations in NYS.'.format(Count=count))
        w.save('./shapefiles/ny.shp')
            
