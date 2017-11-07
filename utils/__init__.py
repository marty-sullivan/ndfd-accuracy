import csv
import gzip
import logging
import shapefile
import subprocess
import yaml
from boto3.session import Session
from botocore.exceptions import ClientError, WaiterError
from datetime import datetime, timedelta
from glob import glob
from os import path, remove
from pyproj import Proj
from sys import stdout
from urllib2 import urlopen

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
        with open('./isd-history.csv', 'rb') as csvHistory:
            stations = csv.DictReader(csvHistory)
            startTime = datetime.strptime('20151001', '%Y%m%d')
            endTime = datetime.strptime('20171031', '%Y%m%d')

            p = Proj('+proj=utm +zone=18T, +north +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
            w = shapefile.Writer(shapeType=shapefile.POINTZ)
            
            w.field('id', 'C', size=15)
            w.field('name', 'C', size=30)
            w.field('lat', 'N', decimal=3)
            w.field('lon', 'N', decimal=3)
            w.field('elev', 'N', decimal=1)

            skipStations = ['999999-04728', '725186-99999', '725283-99999']

            stationIds = []
            for station in stations:
                if station['STATE'] == 'NY':
                    begin = datetime.strptime(station['BEGIN'], '%Y%m%d')
                    end = datetime.strptime(station['END'], '%Y%m%d')

                    if begin <= startTime and end >= endTime:
                        stationId = '{USAF}-{WBAN}'.format(USAF=station['USAF'], WBAN=station['WBAN'])

                        if stationId in skipStations:
                            continue

                        stationIds.append(stationId)

                        lat = float(station['LAT'])
                        lon = float(station['LON'])
                        ele = float(station['ELEV(M)'])
                        x, y = p(lon, lat)
                        w.point(x, y, ele)
                        w.record(stationId, station['STATION NAME'], lat, lon, ele)

            w.save('./output/ny_asos')
            return stationIds

    def retrieve_observations(self, stations, years):
        localLocation = './data/{Station}-{Year}'
        ftpLocation = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/{Year}/{Station}-{Year}.gz'

        localZips = []
        for station in stations:
            for year in years:
                localFile = localLocation.format(Station=station, Year=year)

                if not path.exists(localFile) and not path.exists(localFile + '.gz'):
                    self.logger.info('Downloading {File}'.format(File=localFile))
                    localZips.append(localFile)
                    remoteFile = urlopen(ftpLocation.format(Station=station, Year=year))

                    with open(localFile + '.gz', 'wb') as localZipFile:
                        localZipFile.write(remoteFile.read())

        for localZip in glob('./data/*.gz'):
            with gzip.open(localZip, 'rb') as zipFile:
                with open(localZip[:-3], 'wb') as localFile:
                    self.logger.info('Unzipping {File}'.format(File=localZip))
                    for line in zipFile:
                        localFile.write(line)

            remove(localZip)

