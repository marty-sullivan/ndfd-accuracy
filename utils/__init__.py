import csv
import gzip
import json
import logging
import pandas
import shapefile
import sqlite3
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

    def parse_asos_stations(self, startTime, endTime):
        with open('./isd-history.csv', 'rb') as csvHistory:
            stations = csv.DictReader(csvHistory)

            p = Proj('+proj=utm +zone=18T, +north +ellps=GRS80 +datum=NAD83 +units=m +no_defs')
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

    def retrieve_asos_observations(self, stations, years):
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

        if path.exists('./data/obs.sqlite'):
            remove('./data/obs.sqlite')
        sqlconn = sqlite3.connect('./data/obs.sqlite')
        c = sqlconn.cursor()
        c.execute((
            'CREATE TABLE station_hours ('
                'stationId TEXT,'
                'date INT,'
                'temp FLOAT,'
                'qpf1 FLOAT,'
                'qpf6 FLOAT,'
                'PRIMARY KEY(stationId, date)'
            ');'
        ))

        colspec = [(0, 4), (5, 7), (8, 10), (11, 14), (13, 20), (19, 25), (25, 32), (31, 38), (37, 44), (43, 50), (49, 56), (55, 62)]
        headers = ['year', 'month', 'day', 'hour', 'temp', 'dpt', 'pres', 'wdir', 'wspd', 'skyc', 'qpf1', 'qpf6']
        missing = '-9999'

        for dataFile in glob('./data/*'):
            stationId = dataFile[7:-5]
            stationYear = pandas.read_fwf(dataFile, colspecs=colspec, names=headers)
            for i, row in stationYear.iterrows():
                return
                hour = datetime.strptime(row['year'] + row['month'] + row['day'] + row['hour'], '%Y%m%d%H')
                #c.execute(
                #    'INSERT INTO station_hours (stationId, date, temp, qpf1, qpf6) VALUES ('
                #        stationId
                #        hour.strftime(
                #    ');'
                #)

    def retrieve_ghcn_observations(self, years):
        ftpYear = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{Year}.csv.gz'
        localYear = './data/{Year}.csv'

        localZips = []
        for year in years:
            localFile = localYear.format(Year=year)
            
            if not path.exists(localFile) and not path.exists(localFile + '.gz'):
                self.logger.info('Downloading {File}'.format(File=localFile))
                localZips.append(localFile)
                remoteFile = urlopen(ftpYear.format(Year=year))

                with open(localFile + '.gz', 'wb') as localZipFile:
                    localZipFile.write(remoteFile.read())

        for localZip in glob('./data/*.gz'):
            with gzip.open(localZip, 'rb') as zipFile:
                with open(localZip[:-3], 'wb') as localFile:
                    self.logger.debug('Unzipping {File}'.format(File=localZip))
                    for line in zipFile:
                        localFile.write(line)

            remove(localZip)

    def parse_ghcn_stations(self, startTime, endTime):
        stationFile = './data/ghcnd-stations.txt'
        inventoryFile = './data/ghcnd-inventory.txt'
        ftpStations = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
        ftpInventory = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt'        

        startYear = int(startTime.strftime('%Y'))
        endYear = int(endTime.strftime('%Y'))

        remoteFile = urlopen(ftpStations)
        with open(stationFile, 'w') as localFile:
            localFile.write(remoteFile.read())

        remoteFile = urlopen(ftpInventory)
        with open(inventoryFile, 'w') as localFile:
            localFile.write(remoteFile.read())

        stationSpec = [(0, 11), (12, 20), (21, 30), (31, 37), (38, 40), (41, 71), (72, 75), (76, 79), (80, 85)]
        stationHead = ['id', 'lat', 'lon', 'elev', 'st', 'name', 'gsn', 'hcncrn', 'wmo']
        inventorySpec = [(0, 11), (12, 20), (21, 30), (31, 35), (36, 40), (41, 45)]
        inventoryHead = ['id', 'lat', 'lon', 'ele', 'start', 'end']

        stations = pandas.read_fwf(stationFile, colspecs=stationSpec, names=stationHead)
        inventory = pandas.read_fwf(inventoryFile, colspecs=inventorySpec, names=inventoryHead)

        p = Proj('+proj=utm +zone=18T, +north +ellps=GRS80 +datum=NAD83 +units=m +no_defs')

        pointz = shapefile.Writer(shapeType=shapefile.POINTZ)
        pointz.field('stationId', 'C', size=11)
        pointz.field('name', 'C', size=30)
        pointz.field('lat', 'N', decimal=4)
        pointz.field('lon', 'N', decimal=4)
        pointz.field('elev', 'N', decimal=1)
        
        nyStations = []
        for i, row in stations.iterrows():
            if row['st'] == 'NY':
                inv = inventory.loc[(inventory['id'] == row['id']) & (inventory['ele'] == 'PRCP') & (inventory['start'] <= startYear) & (inventory['end'] >= endYear)]
                if not inv.empty:
                    nyStations.append(row['id'])
                    x, y = p(row['lon'], row['lat'])
                    pointz.point(x, y, row['elev'])
                    pointz.record(row['id'], row['name'], row['lat'], row['lon'], row['elev'])

        self.logger.info('{Count} valid stations in NYS.'.format(Count=len(nyStations)))
        pointz.save('./output/ny_ghcn')

        with open('./data/stations.json', 'w') as stationsJson:
            stationsJson.write(json.dumps(nyStations))

    def parse_ghcn_observations(self, ele, years, startTime, endTime):
        yearHeaders = ['id', 'date', 'ele', 'val', 'time']

        with open('./data/stations.json', 'r') as stationsJson:
            stations = json.load(stationsJson)

        for year in years:
            self.logger.info('Reading Year: ' + year)
            yearFile = './data/{Year}.csv'.format(Year=year)
            
            with open(yearFile, 'r') as yearCsv:
                reader = csv.DictReader(yearCsv, fieldnames=yearHeaders)

                for row in reader:
                    if row['ele'] == ele:
                        obsTime = datetime.strptime(row['date'], '%Y%m%d')
                        if obsTime >= startTime and obsTime <= endTime and row['id'] in stations:
                            print row
