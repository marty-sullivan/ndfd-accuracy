import csv
import gzip
import json
import logging
import pandas
import pygrib
import shapefile
import sqlite3
import yaml
from boto3.session import Session
from botocore.exceptions import ClientError, WaiterError
from datetime import datetime, timedelta
from ftplib import FTP
from glob import glob
from multiprocessing import cpu_count, Process, JoinableQueue as Queue
from numpy.ma.core import MaskedConstant as NAN
from os import mkdir, path, remove
from pyproj import Geod, Proj
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

        self.start = datetime.strptime(self.args.start, '%Y%m%d')
        self.end = datetime.strptime(self.args.end, '%Y%m%d')
        self.gribQ = Queue()
        self.yearQ = Queue()
        self.sqlQ = Queue()

    def init_aws(self):
        if self.args.profile:
            self.aws = Session(profile_name=self.args.profile, region_name=self.args.region)
        else:
            self.aws = Session(region_name=self.args.region)

        self.cfn = self.aws.resource('cloudformation')
        self.ddb = self.aws.resource('dynamodb')
        self.s3 = self.aws.resource('s3')

    def init_logging(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG if self.args.debug else logging.INFO)

        sh = logging.StreamHandler(stdout)
        sh.setLevel(logging.DEBUG if self.args.debug else logging.INFO)
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(message)s')
        sh.setFormatter(formatter)
        self.logger.addHandler(sh)

        logging.getLogger('boto').propagate = False
        logging.getLogger('boto3').propagate = False
        logging.getLogger('botocore').propagate = False

    def row_threader(self):
        rowSetCount = 0
        rowCount = 0
        while True:
            sqlconn = sqlite3.connect('./data/obs.sqlite')
            c = sqlconn.cursor()
            sql_rows = self.sqlQ.get()
            for sql in sql_rows:
                try:
                    c.execute(sql)
                    sqlconn.commit()
                
                except:
                    self.logger.error('Existing Row')

            sqlconn.close()

            rowSetCount += 1
            rowCount += len(sql_rows)
            self.logger.debug('{Count} Total Row Sets Processed'.format(Count=rowSetCount))
            self.logger.debug('{Count} Total Rows Processed'.format(Count=rowCount))

            self.sqlQ.task_done()
            
    def daterange(self):
        for n in range(int ((self.end - self.start).days + 1)):
            yield self.start + timedelta(n)

    def retrieve_ghcn_observations(self):
        ftpYear = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{Year}.csv.gz'
        localYear = './data/{Year}.csv'

        if not path.exists('./data'):
            mkdir('./data')

        for year in range(self.start.year, self.end.year + 1):
            localFile = localYear.format(Year=year)
            remoteFile = urlopen(ftpYear.format(Year=year))
            self.logger.info('Downloading {File}\a'.format(File=localFile))

            with open(localFile + '.gz', 'wb') as localZipFile:
                localZipFile.write(remoteFile.read())
            
            with gzip.open(localFile + '.gz', 'rb') as localZipFile:
                with open(localFile, 'wb') as localCsvFile:
                    self.logger.debug('Unzipping {File}'.format(File=localFile))
                    localCsvFile.write(localZipFile.read())
            remove(localFile + '.gz')

    def retrieve_ndfd_forecasts(self):
        ftpDomain = 'nomads.ncdc.noaa.gov'
        httpPath = 'https://nomads.ncdc.noaa.gov/data/ndfd/{DayPath}/{File}'
        localPath = './data/ndfd/{File}'

        if not path.exists('./data/ndfd'):
            mkdir('./data/ndfd')
    
        ftp = FTP()
        ftp.connect(ftpDomain)
        ftp.login()

        for day in self.daterange():
            dayPath = day.strftime('/NDFD/%Y%m/%Y%m%d/')
            self.logger.info('Enumerating Variables for {DayPath}'.format(DayPath=dayPath))

            try:
                ftp.cwd(dayPath)
            except:
                self.logger.error('Problem with FTP DayPath: {DayPath}'.format(DayPath=dayPath))
                continue

            ls = []
            ftp.retrlines('MLSD', ls.append)
            for entry in ls:
                f = entry.split(';')[-1].strip()
                if self.args.retrieve_forecasts in f:
                    dayPath = day.strftime('%Y%m/%Y%m%d')
                    remoteHttp = httpPath.format(DayPath=dayPath, File=f)
                    self.logger.info('Downloading {Http}'.format(Http=remoteHttp))

                    try:                    
                        remoteFile = urlopen(remoteHttp)
                        with open(localPath.format(File=f), 'w') as localFile:
                            localFile.write(remoteFile.read())

                    except:
                        self.logger.error('Problem with HTTP File: {Http}'.format(Http=remoteHttp))
                        continue

        self.logger.info('NDFD Data Retrieved!')

    def parse_ghcn_stations(self):
        stationFile = './data/ghcnd-stations.txt'
        inventoryFile = './data/ghcnd-inventory.txt'
        ftpStations = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
        ftpInventory = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt'        
        startYear = int(self.start.strftime('%Y'))
        endYear = int(self.end.strftime('%Y'))

        self.logger.info('Retrieving Latest GHCN Station Inventory')
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

        sqlconn = sqlite3.connect('./data/obs.sqlite')
        c = sqlconn.cursor()
        c.execute((
            'CREATE TABLE IF NOT EXISTS stations ('
                'stationId TEXT PRIMARY KEY,'
                'stationName TEXT,'
                'lat FLOAT,'
                'lon FLOAT,'
                'elev FLOAT'
            ');'
        ))

        for i, row in stations.iterrows():
            if row['st'] in self.args.states:
                inv = inventory.loc[(inventory['id'] == row['id']) & (inventory['ele'] == 'PRCP') & (inventory['start'] <= startYear) & (inventory['end'] >= endYear)]
                if not inv.empty:
                    self.logger.info('Valid Station: {Name}'.format(Name=row['name']))
                    sql = 'INSERT INTO stations (stationId, stationName, lat, lon, elev) VALUES ("{Station}", "{Name}", {Lat}, {Lon}, {Elev});'
                    c.execute(sql.format(Station=row['id'], Name=row['name'], Lat=row['lat'], Lon=row['lon'], Elev=row['elev']))

        sqlconn.commit()
        sqlconn.close()

    def year_parser(self):
        while True:
            year = self.yearQ.get()

            self.logger.info('Reading Year: {Year}\a'.format(Year=year))
            yearFile = './data/{Year}.csv'.format(Year=year)
            yearHeaders = ['id', 'date', 'ele', 'val', 'mflag', 'qflag', 'sflag', 'time']
            sql = 'INSERT INTO observations (stationId, date, element, val) VALUES ("{Station}", {Date}, "{Element}", {Val});'
            sql_rows = []

            with open(yearFile, 'r') as yearCsv:
                reader = csv.DictReader(yearCsv, fieldnames=yearHeaders)

                for row in reader:
                    if row['ele'] in self.args.elements:
                        obsDate = datetime.strptime(row['date'], '%Y%m%d')
                        if row['id'] in self.stations and obsDate >= self.start and obsDate <= self.end:
                            try:
                                if len(row['time']) > 0:
                                    row['time'] = row['time'].replace('24', '00')
                                    obsTime = datetime.strptime(row['date'] + row['time'], '%Y%m%d%H%M')
                                else:
                                    obsTime = obsDate

                            except:
                                self.logger.warning(row['date'] + row['time'])
                                self.logger.warning('Problem with {Row}'.format(Row=row))
                                obsTime = obsDate

                            sql_rows.append(sql.format(Station=row['id'], Date=obsTime.strftime('%s'), Element=row['ele'], Val=float(row['val'])))

            self.sqlQ.put(sql_rows)

    def parse_ghcn_observations(self):
        sqlconn = sqlite3.connect('./data/obs.sqlite')
        sqlconn.row_factory = lambda cursor, row: row[0]
        c = sqlconn.cursor()
        c.execute((
            'CREATE TABLE IF NOT EXISTS observations ('
                'stationId TEXT,'
                'date INT,'
                'element TEXT,'
                'val FLOAT,'
                'PRIMARY KEY (stationId, date, element),'
                'FOREIGN KEY (stationId) REFERENCES stations(stationId)'
            ');'
        ))

        self.stations = c.execute('SELECT stationId FROM stations').fetchall()
        sqlconn.close()

        obsT = []
        for i in range(cpu_count()):
            t = Thread(target=self.year_parser)
            t.daemon = True
            t.start()
            obsT.append(t)

        self.logger.info('Parsing Observations for {Count} Stations'.format(Count=len(self.stations)))
        for year in range(self.start.year, self.end.year + 1):
             self.yearQ.put(year)

        self.yearQ.join()

        sqlT = Thread(target=self.row_threader)
        sqlT.daemon = True
        sqlT.start()

        self.sqlQ.join()
        self.logger.info('Observations Parsed!\a')

    def get_nearest_point(self, grib, lat, lon):
        p = Proj(grib.projparams)
        offsetX, offsetY = p(grib['longitudeOfFirstGridPointInDegrees'], grib['latitudeOfFirstGridPointInDegrees'])
        gridX, gridY = p(lon, lat)
        x = int(round((gridX - offsetX) / grib['DxInMetres']))
        y = int(round((gridY - offsetY) / grib['DyInMetres']))
        return x, y

    def grib_parser(self):
        sql = 'INSERT INTO forecasts (stationId, date, var, val) VALUES ("{Station}", {Date}, "{Var}", {Val});'

        while True:
            gribPath = self.gribQ.get()
            self.logger.info('Parsing Stations for {Date}'.format(Date=datetime.strptime(gribPath, './data/ndfd/YIUZ98_KWBN_%Y%m%d%H%M').replace(hour=0, minute=0)))

            forecasts = { }
            with pygrib.open(gribPath) as gribs:
                for grib in gribs:
                    t = datetime(grib['year'], grib['month'], grib['day'])
                    timeMultiplyer = 60 if grib['forecastTime'] > 72 else 1
                
                    if 30 * timeMultiplyer <= grib['forecastTime'] <= 48 * timeMultiplyer:
                        d = (t + timedelta(hours=grib['forecastTime']))

                        for station in self.stations:
                            stationName = station[0]

                            x, y = self.get_nearest_point(grib, station[1], station[2])
                            val = grib.values[y][x]
                            val = val if type(val) != NAN else float('nan')
                            if val == float('nan'): continue
                            stationForecast = forecasts[stationName] if stationName in forecasts else { 'total': 0, 'date': (t + timedelta(hours=24)).strftime('%Y%m%d') }
                            stationForecast['total'] += val
                            forecasts[stationName] = stationForecast

            forecastQueries = []
            for stationName, station in forecasts.iteritems():
                forecastQueries.append(sql.format(Station=stationName, Date=station['date'], Var='YIUZ', Val=station['total']))

            if len(forecastQueries) > 0:
                self.sqlQ.put(forecastQueries)

            self.gribQ.task_done()

    def parse_ndfd_forecasts(self):
        sqlconn = sqlite3.connect('./data/obs.sqlite')
        sqlconn.row_factory = lambda cursor, row: [row[0], row[1], row[2]]
        c = sqlconn.cursor()
        c.execute((
            'CREATE TABLE IF NOT EXISTS forecasts ('
                'stationId TEXT,'
                'date INT,'
                'var TEXT,'
                'val FLOAT,'
                'PRIMARY KEY (stationId, date, var),'
                'FOREIGN KEY (stationId) REFERENCES stations(stationId)'
            ');'
        ))

        self.stations = c.execute('SELECT stationId, lat, lon FROM stations').fetchall()
        sqlconn.close()

        for day in self.daterange():
            gribDates = []

            for grib in glob(day.strftime('./data/ndfd/YIUZ98_KWBN_%Y%m%d*')):
                gribDates.append(datetime.strptime(grib, './data/ndfd/YIUZ98_KWBN_%Y%m%d%H%M'))

            if len(gribDates) > 0:
                bestGrib = min(gribDates, key=lambda d: abs(d - day))
                grib = bestGrib.strftime('./data/ndfd/YIUZ98_KWBN_%Y%m%d%H%M')
                self.logger.info('Using {Grib} for {Date}'.format(Grib=path.basename(grib), Date=day))
                self.gribQ.put(grib)
            
            else:
                self.logger.warning('No Gribs for {Date}'.format(Date=day))

        sqlP = Process(target=self.row_threader)
        sqlP.daemon = True
        sqlP.start()

        gribP = []
        for i in range(cpu_count()):
            p = Process(target=self.grib_parser)
            p.daemon = True
            p.start()
            gribP.append(p)

        self.gribQ.join()
        self.sqlQ.join()

        self.logger.info('Forecasts Parsed!\a') 

    def analyze_accuracy(self):
        sql = 'SELECT COUNT(*) FROM forecasts JOIN observations ON observations.stationId=forecasts.stationId and forecasts.date=observations.date WHERE forecasts.stationId="{Station}" AND observations.element="PRCP" AND forecasts.val{ForecastOp} AND observations.val{ObservationOp};'
        sqlconn = sqlite3.connect('./data/obs.sqlite')
        c = sqlconn.cursor()
        stations = c.execute('SELECT * FROM stations').fetchall()

        c.execute((
            'CREATE TABLE IF NOT EXISTS accuracy ('
                'stationId TEXT PRIMARY KEY,'
                'good FLOAT,'
                'bad FLOAT,'
                'total FLOAT,'
                'accuracy FLOAT,'
                'FOREIGN KEY (stationId) REFERENCES stations(stationId)'
            ');'
        ))

        noMatch = 0
        for station in stations:
            rainPredicted =      float(c.execute(sql.format(Station=station[0], ForecastOp='>=50', ObservationOp='>=3')).fetchall()[0][0])
            noRainPredicted =    float(c.execute(sql.format(Station=station[0], ForecastOp='<=50', ObservationOp='<3')).fetchall()[0][0])
            rainNotPredicted =   float(c.execute(sql.format(Station=station[0], ForecastOp='<=50', ObservationOp='>=3')).fetchall()[0][0])
            noRainNotPredicted = float(c.execute(sql.format(Station=station[0], ForecastOp='>=50', ObservationOp='<3')).fetchall()[0][0])

            goodPrediction = rainPredicted + noRainPredicted
            badPrediction = rainNotPredicted + noRainNotPredicted
            totalDays = rainPredicted + noRainPredicted + rainNotPredicted + noRainNotPredicted

            if totalDays < 365:
                self.logger.debug('Not Enough Forecast-Observation Matches for {Station}'.format(Station=station[0]))
                accuracy = -1

            elif goodPrediction == 0:
                self.logger.debug('No Good Predictions for {Station}'.format(Station=station[0]))
                accuracy = -1
              
            else:  
                accuracy = round(goodPrediction / totalDays * 100, 1)

            self.logger.debug('{Station} Accuracy: {Accuracy}%'.format(Station=station[0], Accuracy=accuracy))
            c.execute('INSERT INTO accuracy (stationId, good, bad, total, accuracy) VALUES ("{Station}", {Good}, {Bad}, {Total}, {Accuracy});'.format(Station=station[0], Good=goodPrediction, Bad=badPrediction, Total=totalDays, Accuracy=accuracy))
                


        sqlconn.commit()
        sqlconn.close()

        self.logger.info('Analysis Done!')

    def init_shapefile(self):
        s = shapefile.Writer(shapeType=shapefile.POINTZ)
        s.field('id', 'C', size=15)
        s.field('name', 'C', size=30)
        s.field('lat', 'N', decimal=3)
        s.field('lon', 'N', decimal=3)
        s.field('elev', 'N', decimal=1)
        s.field('good', 'N', decimal=0)
        s.field('bad', 'N', decimal=0)
        s.field('total', 'N', decimal=0)
        s.field('accuracy', 'N', decimal=1)
        return s

    def create_accuracy_shapefiles(self):
        sqlconn = sqlite3.connect('./data/obs.sqlite')
        c = sqlconn.cursor()
        stations = c.execute('SELECT * FROM stations JOIN accuracy ON stations.stationId=accuracy.stationId').fetchall()

        p = Proj('+proj=utm +zone=18T, +north +ellps=GRS80 +datum=NAD83 +units=m +no_defs')
        accuracy = self.init_shapefile()
        missing = self.init_shapefile()
        allStations = self.init_shapefile()

        for station in stations:
            lat = station[2]
            lon = station[3]
            x, y = p(lon, lat)

            if station[9] != -1:
                accuracy.point(x, y, station[4])
                accuracy.record(station[0], station[1], lat, lon, station[4], station[6], station[7], station[8], station[9])

            else:
                missing.point(x, y, station[4])
                missing.record(station[0], station[1], lat, lon, station[4], station[6], station[7], station[8], station[9])
                
            allStations.point(x, y, station[4])
            allStations.record(station[0], station[1], lat, lon, station[4], station[6], station[7], station[8], station[9])
            

        if not path.exists('./data/shapefiles'):
            mkdir('./data/shapefiles')

        accuracy.save('./data/shapefiles/ny_accuracy')
        missing.save('./data/shapefiles/ny_missing')
        allStations.save('./data/shapefiles/ny_all')
