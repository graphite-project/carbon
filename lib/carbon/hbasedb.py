from json import loads as json_loads
from json import dumps as json_dumps
from time import time, sleep
from fnmatch import filter as fnmatch_filter
from struct import pack as struct_pack
from struct import unpack as struct_unpack
from socket import gethostbyname
from carbon.tsdb import TSDB
from carbon import util, log
import happybase
from random import choice
from random import randrange

# we manage a namespace table (NS) and then a data table (data)

# the NS table is organized to mimic a tree structure, with a ROOT node containing links to its children.
# Nodes are either a BRANCH node which contains multiple child columns prefixed with c_,
# or a LEAF node containing a single INFO column with the JSON included in the info() method's comment

# IDCTR
#   - unique id counter

# ROOT
#   - c_branch1 -> m_branch1
#   - c_leaf1 -> m_leaf1

# m_branch1
#   - c_leaf2 -> m_branch1.leaf2

# m_leaf1
#    - INFO -> info json

# m_branch1.leaf2
#    - INFO -> info json

# the INFO json on branch nodes contains graphite info plus an ID field, consisting of a 32bit int

# we then maintain a data table with keys that are a compound of metric ID + unix timestamp for 8 byte keys


KEY_FMT = ">LL" # format for ts data row keys
VAL_FMT = ">Ld" # format for ts data row values


class ArchiveConfig:
    __slots__ = ('archiveId', 'secondsPerPoint', 'points')

    def __init__(self, tuple, id):
        self.secondsPerPoint, self.points = tuple
        self.archiveId = id

class HbaseTSDB(TSDB):
    __slots__ = ('client', 'batch_size', 'table_prefix', 
                 'meta_name', 'data_name', 'send_interval',
                 'reset_interval', 'meta_table', 'data_table',
                 'data_batch', 'send_time', 'reset_time')

    def __init__(self, host_list, port, table_prefix, batch_size=1000,
                 transport='buffered', send_interval=60,
                 reset_interval=1800, protocol='binary'):

        self.host_list = (gethostbyname(host) for host in host_list)
        self.thrift_port = port
        self.transport_type = transport

        self.batch_size = batch_size
        self.table_prefix = table_prefix
        self.meta_name = "META"
        self.data_name = "DATA"
        self.send_interval = send_interval
        self.reset_interval = reset_interval + randrange(120)
        self.connection_retries = 3
        self.protocol = protocol
        log.msg('Using %s protocol and %s transport' % (protocol, transport))
        #use the reset function only for consistent connection creation
        self.__reset_conn(send_batch=False)

    def __make_conn(self):
        try:
            del self.client
        except Exception, e:
            pass
        self.thrift_host = choice(self.host_list)
        log.msg("Reconnecting to %s::%s" % (self.thrift_host, self.thrift_port))
        self.client = happybase.Connection(
            host=self.thrift_host,
            port=self.thrift_port,
            table_prefix=self.table_prefix,
            transport=self.transport_type,
            protocol=self.protocol,
            autoconnect=False
        )
        self.client.open()
        sleep(0.25)
        e = None
        try:
            res = len(self.client.tables())
        except Exception, e:
            res = 0
        return res > 0, e

    def __reset_conn(self, send_batch=True):
        if send_batch:
            try:
                self.data_batch.send()
            except Exception, e:
                log.msg('Failed to send batch %s because %s' % (self.data_batch, e))
                pass

        for conn in xrange(self.connection_retries):
            res, e = self.__make_conn()
            if res:
                break
        else:
            log.msg('Cannot get connection to HBase because %s.' % e)
            exit(2)

        self.meta_table = self.client.table(self.meta_name)
        self.data_table = self.client.table(self.data_name)
        self.data_batch = self.data_table.batch()
        self.batch_count = 0
        self.send_time = time()
        self.reset_time = time()

    def __refresh_conn(self, wait_time=60):
        self.client.close()
        #try and refresh for 1 minute
        give_up_time = time() + wait_time
        log.msg('Connection failed to %s' % self.thrift_host)
        while time() < give_up_time:
            try:
                log.msg('Retrying connection to %s' % self.thrift_host)
                sleep(1)
                self.client.open()
                log.msg('Connection resumed...')
            except Exception, e:
                pass
            else:
                break
        else:
            self.__reset_conn()

    def __send(self):
        if time() - self.send_time > self.send_interval or \
                self.batch_count > self.batch_size:
            try:
                self.data_batch.send()
            except Exception:
                self.__refresh_conn()
                self.data_batch.send()
            self.send_time = time()
        if time() - self.reset_time > self.reset_interval:
            self.__reset_conn()

    def __get_row(self, row, columns=None, send_batch=True):
        if time() - self.reset_time > self.reset_interval:
            self.__reset_conn(send_batch=send_batch)
        res = None
        try:
            if columns:
                res = self.meta_table.row(row, columns)
            else:
                res = self.meta_table.row(row)
        except Exception:
            self.__refresh_conn()

            if columns:
                res = self.meta_table.row(row, columns)
            else:
                res = self.meta_table.row(row)

        return res

    # returns info for the underlying db (including 'aggregationMethod')

    # info returned in the format
    #info = {
    #  'aggregationMethod' : aggregationTypeToMethod.get(aggregationType, 'average'),
    #  'maxRetention' : maxRetention,
    #  'xFilesFactor' : xff,
    #  'archives' : archives,
    #}
    # where archives is a list of
    # archiveInfo = {
    #  'archiveId': unique id,
    #  'secondsPerPoint' : secondsPerPoint,
    #  'points' : points, number of points per
    #  'retention' : secondsPerPoint    * points,
    #  'size' : points * pointSize,
    #}
    #
    def info(self, metric):
        # info is stored as serialized map under META#METRIC
        try:
            result = self.__get_row("m_%s" % metric, columns=["cf:INFO"])
            result = json_loads(result['cf:INFO'])
        except Exception, e:
            raise Exception('No metric: %s because %s' % (metric, e))
        return result

    # aggregationMethod specifies the method to use when propogating data (see ``whisper.aggregationMethods``)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur.  If None, the existing xFilesFactor in path will not be changed
    def setAggregationMethod(self, metric, aggregationMethod, 
        xFilesFactor=None):

        currInfo = self.info(metric)
        currInfo['aggregationMethod'] = aggregationMethod
        currInfo['xFilesFactor'] = xFilesFactor

        infoJson = json_dumps(currInfo)
        metric_name = "m_%s" % metric
        self.meta_table.put(metric_name, {"cf:INFO": infoJson})

    # archiveList is a list of archives, each of which is of the form (secondsPerPoint,numberOfPoints)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # aggregationMethod specifies the function to use when propogating data (see ``whisper.aggregationMethods``)
    def create(self, metric, archiveList, xFilesFactor, aggregationMethod, 
        isSparse, doFallocate):
        archive_id = self.meta_table.counter_inc('CTR', 'cf:CTR')
        archiveMapList = [
            {'archiveId': archive_id,
             'secondsPerPoint': a[0],
             'points': a[1],
             'retention': a[0] * a[1],
            }
            for a in archiveList
        ]

        oldest = max([secondsPerPoint * points 
            for secondsPerPoint, points in archiveList])
        # then write the metanode
        info = {
            'aggregationMethod': aggregationMethod,
            'maxRetention': oldest,
            'xFilesFactor': xFilesFactor,
            'archives': archiveMapList,
        }
        metric_name = "m_%s" % metric
        self.meta_table.put(metric_name, {"cf:INFO": json_dumps(info)})
        metric_parts = metric.split('.')
        priorParts = ""
        for part in metric_parts:
            # if parent is empty, special case for root
            if priorParts == "":
                metricParentKey = "ROOT"
                metricKey = "m_" + part
                priorParts = part
            else:
                metricParentKey = "m_" + priorParts
                metricKey = "m_" + priorParts + "." + part
                priorParts += "." + part

            # make sure parent of this node exists and is linked to us
            parentLink = self.__get_row(metricParentKey, 
                                             columns=["cf:c_%s" % part])
            if len(parentLink) == 0:
                metric_name = "cf:c_%s" % part
                self.meta_table.put(metricParentKey, {metric_name:
                                                      metricKey})

    def update_many(self, metric, points, retention_config):
        """Update many datapoints.

        Keyword arguments:
        points  -- Is a list of (timestamp,value) points
        retention_config -- Is silently ignored

        """

        info = self.info(metric)
        now = int(time())
        archives = iter(info['archives'])
        currentArchive = archives.next()
        currentPoints = []

        for point in points:
            age = now - point[0]
            #we can't fit any more points in this archive
            while currentArchive['retention'] < age: 
                #commit all the points we've found that it can fit
                if currentPoints: 
                    #put points in chronological order
                    currentPoints.reverse() 
                    self.__archive_update_many(info, currentArchive, currentPoints)
                    currentPoints = []
                try:
                    currentArchive = archives.next()
                except StopIteration:
                    currentArchive = None
                    break

            if not currentArchive:
                break #drop remaining points that don't fit in the database

            currentPoints.append(point)
        #don't forget to commit after we've checked all the archives
        if currentArchive and currentPoints: 
            currentPoints.reverse()
            self.__archive_update_many(info, currentArchive, currentPoints)

    def __archive_update_many(self, info, archive, points):
        numPoints = archive['points']
        step = archive['secondsPerPoint']
        archiveId = archive['archiveId']
        alignedPoints = [(timestamp - (timestamp % step), value)
                         for (timestamp, value) in points]

        len_aligned = len(alignedPoints)
        for i in xrange(len_aligned):
            #if the point is the same as the last one, skip it
            if i+1 < len_aligned and alignedPoints[i][0] == alignedPoints[i+1][0]:
                continue
            (timestamp, value) = alignedPoints[i]
            slot = int((timestamp / step) % numPoints)
            rowkey = struct_pack(KEY_FMT, archiveId, slot)
            rowval = struct_pack(VAL_FMT, timestamp, value)
            self.data_batch.put(rowkey, {'cf:d': rowval})
            self.batch_count += 1
        self.__send()

    def propagate(self, info, archives, points):
        higher = archives.next()
        lowerArchives = [arc for arc in info['archives'] if arc['secondsPerPoint'] > higher['secondsPerPoint']]
        for lower in lowerArchives:
            fit = lambda i: i - (i % lower['secondsPerPoint'])
            lowerIntervals = [fit(p[0]) for p in points] # the points are already aligned
            uniqueLowerIntervals = set(lowerIntervals)
            propagateFurther = False
            for interval in uniqueLowerIntervals:
                if self.__archive_propagate(info, interval, higher, lower):
                    propagateFurther = True
            if not propagateFurther:
                break
            higher = lower

    def __archive_propagate(self, info, timestamp, higher, lower):
        aggregationMethod = info['aggregationMethod']
        xff = info['xFilesFactor']

        # we want to update the items from higher between these two
        intervalStart = timestamp - (timestamp % lower['secondsPerPoint'])
        intervalEnd = intervalStart + lower['secondsPerPoint']
        (higherResInfo, higherResData) = self.__archive_fetch(higher, intervalStart, intervalEnd)

        known_datapts = [v for v in higherResData if v is not None] # strip out "nones"
        if (len(known_datapts) / len(higherResData)) > xff: # we have enough data, so propagate downwards
            aggregateValue = int(util.aggregate(aggregationMethod, known_datapts))
            lowerSlot = int(timestamp / lower['secondsPerPoint'] % lower['points'])
            rowkey = struct_pack(KEY_FMT, lower['archiveId'], lowerSlot)
            rowval = struct_pack(VAL_FMT, timestamp, aggregateValue)
            self.data_batch.put(rowkey, {"cf:d": rowval})
            self.batch_count += 1
        self.__send()

    # returns list of values between the two times.  
    # length is endTime - startTime / secondsPerPorint.
    # should be aligned with secondsPerPoint for proper results
    def __archive_fetch(self, archive, startTime, endTime):
        step = int(archive['secondsPerPoint'])
        numPoints = int(archive['points'])
        startTime = int(startTime - (startTime % step) + step)
        endTime = int(endTime - (endTime % step) + step)
        numSlots = int((endTime - startTime) / archive['secondsPerPoint'])
        if numSlots > numPoints:
            startSlot = 0
        else:
            startSlot = int((startTime / step) % numPoints)
        endSlot = int((endTime / step) % numPoints)
        ret = [None] * numSlots

        if startSlot > endSlot: # we wrapped so make 2 queries
            ranges = [(0, endSlot + 1), (startSlot, numPoints)]
        else:
            ranges = [(startSlot, endSlot + 1)]
        for t in ranges:
            startkey = struct_pack(KEY_FMT, archive['archiveId'], t[0])
            endkey = struct_pack(KEY_FMT, archive['archiveId'], t[1])
            scan = self.data_table.scan(row_start = startkey, row_stop = endkey,
                                        batch_size = self.batch_size)

            for row in scan:
                (timestamp, value) = struct_unpack(VAL_FMT, row[1]["cf:d"])
                if timestamp >= startTime and timestamp <= endTime:
                    returnslot = int((timestamp - startTime)
                                    / archive['secondsPerPoint']) % numSlots
                    ret[returnslot] = value

        timeInfo = (startTime, endTime, step)
        return timeInfo, ret

    def exists(self, metric):
        try:
            res = self.__get_row("m_%s" % metric, columns=['cf:INFO'])
            metric_len = len(json_loads(res['cf:INFO']))
        except Exception, e:
            return False
        else:
            return metric_len > 0

    def delete(self, metric):
        #make sure the metric exists
        if not self.exists(metric):
            return
        #Get data on metric
        info = self.info(metric)
        for archive in info['archives']:
            endSlot = int((time() / archive['secondsPerPoint']) % archive['points'])
            start_key = struct_pack(KEY_FMT, archive['archiveId'], 0)
            end_key = struct_pack(KEY_FMT, archive['archiveId'], endSlot)
            #we'll first get a scan object of all the data table rows associated
            scan = self.data_table.scan(row_start = start_key, row_stop = end_key,
                                        batch_size = self.batch_size)
            #then batch delete
            for row in scan:
                self.data_batch.delete(row[0])
                self.batch_count += 1
        #make sure there aren't any left over deletes
        self.data_batch.send()
        #now we remove the meta table row
        self.meta_table.delete('m_%s' % metric)
        self.meta_table.counter_dec('CTR', 'cf:CTR')


    def build_index(self, tmp_index):
        scan = self.meta_table.scan(columns=['cf:INFO'],
                                    batch_size = self.batch_size)
        t = time()
        total_entries = 0
        for row in scan:
            tmp_index.write('%s\n' % row[0][2:])
            total_entries += 1
        tmp_index.flush()

        log.msg("[IndexSearcher] index rebuild took %.6f seconds (%d entries)" %
             (time() - t, total_entries))

    # returns [ start, end ] where start,end are unixtime ints
    def get_intervals(self, metric):
        start = time() - self.db.info(metric)['maxRetention']
        end = time()
        return [start, end]



def create_tables(host, port, table_prefix, transport, protocol):
    meta = "META"
    data = "DATA"
    meta_name = "%s_%s" % (table_prefix, meta)
    data_name = "%s_%s" % (table_prefix, data)

    client = happybase.Connection(
        host=host,
        port=port,
        table_prefix=table_prefix,
        transport=transport,
        protocol=protocol
    )
    sleep(0.25)
    try:
        tables = client.tables()
    except Exception as e:
        print("HBase tables can't be retrieved. Cluster offline?")
        exit(1)

    if meta_name not in tables:
        print('Creating %s table' % meta_name)
        families = {'cf:': dict(compression="Snappy",
                                block_cache_enabled=True,
                                bloom_filter_type="ROW")}
        client.create_table(meta, families)
        meta_table = client.table("META")
        # add counter record
        meta_table.counter_set('CTR', 'cf:CTR', 1)
    elif data_name not in tables:
        print('Creating %s table' % data_name)
        families = {'cf:': dict(compression="Snappy",
                                bloom_filter_type='ROW',
                                block_cache_enabled=True)}
        client.create_table(data, families)
    else:
        print('Both Graphite tables available!')


