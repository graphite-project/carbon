import json
import fnmatch
import time
import struct

from thrift.transport import TSocket
from carbon.tsdb import TSDB
from carbon.hbase.ttypes import *
from carbon.hbase.Hbase import Client
#from carbon.hbase.Hbase import BatchMutation
from carbon import util
from carbon import log

from thriftpool import client as tpClient


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
    __slots__ = ('transport', 'client', 'metaTable', 'dataTable')

    def __init__(self, host, port, table_prefix):
        # set up client
        self.metaTable = table_prefix + "META"
        self.dataTable = table_prefix + "DATA"
        self.client = tpClient.Client(iface_cls=Client,
            host=host,
            port=port,
            pool_size=20,
            retries=3)
        # ensure both our tables exist
        tables = self.client.getTableNames()
        if self.metaTable not in tables:
            self.client.createTable(self.metaTable, [ColumnDescriptor("cf:", compression="Snappy")])
            # add counter record
            self.client.atomicIncrement(self.metaTable, "CTR", "cf:CTR", 1)
        if self.dataTable not in tables:
            self.client.createTable(self.dataTable, [ColumnDescriptor("cf:", compression="Snappy")])

    def __get_rows(self, scanId):
        row = self.client.scannerGet(scanId)
        while row:
            yield row
            row = self.client.scannerGet(scanId)

    def __refresh_thrift_client(self):
         log.cache("Attempting to refresh thrift client") 
         self.transport.close()
         time.sleep(0.25)
         self.transport.open()

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
        # key = "m_" + metric 
        result = self.client.get(self.metaTable, "m_" + metric, "cf:INFO", None)
        if len(result) == 0:
            raise Exception("No metric " + metric)
        val = json.loads(result[0].value)
        return val

    # aggregationMethod specifies the method to use when propogating data (see ``whisper.aggregationMethods``)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur.  If None, the existing xFilesFactor in path will not be changed
    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        currInfo = self.info(metric)
        currInfo['aggregationMethod'] = aggregationMethod
        currInfo['xFilesFactor'] = xFilesFactor

        infoJson = json.dumps(currInfo)
        self.client.mutateRow(self.metaTable, "m_" + metric, [Mutation(column="cf:INFO", value=infoJson)], None)
        return


    # archiveList is a list of archives, each of which is of the form (secondsPerPoint,numberOfPoints)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # aggregationMethod specifies the function to use when propogating data (see ``whisper.aggregationMethods``)
    def create(self, metric, archiveList, xFilesFactor, aggregationMethod, isSparse, doFallocate):
        #self.transport.open()

        #for a in archiveList:
        #    a['archiveId'] = (self.client.atomicIncrement(self.metaTable,"CTR","cf:CTR",1))

        archiveMapList = [
            {'archiveId': (self.client.atomicIncrement(self.metaTable, "CTR", "cf:CTR", 1)),
             'secondsPerPoint': a[0],
             'points': a[1],
             'retention': a[0] * a[1],
            }
            for a in archiveList
        ]
        #newId = self.client.atomicIncrement(self.metaTable,"CTR","cf:CTR",1)

        oldest = max([secondsPerPoint * points for secondsPerPoint, points in archiveList])
        # then write the metanode
        info = {
            'aggregationMethod': aggregationMethod,
            'maxRetention': oldest,
            'xFilesFactor': xFilesFactor,
            'archives': archiveMapList,
        }
        self.client.mutateRow(self.metaTable, "m_" + metric, [Mutation(column="cf:INFO", value=json.dumps(info))], None)
        # finally, ensure links exist
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
            parentLink = self.client.get(self.metaTable, metricParentKey, "cf:c_" + part, None)
            if len(parentLink) == 0:
                self.client.mutateRow(self.metaTable, metricParentKey,
                                      [Mutation(column="cf:c_" + part, value=metricKey)], None)
        #self.transport.close()

    def update_many(self, metric, points, retention_config):
        """Update many datapoints.

        Keyword arguments:
        points  -- Is a list of (timestamp,value) points
	retention_config -- Is silently ignored

        """

#        log.cache(retention_config)
        info = self.info(metric)
        now = int(time.time())
        archives = iter(info['archives'])
        currentArchive = archives.next()
        currentPoints = []

        for point in points:
            age = now - point[0]

            while currentArchive['retention'] < age: #we can't fit any more points in this archive
                if currentPoints: #commit all the points we've found that it can fit
                    currentPoints.reverse() #put points in chronological order
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

        if currentArchive and currentPoints: #don't forget to commit after we've checked all the archives
            currentPoints.reverse()
            self.__archive_update_many(info, currentArchive, currentPoints)

    def __archive_update_many(self, info, archive, points):
        #self.transport.open()
        numPoints = archive['points']
        step = archive['secondsPerPoint']
        archiveId = archive['archiveId']
        alignedPoints = [(timestamp - (timestamp % step), value)
                         for (timestamp, value) in points]
        alignedPoints = dict(alignedPoints).items() # Take the last val of duplicates
        mutationsbatch = []
        for timestamp, value in alignedPoints:
            slot = int((timestamp / step) % numPoints)
            rowkey = struct.pack(KEY_FMT, archiveId, slot)
            rowval = struct.pack(VAL_FMT, timestamp, value)
            mutationsbatch.append(BatchMutation(rowkey, [Mutation(column="cf:d", value=rowval, writeToWAL=False)]))
#            self.client.mutateRow(self.dataTable, rowkey, [Mutation(column="cf:d", value=rowval)], None)

#        log.cache("Length of batch: %d" %(len(mutationsbatch)))
        self.client.mutateRows(self.dataTable, mutationsbatch, None)

        #Now we propagate the updates to lower-precision archives
        higher = archive
        lowerArchives = [arc for arc in info['archives'] if arc['secondsPerPoint'] > archive['secondsPerPoint']]
        for lower in lowerArchives:
            fit = lambda i: i - (i % lower['secondsPerPoint'])
            lowerIntervals = [fit(p[0]) for p in alignedPoints]
            uniqueLowerIntervals = set(lowerIntervals)
            propagateFurther = False
            for interval in uniqueLowerIntervals:
                if self.__propagate(info, interval, higher, lower):
                    propagateFurther = True

            if not propagateFurther:
                break
            higher = lower
        #self.transport.close()

    def __propagate(self, info, timestamp, higher, lower):
        aggregationMethod = info['aggregationMethod']
        xff = info['xFilesFactor']

        # we want to update the items from higher between these two
        intervalStart = timestamp - (timestamp % lower['secondsPerPoint'])
        intervalEnd = intervalStart + lower['secondsPerPoint']
        (higherResInfo, higherResData) = self.__archive_fetch(higher, intervalStart, intervalEnd)
        
        known_datapts = [v for v in higherResData if v is not None] # strip out "nones"
        if (len(known_datapts) / len(higherResData)) > xff: # we have enough data, so propagate downwards
            aggregateValue = util.aggregate(aggregationMethod, known_datapts)
            lowerSlot = timestamp / lower['secondsPerPoint'] % lower['points']
            rowkey = struct.pack(KEY_FMT, lower['archiveId'], lowerSlot)
            rowval = struct.pack(VAL_FMT, timestamp, aggregateValue)
            self.client.mutateRow(self.dataTable, rowkey, [Mutation(column="cf:d", value=rowval)], None)

    # returns list of values between the two times.  length is endTime - startTime / secondsPerPorint.
    # should be aligned with secondsPerPoint for proper results
    def __archive_fetch(self, archive, startTime, endTime):
        step = archive['secondsPerPoint']
        numPoints = archive['points']
        startTime = int(startTime - (startTime % step) + step)
        endTime = int(endTime - (endTime % step) + step)
        startSlot = int((startTime / step) % numPoints)
        endSlot = int((endTime / step) % numPoints)
        numSlots = (endTime - startTime) / archive['secondsPerPoint']
        ret = [None] * numSlots
        if startSlot > endSlot: # we wrapped so make 2 queries
            ranges = [(0, endSlot + 1), (startSlot, numPoints)]
        else:
            ranges = [(startSlot, endSlot + 1)]
        for t in ranges:
            startkey = struct.pack(KEY_FMT, archive['archiveId'], t[0])
            endkey = struct.pack(KEY_FMT, archive['archiveId'], t[1])
            
            scan = TScan(startRow = startkey, stopRow = endkey, 
                         timestamp = None, caching = 2000, 
                         filterString = None, batchSize = None, 
                         sortColumns = False)

            scannerId = self.client.scannerOpenWithScan(self.dataTable, scan, {})

            for row in self.__get_rows(scannerId):
                (timestamp, value) = struct.unpack(VAL_FMT, row[0].columns["cf:d"].value) # this is dumb.
                if timestamp >= startTime and timestamp <= endTime:
                    returnslot = int((timestamp - startTime) / archive['secondsPerPoint']) % numSlots
                    ret[returnslot] = value
            self.client.scannerClose(scannerId)
        timeInfo = (startTime, endTime, step)
        return timeInfo, ret


    def exists(self, metric):
        metric_len = len(self.client.getRow(self.metaTable, "m_" + metric, None))
        return metric_len > 0

    # fromTime is an epoch time
    # untilTime is also an epoch time, but defaults to now.
    #
    # Returns a tuple of (timeInfo, valueList)
    # where timeInfo is itself a tuple of (fromTime, untilTime, step)
    # Returns None if no data can be returned
    def fetch(self, info, fromTime, untilTime):
        now = int(time.time())
        if untilTime is None:
            untilTime = now
        fromTime = int(fromTime)
        untilTime = int(untilTime)
        if untilTime > now:
            untilTime = now
        if (fromTime > untilTime):
            raise Exception("Invalid time interval: from time '%s' is after until time '%s'" % (fromTime, untilTime))

        if fromTime > now:  # from time in the future
            return None
        oldestTime = now - info['maxRetention']
        if fromTime < oldestTime:
            fromTime = oldestTime
            # iterate archives to find the smallest
        diff = now - fromTime
        for archive in info['archives']:
            if archive['retention'] >= diff:
                break
        (timeInfo, ret) = self.__archive_fetch(archive, fromTime, untilTime)
        return timeInfo, ret

    def build_index(self, tmp_index):
        scannerId = self.client.scannerOpen(self.metaTable, "", ['cf:INFO'], {})

        t = time.time()
        total_entries = 0
        for row in self.__get_rows(scannerId):
            total_entries += 1
            tmp_index.write('%s\n' % row[0].row[2:])
        tmp_index.flush()
        self.client.scannerClose(scannerId)
        log.msg("[IndexSearcher] index rebuild took %.6f seconds (%d entries)" % (time.time() - t, total_entries))

    # returns [ start, end ] where start,end are unixtime ints
    def get_intervals(self, metric):
        start = time.time() - self.info(metric)['maxRetention']
        end = time.time()
        return [start, end]

    # returns list of metrics as strings
    def find_nodes(self, query):
        # break query into parts
        clean_pattern = query.pattern.replace('\\', '')
        pattern_parts = clean_pattern.split('.')
        ret = self._find_paths("ROOT", pattern_parts)
        return ret

    def _find_paths(self, currNodeRowKey, patterns):
        """Recursively generates absolute paths whose components underneath current_node
        match the corresponding pattern in patterns"""

        from graphite.node import BranchNode, LeafNode
        from graphite.intervals import Interval, IntervalSet

        pattern = patterns[0]
        patterns = patterns[1:]

        nodeRow = self.client.getRow(self.metaTable, currNodeRowKey, None)
        if len(nodeRow) == 0:
            return

        subnodes = {}
        for k, v in nodeRow[0].columns.items():
            if k.startswith("cf:c_"): # branches start with c_
                key = k.split("_", 1)[1] # pop off cf:c_ prefix
                subnodes[key] = v.value

        matching_subnodes = match_entries(subnodes.keys(), pattern)

        if patterns: # we've still got more directories to traverse
            for subnode in matching_subnodes:
                rowKey = subnodes[subnode]
                subNodeContents = self.client.getRow(self.metaTable, rowKey, None)

                # leafs have a cf:INFO column describing their data
                # we can't possibly match on a leaf here because we have more components in the pattern,
                # so only recurse on branches
                if "cf:INFO" not in subNodeContents[0].columns:
                    for m in self._find_paths(rowKey, patterns):
                        yield m



        else: # at the end of the pattern
            for subnode in matching_subnodes:
                rowKey = subnodes[subnode]
                nodeRow = self.client.getRow(self.metaTable, rowKey, None)
                if len(nodeRow) == 0:
                    continue
                metric = rowKey.split("_", 1)[1] # pop off "m_" in key
                if "cf:INFO" in nodeRow[0].columns:
                    info = json.loads(nodeRow[0].columns["cf:INFO"].value)
                    start = time.time() - info['maxRetention']
                    end = time.time()
                    intervals = IntervalSet( [Interval(start, end)] )
                    reader = HbaseReader(metric,intervals,info,self)
                    yield LeafNode(metric, reader)
                else:
                    yield BranchNode(metric)

class HbaseReader(object):
    __slots__ = ('db','metric','intervals','info')
    def __init__(self,metric,intervals,info,db):
        self.metric=metric
        self.db=db
        self.info = info
        self.intervals = intervals

    def get_intervals(self):
        return self.intervals

    def fetch(self, startTime, endTime):
        return self.db.fetch(self.info, startTime, endTime)

    def __repr__(self):
        return '<HbaseReader[%x]: %s >' % (id(self), self.metric)


def match_entries(entries, pattern):
    """A drop-in replacement for fnmatch.filter that supports pattern
    variants (ie. {foo,bar}baz = foobaz or barbaz)."""
    v1, v2 = pattern.find('{'), pattern.find('}')

    if v1 > -1 and v2 > v1:
        variations = pattern[v1 + 1:v2].split(',')
        variants = [pattern[:v1] + v + pattern[v2 + 1:] for v in variations]
        matching = []

        for variant in variants:
            matching.extend(fnmatch.filter(entries, variant))

        return list(_deduplicate(matching)) #remove dupes without changing order

    else:
        matching = fnmatch.filter(entries, pattern)
        matching.sort()
        return matching


def _deduplicate(entries):
    yielded = set()
    for entry in entries:
        if entry not in yielded:
            yielded.add(entry)
            yield entry
