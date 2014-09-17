from json import loads as json_loads
from json import dumps as json_dumps
from time import time, sleep
from fnmatch import filter as fnmatch_filter
from struct import pack as struct_pack
from struct import unpack as struct_unpack

from carbon.tsdb import TSDB
from carbon import util, log

import happybase

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

    def __init__(self, host, port, table_prefix, batch_size=1000,
                 transport='framed', send_interval=60,
                 reset_interval=900):
        # set up client
        self.client = happybase.Connection(
            host=host,
            port=port,
            table_prefix=table_prefix,
            transport=transport,
            compat="0.94",
            autoconnect=False
        )
        self.batch_size = batch_size
        self.table_prefix = table_prefix
        self.meta_name = "META"
        self.data_name = "DATA"
        self.send_interval = send_interval
        self.reset_interval = reset_interval
        self.client.open()
        self.meta_table = self.client.table(self.meta_name)
        self.data_table = self.client.table(self.data_name)
        self.data_batch = self.data_table.batch(batch_size=self.batch_size)
        self.send_time = time()
        self.reset_time = time()

    def __send(self):
        if time() - self.send_time > self.send_interval:
            self.data_batch.send()
            self.send_time = time()
        if time() - self.reset_time > self.reset_interval:
            self.data_batch.send()
            self.client.close()
            self.client.open()
            sleep(0.25)
            self.meta_table = self.client.table(self.meta_name)
            self.data_table = self.client.table(self.data_name)
            self.data_batch = self.data_table.batch(batch_size=self.batch_size)
            self.reset_time = time()

    def __get_row(self, row, columns=None):
        try:
            if columns:
                res = self.meta_table.row(row, columns)
            else:
                res = self.meta_table.row(row)
        except Exception, e:
            self.client.close()
            self.client.open()
            sleep(0.25)
            try:
                if columns:
                    res = self.meta_table.row(row, columns)
                else:
                    res = self.meta_table.row(row)
            except Exception, e:
                raise Exception("Failed to get row %s because %s" % (row, e))
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
            if i+1 < len_aligned and alignedPoints[i][0] == alignedPoints[i+1][0]:
                continue
            (timestamp, value) = alignedPoints[i]
            slot = int((timestamp / step) % numPoints)
            rowkey = struct_pack(KEY_FMT, archiveId, slot)
            rowval = struct_pack(VAL_FMT, timestamp, value)
            self.data_batch.put(rowkey, {'cf:d': rowval})
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
            aggregateValue = util.aggregate(aggregationMethod, known_datapts)
            lowerSlot = timestamp / lower['secondsPerPoint'] % lower['points']
            rowkey = struct_pack(KEY_FMT, lower['archiveId'], lowerSlot)
            rowval = struct_pack(VAL_FMT, timestamp, aggregateValue)
            self.data_batch.put(rowkey, {"cf:d": rowval})
        self.__send()

    # returns list of values between the two times.  
    # length is endTime - startTime / secondsPerPorint.
    # should be aligned with secondsPerPoint for proper results
    def __archive_fetch(self, archive, startTime, endTime):
        step = archive['secondsPerPoint']
        numPoints = archive['points']
        startTime = int(startTime - (startTime % step) + step)
        endTime = int(endTime - (endTime % step) + step)
        numSlots = (endTime - startTime) / archive['secondsPerPoint']
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


    # fromTime is an epoch time
    # untilTime is also an epoch time, but defaults to now.
    #
    # Returns a tuple of (timeInfo, valueList)
    # where timeInfo is itself a tuple of (fromTime, untilTime, step)
    # Returns None if no data can be returned
    def fetch(self, info, fromTime, untilTime):
        now = int(time())
        if untilTime is None:
            untilTime = now
        fromTime = int(fromTime)
        untilTime = int(untilTime)
        if untilTime > now:
            untilTime = now
        if (fromTime > untilTime):
            raise Exception("Invalid time interval: from time '%s' is after " +
                "until time '%s'" % (fromTime, untilTime))

        if fromTime > now:  # from time in the future
            return None
        oldestTime = now - info['maxRetention']
        if fromTime < oldestTime:
            fromTime = oldestTime
            # iterate archives to find the smallest
        diff = now - fromTime
        full_ret = []
        for archive in info['archives']:
            if untilTime < (now - (archive['secondsPerPoint'] * archive['points'])):
                continue
            (timeInfo, ret) = self.__archive_fetch(archive, fromTime, untilTime)
            full_ret.append((timeInfo, ret))
            if archive['retention'] >= diff:
                break

        full_ret = reduce(self.__merge, full_ret)
        return full_ret

    def __merge(self, results1, results2):
        # Ensure results1 is finer than results2
        if results1[0][2] > results2[0][2]:
            results1, results2 = results2, results1

        time_info1, values1 = results1
        time_info2, values2 = results2
        start1, end1, step1 = time_info1
        start2, end2, step2 = time_info2

        start  = min(start1, start2)  # earliest start
        end    = max(end1, end2)      # latest end

        #We need to determine the difference in the
        #number of datapoints between lists 1 & 2
        #and basically explode the values in the older
        #list to match the newer one
        try:
            padding = int(step2 / step1)
        except ValueError:
            padding = 1
        if padding < 1:
            padding = 1

        time_info = (start, end, step1)
        values = []
        values1_len = len(values1)
        values2_len = len(values2)
        t = start

        while t < end:
            # Look for the finer precision value first if available
            index = (t - start1) / step1
            if values1_len > index and values1[index] is not None:
                val = [values1[index]]
                t += step1
            else:
                index = (t - start2) / step2
                t += step2

                if values2_len > index:
                    val = [values2[index] for pad in xrange(padding)]
                else:
                    val = [None] * padding

            values.extend(val)

        return (time_info, values)

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

    def find_nodes(self, query):
        '''We import these here because then we'll have the graphite-web
        paths set up. Means we don't need to import sys in this one.'''
        from graphite.node import BranchNode, LeafNode
        from graphite.intervals import Interval, IntervalSet
        # break query into parts
        clean_pattern = query.pattern.replace('\\', '')
        pattern_parts = _cheaper_patterns(clean_pattern.split('.'))

        if pattern_parts[0] == "*" or pattern_parts[0] == "ROOT":
            start_string = "ROOT"
        else:
            start_string = "m_%s" % pattern_parts[0]
        pattern_parts = pattern_parts[1:]

        for subnode, subnodes in self._find_paths(start_string, pattern_parts):
            rowKey = subnodes[subnode]
                
            nodeRow = self.__get_row(rowKey)
            if len(nodeRow) == 0:
                continue
            metric = rowKey[2:] # pop off "m_" in key
            if "cf:INFO" in nodeRow.keys():
                info = json_loads(nodeRow["cf:INFO"])
                start = time() - info['maxRetention']
                end = time()
                intervals = IntervalSet( [Interval(start, end)] )
                reader = HbaseReader(metric, intervals, info, self)
                yield LeafNode(metric, reader)
            else:
                yield BranchNode(metric)

    def _find_paths(self, currNodeRowKey, patterns):
        """Recursively generates absolute paths whose components underneath current_node
        match the corresponding pattern in patterns"""
        nodeRow = self.__get_row(currNodeRowKey)

        if len(nodeRow) == 0:
            return

        try:
            metric_name = nodeRow['cf:INFO']
        except KeyError:
            pass
        else:
            yield currNodeRowKey, {currNodeRowKey: currNodeRowKey}
        if patterns:
            pattern = patterns[0]
            patterns = patterns[1:]
        else:
            pattern = "*"

        subnodes = {}
        for k, v in nodeRow.items():
            if k.startswith("cf:c_"): # branches start with c_
                key = k[5:] # pop off cf:c_ prefix
                subnodes[key] = v

        matching_subnodes = _match_entries(subnodes.keys(), pattern)
        if patterns: # we still have more directories to traverse
            for subnode in matching_subnodes:
                rowKey = subnodes[subnode]
                subNodeContents = self.__get_row(rowKey)
                # leaves have a cf:INFO column describing their data
                # we can't possibly match on a leaf here because we have more components in the pattern,
                # so only recurse on branches
                if "cf:INFO" not in subNodeContents.keys():
                    for metric, node_list in self._find_paths(rowKey, patterns, conn):
                        yield metric, node_list
        else:
            for node in matching_subnodes:
                yield node, subnodes

"""
This will break up a list like:
['Platform', 'MySQL', '*', '*', '*qps*']
into
['Platform.MySQL', '*', '*', '*qps*']
Which means fewer scans for each metric.
Thus...cheaper!
['Infrastructure.servers.CH', 'ag*', 'loadavg', '[01][15]']
In this case, two fewer scans!
Extrapolate across some of our bigger requests, and this does 
save time.
"""
def _cheaper_patterns(pattern):
    if len(pattern) < 2:
        return pattern
    excluded = ['*', '[', ']', '{', '}']
    current_string = pattern[0]
    chunk = pattern[1]
    del pattern[0]
    while not any(x in chunk for x in excluded):
        current_string += ".%s" % chunk
        del pattern[0]
        try:
            chunk = pattern[0]
        except IndexError:
            break
    final_chunks = [current_string]
    final_chunks.extend(pattern)
    return final_chunks

def _match_entries(entries, pattern):
    """A drop-in replacement for fnmatch.filter that supports pattern 
    variants (ie. {foo,bar}baz = foobaz or barbaz)."""
    v1, v2 = pattern.find('{'), pattern.find('}')

    if v1 > -1 and v2 > v1:
        variations = pattern[v1 + 1:v2].split(',')
        variants = [pattern[:v1] + v + pattern[v2 + 1:] for v in variations]
        matching = []

        for variant in variants:
            matching.extend(fnmatch_filter(entries, variant))

        return list(_deduplicate(matching)) #remove dupes without changing order 

    else:
        matching = fnmatch_filter(entries, pattern)
        matching.sort()
        return matching

def _deduplicate(entries):
    yielded = set()
    for entry in entries:
        if entry not in yielded:
            yielded.add(entry)
            yield entry

def create_tables(host, port, table_prefix, transport):
    meta = "%s_%s" % (table_prefix, "META")
    data = "%s_%s" % (table_prefix, "DATA")

    client = happybase.Connection(
        host=host,
        port=port,
        table_prefix=table_prefix,
        transport=transport,
        compat="0.94",
        autoconnect=True
    )

    try:
        tables = client.tables()
    except Exception as e:
        print("HBase tables can't be retrieved. Cluster offline?")
        exit(1)

    if meta not in tables:
        print('Creating %s table' % meta)
        families = {'cf:': dict(compression="Snappy",
                                block_cache_enabled=True,
                                bloom_filter_type="ROW")}
        client.create_table(meta, families)
        meta_table = client.table("META")
        # add counter record
        meta_table.counter_set('CTR', 'cf:CTR', 1)
    elif data not in tables:
        print('Creating %s table' % data)
        families = {'cf:': dict(compression="Snappy",
                                bloom_filter_type='ROW',
                                block_cache_enabled=True)}
        client.create_table(data, families)
    else:
        print('Both Graphite tables available!')

class HbaseReader(object):
    __slots__ = ('db', 'metric', 'intervals', 'info')
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

