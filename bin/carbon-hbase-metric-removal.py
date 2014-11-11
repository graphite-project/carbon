#!/usr/local/rnt/bin/python

from carbon.plugin import HbaseDB
import argparse, sys, os
from time import time
os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'
sys.path.append('/usr/local/rnt/webapp')
from graphite.storage import FindQuery
from graphite.node import LeafNode, BranchNode


patterns = ['*', '[', ']', '{', '}']

db = HbaseDB()

def _get_nodes(query):
    results = db.find_nodes(query)
    metric_list = []
    branch_list = []
    for node in results:
        if isinstance(node, LeafNode):
            metric_list.append(node.path)
        else:
            branch_list.append(node.path)
            node = FindQuery(node.path, 0, time())
            mlst, blst = _get_nodes(node)
            metric_list.extend(mlst)
            branch_list.extend(blst)
    return metric_list, branch_list

def main(metric):
    if any(ex in metric for ex in patterns):
        metric_query = FindQuery(metric, 0, time())
        metric_list, branch_list = _get_nodes(metric_query)
    else:
        metric_list = [metric]
    for metric in metric_list:
        print('Deleting %s' % metric)
        db.delete(metric)

    batch = db.meta_table.batch(batch_size=db.batch_size)

    for branch in branch_list:
        print('Deleting branch %s' % branch)
        batch.delete("m_%s" % branch)
    batch.send()

def cli_opts():
    parser = argparse.ArgumentParser(
        description="Delete a single metric or a group of metrics from HBase")
    parser.add_argument('--metric', '-m',
        action='store', dest='metric', required=True,
        help='The number of days to collect for')
    return parser.parse_args()
if __name__ == '__main__':
    main(cli_opts().metric)

