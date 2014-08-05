#!/usr/local/rnt/bin/python

from time import time
import argparse
import os
import sys

from carbon.plugin import HbaseDB
import carbon.analyzer.algorithms 

os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'
sys.path.append('/usr/local/rnt/webapp')
from graphite.storage import FindQuery

def get_cli():
    parser = argparse.ArgumentParser(description='Get metric and do math!')
    parser.add_argument('--hours', metavar='n', type=int, 
                        default=1, help='Back to collect.')
    parser.add_argument('--path', metavar='p', type=str, 
                        default=None, help='grab all metrics matching')
    return parser.parse_args()

def main():
    args = get_cli()
    hours = args.hours
    start = int(time()) / 3600 * 3600 - (hours * 3600) # Hacky way to grab floored time
    end = int(time()) / 3600 * 3600

    hbase = HbaseDB()
   
    query = analyzer_utils.FindQuery(args.path, start, end)
    bad_metrics = []
    for metric in hbase.find_metrics(query):
        try:
            points = metric.reader.fetch(start, end)
        except Exception as e:
            continue
        count = 0
        for algorithm in algorithms.algorithm_names:
            try:
                results = algorithm(points)
            except:
                continue
            if results:
                count += 1
        if count >= len(algorithms.algorithm_names) - 1:
            bad_metrics.append(metric.path)
    print('Bad: %s' % bad_metrics)

if __name__ == '__main__':
    main()

