#!/usr/bin/env python3
import json
import sys
import os
from termcolor import colored

class Result:
    def __init__(self, path, params):
        self.path = path
        self.params = params

    def throughput_relative_change(self):
        frags = self.params['frag/s']
        return float(frags[1] - frags[0])/frags[0]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <left:path-to-result-directory> <right:path-to-result-directory> " % sys.argv[0], file=sys.stderr)
    left = sys.argv[1]
    right = sys.argv[2]

    more_is_better = set(['frag/s', 'max f/s', 'min f/s'])
    should_be_equal = set(['frags'])

    results = list()

    for test_name in os.listdir(left):
        for dataset in os.listdir(os.path.join(left, test_name)):
            for d, s, f in os.walk(os.path.join(left, test_name, dataset)):
                for test_case in f:
                    if not test_case.endswith('.json'):
                        continue
                    left_path = os.path.join(left, test_name, dataset, test_case)
                    right_path = os.path.join(right, test_name, dataset, test_case)
                    if not os.path.exists(right_path):
                        print("File not found: %s" % right_path, file=sys.stderr)
                        continue
                    path = "%s/%s/%s" % (test_name, dataset, test_case)
                    left_f = open(left_path)
                    right_f = open(right_path)
                    left_json = json.load(left_f)
                    right_json = json.load(right_f)
                    params = {}
                    for param, left_v in left_json["results"]["parameters"].items():
                        if ',' in param: # drop the entry which contains all params concatenated
                            continue
                        right_v = right_json["results"]["parameters"][param]
                        params[param] = (left_v, right_v)
                    for stat, left_v in left_json["results"]["stats"].items():
                        right_v = right_json["results"]["stats"][stat]
                        params[stat] = (left_v, right_v)
                    results.append(Result(path, params))

    for result in sorted(results, key=lambda r: r.throughput_relative_change()):
        print("\n === Comparing %s === \n" % result.path)
        align = '%-10s'
        for stat, (left_v, right_v) in result.params.items():
            annotation = None
            try:
                if left_v:
                    change = (right_v - left_v) * 100. / left_v
                    if change:
                        change_tolerance = 0 if stat in should_be_equal else 1
                        less_is_better = not stat in more_is_better and not stat in should_be_equal
                        if (less_is_better and change < 0) or (stat in more_is_better and change > 0):
                            color = 'green'
                        elif abs(change) > change_tolerance:
                            color = 'red'
                        else:
                            color = 'yellow'
                        annotation = colored(align % ('(%s%.2f%%)' % (('', '+')[change >= 0], change)), color)
                elif right_v:
                    if stat in should_be_equal or not stat in more_is_better:
                        color = 'red'
                    else:
                        color = 'grey'
                    annotation = colored(align % '(+inf)', color)
            except TypeError:
                pass
            if annotation:
                print("%-10s %s : %-20s %-20s " % (stat, annotation, left_v, right_v))
            else:
                print("%-21s : %-20s %-20s " % (stat, left_v, right_v))
