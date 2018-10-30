#!/usr/bin/env python3
import json
import sys
import os
from termcolor import colored

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <left:path-to-result-directory> <right:path-to-result-directory> " % sys.argv[0], file=sys.stderr)
    left = sys.argv[1]
    right = sys.argv[2]

    more_is_better = set(['frag/s', 'max f/s', 'min f/s'])
    should_be_equal = set(['frags'])

    for test_name in os.listdir(left):
        for d, s, f in os.walk(os.path.join(left, test_name)):
            for test_case in f:
                left_path = os.path.join(left, test_name, test_case)
                right_path = os.path.join(right, test_name, test_case)
                if not os.path.exists(right_path):
                    print("File not found: %s" % right_path, file=sys.stderr)
                    continue
                print("\n === Comparing %s/%s === \n" % (test_name, test_case))
                left_f = open(left_path)
                right_f = open(right_path)
                left_json = json.load(left_f)
                right_json = json.load(right_f)
                print(left_json["results"]["parameters"])
                for stat, left_v in left_json["results"]["stats"].items():
                    right_v = right_json["results"]["stats"][stat]
                    if left_v:
                        change = (right_v - left_v) * 100. / left_v
                        change_tolerance = 0 if stat in should_be_equal else 1
                        less_is_better = not stat in more_is_better and not stat in should_be_equal
                        if (less_is_better and change < 0) or (stat in more_is_better and change > 0):
                            color = 'green'
                        elif abs(change) > change_tolerance:
                            color = 'red'
                        else:
                            color = 'grey'

                        annotation = colored('(%s%.2f%%)' % (('', '+')[change >= 0], change), color)
                    elif right_v:
                        if stat in should_be_equal or not stat in more_is_better:
                            color = 'red'
                        else:
                            color = 'grey'
                        annotation = colored('(+inf)', color)
                    else:
                        annotation = ''
                    print("%-10s: %-20s %-20s %s" % (stat, left_v, right_v, annotation))
