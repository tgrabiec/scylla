#!/usr/bin/env python3

# Works on sorted merged log, collected like this:
#
# Usage:
#
#   egrep -h '(repair|Suppressed|migration_manager|schema_table|version changed|listening|Shutting down local)' \
#       logs-scylla-* | sort -s -k1,3 > log
#   ./schema-chg-vis.py log
#
# You may need to adjust -k argument to sort to catch the timestamp properly.
#
# You may also need to adjust the following variables in the script:
#
#  name_to_ip - host names to IPs
#  parse_time_and_host() - to recognize timestamp pattern of your log
#

import datetime
import re

import matplotlib
import sys
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from collections import defaultdict
from matplotlib import collections  as mc
import matplotlib.cm as cmx
import matplotlib.colors as colors


name_to_ip = dict()

ip_to_name = {v: k for k, v in name_to_ip.items()}

cmap = matplotlib.colors.TABLEAU_COLORS
cnames = list(cmap)

color_by_version = dict()

class Pull(object):
    def __init__(self, t, node, v1):
        self.t = t
        self.node = node
        self.v1 = v1
        self.v2 = None
        self.end = None
        self.incomplete = False

    def __str__(self) -> str:
        return 'pull: %s, %s, %s, %s' % (self.node, self.t, self.end, self.v1)

    def __repr__(self) -> str:
        return str(self)


class Node(object):
    def __init__(self):
        self.versions = list()
        self.pulls = dict()
        self.repairs = list()
        self.pull_history = list()
        self.restarts = list()
        self.last_schema_change_time = None
        self.last_schema_version = None
        self.last_time = None

    def version(self):
        return None if not self.versions else self.versions[-1][1]


nodes = defaultdict(Node)


def parse_time_and_host(line):
    m = re.match(r"^(\w+ \d+ \d+:\d+:\d+) ([\w\-]+) (.*)", line)
    if m:
        t = datetime.datetime.strptime(m.group(1), '%b %d %H:%M:%S')
        return t, m.group(2), m.group(3)

    # 2022-02-07T16:57:31+00:00
    m = re.match(r"^(\d+-\d+-\d+T\d+:\d+:\d+)\+00:00 ([\w\-]+) (.*)", line)
    if m:
        t = datetime.datetime.strptime(m.group(1), '%Y-%m-%dT%H:%M:%S')
        return t, m.group(2), m.group(3)


def parse_scylla_line(line):
    m = re.match(r"^.*shard[ ]+(\d+)] (.*)", line)
    if m:
        return m.group(1), m.group(2)


min_time = datetime.datetime.strptime('Feb 9 00:00:00', '%b %d %H:%M:%S')
last_time = None
agreement = None

f = open(sys.argv[1], 'r')

for line in f:
    m = parse_time_and_host(line)
    if not m:
        continue
    t, node, line = m

    m = parse_scylla_line(line)
    if not m:
        continue
    shard, msg = m

    m = re.match(r"^.* Starting listening for CQL clients on ([\d.]+):9042.*", msg)
    if m and not node in name_to_ip:
        ip = m.group(1)
        print(node, ip)
        name_to_ip[node] = ip
        ip_to_name[ip] = node
        nodes[node].restarts.append(t)
        continue

f = open(sys.argv[1], 'r')

for line in f:
    m = parse_time_and_host(line)
    if not m:
        continue
    t, node, line = m

    m = re.match(r"^.*systemd-journal.*Suppressed.*", line)
    if m:
        print('SS', line)
        for p in nodes[node].pulls.values():
            p.end = t
            p.incomplete = True
        nodes[node].pulls.clear()
        continue

    m = parse_scylla_line(line)
    if m:
        shard, msg = m
        last_time = t

        if t < min_time:
            continue

        m = re.match(r".* Schema version changed to ([a-z0-9\-]+)", msg)
        if m:
            v = m.group(1)

            n = nodes[node]
            this_v = n.last_schema_version
            if this_v:
                if this_v not in color_by_version:
                    color_by_version[this_v] = cmap[cnames[len(color_by_version) % len(cnames)]]
                c = color_by_version[this_v]
                n.versions.append(((n.last_schema_change_time, t), this_v))

            n.last_schema_change_time = t
            n.last_schema_version = v

            if agreement and v != agreement:
                agreement = None
            if not agreement and all(n.last_schema_version == v for n in nodes.values()):
                agreement = v
                print(t, 'Agreement ', v)

            continue

        m = re.match(r"^.* Starting listening for CQL clients on ([\d.]+):9042.*", msg)
        if m:
            ip = m.group(1)
            print(node, ip)
            name_to_ip[node] = ip
            ip_to_name[ip] = node
            nodes[node].restarts.append(t)
            continue

        m = re.match(r"^.* Pulling schema from ([0-9.]+):0", msg)
        if m:
            pull_from = m.group(1)
            if pull_from not in ip_to_name:
                raise Exception('Unknown host: %s' % pull_from)
            target = ip_to_name[pull_from]
            x = nodes[target]
            assert target not in nodes[target].pulls or nodes[node].pulls[(target, shard)].end
            p = Pull(t, (target, shard), nodes[node].version())
            nodes[node].pulls[(target, shard)] = p
            nodes[node].pull_history.append(p)
            continue

        m = re.match(r"^.* Schema merge with ([0-9.]+):0 completed", msg)
        if m:
            pull_from = m.group(1)
            if pull_from not in ip_to_name:
                raise Exception('Unknown host: %s' % pull_from)
            target = ip_to_name[pull_from]
            if (target, shard) in nodes[node].pulls:
                x = nodes[target]
                last_pull = nodes[node].pulls[(target, shard)]
                assert last_pull.end is None
                last_pull.end = t
                last_pull.v2 = nodes[node].version()
            continue

        m = re.match(r"^.*Repair \d+ out of \d+ ranges", msg)
        if m:
            nodes[node].repairs.append(t)

for n in nodes.values():
    v = n.last_schema_version
    if v:
        if v not in color_by_version:
            color_by_version[v] = cmap[cnames[len(color_by_version)]]
        c = color_by_version[v]
        n.versions.append(((n.last_schema_change_time, last_time), v))

yticks = list()
ytickslabels = list()

fig, ax = plt.subplots()

y = 0
h = 33
for n in nodes.values():
    n.y = y
    y += h

for node, n in nodes.items():
    y = n.y
    bars = list()
    colors = list()
    for p in n.versions:
        x1, x2 = p[0]
        v = p[1]
        c = color_by_version[v]
        bars.append((x1, x2 - x1))
        colors.append(c)
    ax.broken_barh(bars, (y, h - 4), facecolors=colors, edgecolor='face')

    restarts = list((x, y) for x in n.restarts)
    ax.scatter(n.restarts, list(y for x in n.restarts), s=h, c=cmap['tab:red'], marker='^')
    ax.scatter(n.repairs, list(y for x in n.repairs), s=h, c=cmap['tab:green'], marker='x')

    yticks.append(y + h/2)
    ytickslabels.append(node)


# Print pull history
pulls = list()
for name, n in nodes.items():
    for p in n.pull_history:
        # if name == 'scylla-channels-prd-2-10' or p.node == 'scylla-channels-prd-2-10':
        p.source = name
        pulls.append(p)

for p in sorted(pulls, key=lambda p: p.t):
    if not p.end:
        print(p.source, 'pull from ', p.node, p.t)
    else:
        print(p.source, 'pull from ', p.node, p.t, p.end - p.t, '+' if p.incomplete else '')

# for node, n in nodes.items():
#     y = n.y
#     for p in n.pull_history:
#         if not p.end:
#             continue
#         target = p.node[0]
#         if not p.v1:
#             color = color_by_version[n.versions[-1][1]]
#         else:
#             color = color_by_version[p.v1]
#
#         # ax.scatter([p.t], [y], s=10*h, marker='x')
#         # ax.scatter([p.end], [y], s=10*h, c=color_by_version[p.v2], marker='o')
#
#         # ax.arrow(p.t, y, 0, nodes[target].y - y, head_width=0.05, head_length=0.1, fc='k', ec='k', color=color)
#         # ax.arrow(p.t, nodes[target].y, matplotlib.dates.date2num(p.end) - matplotlib.dates.date2num(p.t), y - nodes[target].y, color=color_by_version[p.v2])
#     break

ax.set_yticks(yticks)
ax.set_yticklabels(ytickslabels)

ax.grid(True)
ax.annotate('schema changes', (61, 25),
            xytext=(0.8, 0.9), textcoords='axes fraction',
            arrowprops=dict(facecolor='black', shrink=0.05),
            fontsize=16,
            horizontalalignment='right', verticalalignment='top')


patches = []
labels = []
for v, c in color_by_version.items():
    label = v
    patches.append(mpatches.Patch(color=c, label=label))
    labels.append(label)
plt.legend(patches, labels, loc='lower right')

plt.show()
