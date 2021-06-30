#!/usr/bin/env python3

import json

filename = "tree_16_4zones"

tree_file = open(f'../config/{filename}.json')
output = open(f'../config/tc_{filename}', "w")

data = json.load(tree_file)

nodes = data['nodes']

latency = [[0, 194, 200, 313],
           [189, 0, 90, 205],
           [197, 91, 0, 116],
           [312, 204, 115, 0]]
dc_up = 5000
dc_down = 5000
edge_up = 500
edge_down = 1000

n_clients = 12

output.write("# Server Nodes:\n")
for i in nodes:
    if nodes[i]['dc']:
        output.write(str(dc_down) + " " * (6 - len(str(dc_down))) + str(dc_up) + " " * (10 - len(str(dc_up))))
    else:
        output.write(str(edge_down) + " " * (6 - len(str(edge_down))) + str(edge_up) + " " * (10 - len(str(edge_up))))

    for j in nodes:

        if j == i:
            output.write("-1" + " " * 3)
        else:
            add = 0
            if not nodes[i]['dc']:
                add += 20
            if not nodes[j]['dc']:
                add += 20
            lat = str(round(latency[nodes[i]['region']][nodes[j]['region']] / 2 + add))
            output.write(lat + " " * (5 - len(str(lat))))
    output.write("\n")
output.write("# Client Nodes:\n")

for i in range(1, n_clients + 1):
    output.write(str(0) + " " * (6 - len(str(0))) + str(0) + " " * (10 - len(str(0))))
    client_reg = nodes["node-" + str(i)]['region']
    for j in nodes:
        server_reg = nodes[j]['region']
        add = 20
        if not nodes[j]['dc']:
            add += 20
        lat = str(round(latency[client_reg][server_reg] / 2 + add))
        output.write(lat + " " * (5 - len(str(lat))))
    output.write("\n")
