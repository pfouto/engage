#!/usr/bin/env python3

import json

filename = "tree_16_4_global"

tree_file = open(f'../config/{filename}.json')
output = open(f'../config/tc_{filename}', "w")

data = json.load(tree_file)

nodes = data['nodes']

latency = [[0, 213, 145, 257],  # "Asia Pacific (Tokyo) ap-northeast-1"
           [210, 0, 80, 190],  # "EU (London) eu-west-2"
           [149, 78, 0, 118],  # "US East (N, Virginia) us-east-1"
           [258, 190, 117, 0]]  # "SA (São Paulo) sa-east-1"
dc_up = 10000
dc_down = 10000
edge_up = 500
edge_down = 1000

n_clients = 12

output.write("# Server Nodes:\n")
for i in nodes:
    server_reg = nodes[i]['region']
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
                add += 10
            if not nodes[j]['dc']:
                add += 10
            lat = str(round(latency[server_reg][nodes[j]['region']] / 2 + add))
            output.write(lat + " " * (5 - len(str(lat))))
    for j in range(1, n_clients + 1):
        client_reg = nodes["node-" + str(j)]['region']
        add = 10
        lat = str(round(latency[client_reg][server_reg] / 2 + add))
        output.write(lat + " " * (5 - len(str(lat))))
    output.write("\n")
output.write("# Client Nodes:\n")

for i in range(1, n_clients + 1):
    output.write(str(0) + " " * (6 - len(str(0))) + str(0) + " " * (10 - len(str(0))))
    client_reg = nodes["node-" + str(i)]['region']
    for j in nodes:
        server_reg = nodes[j]['region']
        add = 10
        lat = str(round(latency[client_reg][server_reg] / 2 + add))
        output.write(lat + " " * (5 - len(str(lat))))
    output.write("\n")
