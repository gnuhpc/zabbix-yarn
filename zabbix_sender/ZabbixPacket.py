import json
import time


class ZabbixPacket:
    def __init__(self):
        self.packet = {'request': 'sender data',
                       'data': []}

    def __str__(self):
        return json.dumps(self.packet)

    def add(self, host, key, value, clock=("%d" % time.time())):
        metric = {'host': str(host),
                  'key': str(key),
                  'value': str(value),
                  'clock': int(clock)}

        self.packet['data'].append(metric)

    def clean(self):
        self.packet = {'request': 'sender data',
                       'data': []}
