import socket
import requests
from zabbix_sender.ZabbixPacket import ZabbixPacket
from zabbix_sender.ZabbixSender import ZabbixSender
from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager

ZABBIX_ADDR = '197.3.128.135'
ZABBIX_PORT = '10051'
JMX_ADDR= 'http://BIGL2TMP:8088/jmx'
RM_ADDR = ('hadoop1','hadoop2')
#RM_ADDR = ('BIGL1TMP','BIGL2TMP')

class ZabbixHadoop:
    def __init__(self, apitype , zaddr=ZABBIX_ADDR,zport=ZABBIX_PORT):
        self._API_TYPE = {
            1: {
                'API_ID': 'clusterInfo',
                'API_PREFIX': 'RM',
                'API_ADDRESS': 'http://RMADDRESS:8088/ws/v1/cluster/info',
                'KEY_PREFIX': 'Info'
            },
            2: {
                'API_ID': 'clusterMetrics',
                'API_PREFIX': 'RM',
                'API_ADDRESS': 'http://RMADDRESS:8088/ws/v1/cluster/metrics',
                'KEY_PREFIX': 'Metrics'
            },
            3:{
                'API_ID': 'scheduler',
                'API_PREFIX': 'RM',
                'API_ADDRESS': 'http://RMADDRESS:8088/ws/v1/cluster/scheduler',
                'KEY_PREFIX': 'Scheduler'
            },
            4:{
                'API_ID': 'apps',
                'API_PREFIX': 'RM',
                'API_ADDRESS': 'http://RMADDRESS:8088/ws/v1/cluster/apps',
                'KEY_PREFIX': 'Apps'
            },
            5: {
                'API_ID':'appStatInfo',
                'API_PREFIX':'RM',
                'API_ADDRESS':'http://RMADDRESS:8088/ws/v1/cluster/appstatistics',
                'KEY_PREFIX':'AppStatInfo'
            },
            6: {
                'API_ID':'nodes',
                'API_PREFIX':'RM',
                'API_ADDRESS':'http://RMADDRESS:8088/ws/v1/cluster/nodes',
                'KEY_PREFIX':'Nodes'
            },
        }
        self._activerm = self._get_activerm()
        self.apitype= apitype
        self.zaddr = zaddr
        self.zport = zport
        self.ret_result = ''
        self.final_result ={}
        self.zbserver = ZabbixSender(zaddr, zport)
        self._HOSTNAME = socket.gethostname()


    def collect_metrics(self):
        self.ret_result = requests.get(self._API_TYPE[self.apitype]['API_ADDRESS'].replace('RMADDRESS',self._activerm))
        if self.ret_result.status_code != 200:
            return -1
        else:
            self.ret_result = self.ret_result.json()
            print self.ret_result
            if self.apitype==2:
                self.final_result = self.ret_result[self._API_TYPE[self.apitype]['API_ID']]

    def send_zabbix(self):
        packet = ZabbixPacket()
        for k,v in self.final_result.iteritems():
            packet.add(self._API_TYPE[self.apitype]['API_PREFIX']+'_'+self._HOSTNAME, self._API_TYPE[self.apitype]['KEY_PREFIX']+'['+k+']', v)
        self.zbserver.send(packet)
        print self.zbserver.status


    def _get_activerm(self):
        for addr in RM_ADDR:
            ret_val = requests.get(self._API_TYPE[1]['API_ADDRESS'].replace('RMADDRESS',addr))
            if ret_val.status_code == 200:
                json_val = ret_val.json()[self._API_TYPE[1]['API_ID']]
                if json_val['haState'] == 'ACTIVE' and json_val['state'] == 'STARTED':
                    return  addr




if __name__ == '__main__':
    #For Cluster Metrics
    zh1  = ZabbixHadoop(apitype=2)
    zh1.collect_metrics()
    zh1.send_zabbix()

    #For

