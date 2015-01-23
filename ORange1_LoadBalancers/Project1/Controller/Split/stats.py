import threading,time

#This class represents a thread which is used to ask statistics about the Rids table from the datapath every predetermined period of time, used for rebalancing the servers.

class StatsMonitor(threading.Thread):
    def __init__(self, dp):
        super(StatsMonitor, self).__init__()
        self.dp = dp
          
    def run(self):
        interval=15
        print "Stats will be sent every %d seconds!" % interval
        print "-----\n"
        dp = self.dp
        while True:
            time.sleep(interval)
            stats = dp.ofproto_parser.OFPFlowStatsRequest(datapath=dp,table_id=4)
            dp.send_msg(stats)
