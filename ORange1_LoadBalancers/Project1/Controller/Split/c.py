import array,math

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ipv4,arp,ethernet
import Range,stats,CompareTable,ClientsTable,Elcp1Table,Elcp0Table,RidsTable

#A class for a controller designed to balance traffic between a fixed number of servers

class LoadBalancingSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    numOfClients=10 #number of hosts used as clients
  
    def __init__(self, *args, **kwargs):
        super(LoadBalancingSwitch, self).__init__(*args, **kwargs)
        self.subnet='192.168'
        self.firstPacketRange=-1
        self.numOfServers=-1
        self.totalHosts=-1
        self.areFlowsSet=False
        self.lastStats=[]
        self.ranges=[]
        self.servers=[] 
        self.clients=[]
    
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        pkt=packet.Packet(array.array('B', ev.msg.data))
        eth_pkt = pkt.get_protocol(ethernet.ethernet)
        
        #Handle incoming packet
        self.handlePacket(datapath,msg,pkt)

        #If packet is part of the first packets sent from hosts on network start , learn the hosts
        arp_pkt = pkt.get_protocol(arp.arp)
        if (arp_pkt):
            clientPort=self.getInPort(msg)
            tup=(clientPort,arp_pkt.src_ip,eth_pkt.src)
            if (clientPort<=LoadBalancingSwitch.numOfClients and tup not in self.clients):
                self.clients.append(tup)
            else:
                if clientPort>LoadBalancingSwitch.numOfClients and tup not in self.servers:
                    self.servers.append(tup)
                    self.servers =sorted(self.servers, key=lambda tup: tup[0])

        
        #Update number of hosts in network for ranges defining
        self.updateTopology(datapath)

        #Populate all flows tables, once all hosts were seen by the controller
        if  self.totalHosts ==len (self.servers)+len(self.clients):
            self.initializeFlows(datapath)
        

    #Given a packet-in message, returns the port the packet was sent from
    def getInPort(self,msg):
        for f in msg.match.fields:
            if f.header == ofproto_v1_3.OXM_OF_IN_PORT:
                in_port = f.value
        return in_port

    #Updates information about the topology (number of host,servers and clients)
    def updateTopology(self,datapath):
        if (self.numOfServers==-1):
            self.numOfServers=len(datapath.ports)-(LoadBalancingSwitch.numOfClients+1)
        if (self.totalHosts==-1):
            self.totalHosts=len(datapath.ports)-1

    #Defines all the flows for all the tables for the first time
    def initializeFlows(self,datapath):
        #make sure we only set once
        if (not self.areFlowsSet):

            #Define all the flow for the tables
            self.logger.info("\nDefining all flows...")
            #get initial weights for servers, used for balancing, all 1 by default.
            self.getDefaultWeights()

            #Create a range object for each server using the initilaized weights
            #self.setRanges()
            self.setRangesSubnetVersion(self.subnet)

            #Define all flows
            self.defineAllFlows(datapath,True)

            #Mark job as done to make sure it's only done once
            self.areFlowsSet=True
            
            #Start the timer which receives stats from the RIDS table
            self.flow_request(datapath)


    #Since the switch is defined to handle all traffic, this function is used only for the first arp packets send while the controller is learning the network
    #Acts as a hub.
    def handlePacket(self,datapath,msg,pkt):
        ofproto=ofproto_v1_3
        parser = datapath.ofproto_parser
        outputAction = parser.OFPActionOutput(ofproto.OFPP_FLOOD,ofproto.OFPCML_NO_BUFFER)
        packet_out = parser.OFPPacketOut(datapath, 0xffffffff,
                                         ofproto.OFPP_CONTROLLER,
                                         [outputAction], msg.data)    
        datapath.send_msg(packet_out)
    
    #Populates all tables with flows, a flag is used since the first and third table are static and are defined only once at start,
    # unlike the others which might be cleared in case of a rebalancing.
    def defineAllFlows(self,datapath,firstTime):
        if (firstTime):
            ClientsTable.prepareStable(datapath,self.clients,self.servers)
        Elcp1Table.prepareELCP1Table(datapath,self.ranges)
        if (firstTime):
            CompareTable.prepareCompareTable(datapath)
        Elcp0Table.prepareELCP0Table(datapath,self.ranges)
        RidsTable.prepareRIDTable(datapath,self.ranges,self.servers,LoadBalancingSwitch.numOfClients)
         
                        
#-------------------------Load balancing------------------------------------------------
 
    #Given the recent packet counts from the switch, checks if a rebalance is required. If so, clears all the dynamic tables and redefines the ranges and flows
    #according to the newly assigned weights
    def doBalancing(self, newCounts, dp):
        counts=newCounts
        if (len(self.lastStats)>0):
            counts=self.deductArrays(newCounts,self.lastStats)
        self.lastStats=newCounts 
        print "Statistics recieved!"
        print "Interval packet counts: %s" % counts
        print "Overall packet counts: %s" % newCounts
        oldWeights=list(self.weights)
        weightsChanged=self.getNewWeights(newCounts)
        if weightsChanged:
            self.logger.info("Performing rebalancing...")
            print "Former weights: %s" % oldWeights
            print "New weights: %s" % self.weights
            self.clearTables(dp)
            Range.Range.idGen=0
            #self.setRanges()
            self.setRangesSubnetVersion(self.subnet)
            self.defineAllFlows(dp,False)
            self.lastStats=[]
        else:
            self.logger.info("No rebalancing required")
        print "-----"

    # If needed, changes the former server weights according to packet count statistics recived from the switch.
    # A server is considered overloaded if its packet count is more than 1.5 times the average packet count in all servers.
    def getNewWeights(self,packets):
        flag=False
        avgPacketCount= self.avg(packets)
        print "Average packets in server: %3f" % avgPacketCount
        if (avgPacketCount>10):
            for i in range(0,len(packets)):
                if (packets[i]>avgPacketCount*1.5):
                    flag=True
                    self.weights[i]=self.weights[i]* (avgPacketCount/packets[i])
        return flag
   
    # Retruns a new list in which the integer in the Ith position is the result of the Ith element of the second list deducted from the Ith element of the first list
    def deductArrays(self,l1,l2):
        res=[]
        for i in range(0,len(l1)):
            res.append(l1[i]-l2[i])
        return res

    #Given an integer list, return the list average
    def avg(self,packets):
        return sum(packets) / float(len(packets))

    #Removes all flows from all the dynamic tables (3 total), used in case of a rebalancing 
    def clearTables(self, dp):
        for i in [1, 3, 4]:
            self.remove_flows(dp, i)
    
    #Sends a message to a table (by table_id) to clear all flows        
    def remove_flows(self, datapath, table_id):
        flow_mod = self.remove_table_flows(datapath, table_id)
        datapath.send_msg(flow_mod)

    #Given a table id, creates a message to the switch to clear all flows from table
    def remove_table_flows(self, datapath, table_id):
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto
        match = parser.OFPMatch()
        inst = []
        flow_mod = datapath.ofproto_parser.OFPFlowMod(datapath, 0, 0,table_id,
                                                      ofproto.OFPFC_DELETE,0, 0,
                                                      1,ofproto.OFPCML_NO_BUFFER,
                                                      ofproto.OFPP_ANY,
                                                      ofproto.OFPG_ANY, 0,
                                                      match, inst)
        return flow_mod

    #Defines initial weights for servers, by default all 1
    def getDefaultWeights(self):
        res=[]
        for i in range(0,self.numOfServers):
            res.append(1)
        self.weights=res
        return res

     
    #Using the weights defined in the controller, divides the entire IP spectrum into ranges, creates a range object for each one and adds it to a global list       
    def setRanges(self):
        self.ranges=[]
        jump = math.floor(4294967296 / sum(self.weights))
        fill = 4294967296 - jump * sum(self.weights)
        for i in range(0, len(self.weights)):
            rangeStart = jump * sum(self.weights[0:i])
            rangeEnd = jump * sum(self.weights[0:i + 1]) - 1
            if (i == len(self.weights) - 1):
                rangeEnd += fill
            self.ranges.append(Range.Range(rangeStart, rangeEnd))
        #self.printRanges()
  

    #A secondary version for SetRanges, designed for a subnet
    def setRangesSubnetVersion(self, subnet):
        self.ranges=[]
        subnetArr= subnet.split(".")
        subnetLen= len(subnetArr)
        for i in range(0,4-subnetLen):
           subnet+=".0"
        factor=Range.IP2Int(subnet)
        spectrum= pow(2, 8*(4-subnetLen))
        jump = math.floor(spectrum / sum(self.weights))
        fill = spectrum - jump * sum(self.weights)
        for i in range(0, len(self.weights)):
            rangeStart = factor+ jump * sum(self.weights[0:i])
            rangeEnd = factor+ jump * sum(self.weights[0:i + 1]) - 1
            if (i == len(self.weights) - 1):
                rangeEnd += fill
            self.ranges.append(Range.Range(rangeStart, rangeEnd))
        #self.printRanges()
        

    #Prints all ranges defined in the switch, used for debugging
    def printRanges(self):
        for i in range (0, len(self.ranges)):
            print ("-----------")
            print ("%s" % self.ranges[i])

        
#--------------------Flow statistics----------------------------- 

    #Starts a thread which will send stats requests to the switch
    def flow_request(self, dp):
        self.statTimerOn = True
        monitor = stats.StatsMonitor(dp)
        monitor.start()

    #Handle stats recieved from switch
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _stats_reply_handler(self, ev):
        packetCounts = []
        msg = ev.msg
        dp = msg.datapath
        #print "Flow statistics:"
        for stats in ev.msg.body:
            packetCount = stats.packet_count
            index = stats.cookie
            tableID = stats.table_id
            packetCounts.append((index,stats.packet_count))

        #Packet count are represented as a tuple (id, count) and sorted by the id of the flow, which represents the id of the range
        sorted_by_index = sorted(packetCounts, key=lambda tup: tup[0])

        #Do rebalancig, if required
        self.doBalancing(self.extractCounts(sorted_by_index), dp)
     
    #Given a list of 2-tuples, returns a list of the second part of each tuple
    def extractCounts(self, tuppleList):
        res=[]
        for i in range(0,len(tuppleList)):
            res.append(tuppleList[i][1])
        return res
            
            

