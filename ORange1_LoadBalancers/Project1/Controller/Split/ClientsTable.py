import Flow
from ryu.ofproto import ofproto_v1_3

#This file contains all the logic for populating the first table, used for traffic from servers to clients, which should no be run through the balancing process

#Create a flow for this table, one is created for each client
def createStableFlow(clientIP,clientPort,datapath,servers):
    ofproto=ofproto_v1_3
    parser = datapath.ofproto_parser
    match = datapath.ofproto_parser.OFPMatch(eth_type=0x800,ipv4_dst=clientIP)
    actions = [datapath.ofproto_parser.OFPActionSetField(ipv4_src=servers[len(servers)-1][1]),
                   datapath.ofproto_parser.OFPActionSetField(eth_src=servers[len(servers)-1][2]),
                   datapath.ofproto_parser.OFPActionOutput(clientPort)]
    apply = parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,actions)
    inst = [apply]
    return Flow.createFlow(datapath,0,0,2,match,inst)

#Install the flow
def installStableFlow(clientIP,clientPort,datapath,servers):
    #print "Defining flow..IP={0} .  port={1}".format(clientIP,clientPort)
    datapath.send_msg(createStableFlow(clientIP,clientPort,datapath,servers))
  
#Creates a table miss flow, used in case the packet destination is not a client but rather a server, sends it to the balancing tables.      
def createStableMissFlow(datapath):
    ofproto=ofproto_v1_3
    match = datapath.ofproto_parser.OFPMatch()
    inst = [datapath.ofproto_parser.OFPInstructionGotoTable(1)]
    return Flow.createFlow(datapath,0,0,1,match,inst)

#Install all flows in table
def prepareStable(dp,clients,servers):
    for i in range(0,len(clients)):
        installStableFlow(clients[i][1],clients[i][0],dp,servers)
    dp.send_msg(createStableMissFlow(dp))   
