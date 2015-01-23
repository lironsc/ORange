import Flow,Range
from ryu.ofproto import ofproto_v1_3

#This file contains all the logic for populating the last table, used for the balancing of traffic

#Creates a flow for the table, one for each range
def createFourthTableFlow(flowRange, index, datapath,servers,numOfClients):
    ofproto=ofproto_v1_3
    parser = datapath.ofproto_parser
    match = datapath.ofproto_parser.OFPMatch(eth_type=0x800,metadata=Range.fromBinary(Range.toBinary(int(flowRange.ID)) + flowRange.end))
    #If a match is found, send the packet to the server which is assigned to the matched range
    actions = [datapath.ofproto_parser.OFPActionSetField(ipv4_dst= servers[index][1]),
                   datapath.ofproto_parser.OFPActionSetField(eth_dst= servers[index][2]),
                   datapath.ofproto_parser.OFPActionOutput(int(flowRange.ID )+ 1 +numOfClients)]
    apply = parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,actions)
    inst = [apply]
    return Flow.createFlow(datapath,index,4,100-index,match,inst)
        
#Install all flows in table    
def prepareRIDTable(dp,ranges,servers,numOfClients):
    for i in range(0, len(ranges)):
        dp.send_msg(createFourthTableFlow(ranges[i], i, dp,servers,numOfClients))
