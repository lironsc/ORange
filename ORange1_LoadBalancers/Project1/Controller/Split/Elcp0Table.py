import Flow,Range
from ryu.ofproto import ofproto_v1_3

#This file contains all the logic for populating the fourth table, used for the balancing of traffic

#Creates a flow for the table, one for each range, representing the start of a range
def createThirdTableFlow(flowRange, datapath):
    ofproto=ofproto_v1_3
    match = datapath.ofproto_parser.OFPMatch(eth_type=0x800,ipv4_src=flowRange.getZeroELCP())
    #If a match is found, send to the last table which will send the packet to the chosen server
    inst = [datapath.ofproto_parser.OFPInstructionGotoTable(4),
               datapath.ofproto_parser.OFPInstructionWriteMetadata(Range.fromBinary(Range.toBinary(int(flowRange.ID)) +flowRange.end), Flow.getMetaDataMask(), type_=None, len_=None)]     
    return Flow.createFlow(datapath,int(flowRange.ID),3,100-Range.starsInString(flowRange.zeroELCP),match,inst)
        
#Install all flows in table
def prepareELCP0Table(dp,ranges):
    for i in range(0, len(ranges)):
        dp.send_msg(createThirdTableFlow(ranges[i], dp)) 
