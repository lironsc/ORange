import Flow,Range
from ryu.ofproto import ofproto_v1_3

#This file contains all the logic for populating the second table, used for the balancing of traffic

#Creates a flow for the table, one for each range, representing the end of a range
def createFirstTableFlow(flowRange, datapath):
    ofproto=ofproto_v1_3
    match = datapath.ofproto_parser.OFPMatch(eth_type=0x800,ipv4_src=flowRange.getOneELCP())
    #If a match is found, write the range id and end into the metadata and send to third table in order to verify the range's end is higher than the source ip.
    inst = [datapath.ofproto_parser.OFPInstructionGotoTable(2),
               datapath.ofproto_parser.OFPInstructionWriteMetadata
               (Range.fromBinary(Range.toBinary(int(flowRange.ID)) +flowRange.end), Flow.getMetaDataMask(), type_=None, len_=None)]
    return Flow.createFlow(datapath,int(flowRange.ID),1,100-Range.starsInString(flowRange.oneELCP),match,inst)


#Creates a table miss in case no match was found, sends it to the fourth table in order to find a match againt the range's start.
def createFirstTableMissFlow(datapath): 
    ofproto=ofproto_v1_3
    match = datapath.ofproto_parser.OFPMatch()
    inst = [datapath.ofproto_parser.OFPInstructionGotoTable(3)]
    return Flow.createFlow(datapath,404,1,1,match,inst)

#Install all flows in table        
def prepareELCP1Table(dp,ranges):
    for i in range(0, len(ranges)):
        dp.send_msg(createFirstTableFlow(ranges[i], dp))
    dp.send_msg(createFirstTableMissFlow(dp))
