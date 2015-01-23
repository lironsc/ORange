import Range
from ryu.ofproto import ofproto_v1_3


#Create a flowmod objects ready to be sent to the datapath, the command is "add".
def createFlow(dp,cookie,table,priority,match,inst):
       ofproto=ofproto_v1_3
       return dp.ofproto_parser.OFPFlowMod(dp, cookie, 0, table,
                                                     ofproto.OFPFC_ADD, 0, 0,
                                                     priority,
                                                     ofproto.OFPCML_NO_BUFFER,
                                                     ofproto.OFPP_ANY,
                                                     ofproto.OFPG_ANY, 0,
                                                     match, inst)

#Returns a string composed of 64 ones, used for masking the metadata
def getMetaDataMask():
    str=""
    for i in range (0,64):
        str+="1"
    return Range.fromBinary(str)
