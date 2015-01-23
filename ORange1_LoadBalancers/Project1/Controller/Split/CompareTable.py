import Flow,Range
from ryu.ofproto import ofproto_v1_3

#This file contains all the logic for populating the compare table used to compare the ip source with a range's end

#Creates a flow using a spesific pattern
def createSecondTableFlow(pattern, index, datapath):
    ofproto = datapath.ofproto
    match = datapath.ofproto_parser.OFPMatch(eth_type=0x800,ipv4_src=getPatternIPPart(pattern),
                                                 metadata=getPatternEndPart(pattern))
    inst = []
    if (index % 2 == 1):
        # False case
        inst = [datapath.ofproto_parser.OFPInstructionGotoTable(3)]
    else:
        # True case
        inst = [datapath.ofproto_parser.OFPInstructionGotoTable(4)]
      
    return Flow.createFlow(datapath,0,2,1000-index,match,inst) 

#Populates the compare table with all flows
def prepareCompareTable(dp):
    patternList = getCompareTablePatterns()
    for i in range(0, len(patternList)):
        dp.send_msg(createSecondTableFlow(patternList[i], i, dp))

#Returns a list of all the patterns in the compare table
def getCompareTablePatterns():
    res=[]
    last=""
    for i in range(0,32):
        res.append(getPattern(i, True))
        res.append(getPattern(i, False))
        last+="**"
    res.append(last)
    return res

#Returns a string which has a length 64, composed of stars, except the characters in the Ith and i+32 positions, which are zero/one, according to the flag       
def getPattern(i,flag):
    res=""
    fst="1" if flag else "0"
    scnd="0" if flag else "1"
    for j in range(0,64):
        if (j==i):
            res+=fst
        elif (j==i+32):
            res+=scnd
        else:
            res+="*"
    return res

#Extracts, from the pattern ,the pattern used for matching againsts a range's end
def getPatternEndPart(s):
    end= s[:int(len(s)/2)]
    end= emptyPattern()+end
    return (Range.fromBinary(pattern2Ip(end)),Range.fromBinary(pattern2Mask(end)))

#Returns a string of 32 stars, used as a wildcard pattern
def emptyPattern():
    p=""
    for i in range(0,32):
        p+="*"
    return p

#Extracts, from the pattern ,the pattern used for matching againsts a source IP
def getPatternIPPart(s):
    ip= s[int(len(s)/2):]
    return (Range.Int2IP(Range.fromBinary(pattern2Ip(ip))),Range.Int2IP(Range.fromBinary(pattern2Mask(ip))))

#Converts a pattern to a real ip address by replacing the stars for zeros    
def pattern2Ip(p):
    return p.replace("*", "0")

#Converts a pattern to a mask string
def pattern2Mask(p):
    p= p.replace("0","1")
    p= p.replace("*","0")
    return p
