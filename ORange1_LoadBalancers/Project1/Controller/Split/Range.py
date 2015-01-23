import math,socket,struct

#A class which represents a range of ip addresses

class Range:
    idGen=0
    def __init__(self,start,end):
        self.ID="%d" % Range.idGen
        self.start=toBinary(start)
        self.end=toBinary(end)
        self.oneELCP=getOneELCP(self.start, self.end)
        self.zeroELCP=getZeroELCP(self.start, self.end)
        Range.idGen+=1
    
    #String representation of this range
    def __str__(self):
        return "ID: {0}\nStart: {1}, {2}\nEnd: {3}, {4}\nOneELCP: {5}\nZeroELCP: {6}".format(self.ID,Int2IP(fromBinary(self.start)),fromBinary(self.start),
                Int2IP(fromBinary(self.end)),fromBinary(self.end),self.oneELCP, self.zeroELCP)

    #Given an ip string address, returns true iff ip is in this range
    def isIPAdressInRange(self,ip):
        ip=IP2Int(ip)
        return ip>=fromBinary(self.start) and ip<=fromBinary(self.end)
 
    #Returns a tupple of (ip,mask) representing this range one Elcp
    def getOneELCP(self):
        return getELCPTupple(self.oneELCP)

    #Returns a tupple of (ip,mask) representing this range zero Elcp
    def getZeroELCP(self):
        return getELCPTupple(self.zeroELCP)

#Given a string, returns # of stars it contains
def starsInString(s):
    count=0
    for i in range(0,len(s)):
        if (s[i]=='*'):
            break
        else:
            count+=1
    return len(s)-count

#Given an IP string, converts and returns the integer value of this address
def IP2Int(ip):
    o = map(int, ip.split('.'))
    res = (16777216 * o[0]) + (65536 * o[1]) + (256 * o[2]) + o[3]
    return res

#Given an integer representation of an IP address, converts and returns the IP address as a string
def Int2IP(ipnum):
    o1 = int(ipnum / 16777216) % 256
    o2 = int(ipnum / 65536) % 256
    o3 = int(ipnum / 256) % 256
    o4 = int(ipnum) % 256
    return '%(o1)s.%(o2)s.%(o3)s.%(o4)s' % locals()


#Given an integer, returns the binary string representation of it     
def toBinary(x):
    res = ""
    while (x != 0):
        res = ("%d" % (x % 2)) + res
        x = math.floor(x/ 2)
    for i in range(0,32-len(res)):
        res="0"+res
    return res

#Given a binary string , returns the integer value of it  
def fromBinary(binStr):
    return int(binStr,2)

#Returns a string representing the Elcp using start and end of range
def getELCP(x,y,ruleType):
    res = ""
    for i in range(0,len(x)):
        if (x[i]==y[i]):
            res+=x[i]
        else:
            res+=ruleType
            for j in range(0,len(x)-(i+1)):
                res+="*"
            break
    return res

def getZeroELCP(x,y):
    return getELCP(x,y,"0")

def getOneELCP(x,y):
    return getELCP(x,y,"1")

#returns a tuple of (ip,mask) for a elcp
def getELCPTupple(elcp):
    return (Int2IP(fromBinary(elcp.replace("*","0"))),cidrToMask(32-starsInString(elcp)))

#Converts the convential cidr number to an IP mask string
def cidrToMask(prefix):
    return socket.inet_ntoa(struct.pack(">I", (0xffffffff << (32 - prefix)) & 0xffffffff))
