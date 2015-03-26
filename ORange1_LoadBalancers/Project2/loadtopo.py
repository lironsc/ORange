#!/usr/bin/python

# usage: mn --custom <path to loadtopo.py> --topo load ...

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.cli import CLI
import os

class LoadTopo(Topo):
        def __init__(self, switches = 2, res_hosts = 4, out_hosts = 3, **opts):
		Topo.__init__(self, **opts)
		self.switchNum = switches
		sws = [self.addSwitch('s%d' %(i+1, )) for i in xrange(switches)]
		rhosts = [self.addHost('h%d' % (i + 1)) for i in xrange(res_hosts)]
		ohosts = [self.addHost('h%d' % (i + 1+res_hosts)) for i in xrange(out_hosts)]
		for i in xrange(res_hosts):
			self.addLink(sws[0], rhosts[i])

		for sw in xrange(1, switches):
			for i in xrange(out_hosts):
				self.addLink(sws[sw], ohosts[i])

		for sw in xrange(1, switches):
			self.addLink(sws[sw], sws[0])

	def setToOF13(self):
	# 	#change to default protocol
		for sw in xrange(self.switchNum):
			print "sudo ovs-vsctl set bridge s%d protocols=OpenFlow13" %(sw+1, )
			os.system("sudo ovs-vsctl set bridge s%d protocols=OpenFlow13" %(sw+1, ))


	   

topos = { 'load': LoadTopo }
