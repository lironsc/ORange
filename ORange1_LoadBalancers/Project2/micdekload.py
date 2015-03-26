# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
import thread
import time
from random import random

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.ofp_event import EventOFPFlowStatsReply
from ryu.lib.packet import ethernet, packet
from ryu.ofproto import ofproto_v1_3, ether

import confighelper
import loadbalancerconfig


class MicDekLoad(app_manager.RyuApp):
    """
    Rule-based Load Balancer for OpenFlow.
    Based on multiple articles by Liron Schiff et al, Tel-Aviv University.
    Written by Michal Shagam and Dekel Auster.
    """
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    def __init__(self, *args, **kwargs):
        super(MicDekLoad, self).__init__(*args, **kwargs)
        self.mac_to_port = {} # Used by the learning switch, for packets when not handled by the LB.
        self.numberOfRules = 0
        self.changedRules = 0
        self.virtual_mac = loadbalancerconfig.virtual_server[0] # Virtual MAC Address of the LB.
        self.virtual_ip = loadbalancerconfig.virtual_server[1] # Virtual IP of the LB.
        self.servers = loadbalancerconfig.servers # List of tuples: (MAC, IP, weight) of the servers.
        self.number_of_servers = len(loadbalancerconfig.servers) # Number of servers.
        self.num_packets = [0] * self.number_of_servers # Number of packets processed by each server. 
        self.ranges = [] # Range separation of the servers.
        

    def create_first_ranges(self):
        """
        Returns a preliminary range separation of the servers.
        """
        ranges = []
        start = 0
        end = 0
        random_weights = confighelper.create_random_weights(self.number_of_servers) 

        # Create (start, end) tuple for each server.
        for i in xrange(self.number_of_servers):
            weight = self.servers[i][2]
            end = start + int(math.floor((2**32 - 1) * weight))
            ranges += [(start, end)]
            start = end + 1
        # Fix rounding errors, by making sure that the last server's last IP is 255.255.255.255.
        ranges[self.number_of_servers - 1] = (ranges[self.number_of_servers - 1][0], 2**32-1)
        return ranges


    def create_comparator_metas(self):
        """
        Creates metadata information and masks for the comparator tables (m<=end and m>start).
        """
        # This holds tuples of: metadata to match, a string representation of IP to match,
        # and the mask for this match ("000...1...000"). The last parameter is a boolean, whether the
        # metadata in this index should be bigger or smaller than the IP.
        metas = []
       
        for i in xrange(32):
            #              METADATA                       IP                          MASK                        M>=P
            metas += [(int("0"*i + "1" + "0"*(32-i-1),2), "0"*i + "0" + "0"*(32-i-1), "0"*i + "1" + "0"*(32-i-1), True)]
            metas += [(int("0"*i + "0" + "0"*(32-i-1),2), "0"*i + "1" + "0"*(32-i-1), "0"*i + "1" + "0"*(32-i-1), False)]
        # Wildcard rule.
        metas += [(int("0"*32,2), "0"*32, "0"*32, True)]
        return metas

    def create_rule_set(self, datapath):
        """
        Creates all rules for switches that should support the load balancing feature.

        The tables are as follows:
        0 - basic operations on requests to/from the LB.
        1, 3 - add metadata to the packet, and forward to comparator tables.
        2, 4 - comparators. receive the metadata from tables 1 and 3 (respectively), and if the 
               compare is successful, forwards to the given range.
        5 - changes destination IP and MAC to the requested server, according to the given range ID.
        """
        self.build_table_0(datapath)
        self.build_table_1_3(datapath)
        self.build_table_2_4(datapath)
        self.build_table_5(datapath)
        print "The number of rules is: %d" %(self.numberOfRules, )


    def build_table_0(self, datapath):
        """
        Builds table 0, as defined in create_rule_set doc.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Create first rule - if the dest is the LB, move forward.
        self.add_flow_to_table(datapath, 1, parser.OFPMatch(eth_type=ether.ETH_TYPE_IP, ipv4_dst=self.virtual_ip), [], [parser.OFPInstructionGotoTable(1)], 0)
        self.numberOfRules +=1
        # Create rule to change src for each server to the LB IP and MAC.
        for i in xrange(self.number_of_servers):
            actions = [parser.OFPActionSetField(eth_src=self.virtual_mac),
                       parser.OFPActionSetField(ipv4_src=self.virtual_ip),
                       parser.OFPActionOutput(ofproto.OFPP_NORMAL, )]
            self.add_flow_to_table(datapath, 2, parser.OFPMatch(eth_type = ether.ETH_TYPE_IP, ipv4_src=self.servers[i][1]), actions, [], 0)
            self.numberOfRules +=1

        
    def build_table_1_3(self, datapath):
        """
        Builds tables 1 and 3, as defined in create_rule_set doc.

        This creates first-level separation of each server, and adds rules so that
        each candidate is forwarded to comparators 2 and 4 (respectively). By adding
        metadata, we allow the comparators to decide if the packet should be forwarded to
        the destination that we determine here by the range.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        self.ranges = self.create_first_ranges()

        # Add one rule (of the LCP+1) for each server, for tables 1 and 3.
        for i in xrange(self.number_of_servers):
            self.add_rules_to_1_3(datapath, self.ranges[i][0], self.ranges[i][1], i)
            self.numberOfRules +=2

        # If no match was found in table 1, move to table 3.
        self.add_flow_to_table(datapath, 0, parser.OFPMatch(), [], [parser.OFPInstructionGotoTable(3)], 1)
        self.numberOfRules +=1


    def build_table_2_4(self, datapath):
        """
        Builds tables 2 and 4, as defined in create_rule_set doc.
        Table 2 compares m >= p. Table 4 compares m < p. If successful, forward to table 5.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        metas = self.create_comparator_metas()

        comparator_priority = 65
        for meta in metas:
            if meta[3]:
                # m >= p - in table 2, found a match. in table 4, discard
                instructions2 = [parser.OFPInstructionGotoTable(5)]
                instructions4 = [] # This should never happen! We missed a rule.
                if meta == metas[-1]: # If in the wildcard meta, send also from table 4.
                    instructions4 = instructions2
            else:
                # m < p - in table 2, no match, move to table 3. in table 4, found a match.
                instructions2 = [parser.OFPInstructionGotoTable(3)]
                instructions4 = [parser.OFPInstructionGotoTable(5)]
                
            # Create a string representation of the source IP.
            vals = [ str(int(meta[1][i*8:i*8+8], 2)) for i in xrange(4)]
            src_ipv4 = ".".join(vals)
            # Create the masks for the metadata (as a number) and the IP (as a string).
            vals = [ str(int(meta[2][i*8:i*8+8], 2)) for i in xrange(4)]
            meta_mask = int(meta[2], 2)
            src_mask = ".".join(vals)
        
            # Add rule to table 2.
            self.add_flow_to_table(datapath, comparator_priority, parser.OFPMatch(metadata=(meta[0], meta_mask), eth_type=ether.ETH_TYPE_IP, ipv4_src=(src_ipv4, src_mask)), [], instructions2, 2)
            # Add rule to table 4.
            self.add_flow_to_table(datapath, comparator_priority, parser.OFPMatch(metadata=(meta[0], meta_mask), eth_type=ether.ETH_TYPE_IP, ipv4_src=(src_ipv4, src_mask)), [], instructions4, 4)
            comparator_priority-=1
            self.numberOfRules +=2


    def build_table_5(self, datapath):
        """
        Builds table 5, as defined in create_rule_set doc.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        rid_meta_mask = (2**32-1)*(2**32) # only the RID part of the metadata - the most significant 32 bits.

        for i in xrange(self.number_of_servers):
            actions = [parser.OFPActionSetField(eth_dst=self.servers[i][0]),
                       parser.OFPActionSetField(ipv4_dst=self.servers[i][1]),
                       parser.OFPActionOutput(ofproto.OFPP_NORMAL)]
            self.add_flow_to_table(datapath, 0, parser.OFPMatch(eth_type=ether.ETH_TYPE_IP, metadata=(i*(2**32), rid_meta_mask)), actions, [], 5)
            self.numberOfRules +=1


    def convert_lcp_to_ipv4(self, elcp, pref_length):
        """
        Converts the given ELCP and prefix length into a representation that is acceptable as src_ipv4.
        """
        # Create string representation of the ELCP.
        vals = [str(int(elcp[i * 8:i * 8 + 8], 2)) for i in xrange(4)]
        ipv4 = ".".join(vals)
        # Create string representation of its prefix mask.
        mask = "1" * pref_length + "0" * (32 - pref_length)
        maskvals = [str(int(mask[i * 8:i * 8 + 8], 2)) for i in xrange(4)]
        mask_str = ".".join(maskvals)
        return (ipv4, mask_str)


    def add_rules_to_1_3(self, datapath, lower, upper, range_id):
        """
        Add rules to tables 1 and 3 for the given range ID and its limits.
        """
        (lcp, elcp0, elcp1, priority) = self.find_lcp(lower, upper)

        # Table 1 uses the upper bound and ELCP1.
        self.add_rules_to_1_3_for_table(datapath, elcp1, upper, priority, range_id, 1) 
        # Table 3 uses the lower bound and ELCP0.
        self.add_rules_to_1_3_for_table(datapath, elcp0, lower, priority, range_id, 3)


    def add_rules_to_1_3_for_table(self, datapath, elcp, range_limit, priority, range_id, table_id):
        """
        Helper method for add_rules_to_1_3, for each table. 
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        (ip, wildcard_mask) = self.convert_lcp_to_ipv4(elcp, priority + 1)
        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_IP, ipv4_src=(ip, wildcard_mask))
        meta = range_id * (2**32) + range_limit
        meta_mask = 2**64 - 1 # Write the entire metadata. The 32 MSBs are for the RID, the others for the comparators.
        write_meta = parser.OFPInstructionWriteMetadata(meta, meta_mask)
        self.add_flow_to_table(datapath, priority, match, [], [write_meta, parser.OFPInstructionGotoTable(table_id + 1)], table_id)


    def rewrite_rules_in_1_3(self, datapath, old_lower, old_upper, new_lower, new_upper, range_id):
        """
        Rewrite rules in tables 1 and 3, when the lower and upper values of a range have changed.
        """
        (old_lcp, old_elcp0, old_elcp1, old_priority) = self.find_lcp(old_lower, old_upper)
        (new_lcp, new_elcp0, new_elcp1, new_priority) = self.find_lcp(new_lower, new_upper)  

        # Rewrite rules in table 1, using ELCP1 and upper bound.
        self.rewrite_rules_in_1_3_for_table(datapath, old_elcp1, new_elcp1, old_upper, new_upper, old_priority, new_priority, range_id, 1)
        # Rewrite rules in table 3, using ELCP0 and lower bound.
        self.rewrite_rules_in_1_3_for_table(datapath, old_elcp0, new_elcp0, old_lower, new_lower, old_priority, new_priority, range_id, 3)
        self.changedRules+=2

    def rewrite_rules_in_1_3_for_table(self, datapath, old_elcp, new_elcp, old_range, new_range, old_priority, new_priority, range_id, table_id):
        """
        Helper method for rewrite_rules_in_1_3, for each table.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        meta_mask = 2**64 - 1 # Write the entire metadata. The 32 MSBs are for the RID, the others for the comparators.

        (old_ip, old_wildcard_mask) = self.convert_lcp_to_ipv4(old_elcp, old_priority + 1)
        (new_ip, new_wildcard_mask) = self.convert_lcp_to_ipv4(new_elcp, new_priority + 1)
        old_match = parser.OFPMatch(eth_type=ether.ETH_TYPE_IP, ipv4_src=(old_ip, old_wildcard_mask))
        new_match = parser.OFPMatch(eth_type=ether.ETH_TYPE_IP, ipv4_src=(new_ip, new_wildcard_mask))
        old_meta = range_id * (2**32) + old_range
        new_meta = range_id * (2**32) + new_range
        old_write_meta = parser.OFPInstructionWriteMetadata(old_meta, meta_mask)
        new_write_meta = parser.OFPInstructionWriteMetadata(new_meta, meta_mask)
        # If any part of the rule is different, replace the old rule by a new one.
        if old_ip != new_ip or old_wildcard_mask != new_wildcard_mask or old_meta != new_meta or old_priority != new_priority:
            # We first delete the old rule, since otherwise, we cannot tell which rule will be
            # deleted since not all field are matched.
            self.delete_flow_from_table(datapath, old_priority, old_match, [], [old_write_meta, parser.OFPInstructionGotoTable(table_id + 1)], table_id)
            self.add_flow_to_table(datapath, new_priority, new_match, [], [new_write_meta, parser.OFPInstructionGotoTable(table_id + 1)], table_id)


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """
        Event that is dispatched in the beginning of the run of the controller.
        For all switches except 1 (the inner switch of the servers), add all rules.
        Also start monitoring statistics, for re-partitioning of the ranges.
        """
        datapath = ev.msg.datapath
        # Do the LB process except in the inner switch of the LB (it should know its real servers!).
        if ev.msg.datapath_id != 1:
           self.create_rule_set(datapath)
           thread.start_new_thread(self.send_flow_stats_request, (datapath,))

    def add_flow_to_table(self, datapath, priority, match, actions, instructions, table):
        """
        Adds a flow with the given parameters to the table.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = instructions

        # Add an apply_actions instruction for the actions.
        if actions != []:
            inst += [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                                 actions)]

        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst, table_id=table)
        datapath.send_msg(mod)


    def delete_flow_from_table(self, datapath, priority, match, actions, instructions, table):
        """
        Deletes a flow with the given parameter from the table, strictly.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = instructions

        # Add an apply_actions instruction for the actions.
        if actions != []:
            inst += [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                                 actions)]

        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY, command=ofproto.OFPFC_DELETE_STRICT, match=match, instructions=inst, table_id=table)

        datapath.send_msg(mod)


    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """
        This function (taken from simple_switch) handles all packets, except the ones directed
        to the load balancer. Also, after the load balances does its job, the packets arrive here
        to get to the actual servers.
        """
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        dst = eth.dst
        src = eth.src
                
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [datapath.ofproto_parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            self.add_flow_to_table(datapath, 1, match, actions, [], 0)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)


    def find_lcp(self, lower, upper):
        """
        Finds the LCP (longest common prefix) of the given IP ranges, 
        in binary format.
        """
        lower_bin = bin(lower)[2:]
        upper_bin = bin(upper)[2:]
        prefix_length = 0
        lcp = ''
        elcp0 = ''
        elcp1 = ''

        # Pad to 32 characters.
        if (len(lower_bin) < 32):
            lower_bin = '0' * (32 - len(lower_bin)) + lower_bin
        if (len(upper_bin) < 32):
            upper_bin = '0' * (32 - len(upper_bin)) + upper_bin

        # If they are the same, no requests can be retrieved to the server.
        if (lower_bin.startswith(upper_bin)): # they have the same length
            return ('0'*32, '0'*32, '0'*32, 0)

        # Calculate LCP.
        for i in xrange(32):
            if (upper_bin[i] == lower_bin[i]):
                lcp = lcp + upper_bin[i]
                prefix_length += 1
            else:
                # Found a different bit - return.
                elcp0 = lcp + '0'
                elcp1 = lcp + '1'
                num_wild = 32 - len(elcp0)
                elcp0 += '0' * num_wild
                elcp1 += '0' * num_wild
                
                return (lcp, elcp0, elcp1, prefix_length)


    def send_flow_stats_request(self, datapath):
       """
       Send stat requests from a different thread, and handle its result - infinitely.
       """
       ofp = datapath.ofproto
       ofp_parser = datapath.ofproto_parser
       last_packets = [0] * self.number_of_servers # Keep the last packet results, because we compare deltas.
       self.skip_partition = True # Whether to skip re-partitioning in the next run.
       
       while True:
           # Send request to get connection stats.
           req = ofp_parser.OFPFlowStatsRequest(datapath)
           datapath.send_msg(req)
           self.received_reply = False

           # Wait until the results are all done.
           while self.received_reply == False:
               time.sleep(1)

           # Read results, fix the partitioning accordingly.
           # If the difference between the busiest and most relieved server are big, re-partition. 
           min_delta = 0
           min_delta_id = 0
           max_delta = 0
           max_delta_id = 0
           # Find the busiest and most relieved servers.
           for i in xrange(self.number_of_servers):
               # Calculate a weighted delta.
               delta = (self.num_packets[i] - last_packets[i]) * self.servers[i][2]
               if i == 0 or min_delta > delta:
                   min_delta = delta
                   min_delta_id = i
	       if i == 0 or max_delta < delta:
                   max_delta = delta
                   max_delta_id = i
               last_packets[i] = self.num_packets[i]

           # Re-partition if needed.
           if not self.skip_partition and min_delta * 2 < max_delta:
               self.repartition(datapath, min_delta_id, max_delta_id)
               # Skip partition, so that next time we will have the refreshed deltas of the
               # new rules.
               self.skip_partition = True
               
           else:
               self.skip_partition = False

           # Sleep until next iteration.
           time.sleep(3)

    @set_ev_cls(EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        """
        Handle stats reply. Set self.num_packets accordingly.
        """
        # Reset number of packets
        for i in range(self.number_of_servers):
            self.num_packets[i] = 0
        
        # Calculate number of packets that went through table 5, by the dest MAC address.
        for stat in ev.msg.body:
            # Only care about valid rules in table 5.
            if stat.table_id != 5 or len(stat.instructions) != 1 or len(stat.instructions[0].actions) != 3:
                continue
            # Convert MAC address to string (e.g. "0000000000").
            mac = str(stat.instructions[0].actions[0].field.value).encode('hex').lower()
            # Find the server with the matching MAC address.
            server = -1
            for i in xrange(self.number_of_servers):
                # Convert server's MAC to the same format.
                current_mac = self.servers[i][0].replace(":", "").lower()
                if current_mac == mac:
                  server = i

            # If found, add the packet count to this server's counting.
            if server >= 0 and server < self.number_of_servers:
                self.num_packets[server] = self.num_packets[server] + stat.packet_count

        # Allow other thread to continue.
        self.received_reply = True
	        
    def repartition(self, datapath, min_id, max_id):
         """
         Re-partition the ranges, in order to make the busiest server more relieved.
         The current logic is decreasing the range of the busiest server, without modifying the min. server.
         """
         percent_to_take = random() / 2 # How much % to take from the busiest server.
         total_max_size = self.ranges[max_id][1] - self.ranges[max_id][0] # Total size of the busiest server.
         # if we have two sides - how much to give to each side?
         if max_id != 0 and max_id != self.number_of_servers:
             up = random()
         # we only have 1 side
         elif max_id == 0:
             up = 1
         else:
             up = 0
         percent_up = percent_to_take * up
         percent_down = percent_to_take * (1 - up)
         amount_up = int(math.floor(total_max_size * percent_up)) # Amount to give to max_id+1
         amount_down = int(math.floor(total_max_size * percent_down)) # Amount to give to max_id-1
     
         for i in xrange(self.number_of_servers):
             old_range = self.ranges[i]
             # Increase top of max_id-1
             if i == max_id - 1:
                 self.ranges[i] = (old_range[0], old_range[1] + amount_down)
             # Increase bottom of max_id and decrease its top.
             elif i == max_id:
                 self.ranges[i] = (old_range[0] + amount_down, old_range[1] - amount_up)
             # Decrease bottom of max_id - 1.
             elif i == max_id + 1:
                 self.ranges[i] = (old_range[0] - amount_up, old_range[1])

             self.rewrite_rules_in_1_3(datapath, old_range[0], old_range[1], self.ranges[i][0], self.ranges[i][1], i)
         print "The number of updated rules is: %d" %(self.changedRules, )

