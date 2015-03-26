
# Add a list of servers, each represented by (MAC, IP, weight).
# The weights should sum up to 1.0.
servers = [
    ("00:00:00:00:00:01", "10.0.0.1", 0.33),
    ("00:00:00:00:00:02", "10.0.0.2", 0.33),
    ("00:00:00:00:00:03", "10.0.0.3", 0.34)
    ]

#For testing, we can use the utility functions in confighelper

#import confighelper
#servers = confighelper.create_even_weight_servers(50)

# The virtual MAC and IP for the load balancer.
# Due to some limitations in this exercise, this must represent
# a real host in the same switch as the servers. This host will be
# disconnected from the outer world.
virtual_server = ("00:00:00:00:00:4", "10.0.0.4")

# Whether to override the server weights by random weights.
#override_weights_by_random = True
