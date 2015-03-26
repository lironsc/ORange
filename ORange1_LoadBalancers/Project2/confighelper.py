import random

def create_even_weights(num_hosts):
    """
    Creates host list with even weights
    """
    even = [1.0/num_hosts for i in xrange(num_hosts)]
    return even

def create_even_weight_servers(num_hosts):
    """
    Creates host list with even weights - under 255 hosts
    --mac option is assumed to have been used
    """
    even = [("00:00:00:00:00:%0.2x" % (i+1, ), "10.0.0.%d" % (i+1, ), 1.0/num_hosts) for i in xrange(num_hosts)]
    return even

def create_random_weights(num_hosts):
    """
    Creates host list with random weights
    """
    sum_weights = 1.0
    #random returns [0.0, 1.0) so there won't be zero weights 
    weights = []
    for i in xrange(num_hosts-1):
        r = random.random()
        if r < sum_weights:
            weights += [r]
            sum_weights -= r
        else:
            weights += [r * sum_weights]
            sum_weights -= r * sum_weights
    weights += [sum_weights]
    return weights


def create_random_weight_servers(num_hosts):
    """
    Creates host list with random weights - under 255 hosts
    --mac option is assumed to have been used
    """
    rand_w = create_random_weights(num_hosts)
    servers = [("00:00:00:00:00:%0.2x" % (i+1, ), "10.0.0.%d" % (i+1, ), rand_w[i] for i in xrange(num_hosts)]

    return servers


