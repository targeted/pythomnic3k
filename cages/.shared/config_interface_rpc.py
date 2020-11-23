# Configuration file for interface "rpc". This interface is
# used in conjunction with RPC resource for cage-to-cage RPC calls.
#
# If location discovery at runtime is used (which is recommended),
# then all the cages that wish to share the same RPC "namespace" need
# identical broadcast ports, broadcast addresses that face the same
# subnet and the same flock_id, which is an arbitrary identifier around
# which all the related cages are grouped, same port broadcasts with
# different flock id will be ignored.
#
# The RPC listener is bound to a random port in specified range,
# which is later advertised at runtime to other cages. In case
# such broadcast advertisement are forbidden an exact port number
# can be specified, as a positive number (vs. negative for range).
# In this case other cages will likely have an entry in
# config_resource_rpc.py exact_locations parameter specifying this
# cage's address.
#
# There is no need to make a copy of this file for each cage,
# but you may need to modify the broadcast_address parameter
# if your OS doesn't work with 255.255.255.255 broadcasts,
# for example, under FreeBSD change it to something like
# "192.168.0.1/192.168.255.255".

config = dict \
(
protocol = "rpc",                                       # meta
random_port = -63000,                                   # tcp, negative means "in range 63000..63999"
max_connections = 100,                                  # tcp
broadcast_address = ("0.0.0.0/255.255.255.255", 12480), # rpc, "interface address/broadcast address", port
ssl_ciphers = None,                                     # ssl, optional str
ssl_protocol = None,                                    # ssl, optional "SSLv23", "TLSv1", "TLSv1_1", "TLSv1_2" or "TLS"
flock_id = "DEFAULT",                                   # rpc
marshaling_methods = ("msgpack", "pickle"),             # rpc, allowed marshaling methods
max_packet_size = 1048576,                              # rpc, maximum allowed request/response size in bytes
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF
