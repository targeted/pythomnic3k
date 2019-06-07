# configuration file for resource "udp"

config = dict \
(
protocol = "udp",                       # meta
server_address = ("1.2.3.4", 5678),     # udp, target server address
broadcast = False,                      # udp, whether SO_BROADCAST
)

# self-tests of protocol_udp.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
server_address = ("255.255.255.255", 5371),
broadcast = True,
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF