# configuration file for resource "smpp_1"
#
# very little settings here, SMPP resource "foo" requires SMPP
# interface "foo" to be enabled and indirectly uses its settings

config = dict \
(
protocol = "smpp",       # meta
source_addr_ton = 0x00,  # smpp
source_addr_npi = 0x00,  # smpp
source_addr = "",        # smpp
asynchronous = False,    # smpp, do not wait for submit responses
pack_7bit = False,       # smpp, pack 7-bit characters to septets
frag_method = None,      # smpp, "udh", "sar" or None
)

# self-tests of protocol_smpp.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
source_addr_ton = 0x05,
source_addr_npi = 0x01,
source_addr = "demo",
asynchronous = False,
pack_7bit = False,
frag_method = "udh",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
