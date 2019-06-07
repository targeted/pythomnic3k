# configuration file for interface "smpp_1"
# this file exists as a reference for configuring SMPP interfaces
#
# copy this file to your own cage, possibly renaming into
# config_interface_YOUR_INTERFACE_NAME.py, then modify the copy

config = dict \
(
protocol = "smpp",                           # meta
server_address = ("smsc.domain.com", 1234),  # tcp
connect_timeout = 5.0,                       # tcp + smpp bind
response_timeout = 3.0,                      # smpp
ping_interval = 60.0,                        # smpp optional
system_id = "system_id",                     # smpp
password = "password",                       # smpp
system_type = "PYTHOMNIC3K",                 # smpp
esme_ton = 0x01,                             # smpp
esme_npi = 0x01,                             # smpp
esme_addr = "000000",                        # smpp
esme_type = "xcvr",                          # smpp, "rcvr", "xcvr" or "xmit"
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF