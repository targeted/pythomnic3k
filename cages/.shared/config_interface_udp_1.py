# configuration file for interface "udp"

config = dict \
(
protocol = "udp",                          # meta
listener_address = ("1.2.3.4", 5678),      # udp, listener address
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF