# configuration file for interface "email_1"
# this file exists as a reference for configuring e-mail interfaces
#
# copy this file to your own cage, possibly renaming into
# config_interface_YOUR_INTERFACE_NAME.py, then modify the copy

config = dict \
(
protocol = "email",                         # meta
server_address = ("mail.domain.com", 110),  # tcp
connect_timeout = 3.0,                      # tcp
ssl_key_cert_file = None,                   # ssl, optional filename
ssl_ca_cert_file = None,                    # ssl, optional filename
ssl_ciphers = None,                         # ssl, optional str
username = "user",                          # email
password = "pass",                          # email
interval = 60.0,                            # email, polling interval
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF