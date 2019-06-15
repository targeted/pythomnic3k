# configuration file for interface "xmlrpc_1"
# this file exists as a reference for configuring XMLRPC interfaces
#
# copy this file to your own cage, possibly renaming into
# config_interface_YOUR_INTERFACE_NAME.py, then modify the copy

config = dict \
(
protocol = "xmlrpc",                      # meta
listener_address = ("0.0.0.0", 80),       # tcp
max_connections = 100,                    # tcp
ssl_key_cert_file = None,                 # ssl, optional filename
ssl_ca_cert_file = None,                  # ssl, optional filename
ssl_ciphers = None,                       # ssl, optional str
ssl_protocol = None,                      # ssl, optional "SSLv23", "TLSv1", "TLSv1_1", "TLSv1_2" or "TLS"
response_encoding = "windows-1251",       # http
original_ip_header_fields = (),           # http
keep_alive_support = True,                # http
keep_alive_idle_timeout = 120.0,          # http
keep_alive_max_requests = 10,             # http
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF
