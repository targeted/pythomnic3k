# configuration file for resource "xmlrpc_1"
#
# this file exists as a reference for configuring XMLRPC resources
# and to support self-test run of module protocol_xmlrpc.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "xmlrpc",                          # meta
server_address = ("xmlrpc.domain.com", 80),   # tcp
connect_timeout = 3.0,                        # tcp
ssl_key_cert_file = None,                     # ssl, optional filename
ssl_ca_cert_file = None,                      # ssl, optional filename
ssl_ciphers = None,                           # ssl, optional str
ssl_protocol = None,                          # ssl, optional "SSLv23", "TLSv1", "TLSv1_1" or "TLSv1_2"
extra_headers = {},                           # http
http_version = "HTTP/1.1",                    # http
server_uri = "/xmlrpc",                       # xmlrpc
request_encoding = "windows-1251",            # xmlrpc
allow_none = False,                           # xmlrpc, Python-specific, optional
)

# self-tests of protocol_xmlrpc.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
server_address = ("127.0.0.1", 23673),
extra_headers = { "Authorization": "Basic dXNlcjpwYXNz" },
allow_none = True,
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
