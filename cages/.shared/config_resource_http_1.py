# configuration file for resource "http_1"
#
# this file exists as a reference for configuring HTTP resources
# and to support self-test run of module protocol_http.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "http",                       # meta
server_address = ("www.domain.com", 80), # tcp
connect_timeout = 3.0,                   # tcp
ssl_key_cert_file = None,                # ssl, optional filename
ssl_ca_cert_file = None,                 # ssl, optional filename
ssl_ciphers = None,                      # ssl, optional str
ssl_protocol = None,                     # ssl, optional "SSLv23", "TLSv1", "TLSv1_1" or "TLSv1_2"
extra_headers = {},                      # http
http_version = "HTTP/1.1",               # http
)

# self-tests of protocol_http.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
server_address = ("127.0.0.1", 23673),
extra_headers = { "X-Foo": "Abc", "X-Bar": "Def" },
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
