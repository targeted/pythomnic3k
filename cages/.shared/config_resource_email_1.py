# configuration file for resource "email_1"
#
# this file exists as a reference for configuring email resources
# and to support self-test run of module protocol_email.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "email",                           # meta
server_address = ("mail.domain.com", 25),     # tcp
connect_timeout = 3.0,                        # tcp
ssl_key_cert_file = None,                     # ssl, optional filename
ssl_ca_cert_file = None,                      # ssl, optional filename
ssl_ciphers = None,                           # ssl, optional str
ssl_protocol = None,                          # ssl, optional "SSLv23", "TLSv1", "TLSv1_1", "TLSv1_2" or "TLS"
ssl_server_hostname = None,                   # ssl, optional str
ssl_ignore_hostname = False,                  # ssl, ignore certificate common/alt name name mismatch
encoding = "windows-1251",                    # email
helo = "hostname",                            # email
auth_method = None,                           # email, optional "PLAIN" or "LOGIN"
username = None,                              # email, optional string
password = None,                              # email, optional string
)

# self-tests of protocol_email.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
server_address = ("mail.domain.com", 25),
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
