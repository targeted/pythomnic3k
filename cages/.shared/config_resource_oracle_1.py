# configuration file for resource "oracle_1"
#
# this file exists as a reference for configuring oracle resources
# and to support self-test run of module protocol_oracle_cx.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "oracle_cx",                     # meta
decimal_precision = (10, 2),                # sql
server_address = ("db.domain.com", 1521),   # oracle
database = "database",                      # oracle
username = "user",                          # oracle
password = "pass",                          # oracle
)

# self-tests of protocol_oracle_cx.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
decimal_precision = (7, 4),                 # sql
server_address = ("test.oracle", 1521),     # oracle
database = "db",                            # oracle
username = "user",                          # oracle
password = "pass",                          # oracle
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
