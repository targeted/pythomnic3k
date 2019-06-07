# configuration file for resource "postgresql_1"
#
# this file exists as a reference for configuring postgresql resources
# and to support self-test run of module protocol_postgresql_pg8000.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "postgresql_pg8000",             # meta
decimal_precision = (10, 2),                # sql
server_address = ("db.domain.com", 5432),   # postgresql
connect_timeout = 3.0,                      # postgresql
database = "database",                      # postgresql
username = "user",                          # postgresql
password = "pass",                          # postgresql
server_encoding = None,                     # postgresql, optional str
)

# self-tests of protocol_postgresql_pg8000.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
server_address = ("test.postgres", 5432),
database = "test_db",
username = "user",
password = "pass",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
