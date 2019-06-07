# configuration file for resource "mongodb_1"
#
# this file exists as a reference for configuring mongodb resources
# and to support self-test run of module protocol_mongodb.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "mongodb",                       # meta
server_address = ("db.domain.com", 27017),  # mongodb
connect_timeout = 3.0,                      # mongodb
database = "db",                            # mongodb
username = None,                            # mongodb, optional str
password = None,                            # mongodb, optional str
)

# self-tests of protocol_mongodb.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
server_address = ("127.0.0.1", 27017),
database = "test",
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
