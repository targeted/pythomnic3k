# configuration file for resource "sqlserver_1"
#
# this file exists as a reference for configuring sqlserver resources
# and to support self-test run of module protocol_sqlserver_adodb.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "sqlserver_adodb",                        # meta
decimal_precision = (10, 2),                         # sql
connection_string = "Provider=SQLOLEDB.1;" \
                    "Integrated Security=SSPI;" \
                    "Persist Security Info=False;" \
                    "Initial Catalog=database;" \
                    "Data Source=.",                 # sqlserver
)

# self-tests of protocol_sqlserver.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
decimal_precision = (24, 6),
connection_string = "Provider=SQLOLEDB.1;" \
                    "Integrated Security=SSPI;" \
                    "Persist Security Info=False;" \
                    "Initial Catalog=test;" \
                    "Data Source=.\\SQLEXPRESS",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF