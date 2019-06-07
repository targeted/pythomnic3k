# configuration file for resource "mysql_1"
#
# this file exists as a reference for configuring mysql resources
# and to support self-test run of module protocol_mysql_pymysql.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "mysql_pymysql",                 # meta
decimal_precision = (10, 2),                # sql
server_address = ("db.domain.com", 3306),   # mysql
connect_timeout = 3.0,                      # mysql
database = "database",                      # mysql
username = "user",                          # mysql
password = "pass",                          # mysql
sql_mode = None,                            # mysql, optional str
charset = None,                             # mysql, optional str
)

# self-tests of protocol_mysql_pymysql.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
server_address = ("localhost", 3306),
decimal_precision = (10, 4),
database = "test",
username = "root",
password = "root",
charset = "cp1251",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
