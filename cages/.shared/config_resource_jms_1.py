# configuration file for resource "jms_1"
#
# this file exists as a reference for configuring JMS resources
# and to support self-test run of module protocol_jms.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "jms",                                          # meta
java = "C:\\PROGRA~1\\JAVA\\JDK16~1.0_1\\BIN\\java.exe",   # jms
arguments = ("-Dfile.encoding=windows-1251", ),            # jms, extra arguments to java process, useful for tuning
classpath = "c:\\pythomnic3k\\lib;"
            "c:\\pythomnic3k\\lib\\jms.jar;"
            "c:\\pythomnic3k\\lib\\imq.jar;"
            "c:\\pythomnic3k\\lib\\fscontext.jar",         # jms, note different separators: ; for Windows, : for Unix
jndi = { "java.naming.factory.initial":
            "com.sun.jndi.fscontext.RefFSContextFactory",
         "java.naming.provider.url":
            "file:///c:/pythomnic3k/lib/jndi" },           # jms, this configures the jndi
factory = "connection_factory",                            # jms, this name is looked up in jndi
queue = "work.queue",                                      # jms, this name is looked up in jndi
username = "user",                                         # jms, this can be empty string
password = "pass",                                         # jms, this can be empty string
)

# self-tests of protocol_jms.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
queue = "test.queue",
username = "",
password = "",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
