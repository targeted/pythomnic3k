# configuration file for interface "jms_1"
# this file exists as a reference for configuring JMS interfaces
#
# copy this file to your own cage, possibly renaming into
# config_interface_YOUR_INTERFACE_NAME.py, then modify the copy
#
# this particular configuration works with OpenMQ and file-based JNDI

config = dict \
(
protocol = "jms",                                          # meta
java = "C:\\JDK\\BIN\\java.exe",                           # jms
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

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF