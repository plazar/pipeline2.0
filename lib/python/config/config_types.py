import sys
import types
import os.path

INVALIDCONFIG = NotImplemented

class ConfigList(object):
    """A logical collection of Configurable objects that should
        all be validated together.
    """
    def __init__(self, name):
        """Constructor for ConfigList object.
        
            Inputs:
                None
        """
        self.configs = {}
        self.sanity_checked = False
        self.name = name

    def add_config(self, key, cfg):
        """Add a Configurable object, 'cfg', to the list of configurations.
            
            NOTE: 'cfg' must be an instance of a subclass of 'Configurable'.

            Input:
                key: The name by which 'cfg' will be keyed on.
                cfg: An Configurable object.
        """
        if not isinstance(cfg, Configurable):
            raise TypeError("Configuration cannot be added it is not " \
                            "a Configurable instance.")
        else:
            self.configs[key] = cfg
            self.sanity_checked = False

    def populate_configs(self, settings):
        """Given a dictionary of configurations, 'settings'.
            Set the configs.
        """
        for key, value in settings.iteritems():
            if key in self.configs:
                self.configs[key].set_value(value)

    def check_sanity(self):
        """Validate each configuration. 
            If any configurations are invalid print a report 
            and raise an exception.
        """
        invalids_msg = []
        for key, cfg in self.configs.iteritems():
            # print 'validating %s = %s' % (key, str(cfg))
            try:
                cfg.validate()
            except ConfigValidationError:
                invalids_msg.append("%s.%s = %s is invalid!\n\t%s\n" % \
                                        (self.name, key, str(cfg), cfg.msg))
        if invalids_msg:
            sys.stderr.write("\n========= INVALID CONFIGURATIONS =========\n")
            sys.stderr.write("\n".join(invalids_msg))
            sys.stderr.write("==========================================\n\n")
            raise InsaneConfigsError("Some configurations (%d) failed sanity check." % \
                                        len(invalids_msg))
        else:
            self.sanity_checked = True

    # def __getattr__(self, key):
    #     if not self.sanity_checked:
    #         self.check_sanity()
    #     return self.configs[key].value

    # def __setattr__(self, key, value):
    #     if 'configs' in self.__dict__ and key in self.configs:
    #         self.configs[key].set_value(value)
    #         self.sanity_checked = False
    #     else:
    #         self.__dict__[key] = value

    def __repr__(self):
        strs = []
        for key, cfg in self.configs.iteritems():
            strs.append("%s = %s" % (key, str(cfg)))
        return "\n".join(strs)


class Configurable(object):
    """A configurable object that can be validated.
    """
    # A helpful message to print when a configuration is invalid.
    # This message should help the user fix the issue found.
    msg = "Configuration is invalid"

    def __init__(self, value=INVALIDCONFIG):
        """Constructor for configurable object.

            Inputs:
                value: the configuration's value. (Default: INVALIDCONFIG)
        """
        self.set_value(value)

    def validate(self):
        """Validate the configuration's value. If the configuration
            is invalid a ConfigValidationError is raised.
        """
        if not self.isvalid():
            raise ConfigValidationError

    def isvalid(self):
        if self.value is INVALIDCONFIG:
            return False
        else:
            return True

    def set_value(self, value):
        self.value = value

    def __str__(self):
        return "%s" % repr(self.value)


class BoolConfig(Configurable):
    msg = "Must be a boolean value."
    def isvalid(self):
        return super(BoolConfig, self).isvalid() and \
                    (type(self.value) == types.BooleanType)


class IntConfig(Configurable):
    msg = "Must be an integer value."
    def isvalid(self):
        return super(IntConfig, self).isvalid() and \
                    (type(self.value) == types.IntType)


class IntOrLongConfig(Configurable):
    msg = "Must be an integer or long value."
    def isvalid(self):
        return super(IntOrLongConfig, self).isvalid() and \
                    (type(self.value) == types.IntType or \
                        type(self.value) == types.LongType)


class PosIntConfig(IntConfig):
    msg = "Must be a positive integer value (i.e. > 0)."
    def isvalid(self):
        return super(PosIntConfig, self).isvalid() and \
                    (self.value > 0)


class FloatConfig(Configurable):
    msg = "Must be a float value."
    def isvalid(self):
        return super(FloatConfig, self).isvalid() and \
                    (type(self.value) == types.FloatType)


class StrConfig(Configurable):
    msg = "Must be a string value."
    def isvalid(self):
        return super(StrConfig, self).isvalid() and \
                    (type(self.value) == types.StringType)


class FuncConfig(Configurable):
    msg = "Must be a function."
    def isvalid(self):
        return super(FuncConfig, self).isvalid() and \
                    (type(self.value) == types.FunctionType)


class StrOrNoneConfig(Configurable):
    msg = "Must be a string value."
    def isvalid(self):
        return super(StrOrNoneConfig, self).isvalid() and \
                    (type(self.value) in (types.StringType, types.NoneType))


class DirConfig(Configurable):
    msg = "Must be an existing directory."
    def isvalid(self):
        return super(DirConfig, self).isvalid() and \
                    os.path.isdir(self.value)


class ReadWriteConfig(Configurable):
    msg = "Must be able to read-write path."
    def isvalid(self):
        return super(ReadWriteConfig, self).isvalid() and \
                    (os.path.exists(self.value) and \
                    os.access(self.value, os.R_OK | os.W_OK))


class FileConfig(Configurable):
    msg = "Must be an existing file."
    def isvalid(self):
        return super(FileConfig, self).isvalid() and \
                    os.path.isfile(self.value)


class DatabaseConfig(Configurable):
    msg = "Must be an absolute path. " \
            "If path exists file must be readable and writeable"
    def isvalid(self):
        valid = super(DatabaseConfig, self).isvalid() and \
                    os.path.isabs(self.value) 
        if os.path.exists(self.value):
            valid = valid and os.path.isfile(self.value) and \
                        os.access(self.value, os.R_OK | os.W_OK)
        else:
            pass
        return valid


class QManagerConfig(Configurable):
    msg = "Must be a subclass of PipelineQueueManager."
    def isvalid(self):
        import queue_managers.generic_interface
        if super(QManagerConfig, self).isvalid() and \
                isinstance(self.value, queue_managers.generic_interface.PipelineQueueManager):
            # Check if appropriate functions are defined.
            methods = ['submit', \
                        'is_running', \
                        'delete', \
                        'status', \
                        'had_errors', \
                        'get_errors']
            alldefined = True
            undefined = []
            for m in methods:
                if m not in self.value.__class__.__dict__:
                    alldefined = False
                    undefined.append(m)
            if alldefined:
                return True
            else:
                QManagerConfig.msg = "The following methods must be defined: %s" % ', '.join(undefined)
                return False
        else:
            return False


class ConfigValidationError(Exception):
    """An exception to raise when a configuration is found to be
        invalid.
    """
    pass


class InsaneConfigsError(Exception):
    """An exception to raise when configurations in a ConfigList
        are found to fail the sanity check.
    """
    pass
