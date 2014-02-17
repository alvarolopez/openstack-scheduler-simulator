import logging

from oslo.config import cfg

import simulator

ENV = simulator.ENV

opts = [
    cfg.BoolOpt('debug',
                short='d',
                default=False,
                help='Print debugging output (set logging level to '
                     'DEBUG instead of default WARNING level).'),
    cfg.BoolOpt('verbose',
                short='v',
                default=False,
                help='Print more verbose output (set logging level to '
                     'INFO instead of default WARNING level).'),
]

CONF = cfg.CONF
CONF.register_opts(opts, group='simulator')


class SimulatorFormatter(logging.Formatter):

    def format(self, record):
        record.created = ENV.now
#        self._fmt = ('%(created)2.1f %(name)s [%(levelname)s] '
#                     '%(message)s')
        self._fmt = ('%(created)2.1f %(name)20s [%(id)s] '
                     '%(message)s')
        return logging.Formatter.format(self, record)


class SimulatorAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra):
        self.logger = logger
        self.extra = {"id": "-"}
        self.extra.update(extra)

    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        extra = self.extra.copy()
        extra.update(kwargs['extra'])
        kwargs["extra"] = extra
        return msg, kwargs

_loggers = {}


def getLogger(name='unknown', extra={}):
    if name not in _loggers:
        _loggers[name] = SimulatorAdapter(logging.getLogger(name), extra)

    # This handles the logging level for each of the
    # simulator loggers
    if CONF.simulator.debug:
        _loggers[name].logger.setLevel(logging.DEBUG)
    elif CONF.simulator.verbose:
        _loggers[name].logger.setLevel(logging.INFO)
    else:
        _loggers[name].logger.setLevel(logging.WARNING)
    return _loggers[name]


def setup():
    log_root = logging.getLogger(None)
    for handler in log_root.handlers:
        log_root.removeHandler(handler)

#    streamlog = ColorHandler()
    streamlog = logging.StreamHandler()
    log_root.addHandler(streamlog)

    for handler in log_root.handlers:
        handler.setFormatter(SimulatorFormatter())

    # Handle logging levels from nova.
    # TODO(aloga): review this
#    if CONF.debug:
#        log_root.setLevel(logging.DEBUG)
#    elif CONF.verbose:
#        log_root.setLevel(logging.INFO)
#    else:
#        log_root.setLevel(logging.WARNING)
