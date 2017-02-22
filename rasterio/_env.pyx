# cython: c_string_type=unicode, c_string_encoding=utf8
"""GDAL and OGR driver management."""

import logging
import os
import os.path
import sys

from rasterio.compat import string_types

from rasterio._gdal cimport (
    CPLSetConfigOption, GDALAllRegister, GDALGetDriver,
    GDALGetDriverCount, GDALGetDriverLongName, GDALGetDriverShortName,
    OGRGetDriverCount, OGRRegisterAll, CPLPopErrorHandler, CPLPushErrorHandler)

include "gdal.pxi"


level_map = {
    0: 0,
    1: logging.DEBUG,
    2: logging.WARNING,
    3: logging.ERROR,
    4: logging.CRITICAL }

code_map = {
    0: 'CPLE_None',
    1: 'CPLE_AppDefined',
    2: 'CPLE_OutOfMemory',
    3: 'CPLE_FileIO',
    4: 'CPLE_OpenFailed',
    5: 'CPLE_IllegalArg',
    6: 'CPLE_NotSupported',
    7: 'CPLE_AssertionFailed',
    8: 'CPLE_NoWriteAccess',
    9: 'CPLE_UserInterrupt',
    10: 'ObjectNull',

    # error numbers 11-16 are introduced in GDAL 2.1. See 
    # https://github.com/OSGeo/gdal/pull/98.
    11: 'CPLE_HttpResponse',
    12: 'CPLE_AWSBucketNotFound',
    13: 'CPLE_AWSObjectNotFound',
    14: 'CPLE_AWSAccessDenied',
    15: 'CPLE_AWSInvalidCredentials',
    16: 'CPLE_AWSSignatureDoesNotMatch'}

log = logging.getLogger(__name__)


cdef void logging_error_handler(CPLErr err_class, int err_no,
                                const char* msg) with gil:
    """Send CPL debug messages and warnings to Python's logger."""
    log = logging.getLogger('rasterio._gdal')
    if err_no in code_map:
        # 'rasterio._gdal' is the name in our logging hierarchy for
        # messages coming direct from CPLError().
        log.log(level_map[err_class], "%s in %s", code_map[err_no], msg)
    else:
        log.info("Unknown error number %r", err_no)


cpdef get_gdal_config(key):
    """Get the value of a GDAL configuration option"""
    key = key.upper().encode('utf-8')
    val = CPLGetConfigOption(<const char *>key, NULL)
    if not val:
        return None
    else:
        if val == u'ON':
            return True
        elif val == u'OFF':
            return False
        else:
            return val


cpdef set_gdal_config(key, val):
    """Set a GDAL configuration option's value"""
    key = key.upper().encode('utf-8')
    if isinstance(val, string_types):
        val = val.encode('utf-8')
    else:
        val = ('ON' if val else 'OFF').encode('utf-8')
    CPLSetConfigOption(<const char *>key, <const char *>val)


cpdef del_gdal_config(key):
    """Delete a GDAL configuration option"""
    key = key.upper().encode('utf-8')
    CPLSetConfigOption(<const char *>key, NULL)


cdef class GDALEnv(object):

    """A bridge between Rasterio and the GDAL environment."""

    cdef public object _set_config_options
    cdef bint _active

    def __init__(self):
        """This class provides methods for Rasterio to interact with and
        manage GDAL's environment.  Config options can be set once
        ``GDALEnv._start()`` has been called.
        """
        self._set_config_options = set()
        self._active = False

    def _ensure_active(self):
        """Ensure's ``GDALEnv()`` has been activated before setting config
        options.
        """
        if not self._active:
            raise EnvironmentError(
                "A Rasterio managed GDAL environment is not active.")

    @property
    def config_options(self):
        """Returns a dictionary containing the currently set GDAL config
        options.

        Returns
        -------
        dict
            Like: ``{'CHECK_WITH_INVERT_PROJ: True}``.
        """
        out = {}
        for key in self._set_config_options:
            val = self.get_config(key)
            if val is not None:
                out[key] = val
        return out

    def _redact_val(self, key, val):
        """Some config options store sensitive information that shouldn't
        be logged.  This method is aware of these options and modifies the
        value.

        For example:

            >>> key = 'AWS_SECRET_ACCESS_KEY'
            >>> val = '<actual secret access key>'
            >>> print(_redact_val(key, val))
            ('AWS_SECRET_ACCESS_KEY', '******')

        Parameters
        ----------
        key : str
            GDAL config option name.
        val : str or None
            Config option value.

        Returns
        -------
        tuple
            ``(key, val)`` where ``val`` may be converted to ``'******'``.
        """
        if key.upper() in ['AWS_ACCESS_KEY_ID',
                               'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']:
            val = '******'
        return key, val

    def clear_config(self):
        """Clear all config options."""
        while self._set_config_options:
            key = self._set_config_options.pop()
            del_gdal_config(key)

    def set_config(self, **options):
        """Set GDAL config options.

        Parameters
        ----------
        options : **kwargs
            Like: ``name=value``.
        """
        self._ensure_active()
        for key, val in options.items():
            set_gdal_config(key, val)
            self._set_config_options.add(key)
            key, val = self._redact_val(key, val)
            log.debug("Set option '%s=%s' in env %r", key, val, self)

    def get_config(self, key):
        """Get a config option's value from the GDAL environment.

        Parameters
        ----------
        key : str
            Config option name.

        Returns
        -------
        str or None
            Will be ``None`` if not set.
        """
        self._ensure_active()
        return get_gdal_config(key)

    def del_config(self, key):
        """Remove a config option from the GDAL environment.

        Parameters
        ----------
        key : str
            Config option name.
        """
        self._ensure_active()
        del_gdal_config(key)
        log.debug("Deleted config option '%s' in env %r", key, self)

    @property
    def drivers(self):
        """A mapping of GDAL driver short names to long names.

        Returns
        -------
        dict
            Like: ``{'GTiff': 'GeoTIFF'}``.
        """
        cdef GDALDriverH driver = NULL
        cdef int i

        result = {}
        for i in range(GDALGetDriverCount()):
            driver = GDALGetDriver(i)
            key = GDALGetDriverShortName(driver)
            val = GDALGetDriverLongName(driver)
            result[key] = val

        return result

    def _start(self):
        """Start the GDAL environment by pushing the Rasterio error handler,
        registering both GDAL and OGR drivers, discovering ``GDAL_DATA``,
        and setting whatever initial environment options were
        """
        CPLPushErrorHandler(<CPLErrorHandler>logging_error_handler)
        log.debug("Logging error handler pushed.")
        GDALAllRegister()
        OGRRegisterAll()
        log.debug("All drivers registered.")

        if GDALGetDriverCount() + OGRGetDriverCount() == 0:
            CPLPopErrorHandler()
            log.debug("Error handler popped")
            raise ValueError("Drivers not registered.")

        if 'GDAL_DATA' not in os.environ:
            whl_datadir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "gdal_data"))
            share_datadir = os.path.join(sys.prefix, 'share/gdal')
            if os.path.exists(os.path.join(whl_datadir, 'pcs.csv')):
                os.environ['GDAL_DATA'] = whl_datadir
            elif os.path.exists(os.path.join(share_datadir, 'pcs.csv')):
                os.environ['GDAL_DATA'] = share_datadir

        if 'PROJ_LIB' not in os.environ:
            whl_datadir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "proj_data"))
            os.environ['PROJ_LIB'] = whl_datadir
        log.debug("Started GDALEnv %r.", self)
        self._active = True

    def _stop(self):
        """Stop the GDAL environment by removing Rasterio's error handler
        and clearing the config options.  The drivers are left untouched.
        """
        # NB: do not restore the CPL error handler to its default
        # state here. If you do, log messages will be written to stderr
        # by GDAL instead of being sent to Python's logging module.
        self.clear_config()
        log.debug("Stopping GDALEnv %r.", self)
        CPLPopErrorHandler()
        log.debug("Error handler popped.")
        log.debug("Stopped GDALEnv %r.", self)
        self._active = False
