"""Rasterio's GDAL/AWS environment."""


from functools import wraps
import logging

from rasterio._env import GDALEnv
from rasterio.errors import EnvError


__all__ = ['Env', 'ensure_env']


# The currently active GDAL/AWS environment is a private attribute.
_ENV = None

log = logging.getLogger(__name__)


class Env(GDALEnv):

    """Abstraction for GDAL and AWS configuration.

    The GDAL library is stateful: it has a registry of format drivers,
    an error stack, and dozens of configuration options.
    Rasterio's approach to working with GDAL is to wrap all the state
    up using a Python context manager (see PEP 343,
    https://www.python.org/dev/peps/pep-0343/). When the context is
    entered GDAL drivers are registered, error handlers are
    configured, and configuration options are set. When the context
    is exited, drivers are removed from the registry and other
    configurations are removed.

    Example:

        with rasterio.Env(GDAL_CACHEMAX=512) as env:
            # All drivers are registered, GDAL's raster block cache
            # size is set to 512MB.
            # Commence processing...
            ...
            # End of processing.

        # At this point, configuration options are set to their
        # previous (possible unset) values.

    A boto3 session or boto3 session constructor arguments
    `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`
    may be passed to Env's constructor. In the latter case, a session
    will be created as soon as needed. AWS credentials are configured
    for GDAL as needed.
    """

    def __init__(self, aws_session=None, aws_access_key_id=None,
                 aws_secret_access_key=None, aws_session_token=None,
                 aws_region_name=None, aws_profile_name=None, **options):

        """Create a new GDAL/AWS environment.

        Note: this class is a context manager. GDAL isn't configured
        until the context is entered via `with rasterio.Env()`.

        Parameters
        ----------
        aws_session: object, optional
            A boto3 session.
        aws_access_key_id: string, optional
            An access key id, as per boto3.
        aws_secret_access_key: string, optional
            A secret access key, as per boto3.
        aws_session_token: string, optional
            A session token, as per boto3.
        region_name: string, optional
            A region name, as per boto3.
        profile_name: string, optional
            A shared credentials profile name, as per boto3.
        **options: optional
            A mapping of GDAL configuration options, e.g.,
            `CPL_DEBUG=True, CHECK_WITH_INVERT_PROJ=False`.

        Raises
        ------
        EnvError
            If the GDAL config options `AWS_ACCESS_KEY_ID` or
            `AWS_SECRET_ACCESS_KEY` are given. AWS credentials are handled
            exclusively by `boto3`.

        Returns
        -------
        Env
            A new instance of Env.
        """

        super(Env, self).__init__()

        if ('AWS_ACCESS_KEY_ID' in options or
                'AWS_SECRET_ACCESS_KEY' in options):
            raise EnvError(
                "GDAL's AWS config options can not be directly set. "
                "AWS credentials are handled exclusively by boto3.")

        self._parent_config_options = None

        # These aren't populated until __enter__ so they need to live
        # somewhere
        self._init_options = options.copy()

        self.aws_session = aws_session
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.aws_region_name = aws_region_name
        self.aws_profile_name = aws_profile_name
        self._aws_creds = (
            self.aws_session._session.get_credentials()
            if self.aws_session else None)

    def auth_aws(self):
        """Use `boto3` to get AWS credentials and set the appropriate GDAL
        environment options.
        """

        import boto3
        if not self.aws_session:
            self.aws_session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_region_name,
                profile_name=self.aws_profile_name)
        self._aws_creds = self.aws_session._session.get_credentials()

        # Pass these credentials to the GDAL environment.
        if self._aws_creds.access_key:  # pragma: no branch
            self.set_config(aws_access_key_id=self._aws_creds.access_key)
        if self._aws_creds.secret_key:  # pragma: no branch
            self.set_config(aws_secret_access_key=self._aws_creds.secret_key)
        if self._aws_creds.token:
            self.set_config(aws_session_token=self._aws_creds.token)
        if self.aws_session.region_name:
            self.set_config(aws_region=self.aws_session.region_name)

    @staticmethod
    def default_options():
        """Use these options when instantiating from ``self.from_defaults()``.

        Returns
        -------
        dict
        """
        return {
            'CHECK_WITH_INVERT_PROJ': True,
            'GTIFF_IMPLICIT_JPEG_OVR': False,
            'DEFAULT_RASTERIO_ENV': True
        }

    @classmethod
    def from_defaults(cls, *args, **kwargs):
        """Instantiate a new ``Env()`` with a set of default config
        options.  Additional options can be given.

        Parameters
        ----------
        args : *args
            For ``Env()``.
        kwargs : **kwargs
            For ``Env()``.

        Returns
        -------
        Env
        """

        options = Env.default_options().copy()
        options.update(**kwargs)
        return cls(*args, **options)

    def __enter__(self):
        """Start the GDAL environment and set environment options."""
        global _ENV
        if _ENV is not None:
            self._parent_config_options = _ENV.config_options.copy()
            _ENV.close()
        self._start()
        self.set_config(**self._init_options)
        _ENV = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Teardown the GDAL environment, unset environment options, and
        reset if this is a nested environment, reinstate the parent context.
        """
        global _ENV
        self.close()
        if self._parent_config_options is not None:
            _ENV = Env(**self._parent_config_options)
            _ENV._start()
        else:
            _ENV = None

    def close(self):
        """Stop the GDAL environment."""
        self._stop()

    def __getitem__(self, key):
        """Get a GDAL environment option.

        Parameters
        ----------
        key : str
            Option name.

        Returns
        -------
        str or bool or None
        """
        return self.get_config(key)

    def __setitem__(self, key, value):
        """Set a GDAL environment option.

        Parameters
        ----------
        key : str
            Option name.
        value : str or bool or None
            Desired value.
        """
        self.set_config(key=value)

    def __delitem__(self, key):
        """Unset a GDAL environment option.

        Parameters
        ----------
        key : str
            Option name.
        """
        self.del_config(key)


def ensure_env(f):

    """A decorator that ensures a ``rasterio.Env()`` exists before a function
    executes.  If one already exists nothing is changed, if not then one is
    created from ``rasterio.Env.from_defaults()`` and is torn down when the
    function exits.

    Parameter
    ---------
    f : function
        Object to wrap.

    Returns
    -------
    function
        Wrapped function.
    """

    @wraps(f)
    def wrapper(*args, **kwds):
        global _ENV
        if _ENV is not None:
            return f(*args, **kwds)
        else:
            with Env.from_defaults():
                return f(*args, **kwds)
    return wrapper


def _current_env():

    """Get the current ``rasterio.Env()``.

    Raises
    ------
    EnvError
        If an environment does not exist.

    Returns
    -------
    Env
    """

    global _ENV

    if _ENV is None:
        raise EnvError("A 'rasterio.Env()' does not exist.")
    else:
        return _ENV