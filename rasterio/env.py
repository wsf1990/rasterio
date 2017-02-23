"""Rasterio's GDAL/AWS environment"""


from functools import wraps
import logging

from rasterio._env import GDALEnv
from rasterio.errors import EnvError


__all__ = ['Env', 'ensure_env']


# The currently active GDAL/AWS environment is a private attribute.
_ENV = None

log = logging.getLogger(__name__)


class Env(GDALEnv):

    """Manage's the GDAL environment."""

    def __init__(self, aws_session=None, aws_access_key_id=None,
                 aws_secret_access_key=None, aws_session_token=None,
                 aws_region_name=None, aws_profile_name=None, **options):

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
        """Get credentials from ``boto3`` and add them to the GDAL
        environment."""

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
        return {
            'CHECK_WITH_INVERT_PROJ': True,
            'GTIFF_IMPLICIT_JPEG_OVR': False,
            'DEFAULT_RASTERIO_ENV': True
        }

    @classmethod
    def from_defaults(cls, *args, **kwargs):
        options = Env.default_options().copy()
        options.update(**kwargs)
        return cls(*args, **options)

    def __enter__(self):
        global _ENV
        if _ENV is not None:
            self._parent_config_options = _ENV.config_options.copy()
            _ENV.close()
        self._start()
        self.set_config(**self._init_options)
        _ENV = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _ENV
        self.close()
        if self._parent_config_options is not None:
            _ENV = Env(**self._parent_config_options)
            _ENV._start()
        else:
            _ENV = None

    def close(self):
        self._stop()

    def __getitem__(self, key):
        return self.get_config(key)

    def __setitem__(self, key, value):
        self.set_config(key=value)

    def __delitem__(self, key):
        self.del_config(key)


def ensure_env(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        global _ENV
        if _ENV is not None:
            return f(*args, **kwds)
        else:
            with Env.from_defaults():
                return f(*args, **kwds)
    return wrapper
