"""
AnySQL Database URI Implementation
"""
from urllib.parse import parse_qsl, unquote, urlsplit
from typing import Any, Optional
from typing_extensions import Self

#** Variables **#
__all__ = ['DatabaseURI']

#** Classes **#

class Empty(str):
    def __bool__(self) -> bool:
        return True

class DatabaseURI:
    """Database URI Parser"""

    def __init__(self, uri: str):
        self._raw = uri
        self._uri = urlsplit(uri)

    def __str__(self) -> str:
        return self._raw

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._raw})'

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    @property
    def scheme(self) -> str:
        return self._uri.scheme

    @property
    def dialect(self) -> str:
        return self.scheme.split('+', 1)[0]

    @property
    def driver(self) -> Optional[str]:
        if '+' not in self.scheme:
            return
        return self.scheme.split('+', 1)[1]

    @property
    def userinfo(self) -> Optional[str]:
        args = (self._uri.username, self._uri.password)
        return ':'.join(c for c in args if c) or None

    @property
    def username(self) -> Optional[str]:
        return unquote(self._uri.username) if self._uri.username else None

    @property
    def password(self) -> Optional[str]:
        return unquote(self._uri.password) if self._uri.password else None

    @property
    def options(self) -> dict:
        return dict(parse_qsl(self._uri.query))

    @property
    def hostname(self) -> Optional[str]:
        return self._uri.hostname or \
            self.options.get('host') or \
            self.options.get('unix_host')

    @property
    def database(self) -> str:
        return unquote(self._uri.path.lstrip('/'))

    @property
    def port(self) -> Optional[int]:
        return self._uri.port

    @property
    def netloc(self) -> str:
        return self._uri.netloc

    def replace(self, **kwargs) -> Self:
        """"""
        netlocs = ('username', 'password', 'hostname', 'port')
        if any(comp in kwargs for comp in netlocs):
            port     = kwargs.pop("port", self.port)
            hostname = kwargs.pop("hostname", str(self.hostname))
            username = kwargs.pop("username", self._uri.username)
            password = kwargs.pop("password", self._uri.password)
            netloc   = hostname
            if port is not None:
                netloc += f':{port}'
            if username is not None:
                userpass = username
                if password is not None:
                    userpass += f':{password}'
                netloc = f'{userpass}@{netloc}'
            kwargs['netloc'] = netloc
        if 'database' in kwargs:
            kwargs['path'] = f'/{kwargs.pop("database")}'
        if 'dialect' in kwargs or 'driver' in kwargs:
            dialect = kwargs.pop('dialect', self.dialect)
            driver  = kwargs.pop('driver', self.driver)
            kwargs['scheme'] = f'{dialect}+{driver}' if driver else dialect
        if not kwargs.get('netloc', self.netloc):
            kwargs['netloc'] = Empty()
        uri = self._uri._replace(**kwargs)
        return self.__class__(uri.geturl())

    @property
    def obscure_password(self) -> str:
        if self.password:
            return self.replace(password='********')._raw
        return self._raw
