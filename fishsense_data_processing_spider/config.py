'''Config
'''

import datetime as dt
from pathlib import Path

from dynaconf import Dynaconf, Validator
from humanfriendly import parse_timespan

validators = [
    Validator(
        'scraper.data_paths',
        cast=list,
        required=True
    ),
    Validator(
        'scraper.data_paths',
        condition=lambda x: all(Path(y) is not None for y in x)
    ),
    Validator(
        'scraper.interval',
        cast=lambda x: dt.timedelta(seconds=parse_timespan(x)),
        default='1m'
    ),
    Validator(
        'postgres.username',
        required=True,
        cast=str
    ),
    Validator(
        'postgres.password_file',
        required=True,
        cast=Path,
        condition=lambda x: Path(x).is_file()
    ),
    Validator(
        'postgres.host',
        required=True,
        cast=str
    ),
    Validator(
        'postgres.port',
        required=True,
        cast=str,
        default=5432
    ),
    Validator(
        'postgres.database',
        required=True,
        cast=str,
        default='postgres'
    )
]

settings = Dynaconf(
    envvar_prefix='E4EFS',
    environments=False,
    settings_files=['settings.toml', '.secrets.toml'],
    merge_enabled=True,
    validators=validators
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
with open(settings.postgres.password_file, 'r', encoding='utf-8') as handle:
    __postgres_password = handle.read(256)
POSTGRES_CONNECTION_STR = (f'postgres://{settings.postgres.username}:{__postgres_password}@'
                           f'{settings.postgres.host}:{settings.postgres.port}/'
                           f'{settings.postgres.database}')
