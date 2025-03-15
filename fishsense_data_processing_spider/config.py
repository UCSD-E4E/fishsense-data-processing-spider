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
