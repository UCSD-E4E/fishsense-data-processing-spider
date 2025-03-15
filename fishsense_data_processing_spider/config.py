'''Config
'''

from pathlib import Path

from dynaconf import Dynaconf, Validator

validators = [
        Validator(
            'scraper.data_paths',
            cast=list,
            required=True
        ),
        Validator(
            'scraper.data_paths',
            condition=lambda x: all(Path(y) is not None for y in x)
        )
    ]

settings = Dynaconf(
    envvar_prefix='E4EFS',
    settings_files=['settings.toml', '.secrets.toml'],
    merge_enabled=True,
    validators=validators
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
