'''Tests config
'''
import datetime as dt
from pathlib import Path
from typing import Dict

import pytest
import tomlkit
from dynaconf import Dynaconf

from fishsense_data_processing_spider.config import validators

configs = [
    {
        'scraper': {
            'interval': '15s',
            'data_paths': './README.md'
        },
        'postgres': {
            'username': 'postgres',
            'password_file': './README.md',
            'host': 'localhost'
        },
        'exiftool': {
            'path': './README.md'
        }
    },
    {
        'scraper': {
            'data_paths': './README.md'
        },
        'postgres': {
            'username': 'postgres',
            'password_file': './README.md',
            'host': 'localhost'
        },
        'exiftool': {
            'path': './README.md'
        }
    },
]


@pytest.mark.parametrize('config', configs)
def test_config_validators(tmp_path: Path, config: Dict):
    """Tests config validators

    Args:
        tmp_path (Path): Temporary directory
    """
    with open(tmp_path / 'settings.toml', 'w', encoding='utf-8') as handle:
        tomlkit.dump(config, handle)
    settings = Dynaconf(
        envvar_prefix='E4EFS',
        settings_files=[tmp_path / 'settings.toml'],
        merge_enabled=True,
        validators=validators
    )
    assert settings
    assert isinstance(settings.scraper.interval, dt.timedelta)
