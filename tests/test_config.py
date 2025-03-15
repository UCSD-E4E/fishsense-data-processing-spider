'''Tests config
'''
from pathlib import Path

import tomlkit
from dynaconf import Dynaconf

from fishsense_data_processing_spider.config import validators


def test_config_validators(tmp_path: Path):
    """Tests config validators

    Args:
        tmp_path (Path): Temporary directory
    """
    config = {
        'scraper': {
            'data_paths': [
                '//e4e-nas.ucsd.edu/fishsense_data/REEF/data/'
            ]
        }
    }
    with open(tmp_path / 'settings.toml', 'w', encoding='utf-8') as handle:
        tomlkit.dump(config, handle)
    settings = Dynaconf(
        envvar_prefix='E4EFS',
        settings_files=[tmp_path / 'settings.toml'],
        merge_enabled=True,
        validators=validators
    )
    assert settings
