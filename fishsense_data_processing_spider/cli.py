'''Spider CLI
'''
from __future__ import annotations

import argparse
import datetime as dt
from typing import Optional

import rpyc

from fishsense_data_processing_spider.web_auth import Permission


def _get_api_key(comment: str, expires: Optional[dt.datetime] = None):
    new_key, expires = rpyc.connect(
        'localhost', 18861).root.get_api_key(comment, expires)
    print(f'Key: {new_key}')
    print(f'Expires: {expires.isoformat()}')


def _set_api_key_perms(key: str, permission: str, value: bool):
    rpyc.connect('localhost', 18861).root.set_api_key_permissions(
        key,
        permission,
        value
    )

def main():
    """Main entry point
    """
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=parser.print_help)
    subparsers = parser.add_subparsers()

    __get_api_key_setup(subparsers)
    __set_api_key_perms_setup(subparsers)

    arg_dict = vars(parser.parse_args())
    arg_fn = arg_dict.pop('func')
    arg_fn(**arg_dict)


def __get_api_key_setup(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]):
    get_api_key_parser = subparsers.add_parser('get_api_key')
    get_api_key_parser.add_argument(
        '-c', '--comment',
        type=str,
        required=True
    )
    get_api_key_parser.add_argument(
        '-e', '--expires',
        type=dt.datetime.fromisoformat,
        help='Expiration timestamp in ISO 8601',
        default=None
    )
    get_api_key_parser.set_defaults(func=_get_api_key)


def __set_api_key_perms_setup(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]):
    parser = subparsers.add_parser('set_api_key_perms')
    parser.add_argument(
        '-k', '--key',
        type=str,
        required=True
    )
    parser.add_argument(
        '-p', '--permission',
        type=str,
        choices=[p.name for p in Permission],
        required=True
    )
    parser.add_argument(
        'value',
        type=bool
    )
    parser.set_defaults(func=_set_api_key_perms)

if __name__ == '__main__':
    main()
