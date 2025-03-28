'''Spider CLI
'''
import argparse
import datetime as dt
from typing import Optional

import rpyc


def _get_api_key(comment: str, expires: Optional[dt.datetime] = None):
    new_key = rpyc.connect('localhost', 18861).root.get_api_key(comment, expires)
    print(new_key)

def main():
    """Main entry point
    """
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=parser.print_help)
    subparsers = parser.add_subparsers()

    get_api_key_parser = subparsers.add_parser('get_api_key')
    get_api_key_parser.add_argument(
        '-c', '--comment',
        type=str,
        required=True
    )
    get_api_key_parser.add_argument(
        '-e', '--expires',
        type=dt.datetime.fromisoformat,
        default=None
    )
    get_api_key_parser.set_defaults(func=_get_api_key)

    arg_dict = vars(parser.parse_args())
    arg_fn = arg_dict.pop('func')
    arg_fn(**arg_dict)

if __name__ == '__main__':
    main()
