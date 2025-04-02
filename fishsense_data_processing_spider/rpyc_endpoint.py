'''RPyC endpoints
'''
import datetime as dt
from typing import Optional

import rpyc

from fishsense_data_processing_spider.web_auth import KeyStore, Permission


class CliService(rpyc.Service):
    """CLI RPyC Service
    """
    def __init__(
        self,
        key_store: KeyStore
    ) -> None:
        self.__key_store = key_store
        super().__init__()

    def exposed_get_api_key(self, comment: str, expiration: Optional[dt.datetime] = None) -> str:
        """Exposed - gets a new API key

        Args:
            comment (str): Comment describing key
            expiration (Optional[dt.datetime], optional): Expiration. Defaults to 400 days.

        Returns:
            str: API key
        """
        return self.__key_store.get_new_key(
            comment=comment,
            expires=expiration
        )

    def exposed_set_api_key_permissions(self, api_key: str, permission: str, value: bool) -> None:
        """Exposed - sets the api key permissions

        Args:
            api_key (str): API Key
            permission (str): Permission name to set
            value (bool): Value to set
        """
        self.__key_store.set_perm(
            key=api_key,
            op=Permission[permission],
            value=value
        )
