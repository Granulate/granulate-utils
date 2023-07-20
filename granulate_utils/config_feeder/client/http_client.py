from typing import Any, Dict, Optional, Union, cast

from pydantic import BaseModel
from requests import Session
from requests.exceptions import ConnectionError, JSONDecodeError

from granulate_utils.config_feeder.client.exceptions import APIError, ClientError
from granulate_utils.config_feeder.core.errors import raise_for_code

DEFAULT_API_SERVER_ADDRESS = "https://api.granulate.io/config-feeder/api/v1"
DEFAULT_REQUEST_TIMEOUT = 3


class AuthCredentials(BaseModel):
    scheme: str
    credentials: str


class HttpClient:
    def __init__(self, auth: AuthCredentials, server_address: Optional[str]) -> None:
        self._server_address: str = server_address.rstrip("/") if server_address else DEFAULT_API_SERVER_ADDRESS
        self._session = Session()
        self._session.headers.update(
            {"Accept": "application/json", "Authorization": f"{auth.scheme} {auth.credentials}"}
        )

    def request(
        self,
        method: str,
        path: str,
        request_data: Optional[Union[BaseModel, Dict[str, Any]]] = None,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> Dict[str, Any]:
        try:
            resp = self._session.request(
                method,
                f"{self._server_address}{path}",
                json=request_data.dict()
                if isinstance(request_data, BaseModel)
                else request_data
                if request_data
                else None,
                timeout=timeout,
            )
            if resp.ok:
                return cast(Dict[str, Any], resp.json())
            try:
                res = resp.json()
                if "detail" in res:
                    raise APIError(res["detail"], path, resp.status_code)
                error = res["error"]
                raise_for_code(error["code"], error["message"])
                return cast(Dict[str, Any], res)
            except (KeyError, JSONDecodeError):
                raise APIError(resp.text or resp.reason, path, resp.status_code)
        except ConnectionError:
            raise ClientError(f"could not connect to {self._server_address}")
