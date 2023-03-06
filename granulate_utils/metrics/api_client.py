#
# Copyright (c) Granulate. All rights reserved.
# Licensed under the AGPL3 License. See LICENSE.md in the project root for license information.
#
import gzip
import json
from io import BytesIO
from typing import IO, Any, Dict, List, cast

import requests

DEFAULT_API_SERVER_ADDRESS = "https://api.granulate.io"
DEFAULT_REQUEST_TIMEOUT = 5
DEFAULT_UPLOAD_TIMEOUT = 120


class APIError(Exception):
    def __init__(self, message: str, full_data: dict = None):
        self.message = message
        self.full_data = full_data

    def __str__(self) -> str:
        return self.message


class APIClient:
    def __init__(
        self,
        server_address: str = DEFAULT_API_SERVER_ADDRESS,
        timeout: int = DEFAULT_UPLOAD_TIMEOUT,
    ):
        self._server_address = server_address
        self._timeout = timeout
        self._session = requests.Session()

    def _request_url(
        self,
        method: str,
        url: str,
        data: Any,
        files: Dict = None,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
        params: Dict[str, str] = None,
    ) -> Dict:
        headers = self.headers().copy()
        if params is None:
            params = {}

        kwargs = {}
        if method.upper() == "GET":
            if data is not None:
                params.update(data)
        else:
            headers["Content-Encoding"] = "gzip"
            headers["Content-type"] = "application/json"
            buffer = BytesIO()
            with gzip.open(buffer, mode="wt", encoding="utf-8") as gzip_file:
                try:
                    json.dump(data, cast(IO[str], gzip_file), ensure_ascii=False)
                except TypeError:
                    # This should only happen while in development, and is used to get a more indicative error.
                    self.log_bad_json(data)
                    raise
            kwargs["data"] = buffer.getvalue()

        resp = self._session.request(
            method, url, headers=headers, files=files, timeout=timeout, params=params, **kwargs
        )
        self.log_response(resp)

        if 400 <= resp.status_code < 500:
            try:
                response_data = resp.json()
                raise APIError(response_data.get("message", "(no message in response)"), response_data)
            except ValueError:
                raise APIError(resp.text)
        else:
            resp.raise_for_status()
        return cast(dict, resp.json())

    def headers(self) -> dict[str, str]:
        return {}

    def log_bad_json(self, data: Any) -> None:
        pass

    def log_response(self, response: requests.Response) -> None:
        pass

    def submit_spark_metrics(self, url: str, timestamp: int, metrics: List[Dict[str, Any]]) -> Dict:
        return self._request_url(
            "POST",
            url,
            {
                "format_version": 0,
                "timestamp": timestamp,
                "metrics": metrics,
            },
            timeout=self._timeout,
        )
