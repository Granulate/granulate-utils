class ClientError(Exception):
    pass


class MaximumRetriesExceeded(Exception):
    def __init__(self, message: str, retry_count: int) -> None:
        self.message = message
        self.retry_count = retry_count
        super().__init__(message)


class APIError(Exception):
    def __init__(self, message: str, path: str, status_code: int) -> None:
        self.message = message
        self.path = path
        self.status_code = status_code
        super().__init__(message)

    def __str__(self) -> str:
        return f"{self.status_code} {self.message} {self.path}"
