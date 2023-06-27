INVALID_TOKEN = "INVALID_TOKEN"
INVALID_ID = "INVALID_ID"

ACCESS_DENIED = "ACCESS_DENIED"

SERVICE_NOT_FOUND = "SERVICE_NOT_FOUND"

CLUSTER_NOT_FOUND = "CLUSTER_NOT_FOUND"
CLUSTER_EXISTS = "CLUSTER_EXISTS"

NODE_NOT_FOUND = "NODE_NOT_FOUND"
NODE_EXISTS = "NODE_EXISTS"


class BusinessLogicException(Exception):
    def __init__(self, code: str, message: str, status_code: int) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class InvalidIdException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(INVALID_ID, message, 400)


class ServiceNotFoundException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(SERVICE_NOT_FOUND, message, 404)


class ClusterNotFoundException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(CLUSTER_NOT_FOUND, message, 404)


class ClusterExistsException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(CLUSTER_EXISTS, message, 400)


class NodeNotFoundException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(NODE_NOT_FOUND, message, 404)


class NodeExistsException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(NODE_EXISTS, message, 400)


class InvalidTokenException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(INVALID_TOKEN, message, 401)


class AccessDeniedException(BusinessException):
    def __init__(self, message: str) -> None:
        super().__init__(ACCESS_DENIED, message, 403)


EXCEPTIONS = {
    INVALID_ID: InvalidIdException,
    INVALID_TOKEN: InvalidTokenException,
    ACCESS_DENIED: AccessDeniedException,
    SERVICE_NOT_FOUND: ServiceNotFoundException,
    CLUSTER_NOT_FOUND: ClusterNotFoundException,
    CLUSTER_EXISTS: ClusterExistsException,
    NODE_NOT_FOUND: NodeNotFoundException,
    NODE_EXISTS: NodeExistsException,
}


def raise_for_code(code: str, message: str) -> None:
    if code in EXCEPTIONS:
        raise EXCEPTIONS[code](message)
    raise BusinessException(code, message, 400)
