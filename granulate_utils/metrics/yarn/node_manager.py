from granulate_utils.metrics.yarn.yarn_web_service import YarnWebService


class NodeManagerAPI(YarnWebService):
    def __init__(self, nm_address: str):
        super().__init__(nm_address)
