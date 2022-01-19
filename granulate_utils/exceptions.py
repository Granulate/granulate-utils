class UnsupportedNamespaceError(Exception):
    def __init__(self, nstype: str):
        super().__init__(f"Namespace {nstype!r} is not supported by this kernel")
        self.nstype = nstype
