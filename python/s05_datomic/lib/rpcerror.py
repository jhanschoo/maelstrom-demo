class RPCError(ValueError):
    @staticmethod
    def timeout(*args):
        return RPCError(0, *args)

    @staticmethod
    def no_handler(*args):
        return RPCError(10, *args)

    @staticmethod
    def temporarily_unavailable(*args):
        return RPCError(11, *args)

    @staticmethod
    def malformed_request(*args):
        return RPCError(12, *args)

    @staticmethod
    def crash(*args):
        return RPCError(13, *args)

    @staticmethod
    def abort(*args):
        return RPCError(14, *args)

    @staticmethod
    def key_does_not_exist(*args):
        return RPCError(20, *args)

    @staticmethod
    def precondition_failed(*args):
        return RPCError(22, *args)

    @staticmethod
    def txn_conflict(*args):
        return RPCError(30, *args)

    def __init__(self, code: int, *args) -> None:
        self.code = code
        super().__init__(*args)

    def as_dict(self):
        return { "type": "error", "code": self.code, "text": str(self) }
