class WriterError(Exception):
    pass


class PreconditionFailed(WriterError):
    pass


class DestructiveOperationNotAllowed(WriterError):
    pass
