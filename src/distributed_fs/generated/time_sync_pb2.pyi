from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TimeRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TimeReply(_message.Message):
    __slots__ = ("timestamp",)
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    timestamp: float
    def __init__(self, timestamp: _Optional[float] = ...) -> None: ...
