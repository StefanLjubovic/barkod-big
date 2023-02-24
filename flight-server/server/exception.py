class FlightServerException(Exception):
    """Base exception class for flight server application."""


class SchemaNotExistsException(FlightServerException):
    """Schema does not exists on specified path."""


class UnsupportedDeserializerException(FlightServerException):
    """Raises when consumers subscribe to topic that has no defined schema."""


class KafkaMessageFetchException(FlightServerException):
    """Raises when there is error while fetching messages."""


class UnsupportedMessageException(FlightServerException):
    """Raises when there is unsupported message type on the input."""
