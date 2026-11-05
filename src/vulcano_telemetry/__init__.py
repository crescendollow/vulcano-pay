"""API pública de Vulcano Telemetry.

Expone las excepciones del paquete y los validadores de configuración
para poder importarlos directamente desde ``vulcano_telemetry``.
"""

from .exceptions import (
    VulcanoError,
    SQSConnectionError,
    QueueTimeoutError,
    CorruptedMessageError,
    WebhookSignatureError,
    WebhookTimestampError,
    WebhookDeliveryError,
)

from .sanitizer import (
    parse_queue_name,
    parse_wait_time,
    parse_webhook_url,
    parse_secret_key,
)

__all__ = [
    # Excepciones
    "VulcanoError",
    "SQSConnectionError",
    "QueueTimeoutError",
    "CorruptedMessageError",
    "WebhookSignatureError",
    "WebhookTimestampError",
    "WebhookDeliveryError",
    # Sanitizadores
    "parse_queue_name",
    "parse_wait_time",
    "parse_webhook_url",
    "parse_secret_key",
]