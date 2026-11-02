class VulcanoError(Exception):
    """Excepción base para todos los errores del ecosistema Vulcano."""
    pass


# ----------------------------------------------------------------------
# Subclases de Dominio SQS
# ----------------------------------------------------------------------

class SQSConnectionError(VulcanoError):
    """Lanzada ante fallos de comunicación o red con AWS SQS / LocalStack."""
    pass


class QueueTimeoutError(VulcanoError):
    """Lanzada cuando se supera el tiempo límite de espera en la cola SQS."""
    pass


class CorruptedMessageError(VulcanoError):
    """Lanzada cuando el cuerpo del mensaje SQS no es un JSON válido o está corrupto."""
    pass


# ----------------------------------------------------------------------
# Subclases de Dominio Webhook
# ----------------------------------------------------------------------

class WebhookSignatureError(VulcanoError):
    """Lanzada cuando la firma criptográfica HMAC-SHA256 es inválida o está ausente."""
    pass


class WebhookTimestampError(VulcanoError):
    """Lanzada cuando el timestamp del Webhook está fuera de la ventana de tolerancia (Replay Attack)."""
    pass


class WebhookDeliveryError(VulcanoError):
    """Lanzada cuando el envío HTTP POST del Webhook falla tras los reintentos."""
    pass