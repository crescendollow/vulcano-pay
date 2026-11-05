"""Excepciones semánticas usadas por los componentes de Vulcano Telemetry."""


class VulcanoError(Exception):
    """Excepción base para los errores propios de Vulcano Telemetry."""

    pass


class SQSConnectionError(VulcanoError):
    """Indica que no fue posible conectar o comunicarse con Amazon SQS."""

    pass


class QueueTimeoutError(VulcanoError):
    """Indica que no se recibió un mensaje de la cola dentro del plazo."""

    pass


class CorruptedMessageError(VulcanoError):
    """Indica que un mensaje recibido no pudo interpretarse correctamente."""

    pass


class WebhookSignatureError(VulcanoError):
    """Indica que la firma de un Webhook falta, es inválida o no coincide."""

    pass


class WebhookTimestampError(VulcanoError):
    """Indica que el timestamp del Webhook está fuera de la tolerancia."""

    pass


class WebhookDeliveryError(VulcanoError):
    """Indica que no se pudo entregar un Webhook al destino."""

    pass