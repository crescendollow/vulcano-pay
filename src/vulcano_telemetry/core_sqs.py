import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectTimeoutError,
    ReadTimeoutError,
)

from .exceptions import (
    CorruptedMessageError,
    QueueTimeoutError,
    SQSConnectionError,
)

logger = logging.getLogger("vulcano_engine")

DEFAULT_ENDPOINT_URL = "http://localhost:4566"
DEFAULT_REGION = "us-east-1"
DEFAULT_VISIBILITY_TIMEOUT = 30
DEFAULT_MAX_RECEIVE_COUNT = 3
SQS_MAX_BATCH = 10  # Límite duro de SQS para receive_message


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------
def get_sqs_client(endpoint_url: str = DEFAULT_ENDPOINT_URL):
    """Creacion de un cliente Boto3 SQS configurado para LocalStack.
    """
    config = Config(
        connect_timeout=5,
        read_timeout=30,
        retries={"max_attempts": 3, "mode": "standard"},
    )
    return boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        region_name=DEFAULT_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=config,
    )


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------
def _utc_now_iso() -> str:
    """Marca de tiempo ISO 8601 en UTC."""
    return datetime.now(timezone.utc).isoformat()


def _wrap_boto_error(
    err: Exception, message: str, **context: Any
) -> Exception:
    """Traduce un error nativo de Boto3 a una excepción semántica.
    Agrega contexto con ``add_note()`` (PEP 678). El llamador debe
    re-lanzarla encadenada: ``raise _wrap_boto_error(...) from err``.
    """
    if isinstance(err, (ConnectTimeoutError, ReadTimeoutError)):
        new_err: Exception = QueueTimeoutError(message)
    else:
        new_err = SQSConnectionError(message)

    for key, value in context.items():
        new_err.add_note(f"{key}: {value}")

    if isinstance(err, ClientError):
        code = err.response.get("Error", {}).get("Code", "Unknown")
        new_err.add_note(f"AWS ErrorCode: {code}")
    else:
        new_err.add_note(f"Causa original: {type(err).__name__}")
    return new_err


def _flatten_attributes(raw: dict) -> dict:
    """Convierte MessageAttributes de SQS a un dict simple clave/valor."""
    return {
        name: attr.get("StringValue")
        for name, attr in (raw or {}).items()
    }


# ---------------------------------------------------------------------------
# Colas (cola principal + Dead Letter Queue)
# ---------------------------------------------------------------------------
def get_queue_url(sqs_client, queue_name: str) -> str:
    """Resuelve la URL de una cola existente a partir de su nombre."""
    try:
        return sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except (ClientError, BotoCoreError) as err:
        raise _wrap_boto_error(
            err,
            "No se pudo resolver la URL de la cola SQS.",
            QueueName=queue_name,
        ) from err


def setup_queues(
    sqs_client,
    queue_name: str = "vulcano-events-queue",
    dlq_name: str = "vulcano-events-dlq",
    max_receive_count: int = DEFAULT_MAX_RECEIVE_COUNT,
) -> tuple[str, str]:
    """Crea (idempotente) la cola principal y su DLQ con RedrivePolicy.

    Tras ``max_receive_count`` recepciones sin borrado, SQS deriva el
    mensaje automáticamente a la DLQ (mensajes venenosos).

    Returns:
        Tupla ``(queue_url, dlq_url)``.
    """
    try:
        dlq_url = sqs_client.create_queue(QueueName=dlq_name)["QueueUrl"]
        dlq_arn = sqs_client.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        redrive = {
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": str(max_receive_count),
        }
        queue_url = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={
                "RedrivePolicy": json.dumps(redrive),
                "VisibilityTimeout": str(DEFAULT_VISIBILITY_TIMEOUT),
            },
        )["QueueUrl"]
    except (ClientError, BotoCoreError) as err:
        raise _wrap_boto_error(
            err,
            "Error al crear las colas SQS en LocalStack.",
            QueueName=queue_name,
            DLQName=dlq_name,
        ) from err

    logger.info(
        "Colas SQS listas: %s (DLQ: %s)", queue_name, dlq_name
    )
    return queue_url, dlq_url


# ---------------------------------------------------------------------------
# Productor
# ---------------------------------------------------------------------------
def send_event(
    sqs_client,
    queue_url: str,
    event_data: dict,
    event_topic: str = "vulcano.event",
    trace_id: Optional[str] = None,
) -> str:
    """Publica un evento JSON en SQS inyectando MessageAttributes.

    Atributos enviados: ``TraceId``, ``EventTopic`` y ``Timestamp``.

    Returns:
        El ``MessageId`` asignado por SQS.

    Raises:
        SQSConnectionError / QueueTimeoutError: ante fallos de red o AWS.
    """
    trace_id = trace_id or str(uuid.uuid4())
    body = json.dumps(event_data, ensure_ascii=False)

    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=body,
            MessageAttributes={
                "TraceId": {"DataType": "String", "StringValue": trace_id},
                "EventTopic": {
                    "DataType": "String",
                    "StringValue": event_topic,
                },
                "Timestamp": {
                    "DataType": "String",
                    "StringValue": _utc_now_iso(),
                },
            },
        )
    except (ClientError, BotoCoreError) as err:
        raise _wrap_boto_error(
            err,
            "Error al enviar mensaje a AWS SQS LocalStack.",
            QueueUrl=queue_url,
            TraceId=trace_id,
            EventTopic=event_topic,
        ) from err

    message_id = response.get("MessageId", "N/A")
    logger.info(
        "Evento publicado en SQS. MessageId: %s",
        message_id,
        extra={"message_id": message_id, "trace_id": trace_id},
    )
    return message_id


# ---------------------------------------------------------------------------
# Consumidor
# ---------------------------------------------------------------------------
def receive_events(
    sqs_client,
    queue_url: str,
    wait_time: int = 10,
    max_messages: int = 5,
    visibility_timeout: int = DEFAULT_VISIBILITY_TIMEOUT,
) -> list[dict]:
    """Consume mensajes de SQS aplicando Long Polling.

    NO borra los mensajes: el despachador de webhooks debe llamar a
    ``delete_event()`` solo cuando la entrega fue exitosa. Si no se
    borra, el mensaje reaparece tras ``visibility_timeout`` y, superado
    ``maxReceiveCount``, SQS lo deriva a la DLQ.
    Returns:
        Lista de dicts con ``message_id``, ``receipt_handle``, ``body``,
        ``attributes`` y ``receive_count``.
    """
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(max_messages, SQS_MAX_BATCH),
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            MessageAttributeNames=["All"],
            AttributeNames=["ApproximateReceiveCount"],
        )
    except (ClientError, BotoCoreError) as err:
        raise _wrap_boto_error(
            err,
            "Error en recepción de mensajes SQS.",
            QueueUrl=queue_url,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
        ) from err

    events: list[dict] = []
    for msg in response.get("Messages", []):
        message_id = msg.get("MessageId", "N/A")
        try:
            body = json.loads(msg["Body"])
        except (json.JSONDecodeError, TypeError) as err:
            corrupt = CorruptedMessageError(
                f"Mensaje SQS corrupto ID {message_id}"
            )
            corrupt.add_note(f"QueueUrl: {queue_url}")
            corrupt.__cause__ = err
            logger.error(
                "Mensaje venenoso detectado; se deja en la cola "
                "para derivación a la DLQ",
                exc_info=corrupt,
                extra={"message_id": message_id},
            )
            continue

        attributes = _flatten_attributes(msg.get("MessageAttributes", {}))
        events.append(
            {
                "message_id": message_id,
                "receipt_handle": msg["ReceiptHandle"],
                "body": body,
                "attributes": attributes,
                "receive_count": int(
                    msg.get("Attributes", {}).get(
                        "ApproximateReceiveCount", 1
                    )
                ),
            }
        )

    logger.info(
        "Recibidos %d eventos válidos de SQS.",
        len(events),
    )
    return events


def delete_event(sqs_client, queue_url: str, receipt_handle: str) -> None:
    """Elimina explícitamente un mensaje procesado con éxito."""
    try:
        sqs_client.delete_message(
            QueueUrl=queue_url, ReceiptHandle=receipt_handle
        )
    except (ClientError, BotoCoreError) as err:
        raise _wrap_boto_error(
            err,
            "Error al eliminar mensaje de SQS.",
            QueueUrl=queue_url,
        ) from err
    logger.info("Mensaje eliminado de SQS tras procesamiento exitoso.")


# ---------------------------------------------------------------------------
# Alias de compatibilidad con la plantilla de referencia
# ---------------------------------------------------------------------------
produce_event = send_event
consume_events = receive_events
