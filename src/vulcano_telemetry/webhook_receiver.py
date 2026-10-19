"""Servidor HTTP (FastAPI) receptor de Webhooks firmados con HMAC-SHA256."""

import asyncio
import json
import logging
import os
import re
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from functools import partial

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status

from .core_sqs import get_sqs_client, send_event
from .exceptions import (
    SQSConnectionError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .webhook_crypto import SIGNATURE_HEADER, verify_signature

logger = logging.getLogger("vulcano_engine.receiver")

SECRET_ENV_VAR = "VULCANO_WEBHOOK_SECRET"
DEFAULT_ENDPOINT_URL = "http://localhost:4566"
DEFAULT_QUEUE_URL = "http://localhost:4566/000000000000/vulcano-events-queue"
WEBHOOK_ID_HEADER = "X-Webhook-Id"

# Tiempo máximo esperando que SQS confirme. Sin este límite, si LocalStack
# no responde, Boto3 reintenta durante ~30 s y el emisor queda colgado.
PUBLISH_TIMEOUT_SEC = 3.0

# Si el emisor manda su propio X-Webhook-Id se respeta (sirve para detectar
# entregas duplicadas); si no viene o trae caracteres raros, se genera uno.
_WEBHOOK_ID_PATTERN = re.compile(r"[A-Za-z0-9_.:-]{1,128}")


def _resolve_webhook_id(header_value: str | None) -> str:
    """Devuelve el X-Webhook-Id recibido si es válido, o uno nuevo."""
    if header_value and _WEBHOOK_ID_PATTERN.fullmatch(header_value):
        return header_value
    return uuid.uuid4().hex


def create_app(
    secret: str | None = None,
    queue_url: str = DEFAULT_QUEUE_URL,
    endpoint_url: str = DEFAULT_ENDPOINT_URL,
    publisher: Callable[[dict], str] | None = None,
    publish_timeout: float = PUBLISH_TIMEOUT_SEC,
) -> FastAPI:
    """
    Construye la aplicación FastAPI del receptor de Webhooks.

    - secret: clave HMAC compartida con el emisor. Si no se pasa, se lee de
      la variable de entorno VULCANO_WEBHOOK_SECRET (nunca va en el código).
    - publisher: función que recibe el evento y devuelve el MessageId de
      SQS. Por defecto publica en LocalStack con core_sqs (Boto3); se puede
      reemplazar para hacer pruebas sin SQS.
    - publish_timeout: segundos máximos esperando la confirmación de SQS
      antes de responder 503.
    """
    secret = secret or os.environ.get(SECRET_ENV_VAR, "")
    if not secret:
        raise RuntimeError(
            f"Falta la clave HMAC del receptor: pasala por parámetro o "
            f"definí la variable de entorno {SECRET_ENV_VAR}."
        )

    if publisher is None:
        # El cliente Boto3 se crea una sola vez al iniciar. Crearlo en cada
        # request cuesta cientos de ms y rompería el objetivo de 100 ms.
        sqs_client = get_sqs_client(endpoint_url)
        publisher = partial(send_event, sqs_client, queue_url)

    app = FastAPI(title="Vulcano Webhook Receiver")

    @app.get("/health")
    async def health() -> dict:
        """Chequeo simple para saber si el servidor está levantado."""
        return {"status": "ok"}

    @app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
    async def receive_webhook(request: Request) -> dict:
        """
        1. Lee los bytes crudos del cuerpo (raw body).
        2. Verifica la firma HMAC-SHA256 y la ventana de tiempo.
        3. Recién entonces interpreta el JSON.
        4. Publica el evento en SQS y responde 202 Accepted.
        """
        started = time.perf_counter()
        trace_id = uuid.uuid4().hex
        signature_header = request.headers.get(SIGNATURE_HEADER, "")
        log_context = {
            "trace_id": trace_id,
            "webhook_signature": signature_header,
        }

        # La firma se calcula sobre exactamente lo que llegó por la red:
        # re-serializar el JSON (espacios, orden de claves) la cambiaría.
        raw_body = await request.body()

        try:
            verify_signature(secret, signature_header, raw_body)
        except WebhookTimestampError as err:
            logger.warning(
                "Webhook rechazado: posible replay o reloj desincronizado.",
                exc_info=err,
                extra=log_context,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Timestamp fuera de la ventana de tolerancia.",
            ) from err
        except WebhookSignatureError as err:
            logger.warning(
                "Webhook rechazado: firma HMAC inválida o ausente.",
                exc_info=err,
                extra=log_context,
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Firma HMAC inválida o ausente.",
            ) from err

        try:
            payload = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            logger.warning(
                "Webhook rechazado: el cuerpo no es un JSON válido.",
                extra=log_context,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El cuerpo no es un JSON válido.",
            ) from err
        if not isinstance(payload, dict):
            logger.warning(
                "Webhook rechazado: el cuerpo no es un objeto JSON.",
                extra=log_context,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El cuerpo debe ser un objeto JSON.",
            )

        webhook_id = _resolve_webhook_id(
            request.headers.get(WEBHOOK_ID_HEADER)
        )
        log_context["webhook_id"] = webhook_id
        event = {
            "webhook_id": webhook_id,
            "trace_id": trace_id,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        # Boto3 es bloqueante: se ejecuta en un hilo aparte para no frenar
        # el event loop mientras SQS confirma la recepción.
        try:
            message_id = await asyncio.wait_for(
                asyncio.to_thread(publisher, event), timeout=publish_timeout
            )
        except (SQSConnectionError, TimeoutError) as err:
            if isinstance(err, TimeoutError):
                # Si SQS confirma tarde, el reintento del emisor puede
                # generar un duplicado: el webhook_id permite descartarlo.
                err.add_note(f"SQS no respondió en {publish_timeout} s.")
            logger.error(
                "No se pudo encolar el webhook en SQS.",
                exc_info=err,
                extra=log_context,
            )
            # 503 (y no 202): el emisor sabe que tiene que reintentar.
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Cola de eventos no disponible, reintentar luego.",
                headers={"Retry-After": "5"},
            ) from err

        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        logger.info(
            "Webhook verificado y encolado en SQS.",
            extra={
                **log_context,
                "message_id": message_id,
                "latency_ms": latency_ms,
            },
        )
        return {
            "status": "accepted",
            "message_id": message_id,
            "trace_id": trace_id,
            "webhook_id": webhook_id,
        }

    return app


def run_receiver(
    secret: str | None = None,
    queue_url: str = DEFAULT_QUEUE_URL,
    endpoint_url: str = DEFAULT_ENDPOINT_URL,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """
    Levanta el receptor con uvicorn (para el subcomando start-receiver de
    app_operator).
    """
    app = create_app(
        secret=secret, queue_url=queue_url, endpoint_url=endpoint_url
    )
    uvicorn.run(app, host=host, port=port)
