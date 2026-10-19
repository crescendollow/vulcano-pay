"""Firma y verificación HMAC-SHA256 de Webhooks (timing-safe + timestamp)."""

import hashlib
import hmac
import re
import time

from .exceptions import WebhookSignatureError, WebhookTimestampError

SIGNATURE_HEADER = "X-Webhook-Signature"

# Ventana máxima de validez de un webhook firmado: 5 minutos.
MAX_TOLERANCE_SEC = 300

# t: segundos Unix en ASCII. Se acota la longitud para que int() nunca
# reciba un número gigante (Python limita la conversión a 4300 dígitos).
_TIMESTAMP_PATTERN = re.compile(r"[0-9]{1,15}")

# v1: SHA-256 en hexadecimal = 64 caracteres. Validarlo antes de comparar
# evita el TypeError de compare_digest() con strings no ASCII.
_SIGNATURE_PATTERN = re.compile(r"[0-9a-f]{64}")


def generate_signature(
    secret: str, timestamp: int, payload_bytes: bytes
) -> str:
    """
    Genera la firma HMAC-SHA256 (hex) sobre f"{timestamp}." + payload_bytes.

    El timestamp forma parte del mensaje firmado: si alguien lo modifica
    para reutilizar un webhook capturado, la firma deja de coincidir.
    """
    to_sign = f"{timestamp}.".encode("utf-8") + payload_bytes
    return hmac.new(
        secret.encode("utf-8"), to_sign, hashlib.sha256
    ).hexdigest()


def build_signature_header(
    secret: str, payload_bytes: bytes, timestamp: int | None = None
) -> str:
    """
    Arma el valor de la cabecera X-Webhook-Signature con el formato
    't=<timestamp>,v1=<firma>'. Si no se indica timestamp, usa el actual.
    """
    if timestamp is None:
        timestamp = int(time.time())
    signature = generate_signature(secret, timestamp, payload_bytes)
    return f"t={timestamp},v1={signature}"


def _parse_signature_header(header_value: str) -> tuple[int, str]:
    """
    Extrae el timestamp (t) y la firma (v1) de la cabecera.

    Cualquier formato inesperado se traduce a WebhookSignatureError, así
    el receptor responde 401 en lugar de cortarse con un error 500.
    """
    if not header_value:
        raise WebhookSignatureError(f"Cabecera {SIGNATURE_HEADER} ausente.")

    fields = {}
    for item in header_value.split(","):
        key, separator, value = item.strip().partition("=")
        if separator:
            fields[key] = value.strip()

    raw_timestamp = fields.get("t", "")
    signature = fields.get("v1", "")

    if not _TIMESTAMP_PATTERN.fullmatch(raw_timestamp):
        raise WebhookSignatureError(
            f"Cabecera {SIGNATURE_HEADER} malformada: 't' no es un "
            f"timestamp Unix válido."
        )
    if not _SIGNATURE_PATTERN.fullmatch(signature):
        raise WebhookSignatureError(
            f"Cabecera {SIGNATURE_HEADER} malformada: 'v1' no es una firma "
            f"SHA-256 hexadecimal."
        )
    return int(raw_timestamp), signature


def verify_signature(
    secret: str,
    header_value: str,
    payload_bytes: bytes,
    tolerance_sec: int = MAX_TOLERANCE_SEC,
) -> bool:
    """
    Verifica la cabecera X-Webhook-Signature (t=timestamp,v1=firma) contra
    los bytes crudos del cuerpo.

    - Replay Attack: rechaza timestamps fuera de la ventana de tolerancia
      (WebhookTimestampError).
    - Timing Attack: compara las firmas en tiempo constante con
      hmac.compare_digest() (WebhookSignatureError si no coinciden).
    """
    if not 0 < tolerance_sec <= MAX_TOLERANCE_SEC:
        raise ValueError(
            f"tolerance_sec debe estar entre 1 y {MAX_TOLERANCE_SEC} "
            f"segundos (recibido: {tolerance_sec})."
        )

    timestamp, received_sig = _parse_signature_header(header_value)

    # 1. Ventana de tiempo: se controla antes de calcular el HMAC, así un
    #    webhook viejo se descarta sin gastar CPU. abs() también cubre
    #    timestamps "del futuro" por relojes desincronizados.
    current_time = int(time.time())
    drift = abs(current_time - timestamp)
    if drift > tolerance_sec:
        err = WebhookTimestampError(
            f"Timestamp fuera de tolerancia ({drift}s > {tolerance_sec}s)."
        )
        err.add_note(
            f"Timestamp recibido: {timestamp} | hora del servidor: "
            f"{current_time}"
        )
        raise err

    # 2. compare_digest() tarda lo mismo sin importar en qué posición
    #    difieren las firmas: no filtra información por tiempo de respuesta.
    expected_sig = generate_signature(secret, timestamp, payload_bytes)
    if not hmac.compare_digest(expected_sig, received_sig):
        raise WebhookSignatureError("La firma HMAC-SHA256 no coincide.")
    return True
