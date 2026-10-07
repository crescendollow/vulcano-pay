"""
Validadores sanitizadores declarativos para la CLI de Vulcano Pay.
Módulo a cargo del Integrante 1.
"""

import argparse
import re
from urllib.parse import urlparse


def parse_queue_name(value: str) -> str:
    """
    Valida que el nombre de la cola SQS cumpla estrictamente con el patrón:
    vulcano-<nombre>-queue (ejemplo: 'vulcano-events-queue').
    """
    pattern = r"^vulcano-[a-z0-9-]+-queue$"
    if not re.match(pattern, value):
        raise argparse.ArgumentTypeError(
            f"Nombre de cola inválido '{value}'. Debe seguir el patrón 'vulcano-<nombre>-queue'."
        )
    return value


def parse_wait_time(value: str) -> int:
    """
    Sanitiza el tiempo de Long Polling (WaitTimeSeconds) para SQS.
    Debe ser un entero estrictamente entre 1 y 20 segundos.
    """
    try:
        val = int(value)
        if not (1 <= val <= 20):
            raise ValueError()
        return val
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"WaitTimeSeconds inválido '{value}'. Debe ser un número entero entre 1 y 20 segundos."
        )


def parse_webhook_url(value: str) -> str:
    """
    Valida mediante urllib.parse que la URL del receptor sea un HTTP/HTTPS válido
    (ejemplo: 'http://localhost:8000/webhook').
    """
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            f"URL de Webhook inválida '{value}'. Debe incluir esquema HTTP o HTTPS válido."
        )
    return value


def parse_secret_key(value: str) -> str:
    """
    Valida que la clave secreta para la firma HMAC-SHA256 tenga una longitud
    mínima de seguridad de 16 caracteres.
    """
    if len(value) < 16:
        raise argparse.ArgumentTypeError(
            f"Clave secreta HMAC demasiado corta ({len(value)} caracteres). "
            f"Debe tener al menos 16 caracteres."
        )
    return value