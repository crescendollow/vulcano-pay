"""Validadores de argumentos de configuración para Vulcano Telemetry."""

import argparse
import re
from urllib.parse import urlparse


def parse_queue_name(value: str) -> str:
    """Valida un nombre de cola con el formato ``vulcano-<nombre>-queue``.

    Args:
        value: Nombre de cola recibido como texto.

    Returns:
        El nombre de cola sin modificar.

    Raises:
        argparse.ArgumentTypeError: Si el nombre no cumple el formato esperado.
    """
    pattern = r"^vulcano-[a-z0-9-]+-queue$"
    if not re.match(pattern, value):
        raise argparse.ArgumentTypeError(
            f"Nombre de cola inválido '{value}'. Debe seguir el patrón 'vulcano-<nombre>-queue'."
        )
    return value


def parse_wait_time(value: str) -> int:
    """Convierte y valida un tiempo de espera entero entre 1 y 20 segundos.

    Args:
        value: Tiempo de espera recibido como texto.

    Returns:
        El tiempo de espera como entero.

    Raises:
        argparse.ArgumentTypeError: Si el valor no es un entero entre 1 y 20.
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
    """Valida que la URL de Webhook tenga esquema HTTP(S) y host.

    Args:
        value: URL recibida como texto.

    Returns:
        La URL sin modificar.

    Raises:
        argparse.ArgumentTypeError: Si la URL no es HTTP(S) o no tiene host.
    """
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            f"URL de Webhook inválida '{value}'. Debe incluir esquema HTTP o HTTPS válido."
        )
    return value


def parse_secret_key(value: str) -> str:
    """Valida que la clave secreta tenga al menos 16 caracteres.

    Args:
        value: Clave secreta recibida como texto.

    Returns:
        La clave sin modificar.

    Raises:
        argparse.ArgumentTypeError: Si la clave tiene menos de 16 caracteres.
    """
    if len(value) < 16:
        raise argparse.ArgumentTypeError(
            f"Clave secreta HMAC demasiado corta ({len(value)} caracteres). "
            f"Debe tener al menos 16 caracteres."
        )
    return value