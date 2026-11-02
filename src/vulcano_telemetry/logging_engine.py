"""Logging JSON forense no bloqueante (QueueHandler/QueueListener + Gzip)."""

import atexit
import copy
import gzip
import json
import logging
import logging.handlers
import os
import queue
import shutil
import traceback
from datetime import datetime, timezone

LOGGER_NAME = "vulcano_engine"
DEFAULT_LOG_FILE = "vulcano_events.log"
MAX_LOG_BYTES = 2 * 1024 * 1024
BACKUP_COUNT = 3

# Claves de `extra` (snake_case en el código) y el nombre con el que se
# publican en el JSON. Las tres primeras aparecen siempre (null si faltan)
# para que todos los registros tengan el mismo esquema de correlación.
CORRELATION_FIELDS = {
    "trace_id": "TraceId",
    "message_id": "MessageId",
    "webhook_id": "WebhookId",
}
FIELD_ALIASES = {
    **CORRELATION_FIELDS,
    "webhook_signature": "X-Webhook-Signature",
}

# Atributos estándar de un LogRecord: todo lo que no esté acá llegó por
# `extra`. Se calculan con un registro vacío para no depender de la
# versión de Python (por ejemplo, taskName se agregó en la 3.12).
_RESERVED_ATTRS = frozenset(
    vars(logging.LogRecord("", logging.INFO, "", 0, "", (), None))
) | {"message", "asctime"}


# ----------------------------------------------------------------------
# Serialización recursiva de excepciones
# ----------------------------------------------------------------------

def _safe_str(value: object) -> str:
    """str() que nunca falla: un __str__ roto no debe perder el log."""
    try:
        return str(value)
    except Exception:
        return f"<{type(value).__name__} no imprimible>"


def _exception_type(exc: BaseException) -> str:
    """Nombre calificado del tipo (los builtins van sin prefijo)."""
    exc_type = type(exc)
    if exc_type.__module__ == "builtins":
        return exc_type.__qualname__
    return f"{exc_type.__module__}.{exc_type.__qualname__}"


def serialize_exception(
    exc: BaseException, _seen: set[int] | None = None
) -> dict:
    """
    Convierte una excepción en un diccionario JSON-serializable.

    Recorre recursivamente:
    - las sub-excepciones de un ExceptionGroup (árbol de errores
      concurrentes),
    - la causa encadenada con `raise ... from err` (__cause__) o, si no
      hay, el contexto implícito (__context__),
    e incluye las notas agregadas con add_note() y la traza de cada nodo.
    `_seen` corta ciclos de __context__ para no entrar en recursión
    infinita.
    """
    seen = set() if _seen is None else _seen
    if id(exc) in seen:
        return {"type": _exception_type(exc), "circular_reference": True}
    seen.add(id(exc))

    node = {
        "type": _exception_type(exc),
        "message": _safe_str(exc),
        "notes": [_safe_str(note) for note in getattr(exc, "__notes__", ())],
        "traceback": [
            {
                "file": frame.filename,
                "line": frame.lineno,
                "function": frame.name,
                "code": frame.line,
            }
            for frame in traceback.extract_tb(exc.__traceback__)
        ],
    }

    if isinstance(exc, BaseExceptionGroup):
        node["exceptions"] = [
            serialize_exception(sub_exc, seen) for sub_exc in exc.exceptions
        ]

    if exc.__cause__ is not None:
        node["cause"] = serialize_exception(exc.__cause__, seen)
    elif exc.__context__ is not None and not exc.__suppress_context__:
        node["context"] = serialize_exception(exc.__context__, seen)

    return node


# ----------------------------------------------------------------------
# Formateador JSON forense
# ----------------------------------------------------------------------

class AsyncJSONFormatter(logging.Formatter):
    """
    Traduce cada LogRecord a una línea JSON.

    Se ejecuta en el hilo del QueueListener, es decir, un rato después de
    que ocurrió el evento. Por eso la marca de tiempo sale de
    record.created (momento real del log) y no de datetime.now().
    """

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc)
        log_data = {
            "timestamp": timestamp.isoformat(timespec="microseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in CORRELATION_FIELDS.values():
            log_data[field] = None
        log_data["process"] = record.process
        log_data["threadName"] = record.threadName

        # Atributos de `extra`: nunca pisan un campo base ya completado.
        for key, value in vars(record).items():
            if key in _RESERVED_ATTRS:
                continue
            field = FIELD_ALIASES.get(key, key)
            if log_data.get(field) is None:
                log_data[field] = value

        if record.exc_info and record.exc_info[1] is not None:
            log_data["exception"] = serialize_exception(record.exc_info[1])
        if record.stack_info:
            log_data["stack_info"] = record.stack_info

        # default=str: un objeto no serializable en `extra` (UUID, datetime,
        # etc.) se guarda como texto en lugar de romper el log.
        return json.dumps(log_data, ensure_ascii=False, default=str)


# ----------------------------------------------------------------------
# Pipeline no bloqueante
# ----------------------------------------------------------------------

class VulcanoQueueHandler(logging.handlers.QueueHandler):
    """
    QueueHandler que conserva la excepción original del registro.

    El prepare() estándar formatea el mensaje y borra exc_info y
    stack_info, porque está pensado para colas entre procesos que
    necesitan pickle. Acá la cola es en memoria (queue.Queue), así que se
    mantiene exc_info para que el AsyncJSONFormatter pueda serializar el
    árbol completo del ExceptionGroup en el hilo del listener.
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        record = copy.copy(record)
        # msg % args se resuelve ahora, en el hilo que loguea, por si los
        # argumentos cambian antes de que el listener procese el registro.
        record.message = record.getMessage()
        record.msg = record.message
        record.args = None
        return record


def gzip_namer(default_name: str) -> str:
    """Nombre de los respaldos rotados: vulcano_events.log.1.gz, etc."""
    return default_name + ".gz"


def gzip_rotator(source: str, dest: str) -> None:
    """
    Comprime el log rotado a .gz de forma atómica.

    Escribe primero un archivo temporal y recién al terminar lo renombra
    con os.replace(). Si la compresión falla a mitad de camino nunca
    queda un .gz truncado y el log original no se borra.
    """
    if not os.path.exists(source):
        return

    temp_dest = dest + ".tmp"
    try:
        with open(source, "rb") as f_in, gzip.open(temp_dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.replace(temp_dest, dest)
    except OSError:
        if os.path.exists(temp_dest):
            os.remove(temp_dest)
        raise
    os.remove(source)


def setup_vulcano_logging(
    log_filename: str = DEFAULT_LOG_FILE,
    level: int = logging.INFO,
    console: bool = False,
) -> logging.Logger:
    """
    Configura el pipeline de logging no bloqueante y devuelve el logger
    "vulcano_engine".

    El hilo que loguea solo encola el registro (microsegundos). El
    QueueListener, en un hilo secundario, lo formatea a JSON y lo escribe
    en un RotatingFileHandler de 2 MB con 3 respaldos comprimidos en Gzip.
    El listener queda en logger.listener para detenerlo al cerrar la
    aplicación (ver stop_vulcano_logging).
    """
    logger = logging.getLogger(LOGGER_NAME)
    if getattr(logger, "listener", None) is not None:
        # Ya estaba configurado: evita handlers e hilos duplicados.
        return logger

    log_dir = os.path.dirname(os.path.abspath(log_filename))
    os.makedirs(log_dir, exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        log_filename,
        maxBytes=MAX_LOG_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(AsyncJSONFormatter())
    file_handler.namer = gzip_namer
    file_handler.rotator = gzip_rotator
    handlers = [file_handler]

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(AsyncJSONFormatter())
        handlers.append(console_handler)

    log_queue = queue.Queue(-1)
    listener = logging.handlers.QueueListener(
        log_queue, *handlers, respect_handler_level=True
    )
    listener.start()

    logger.setLevel(level)
    logger.addHandler(VulcanoQueueHandler(log_queue))
    # Todo sale por la cola: no se duplica en los handlers del root logger.
    logger.propagate = False
    logger.listener = listener

    # Red de seguridad: si la aplicación termina sin llamar a
    # stop_vulcano_logging(), igual se escriben los registros encolados.
    atexit.register(stop_vulcano_logging, logger)
    return logger


def stop_vulcano_logging(logger: logging.Logger | None = None) -> None:
    """
    Vacía la cola, detiene el hilo del QueueListener y cierra el archivo.

    Pensado para el bloque finally de app_operator. Llamarlo más de una
    vez no tiene efecto.
    """
    if logger is None:
        logger = logging.getLogger(LOGGER_NAME)

    listener = getattr(logger, "listener", None)
    if listener is None:
        return

    # stop() procesa todo lo que quedaba en la cola antes de terminar.
    listener.stop()
    for handler in listener.handlers:
        handler.close()
    for handler in list(logger.handlers):
        if isinstance(handler, VulcanoQueueHandler):
            logger.removeHandler(handler)
            handler.close()
    logger.listener = None
