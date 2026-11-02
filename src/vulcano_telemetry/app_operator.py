import argparse
import sys
import urllib.request
import json
import time
from urllib.error import URLError


from .exceptions import VulcanoError, WebhookDeliveryError, WebhookSignatureError, WebhookTimestampError, SQSConnectionError
from .sanitizer import parse_queue_name, parse_wait_time, parse_webhook_url, parse_secret_key
from .core_sqs import get_sqs_client, setup_queues, send_event, receive_events, delete_event
from .webhook_crypto import generate_signature
from .logging_engine import setup_vulcano_logging, stop_vulcano_logging

# ---------------------------------------------------------
# Integrante 5: Despachador de Webhooks
# ---------------------------------------------------------
def dispatch_webhook(target_url: str, secret: str, payload_dict: dict, trace_id: str = "N/A"):
    """Firma el payload y lo envía por HTTP POST al receptor."""
    payload_bytes = json.dumps(payload_dict).encode('utf-8')
    timestamp = int(time.time())
    
    # Genera la firma usando el código del Integrante 3
    signature = generate_signature(secret, timestamp, payload_bytes)
    header_value = f"t={timestamp},v1={signature}"

    req = urllib.request.Request(target_url, data=payload_bytes, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Webhook-Signature", header_value)

    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status != 202:
                raise WebhookDeliveryError(f"El servidor respondió con status {response.status}")
    except URLError as err:
        raise WebhookDeliveryError(f"Fallo al conectar con {target_url}") from err


# ---------------------------------------------------------
# Integrante 5: Comandos CLI
# ---------------------------------------------------------
def cmd_produce_sqs(args):
    print(f"[*] Publicando evento en la cola {args.queue_name}...")
    client = get_sqs_client()
    queue_url, _ = setup_queues(client, queue_name=args.queue_name)
    
    payload = {"evento": "prueba_desde_cli", "data": "Hola mundo"}
    msg_id = send_event(client, queue_url, payload, trace_id="TEST-CLI")
    print(f"[+] Mensaje enviado con éxito. ID: {msg_id}")


def cmd_start_dispatcher(args):
    print(f"[*] Iniciando despachador. Consumiendo de cola y enviando a {args.target_url}")
    client = get_sqs_client()
    queue_url, _ = setup_queues(client, queue_name=args.queue_name)

    print("[*] Esperando mensajes (Ctrl+C para salir)...")
    while True:
        events = receive_events(client, queue_url, wait_time=args.wait_time)
        for event in events:
            print(f"[*] Procesando mensaje ID: {event['message_id']}")
            try:
                dispatch_webhook(args.target_url, args.secret, event['body'], event['message_id'])
                print("[+] Webhook entregado exitosamente. Borrando de la cola.")
                delete_event(client, queue_url, event['receipt_handle'])
            except WebhookDeliveryError as e:
                print(f"[!] Error al entregar webhook: {e}")


def cmd_start_receiver(args):
    print(f"[*] Iniciando servidor receptor en el puerto {args.port}...")
    print("[!] (Este comando requiere que implementes el webhook_receiver.py con Flask/FastAPI)")
    # Nota: El Integrante 3 debería tener una función acá para levantar el servidor.
    # Si no la tiene, te va a tirar un error. ¡Avisame si pasa eso!
    try:
        from .webhook_receiver import run_receiver
        run_receiver(port=args.port, secret=args.secret)
    except ImportError:
        print("[-] ERROR: Falta la función 'run_server' en webhook_receiver.py")
        sys.exit(1)


# ---------------------------------------------------------
# Integrante 5: Configuración del CLI y Excepciones
# ---------------------------------------------------------
def build_parser():
    parser = argparse.ArgumentParser(description="Vulcano Pay CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Comando: produce-sqs
    p_produce = subparsers.add_parser("produce-sqs", help="Publica un evento en SQS")
    p_produce.add_argument("--queue-name", type=parse_queue_name, required=True)
    p_produce.set_defaults(func=cmd_produce_sqs)

    # Comando: start-dispatcher
    p_dispatch = subparsers.add_parser("start-dispatcher", help="Consume SQS y despacha webhooks")
    p_dispatch.add_argument("--queue-name", type=parse_queue_name, required=True)
    p_dispatch.add_argument("--target-url", type=parse_webhook_url, required=True)
    p_dispatch.add_argument("--secret", type=parse_secret_key, required=True)
    p_dispatch.add_argument("--wait-time", type=parse_wait_time, default=10)
    p_dispatch.set_defaults(func=cmd_start_dispatcher)

    # Comando: start-receiver
    p_receiver = subparsers.add_parser("start-receiver", help="Inicia el servidor HTTP local")
    p_receiver.add_argument("--port", type=int, default=8000)
    p_receiver.add_argument("--secret", type=parse_secret_key, required=True)
    p_receiver.add_argument("--queue-name", type=parse_queue_name, required=True)
    p_receiver.set_defaults(func=cmd_start_receiver)

    return parser


def main():
    # Inicializamos el logger del Integrante 4
    logger = setup_vulcano_logging(console=True)
    
    try:
        parser = build_parser()
        args = parser.parse_args()
        args.func(args)

    # Acá usamos el except* que pide el PDF
    except* WebhookSignatureError as eg:
        print(f"[!] Error de Seguridad: Firma inválida. {eg.exceptions}")
    except* WebhookTimestampError as eg:
        print(f"[!] Error de Seguridad: Posible ataque de Replay. {eg.exceptions}")
    except* SQSConnectionError as eg:
        print(f"[!] Error de Red: No se pudo conectar a LocalStack. {eg.exceptions}")
    except* VulcanoError as eg:
        print(f"[!] Error general de Vulcano: {eg.exceptions}")
    except* Exception as eg:
        print(f"[!] Error inesperado: {eg.exceptions}")
    
    finally:
        # Apagado ordenado que pide el PDF (sin return, break o continue)
        print("[*] Cerrando aplicación y vaciando logs...")
        stop_vulcano_logging(logger)


if __name__ == "__main__":
    main()