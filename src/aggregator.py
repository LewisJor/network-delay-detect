import socket
import json
import time
import os
from datetime import datetime
from crypto_utils import decrypt_json
from utils import ensure_directory_exists

LISTEN_IP = "0.0.0.0"
LISTEN_PORT = 5005
BUFFER_SIZE = 4096

def log_message(message: dict) -> None:
    """
    Logs a received message to the appropriate log file.
    """
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        log_path = f"data/aggregator_logs/aggregator_{date_str}.log"
        ensure_directory_exists(log_path)
        with open(log_path, "a") as f:
            f.write(json.dumps(message) + "\n")
    except Exception as e:
        print(f"[ERROR] Failed to log message: {e}")

def main():
    print(f"[INFO] Starting aggregator...")
    print(f"[INFO] Listening on {LISTEN_IP}:{LISTEN_PORT}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((LISTEN_IP, LISTEN_PORT))
    except Exception as e:
        print(f"[ERROR] Failed to bind socket: {e}")
        return

    print(f"[INFO] Aggregator listening on {LISTEN_IP}:{LISTEN_PORT}")

    while True:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            try:
                encrypted_payload = json.loads(data.decode('utf-8'))
                try:
                    message = decrypt_json(encrypted_payload)
                except Exception as e:
                    print(f"[ERROR] Failed to decrypt message: {e}")
                    continue
                timestamp = datetime.now().isoformat()
                message["_received_timestamp"] = timestamp
                message["_source_ip"] = addr[0]

                print(f"[RECV] {message}")
                log_message(message)

            except json.JSONDecodeError as e:
                print(f"[ERROR] Failed to parse JSON message from {addr}: {e}")
            except Exception as e:
                print(f"[ERROR] Failed to process message from {addr}: {e}")
                
        except KeyboardInterrupt:
            print("[INFO] Aggregator shutting down...")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error in main loop: {e}")
            time.sleep(1)  # Brief pause before continuing

if __name__ == "__main__":
    main()