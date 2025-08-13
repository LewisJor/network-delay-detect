import socket
import json
import time
import random
import os
from crypto_utils import encrypt_json
from utils import ensure_directory_exists

AGGREGATOR_IP = "aggregator"
AGGREGATOR_PORT = 5005
SEND_INTERVAL = 5  # seconds
BUFFER_FILE = "data/sensor_logs/buffer.jsonl"

def generate_sensor_data() -> dict:
    """
    Generates a synthetic sensor data record.
    """
    return {
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "motion_detected": random.choice([True, False]),
        "battery": round(random.uniform(3.5, 4.2), 2),
        "timestamp": time.time(),
        "sensor_id": os.getenv("SENSOR_ID", "sensor_default")
    }

def send_message(sock: socket.socket, message: bytes) -> bool:
    """
    Attempts to send a UDP message to the aggregator.
    Returns True if send does not raise an exception.
    """
    try:
        sock.sendto(message, (AGGREGATOR_IP, AGGREGATOR_PORT))
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send data: {e}")
        return False

def load_buffer() -> list:
    """
    Loads buffered messages from the persistent buffer file.
    Returns a list of JSON-deserialized messages.
    """
    if not os.path.exists(BUFFER_FILE):
        return []

    buffered_messages = []
    try:
        with open(BUFFER_FILE, "r") as f:
            for line in f:
                try:
                    buffered_messages.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue  # skip corrupt lines
    except Exception as e:
        print(f"[ERROR] Failed to load buffer file {BUFFER_FILE}: {e}")
        return []

    return buffered_messages

def save_buffer(messages: list) -> None:
    """
    Overwrites the buffer file with the current list of messages.
    """
    try:
        ensure_directory_exists(BUFFER_FILE)
        with open(BUFFER_FILE, "w") as f:
            for message in messages:
                f.write(json.dumps(message) + "\n")
    except Exception as e:
        print(f"[ERROR] Failed to save buffer file {BUFFER_FILE}: {e}")

def append_to_buffer(message: dict) -> None:
    """
    Appends a message to the buffer file.
    """
    try:
        ensure_directory_exists(BUFFER_FILE)
        with open(BUFFER_FILE, "a") as f:
            f.write(json.dumps(message) + "\n")
    except Exception as e:
        print(f"[ERROR] Failed to append to buffer file {BUFFER_FILE}: {e}")

def log_sent_message(message: dict) -> None:
    """
    Logs a successfully sent message to the appropriate log file.
    """
    try:
        log_path = f"data/sensor_logs/{message['sensor_id']}_sent.log"
        ensure_directory_exists(log_path)
        with open(log_path, "a") as f:
            f.write(json.dumps(message) + "\n")
    except Exception as e:
        print(f"[ERROR] Failed to log sent message: {e}")

def main() -> None:
    """
    Main execution loop for the sensor node.
    Generates and sends sensor data, ensuring unsent messages are retried.
    """
    print(f"[INFO] Starting sensor node with ID: {os.getenv('SENSOR_ID', 'sensor_default')}")
    print(f"[INFO] Connecting to aggregator at {AGGREGATOR_IP}:{AGGREGATOR_PORT}")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    while True:
        try:
            # Load and attempt to send buffered messages first
            buffered_messages = load_buffer()
            remaining_buffer = []

            for msg in buffered_messages:
                try:
                    encrypted = encrypt_json(msg)
                    msg_bytes = json.dumps(encrypted).encode('utf-8')
                except Exception as e:
                    print(f"[ERROR] Encryption failed: {e}")
                    continue
                
                if send_message(sock, msg_bytes):
                    print(f"[RESEND] Successfully sent buffered message: {msg}")
                    log_sent_message(msg)
                else:
                    remaining_buffer.append(msg)

            save_buffer(remaining_buffer)

            # Generate and attempt to send a new message
            data = generate_sensor_data()
            try:
                encrypted = encrypt_json(data)
                message_bytes = json.dumps(encrypted).encode('utf-8')
            except Exception as e:
                print(f"[ERROR] Encryption failed: {e}")
                continue

            if send_message(sock, message_bytes):
                print(f"[SEND] Successfully sent message: {data}")
                log_sent_message(data)
            else:
                # Store unsent message in buffer
                print(f"[BUFFER] Storing message to buffer due to send failure.")
                append_to_buffer(data)

            time.sleep(SEND_INTERVAL)
            
        except KeyboardInterrupt:
            print("[INFO] Sensor node shutting down...")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error in main loop: {e}")
            time.sleep(SEND_INTERVAL)  # Continue running despite errors

if __name__ == "__main__":
    main()