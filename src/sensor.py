import socket
import json
import time
import random
import os
from collections import deque
from crypto_utils import encrypt_json
from utils import ensure_directory_exists

AGGREGATOR_IP = "aggregator"
AGGREGATOR_PORT = 5005
SEND_INTERVAL = 5  # seconds
HEARTBEAT_INTERVAL = 2  # seconds
BUFFER_FILE = "data/sensor_logs/buffer.jsonl"
NETWORK_METRICS_FILE = "data/sensor_logs/network_metrics.jsonl"
RTT_WINDOW_SIZE = 20  # samples for jitter calculation

class NetworkMonitor:
    """
    Tracks network performance metrics including RTT, jitter, and packet loss.
    """
    def __init__(self):
        self.rtt_samples = deque(maxlen=RTT_WINDOW_SIZE)
        self.sequence_number = 0
        self.heartbeats_sent = 0
        self.heartbeats_received = 0
        self.last_heartbeat_time = 0
        
    def get_next_sequence(self) -> int:
        self.sequence_number += 1
        return self.sequence_number
        
    def calculate_jitter(self) -> float:
        """Calculate jitter as standard deviation of RTT samples."""
        if len(self.rtt_samples) < 2:
            return 0.0
        mean_rtt = sum(self.rtt_samples) / len(self.rtt_samples)
        variance = sum((rtt - mean_rtt) ** 2 for rtt in self.rtt_samples) / len(self.rtt_samples)
        return variance ** 0.5
        
    def calculate_packet_loss(self) -> float:
        """Calculate packet loss percentage for heartbeats."""
        if self.heartbeats_sent == 0:
            return 0.0
        return max(0.0, (self.heartbeats_sent - self.heartbeats_received) / self.heartbeats_sent * 100)
        
    def get_network_condition(self) -> str:
        """Classify network condition based on metrics."""
        if len(self.rtt_samples) < 5:
            return "INITIALIZING"
            
        avg_rtt = sum(self.rtt_samples) / len(self.rtt_samples)
        jitter = self.calculate_jitter()
        packet_loss = self.calculate_packet_loss()
        
        if packet_loss > 10 or avg_rtt > 500:  # >500ms or >10% loss
            return "CONTESTED"
        elif packet_loss > 2 or avg_rtt > 200 or jitter > 50:  # degraded thresholds
            return "DEGRADED"
        else:
            return "GOOD"

def generate_sensor_data(monitor: NetworkMonitor) -> dict:
    """
    Generates a synthetic sensor data record with network timing metadata.
    """
    return {
        "message_type": "SENSOR_DATA",
        "sequence": monitor.get_next_sequence(),
        "send_timestamp_ns": time.perf_counter_ns(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "motion_detected": random.choice([True, False]),
        "battery": round(random.uniform(3.5, 4.2), 2),
        "timestamp": time.time(),
        "sensor_id": os.getenv("SENSOR_ID", "sensor_default")
    }

def generate_heartbeat(monitor: NetworkMonitor) -> dict:
    """
    Generates a heartbeat message for RTT measurement.
    """
    return {
        "message_type": "HEARTBEAT",
        "sequence": monitor.get_next_sequence(),
        "send_timestamp_ns": time.perf_counter_ns(),
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
                    continue
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

def log_network_metrics(monitor: NetworkMonitor) -> None:
    """
    Logs current network performance metrics.
    """
    try:
        metrics = {
            "timestamp": time.time(),
            "sensor_id": os.getenv("SENSOR_ID", "sensor_default"),
            "avg_rtt_ms": sum(monitor.rtt_samples) / len(monitor.rtt_samples) if monitor.rtt_samples else 0,
            "jitter_ms": monitor.calculate_jitter(),
            "packet_loss_percent": monitor.calculate_packet_loss(),
            "network_condition": monitor.get_network_condition(),
            "samples_count": len(monitor.rtt_samples),
            "heartbeats_sent": monitor.heartbeats_sent,
            "heartbeats_received": monitor.heartbeats_received
        }
        
        ensure_directory_exists(NETWORK_METRICS_FILE)
        with open(NETWORK_METRICS_FILE, "a") as f:
            f.write(json.dumps(metrics) + "\n")
    except Exception as e:
        print(f"[ERROR] Failed to log network metrics: {e}")

def process_heartbeat_response(monitor: NetworkMonitor, response: dict) -> None:
    """
    Process a heartbeat response to calculate RTT and update metrics.
    """
    try:
        if response.get("message_type") == "HEARTBEAT_ACK":
            send_time = response.get("original_send_timestamp_ns")
            receive_time = time.perf_counter_ns()
            
            if send_time:
                rtt_ns = receive_time - send_time
                rtt_ms = rtt_ns / 1_000_000  # Convert to milliseconds
                monitor.rtt_samples.append(rtt_ms)
                monitor.heartbeats_received += 1
                
                print(f"[RTT] {rtt_ms:.2f}ms - Condition: {monitor.get_network_condition()}")
    except Exception as e:
        print(f"[ERROR] Failed to process heartbeat response: {e}")

def main() -> None:
    """
    Main execution loop for the sensor node with network delay monitoring.
    Sends sensor data and heartbeats, measures network performance.
    """
    print(f"[INFO] Starting sensor node with ID: {os.getenv('SENSOR_ID', 'sensor_default')}")
    print(f"[INFO] Connecting to aggregator at {AGGREGATOR_IP}:{AGGREGATOR_PORT}")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.1)
    monitor = NetworkMonitor()
    
    last_sensor_data_time = 0
    last_heartbeat_time = 0
    last_metrics_log_time = 0

    while True:
        try:
            current_time = time.time()
            
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
                    print(f"[RESEND] Successfully sent buffered message")
                    log_sent_message(msg)
                else:
                    remaining_buffer.append(msg)

            save_buffer(remaining_buffer)

            if current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                heartbeat = generate_heartbeat(monitor)
                try:
                    encrypted = encrypt_json(heartbeat)
                    heartbeat_bytes = json.dumps(encrypted).encode('utf-8')
                    
                    if send_message(sock, heartbeat_bytes):
                        monitor.heartbeats_sent += 1
                        monitor.last_heartbeat_time = current_time
                        print(f"[HEARTBEAT] Sent sequence {heartbeat['sequence']}")
                    else:
                        append_to_buffer(heartbeat)
                        
                except Exception as e:
                    print(f"[ERROR] Heartbeat encryption failed: {e}")
                
                last_heartbeat_time = current_time

            condition = monitor.get_network_condition()
            
            adaptive_interval = SEND_INTERVAL
            if condition == "CONTESTED":
                adaptive_interval = SEND_INTERVAL * 2
            elif condition == "DEGRADED":
                adaptive_interval = SEND_INTERVAL * 1.5
            
            if current_time - last_sensor_data_time >= adaptive_interval:
                data = generate_sensor_data(monitor)
                try:
                    encrypted = encrypt_json(data)
                    message_bytes = json.dumps(encrypted).encode('utf-8')
                    
                    if send_message(sock, message_bytes):
                        interval_info = f"(interval: {adaptive_interval}s)" if adaptive_interval != SEND_INTERVAL else ""
                        print(f"[SEND] Data sequence {data['sequence']} - Network: {condition} {interval_info}")
                        log_sent_message(data)
                    else:
                        print(f"[BUFFER] Storing sensor data due to send failure")
                        append_to_buffer(data)
                        
                except Exception as e:
                    print(f"[ERROR] Sensor data encryption failed: {e}")
                
                last_sensor_data_time = current_time

            # Log network metrics every 30 seconds
            if current_time - last_metrics_log_time >= 30:
                log_network_metrics(monitor)
                last_metrics_log_time = current_time
                
                if len(monitor.rtt_samples) >= 5:
                    condition = monitor.get_network_condition()
                    avg_rtt = sum(monitor.rtt_samples) / len(monitor.rtt_samples)
                    jitter = monitor.calculate_jitter()
                    loss = monitor.calculate_packet_loss()
                    
                    print(f"[METRICS] Condition: {condition}, RTT: {avg_rtt:.2f}ms, "
                          f"Jitter: {jitter:.2f}ms, Loss: {loss:.1f}%")

            try:
                response_data, _ = sock.recvfrom(4096)
                try:
                    encrypted_response = json.loads(response_data.decode('utf-8'))
                    response = decrypt_json(encrypted_response)
                    process_heartbeat_response(monitor, response)
                except Exception as e:
                    print(f"[ERROR] Failed to process response: {e}")
            except socket.timeout:
                pass
            except Exception as e:
                print(f"[ERROR] Socket receive error: {e}")
            
            time.sleep(0.1)
            
        except KeyboardInterrupt:
            print("[INFO] Sensor node shutting down...")
            log_network_metrics(monitor)
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error in main loop: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()