import socket
import json
import time
import os
import logging
from datetime import datetime
from collections import defaultdict, deque
from crypto_utils import decrypt_json, encrypt_json
from utils import ensure_directory_exists

LISTEN_IP = "0.0.0.0"
LISTEN_PORT = 5005
BUFFER_SIZE = 4096
NETWORK_ANALYSIS_FILE = "data/aggregator_logs/network_analysis.jsonl"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_LEVEL = os.getenv("AGGREGATOR_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT
)
logger = logging.getLogger("network_delay_detector")

class NetworkAnalyzer:
    """
    Analyzes network performance patterns across all sensor nodes.
    """
    def __init__(self):
        self.sensor_metrics = defaultdict(lambda: {
            'rtt_samples': deque(maxlen=50),
            'last_sequence': 0,
            'packet_count': 0,
            'missed_sequences': set(),
            'last_seen': 0
        })
        
    def update_sensor_metrics(self, sensor_id: str, message: dict):
        """Update metrics for a specific sensor based on received message."""
        metrics = self.sensor_metrics[sensor_id]
        current_time = time.time()
        
        metrics['last_seen'] = current_time
        
        if 'sequence' in message:
            sequence = message['sequence']
            
            if metrics['last_sequence'] > 0:
                for missed_seq in range(metrics['last_sequence'] + 1, sequence):
                    metrics['missed_sequences'].add(missed_seq)
                    
            metrics['last_sequence'] = max(metrics['last_sequence'], sequence)
            metrics['packet_count'] += 1
            
        if message.get('message_type') in ['SENSOR_DATA', 'HEARTBEAT'] and 'send_timestamp_ns' in message:
            receive_time_ns = time.perf_counter_ns()
            send_time_ns = message['send_timestamp_ns']
            rtt_ms = (receive_time_ns - send_time_ns) / 1_000_000
            metrics['rtt_samples'].append(rtt_ms)
            
    def get_sensor_analysis(self, sensor_id: str) -> dict:
        """Generate network analysis for a specific sensor."""
        metrics = self.sensor_metrics[sensor_id]
        
        if not metrics['rtt_samples']:
            return {
                'sensor_id': sensor_id,
                'status': 'NO_DATA',
                'last_seen': metrics['last_seen']
            }
            
        rtt_samples = list(metrics['rtt_samples'])
        avg_rtt = sum(rtt_samples) / len(rtt_samples)
        
        jitter = 0
        if len(rtt_samples) > 1:
            variance = sum((rtt - avg_rtt) ** 2 for rtt in rtt_samples) / len(rtt_samples)
            jitter = variance ** 0.5
            
        total_expected = metrics['last_sequence'] if metrics['last_sequence'] > 0 else metrics['packet_count']
        packet_loss = len(metrics['missed_sequences']) / max(total_expected, 1) * 100
        
        condition = 'GOOD'
        if packet_loss > 10 or avg_rtt > 500:
            condition = 'CONTESTED'
        elif packet_loss > 2 or avg_rtt > 200 or jitter > 50:
            condition = 'DEGRADED'
            
        return {
            'sensor_id': sensor_id,
            'avg_rtt_ms': round(avg_rtt, 2),
            'jitter_ms': round(jitter, 2),
            'packet_loss_percent': round(packet_loss, 2),
            'network_condition': condition,
            'samples_count': len(rtt_samples),
            'packets_received': metrics['packet_count'],
            'missed_packets': len(metrics['missed_sequences']),
            'last_seen': metrics['last_seen'],
            'status': 'ACTIVE'
        }

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
        logger.error("Failed to log message: %s", e)

def log_network_analysis(analysis: dict) -> None:
    """
    Logs network analysis results.
    """
    try:
        ensure_directory_exists(NETWORK_ANALYSIS_FILE)
        with open(NETWORK_ANALYSIS_FILE, "a") as f:
            f.write(json.dumps(analysis) + "\n")
    except Exception as e:
        logger.error("Failed to log network analysis: %s", e)

def send_heartbeat_ack(sock: socket.socket, addr: tuple, original_message: dict) -> None:
    """
    Send heartbeat acknowledgment back to sensor for RTT calculation.
    """
    try:
        ack_message = {
            "message_type": "HEARTBEAT_ACK",
            "original_sequence": original_message.get("sequence"),
            "original_send_timestamp_ns": original_message.get("send_timestamp_ns"),
            "aggregator_timestamp": time.time()
        }
        
        encrypted = encrypt_json(ack_message)
        response_bytes = json.dumps(encrypted).encode('utf-8')
        sock.sendto(response_bytes, addr)
        
    except Exception as e:
        logger.error("Failed to send heartbeat ACK: %s", e)

def main():
    logger.info("Starting network delay detection aggregator")
    logger.info("Listening on %s:%s", LISTEN_IP, LISTEN_PORT)
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((LISTEN_IP, LISTEN_PORT))
    except Exception as e:
        logger.error("Failed to bind socket: %s", e)
        return
    
    logger.info("Aggregator listening on %s:%s", LISTEN_IP, LISTEN_PORT)
    
    analyzer = NetworkAnalyzer()
    last_analysis_time = 0

    while True:
        try:
            data, addr = sock.recvfrom(BUFFER_SIZE)
            try:
                encrypted_payload = json.loads(data.decode('utf-8'))
                try:
                    message = decrypt_json(encrypted_payload)
                except Exception as e:
                    logger.error("Failed to decrypt message from %s: %s", addr[0], e)
                    continue
                    
                message["_received_timestamp"] = datetime.now().isoformat()
                message["_source_ip"] = addr[0]
                
                sensor_id = message.get("sensor_id", "unknown")
                analyzer.update_sensor_metrics(sensor_id, message)
                
                msg_type = message.get("message_type", "UNKNOWN")
                
                if msg_type == "HEARTBEAT":
                    logger.info("[HEARTBEAT] From %s seq:%s", sensor_id, message.get('sequence', '?'))
                    send_heartbeat_ack(sock, addr, message)
                    
                elif msg_type == "SENSOR_DATA":
                    condition = analyzer.get_sensor_analysis(sensor_id).get('network_condition', 'UNKNOWN')
                    logger.info("[DATA] %s seq:%s condition:%s", sensor_id, message.get('sequence', '?'), condition)
                    
                else:
                    logger.info("[RECV] %s from %s", msg_type, sensor_id)

                log_message(message)

            except json.JSONDecodeError as e:
                logger.error("Failed to parse JSON message from %s: %s", addr, e)
            except Exception as e:
                logger.error("Failed to process message from %s: %s", addr, e)
                
            current_time = time.time()
            if current_time - last_analysis_time >= 60:
                logger.info("[NETWORK ANALYSIS]")
                for sensor_id in analyzer.sensor_metrics.keys():
                    analysis = analyzer.get_sensor_analysis(sensor_id)
                    if analysis['status'] == 'ACTIVE':
                        logger.info(
                            "  %s: %s (RTT:%.1fms, Jitter:%.1fms, Loss:%.1f%%)",
                            sensor_id,
                            analysis['network_condition'],
                            analysis['avg_rtt_ms'],
                            analysis['jitter_ms'],
                            analysis['packet_loss_percent']
                        )
                        
                        analysis['timestamp'] = current_time
                        log_network_analysis(analysis)
                
                last_analysis_time = current_time
                
        except KeyboardInterrupt:
            logger.info("Aggregator shutting down")
            
            logger.info("[FINAL NETWORK ANALYSIS]")
            for sensor_id in analyzer.sensor_metrics.keys():
                analysis = analyzer.get_sensor_analysis(sensor_id)
                analysis['timestamp'] = time.time()
                analysis['final_report'] = True
                log_network_analysis(analysis)
                logger.info("  %s: %s", sensor_id, analysis.get('network_condition', 'NO_DATA'))
            
            break
        except Exception as e:
            logger.exception("Unexpected error in main loop: %s", e)
            time.sleep(1)

if __name__ == "__main__":
    main()