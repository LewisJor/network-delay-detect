Network Delay Sensor

A simulation-ready, encrypted, UDP-based sensor network for edge environments and contested networks.

Description

Simulates distributed sensor nodes sending encrypted telemetry over unreliable UDP to a central aggregator. Supports message buffering, retransmission, and secure transport.

Tech Stack
	•	Python 3.11
	•	AES-256-GCM encryption (via pycryptodome)
	•	UDP sockets
	•	Docker & Docker Compose

Structure

.
├── aggregator/
│   ├── aggregator.py
│   └── crypto_utils.py
├── sensor/
│   ├── sensor.py
│   └── crypto_utils.py
├── data/
│   └── aggregator_logs/
├── docker-compose.yaml
├── .gitignore
└── README.md

Running Locally

# Generate and export a base64 AES key
export ENCRYPTION_KEY=$(openssl rand -base64 32)

# Build and run containers
docker-compose up --build

All logs will be written to data/aggregator_logs/.