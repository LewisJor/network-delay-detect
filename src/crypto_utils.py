import os
import base64
import json
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Validate and decode encryption key
raw = os.environ.get("ENCRYPTION_KEY", "")
if not raw:
    raise EnvironmentError("ENCRYPTION_KEY not set.")
ENCRYPTION_KEY = base64.b64decode(raw)
if len(ENCRYPTION_KEY) != 32:
    raise ValueError("ENCRYPTION_KEY must decode to 32 bytes (AES-256).")

def encrypt_json(data: dict) -> dict:
    json_data = json.dumps(data).encode('utf-8')
    nonce = get_random_bytes(12)
    cipher = AES.new(ENCRYPTION_KEY, AES.MODE_GCM, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(json_data)
    return {
        "nonce": base64.b64encode(nonce).decode(),
        "tag": base64.b64encode(tag).decode(),
        "ciphertext": base64.b64encode(ciphertext).decode()
    }

def decrypt_json(encrypted: dict) -> dict:
    nonce = base64.b64decode(encrypted["nonce"])
    tag = base64.b64decode(encrypted["tag"])
    ciphertext = base64.b64decode(encrypted["ciphertext"])
    cipher = AES.new(ENCRYPTION_KEY, AES.MODE_GCM, nonce=nonce)
    plaintext = cipher.decrypt_and_verify(ciphertext, tag)
    return json.loads(plaintext.decode('utf-8'))