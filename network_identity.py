import random

class NetworkIdentity:
    """Generate and store automatic IP + MAC addresses for a node."""

    def __init__(self):
        self.ip_address = self._generate_ip()
        self.mac_address = self._generate_mac()

    def _generate_ip(self):
        # Simule un réseau local 10.0.x.x
        return f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}"

    def _generate_mac(self):
        # Génère une adresse MAC aléatoire
        return ":".join(f"{random.randint(0, 255):02x}" for _ in range(6))

    def to_dict(self):
        return {
            "ip": self.ip_address,
            "mac": self.mac_address
        }
