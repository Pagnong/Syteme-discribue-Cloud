import time
from network.storage_virtual_network import StorageVirtualNetwork
from node.storage_virtual_node import StorageVirtualNode

# ==========================================================
# 1  INITIALISATION DU RÉSEAU ET DES ÉVÉNEMENTS
# ==========================================================

network = StorageVirtualNetwork()

# --- Gestion des événements réseau ---
def on_transfer_started(data):
    print(f"\n Transfer started: {data['file_name']} ({data['file_size'] / 1024 / 1024:.2f} MB)")
    print(f"   From {data['source']} -> {data['target']}")

def on_transfer_completed(data):
    print(f" Transfer completed: {data['file_id']} — {data['total_size'] / 1024 / 1024:.2f} MB")
    print(f"   Source: {data['source']} | Target: {data['target']}\n")

def on_node_added(data):
    print(f" Node added: {data['node_id']}")

def on_nodes_connected(data):
    print(f" Connected {data['node1']} ↔ {data['node2']} @ {data['bandwidth']} Mbps")

# --- Abonnement aux événements ---
network.events.on("transfer_started", on_transfer_started)
network.events.on("transfer_completed", on_transfer_completed)
network.events.on("node_added", on_node_added)
network.events.on("nodes_connected", on_nodes_connected)


# ==========================================================
# 2  CRÉATION DES NŒUDS
# ==========================================================

node1 = StorageVirtualNode(
    node_id="node1",
    cpu_capacity=4,
    memory_capacity=16,
    storage_capacity=500,  # GB
    bandwidth=1000         # Mbps
)

node2 = StorageVirtualNode(
    node_id="node2",
    cpu_capacity=8,
    memory_capacity=32,
    storage_capacity=1000, # GB
    bandwidth=2000         # Mbps
)

network.add_node(node1)
network.add_node(node2)


# ==========================================================
# 3  CONNEXION ENTRE LES NŒUDS
# ==========================================================

network.connect_nodes("node1", "node2", bandwidth=1000)


# ==========================================================
# 4  LANCEMENT DU TRANSFERT
# ==========================================================

transfer = network.transfers.initiate_transfer(
    source_node_id="node1",
    target_node_id="node2",
    file_name="large_dataset.zip",
    file_size=100 * 1024 * 1024  # 100 MB
)

if not transfer:
    print(" Failed to initiate transfer (insufficient resources or network error)")
    exit()

print(f"\n Transfer initiated: {transfer.file_name} (id: {transfer.file_id})\n")


# ==========================================================
# 5  SIMULATION DE TRANSFERT ET AFFICHAGE DES STATS
# ==========================================================

while True:
    chunks_done, completed = network.transfers.process_transfer(
        source_node_id="node1",
        target_node_id="node2",
        file_id=transfer.file_id,
        chunks_per_step=3  # nombre de transferts parallèles
    )

    # Récupération des statistiques
    net_stats = network.get_network_stats()
    node2_stats = node2.get_storage_utilization()

    print(f" Transferred chunks: {chunks_done}")
    print(f" Network utilization: {net_stats['bandwidth_utilization']:.2f}%")
    print(f" Node2 storage usage: {node2_stats['utilization_percent']:.2f}%")
    print(f"  CPU: {node2_stats['cpu_usage_percent']}% | RAM: {node2_stats['memory_usage_percent']}%\n")

    if completed:
        print(" Transfer completed successfully!\n")
        break

    # Petite pause pour visualiser la progression
    time.sleep(0.5)
