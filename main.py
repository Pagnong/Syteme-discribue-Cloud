# main.py
import time
import threading
import sys
import json
from network.storage_virtual_network import StorageVirtualNetwork
from node.storage_virtual_node import StorageVirtualNode
from core.persistence import PersistenceManager

# ----------------------------
# Configuration
# ----------------------------
GC_INTERVAL_SECONDS = 60      # intervalle du garbage collector par nœud
MANUAL_INPUT_POLL = 0.5       # attente entre vérifications de l'input (s)


# ----------------------------
# Helper: background GC thread
# ----------------------------
def start_node_gc_thread(node: StorageVirtualNode, interval: int, stop_event: threading.Event):
    """
    Démarre un thread démon qui appelle node.cleanup(interval) en boucle.
    Le thread s'arrêtera si stop_event est positionné.
    """
    def loop():
        while not stop_event.is_set():
            try:
                removed = node.cleanup(max_age_seconds=interval * 5)  # e.g. retire les transferts > 5 * interval
                if removed:
                    print(f"[GC] Node {node.node_id} cleaned transfers: {removed}")
            except Exception as e:
                print(f"[GC] Node {node.node_id} cleanup error: {e}")
            # attend avant prochain nettoyage (intervalles courts pour la démo)
            for _ in range(interval):
                if stop_event.is_set():
                    break
                time.sleep(1)
    t = threading.Thread(target=loop, daemon=True, name=f"gc-{node.node_id}")
    t.start()
    return t


# ----------------------------
# 1) Initialisation réseau & persistence
# ----------------------------
network = StorageVirtualNetwork()
persistence = PersistenceManager()  # utilisé si on veut lire l'historique

# handlers événements
def on_transfer_started(data):
    print(f"\n🟢 Transfer started: {data['file_name']} ({data['file_size'] / 1024 / 1024:.2f} MB)")
    print(f"   From {data['source']} ➜ {data['target']}")

def on_transfer_completed(data):
    print(f"✅ Transfer completed: {data['file_id']} — {data['total_size'] / 1024 / 1024:.2f} MB")
    print(f"   Source: {data['source']} | Target: {data['target']}\n")

def on_node_added(data):
    print(f"🧩 Node added: {data['node_id']}")

def on_nodes_connected(data):
    print(f"🔗 Connected {data['node1']} ↔ {data['node2']} @ {data['bandwidth']} Mbps")

network.events.on("transfer_started", on_transfer_started)
network.events.on("transfer_completed", on_transfer_completed)
network.events.on("node_added", on_node_added)
network.events.on("nodes_connected", on_nodes_connected)


# ----------------------------
# 2) Création des nœuds
# ----------------------------
node1 = StorageVirtualNode("node1", cpu_capacity=4, memory_capacity=16, storage_capacity=500, bandwidth=1000)
node2 = StorageVirtualNode("node2", cpu_capacity=8, memory_capacity=32, storage_capacity=1000, bandwidth=2000)
network.add_node(node1)
network.add_node(node2)

# ----------------------------
# 3) Connexion entre nœuds
# ----------------------------
network.connect_nodes("node1", "node2", bandwidth=1000)


# ----------------------------
# 4) Démarrer GC threads pour chaque nœud
# ----------------------------
stop_events = {}
gc_threads = {}
for node in [node1, node2]:
    stop_event = threading.Event()
    stop_events[node.node_id] = stop_event
    t = start_node_gc_thread(node, GC_INTERVAL_SECONDS, stop_event)
    gc_threads[node.node_id] = t
    print(f"[MAIN] Started GC thread for {node.node_id}")


# ----------------------------
# 5) Lancer un transfert (exemple)
# ----------------------------
transfer = network.transfers.initiate_transfer(
    source_node_id="node1",
    target_node_id="node2",
    file_name="large_dataset.zip",
    file_size=100 * 1024 * 1024  # 100 MB
)

if not transfer:
    print("❌ Failed to initiate transfer (insufficient resources or network error)")
    # stop GC threads before exit
    for e in stop_events.values():
        e.set()
    sys.exit(1)

print(f"\n🚀 Transfer initiated: {transfer.file_name} (id: {transfer.file_id})\n")


# ----------------------------
# 6) Thread d'input utilisateur (commande interactive)
#    - commandes supportées :
#       cleanup   -> force cleanup pour chaque nœud maintenant
#       history   -> affiche transfer_history.json
#       status    -> montre stats réseau / node2
#       quit/exit -> stoppe proprement
# ----------------------------
def user_input_thread(stop_event: threading.Event):
    """
    Poll console input sans bloquer la boucle principale. Fonctionne en thread.
    """
    print("[MAIN] Input commands: cleanup | history | status | quit")
    while not stop_event.is_set():
        try:
            # Utiliser input() bloque le thread; c'est acceptable car c'est dans un thread dédié.
            cmd = input().strip().lower()
            if cmd == "cleanup":
                # Appel manuel du cleanup sur tous les nœuds
                for node in [node1, node2]:
                    removed = node.cleanup(max_age_seconds=60*60)  # supprime > 1h
                    print(f"[MANUAL-CLEANUP] Node {node.node_id} removed transfers: {removed}")
            elif cmd == "history":
                hist = persistence.get_history()
                print(f"[HISTORY] {len(hist)} records")
                print(json.dumps(hist, indent=2))
            elif cmd == "status":
                stats = network.get_network_stats()
                node2_stats = node2.get_storage_utilization()
                print("[STATUS] Network:", stats)
                print("[STATUS] Node2:", node2_stats)
            elif cmd in ("quit", "exit"):
                print("[MAIN] Exit requested by user.")
                stop_event.set()
            else:
                if cmd:
                    print("[MAIN] Unknown command:", cmd)
        except EOFError:
            # Ctrl-D on some terminals
            stop_event.set()
        except Exception as e:
            print("[MAIN] Input thread error:", e)
            stop_event.set()


input_stop = threading.Event()
input_thread = threading.Thread(target=user_input_thread, args=(input_stop,), daemon=True, name="input-thread")
input_thread.start()


# ----------------------------
# 7) Boucle principale de simulation
# ----------------------------
try:
    while True:
        chunks_done, completed = network.transfers.process_transfer(
            source_node_id="node1",
            target_node_id="node2",
            file_id=transfer.file_id,
            chunks_per_step=3
        )

        # affichage périodique des stats
        net_stats = network.get_network_stats()
        node2_stats = node2.get_storage_utilization()
        print(f"📦 Transferred chunks: {chunks_done} | Network utilization: {net_stats['bandwidth_utilization']:.2f}% | Node2 storage: {node2_stats['utilization_percent']:.2f}% | CPU {node2_stats['cpu_usage_percent']}%")

        if completed:
            print("✅ Transfer completed successfully!")
            break

        # check if user requested exit via input thread
        if input_stop.is_set():
            print("[MAIN] Stopping simulation due to user request.")
            break

        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n[MAIN] KeyboardInterrupt received. Shutting down...")

finally:
    # demande d'arrêt des background threads
    input_stop.set()
    for ev in stop_events.values():
        ev.set()

    # petite attente pour laisser threads se terminer proprement (daemon threads s'arrêtent à la sortie)
    time.sleep(0.5)
    print("[MAIN] Exiting.")
    sys.exit(0)
