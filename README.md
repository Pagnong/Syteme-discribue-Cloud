# Simulation de Cloud Storage Distribué

Ce projet implémente une **simulation de système de stockage distribué** avec tolérance aux pannes et réplication de fichiers. Il permet de lancer :

- **Un contrôleur de réseau** qui gère les nœuds de stockage, la réplication et l’état du cluster.
- **Plusieurs nœuds de stockage virtuels** qui stockent des fichiers, communiquent avec le contrôleur et entre eux.
- Des **démos** pour illustrer la tolérance aux pannes et le transfert parallèle.

La communication réseau est basée sur des sockets Python classiques, et une partie du projet utilise également **gRPC** (fichiers `cloud_storage.proto`, `cloud_storage_pb2.py`, `cloud_storage_pb2_grpc.py`).

---

## 1. Prérequis

- **Python 3.8+** (recommandé)
- `pip` installé

Installation des dépendances :

```bash
pip install -r requirements.txt
```

Le fichier `requirements.txt` contient notamment :

- `grpcio`
- `grpcio-tools`
- `protobuf`

---

## 2. Structure principale du projet

Fichiers importants :

- **`main.py`** : point d’entrée principal pour lancer
  - le **contrôleur de réseau**, ou
  - un **nœud de stockage**.
- **`storage_virtual_network.py`** : implémentation du contrôleur de réseau (gestion des nœuds, réplication, suivi des fichiers, etc.).
- **`storage_virtual_node.py`** : implémentation d’un nœud de stockage virtuel (création de fichiers, upload/download, stockage local, métriques, etc.).
- **`network_identity.py`** : génération automatique d’adresse IP et MAC pour chaque nœud.
- **`cloud_storage.proto` / `cloud_storage_pb2*.py`** : définition et code généré pour l’API gRPC.
- **`demo_fault_tolerance.py`** : démonstration de la tolérance aux pannes (réplication des fichiers entre nœuds).
- **`demo_parallel_transfer.py`** : démonstration de transferts parallèles.

---

## 3. Lancer le contrôleur de réseau

Depuis la racine du projet :

```bash
python main.py --network --host 0.0.0.0 --network-port 5000
```

Paramètres principaux :

- `--network` : lance le contrôleur de réseau.
- `--host` : adresse d’écoute (par défaut `0.0.0.0`).
- `--network-port` : port du contrôleur (par défaut `5000`).

Le contrôleur affiche qu’il est démarré et attend les connexions des nœuds.

---

## 4. Lancer un nœud de stockage

Dans un **autre terminal**, lancer un nœud :

```bash
python main.py \
  --node \
  --node-id node1 \
  --cpu 4 \
  --memory 16 \
  --storage 500 \
  --bandwidth 1000 \
  --network-host localhost \
  --network-port 5000
```

Paramètres principaux :

- `--node` : lance un nœud de stockage.
- `--node-id` : identifiant unique du nœud (`node1`, `node2`, etc.).
- `--cpu` : capacité CPU (valeur arbitraire).
- `--memory` : mémoire (en Go, ex. `16`).
- `--storage` : capacité de stockage (en Go, ex. `500`).
- `--bandwidth` : bande passante (en Mbps).
- `--network-host` / `--network-port` : adresse du contrôleur de réseau.

À chaque démarrage :

- Le nœud obtient un **port unique** pour son service.
- Une **IP et une adresse MAC** sont générées automatiquement via `NetworkIdentity`.
- Le nœud se **registre auprès du contrôleur** et envoie régulièrement des **heartbeats**.

Le nœud démarre ensuite une **interface interactive** dans le terminal.

---

## 5. Interface interactive d’un nœud

Une fois le nœud lancé, vous voyez un prompt du type :

```text
[Node node1]> 
```

Commandes disponibles :

- `create <filename> <contenu>`
  - Crée un fichier localement sur le nœud.
- `upload <filename>`
  - Upload le fichier vers le cloud distribué via le contrôleur (avec réplication sur plusieurs nœuds).
- `download <filename>`
  - Télécharge un fichier depuis le cloud vers ce nœud (avec mécanismes de tolérance aux pannes).
- `list`
  - Liste les fichiers stockés localement.
- `cloud_files`
  - Liste les fichiers connus dans le cloud (via le contrôleur de réseau).
- `status`
  - Affiche l’état du nœud :
    - utilisation du stockage,
    - utilisation réseau simulée,
    - nombre de fichiers,
    - transferts actifs.
- `network_status`
  - Affiche l’état réseau du nœud :
    - adresse du contrôleur,
    - port de service du nœud,
    - **adresse IP et MAC automatiques du nœud**, par exemple :
      ```text
      [Node node1] Network Status:
        Connected to controller: localhost:5000
        Node service port: 60xx
        IP: 10.0.x.y
        MAC: aa:bb:cc:dd:ee:ff
        Replication enabled: YES
      ```
- `metrics`
  - Affiche des métriques de performance (requêtes traitées, volume de données transféré, transferts en cours, etc.).
- `help`
  - Rappelle la liste des commandes.
- `quit`
  - Arrête proprement le nœud.

---

## 6. Démos fournies

### 6.1. Tolérance aux pannes (`demo_fault_tolerance.py`)

Ce script montre comment le système continue à fonctionner même si certains nœuds tombent en panne, grâce à la **réplication** des fichiers.

Exécution (exemple) :

```bash
python demo_fault_tolerance.py
```

(Consultez le code du script pour voir le scénario exact : création de nœuds, upload de fichiers, arrêt de nœuds, etc.)

### 6.2. Transfert parallèle (`demo_parallel_transfer.py`)

Ce script montre comment plusieurs fichiers peuvent être transférés en parallèle dans le réseau de stockage.

Exécution :

```bash
python demo_parallel_transfer.py
```

---

## 7. Utilisation de gRPC (optionnel)

Les fichiers :

- `cloud_storage.proto`
- `cloud_storage_pb2.py`
- `cloud_storage_pb2_grpc.py`

fournissent une base pour une interface gRPC de cloud storage (définition de services, messages, etc.). 

Si vous modifiez le fichier `.proto`, vous pouvez régénérer les fichiers Python avec :

```bash
python -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  cloud_storage.proto
```

---

## 8. Arrêt des composants

- Pour **arrêter un nœud** :
  - Dans l’interface interactive : taper `quit`, ou
  - Utiliser `Ctrl+C` dans le terminal du nœud.

- Pour **arrêter le contrôleur de réseau** :
  - Utiliser `Ctrl+C` dans le terminal où il tourne.

Le projet est conçu comme un **simulateur éducatif** pour illustrer :

- la gestion d’un réseau de nœuds de stockage,
- la réplication et la tolérance aux pannes,
- la gestion d’un contrôleur centralisé,
- la mesure simple de performances et de l’état du système.
