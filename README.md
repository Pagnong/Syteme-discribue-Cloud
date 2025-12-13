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
 - `Flask`

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
 - **`fault_tolerant_client.py`** : client de téléchargement tolérant aux pannes utilisé par la démo de tolérance aux pannes.
 - **`parallel_transfer.py`** : gestionnaire de transferts parallèles et simulateur réseau utilisés par la démo de transferts parallèles.
- **`web_app.py`** : application web Flask offrant une interface graphique pour visualiser l’état du réseau et des nœuds.
- **`templates/`** : pages HTML (`index.html`, `network.html`, `node_files.html`, etc.) utilisées par l’interface web.

### Interface web graphique (Flask)

L’interface web permet de **piloter et visualiser la simulation** dans un navigateur web.

Pour la lancer :

```bash
python web_app.py
```

Puis ouvrir dans un navigateur : `http://localhost:8000`.

Fonctionnalités principales :

- **Page d’accueil (`/`)** : accès général à la simulation.
- **Vue réseau (`/network`)** :
  - démarrer le contrôleur réseau via l’API (`/api/network/start`),
  - voir l’état global du contrôleur et la liste des nœuds/fichiers (`/api/network/full-status`).
- **Vue fichiers d’un nœud (`/nodes/<node_id>/files`)** :
  - lister les fichiers locaux et dans le cloud pour un nœud,
  - créer, uploader, télécharger, supprimer des fichiers,
  - gérer le disque virtuel (format/resize) via les endpoints `/api/nodes/...`.

Cette interface graphique repose sur l’API REST exposée par `web_app.py` (endpoints `/api/network/*` et `/api/nodes/*`).

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
  --disk-mb 250 \
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
- `--disk-mb` : taille du **disque virtuel local** par nœud (en Mo, ex. `250`, `1024`, etc.).
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
    - **taille du disque virtuel local** (en Mo),
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
- `gestion_disk format`
  - Formate le **disque virtuel local** du nœud (fichier image), en réinitialisant l’espace disponible et les fichiers locaux enregistrés sur ce nœud.
- `gestion_disk resize <taille_mb>`
  - Redéfinit la taille (en Mo) du **disque virtuel local** du nœud et reformate le disque à cette nouvelle taille.
- `help`
  - Rappelle la liste des commandes.
- `quit`
  - Arrête proprement le nœud.

---

### 5.1. Persistance du disque virtuel local

Pour chaque nœud `nodeX`, le stockage local est géré via :

- un **fichier disque virtuel** : `node_nodeX_disk.img`,
- un **fichier de métadonnées JSON** : `node_nodeX_disk_meta.json`.

À chaque création ou téléchargement de fichier sur un nœud :

- les données sont écrites dans `node_nodeX_disk.img` à un offset donné,
- les métadonnées du fichier (nom, taille, checksum, offset, etc.) sont enregistrées dans `node_nodeX_disk_meta.json`,
- l’espace utilisé (`used_storage`) est mis à jour.

Lorsqu’un nœud est arrêté puis relancé avec le **même `--node-id`** :

- le nœud recharge les métadonnées depuis `node_nodeX_disk_meta.json`,
- il relit les données depuis `node_nodeX_disk.img` à partir des offsets enregistrés,
- les fichiers locaux redeviennent automatiquement accessibles (`list`, `upload`, etc.).

La persistance est donc **conservée entre deux exécutions** tant que :

- le disque n’a pas été formaté (`gestion_disk format`),
- ou redimensionné (`gestion_disk resize ...`).

Ces deux commandes réinitialisent l’image disque et le fichier de métadonnées : les fichiers locaux du nœud sont alors perdus pour les prochaines exécutions de ce nœud.

---

## 6. Démos fournies

### 6.1. Tolérance aux pannes (`demo_fault_tolerance.py`)

Ce script montre comment le système continue à fonctionner même si certains nœuds tombent en panne, grâce à la **réplication** des fichiers et à un **client de téléchargement tolérant aux pannes** (`FaultTolerantDownloadClient`).

Exécution (exemple) :

```bash
python demo_fault_tolerance.py
```

(Consultez le code du script pour voir le scénario exact : création de nœuds, upload de fichiers, arrêt de nœuds, téléchargements tolérants aux pannes, affichage de l’état de réplication, etc. Les fichiers sont marqués comme **dégradés** si le nombre de réplicas actifs devient insuffisant, et la **santé de réplication** est affichée.)

### 6.2. Transfert parallèle (`demo_parallel_transfer.py`)

Ce script montre comment plusieurs fichiers peuvent être transférés en parallèle dans le réseau de stockage, avec un **simulateur de réseau** (perte de paquets, latence, jitter) et un **gestionnaire de transferts parallèles** (`ParallelTransferManager`).

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

---

## 9. Contexte pédagogique du projet

Ce projet peut être utilisé comme **support de TP / mini-projet** dans un cours de :

- systèmes distribués,
- réseaux et communication inter-processus,
- stockage distribué et tolérance aux pannes,
- introduction au cloud computing.

Il illustre concrètement les notions suivantes :

- orchestration d’un ensemble de nœuds par un contrôleur central,
- enregistrement des nœuds et surveillance de leur état (heartbeats),
- réplication de données et détection de situations dégradées,
- gestion simple des ressources (stockage, bande passante simulée).

---

## 10. Objectifs d’apprentissage possibles

En travaillant sur ce projet, un étudiant peut :

- **Comprendre** la différence entre stockage local et stockage distribué.
- **Observer** comment un contrôleur de réseau peut suivre l’état de plusieurs nœuds.
- **Analyser** le comportement du système en cas de panne de nœud (perte d’un réplique, re-réplication).
- **Mettre en œuvre** des scénarios de transfert et de réplication de fichiers.
- **Explorer** une base pour exposer le système via une API gRPC.

Selon les consignes du TP/projet, l’enseignant peut demander :

- d’ajouter de nouveaux types de métriques,
- de modifier la stratégie de réplication,
- d’améliorer la sélection des nœuds de stockage,
- d’intégrer une vraie persistance disque ou une base de données,
- d’exposer des fonctionnalités supplémentaires en gRPC.

---

## 11. Vue d’ensemble de l’architecture

### 11.1. Composants principaux

- **Contrôleur de réseau (`StorageVirtualNetwork` / `NetworkController`)**
  - Accepte les enregistrements de nœuds (`REGISTER`).
  - Suit leur état via des heartbeats (`HEARTBEAT`).
  - Gère la réplication des fichiers et la file de demandes de réplication.
  - Décide sur quels nœuds placer ou répliquer un fichier.
  - Suit pour chaque fichier des informations de **santé de réplication** (réplicas actifs, statut disponible / dégradé, etc.).

- **Nœuds de stockage (`StorageVirtualNode`)**
  - Hébergent des fichiers localement.
  - Communiquent avec le contrôleur pour l’upload/download.
  - Transfèrent des fichiers entre eux lors de la réplication.
  - Maintiennent des métriques simples (nombre de requêtes, volume de données, etc.).

- **Identité réseau (`NetworkIdentity`, `NetworkInfo`)**
  - Génèrent une IP et une adresse MAC pour chaque nœud.
  - Permettent d’afficher, via `network_status`, une vue réseau du nœud.

### 11.2. Interactions principales

1. Démarrage du contrôleur de réseau.
2. Démarrage de plusieurs nœuds qui se **registrent** auprès du contrôleur.
3. Création de fichiers sur un nœud, puis **upload** vers le cloud (décision du contrôleur sur les nœuds de stockage).
4. Téléchargement (**download**) d’un fichier depuis n’importe quel nœud, en s’appuyant sur les répliques disponibles.
5. Éventuelle panne de nœud et re-réplication pour revenir au niveau de réplication souhaité.

---

## 12. Scénarios de test suggérés

Voici quelques scénarios possibles à décrire dans un rapport ou à tester en TP :

1. **Upload et download de base**
   - Lancer un contrôleur et deux nœuds.
   - Créer un fichier sur `node1`, l’uploader, puis le télécharger depuis `node2`.

2. **Tolérance aux pannes**
   - Lancer plusieurs nœuds, uploader un fichier avec réplication.
   - Arrêter un nœud contenant une réplique.
   - Observer le comportement du contrôleur (fichier marqué dégradé, re-réplication, etc.).

3. **Transferts parallèles**
   - Utiliser `demo_parallel_transfer.py` pour lancer plusieurs uploads/downloads.
   - Observer les métriques réseau et le temps de traitement.

4. **Mesure de performances** (simple)
   - Lancer une série de transferts et relever :
     - le nombre total de requêtes,
     - le volume de données transféré,
     - le temps de réponse approximatif.

---

## 13. Limitations actuelles et pistes d’amélioration

Ce projet reste une **simulation simplifiée**. Parmi les limitations possibles :

- Pas de véritable persistance disque robuste (pas de système de fichiers distribué réel).
- Pas de gestion fine des erreurs réseau, des partitions ou de la congestion.
- Stratégies de placement/réplication simplifiées.
- Sécurité, authentification et chiffrement absents.

Quelques pistes d’amélioration pour un projet avancé :

- Implémenter une **stratégie de placement intelligente** (prise en compte de la capacité restante, de la charge, de la localisation, etc.).
- Ajouter une **interface web** ou une API REST/gRPC complète pour gérer le cluster.
- Enregistrer des **logs structurés** et des métriques détaillées pour une analyse avec des outils externes.
- Simuler des latences et des débits réseau différents entre nœuds (topologie de réseau plus réaliste).
- Introduire des mécanismes de **consensus** ou de coordination plus avancés.

---

## 14. Auteurs et informations

- **Nom** : PAGNONG FREDY
- **Date** : Novembre 2025
