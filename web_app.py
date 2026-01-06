from flask import Flask, jsonify, request, render_template
import tempfile
import os
from threading import Lock

from storage_virtual_node import StorageVirtualNode
from storage_virtual_network import StorageVirtualNetwork

app = Flask(__name__)

# Global state for web simulation
network = None
network_lock = Lock()
nodes = {}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/network")
def network_page():
    return render_template("network.html")


@app.route("/nodes/<node_id>/files")
def node_files_page(node_id):
    if node_id not in nodes:
        # Simple 404 page
        return f"Noeud {node_id} introuvable", 404
    return render_template("node_files.html", node_id=node_id)


@app.route("/api/network/start", methods=["POST"])
def start_network():
    global network
    with network_lock:
        if network is None:
            host = request.json.get("host", "localhost") if request.is_json else "localhost"
            port = request.json.get("port", 5000) if request.is_json else 5000
            network = StorageVirtualNetwork(host=host, port=port)
            return jsonify({"status": "started", "host": host, "port": port})
        else:
            return jsonify({"status": "already_running"})


@app.route("/api/network/status", methods=["GET"])
def network_status():
    if network is None:
        return jsonify({"running": False})
    controller = network.controller
    return jsonify({
        "running": controller.running,
        "host": controller.host,
        "port": controller.port,
        "nodes": list(controller.nodes.keys()),
    })


@app.route("/api/network/full-status", methods=["GET"])
def network_full_status():
    if network is None:
        return jsonify({"running": False, "nodes": [], "files": []})
    controller = network.controller
    files = network.list_files()
    nodes_info = []
    with controller.lock:
        for node_id, info in controller.nodes.items():
            nodes_info.append({
                "id": node_id,
                "host": info.get("host"),
                "port": info.get("port"),
                "status": info.get("status", "unknown"),
            })
    return jsonify({
        "running": controller.running,
        "host": controller.host,
        "port": controller.port,
        "nodes": nodes_info,
        "files": files,
    })


@app.route("/api/nodes", methods=["GET"])
def list_nodes():
    result = []
    for node_id, node in nodes.items():
        storage_info = node.get_storage_utilization()
        network_info = node.get_network_utilization()
        result.append({
            "node_id": node_id,
            "cpu_capacity": node.cpu_capacity,
            "memory_capacity": node.memory_capacity,
            # Expose virtual storage as the local virtual disk size (total_bytes)
            "storage_capacity_bytes": storage_info.get("total_bytes", 0),
            # Expose declared local capacity (as entered) in GB for details page
            "declared_local_capacity_gb": getattr(node, 'declared_local_capacity_gb', None),
            "bandwidth": node.bandwidth,
            "network_host": node.network_host,
            "network_port": node.network_port,
            "service_port": node.service_port,
            "storage": storage_info,
            "network": network_info,
        })
    return jsonify(result)


@app.route("/api/nodes", methods=["POST"])
def create_node():
    if network is None:
        return jsonify({"error": "network_not_started"}), 400

    data = request.get_json(force=True)
    node_id = data.get("node_id")
    if not node_id:
        return jsonify({"error": "node_id_required"}), 400
    if node_id in nodes:
        return jsonify({"error": "node_already_exists"}), 400

    cpu = int(data.get("cpu", 4))
    memory = int(data.get("memory", 16))
    storage = int(data.get("storage", 500))
    bandwidth = int(data.get("bandwidth", 1000))
    disk_mb = int(data.get("disk_mb", 250))

    # On utilise le même host/port que le contrôleur réseau
    network_host = data.get("network_host", network.controller.host)
    network_port = int(data.get("network_port", network.controller.port))

    # Choix simple de port : basé sur la taille actuelle du dict + offset
    base_port = 6000 + len(nodes)

    node = StorageVirtualNode(
        node_id=node_id,
        cpu_capacity=cpu,
        memory_capacity=memory,
        storage_capacity=storage,
        bandwidth=bandwidth,
        network_host=network_host,
        network_port=network_port,
        port=base_port,
        disk_size_mb=disk_mb,
    )

    # Sauvegarder la capacité locale déclarée (en GB) séparément pour l'affichage
    try:
        setattr(node, 'declared_local_capacity_gb', storage)
    except Exception:
        pass

    nodes[node_id] = node

    return jsonify({
        "status": "created",
        "node_id": node_id,
        "port": base_port,
    }), 201


@app.route("/api/nodes/<node_id>", methods=["GET"])
def get_node_details(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404

    storage_info = node.get_storage_utilization()
    network_info = node.get_network_utilization()
    metrics = node.get_performance_metrics()

    return jsonify({
        "node_id": node.node_id,
        "cpu_capacity": node.cpu_capacity,
        "memory_capacity": node.memory_capacity,
        # Virtual storage capacity = local disk size (total_bytes)
        "storage_capacity_bytes": storage_info.get("total_bytes", 0),
        # Declared local capacity entered by user (GB)
        "declared_local_capacity_gb": getattr(node, 'declared_local_capacity_gb', None),
        "bandwidth": node.bandwidth,
        "network_host": node.network_host,
        "network_port": node.network_port,
        "service_port": node.service_port,
        "storage": storage_info,
        "network": network_info,
        "metrics": metrics,
    })


@app.route("/api/nodes/<node_id>/files/local", methods=["GET"])
def api_node_local_files(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    return jsonify(node.list_local_files())


@app.route("/api/nodes/<node_id>/files/cloud", methods=["GET"])
def api_node_cloud_files(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    return jsonify(node.list_cloud_files())


@app.route("/api/nodes/<node_id>/files/create", methods=["POST"])
def api_node_create_file(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    data = request.get_json(force=True)
    filename = data.get("filename")
    content = data.get("content", "")
    if not filename:
        return jsonify({"error": "filename_required"}), 400
    ok = node.create_file(filename, content)
    return jsonify({"status": "ok" if ok else "failed"}), (200 if ok else 500)


@app.route("/api/nodes/<node_id>/files/upload", methods=["POST"])
def api_node_upload_file(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    data = request.get_json(force=True)
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "filename_required"}), 400
    ok = node.upload_file(filename)
    return jsonify({"status": "ok" if ok else "failed"}), (200 if ok else 500)


@app.route("/api/nodes/<node_id>/files/download", methods=["POST"])
def api_node_download_file(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    data = request.get_json(force=True)
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "filename_required"}), 400
    # Vérifier que le fichier est présent dans le cloud (réseau) avant de lancer le download
    try:
        cloud_files = node.list_cloud_files()
    except Exception:
        cloud_files = []
    if not any(f.get('file_name') == filename for f in cloud_files):
        return jsonify({
            "error": "file_not_in_cloud",
            "message": "Le fichier n'est pas disponible dans le cloud. Lors de l'import, cochez 'Enregistrer dans le cloud', puis rafraîchissez la liste des fichiers cloud."
        }), 400

    # Vérifier l'espace disque local disponible avant le téléchargement
    try:
        file_entry = next((f for f in cloud_files if f.get('file_name') == filename), None)
        target_size = int(file_entry.get('file_size', 0)) if file_entry else 0
    except Exception:
        target_size = 0
    storage_info = node.get_storage_utilization()
    available = int(storage_info.get('available_bytes', 0))
    if target_size and target_size > available:
        return jsonify({
            "error": "insufficient_space",
            "message": f"Espace disque local insuffisant pour télécharger '{filename}'. Requis: {target_size} bytes, disponible: {available} bytes. Augmentez la taille du disque virtuel ou libérez de l'espace.",
            "required_bytes": target_size,
            "available_bytes": available
        }), 400

    ok = node.download_file(file_name=filename)
    if ok:
        return jsonify({"status": "ok"}), 200
    else:
        return jsonify({
            "status": "failed",
            "message": "Téléchargement impossible depuis le cloud. Vérifiez l'état du contrôleur réseau et des nœuds réplicas."
        }), 500


@app.route("/api/nodes/<node_id>/files/delete", methods=["POST"])
def api_node_delete_file(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    data = request.get_json(force=True)
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "filename_required"}), 400
    ok = node.delete_file(filename)
    return jsonify({"status": "ok" if ok else "failed"}), (200 if ok else 500)


@app.route("/api/nodes/<node_id>/files/import", methods=["POST"])
def api_node_import_file(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    # Expect multipart/form-data with 'file', optional 'cloud_name', optional 'register' (y/n)
    if 'file' not in request.files:
        return jsonify({"error": "file_required"}), 400
    f = request.files['file']
    if f.filename == '':
        return jsonify({"error": "empty_filename"}), 400

    cloud_name = request.form.get('cloud_name') or None
    # Désactiver l'enregistrement cloud par défaut: n'enregistrer que si 'register' == 'y'
    register_flag = (request.form.get('register', 'n').lower() == 'y')

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
            f.save(tmp)

        file_id = node.import_local_file(
            local_path=tmp_path,
            cloud_file_name=cloud_name or f.filename,
            upload_to_cloud=register_flag
        )
        if not file_id:
            return jsonify({"status": "failed"}), 500
        return jsonify({
            "status": "ok",
            "file_id": file_id,
            "registered": register_flag
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

@app.route("/api/nodes/<node_id>/disk/format", methods=["POST"])
def api_node_disk_format(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    ok = node.format_disk()
    return jsonify({"status": "ok" if ok else "failed"}), (200 if ok else 500)


@app.route("/api/nodes/<node_id>/disk/resize", methods=["POST"])
def api_node_disk_resize(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404
    data = request.get_json(force=True)
    try:
        new_size_mb = int(data.get("size_mb"))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_size"}), 400
    ok = node.resize_disk(new_size_mb)
    return jsonify({"status": "ok" if ok else "failed"}), (200 if ok else 500)


@app.route("/api/nodes/<node_id>/shutdown", methods=["POST"])
def shutdown_node(node_id):
    node = nodes.get(node_id)
    if node is None:
        return jsonify({"error": "node_not_found"}), 404

    node.shutdown()
    del nodes[node_id]
    return jsonify({"status": "shut_down", "node_id": node_id})


if __name__ == "__main__":
    # Démarrage simple pour le développement
    app.run(host="0.0.0.0", port=8000, debug=True)
