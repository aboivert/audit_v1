"""
Routes pour l'onglet Audit GTFS
"""
from flask import Blueprint, render_template, request, jsonify, session
from services.audit_engine import AuditEngine
from models.project import Project

audit_bp = Blueprint('audit', __name__)

@audit_bp.route('/audit')
def audit_page():
    """Page principale de l'audit"""
    current_project_id = session.get('current_project_id')
    
    if not current_project_id:
        return render_template('audit.html', 
                             error="Aucun projet sélectionné")
    
    # Récupérer les informations du projet
    project = Project.query.filter_by(project_id=current_project_id).first()
    if not project:
        return render_template('audit.html', 
                             error="Projet non trouvé")
    
    # Vérifier si des données GTFS existent
    from services.gtfs_handler import GTFSHandler
    if not GTFSHandler.has_gtfs_data(current_project_id):
        return render_template('audit.html', 
                             error="Aucune donnée GTFS disponible pour ce projet",
                             project=project)
    
    # Récupérer les informations GTFS disponibles
    gtfs_info = GTFSHandler.get_gtfs_info(current_project_id)
    
    # Filtrer les fichiers à auditer (exclure feed_info.txt)
    auditable_files = [f for f in GTFSHandler.GTFS_FILES if f != 'feed_info.txt']
    
    return render_template('audit.html', 
                         project=project,
                         gtfs_info=gtfs_info,
                         gtfs_files=auditable_files)

@audit_bp.route('/audit/run', methods=['POST'])
def run_audit():
    """Lance un audit sur un fichier spécifique"""
    current_project_id = session.get('current_project_id')
    
    if not current_project_id:
        return jsonify({"error": "Aucun projet sélectionné"}), 400
    
    file_type = request.json.get('file_type')
    if not file_type:
        return jsonify({"error": "Type de fichier non spécifié"}), 400
    
    try:
        # Lancer l'audit
        audit_engine = AuditEngine()
        results = audit_engine.run_file_audit(current_project_id, file_type)
        
        # Nettoyer les résultats pour JSON avant de les retourner
        def clean_for_json(obj):
            """Nettoie récursivement pour JSON"""
            import pandas as pd
            import numpy as np
            
            if isinstance(obj, dict):
                return {key: clean_for_json(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [clean_for_json(item) for item in obj]
            elif isinstance(obj, (np.integer, pd.Int64Dtype)):
                return int(obj)
            elif isinstance(obj, (np.floating, pd.Float64Dtype)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif pd.isna(obj):
                return None
            elif hasattr(obj, 'item'):  # numpy scalars
                return obj.item()
            else:
                return obj
        
        clean_results = clean_for_json(results)
        
        # Test final de sérialisation avant retour
        try:
            import json
            json.dumps(clean_results)
            print(f"✅ JSON final validé pour {file_type}")
        except Exception as e:
            print(f"❌ Erreur JSON finale: {e}")
            return jsonify({"error": f"Erreur de sérialisation JSON: {str(e)}"}), 500
        
        return jsonify(clean_results)
        
    except Exception as e:
        return jsonify({"error": f"Erreur lors de l'audit: {str(e)}"}), 500

@audit_bp.route('/audit/run-all', methods=['POST'])
def run_all_audits():
    """Lance tous les audits disponibles"""
    current_project_id = session.get('current_project_id')
    
    if not current_project_id:
        return jsonify({"error": "Aucun projet sélectionné"}), 400
    
    try:
        # Lancer tous les audits
        audit_engine = AuditEngine()
        results = audit_engine.run_all_audits(current_project_id)
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": f"Erreur lors des audits: {str(e)}"}), 500

@audit_bp.route('/audit/results/<file_type>')
def get_audit_results(file_type):
    """Récupère les résultats d'audit pour un fichier"""
    current_project_id = session.get('current_project_id')
    
    if not current_project_id:
        return jsonify({"error": "Aucun projet sélectionné"}), 400
    
    try:
        # TODO: Récupérer depuis la base de données quand implémentée
        audit_engine = AuditEngine()
        results = audit_engine.get_cached_results(current_project_id, file_type)
        
        if results:
            return jsonify(results)
        else:
            return jsonify({"message": "Aucun résultat trouvé"}), 404
            
    except Exception as e:
        return jsonify({"error": f"Erreur: {str(e)}"}), 500