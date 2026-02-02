"""
Routes pour l'onglet Audit GTFS
"""
from flask import Blueprint, render_template, request, jsonify, session, make_response
from services.audit_engine import AuditEngine
from models.project import Project
import pandas as pd
import io
from models.audit_result import AuditResult  # ← Ajoutez cette ligne
from datetime import datetime
import json

audit_progress = {}

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
    
    return render_template('audit.html', 
                         project=project,
                         gtfs_info=gtfs_info,
                         gtfs_files=GTFSHandler.GTFS_FILES)

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
        return jsonify(results)
        
    except Exception as e:
        return jsonify({"error": f"Erreur lors de l'audit: {str(e)}"}), 500
    
    
@audit_bp.route('/audit/progress/<file_type>')
def get_audit_progress(file_type):
    """Récupère la progression d'un audit en cours"""
    current_project_id = session.get('current_project_id')
    
    if not current_project_id:
        return jsonify({"error": "Aucun projet sélectionné"}), 400
    
    progress_key = f"{current_project_id}_{file_type}"
    progress_data = audit_progress.get(progress_key, {
        'progress': 0,
        'message': 'En attente...',
        'step': 'waiting'
    })
    
    return jsonify(progress_data)

@audit_bp.route('/audit/run-all', methods=['POST'])
def run_all_audits():
    """Lance tous les audits disponibles"""
    current_project_id = session.get('current_project_id')
    
    if not current_project_id:
        return jsonify({"error": "Aucun projet sélectionné"}), 400
    
    try:
        # Lancer tous les audits
        audit_engine = AuditEngine()
        results = audit_engine.run_all_audits(current_project_id, save_to_db=True)
        
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
    
@audit_bp.route('/audit/export-csv')
def export_audit_csv():
    """Exporte les résultats d'audit au format CSV avec JSON complet"""
    #current_project_id = session.get('current_project_id')
    
    #if not current_project_id:
    #    return jsonify({"error": "Aucun projet sélectionné"}), 400
    
    try:
        #audit_results = AuditResult.query.filter_by(project_id=current_project_id).all()
        audit_results = AuditResult.query.all()
        
        if not audit_results:
            return jsonify({"error": "Aucun résultat d'audit trouvé"}), 404
        
        data = []
        for result in audit_results:
            row = {
                'result_id': result.result_id,
                'project_id': result.project_id,
                'date_audit': result.date_audit.isoformat() if result.date_audit else None,
                'agency_audit': json.dumps(result.agency_audit) if result.agency_audit else None,
                'routes_audit': json.dumps(result.routes_audit) if result.routes_audit else None,
                'trips_audit': json.dumps(result.trips_audit) if result.trips_audit else None,
                'stops_audit': json.dumps(result.stops_audit) if result.stops_audit else None,
                'stop_times_audit': json.dumps(result.stop_times_audit) if result.stop_times_audit else None,
                'calendar_audit': json.dumps(result.calendar_audit) if result.calendar_audit else None,
                'calendar_dates_audit': json.dumps(result.calendar_dates_audit) if result.calendar_dates_audit else None,
                'statistiques': json.dumps(result.statistiques) if result.statistiques else None,
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"audit_results_{timestamp}.csv"
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        
        return response
        
    except Exception as e:
        return jsonify({"error": f"Erreur: {str(e)}"}), 500
    
@audit_bp.route('/audit/db-status')
def get_db_audit_status():
    """Vérifie s'il y a des résultats d'audit en base pour le projet courant"""
    current_project_id = session.get('current_project_id')
    
    if not current_project_id:
        return jsonify({"error": "Aucun projet sélectionné"}), 400
    
    try:
        # Compter les résultats en base
        count = AuditResult.query.filter_by(project_id=current_project_id).count()
        
        # Récupérer le dernier audit
        latest_audit = AuditResult.query.filter_by(project_id=current_project_id)\
                                       .order_by(AuditResult.date_audit.desc())\
                                       .first()
        
        return jsonify({
            "has_results": count > 0,
            "total_audits": count,
            "latest_audit": latest_audit.date_audit.isoformat() if latest_audit else None
        })
        
    except Exception as e:
        return jsonify({"error": f"Erreur: {str(e)}"}), 500


# Ajoutez cette route à la fin de votre fichier audit.py

@audit_bp.route('/audit/<project_id>/report/pdf')
def generate_pdf_report(project_id):
    """Génère un rapport PDF complet pour un projet"""
    try:
        # Récupérer le dernier audit pour ce projet
        latest_audit = AuditResult.query.filter_by(project_id=project_id)\
                                       .order_by(AuditResult.date_audit.desc())\
                                       .first()
        
        if not latest_audit:
            return jsonify({"error": "Aucun audit trouvé pour ce projet"}), 404
        
        # Récupérer les informations du projet
        project = Project.query.filter_by(project_id=project_id).first()
        if not project:
            return jsonify({"error": "Projet non trouvé"}), 404
        
        # Générer le PDF
        from services.pdf_generator import PDFReportGenerator
        pdf_generator = PDFReportGenerator()
        
        pdf_buffer = pdf_generator.generate_audit_report(latest_audit, project)
        
        # Créer la réponse
        response = make_response(pdf_buffer.getvalue())
        response.headers['Content-Type'] = 'application/pdf'
        
        timestamp = latest_audit.date_audit.strftime('%Y%m%d_%H%M%S')
        filename = f"rapport_audit_{project.nom_projet}_{timestamp}.pdf"
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        
        return response
        
    except Exception as e:
        return jsonify({"error": f"Erreur génération PDF: {str(e)}"}), 500