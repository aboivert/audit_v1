"""
Routes pour la gestion des audits
"""

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from services.audit_service import AuditService
from services.statistics_service import StatisticsService
from services.gtfs_handler import GTFSHandler
from models.audit_config import AuditConfig
from models.audit_result import AuditResult
from models.project import db
import numpy as np
import csv
from io import StringIO
import json
from flask import make_response
from datetime import datetime, date
from decimal import Decimal

def sanitize_for_json(obj):
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, (np.bool_, bool)):  # ADD THIS LINE
        return bool(obj)  # ADD THIS LINE
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, bytes):
        return obj.decode()
    raise TypeError(f"🔥 Type {type(obj)} not serializable")


audit_bp = Blueprint('audit', __name__)

@audit_bp.route('/audit')
def audit_page():
    """Page principale d'audit"""
    if not session.get('gtfs_loaded'):
        flash('Veuillez d\'abord charger un fichier GTFS', 'warning')
        return redirect(url_for('main.index'))
    
    audit_service = AuditService()
    available_audits = audit_service.get_available_audits()
    
    latest_config = None
    current_project = session.get('current_project')
    
    if current_project:
        latest_config = AuditConfig.query.filter_by(
            project_id=current_project['id']
        ).order_by(AuditConfig.date_configuration.desc()).first()
    
    return render_template('audit.html', 
                         audits=available_audits, 
                         gtfs_info=session.get('gtfs_info', {}),
                         latest_config=latest_config)

@audit_bp.route('/api/audit/run', methods=['POST'])
def run_audit():
    """API pour exécuter un audit"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    data = request.get_json()
    file_type = data.get('file_type')
    function_name = data.get('function_name')
    parameters = data.get('parameters', {})
    
    # Recharger les données GTFS
    #gtfs_handler = GTFSHandler()
    #gtfs_data = gtfs_handler.extract_and_load_gtfs(session['gtfs_data_path'])
    project_id = session.get('current_project_id')
    gtfs_data = GTFSHandler.get_gtfs_data(project_id)
    if not gtfs_data:
        return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
    
    # Exécuter l'audit
    audit_service = AuditService()
    score, problem_ids = audit_service.run_audit(file_type, function_name, gtfs_data, parameters)
    
    if score is None:
        return jsonify({'error': 'Erreur lors de l\'exécution de l\'audit'}), 500
    
    return jsonify({
        'success': True,
        'score': round(score, 2),
        'problem_ids': problem_ids,
        'problem_count': len(problem_ids)
    })

@audit_bp.route('/run_configured_audit', methods=['POST'])
def run_configured_audit():
    """Lance l'audit selon la configuration la plus récente et enregistre les résultats"""
    try:
        # Vérifier qu'un projet est sélectionné
        current_project = session.get('current_project')
        if not current_project:
            return jsonify({'success': False, 'error': 'Aucun projet sélectionné'})
        
        # Vérifier qu'un GTFS est chargé
        if not session.get('gtfs_loaded'):
            return jsonify({'success': False, 'error': 'Aucun GTFS chargé'})
        
        # Récupérer la configuration la plus récente
        latest_config = AuditConfig.query.filter_by(
            project_id=current_project['id']
        ).order_by(AuditConfig.date_configuration.desc()).first()
        
        if not latest_config:
            return jsonify({'success': False, 'error': 'Aucune configuration trouvée'})
        
        # Recharger les données GTFS
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Préparer le dictionnaire des résultats avec NaN par défaut
        results = {}
        
        # Récupérer toutes les colonnes score_ du modèle AuditResult
        result_columns = [column.name for column in AuditResult.__table__.columns 
                         if column.name.startswith('score_') or column.name.startswith('stats_')]
        
        # Initialiser tous les scores à NaN
        for column in result_columns:
            results[column] = np.nan
        
        # Parcourir les fonctions configurées
        config_dict = latest_config.to_dict()
        audit_service = AuditService()
        statistics_service = StatisticsService()

        executed_count = 0

        for config_key, is_enabled in config_dict.items():
            if config_key.startswith('conf_') and is_enabled:
                # Extraire file_type et function_name depuis le nom de la configuration
                # conf_agency_completion_phone -> file_type='agency', function_name='completion_phone'
                config_parts = config_key[5:].split('_', 1)  # Supprime 'conf_' et split sur le premier '_'
                
                if len(config_parts) == 2:
                    file_type, function_name = config_parts
                    
                    try:
                        # Exécuter la fonction d'audit via AuditService
                        score, problem_ids = audit_service.run_audit(file_type, function_name, gtfs_data, {})
                        
                        if score is not None:
                            # Enregistrer le score
                            score_column = f'score_{file_type}_{function_name}'
                            if score_column in results:
                                results[score_column] = score
                            executed_count += 1
                            print(f"Audit {file_type}_{function_name} exécuté: {score}")
                        
                    except Exception as e:
                        print(f"Erreur lors de l'exécution de {file_type}_{function_name}: {str(e)}")
                        # Garder NaN en cas d'erreur

        # Exécuter automatiquement toutes les fonctions stats_... définies dans AuditResult
        for column in AuditResult.__table__.columns:
            col_name = column.name
            if col_name.startswith('stats_'):
                try:
                    # Extraire file_type et function_name
                    parts = col_name[len('stats_'):].split('_', 1)
                    if len(parts) != 2:
                        continue  # Skip columns mal nommées
                    file_type, function_name = parts
                    
                    try:
                        result = statistics_service.run_statistic(file_type, function_name, gtfs_data, {})
                        executed_count += 1
                        if result is not None:
                            try:
                                safe_json = json.loads(json.dumps(result[0], default=sanitize_for_json))
                                stats_column = f'stats_{file_type}_{function_name}'
                                results[stats_column] = safe_json
                                print(f"[Stats] {function_name} exécutée.")
                            except Exception as e:
                                print(f"[Stats] Erreur de conversion JSON pour {function_name}: {e}")
                    except Exception as e:
                        print(f"Erreur lors de l'exécution de {file_type}_{function_name}: {str(e)}")

                except Exception as e:
                    print(f"[Stats] Erreur {col_name} : {str(e)}")
        
        # Créer l'entrée dans audit_results
        audit_result = AuditResult(
            project_id=current_project['id']
        )
        
        # Assigner tous les scores
        for column, value in results.items():
            if hasattr(audit_result, column):
                setattr(audit_result, column, value)
        
        # Sauvegarder en base
        db.session.add(audit_result)
        db.session.commit()
        
        # Nettoyer les results avant de les retourner
        clean_results = {}
        for k, v in results.items():
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                try:
                    # Tester la sérialisation JSON
                    json.dumps(v, default=sanitize_for_json)
                    clean_results[k] = v
                except:
                    clean_results[k] = "Non-serializable"
            else:
                clean_results[k] = None
        
        return jsonify({
            'success': True, 
            'message': f'Audit terminé. {executed_count} fonction(s) exécutée(s).',
            'results': clean_results
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'})

    

@audit_bp.route('/export_audits', methods=['GET'])
def export_audits():
    """Exporte l'historique des audits du projet actif en CSV"""
    try:
        # Vérifier qu'un projet est sélectionné
        current_project = session.get('current_project')
        if not current_project:
            flash('Aucun projet sélectionné', 'error')
            return redirect(url_for('audit.audit_page'))
        
        # Récupérer tous les résultats d'audit du projet
        audit_results = AuditResult.query.filter_by(
            project_id=current_project['id']
        ).order_by(AuditResult.date_audit.desc()).all()
        
        if not audit_results:
            flash('Aucun audit trouvé pour ce projet', 'warning')
            return redirect(url_for('audit.audit_page'))
        
        # Créer le contenu CSV
        output = StringIO()
        writer = csv.writer(output)
        
        # En-têtes
        headers = ['Date Audit', 'Project ID']
        
        # Ajouter les colonnes score_ dynamiquement
        score_columns = [column.name for column in AuditResult.__table__.columns 
                        if column.name.startswith('score_') or column.name.startswith('stats_')]
        headers.extend(score_columns)
        
        writer.writerow(headers)
        
        # Données
        for result in audit_results:
            row = [
                result.date_audit.strftime('%Y-%m-%d %H:%M:%S'),
                result.project_id
            ]
            
            # Ajouter les scores
            for column in score_columns:
                value = getattr(result, column)
                # Remplacer NaN par une chaîne vide
                if value is not None and not (isinstance(value, float) and np.isnan(value)):
                    row.append(value)
                else:
                    row.append('')
            
            writer.writerow(row)
        
        # Préparer la réponse
        output.seek(0)
        
        # Nom du fichier avec trigramme et date actuelle
        date_actuelle = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"export_{current_project['trigramme']}_{date_actuelle}.csv"
        
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        
        return response
        
    except Exception as e:
        flash(f'Erreur lors de l\'export : {str(e)}', 'error')
        return redirect(url_for('audit.audit_page'))
    

# Ajoutez ces nouvelles routes à votre fichier routes/audit.py

@audit_bp.route('/api/config/list')
def list_configurations():
    """Retourne toutes les configurations du projet actuel"""
    try:
        current_project = session.get('current_project')
        if not current_project:
            return jsonify({'error': 'Aucun projet sélectionné'}), 400
        
        configs = AuditConfig.query.filter_by(
            project_id=current_project['id']
        ).order_by(AuditConfig.date_configuration.desc()).all()
        
        configs_list = []
        for config in configs:
            configs_list.append({
                'config_id': config.config_id,
                'date_configuration': config.date_configuration.strftime('%d/%m/%Y %H:%M'),
                'is_latest': config == configs[0] if configs else False
            })
        
        return jsonify({
            'success': True,
            'configurations': configs_list
        })
        
    except Exception as e:
        return jsonify({'error': f'Erreur: {str(e)}'}), 500

@audit_bp.route('/api/config/load/<config_id>')
def load_configuration(config_id):
    """Charge une configuration spécifique"""
    try:
        current_project = session.get('current_project')
        if not current_project:
            return jsonify({'error': 'Aucun projet sélectionné'}), 400
        
        config = AuditConfig.query.filter_by(
            config_id=config_id,
            project_id=current_project['id']
        ).first()
        
        if not config:
            return jsonify({'error': 'Configuration non trouvée'}), 404
        
        # Conversion de la config en format compatible avec les checkboxes
        config_dict = config.to_dict()
        
        # Organiser par file_type et function_name pour le frontend
        organized_config = {}
        for key, value in config_dict.items():
            if key.startswith('conf_'):
                # Extraire file_type et function_name
                parts = key[5:].split('_', 1)  # Supprime 'conf_' et split sur le premier '_'
                if len(parts) == 2:
                    file_type, function_name = parts
                    if file_type not in organized_config:
                        organized_config[file_type] = {}
                    organized_config[file_type][function_name] = value
        
        return jsonify({
            'success': True,
            'config': organized_config,
            'config_id': config.config_id,
            'date_configuration': config.date_configuration.strftime('%d/%m/%Y %H:%M')
        })
        
    except Exception as e:
        return jsonify({'error': f'Erreur: {str(e)}'}), 500

@audit_bp.route('/api/config/save', methods=['POST'])
def save_configuration():
    """Sauvegarde une nouvelle configuration"""
    try:
        current_project = session.get('current_project')
        if not current_project:
            return jsonify({'error': 'Aucun projet sélectionné'}), 400
        
        data = request.get_json()
        selected_functions = data.get('selected_functions', {})
        
        # Créer une nouvelle configuration
        new_config = AuditConfig(project_id=current_project['id'])
        
        # Parcourir toutes les colonnes de configuration et les initialiser à False
        for column in AuditConfig.__table__.columns:
            column_name = column.name
            if column_name.startswith('conf_'):
                setattr(new_config, column_name, False)
        
        # Activer seulement les fonctions sélectionnées
        for file_type, functions in selected_functions.items():
            for function_name, is_selected in functions.items():
                if is_selected:
                    column_name = f'conf_{file_type}_{function_name}'
                    if hasattr(new_config, column_name):
                        setattr(new_config, column_name, True)
        
        # Sauvegarder en base
        db.session.add(new_config)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Configuration sauvegardée avec succès',
            'config_id': new_config.config_id
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Erreur: {str(e)}'}), 500

@audit_bp.route('/api/config/update', methods=['PUT'])
def update_configuration():
    """Met à jour la configuration la plus récente"""
    try:
        current_project = session.get('current_project')
        if not current_project:
            return jsonify({'error': 'Aucun projet sélectionné'}), 400
        
        data = request.get_json()
        selected_functions = data.get('selected_functions', {})
        
        # Récupérer la configuration la plus récente
        latest_config = AuditConfig.query.filter_by(
            project_id=current_project['id']
        ).order_by(AuditConfig.date_configuration.desc()).first()
        
        if not latest_config:
            # Si pas de config, créer une nouvelle
            return save_configuration()
        
        # Mettre à jour toutes les colonnes de configuration
        for column in AuditConfig.__table__.columns:
            column_name = column.name
            if column_name.startswith('conf_'):
                setattr(latest_config, column_name, False)
        
        # Activer seulement les fonctions sélectionnées
        for file_type, functions in selected_functions.items():
            for function_name, is_selected in functions.items():
                if is_selected:
                    column_name = f'conf_{file_type}_{function_name}'
                    if hasattr(latest_config, column_name):
                        setattr(latest_config, column_name, True)
        
        # Mettre à jour la date de modification
        latest_config.date_configuration = datetime.utcnow()
        
        # Sauvegarder en base
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Configuration mise à jour avec succès',
            'config_id': latest_config.config_id
        })
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': f'Erreur: {str(e)}'}), 500

@audit_bp.route('/api/config/current')
def get_current_configuration():
    """Retourne la configuration actuelle (la plus récente) formatée pour les checkboxes"""
    try:
        current_project = session.get('current_project')
        if not current_project:
            return jsonify({'error': 'Aucun projet sélectionné'}), 400
        
        # Récupérer la configuration la plus récente
        latest_config = AuditConfig.query.filter_by(
            project_id=current_project['id']
        ).order_by(AuditConfig.date_configuration.desc()).first()
        
        if not latest_config:
            return jsonify({
                'success': True,
                'has_config': False,
                'config': {}
            })
        
        # Conversion de la config en format compatible avec les checkboxes
        config_dict = latest_config.to_dict()
        
        # Organiser par file_type et function_name pour le frontend
        organized_config = {}
        for key, value in config_dict.items():
            if key.startswith('conf_'):
                # Extraire file_type et function_name
                parts = key[5:].split('_', 1)  # Supprime 'conf_' et split sur le premier '_'
                if len(parts) == 2:
                    file_type, function_name = parts
                    if file_type not in organized_config:
                        organized_config[file_type] = {}
                    organized_config[file_type][function_name] = value
        
        return jsonify({
            'success': True,
            'has_config': True,
            'config': organized_config,
            'config_id': latest_config.config_id,
            'date_configuration': latest_config.date_configuration.strftime('%d/%m/%Y %H:%M')
        })
        
    except Exception as e:
        return jsonify({'error': f'Erreur: {str(e)}'}), 500
    
@audit_bp.route('/api/gtfs/files')
def get_gtfs_files():
    """Retourne la liste des fichiers GTFS disponibles"""
    try:
        if not session.get('gtfs_loaded'):
            return jsonify({'error': 'Aucun GTFS chargé'}), 400
        
        gtfs_info = session.get('gtfs_info', {})
        files = []
        
        for filename, info in gtfs_info.items():
            files.append({
                'name': filename.replace('.txt', ''),
                'rows': info.get('rows', 0)
            })
        
        return jsonify({
            'success': True,
            'files': files
        })
        
    except Exception as e:
        return jsonify({'error': f'Erreur: {str(e)}'}), 500
    
def convert_to_native_types(obj):
    if isinstance(obj, dict):
        return {k: convert_to_native_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native_types(elem) for elem in obj]
    elif isinstance(obj, (bool,  int, float, str)) or obj is None:
        # Types déjà sérialisables
        return obj
    else:
        # Conversion spécifique : exemple pour bool_ non natif
        # Tu peux vérifier avec isinstance(obj, numpy.bool_) si numpy est utilisé
        # ou tout simplement forcer bool()
        try:
            if str(type(obj)).endswith("'bool_'>'"):
                return bool(obj)
        except:
            pass
        # Sinon convertir en string par défaut pour éviter l'erreur
        return str(obj)

@audit_bp.route('/api/audit/run-single-audit', methods=['POST'])
def run_single_audit():
    try:
        data = request.get_json()
        file_type = data.get('file_type')
        audit_name = data.get('audit_name')

        if not file_type or not audit_name:
            return jsonify({'error': 'file_type et audit_name doivent être spécifiés'}), 400
        
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        audit_service = AuditService()
        audits = audit_service.get_audits_for_file_type(file_type)
        # Trouver la fonction d'audit correspondante par nom
        audit = next((a for a in audits if a['name'] == audit_name), None)
        if not audit:
            return jsonify({'error': f'Audit "{audit_name}" non trouvé pour file_type "{file_type}"'}), 404
        results = {}
        try:
            result = audit['function'](gtfs_data)
            results = {
                'result': result,
            }
        except Exception as e:
            results = {
                'result': {},
                'error': str(e),
            }
        try:
            clean_results = convert_to_native_types(results)
            return jsonify({
                'success': True,
                'results': clean_results,
                'file': audit_name
            })
        
        except Exception as e:
            print("Erreur jsonify:", e)
            return jsonify({'error': f'Erreur jsonify: {str(e)}'}), 500
        
    except Exception as e:
        return jsonify({'error': f'Erreur: {str(e)}'}), 500     



@audit_bp.route('/api/audit/run-file', methods=['POST'])
def run_file_audit():
    """Lance toutes les fonctions d'audit sur un fichier spécifique"""
    try:
        if not session.get('gtfs_loaded'):
            return jsonify({'error': 'Aucun GTFS chargé'}), 400
        
        data = request.get_json()
        gtfs_file = data.get('gtfs_file')
        
        if not gtfs_file:
            return jsonify({'error': 'Fichier non spécifié'}), 400
        
        # Recharger les données GTFS
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Exécuter toutes les fonctions d'audit "file"
        audit_service = AuditService()
        file_audits = audit_service.get_audits_for_file_type('file')
        
        results = {}
        for audit in file_audits:
            try:
                result = audit['function'](gtfs_data, gtfs_file)
                results[audit['name']] = {
                    'result': result,
                }
            except Exception as e:
                results[audit['name']] = {
                    'result': {},
                    'error': str(e),
                }
        try:
            clean_results = convert_to_native_types(results)
            return jsonify({
                'success': True,
                'results': clean_results,
                'file': gtfs_file
            })
        except Exception as e:
            print("Erreur jsonify:", e)
            return jsonify({'error': f'Erreur jsonify: {str(e)}'}), 500
        
    except Exception as e:
        return jsonify({'error': f'Erreur: {str(e)}'}), 500