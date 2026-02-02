"""
Routes principales (accueil, upload)
"""

from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from werkzeug.utils import secure_filename
import os
from config import Config
from services.gtfs_handler import GTFSHandler
from models.project import db, Project
import requests
import tempfile
from urllib.parse import urlparse
import time

main_bp = Blueprint('main', __name__)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

@main_bp.route('/')
def index():
    """Page d'accueil - Sélection projet et Upload GTFS"""
    projects = Project.get_all_projects()
    
    # Vérifier si des données GTFS existent pour le projet actuel
    gtfs_loaded = False
    gtfs_info = None
    if session.get('current_project_id'):
        gtfs_loaded = GTFSHandler.has_gtfs_data(session['current_project_id'])
        if gtfs_loaded:
            gtfs_info = GTFSHandler.get_gtfs_info(session['current_project_id'])
    
    return render_template('index.html', 
                         projects=projects, 
                         gtfs_loaded=gtfs_loaded,
                         gtfs_info=gtfs_info)

@main_bp.route('/select_project', methods=['POST'])
def select_project():
    """Sélection du projet actuel"""
    project_id = request.form.get('project_id')
    
    if not project_id:
        flash('Veuillez sélectionner un projet', 'error')
        return redirect(url_for('main.index'))
    
    project = Project.query.get_or_404(project_id)
    
    # Nettoyer les données GTFS de l'ancien projet si changement
    old_project_id = session.get('current_project_id')
    if old_project_id and str(old_project_id) != str(project_id):
        GTFSHandler.clear_gtfs_cache(old_project_id)
        print(f"Cache GTFS supprimé pour l'ancien projet {old_project_id}")
        
        # Supprimer aussi l'ancien fichier physique s'il existe
        if session.get('gtfs_data_path') and os.path.exists(session['gtfs_data_path']):
            os.remove(session['gtfs_data_path'])
    
    # Nettoyer les variables de session liées au GTFS
    session.pop('gtfs_loaded', None)
    session.pop('gtfs_filename', None) 
    session.pop('gtfs_data_path', None)
    session.pop('gtfs_info', None)
    
    # Stocker le nouveau projet sélectionné
    session['current_project_id'] = project.project_id
    session['current_project'] = {
        'id': project.project_id,
        'nom': project.nom_projet,
        'trigramme': project.trigramme,
        'coverage': project.coverage,
        'is_sandbox': project.is_sandbox
    }
    
    flash(f'Projet "{project.trigramme} - {project.nom_projet}" sélectionné', 'success')
    return redirect(url_for('main.index'))
import requests
import tempfile
from urllib.parse import urlparse

@main_bp.route('/upload', methods=['POST'])
def upload_gtfs():
    """Traitement de l'upload GTFS (fichier ou URL)"""
    
    import_type = request.form.get('import_type', 'file')
    
    if import_type == 'url':
        return handle_url_upload()
    else:
        return handle_file_upload()

def handle_file_upload():
    """Gestion upload fichier local (code existant)"""
    if 'gtfs_file' not in request.files:
        flash('Aucun fichier sélectionné', 'error')
        return redirect(url_for('main.index'))
    
    file = request.files['gtfs_file']
    
    if file.filename == '':
        flash('Aucun fichier sélectionné', 'error')
        return redirect(url_for('main.index'))
    
    if file and allowed_file(file.filename):
        # Code existant...
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        filename = secure_filename(file.filename)
        filepath = os.path.join(Config.UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        return process_gtfs_file(filepath, filename)
    
    flash('Format de fichier non supporté. Utilisez un fichier ZIP.', 'error')
    return redirect(url_for('main.index'))

def handle_url_upload():
    """Gestion téléchargement depuis URL"""
    gtfs_url = request.form.get('gtfs_url', '').strip()
    
    if not gtfs_url:
        flash('URL manquante', 'error')
        return redirect(url_for('main.index'))
    
    # Validation basique de l'URL
    try:
        parsed_url = urlparse(gtfs_url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError("URL invalide")
    except:
        flash('URL invalide', 'error')
        return redirect(url_for('main.index'))
    
    try:
        flash('Téléchargement en cours...', 'info')
        
        # Télécharger le fichier
        headers = {
            'User-Agent': 'GTFS-Audit-Tool/1.0'
        }
        
        response = requests.get(
            gtfs_url, 
            headers=headers,
            timeout=30,  # 30 secondes timeout
            stream=True
        )
        response.raise_for_status()
        
        # Vérifier que c'est bien un ZIP
        content_type = response.headers.get('content-type', '').lower()
        if 'zip' not in content_type and not gtfs_url.lower().endswith('.zip'):
            flash('Le fichier téléchargé ne semble pas être un ZIP', 'warning')
        
        # Sauvegarder temporairement
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        filename = f"gtfs_download_{int(time.time())}.zip"
        filepath = os.path.join(Config.UPLOAD_FOLDER, filename)
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        flash('Téléchargement terminé', 'success')
        return process_gtfs_file(filepath, f"Téléchargé depuis URL")
        
    except requests.exceptions.Timeout:
        flash('Timeout - Le téléchargement a pris trop de temps', 'error')
    except requests.exceptions.ConnectionError:
        flash('Erreur de connexion - Vérifiez l\'URL', 'error')
    except requests.exceptions.HTTPError as e:
        flash(f'Erreur HTTP {e.response.status_code} - Fichier introuvable', 'error')
    except Exception as e:
        flash(f'Erreur lors du téléchargement: {str(e)}', 'error')
    
    return redirect(url_for('main.index'))

def process_gtfs_file(filepath, display_name):
    """Traitement commun du fichier GTFS"""
    gtfs_handler = GTFSHandler()
    
    project_id = session['current_project']['id']
    # Utiliser un project_id pour le mode sandbox
    gtfs_data = gtfs_handler.extract_and_cache_gtfs(filepath, project_id)
    
    if gtfs_data:
        session['gtfs_loaded'] = True
        session['gtfs_filename'] = display_name
        session['gtfs_project_id'] = project_id
        session['gtfs_info'] = gtfs_handler.get_gtfs_info(project_id)
        
        flash(f'GTFS "{display_name}" chargé avec succès !', 'success')
        return redirect(url_for('audit.audit_page'))
    else:
        flash('Erreur lors du chargement du GTFS', 'error')
        return redirect(url_for('main.index'))

@main_bp.route('/gtfs_status')
def gtfs_status():
    """API pour vérifier le statut du GTFS chargé"""
    project_id = session.get('current_project_id')
    if not project_id:
        return jsonify({'loaded': False})
    
    gtfs_loaded = GTFSHandler.has_gtfs_data(project_id)
    
    if gtfs_loaded:
        gtfs_info = GTFSHandler.get_gtfs_info(project_id)
        return jsonify({
            'loaded': True,
            'info': gtfs_info,
            'filename': session.get('gtfs_filename', 'Inconnu')
        })
    else:
        return jsonify({'loaded': False})

@main_bp.route('/clear_gtfs', methods=['POST'])
def clear_gtfs():
    """Supprime les données GTFS du projet actuel"""
    project_id = session.get('current_project_id')
    if not project_id:
        return jsonify({'error': 'Aucun projet sélectionné'}), 400
    
    try:
        # Supprimer du cache mémoire et disque
        GTFSHandler.clear_gtfs_cache(project_id)
        
        # Supprimer le fichier physique
        if session.get('gtfs_data_path') and os.path.exists(session['gtfs_data_path']):
            os.remove(session['gtfs_data_path'])
        
        # Nettoyer la session
        session.pop('gtfs_loaded', None)
        session.pop('gtfs_filename', None) 
        session.pop('gtfs_data_path', None)
        session.pop('gtfs_info', None)
        
        return jsonify({'success': True, 'message': 'GTFS supprimé avec succès'})
        
    except Exception as e:
        return jsonify({'error': f'Erreur lors de la suppression: {str(e)}'}), 500

@main_bp.route('/clear')
def clear_session():
    """Nettoie la session et supprime les fichiers temporaires"""
    project_id = session.get('current_project_id')
    
    # Supprimer du cache mémoire et disque
    if project_id:
        GTFSHandler.clear_gtfs_cache(project_id)
    
    # Supprimer le fichier physique
    if session.get('gtfs_data_path') and os.path.exists(session['gtfs_data_path']):
        os.remove(session['gtfs_data_path'])
    
    session.clear()
    flash('Session réinitialisée', 'info')
    return redirect(url_for('main.index'))