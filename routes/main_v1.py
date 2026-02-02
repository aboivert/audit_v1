"""
Routes principales (accueil, upload)
"""

from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.utils import secure_filename
import os
from config import Config
from services.gtfs_handler import GTFSHandler
from models.project import db, Project

main_bp = Blueprint('main', __name__)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

@main_bp.route('/')
def index():
    """Page d'accueil - Sélection projet et Upload GTFS"""
    projects = Project.get_all_projects()
    return render_template('index.html', projects=projects)

@main_bp.route('/select_project', methods=['POST'])
def select_project():
    """Sélection du projet actuel"""
    project_id = request.form.get('project_id')
    
    if not project_id:
        flash('Veuillez sélectionner un projet', 'error')
        return redirect(url_for('main.index'))
    
    project = Project.query.get_or_404(project_id)
    
    # Nettoyer la session précédente si changement de projet
    if session.get('current_project_id') != project_id:
        # Supprimer l'ancien GTFS s'il existe
        if session.get('gtfs_data_path') and os.path.exists(session['gtfs_data_path']):
            os.remove(session['gtfs_data_path'])
        
        # Nettoyer les données GTFS de la session
        session.pop('gtfs_loaded', None)
        session.pop('gtfs_filename', None) 
        session.pop('gtfs_data_path', None)
        session.pop('gtfs_info', None)
    
    # Stocker le projet sélectionné
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

@main_bp.route('/upload', methods=['POST'])
def upload_gtfs():
    """Traitement de l'upload GTFS"""
    # Vérifier qu'un projet est sélectionné
    if not session.get('current_project_id'):
        flash('Veuillez d\'abord sélectionner un projet', 'error')
        return redirect(url_for('main.index'))
    
    if 'gtfs_file' not in request.files:
        flash('Aucun fichier sélectionné', 'error')
        return redirect(url_for('main.index'))
    
    file = request.files['gtfs_file']
    
    if file.filename == '':
        flash('Aucun fichier sélectionné', 'error')
        return redirect(url_for('main.index'))
    
    if file and allowed_file(file.filename):
        # Créer le dossier uploads s'il n'existe pas
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        
        # Supprimer l'ancien fichier s'il existe
        if session.get('gtfs_data_path') and os.path.exists(session['gtfs_data_path']):
            os.remove(session['gtfs_data_path'])
        
        filename = secure_filename(file.filename)
        # Ajouter le trigramme du projet au nom du fichier pour éviter les conflits
        project_trigramme = session['current_project']['trigramme']
        filename_with_project = f"{project_trigramme}_{filename}"
        filepath = os.path.join(Config.UPLOAD_FOLDER, filename_with_project)
        file.save(filepath)
        
        # Charger le GTFS
        gtfs_handler = GTFSHandler()
        gtfs_data = gtfs_handler.extract_and_load_gtfs(filepath)
        
        if gtfs_data:
            # Stocker en session
            session['gtfs_loaded'] = True
            session['gtfs_filename'] = filename
            session['gtfs_data_path'] = filepath
            session['gtfs_info'] = gtfs_handler.get_gtfs_info(gtfs_data)
            
            flash(f'GTFS "{filename}" chargé avec succès pour le projet {project_trigramme}!', 'success')
            return redirect(url_for('audit.audit_page'))
        else:
            flash('Erreur lors du chargement du GTFS', 'error')
            return redirect(url_for('main.index'))
    
    flash('Format de fichier non supporté. Utilisez un fichier ZIP.', 'error')
    return redirect(url_for('main.index'))

@main_bp.route('/clear')
def clear_session():
    """Nettoie la session et supprime les fichiers temporaires"""
    if session.get('gtfs_data_path') and os.path.exists(session['gtfs_data_path']):
        os.remove(session['gtfs_data_path'])
    
    session.clear()
    flash('Session réinitialisée', 'info')
    return redirect(url_for('main.index'))