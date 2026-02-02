"""
Routes d'administration des projets
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from models.project import db, Project

admin_bp = Blueprint('admin', __name__)

from models.audit_config import AuditConfig
from models.audit_result import AuditResult

@admin_bp.route('/admin')
def admin_page():
    """Page d'administration des projets"""
    all_projects = Project.get_all_projects()
    
    # Récupérer le filtre coverage sélectionné
    selected_coverage = request.args.get('coverage_filter')
    
    # Filtrer les projets selon le coverage sélectionné
    if selected_coverage:
        if selected_coverage == "sans_coverage":
            projects = [p for p in all_projects if not p.coverage]
        else:
            projects = [p for p in all_projects if p.coverage == selected_coverage]
    else:
        projects = all_projects
    
    # Récupérer la liste unique des coverages pour le dropdown
    coverages_list = list(set([p.coverage for p in all_projects if p.coverage]))
    coverages_list.sort()
    
    # Récupérer le projet à éditer si spécifié
    edit_project = None
    edit_id = request.args.get('edit_id')
    if edit_id:
        edit_project = Project.query.get(edit_id)
    
    # Récupérer les données du projet actif
    current_project_configs = []
    current_project_results = []
    current_project = session.get('current_project')
    
    if current_project:
        current_project_id = current_project['id']
        current_project_configs = AuditConfig.query.filter_by(project_id=current_project_id).order_by(AuditConfig.date_configuration.desc()).all()
        current_project_results = AuditResult.query.filter_by(project_id=current_project_id).order_by(AuditResult.date_audit.desc()).all()
    
    return render_template('admin.html', 
                         projects=projects, 
                         all_projects=all_projects,
                         coverages_list=coverages_list,
                         selected_coverage=selected_coverage,
                         edit_project=edit_project,
                         current_project_configs=current_project_configs,
                         current_project_results=current_project_results)

@admin_bp.route('/project/create', methods=['POST'])
def create_project():
    """Créer un nouveau projet"""
    try:
        nom_projet = request.form.get('nom_projet', '').strip()
        trigramme = request.form.get('trigramme', '').strip()
        coverage = request.form.get('coverage', '').strip() or None
        
        if not nom_projet or not trigramme:
            flash('Le nom du projet et le trigramme sont obligatoires', 'error')
            return redirect(url_for('admin.admin_page'))
        
        project = Project.create_project(nom_projet, trigramme, coverage)
        flash(f'Projet "{project.nom_projet}" créé avec succès', 'success')
        
    except ValueError as e:
        flash(str(e), 'error')
    except Exception as e:
        db.session.rollback()
        flash(f'Erreur lors de la création : {str(e)}', 'error')
    
    return redirect(url_for('admin.admin_page'))

@admin_bp.route('/project/<project_id>/edit', methods=['POST'])
def edit_project(project_id):
    """Modifier un projet existant"""
    try:
        project = Project.query.get_or_404(project_id)
        if project.is_sandbox:
            flash('Impossible de modifier le projet sandbox', 'error')
            return redirect(url_for('admin.admin_page'))
        
        nom_projet = request.form.get('nom_projet', '').strip()
        trigramme = request.form.get('trigramme', '').strip()
        coverage = request.form.get('coverage', '').strip() or None
        
        if not nom_projet or not trigramme:
            flash('Le nom du projet et le trigramme sont obligatoires', 'error')
            return redirect(url_for('admin.admin_page'))
        project.update_project(nom_projet, trigramme, coverage)
        flash(f'Projet "{project.nom_projet}" modifié avec succès', 'success')
        
    except ValueError as e:
        flash(str(e), 'error')
    except Exception as e:
        db.session.rollback()
        flash(f'Erreur lors de la modification : {str(e)}', 'error')
    
    return redirect(url_for('admin.admin_page'))

@admin_bp.route('/project/<project_id>/delete', methods=['POST'])
def delete_project(project_id):
    """Supprimer un projet"""
    try:
        project = Project.query.get_or_404(project_id)
        
        if project.is_sandbox:
            flash('Impossible de supprimer le projet sandbox', 'error')
            return redirect(url_for('admin.admin_page'))
        
        project_name = project.nom_projet
        project.delete_project()
        flash(f'Projet "{project_name}" supprimé avec succès', 'success')
        
    except ValueError as e:
        flash(str(e), 'error')
    except Exception as e:
        db.session.rollback()
        flash(f'Erreur lors de la suppression : {str(e)}', 'error')
    
    return redirect(url_for('admin.admin_page'))

# ==========================================
# ROUTES API
# ==========================================

@admin_bp.route('/api/projects')
def api_projects():
    """API pour récupérer la liste des projets (pour AJAX)"""
    projects = Project.get_all_projects()
    return jsonify([project.to_dict() for project in projects])

@admin_bp.route('/api/project/<project_id>')
def api_project_detail(project_id):
    """API pour récupérer les détails d'un projet"""
    project = Project.query.get_or_404(project_id)
    return jsonify(project.to_dict())

# Ajoute ces imports en haut
from models.audit_config import AuditConfig

# Ajoute ces nouvelles routes après tes routes existantes

@admin_bp.route('/config/create', methods=['GET', 'POST'])
def create_config():
    """Créer une nouvelle configuration d'audit"""
    if not session.get('current_project'):
        flash('Veuillez d\'abord sélectionner un projet', 'error')
        return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        try:
            current_project_id = session['current_project']['id']
            
            # Récupérer toutes les valeurs des checkboxes
            config = AuditConfig(
                project_id=current_project_id,
                conf_agency_completion_url=bool(request.form.get('conf_agency_completion_url')),
                conf_agency_completion_phone=bool(request.form.get('conf_agency_completion_phone')),
                conf_routes_validate_route_colors=bool(request.form.get('conf_routes_validate_route_colors'))
            )
            
            db.session.add(config)
            db.session.commit()
            
            flash('Configuration créée avec succès', 'success')
            
        except Exception as e:
            db.session.rollback()
            flash(f'Erreur lors de la création : {str(e)}', 'error')
    
    return redirect(url_for('admin.admin_page'))

@admin_bp.route('/config/<config_id>/delete', methods=['POST'])
def delete_config(config_id):
    """Supprimer une configuration d'audit"""
    try:
        config = AuditConfig.query.get_or_404(config_id)
        
        # Vérifier que la config appartient au projet actuel
        if not session.get('current_project') or config.project_id != session['current_project']['id']:
            flash('Vous ne pouvez supprimer que les configurations du projet actuel', 'error')
            return redirect(url_for('admin.admin_page'))
        
        db.session.delete(config)
        db.session.commit()
        
        flash('Configuration supprimée avec succès', 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(f'Erreur lors de la suppression : {str(e)}', 'error')
    
    return redirect(url_for('admin.admin_page'))