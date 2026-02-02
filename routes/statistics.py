"""
Routes pour la gestion des statistiques GTFS - Version corrigée
"""

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from services.statistics_service import StatisticsService
from services.gtfs_handler import GTFSHandler
import numpy as np
import pandas as pd

statistics_bp = Blueprint('statistics', __name__)

@statistics_bp.route('/statistics')
def statistics_page():
    """Page principale des statistiques GTFS"""
    if not session.get('gtfs_loaded'):
        flash('Veuillez d\'abord charger un fichier GTFS', 'warning')
        return redirect(url_for('main.index'))
    
    statistics_service = StatisticsService()
    available_statistics = statistics_service.get_available_statistics()
    categories = statistics_service.get_categories()
    
    return render_template('statistics.html', 
                         statistics=available_statistics,
                         categories=categories,
                         gtfs_info=session.get('gtfs_info', {}))

@statistics_bp.route('/api/statistics/essential', methods=['GET'])
def get_essential_statistics():
    """API pour récupérer les statistiques essentielles"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    try:
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Exécuter les statistiques essentielles
        statistics_service = StatisticsService()
        results = statistics_service.run_essential_statistics(gtfs_data)
        
        return jsonify({
            'success': True,
            'results': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'}), 500

@statistics_bp.route('/api/statistics/category/<category>', methods=['GET'])
def get_category_statistics(category):
    """API pour récupérer la STRUCTURE des statistiques d'une catégorie (sans calcul)"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    try:
        # Récupérer seulement la structure des statistiques disponibles pour cette catégorie
        statistics_service = StatisticsService()
        available_statistics = statistics_service.get_available_statistics()
        
        if category not in available_statistics:
            return jsonify({'error': f'Catégorie {category} non trouvée'}), 404
        
        category_stats = available_statistics[category]
        
        return jsonify({
            'success': True,
            'category': category,
            'results': category_stats  # Structure seulement (nom, description, paramètres)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'}), 500

@statistics_bp.route('/api/statistics/category/<category>/calculate', methods=['POST'])
def calculate_category_statistics(category):
    """API pour CALCULER toutes les statistiques d'une catégorie"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    try:
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Exécuter TOUTES les statistiques de la catégorie
        statistics_service = StatisticsService()
        results = statistics_service.run_category_statistics(category, gtfs_data)
        
        return jsonify({
            'success': True,
            'category': category,
            'results': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'}), 500

@statistics_bp.route('/api/statistics/run', methods=['POST'])
def run_statistic():
    """API pour exécuter une statistique spécifique"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    data = request.get_json()
    category = data.get('category')
    function_name = data.get('function_name')
    parameters = data.get('parameters', {})
    
    if not category or not function_name:
        return jsonify({'error': 'Catégorie et nom de fonction requis'}), 400
    
    try:
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Exécuter la statistique
        statistics_service = StatisticsService()
        result, problem_ids = statistics_service.run_statistic(category, function_name, gtfs_data, parameters)
        
        if result is None:
            return jsonify({'error': 'Erreur lors de l\'exécution de la statistique'}), 500
        
        # Formater le résultat pour l'affichage
        formatted_result = statistics_service.format_statistic_for_display(result, function_name)
        
        return jsonify({
            'success': True,
            'result': formatted_result,
            'problem_ids': problem_ids,
            'category': category,
            'function_name': function_name
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'}), 500

@statistics_bp.route('/api/statistics/all', methods=['GET'])
def get_all_statistics():
    """API pour exécuter toutes les statistiques (attention: peut être lent)"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    try:
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Exécuter toutes les statistiques
        statistics_service = StatisticsService()
        results = statistics_service.run_all_statistics(gtfs_data)
        
        return jsonify({
            'success': True,
            'results': results,
            'execution_time': 'computed'  # Pourrait être chronométré
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'}), 500

@statistics_bp.route('/api/statistics/refresh', methods=['POST'])
def refresh_statistics():
    """API pour actualiser les statistiques après un changement de GTFS"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    try:
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Mettre à jour les informations GTFS en session
        gtfs_info = GTFSHandler().get_gtfs_info(gtfs_data)
        session['gtfs_info'] = gtfs_info
        
        # Recalculer les statistiques essentielles
        statistics_service = StatisticsService()
        essential_stats = statistics_service.run_essential_statistics(gtfs_data)
        summary = statistics_service.get_statistics_summary(gtfs_data)
        
        return jsonify({
            'success': True,
            'message': 'Statistiques actualisées',
            'essential_stats': essential_stats,
            'summary': summary
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'}), 500

@statistics_bp.route('/api/statistics/export/<category>', methods=['GET'])
def export_category_statistics(category):
    """API pour exporter les statistiques d'une catégorie en JSON"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    try:
        project_id = session.get('current_project_id')
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500
        
        # Exécuter les statistiques de la catégorie
        statistics_service = StatisticsService()
        results = statistics_service.run_category_statistics(category, gtfs_data)
        
        # Préparer les données d'export
        export_data = {
            'category': category,
            'export_date': pd.Timestamp.now().isoformat(),
            'gtfs_file': session.get('gtfs_filename', 'unknown'),
            'statistics': results
        }
        
        from flask import make_response
        import json
        
        response = make_response(json.dumps(export_data, indent=2, ensure_ascii=False))
        response.headers['Content-Type'] = 'application/json'
        response.headers['Content-Disposition'] = f'attachment; filename=statistics_{category}_{session.get("gtfs_filename", "gtfs")}.json'
        
        return response
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erreur: {str(e)}'}), 500

@statistics_bp.route('/api/statistics/compare', methods=['POST'])
def compare_statistics():
    """API pour comparer les statistiques entre différentes versions (future feature)"""
    # Cette fonctionnalité pourrait être ajoutée plus tard pour comparer
    # les statistiques entre différentes versions de GTFS
    return jsonify({
        'success': False, 
        'error': 'Fonctionnalité de comparaison non encore implémentée'
    }), 501

@statistics_bp.route('/api/statistics/health', methods=['GET'])
def statistics_health():
    """Endpoint de santé pour vérifier que le service de statistiques fonctionne"""
    try:
        statistics_service = StatisticsService()
        categories = statistics_service.get_categories()
        available_stats = statistics_service.get_available_statistics()
        
        total_functions = sum(len(stats) for stats in available_stats.values())
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'categories_count': len(categories),
            'total_functions': total_functions,
            'categories': categories
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'error': str(e)
        }), 500
    

