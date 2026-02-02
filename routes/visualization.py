"""
Routes Flask pour la gestion des visualisations
"""

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from services.gtfs_handler import GTFSHandler
import pandas as pd
from visualization.decorators import VISUALIZATION_REGISTRY

visualization_bp = Blueprint('visualization', __name__)

@visualization_bp.route('/visualization')
def visualization_page():
    """Page principale de visualisation"""
    if not session.get('gtfs_loaded'):
        flash('Veuillez d\'abord charger un fichier GTFS', 'warning')
        return redirect(url_for('main.index'))
    
    return render_template('visualization.html', 
                         visualizations=VISUALIZATION_REGISTRY,
                         gtfs_info=session.get('gtfs_info', {}))

@visualization_bp.route('/api/visualization/run', methods=['POST'])
def run_visualization():
    """API pour exécuter une visualisation"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    data = request.get_json()
    category = data.get('category')
    function_name = data.get('function_name')
    parameters = data.get('parameters', {})
    
    # Recharger les données GTFS
    project_id = session.get('current_project_id')
    gtfs_data = GTFSHandler.get_gtfs_data(project_id)
    if not gtfs_data:
        return jsonify({'error': 'Erreur lors du rechargement du GTFS'}), 500

    
    # Trouver et exécuter la fonction
    if category in VISUALIZATION_REGISTRY:
        for viz in VISUALIZATION_REGISTRY[category]:
            if viz['function_name'] == function_name:
                try:
                    # ✅ DEBUG - Afficher les paramètres reçus
                    print(f"Paramètres reçus: {parameters}")
                    
                    html_result = viz['function'](gtfs_data, **parameters)
                    return jsonify({
                        'success': True,
                        'html': html_result
                    })
                except Exception as e:
                    print(f"Erreur dans {function_name}: {e}")  # ✅ Debug
                    return jsonify({'error': f'Erreur lors de l\'exécution: {str(e)}'}), 500
    
    return jsonify({'error': 'Fonction de visualisation non trouvée'}), 404

@visualization_bp.route('/api/visualization/options/<category>')
def get_visualization_options(category):
    """API pour récupérer les options dynamiques"""
    if not session.get('gtfs_loaded'):
        return jsonify({'error': 'Aucun GTFS chargé'}), 400
    
    # ✅ NOUVEAU - Utiliser le cache
    project_id = session.get('gtfs_project_id', 'sandbox')
    gtfs_handler = GTFSHandler()
    gtfs_data = gtfs_handler.get_gtfs_data(project_id)
    
    if not gtfs_data:
        return jsonify({'error': 'Données GTFS non trouvées en cache'}), 500
    
    options = {}
    
    # Options pour les routes
    if category == 'routes' and 'routes.txt' in gtfs_data:
        routes_df = gtfs_data['routes.txt']
        routes_options = []
        for _, route in routes_df.iterrows():
            route_id = route['route_id']
            name = route.get('route_short_name', route_id)
            if pd.isna(name) or name == '':
                name = route.get('route_long_name', route_id)
            routes_options.append({
                'value': route_id,
                'label': f"{name} ({route_id})"
            })
        options['routes'] = routes_options
    
    # Options pour les arrêts
    if category == 'stops' and 'stops.txt' in gtfs_data:
        stops_df = gtfs_data['stops.txt']
        stops_options = []
        for _, stop in stops_df.iterrows():
            stops_options.append({
                'value': stop['stop_id'],
                'label': f"{stop['stop_name']} ({stop['stop_id']})"
            })
        options['stops'] = stops_options[:100]  # Limiter à 100 pour les performances
    
    return jsonify(options)