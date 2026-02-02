"""
Fonctions de visualisation GTFS avec Chart.js
"""

from visualization.decorators import visualization_function
import pandas as pd
import json
import random

def generate_chart_id():
    """Génère un ID unique pour chaque graphique"""
    return f"chart_{random.randint(10000, 99999)}"


@visualization_function(
    category="routes",
    name="Trajets par route",
    description="Graphique en barres du nombre de trajets par route",
    parameters={
        "top_n": {
            "type": "slider",
            "min": 5,
            "max": 30,
            "default": 10,
            "description": "Nombre de routes à afficher"
        },
        "chart_type": {
            "type": "select",
            "options": ["bar", "pie", "doughnut"],
            "default": "bar",
            "description": "Type de graphique"
        }
    }
)
def trips_by_route_chartjs(gtfs_data, **params):
    """Graphique des trajets par route avec Chart.js"""
    
    top_n = params.get('top_n', 10)
    chart_type = params.get('chart_type', 'bar')
    
    try:
        if 'trips.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichier trips.txt manquant</div>"
        
        trips_df = gtfs_data['trips.txt']
        routes_df = gtfs_data.get('routes.txt', pd.DataFrame())
        
        # Compter les trajets par route
        trips_count = trips_df['route_id'].value_counts().head(top_n)
        
        # Récupérer les noms de routes
        route_names = {}
        if not routes_df.empty:
            for _, route in routes_df.iterrows():
                route_id = route['route_id']
                name = route.get('route_short_name', route_id)
                if pd.isna(name) or name == '':
                    name = route.get('route_long_name', route_id)
                route_names[route_id] = str(name)
        
        # Préparer les données
        labels = [str(route_names.get(str(route_id), str(route_id))) for route_id in trips_count.index]
        data = [int(x) for x in trips_count.values]
        
        # Générer des couleurs
        colors = [
            f'rgba({random.randint(50, 255)}, {random.randint(50, 255)}, {random.randint(50, 255)}, 0.8)'
            for _ in range(len(labels))
        ]
        
        chart_id = generate_chart_id()
        
        # Configuration selon le type de graphique
        if chart_type in ['pie', 'doughnut']:
            chart_config = {
                'type': chart_type,
                'data': {
                    'labels': labels,
                    'datasets': [{
                        'data': data,
                        'backgroundColor': colors,
                        'borderWidth': 2,
                        'borderColor': '#fff'
                    }]
                },
                'options': {
                    'responsive': True,
                    'maintainAspectRatio': False,
                    'plugins': {
                        'legend': {
                            'position': 'right'
                        },
                        'tooltip': {
                            'callbacks': {
                                'label': 'function(context) { return context.label + ": " + context.parsed + " trajets"; }'
                            }
                        }
                    }
                }
            }
        else:  # bar chart
            chart_config = {
                'type': 'bar',
                'data': {
                    'labels': labels,
                    'datasets': [{
                        'label': 'Nombre de trajets',
                        'data': data,
                        'backgroundColor': colors,
                        'borderColor': colors,
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'maintainAspectRatio': False,
                    'scales': {
                        'y': {
                            'beginAtZero': True,
                            'title': {
                                'display': True,
                                'text': 'Nombre de trajets'
                            }
                        },
                        'x': {
                            'title': {
                                'display': True,
                                'text': 'Routes'
                            }
                        }
                    },
                    'plugins': {
                        'legend': {
                            'display': False
                        }
                    }
                }
            }
        
        html = f'''
        <div style="width: 100%; height: 400px; position: relative;">
            <canvas id="{chart_id}"></canvas>
        </div>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            const config = {json.dumps(chart_config)};
            
            // Fonction personnalisée pour les tooltips
            if (config.options.plugins && config.options.plugins.tooltip && config.options.plugins.tooltip.callbacks) {{
                config.options.plugins.tooltip.callbacks.label = function(context) {{
                    return context.label + ": " + context.parsed + " trajets";
                }};
            }}
            
            new Chart(ctx, config);
        }})();
        </script>
        '''
        
        return html
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"
    
@visualization_function(
    category="routes",
    name="Trajets par route (sélection multiple)",
    description="Comparaison du nombre de trajets pour des routes spécifiques",
    parameters={
        "route_ids": {
            "type": "multiselect",
            "description": "Routes à comparer",
            "source": "routes"
        }
    }
)
def compare_routes_chartjs(gtfs_data, **params):
    """Comparaison de routes spécifiques"""
    
    selected_routes = params.get('route_ids', [])
    
    try:
        if 'trips.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichier trips.txt manquant</div>"
        
        trips_df = gtfs_data['trips.txt']
        routes_df = gtfs_data.get('routes.txt', pd.DataFrame())
        
        # Si aucune route sélectionnée, prendre les 5 premières
        if not selected_routes:
            selected_routes = trips_df['route_id'].value_counts().head(5).index.tolist()
        
        # Filtrer les trajets
        filtered_trips = trips_df[trips_df['route_id'].isin(selected_routes)]
        trips_count = filtered_trips['route_id'].value_counts()
        
        # Récupérer les noms
        route_names = {}
        if not routes_df.empty:
            for _, route in routes_df.iterrows():
                route_id = route['route_id']
                name = route.get('route_short_name', route_id)
                if pd.isna(name) or name == '':
                    name = route.get('route_long_name', route_id)
                route_names[route_id] = str(name)
        
        labels = [route_names.get(route_id, str(route_id)) for route_id in selected_routes]
        data = [int(trips_count.get(route_id, 0)) for route_id in selected_routes]
        
        colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
        
        chart_id = generate_chart_id()
        
        html = f'''
        <div style="width: 100%; height: 400px;">
            <canvas id="{chart_id}"></canvas>
        </div>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            new Chart(ctx, {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [{{
                        label: 'Nombre de trajets',
                        data: {json.dumps(data)},
                        backgroundColor: {json.dumps(colors[:len(data)])},
                        borderWidth: 1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'Nombre de trajets'
                            }}
                        }}
                    }}
                }}
            }});
        }})();
        </script>
        '''
        
        return html
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"

@visualization_function(
    category="stops",
    name="Top arrêts les plus fréquentés",
    description="Arrêts avec le plus de passages",
    parameters={
        "top_n": {
            "type": "slider",
            "min": 5,
            "max": 25,
            "default": 15,
            "description": "Nombre d'arrêts à afficher"
        },
        "orientation": {
            "type": "select",
            "options": ["vertical", "horizontal"],
            "default": "horizontal",
            "description": "Orientation du graphique"
        }
    }
)
def top_stops_chartjs(gtfs_data, **params):
    """Top des arrêts les plus fréquentés"""
    
    top_n = params.get('top_n', 15)
    orientation = params.get('orientation', 'horizontal')
    
    try:
        if 'stop_times.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichier stop_times.txt manquant</div>"
        
        stop_times_df = gtfs_data['stop_times.txt']
        stops_df = gtfs_data.get('stops.txt', pd.DataFrame())
        
        # Compter les passages par arrêt
        stop_frequency = stop_times_df['stop_id'].value_counts().head(top_n)
        
        # Récupérer les noms d'arrêts
        stop_names = {}
        if not stops_df.empty:
            stop_names = dict(zip(stops_df['stop_id'], stops_df['stop_name']))
        
        labels = [stop_names.get(stop_id, str(stop_id)) for stop_id in stop_frequency.index]
        data = [int(x) for x in stop_frequency.values]
        
        chart_id = generate_chart_id()
        
        # Configuration selon l'orientation
        if orientation == 'horizontal':
            chart_config = {
                'type': 'bar',
                'data': {
                    'labels': labels,
                    'datasets': [{
                        'label': 'Nombre de passages',
                        'data': data,
                        'backgroundColor': 'rgba(54, 162, 235, 0.8)',
                        'borderColor': 'rgba(54, 162, 235, 1)',
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'indexAxis': 'y',
                    'responsive': True,
                    'maintainAspectRatio': False,
                    'scales': {
                        'x': {
                            'beginAtZero': True,
                            'title': {
                                'display': True,
                                'text': 'Nombre de passages'
                            }
                        }
                    },
                    'plugins': {
                        'legend': {
                            'display': False
                        }
                    }
                }
            }
        else:
            chart_config = {
                'type': 'bar',
                'data': {
                    'labels': labels,
                    'datasets': [{
                        'label': 'Nombre de passages',
                        'data': data,
                        'backgroundColor': 'rgba(75, 192, 192, 0.8)',
                        'borderColor': 'rgba(75, 192, 192, 1)',
                        'borderWidth': 1
                    }]
                },
                'options': {
                    'responsive': True,
                    'maintainAspectRatio': False,
                    'scales': {
                        'y': {
                            'beginAtZero': True,
                            'title': {
                                'display': True,
                                'text': 'Nombre de passages'
                            }
                        },
                        'x': {
                            'ticks': {
                                'maxRotation': 45
                            }
                        }
                    },
                    'plugins': {
                        'legend': {
                            'display': False
                        }
                    }
                }
            }
        
        html = f'''
        <div style="width: 100%; height: {'600px' if orientation == 'horizontal' else '400px'};">
            <canvas id="{chart_id}"></canvas>
        </div>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            new Chart(ctx, {json.dumps(chart_config)});
        }})();
        </script>
        '''
        
        return html
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"
    
# Ajouter ces fonctions à visualization/functions.py

@visualization_function(
    category="routes",
    name="Distribution des types de transport",
    description="Répartition des lignes par type de transport (bus, métro, tram...)",
    parameters={
        "show_values": {
            "type": "checkbox",
            "default": True,
            "description": "Afficher les valeurs sur le graphique"
        }
    }
)
def route_types_distribution(gtfs_data, **params):
    """Distribution des types de transport"""
    
    show_values = params.get('show_values', True)
    chart_id = f"plotly_{random.randint(10000, 99999)}"
    
    try:
        if 'routes.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichier routes.txt manquant</div>"
        
        routes_df = gtfs_data['routes.txt']
        
        # Types de transport selon la spec GTFS
        transport_types = {
            0: 'Tram, Streetcar, Light rail',
            1: 'Subway, Metro',
            2: 'Rail',
            3: 'Bus',
            4: 'Ferry',
            5: 'Cable tram',
            6: 'Aerial lift, Gondola',
            7: 'Funicular',
            11: 'Trolleybus',
            12: 'Monorail'
        }
        
        # Compter les types
        type_counts = routes_df['route_type'].value_counts()
        
        # Préparer les données
        labels = [transport_types.get(int(route_type), f'Type {route_type}') for route_type in type_counts.index]
        values = [int(x) for x in type_counts.values]
        
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9']
        
        return f'''
        <div id="{chart_id}" style="width: 100%; height: 500px;"></div>
        <script>
        setTimeout(function() {{
            const data = [{{
                labels: {labels},
                values: {values},
                type: 'pie',
                marker: {{
                    colors: {colors[:len(values)]},
                    line: {{color: '#FFFFFF', width: 2}}
                }},
                textinfo: '{"label+percent+value" if show_values else "label+percent"}',
                hovertemplate: '<b>%{{label}}</b><br>Lignes: %{{value}}<br>Pourcentage: %{{percent}}<extra></extra>'
            }}];
            
            const layout = {{
                title: {{
                    text: 'Répartition des lignes par type de transport',
                    font: {{size: 18}}
                }},
                showlegend: true,
                legend: {{
                    orientation: 'v',
                    x: 1.02,
                    y: 0.5
                }},
                margin: {{l: 50, r: 150, t: 80, b: 50}}
            }};
            
            Plotly.newPlot('{chart_id}', data, layout, {{responsive: true}});
        }}, 100);
        </script>
        '''
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"

@visualization_function(
    category="temporal",
    name="Volume de départs par heure",
    description="Analyse des pics de trafic - heures de pointe",
    parameters={
        "route_ids": {
            "type": "multiselect",
            "description": "Routes à analyser (vide = toutes)",
            "source": "routes"
        },
        "show_peak_hours": {
            "type": "checkbox",
            "default": True,
            "description": "Mettre en évidence les heures de pointe"
        }
    }
)
def departure_volume_by_hour(gtfs_data, **params):
    """Volume de départs par heure avec détection des pics"""
    
    selected_routes = params.get('route_ids', [])
    show_peaks = params.get('show_peak_hours', True)
    chart_id = f"plotly_{random.randint(10000, 99999)}"
    
    try:
        if 'stop_times.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichier stop_times.txt manquant</div>"
        
        stop_times_df = gtfs_data['stop_times.txt'].copy()
        
        # Filtrer par routes si spécifié
        if selected_routes and 'trips.txt' in gtfs_data:
            trips_df = gtfs_data['trips.txt']
            filtered_trips = trips_df[trips_df['route_id'].isin(selected_routes)]['trip_id']
            stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(filtered_trips)]
        
        # Extraire l'heure de départ
        stop_times_df['departure_hour'] = stop_times_df['departure_time'].str.split(':').str[0].astype(int) % 24
        
        # Compter par heure
        hourly_counts = stop_times_df['departure_hour'].value_counts().sort_index()
        
        # Créer toutes les heures
        all_hours = list(range(24))
        values = [int(hourly_counts.get(hour, 0)) for hour in all_hours]
        
        # Détecter les heures de pointe (> moyenne + 1 écart-type)
        import numpy as np
        mean_val = np.mean(values)
        std_val = np.std(values)
        peak_threshold = mean_val + std_val
        
        colors = ['#FF6B42' if val > peak_threshold and show_peaks else '#4ECDC4' for val in values]
        
        return f'''
        <div id="{chart_id}" style="width: 100%; height: 500px;"></div>
        <script>
        setTimeout(function() {{
            const data = [{{
                x: {[f"{h}h" for h in all_hours]},
                y: {values},
                type: 'bar',
                marker: {{
                    color: {colors},
                    line: {{color: '#FFFFFF', width: 1}}
                }},
                hovertemplate: '<b>%{{x}}</b><br>Départs: %{{y}}<extra></extra>'
            }}];
            
            const layout = {{
                title: {{
                    text: 'Volume de départs par heure de la journée',
                    font: {{size: 18}}
                }},
                xaxis: {{
                    title: 'Heure de la journée',
                    tickangle: -45
                }},
                yaxis: {{
                    title: 'Nombre de départs'
                }},
                annotations: {f"[{{x: 12, y: {max(values) * 0.9}, text: 'Heures de pointe en rouge', showarrow: false, font: {{color: '#FF6B42'}}}}]" if show_peaks else "[]"},
                margin: {{l: 60, r: 50, t: 80, b: 80}}
            }};
            
            Plotly.newPlot('{chart_id}', data, layout, {{responsive: true}});
        }}, 100);
        </script>
        '''
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"

@visualization_function(
    category="routes",
    name="Durée moyenne des trajets par ligne",
    description="Temps de parcours moyen avec écart-type",
    parameters={
        "top_n": {
            "type": "slider",
            "min": 5,
            "max": 20,
            "default": 10,
            "description": "Nombre de lignes à afficher"
        },
        "show_error_bars": {
            "type": "checkbox",
            "default": True,
            "description": "Afficher les écarts-types"
        }
    }
)
def trip_duration_by_route(gtfs_data, **params):
    """Durée moyenne des trajets par ligne"""
    
    top_n = params.get('top_n', 10)
    show_errors = params.get('show_error_bars', True)
    chart_id = f"plotly_{random.randint(10000, 99999)}"
    
    try:
        if 'stop_times.txt' not in gtfs_data or 'trips.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichiers stop_times.txt ou trips.txt manquants</div>"
        
        stop_times_df = gtfs_data['stop_times.txt']
        trips_df = gtfs_data['trips.txt']
        routes_df = gtfs_data.get('routes.txt', pd.DataFrame())
        
        # Calculer la durée de chaque trip
        def time_to_seconds(time_str):
            try:
                h, m, s = map(int, str(time_str).split(':'))
                return h * 3600 + m * 60 + s
            except:
                return 0
        
        # Trouver le premier et dernier arrêt de chaque trip
        trip_durations = []
        for trip_id in stop_times_df['trip_id'].unique()[:1000]:  # Limiter pour les perfs
            trip_stops = stop_times_df[stop_times_df['trip_id'] == trip_id].sort_values('stop_sequence')
            if len(trip_stops) >= 2:
                start_time = time_to_seconds(trip_stops.iloc[0]['departure_time'])
                end_time = time_to_seconds(trip_stops.iloc[-1]['arrival_time'])
                if end_time > start_time:
                    duration_minutes = (end_time - start_time) / 60
                    route_id = trips_df[trips_df['trip_id'] == trip_id]['route_id'].iloc[0] if trip_id in trips_df['trip_id'].values else None
                    if route_id:
                        trip_durations.append({'route_id': route_id, 'duration': duration_minutes})
        
        if not trip_durations:
            return "<div class='alert alert-warning'>Impossible de calculer les durées de trajet</div>"
        
        # Grouper par route
        durations_df = pd.DataFrame(trip_durations)
        route_stats = durations_df.groupby('route_id')['duration'].agg(['mean', 'std', 'count']).reset_index()
        route_stats = route_stats[route_stats['count'] >= 3].nlargest(top_n, 'mean')  # Au moins 3 trajets
        
        # Récupérer les noms de routes
        route_names = {}
        if not routes_df.empty:
            for _, route in routes_df.iterrows():
                route_id = route['route_id']
                name = route.get('route_short_name', route_id)
                if pd.isna(name) or name == '':
                    name = route.get('route_long_name', route_id)
                route_names[route_id] = str(name)
        
        labels = [route_names.get(str(route_id), str(route_id)) for route_id in route_stats['route_id']]
        means = [float(x) for x in route_stats['mean']]
        stds = [float(x) if not pd.isna(x) else 0 for x in route_stats['std']]
        
        return f'''
        <div id="{chart_id}" style="width: 100%; height: 500px;"></div>
        <script>
        setTimeout(function() {{
            const data = [{{
                x: {labels},
                y: {means},
                type: 'bar',
                marker: {{color: '#45B7D1'}},
                {"error_y: {type: 'data', array: " + str(stds) + ", visible: true}," if show_errors else ""}
                hovertemplate: '<b>%{{x}}</b><br>Durée moyenne: %{{y:.1f}} min<extra></extra>'
            }}];
            
            const layout = {{
                title: {{
                    text: 'Durée moyenne des trajets par ligne',
                    font: {{size: 18}}
                }},
                xaxis: {{
                    title: 'Lignes',
                    tickangle: -45
                }},
                yaxis: {{
                    title: 'Durée (minutes)'
                }},
                margin: {{l: 60, r: 50, t: 80, b: 100}}
            }};
            
            Plotly.newPlot('{chart_id}', data, layout, {{responsive: true}});
        }}, 100);
        </script>
        '''
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"

@visualization_function(
    category="temporal",
    name="Heatmap hebdomadaire des services",
    description="Visualisation des services par jour de la semaine et heure",
    parameters={
        "route_ids": {
            "type": "multiselect",
            "description": "Routes à analyser (vide = toutes)",
            "source": "routes"
        }
    }
)
def weekly_service_heatmap(gtfs_data, **params):
    """Heatmap des services par jour de semaine et heure"""
    
    selected_routes = params.get('route_ids', [])
    chart_id = f"plotly_{random.randint(10000, 99999)}"
    
    try:
        required_files = ['stop_times.txt', 'trips.txt', 'calendar.txt']
        for file in required_files:
            if file not in gtfs_data:
                return f"<div class='alert alert-danger'>Fichier {file} manquant</div>"
        
        stop_times_df = gtfs_data['stop_times.txt']
        trips_df = gtfs_data['trips.txt']
        calendar_df = gtfs_data['calendar.txt']
        
        # Filtrer par routes si spécifié
        if selected_routes:
            filtered_trips = trips_df[trips_df['route_id'].isin(selected_routes)]
            stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(filtered_trips['trip_id'])]
        
        # Joindre avec les services
        trips_services = trips_df[['trip_id', 'service_id']].merge(
            stop_times_df[['trip_id', 'departure_time']], on='trip_id'
        )
        
        # Extraire l'heure
        trips_services['hour'] = trips_services['departure_time'].str.split(':').str[0].astype(int) % 24
        
        # Joindre avec le calendrier
        service_schedule = trips_services.merge(calendar_df, on='service_id')
        
        # Créer la matrice heatmap
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        day_names = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
        hours = list(range(24))
        
        heatmap_data = []
        for day in days:
            day_data = []
            for hour in hours:
                # Compter les services actifs ce jour et cette heure
                active_services = service_schedule[
                    (service_schedule[day] == 1) & 
                    (service_schedule['hour'] == hour)
                ]
                count = len(active_services)
                day_data.append(count)
            heatmap_data.append(day_data)
        
        return f'''
        <div id="{chart_id}" style="width: 100%; height: 600px;"></div>
        <script>
        setTimeout(function() {{
            const data = [{{
                z: {heatmap_data},
                x: {[f"{h}h" for h in hours]},
                y: {day_names},
                type: 'heatmap',
                colorscale: [
                    [0, '#F7FBFF'],
                    [0.25, '#DEEBF7'],
                    [0.5, '#9ECAE1'],
                    [0.75, '#4292C6'],
                    [1, '#08519C']
                ],
                hovertemplate: '<b>%{{y}}</b><br>%{{x}}<br>Services: %{{z}}<extra></extra>'
            }}];
            
            const layout = {{
                title: {{
                    text: 'Heatmap des services par jour et heure',
                    font: {{size: 18}}
                }},
                xaxis: {{
                    title: 'Heure de la journée'
                }},
                yaxis: {{
                    title: 'Jour de la semaine'
                }},
                margin: {{l: 100, r: 50, t: 80, b: 80}}
            }};
            
            Plotly.newPlot('{chart_id}', data, layout, {{responsive: true}});
        }}, 100);
        </script>
        '''
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"

@visualization_function(
    category="stops",
    name="Densité géographique des arrêts",
    description="Carte de densité des arrêts de transport",
    parameters={
        "map_style": {
            "type": "select",
            "options": ["scatter", "density"],
            "default": "scatter",
            "description": "Type de visualisation"
        }
    }
)
def stops_geographic_density(gtfs_data, **params):
    """Carte de densité géographique des arrêts"""
    
    map_style = params.get('map_style', 'scatter')
    chart_id = f"plotly_{random.randint(10000, 99999)}"
    
    try:
        if 'stops.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichier stops.txt manquant</div>"
        
        stops_df = gtfs_data['stops.txt']
        
        # Vérifier les coordonnées
        valid_stops = stops_df[
            (stops_df['stop_lat'].notna()) & 
            (stops_df['stop_lon'].notna()) &
            (stops_df['stop_lat'] != 0) & 
            (stops_df['stop_lon'] != 0)
        ]
        
        if len(valid_stops) == 0:
            return "<div class='alert alert-warning'>Aucune coordonnée géographique valide trouvée</div>"
        
        lats = [float(x) for x in valid_stops['stop_lat']]
        lons = [float(x) for x in valid_stops['stop_lon']]
        names = [str(x) for x in valid_stops['stop_name']]
        
        # Calculer le centre de la carte
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        
        if map_style == 'density':
            plot_type = 'densitymapbox'
            extra_params = '''
                radius: 15,
                opacity: 0.6'''
        else:
            plot_type = 'scattermapbox'
            extra_params = '''
                mode: 'markers',
                marker: {size: 8, color: '#FF6B6B', opacity: 0.7}'''
        
        return f'''
        <div id="{chart_id}" style="width: 100%; height: 600px;"></div>
        <script>
        setTimeout(function() {{
            const data = [{{
                lat: {lats},
                lon: {lons},
                text: {names},
                type: '{plot_type}',
                {extra_params},
                hovertemplate: '<b>%{{text}}</b><br>Lat: %{{lat}}<br>Lon: %{{lon}}<extra></extra>'
            }}];
            
            const layout = {{
                title: {{
                    text: 'Répartition géographique des arrêts',
                    font: {{size: 18}}
                }},
                mapbox: {{
                    style: 'open-street-map',
                    center: {{lat: {center_lat}, lon: {center_lon}}},
                    zoom: 10
                }},
                margin: {{l: 0, r: 0, t: 80, b: 0}}
            }};
            
            Plotly.newPlot('{chart_id}', data, layout, {{responsive: true}});
        }}, 100);
        </script>
        '''
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"

@visualization_function(
    category="quality",
    name="Données manquantes par fichier",
    description="Analyse de la complétude des données GTFS",
    parameters={
        "chart_type": {
            "type": "select",
            "options": ["bar", "radar"],
            "default": "bar",
            "description": "Type de graphique"
        }
    }
)
def missing_data_analysis(gtfs_data, **params):
    """Analyse des données manquantes"""
    
    chart_type = params.get('chart_type', 'bar')
    chart_id = f"plotly_{random.randint(10000, 99999)}"
    
    try:
        file_completeness = []
        
        for filename, df in gtfs_data.items():
            if df is not None and not df.empty:
                total_cells = df.size
                missing_cells = df.isnull().sum().sum()
                completeness = ((total_cells - missing_cells) / total_cells) * 100
                
                file_completeness.append({
                    'file': filename.replace('.txt', ''),
                    'completeness': float(completeness),
                    'missing_cells': int(missing_cells),
                    'total_cells': int(total_cells)
                })
        
        if not file_completeness:
            return "<div class='alert alert-warning'>Aucune donnée à analyser</div>"
        
        files = [item['file'] for item in file_completeness]
        completeness_pct = [item['completeness'] for item in file_completeness]
        
        if chart_type == 'radar':
            return f'''
            <div id="{chart_id}" style="width: 100%; height: 600px;"></div>
            <script>
            setTimeout(function() {{
                const data = [{{
                    type: 'scatterpolar',
                    r: {completeness_pct},
                    theta: {files},
                    fill: 'toself',
                    fillcolor: 'rgba(69, 183, 209, 0.3)',
                    line: {{color: '#45B7D1', width: 2}},
                    marker: {{color: '#45B7D1', size: 8}},
                    hovertemplate: '<b>%{{theta}}</b><br>Complétude: %{{r:.1f}}%<extra></extra>'
                }}];
                
                const layout = {{
                    title: {{
                        text: 'Complétude des données GTFS',
                        font: {{size: 18}}
                    }},
                    polar: {{
                        radialaxis: {{
                            visible: true,
                            range: [0, 100],
                            ticksuffix: '%'
                        }}
                    }},
                    margin: {{l: 80, r: 80, t: 80, b: 80}}
                }};
                
                Plotly.newPlot('{chart_id}', data, layout, {{responsive: true}});
            }}, 100);
            </script>
            '''
        else:
            colors = ['#FF6B6B' if pct < 90 else '#4ECDC4' if pct < 95 else '#96CEB4' for pct in completeness_pct]
            
            return f'''
            <div id="{chart_id}" style="width: 100%; height: 500px;"></div>
            <script>
            setTimeout(function() {{
                const data = [{{
                    x: {files},
                    y: {completeness_pct},
                    type: 'bar',
                    marker: {{
                        color: {colors},
                        line: {{color: '#FFFFFF', width: 1}}
                    }},
                    hovertemplate: '<b>%{{x}}</b><br>Complétude: %{{y:.1f}}%<extra></extra>'
                }}];
                
                const layout = {{
                    title: {{
                        text: 'Complétude des données par fichier GTFS',
                        font: {{size: 18}}
                    }},
                    xaxis: {{
                        title: 'Fichiers GTFS',
                        tickangle: -45
                    }},
                    yaxis: {{
                        title: 'Complétude (%)',
                        range: [0, 100]
                    }},
                    shapes: [{{
                        type: 'line',
                        x0: -0.5, x1: {len(files) - 0.5},
                        y0: 95, y1: 95,
                        line: {{color: '#FFA500', width: 2, dash: 'dash'}}
                    }}],
                    annotations: [{{
                        x: {len(files) / 2}, y: 97,
                        text: 'Seuil recommandé (95%)',
                        showarrow: false,
                        font: {{color: '#FFA500'}}
                    }}],
                    margin: {{l: 60, r: 50, t: 80, b: 100}}
                }};
                
                Plotly.newPlot('{chart_id}', data, layout, {{responsive: true}});
            }}, 100);
            </script>
            '''
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"

@visualization_function(
    category="temporal",
    name="Fréquence par heure",
    description="Distribution des départs par heure de la journée",
    parameters={
        "route_ids": {
            "type": "multiselect",
            "description": "Routes à analyser (vide = toutes)",
            "source": "routes"
        }
    }
)
def frequency_by_hour_chartjs(gtfs_data, **params):
    """Fréquence de service par heure"""
    selected_routes = params.get('route_ids', [])
    
    # ✅ DEBUG
    print(f"Route IDs reçus: {selected_routes}")
    print(f"Type: {type(selected_routes)}")

    try:
        if 'stop_times.txt' not in gtfs_data:
            return "<div class='alert alert-danger'>Fichier stop_times.txt manquant</div>"
        
        stop_times_df = gtfs_data['stop_times.txt'].copy()
        
        # Filtrer par routes si spécifié
        if selected_routes and 'trips.txt' in gtfs_data:
            trips_df = gtfs_data['trips.txt']
            filtered_trips = trips_df[trips_df['route_id'].isin(selected_routes)]['trip_id']
            stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(filtered_trips)]
        
        # Extraire l'heure
        stop_times_df['departure_hour'] = stop_times_df['departure_time'].str.split(':').str[0].astype(int)
        stop_times_df['departure_hour'] = stop_times_df['departure_hour'] % 24  # Gérer > 24h
        
        # Compter par heure
        hourly_counts = stop_times_df['departure_hour'].value_counts().sort_index()
        
        # Créer toutes les heures de 0 à 23
        all_hours = list(range(24))
        data = [int(hourly_counts.get(hour, 0)) for hour in all_hours]
        labels = [f"{hour}h" for hour in all_hours]
        
        chart_id = generate_chart_id()
        
        html = f'''
        <div style="width: 100%; height: 400px;">
            <canvas id="{chart_id}"></canvas>
        </div>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [{{
                        label: 'Nombre de départs',
                        data: {json.dumps(data)},
                        borderColor: 'rgba(255, 99, 132, 1)',
                        backgroundColor: 'rgba(255, 99, 132, 0.2)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.4
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'Nombre de départs'
                            }}
                        }},
                        x: {{
                            title: {{
                                display: true,
                                text: 'Heure de la journée'
                            }}
                        }}
                    }},
                    plugins: {{
                        legend: {{
                            display: false
                        }}
                    }}
                }}
            }});
        }})();
        </script>
        '''
        
        return html
        
    except Exception as e:
        return f"<div class='alert alert-danger'>Erreur: {str(e)}</div>"