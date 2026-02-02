"""
Fonctions de statistiques GTFS complètes
Organisées par catégorie avec le système de décorateurs
"""

from gtfs_statistics.decorators import statistics_function
import pandas as pd
import numpy as np
from datetime import datetime
import re

# =============================================
# CATÉGORIE : FILES (Fichiers)
# =============================================

@statistics_function(
    category="files",
    name="Fichiers présents",
    description="Liste des fichiers GTFS présents dans le dataset"
)
def present_files(gtfs_data, **params):
    """Retourne la liste des fichiers présents"""
    present = list(gtfs_data.keys())
    total_rows = sum(len(df) for df in gtfs_data.values())
    
    return {
        'files': present,
        'count': len(present),
        'total_rows': total_rows
    }, []

@statistics_function(
    category="files",
    name="Fichiers obligatoires",
    description="Vérification de la présence des fichiers obligatoires"
)
def required_files(gtfs_data, **params):
    """Vérifie les fichiers obligatoires selon GTFS spec"""
    required = ['agency.txt', 'routes.txt', 'trips.txt', 'stop_times.txt']
    calendar_present = 'calendar.txt' in gtfs_data or 'calendar_dates.txt' in gtfs_data
    
    missing = [f for f in required if f not in gtfs_data]
    if not calendar_present:
        missing.append('calendar.txt OR calendar_dates.txt')
    
    return {
        'missing': missing,
        'all_present': len(missing) == 0,
        'missing_count': len(missing)
    }, missing

@statistics_function(
    category="files",
    name="Fichiers optionnels",
    description="Fichiers optionnels présents dans le dataset"
)
def optional_files(gtfs_data, **params):
    """Liste les fichiers optionnels présents"""
    optional = [
        'calendar_dates.txt', 'fare_attributes.txt', 'fare_rules.txt',
        'shapes.txt', 'frequencies.txt', 'transfers.txt', 'pathways.txt',
        'levels.txt', 'feed_info.txt', 'attributions.txt', 'translations.txt'
    ]
    
    present_optional = [f for f in optional if f in gtfs_data]
    coverage_rate = (len(present_optional) / len(optional)) * 100
    
    return {
        'present': present_optional,
        'count': len(present_optional),
        'total_optional': len(optional),
        'coverage_rate': round(coverage_rate, 1)
    }, []

# =============================================
# CATÉGORIE : AGENCY (Agences)
# =============================================

@statistics_function(
    category="agency",
    name="Nombre d'agences",
    description="Nombre total d'agences définies"
)
def agency_count(gtfs_data, **params):
    """Compte le nombre d'agences"""
    if 'agency.txt' not in gtfs_data:
        return {'count': 0, 'has_multiple': False}, []
    
    count = len(gtfs_data['agency.txt'])
    return {
        'count': count,
        'has_multiple': count > 1
    }, []

@statistics_function(
    category="agency",
    name="Langues et fuseaux",
    description="Langues et fuseaux horaires utilisés"
)
def agency_localization(gtfs_data, **params):
    """Analyse les langues et fuseaux horaires"""
    if 'agency.txt' not in gtfs_data:
        return {'languages': [], 'timezones': []}, []
    
    df = gtfs_data['agency.txt']
    
    languages = []
    if 'agency_lang' in df.columns:
        languages = df['agency_lang'].dropna().unique().tolist()
    
    timezones = []
    if 'agency_timezone' in df.columns:
        timezones = df['agency_timezone'].dropna().unique().tolist()
    
    return {
        'languages': languages,
        'language_count': len(languages),
        'timezones': timezones,
        'timezone_count': len(timezones)
    }, []



# =============================================
# CATÉGORIE : ROUTES (Routes/Lignes)
# =============================================

@statistics_function(
    category="routes",
    name="Nombre de routes",
    description="Nombre total de routes par type de transport"
)
def routes_count_by_type(gtfs_data, **params):
    """Compte les routes par type de transport"""
    if 'routes.txt' not in gtfs_data:
        return {'total': 0, 'by_type': {}}, []
    
    df = gtfs_data['routes.txt']
    total = len(df)
    
    # Types de transport GTFS
    transport_types = {
        0: 'Tramway/LRT',
        1: 'Métro',
        2: 'Train',
        3: 'Bus',
        4: 'Ferry',
        5: 'Téléphérique',
        6: 'Télécabine',
        7: 'Funiculaire',
        11: 'Trolleybus',
        12: 'Monorail'
    }
    
    by_type = {}
    if 'route_type' in df.columns:
        type_counts = df['route_type'].value_counts()
        for route_type, count in type_counts.items():
            type_name = transport_types.get(route_type, f'Type {route_type}')
            by_type[type_name] = int(count)
    
    return {
        'total': total,
        'by_type': by_type,
        'type_count': len(by_type)
    }, []

@statistics_function(
    category="routes",
    name="Complétude informations",
    description="Pourcentage de routes avec noms courts, longs et couleurs"
)
def routes_information_completion(gtfs_data, **params):
    """Calcule la complétude des informations de routes"""
    if 'routes.txt' not in gtfs_data:
        return {'short_name_rate': 0, 'long_name_rate': 0, 'color_rate': 0}, []
    
    df = gtfs_data['routes.txt']
    total = len(df)
    
    if total == 0:
        return {'short_name_rate': 0, 'long_name_rate': 0, 'color_rate': 0}, []
    
    short_name_rate = 0
    long_name_rate = 0
    color_rate = 0
    text_color_rate = 0
    
    if 'route_short_name' in df.columns:
        short_name_rate = (df['route_short_name'].notna().sum() / total) * 100
    
    if 'route_long_name' in df.columns:
        long_name_rate = (df['route_long_name'].notna().sum() / total) * 100
    
    if 'route_color' in df.columns:
        color_rate = (df['route_color'].notna().sum() / total) * 100
    
    if 'route_text_color' in df.columns:
        text_color_rate = (df['route_text_color'].notna().sum() / total) * 100
    
    return {
        'short_name_rate': round(short_name_rate, 1),
        'long_name_rate': round(long_name_rate, 1),
        'color_rate': round(color_rate, 1),
        'text_color_rate': round(text_color_rate, 1)
    }, []

@statistics_function(
    category="routes",
    name="Stratégie de nommage",
    description="Analyse des stratégies de nommage des routes"
)
def routes_naming_strategy(gtfs_data, **params):
    """Analyse les stratégies de nommage des routes"""
    if 'routes.txt' not in gtfs_data:
        return {'both_names': 0, 'only_short': 0, 'only_long': 0, 'no_name': 0}, []
    
    df = gtfs_data['routes.txt']
    
    has_short = df.get('route_short_name', pd.Series()).notna()
    has_long = df.get('route_long_name', pd.Series()).notna()
    
    both_names = (has_short & has_long).sum()
    only_short = (has_short & ~has_long).sum()
    only_long = (~has_short & has_long).sum()
    no_name = (~has_short & ~has_long).sum()
    
    # Déterminer la stratégie dominante
    strategy = 'mixed'
    if only_short > both_names and only_short > only_long:
        strategy = 'short_only'
    elif only_long > both_names and only_long > only_short:
        strategy = 'long_only'
    elif both_names > only_short and both_names > only_long:
        strategy = 'both_preferred'
    
    return {
        'both_names': int(both_names),
        'only_short': int(only_short),
        'only_long': int(only_long),
        'no_name': int(no_name),
        'strategy': strategy
    }, []

# =============================================
# CATÉGORIE : STOPS (Arrêts)
# =============================================

@statistics_function(
    category="stops",
    name="Nombre d'arrêts",
    description="Nombre total d'arrêts par type de localisation"
)
def stops_count_by_type(gtfs_data, **params):
    """Compte les arrêts par type de localisation"""
    if 'stops.txt' not in gtfs_data:
        return {'total': 0, 'by_type': {}}, []
    
    df = gtfs_data['stops.txt']
    total = len(df)
    
    # Types de localisation GTFS
    location_types = {
        0: 'Arrêts/Quais',
        1: 'Stations',
        2: 'Entrées/Sorties',
        3: 'Nœuds génériques',
        4: 'Aires d\'embarquement'
    }
    
    # Les valeurs nulles/vides sont considérées comme type 0
    df_copy = df.copy()
    if 'location_type' in df_copy.columns:
        df_copy['location_type'] = df_copy['location_type'].fillna(0)
    else:
        df_copy['location_type'] = 0
    
    by_type = {}
    type_counts = df_copy['location_type'].value_counts()
    for location_type, count in type_counts.items():
        type_name = location_types.get(location_type, f'Type {location_type}')
        by_type[type_name] = int(count)
    
    # Calcul des arrêts avec coordonnées
    coords_count = 0
    if 'stop_lat' in df.columns and 'stop_lon' in df.columns:
        coords_count = len(df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())])
    
    return {
        'total': total,
        'by_type': by_type,
        'with_coordinates': coords_count,
        'coordinates_rate': round((coords_count / total * 100) if total > 0 else 0, 1)
    }, []

@statistics_function(
    category="stops",
    name="Couverture géographique",
    description="Zone géographique couverte par les arrêts"
)
def stops_geographic_coverage(gtfs_data, **params):
    """Calcule la zone géographique couverte"""
    if 'stops.txt' not in gtfs_data:
        return {'bounding_box': None, 'center': None}, []
    
    df = gtfs_data['stops.txt']
    
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return {'bounding_box': None, 'center': None}, []
    
    # Filtrer les coordonnées valides
    valid_coords = df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())]
    
    if len(valid_coords) == 0:
        return {'bounding_box': None, 'center': None}, []
    
    lat_min = valid_coords['stop_lat'].min()
    lat_max = valid_coords['stop_lat'].max()
    lon_min = valid_coords['stop_lon'].min()
    lon_max = valid_coords['stop_lon'].max()
    
    # Centre géographique
    center_lat = (lat_min + lat_max) / 2
    center_lon = (lon_min + lon_max) / 2
    
    # Estimation de la taille en km (approximation)
    lat_span_km = (lat_max - lat_min) * 111  # 1 degré lat ≈ 111 km
    lon_span_km = (lon_max - lon_min) * 111 * np.cos(np.radians(center_lat))
    
    return {
        'bounding_box': {
            'lat_min': round(lat_min, 6),
            'lat_max': round(lat_max, 6),
            'lon_min': round(lon_min, 6),
            'lon_max': round(lon_max, 6)
        },
        'center': {
            'lat': round(center_lat, 6),
            'lon': round(center_lon, 6)
        },
        'span_km': {
            'lat': round(lat_span_km, 1),
            'lon': round(lon_span_km, 1)
        }
    }, []

@statistics_function(
    category="stops",
    name="Accessibilité",
    description="Informations d'accessibilité des arrêts"
)
def stops_accessibility(gtfs_data, **params):
    """Analyse l'accessibilité des arrêts"""
    if 'stops.txt' not in gtfs_data:
        return {'total_with_info': 0, 'accessible': 0}, []
    
    df = gtfs_data['stops.txt']
    
    if 'wheelchair_boarding' not in df.columns:
        return {'total_with_info': 0, 'accessible': 0, 'info_completion_rate': 0}, []
    
    total_stops = len(df)
    with_info = df['wheelchair_boarding'].notna().sum()
    accessible = (df['wheelchair_boarding'] == 1).sum()
    not_accessible = (df['wheelchair_boarding'] == 2).sum()
    
    info_rate = (with_info / total_stops * 100) if total_stops > 0 else 0
    accessibility_rate = (accessible / with_info * 100) if with_info > 0 else 0
    
    return {
        'total_with_info': int(with_info),
        'accessible': int(accessible),
        'not_accessible': int(not_accessible),
        'accessibility_rate': round(accessibility_rate, 1),
        'info_completion_rate': round(info_rate, 1)
    }, []

@statistics_function(
    category="stops",
    name="Zones tarifaires",
    description="Répartition des arrêts par zones tarifaires"
)
def stops_fare_zones(gtfs_data, **params):
    """Analyse les zones tarifaires"""
    if 'stops.txt' not in gtfs_data:
        return {'zones': [], 'zone_count': 0}, []
    
    df = gtfs_data['stops.txt']
    
    if 'zone_id' not in df.columns:
        return {'zones': [], 'zone_count': 0, 'no_zone': len(df)}, []
    
    zones = df['zone_id'].dropna().unique().tolist()
    zones.sort()
    
    by_zone = {}
    for zone in zones:
        count = (df['zone_id'] == zone).sum()
        by_zone[str(zone)] = int(count)
    
    no_zone = df['zone_id'].isna().sum()
    
    return {
        'zones': [str(z) for z in zones],
        'zone_count': len(zones),
        'by_zone': by_zone,
        'no_zone': int(no_zone)
    }, []

# =============================================
# CATÉGORIE : TRIPS (Voyages)
# =============================================

@statistics_function(
    category="trips",
    name="Nombre de voyages",
    description="Nombre total de trips avec informations complémentaires"
)
def trips_count_and_info(gtfs_data, **params):
    """Compte les trips avec infos complémentaires"""
    if 'trips.txt' not in gtfs_data:
        return {'total': 0}, []
    
    df = gtfs_data['trips.txt']
    total = len(df)
    
    with_headsign = 0
    with_shape = 0
    with_direction = 0
    with_short_name = 0
    
    if 'trip_headsign' in df.columns:
        with_headsign = df['trip_headsign'].notna().sum()
    
    if 'shape_id' in df.columns:
        with_shape = df['shape_id'].notna().sum()
    
    if 'direction_id' in df.columns:
        with_direction = df['direction_id'].notna().sum()
    
    if 'trip_short_name' in df.columns:
        with_short_name = df['trip_short_name'].notna().sum()
    
    return {
        'total': total,
        'with_headsign': int(with_headsign),
        'with_shape': int(with_shape),
        'with_direction': int(with_direction),
        'with_short_name': int(with_short_name),
        'headsign_rate': round((with_headsign / total * 100) if total > 0 else 0, 1),
        'shape_rate': round((with_shape / total * 100) if total > 0 else 0, 1)
    }, []

@statistics_function(
    category="trips",
    name="Voyages par route",
    description="Statistiques des voyages par route"
)
def trips_per_route(gtfs_data, **params):
    """Analyse les trips par route"""
    if 'trips.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return {'total_routes': 0, 'avg_trips_per_route': 0}, []
    
    trips_df = gtfs_data['trips.txt']
    routes_df = gtfs_data['routes.txt']
    
    if 'route_id' not in trips_df.columns:
        return {'total_routes': len(routes_df), 'routes_without_trips': len(routes_df)}, []
    
    trips_per_route = trips_df['route_id'].value_counts()
    
    total_routes = len(routes_df)
    routes_with_trips = len(trips_per_route)
    routes_without_trips = total_routes - routes_with_trips
    
    avg_trips = trips_per_route.mean() if len(trips_per_route) > 0 else 0
    min_trips = trips_per_route.min() if len(trips_per_route) > 0 else 0
    max_trips = trips_per_route.max() if len(trips_per_route) > 0 else 0
    
    return {
        'total_routes': total_routes,
        'routes_with_trips': routes_with_trips,
        'routes_without_trips': int(routes_without_trips),
        'avg_trips_per_route': round(avg_trips, 1),
        'min_trips': int(min_trips),
        'max_trips': int(max_trips)
    }, []

@statistics_function(
    category="trips",
    name="Analyse directions",
    description="Répartition des trips par direction"
)
def trips_direction_analysis(gtfs_data, **params):
    """Analyse les directions des trips"""
    if 'trips.txt' not in gtfs_data:
        return {'with_direction_id': 0, 'direction_completion_rate': 0}, []
    
    df = gtfs_data['trips.txt']
    total = len(df)
    
    if 'direction_id' not in df.columns:
        return {'with_direction_id': 0, 'direction_completion_rate': 0}, []
    
    with_direction = df['direction_id'].notna().sum()
    direction_0 = (df['direction_id'] == 0).sum()
    direction_1 = (df['direction_id'] == 1).sum()
    
    # Vérifier si les directions sont équilibrées (différence < 10%)
    balanced = False
    if direction_0 > 0 and direction_1 > 0:
        ratio = min(direction_0, direction_1) / max(direction_0, direction_1)
        balanced = bool(ratio >= 0.9)
    
    completion_rate = (with_direction / total * 100) if total > 0 else 0
    
    return {
        'with_direction_id': int(with_direction),
        'direction_0': int(direction_0),
        'direction_1': int(direction_1),
        'balanced': balanced,
        'direction_completion_rate': round(completion_rate, 1)
    }, []

# =============================================
# CATÉGORIE : CALENDAR (Calendrier)
# =============================================

@statistics_function(
    category="calendar",
    name="Période de validité",
    description="Dates de début et fin de validité du GTFS"
)
def calendar_validity_period(gtfs_data, **params):
    """Calcule la période de validité du calendrier"""
    start_date = None
    end_date = None
    duration_days = 0
    
    # Vérifier calendar.txt
    if 'calendar.txt' in gtfs_data:
        cal_df = gtfs_data['calendar.txt']
        if 'start_date' in cal_df.columns and 'end_date' in cal_df.columns:
            if len(cal_df) > 0:
                start_date = cal_df['start_date'].min()
                end_date = cal_df['end_date'].max()
    
    # Vérifier calendar_dates.txt
    if 'calendar_dates.txt' in gtfs_data:
        cal_dates_df = gtfs_data['calendar_dates.txt']
        if 'date' in cal_dates_df.columns and len(cal_dates_df) > 0:
            dates_min = cal_dates_df['date'].min()
            dates_max = cal_dates_df['date'].max()
            
            if start_date is None or dates_min < start_date:
                start_date = dates_min
            if end_date is None or dates_max > end_date:
                end_date = dates_max
    
    # Calculer la durée
    if start_date and end_date:
        try:
            start_dt = pd.to_datetime(str(start_date), format='%Y%m%d')
            end_dt = pd.to_datetime(str(end_date), format='%Y%m%d')
            duration_days = (end_dt - start_dt).days + 1
        except:
            duration_days = 0
    
    return {
        'start_date': str(start_date) if start_date else None,
        'end_date': str(end_date) if end_date else None,
        'duration_days': duration_days,
        'has_valid_period': start_date is not None and end_date is not None
    }, []

@statistics_function(
    category="calendar",
    name="Services par jour",
    description="Nombre de services actifs par jour de la semaine"
)
def calendar_services_by_day(gtfs_data, **params):
    """Compte les services par jour de la semaine"""
    if 'calendar.txt' not in gtfs_data:
        return {'by_day': {}, 'total_services': 0}, []
    
    df = gtfs_data['calendar.txt']
    total_services = len(df)
    
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    day_names = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
    
    by_day = {}
    for day, day_name in zip(days, day_names):
        if day in df.columns:
            count = (df[day] == 1).sum()
            by_day[day_name] = int(count)
        else:
            by_day[day_name] = 0
    
    return {
        'by_day': by_day,
        'total_services': total_services
    }, []

@statistics_function(
    category="calendar",
    name="Exceptions de service",
    description="Analyse des exceptions dans calendar_dates.txt"
)
def calendar_exceptions(gtfs_data, **params):
    """Analyse les exceptions de calendrier"""
    if 'calendar_dates.txt' not in gtfs_data:
        return {'total_exceptions': 0}, []
    
    df = gtfs_data['calendar_dates.txt']
    total_exceptions = len(df)
    
    if total_exceptions == 0:
        return {'total_exceptions': 0}, []
    
    services_added = 0
    services_removed = 0
    
    if 'exception_type' in df.columns:
        services_added = (df['exception_type'] == 1).sum()
        services_removed = (df['exception_type'] == 2).sum()
    
    # Service le plus affecté
    most_affected_service = None
    if 'service_id' in df.columns:
        service_counts = df['service_id'].value_counts()
        if len(service_counts) > 0:
            most_affected_service = service_counts.index[0]
    
    # Densité d'exceptions (approximation)
    exception_density = 0
    if 'calendar.txt' in gtfs_data:
        cal_df = gtfs_data['calendar.txt']
        if 'start_date' in cal_df.columns and 'end_date' in cal_df.columns:
            try:
                total_days = 0
                for _, row in cal_df.iterrows():
                    start = pd.to_datetime(str(row['start_date']), format='%Y%m%d')
                    end = pd.to_datetime(str(row['end_date']), format='%Y%m%d')
                    total_days += (end - start).days + 1
                
                if total_days > 0:
                    exception_density = total_exceptions / total_days
            except:
                pass
    
    return {
        'total_exceptions': total_exceptions,
        'services_added': int(services_added),
        'services_removed': int(services_removed),
        'most_affected_service': most_affected_service,
        'exception_density': round(exception_density, 3)
    }, []

@statistics_function(
    category="calendar",
    name="Motifs de service",
    description="Analyse des motifs de service hebdomadaire"
)
def calendar_service_patterns(gtfs_data, **params):
    """Analyse les motifs de service"""
    if 'calendar.txt' not in gtfs_data:
        return {'weekday_only': 0, 'weekend_only': 0, 'daily': 0}, []
    
    df = gtfs_data['calendar.txt']
    
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    
    # Vérifier que toutes les colonnes existent
    if not all(day in df.columns for day in days):
        return {'error': 'Missing day columns'}, []
    
    weekday_only = 0
    weekend_only = 0
    daily = 0
    custom_patterns = 0
    
    for _, row in df.iterrows():
        weekdays = row[['monday', 'tuesday', 'wednesday', 'thursday', 'friday']].sum()
        weekend = row[['saturday', 'sunday']].sum()
        total_days = row[days].sum()
        
        if weekdays == 5 and weekend == 0:
            weekday_only += 1
        elif weekdays == 0 and weekend == 2:
            weekend_only += 1
        elif total_days == 7:
            daily += 1
        else:
            custom_patterns += 1
    
    # Motif le plus commun
    patterns = {
        'Mo-Fr': weekday_only,
        'Sa-Su': weekend_only,
        'Daily': daily,
        'Custom': custom_patterns
    }
    
    most_common = max(patterns, key=patterns.get) if patterns else 'Unknown'
    
    return {
        'weekday_only': weekday_only,
        'weekend_only': weekend_only,
        'daily': daily,
        'custom_patterns': custom_patterns,
        'most_common_pattern': most_common
    }, []

# =============================================
# CATÉGORIE : SCHEDULE (Horaires)
# =============================================

@statistics_function(
    category="schedule",
    name="Plage horaire",
    description="Analyse des heures de service"
)
def schedule_time_span(gtfs_data, **params):
    """Analyse la plage horaire de service"""
    if 'stop_times.txt' not in gtfs_data:
        return {'earliest_departure': None, 'latest_arrival': None}, []
    
    df = gtfs_data['stop_times.txt']
    
    earliest_departure = None
    latest_arrival = None
    night_service = False
    
    if 'departure_time' in df.columns:
        departures = df['departure_time'].dropna()
        if len(departures) > 0:
            earliest_departure = departures.min()
    
    if 'arrival_time' in df.columns:
        arrivals = df['arrival_time'].dropna()
        if len(arrivals) > 0:
            latest_arrival = arrivals.max()
            # Service de nuit si des arrivées après 24:00:00
            night_service = any(arrival.startswith('2') for arrival in arrivals if isinstance(arrival, str))
    
    # Calcul approximatif de la plage de service
    service_span_hours = 0
    if earliest_departure and latest_arrival:
        try:
            # Conversion simple pour calculer la plage
            def time_to_minutes(time_str):
                if isinstance(time_str, str):
                    parts = time_str.split(':')
                    return int(parts[0]) * 60 + int(parts[1])
                return 0
            
            start_min = time_to_minutes(earliest_departure)
            end_min = time_to_minutes(latest_arrival)
            
            if end_min < start_min:  # Service traverse minuit
                end_min += 24 * 60
            
            service_span_hours = (end_min - start_min) / 60
        except:
            pass
    
    return {
        'earliest_departure': earliest_departure,
        'latest_arrival': latest_arrival,
        'service_span_hours': round(service_span_hours, 2),
        'night_service': night_service
    }, []

@statistics_function(
    category="schedule",
    name="Analyse fréquences",
    description="Analyse des fréquences de service"
)
def schedule_frequency_analysis(gtfs_data, **params):
    """Analyse les fréquences et stop_times"""
    stop_times_count = 0
    trips_count = 0
    
    if 'stop_times.txt' in gtfs_data:
        stop_times_count = len(gtfs_data['stop_times.txt'])
    
    if 'trips.txt' in gtfs_data:
        trips_count = len(gtfs_data['trips.txt'])
    
    avg_stops_per_trip = (stop_times_count / trips_count) if trips_count > 0 else 0
    
    # Analyse frequencies.txt
    trips_with_frequencies = 0
    if 'frequencies.txt' in gtfs_data:
        freq_df = gtfs_data['frequencies.txt']
        if 'trip_id' in freq_df.columns:
            trips_with_frequencies = freq_df['trip_id'].nunique()
    
    return {
        'total_stop_times': stop_times_count,
        'total_trips': trips_count,
        'avg_stops_per_trip': round(avg_stops_per_trip, 1),
        'trips_with_frequencies': trips_with_frequencies,
        'frequency_based_rate': round((trips_with_frequencies / trips_count * 100) if trips_count > 0 else 0, 1)
    }, []

@statistics_function(
    category="schedule",
    name="Points horaires",
    description="Analyse des points horaires vs estimations"
)
def schedule_timepoints(gtfs_data, **params):
    """Analyse les timepoints"""
    if 'stop_times.txt' not in gtfs_data:
        return {'total_stop_times': 0, 'timepoint_rate': 0}, []
    
    df = gtfs_data['stop_times.txt']
    total = len(df)
    
    exact_timepoints = 0
    estimated_times = 0
    
    if 'timepoint' in df.columns:
        exact_timepoints = (df['timepoint'] == 1).sum()
        estimated_times = (df['timepoint'] == 0).sum()
    else:
        # Si pas de colonne timepoint, supposer que tous sont exacts
        exact_timepoints = total
    
    timepoint_rate = (exact_timepoints / total * 100) if total > 0 else 0
    
    return {
        'total_stop_times': total,
        'exact_timepoints': int(exact_timepoints),
        'estimated_times': int(estimated_times),
        'timepoint_rate': round(timepoint_rate, 1)
    }, []

# =============================================
# CATÉGORIE : QUALITY (Qualité)
# =============================================

@statistics_function(
    category="quality",
    name="Complétude globale",
    description="Score de complétude général du dataset"
)
def data_completeness(gtfs_data, **params):
    """Calcule un score de complétude global"""
    scores = {}
    
    # Score agency
    agency_score = 100
    if 'agency.txt' in gtfs_data:
        df = gtfs_data['agency.txt']
        total = len(df)
        if total > 0:
            phone_rate = (df.get('agency_phone', pd.Series()).notna().sum() / total) * 100
            email_rate = (df.get('agency_email', pd.Series()).notna().sum() / total) * 100
            url_rate = (df.get('agency_url', pd.Series()).notna().sum() / total) * 100
            agency_score = (phone_rate + email_rate + url_rate) / 3
    scores['agency.txt'] = round(agency_score, 1)
    
    # Score routes
    routes_score = 100
    if 'routes.txt' in gtfs_data:
        df = gtfs_data['routes.txt']
        total = len(df)
        if total > 0:
            short_rate = (df.get('route_short_name', pd.Series()).notna().sum() / total) * 100
            long_rate = (df.get('route_long_name', pd.Series()).notna().sum() / total) * 100
            color_rate = (df.get('route_color', pd.Series()).notna().sum() / total) * 100
            routes_score = (short_rate + long_rate + color_rate) / 3
    scores['routes.txt'] = round(routes_score, 1)
    
    # Score stops
    stops_score = 100
    if 'stops.txt' in gtfs_data:
        df = gtfs_data['stops.txt']
        total = len(df)
        if total > 0:
            coords_rate = 100
            if 'stop_lat' in df.columns and 'stop_lon' in df.columns:
                coords_count = len(df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())])
                coords_rate = (coords_count / total) * 100
            
            name_rate = (df.get('stop_name', pd.Series()).notna().sum() / total) * 100
            stops_score = (coords_rate + name_rate) / 2
    scores['stops.txt'] = round(stops_score, 1)
    
    # Score trips
    trips_score = 100
    if 'trips.txt' in gtfs_data:
        df = gtfs_data['trips.txt']
        total = len(df)
        if total > 0:
            headsign_rate = (df.get('trip_headsign', pd.Series()).notna().sum() / total) * 100
            shape_rate = (df.get('shape_id', pd.Series()).notna().sum() / total) * 100
            direction_rate = (df.get('direction_id', pd.Series()).notna().sum() / total) * 100
            trips_score = (headsign_rate + shape_rate + direction_rate) / 3
    scores['trips.txt'] = round(trips_score, 1)
    
    # Score global
    overall_score = sum(scores.values()) / len(scores) if scores else 0
    
    # Attribution d'une note
    if overall_score >= 95:
        grade = 'A+'
    elif overall_score >= 90:
        grade = 'A'
    elif overall_score >= 85:
        grade = 'B+'
    elif overall_score >= 80:
        grade = 'B'
    elif overall_score >= 75:
        grade = 'C+'
    elif overall_score >= 70:
        grade = 'C'
    else:
        grade = 'D'
    
    return {
        'overall_score': round(overall_score, 1),
        'by_file': scores,
        'grade': grade
    }, []

@statistics_function(
    category="quality",
    name="Cohérence nommage",
    description="Analyse de la cohérence des noms"
)
def naming_consistency(gtfs_data, **params):
    """Analyse la cohérence du nommage"""
    all_names = []
    
    # Collecter tous les noms de l'ensemble du dataset
    for file_name, df in gtfs_data.items():
        if file_name == 'agency.txt' and 'agency_name' in df.columns:
            all_names.extend(df['agency_name'].dropna().tolist())
        elif file_name == 'routes.txt':
            if 'route_short_name' in df.columns:
                all_names.extend(df['route_short_name'].dropna().tolist())
            if 'route_long_name' in df.columns:
                all_names.extend(df['route_long_name'].dropna().tolist())
        elif file_name == 'stops.txt' and 'stop_name' in df.columns:
            all_names.extend(df['stop_name'].dropna().tolist())
    
    if not all_names:
        return {'mixed_case_rate': 0, 'all_caps_count': 0}, []
    
    total_names = len(all_names)
    all_caps_count = sum(1 for name in all_names if isinstance(name, str) and name.isupper())
    mixed_case_count = sum(1 for name in all_names if isinstance(name, str) and not name.isupper() and not name.islower())
    
    # Chercher des abréviations communes
    abbreviations = ['St.', 'Ave.', 'Blvd.', 'Rd.', 'Dr.', 'Ctr.', 'Stn.']
    abbreviations_found = sum(1 for name in all_names for abbr in abbreviations if isinstance(name, str) and abbr in name)
    
    # Caractères spéciaux
    special_chars_count = sum(1 for name in all_names if isinstance(name, str) and any(c in name for c in ['@', '#', '\'', '%', '&']))
    
    mixed_case_rate = (mixed_case_count / total_names * 100) if total_names > 0 else 0
    
    # Score de cohérence (pénaliser les majuscules et caractères spéciaux)
    naming_score = 100 - (all_caps_count / total_names * 20) - (special_chars_count / total_names * 30)
    naming_score = max(0, naming_score)
    
    return {
        'total_names': total_names,
        'mixed_case_rate': round(mixed_case_rate, 1),
        'all_caps_count': all_caps_count,
        'abbreviations_found': abbreviations_found,
        'special_chars_count': special_chars_count,
        'naming_score': round(naming_score, 1)
    }, []

@statistics_function(
    category="quality",
    name="Qualité identifiants",
    description="Analyse de la qualité des identifiants"
)
def identifier_quality(gtfs_data, **params):
    """Analyse la qualité des identifiants"""
    all_ids = []
    
    # Collecter tous les IDs
    for file_name, df in gtfs_data.items():
        if file_name == 'agency.txt' and 'agency_id' in df.columns:
            all_ids.extend(df['agency_id'].dropna().astype(str).tolist())
        elif file_name == 'routes.txt' and 'route_id' in df.columns:
            all_ids.extend(df['route_id'].dropna().astype(str).tolist())
        elif file_name == 'stops.txt' and 'stop_id' in df.columns:
            all_ids.extend(df['stop_id'].dropna().astype(str).tolist())
        elif file_name == 'trips.txt' and 'trip_id' in df.columns:
            all_ids.extend(df['trip_id'].dropna().astype(str).tolist())
    
    if not all_ids:
        return {'numeric_ids': 0, 'alphanumeric_ids': 0}, []
    
    total_ids = len(all_ids)
    numeric_ids = sum(1 for id_val in all_ids if id_val.isdigit())
    alphanumeric_ids = sum(1 for id_val in all_ids if id_val.isalnum() and not id_val.isdigit())
    descriptive_ids = sum(1 for id_val in all_ids if '_' in id_val or '-' in id_val)
    
    avg_id_length = sum(len(id_val) for id_val in all_ids) / total_ids if total_ids > 0 else 0
    
    # Score de consistance (préférer les IDs courts et cohérents)
    length_consistency = 100 - abs(avg_id_length - 8) * 5  # Pénaliser si trop long ou trop court
    type_consistency = max(numeric_ids, alphanumeric_ids) / total_ids * 100  # Récompenser la cohérence de type
    
    consistency_score = (length_consistency + type_consistency) / 2
    consistency_score = max(0, min(100, consistency_score))
    
    return {
        'total_ids': total_ids,
        'numeric_ids': round((numeric_ids / total_ids * 100), 1),
        'alphanumeric_ids': round((alphanumeric_ids / total_ids * 100), 1),
        'descriptive_ids': round((descriptive_ids / total_ids * 100), 1),
        'avg_id_length': round(avg_id_length, 1),
        'id_consistency_score': round(consistency_score, 1)
    }, []

# =============================================
# CATÉGORIE : SHAPES (Tracés)
# =============================================

@statistics_function(
    category="shapes",
    name="Couverture tracés",
    description="Analyse de la couverture des tracés de lignes"
)
def shapes_coverage(gtfs_data, **params):
    """Analyse la couverture des shapes"""
    total_shapes = 0
    trips_with_shapes = 0
    trips_without_shapes = 0
    avg_points_per_shape = 0
    
    if 'shapes.txt' in gtfs_data:
        shapes_df = gtfs_data['shapes.txt']
        if 'shape_id' in shapes_df.columns:
            total_shapes = shapes_df['shape_id'].nunique()
            avg_points_per_shape = len(shapes_df) / total_shapes if total_shapes > 0 else 0
    
    if 'trips.txt' in gtfs_data:
        trips_df = gtfs_data['trips.txt']
        if 'shape_id' in trips_df.columns:
            trips_with_shapes = trips_df['shape_id'].notna().sum()
            trips_without_shapes = trips_df['shape_id'].isna().sum()
    
    total_trips = trips_with_shapes + trips_without_shapes
    shape_coverage_rate = (trips_with_shapes / total_trips * 100) if total_trips > 0 else 0
    
    return {
        'total_shapes': total_shapes,
        'trips_with_shapes': int(trips_with_shapes),
        'trips_without_shapes': int(trips_without_shapes),
        'shape_coverage_rate': round(shape_coverage_rate, 1),
        'avg_points_per_shape': round(avg_points_per_shape, 1)
    }, []

@statistics_function(
    category="shapes",
    name="Qualité tracés",
    description="Analyse de la qualité des données de tracé"
)
def shapes_quality(gtfs_data, **params):
    """Analyse la qualité des shapes"""
    if 'shapes.txt' not in gtfs_data:
        return {'with_distances': 0, 'distance_coverage_rate': 0}, []
    
    df = gtfs_data['shapes.txt']
    total_points = len(df)
    
    with_distances = 0
    if 'shape_dist_traveled' in df.columns:
        with_distances = df['shape_dist_traveled'].notna().sum()
    
    distance_coverage_rate = (with_distances / total_points * 100) if total_points > 0 else 0
    
    # Estimation approximative de la longueur moyenne des shapes
    avg_shape_length_km = 0
    total_network_length_km = 0
    
    if 'shape_id' in df.columns and 'shape_dist_traveled' in df.columns:
        shape_lengths = []
        for shape_id in df['shape_id'].unique():
            shape_data = df[df['shape_id'] == shape_id]
            if 'shape_dist_traveled' in shape_data.columns:
                distances = shape_data['shape_dist_traveled'].dropna()
                if len(distances) > 0:
                    max_dist = distances.max()
                    shape_lengths.append(max_dist)
        
        if shape_lengths:
            avg_shape_length_km = sum(shape_lengths) / len(shape_lengths) / 1000  # Convertir en km
            total_network_length_km = sum(shape_lengths) / 1000
    
    return {
        'total_points': total_points,
        'with_distances': int(with_distances),
        'distance_coverage_rate': round(distance_coverage_rate, 1),
        'avg_shape_length_km': round(avg_shape_length_km, 2),
        'total_network_length_km': round(total_network_length_km, 2)
    }, []

# =============================================
# CATÉGORIE : FARES (Tarification)
# =============================================

@statistics_function(
    category="fares",
    name="Vue d'ensemble tarifs",
    description="Analyse du système tarifaire (V1 et V2)"
)
def fare_system_overview(gtfs_data, **params):
    """Analyse le système tarifaire"""
    has_fares_v1 = 'fare_attributes.txt' in gtfs_data
    has_fares_v2 = 'fare_products.txt' in gtfs_data
    
    fare_attributes_count = 0
    fare_rules_count = 0
    currencies = []
    price_range = {'min': 0, 'max': 0, 'avg': 0}
    
    if has_fares_v1:
        # Analyse Fares V1
        if 'fare_attributes.txt' in gtfs_data:
            fare_attr_df = gtfs_data['fare_attributes.txt']
            fare_attributes_count = len(fare_attr_df)
            
            if 'currency_type' in fare_attr_df.columns:
                currencies = fare_attr_df['currency_type'].dropna().unique().tolist()
            
            if 'price' in fare_attr_df.columns:
                prices = fare_attr_df['price'].dropna()
                if len(prices) > 0:
                    price_range = {
                        'min': float(prices.min()),
                        'max': float(prices.max()),
                        'avg': float(prices.mean())
                    }
        
        if 'fare_rules.txt' in gtfs_data:
            fare_rules_count = len(gtfs_data['fare_rules.txt'])
    
    return {
        'has_fares_v1': has_fares_v1,
        'has_fares_v2': has_fares_v2,
        'fare_attributes_count': fare_attributes_count,
        'fare_rules_count': fare_rules_count,
        'currencies': currencies,
        'price_range': price_range
    }, []

# =============================================
# CATÉGORIE : TRANSFERS (Correspondances)
# =============================================

@statistics_function(
    category="transfers",
    name="Règles de correspondance",
    description="Analyse des règles de correspondance définies"
)
def transfer_rules(gtfs_data, **params):
    """Analyse les règles de transfer"""
    if 'transfers.txt' not in gtfs_data:
        return {'total_transfers': 0}, []
    
    df = gtfs_data['transfers.txt']
    total_transfers = len(df)
    
    by_type = {
        'Recommended': 0,
        'Timed': 0,
        'Minimum time': 0,
        'Not possible': 0,
        'In-seat': 0
    }
    
    if 'transfer_type' in df.columns:
        type_counts = df['transfer_type'].value_counts()
        for transfer_type, count in type_counts.items():
            if transfer_type == 0:
                by_type['Recommended'] = int(count)
            elif transfer_type == 1:
                by_type['Timed'] = int(count)
            elif transfer_type == 2:
                by_type['Minimum time'] = int(count)
            elif transfer_type == 3:
                by_type['Not possible'] = int(count)
            elif transfer_type == 4:
                by_type['In-seat'] = int(count)
    
    with_min_time = 0
    avg_transfer_time = 0
    
    if 'min_transfer_time' in df.columns:
        with_min_time = df['min_transfer_time'].notna().sum()
        if with_min_time > 0:
            avg_transfer_time = df['min_transfer_time'].dropna().mean()
    
    return {
        'total_transfers': total_transfers,
        'by_type': by_type,
        'with_min_time': int(with_min_time),
        'avg_transfer_time': round(avg_transfer_time, 0)
    }, []

# =============================================
# CATÉGORIE : PATHWAYS (Cheminements)
# =============================================

@statistics_function(
    category="pathways",
    name="Couverture cheminements",
    description="Analyse des cheminements dans les stations"
)
def pathway_coverage(gtfs_data, **params):
    """Analyse les pathways"""
    if 'pathways.txt' not in gtfs_data:
        return {'stations_with_pathways': 0, 'total_pathways': 0}, []
    
    pathways_df = gtfs_data['pathways.txt']
    total_pathways = len(pathways_df)
    
    # Stations avec pathways
    stations_with_pathways = 0
    if 'stops.txt' in gtfs_data:
        stops_df = gtfs_data['stops.txt']
        total_stations = len(stops_df[stops_df.get('location_type', 0) == 1]) if 'location_type' in stops_df.columns else 0
        
        if 'from_stop_id' in pathways_df.columns:
            pathway_stops = set(pathways_df['from_stop_id'].unique()) | set(pathways_df['to_stop_id'].unique() if 'to_stop_id' in pathways_df.columns else [])
            # Compter combien de ces stops sont des stations
            if 'location_type' in stops_df.columns:
                station_stops = stops_df[stops_df['location_type'] == 1]['stop_id'].unique()
                stations_with_pathways = len(set(station_stops) & pathway_stops)
        
        pathway_coverage_rate = (stations_with_pathways / total_stations * 100) if total_stations > 0 else 0
    else:
        pathway_coverage_rate = 0
    
    # Répartition par mode
    by_mode = {}
    if 'pathway_mode' in pathways_df.columns:
        mode_names = {
            1: 'Walkway',
            2: 'Stairs',
            3: 'Moving sidewalk',
            4: 'Escalator',
            5: 'Elevator',
            6: 'Fare gate',
            7: 'Exit gate'
        }
        
        mode_counts = pathways_df['pathway_mode'].value_counts()
        for mode, count in mode_counts.items():
            mode_name = mode_names.get(mode, f'Mode {mode}')
            by_mode[mode_name] = int(count)
    
    return {
        'stations_with_pathways': stations_with_pathways,
        'total_pathways': total_pathways,
        'pathway_coverage_rate': round(pathway_coverage_rate, 1),
        'by_mode': by_mode
    }, []

# =============================================
# CATÉGORIE : EXTENSIONS (Extensions)
# =============================================

@statistics_function(
    category="extensions",
    name="Extensions GTFS",
    description="Détection des extensions et champs non-standard"
)
def gtfs_extensions(gtfs_data, **params):
    """Détecte les extensions GTFS"""
    # Fichiers d'extension standard
    extension_files = []
    standard_extensions = [
        'attributions.txt', 'translations.txt', 'feed_info.txt',
        'pathways.txt', 'levels.txt', 'frequencies.txt'
    ]
    
    for ext_file in standard_extensions:
        if ext_file in gtfs_data:
            extension_files.append(ext_file)
    
    # Champs personnalisés (approximation)
    custom_fields = []
    
    # Vérifier quelques champs d'extension connus
    if 'routes.txt' in gtfs_data:
        routes_df = gtfs_data['routes.txt']
        if 'route_sort_order' in routes_df.columns:
            custom_fields.append('routes.route_sort_order')
    
    if 'stops.txt' in gtfs_data:
        stops_df = gtfs_data['stops.txt']
        if 'platform_code' in stops_df.columns:
            custom_fields.append('stops.platform_code')
    
    # Niveau de conformité
    base_files = ['agency.txt', 'routes.txt', 'trips.txt', 'stops.txt', 'stop_times.txt']
    has_base = all(f in gtfs_data for f in base_files)
    
    if not has_base:
        compliance_level = 'Non-compliant'
    elif len(extension_files) == 0:
        compliance_level = 'Standard'
    else:
        compliance_level = 'Standard+'
    
    return {
        'extension_files': extension_files,
        'custom_fields': custom_fields,
        'compliance_level': compliance_level,
        'has_realtime_fields': False,  # Serait plus complexe à détecter
        'has_flex_fields': 'booking_rules.txt' in gtfs_data
    }, []

# =============================================
# CATÉGORIE : SUMMARY (Résumé)
# =============================================

@statistics_function(
    category="summary",
    name="Vue d'ensemble",
    description="Résumé exécutif complet du dataset"
)
def dataset_overview(gtfs_data, **params):
    """Génère un résumé exécutif du dataset"""
    # Taille du dataset
    total_rows = sum(len(df) for df in gtfs_data.values())
    if total_rows < 10000:
        dataset_size = 'Small'
    elif total_rows < 100000:
        dataset_size = 'Medium'
    elif total_rows < 1000000:
        dataset_size = 'Large'
    else:
        dataset_size = 'XLarge'
    
    # Modes de transport
    transport_modes = []
    if 'routes.txt' in gtfs_data:
        routes_df = gtfs_data['routes.txt']
        if 'route_type' in routes_df.columns:
            type_mapping = {0: 'Tram', 1: 'Metro', 2: 'Rail', 3: 'Bus', 4: 'Ferry'}
            unique_types = routes_df['route_type'].unique()
            transport_modes = [type_mapping.get(t, f'Type{t}') for t in unique_types if t in type_mapping]
    
    # Portée géographique (approximation)
    geographic_scope = 'Urban'  # Par défaut
    if 'stops.txt' in gtfs_data:
        stops_df = gtfs_data['stops.txt']
        if 'stop_lat' in stops_df.columns and 'stop_lon' in stops_df.columns:
            valid_coords = stops_df[(stops_df['stop_lat'].notna()) & (stops_df['stop_lon'].notna())]
            if len(valid_coords) > 0:
                lat_span = valid_coords['stop_lat'].max() - valid_coords['stop_lat'].min()
                if lat_span > 2:  # Plus de 2 degrés de latitude
                    geographic_scope = 'Regional'
                elif lat_span > 5:
                    geographic_scope = 'National'
    
    # Qualité des données (approximation)
    completeness_score = 75  # Score par défaut
    
    # Calcul rapide basé sur la présence de champs optionnels
    quality_indicators = 0
    total_indicators = 0
    
    if 'agency.txt' in gtfs_data:
        agency_df = gtfs_data['agency.txt']
        if 'agency_phone' in agency_df.columns:
            quality_indicators += agency_df['agency_phone'].notna().sum()
        total_indicators += len(agency_df)
    
    if 'routes.txt' in gtfs_data:
        routes_df = gtfs_data['routes.txt']
        if 'route_color' in routes_df.columns:
            quality_indicators += routes_df['route_color'].notna().sum()
        total_indicators += len(routes_df)
    
    if total_indicators > 0:
        completeness_score = (quality_indicators / total_indicators) * 100
    
    # Classification de la qualité
    if completeness_score >= 90:
        data_quality = 'Excellent'
    elif completeness_score >= 75:
        data_quality = 'Good'
    elif completeness_score >= 60:
        data_quality = 'Fair'
    else:
        data_quality = 'Poor'
    
    # Niveau de service (basé sur le nombre de trips)
    service_level = 'Medium'
    if 'trips.txt' in gtfs_data:
        trips_count = len(gtfs_data['trips.txt'])
        if trips_count > 10000:
            service_level = 'High'
        elif trips_count < 1000:
            service_level = 'Low'
    
    # Points forts et faiblesses
    key_strengths = []
    key_weaknesses = []
    
    # Analyse des forces
    if 'stops.txt' in gtfs_data:
        stops_df = gtfs_data['stops.txt']
        if 'stop_lat' in stops_df.columns and 'stop_lon' in stops_df.columns:
            coords_rate = len(stops_df[(stops_df['stop_lat'].notna()) & (stops_df['stop_lon'].notna())]) / len(stops_df)
            if coords_rate > 0.95:
                key_strengths.append('Complete stops coordinates')
    
    if 'shapes.txt' in gtfs_data and len(gtfs_data['shapes.txt']) > 0:
        key_strengths.append('Good shapes coverage')
    
    if 'pathways.txt' in gtfs_data and len(gtfs_data['pathways.txt']) > 0:
        key_strengths.append('Station pathways defined')
    
    # Analyse des faiblesses
    if 'fare_attributes.txt' not in gtfs_data:
        key_weaknesses.append('Missing fare information')
    
    if 'routes.txt' in gtfs_data:
        routes_df = gtfs_data['routes.txt']
        if 'route_color' in routes_df.columns:
            color_rate = routes_df['route_color'].notna().sum() / len(routes_df)
            if color_rate < 0.5:
                key_weaknesses.append('Limited route colors')
    
    if 'pathways.txt' not in gtfs_data:
        key_weaknesses.append('No pathways information')
    
    # Recommandations d'amélioration
    recommended_improvements = []
    if 'Missing fare information' in key_weaknesses:
        recommended_improvements.append('Add fare information')
    if 'Limited route colors' in key_weaknesses:
        recommended_improvements.append('Improve route colors')
    if 'No pathways information' in key_weaknesses:
        recommended_improvements.append('Add station pathways')
    
    return {
        'dataset_size': dataset_size,
        'geographic_scope': geographic_scope,
        'transport_modes': transport_modes,
        'service_level': service_level,
        'data_quality': data_quality,
        'completeness_score': round(completeness_score, 1),
        'key_strengths': key_strengths,
        'key_weaknesses': key_weaknesses,
        'recommended_improvements': recommended_improvements
    }, []