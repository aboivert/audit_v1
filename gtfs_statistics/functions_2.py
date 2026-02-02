# Exemples de fonctions simplifiées (une sortie par fonction)
# À appliquer à toutes vos fonctions existantes

from gtfs_statistics.decorators import statistics_function
import pandas as pd
import numpy as np
from datetime import datetime

# =============================================
# EXEMPLES DE FONCTIONS SIMPLIFIÉES
# =============================================

# FILES
@statistics_function(
    category="files",
    name="Fichiers présents - Liste",
    description="Liste des fichiers GTFS présents"
)
def present_files_files(gtfs_data, **params):
    """Retourne la liste des fichiers présents"""
    return list(gtfs_data.keys())

@statistics_function(
    category="files", 
    name="Fichiers présents - Nombre",
    description="Nombre de fichiers GTFS présents"
)
def present_files_count(gtfs_data, **params):
    """Retourne le nombre de fichiers présents"""
    return len(gtfs_data.keys())

@statistics_function(
    category="files",
    name="Fichiers présents - Total lignes",
    description="Nombre total de lignes dans tous les fichiers"
)
def present_files_total_rows(gtfs_data, **params):
    """Retourne le nombre total de lignes"""
    return sum(len(df) for df in gtfs_data.values())

@statistics_function(
    category="files",
    name="Fichiers obligatoires - Manquants",
    description="Liste des fichiers obligatoires manquants"
)
def required_files_missing(gtfs_data, **params):
    """Liste des fichiers obligatoires manquants"""
    required = ['agency.txt', 'routes.txt', 'trips.txt', 'stop_times.txt']
    calendar_present = 'calendar.txt' in gtfs_data or 'calendar_dates.txt' in gtfs_data
    
    missing = [f for f in required if f not in gtfs_data]
    if not calendar_present:
        missing.append('calendar.txt OR calendar_dates.txt')
    
    return missing

@statistics_function(
    category="files",
    name="Fichiers obligatoires - Tous présents",
    description="Vérifie si tous les fichiers obligatoires sont présents"
)
def required_files_all_present(gtfs_data, **params):
    """Vérifie si tous les fichiers obligatoires sont présents"""
    required = ['agency.txt', 'routes.txt', 'trips.txt', 'stop_times.txt']
    calendar_present = 'calendar.txt' in gtfs_data or 'calendar_dates.txt' in gtfs_data
    
    missing = [f for f in required if f not in gtfs_data]
    if not calendar_present:
        missing.append('calendar.txt OR calendar_dates.txt')
    
    return len(missing) == 0

@statistics_function(
    category="files",
    name="Fichiers obligatoires - Nombre manquants",
    description="Nombre de fichiers obligatoires manquants"
)
def required_files_missing_count(gtfs_data, **params):
    """Nombre de fichiers obligatoires manquants"""
    required = ['agency.txt', 'routes.txt', 'trips.txt', 'stop_times.txt']
    calendar_present = 'calendar.txt' in gtfs_data or 'calendar_dates.txt' in gtfs_data
    
    missing = [f for f in required if f not in gtfs_data]
    if not calendar_present:
        missing.append('calendar.txt OR calendar_dates.txt')
    
    return len(missing)

# AGENCY
@statistics_function(
    category="agency",
    name="Nombre d'agences",
    description="Nombre total d'agences"
)
def agency_count_count(gtfs_data, **params):
    """Compte le nombre d'agences"""
    if 'agency.txt' not in gtfs_data:
        return 0
    return len(gtfs_data['agency.txt'])

@statistics_function(
    category="agency",
    name="Agences multiples",
    description="Indique s'il y a plusieurs agences"
)
def agency_count_has_multiple(gtfs_data, **params):
    """Indique s'il y a plusieurs agences"""
    if 'agency.txt' not in gtfs_data:
        return False
    return len(gtfs_data['agency.txt']) > 1

@statistics_function(
    category="agency",
    name="Langues utilisées",
    description="Liste des langues utilisées par les agences"
)
def agency_localization_languages(gtfs_data, **params):
    """Liste des langues utilisées"""
    if 'agency.txt' not in gtfs_data:
        return []
    
    df = gtfs_data['agency.txt']
    if 'agency_lang' not in df.columns:
        return []
    
    return df['agency_timezone'].dropna().unique().tolist()

@statistics_function(
    category="agency",
    name="Nombre de fuseaux horaires",
    description="Nombre de fuseaux horaires différents"
)
def agency_localization_timezone_count(gtfs_data, **params):
    """Nombre de fuseaux horaires différents"""
    if 'agency.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['agency.txt']
    if 'agency_timezone' not in df.columns:
        return 0
    
    return len(df['agency_timezone'].dropna().unique())

@statistics_function(
    category="agency",
    name="Taux de téléphones",
    description="Pourcentage d'agences avec numéro de téléphone"
)
def agency_contact_completion_phone_rate(gtfs_data, **params):
    """Taux de complétion des téléphones"""
    if 'agency.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['agency.txt']
    total = len(df)
    
    if total == 0 or 'agency_phone' not in df.columns:
        return 0.0
    
    return round((df['agency_phone'].notna().sum() / total) * 100, 1)

@statistics_function(
    category="agency",
    name="Taux d'emails",
    description="Pourcentage d'agences avec email"
)
def agency_contact_completion_email_rate(gtfs_data, **params):
    """Taux de complétion des emails"""
    if 'agency.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['agency.txt']
    total = len(df)
    
    if total == 0 or 'agency_email' not in df.columns:
        return 0.0
    
    return round((df['agency_email'].notna().sum() / total) * 100, 1)

@statistics_function(
    category="agency",
    name="Taux d'URLs de tarification",
    description="Pourcentage d'agences avec URL de tarification"
)
def agency_contact_completion_fare_url_rate(gtfs_data, **params):
    """Taux de complétion des URLs de tarification"""
    if 'agency.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['agency.txt']
    total = len(df)
    
    if total == 0 or 'agency_fare_url' not in df.columns:
        return 0.0
    
    return round((df['agency_fare_url'].notna().sum() / total) * 100, 1)

# ROUTES
@statistics_function(
    category="routes",
    name="Nombre total de routes",
    description="Nombre total de routes"
)
def routes_count_by_type_total(gtfs_data, **params):
    """Nombre total de routes"""
    if 'routes.txt' not in gtfs_data:
        return 0
    return len(gtfs_data['routes.txt'])

@statistics_function(
    category="routes",
    name="Routes par type de transport",
    description="Répartition des routes par type de transport"
)
def routes_count_by_type_by_type(gtfs_data, **params):
    """Routes par type de transport"""
    if 'routes.txt' not in gtfs_data:
        return {}
    
    df = gtfs_data['routes.txt']
    
    # Types de transport GTFS
    transport_types = {
        0: 'Tramway/LRT', 1: 'Métro', 2: 'Train', 3: 'Bus', 4: 'Ferry',
        5: 'Téléphérique', 6: 'Télécabine', 7: 'Funiculaire', 
        11: 'Trolleybus', 12: 'Monorail'
    }
    
    by_type = {}
    if 'route_type' in df.columns:
        type_counts = df['route_type'].value_counts()
        for route_type, count in type_counts.items():
            type_name = transport_types.get(route_type, f'Type {route_type}')
            by_type[type_name] = int(count)
    
    return by_type

@statistics_function(
    category="routes",
    name="Nombre de types de transport",
    description="Nombre de types de transport différents"
)
def routes_count_by_type_type_count(gtfs_data, **params):
    """Nombre de types de transport différents"""
    if 'routes.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['routes.txt']
    
    if 'route_type' not in df.columns:
        return 0
    
    return len(df['route_type'].unique())

# STOPS
@statistics_function(
    category="stops",
    name="Nombre total d'arrêts",
    description="Nombre total d'arrêts"
)
def stops_count_by_type_total(gtfs_data, **params):
    """Nombre total d'arrêts"""
    if 'stops.txt' not in gtfs_data:
        return 0
    return len(gtfs_data['stops.txt'])

@statistics_function(
    category="stops",
    name="Arrêts par type de localisation",
    description="Répartition des arrêts par type de localisation"
)
def stops_count_by_type_by_type(gtfs_data, **params):
    """Arrêts par type de localisation"""
    if 'stops.txt' not in gtfs_data:
        return {}
    
    df = gtfs_data['stops.txt']
    
    # Types de localisation GTFS
    location_types = {
        0: 'Arrêts/Quais', 1: 'Stations', 2: 'Entrées/Sorties',
        3: 'Nœuds génériques', 4: 'Aires d\'embarquement'
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
    
    return by_type

@statistics_function(
    category="stops",
    name="Arrêts avec coordonnées",
    description="Nombre d'arrêts avec coordonnées GPS"
)
def stops_count_by_type_with_coordinates(gtfs_data, **params):
    """Nombre d'arrêts avec coordonnées"""
    if 'stops.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['stops.txt']
    
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return 0
    
    return len(df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())])

@statistics_function(
    category="stops",
    name="Taux de coordonnées",
    description="Pourcentage d'arrêts avec coordonnées GPS"
)
def stops_count_by_type_coordinates_rate(gtfs_data, **params):
    """Taux d'arrêts avec coordonnées"""
    if 'stops.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['stops.txt']
    total = len(df)
    
    if total == 0 or 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return 0.0
    
    coords_count = len(df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())])
    return round((coords_count / total * 100), 1)

# ... [Continuez ce pattern pour toutes les autres fonctions]

# EXEMPLE pour les fonctions plus complexes qui retournent des objets

@statistics_function(
    category="stops",
    name="Zone géographique - Boîte englobante",
    description="Coordonnées de la zone géographique couverte"
)
def stops_geographic_coverage_bounding_box(gtfs_data, **params):
    """Zone géographique couverte - boîte englobante"""
    if 'stops.txt' not in gtfs_data:
        return None
    
    df = gtfs_data['stops.txt']
    
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return None
    
    # Filtrer les coordonnées valides
    valid_coords = df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())]
    
    if len(valid_coords) == 0:
        return None
    
    return {
        'lat_min': round(valid_coords['stop_lat'].min(), 6),
        'lat_max': round(valid_coords['stop_lat'].max(), 6),
        'lon_min': round(valid_coords['stop_lon'].min(), 6),
        'lon_max': round(valid_coords['stop_lon'].max(), 6)
    }

@statistics_function(
    category="stops",
    name="Zone géographique - Centre",
    description="Centre géographique de la zone couverte"
)
def stops_geographic_coverage_center(gtfs_data, **params):
    """Centre géographique"""
    if 'stops.txt' not in gtfs_data:
        return None
    
    df = gtfs_data['stops.txt']
    
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return None
    
    valid_coords = df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())]
    
    if len(valid_coords) == 0:
        return None
    
    lat_min = valid_coords['stop_lat'].min()
    lat_max = valid_coords['stop_lat'].max()
    lon_min = valid_coords['stop_lon'].min()
    lon_max = valid_coords['stop_lon'].max()
    
    return {
        'lat': round((lat_min + lat_max) / 2, 6),
        'lon': round((lon_min + lon_max) / 2, 6)
    }

@statistics_function(
    category="stops",
    name="Zone géographique - Étendue",
    description="Étendue de la zone en kilomètres"
)
def stops_geographic_coverage_span_km(gtfs_data, **params):
    """Étendue géographique en km"""
    if 'stops.txt' not in gtfs_data:
        return None
    
    df = gtfs_data['stops.txt']
    
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return None
    
    valid_coords = df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())]
    
    if len(valid_coords) == 0:
        return None
    
    lat_min = valid_coords['stop_lat'].min()
    lat_max = valid_coords['stop_lat'].max()
    lon_min = valid_coords['stop_lon'].min()
    lon_max = valid_coords['stop_lon'].max()
    
    center_lat = (lat_min + lat_max) / 2
    
    # Estimation de la taille en km (approximation)
    lat_span_km = (lat_max - lat_min) * 111  # 1 degré lat ≈ 111 km
    lon_span_km = (lon_max - lon_min) * 111 * np.cos(np.radians(center_lat))
    
    return {
        'lat': round(lat_span_km, 1),
        'lon': round(lon_span_km, 1)
    }

# =============================================
# PATTERN À RÉPÉTER POUR TOUTES VOS FONCTIONS
# =============================================

"""
INSTRUCTIONS POUR CONVERTIR VOS FONCTIONS EXISTANTES:

1. Pour chaque fonction actuelle qui retourne:
   return {'key1': value1, 'key2': value2, ...}, warnings
   
2. Créez une fonction séparée pour chaque clé:
   
   @statistics_function(category="...", name="...", description="...")
   def original_function_name_key1(gtfs_data, **params):
       # Logique pour calculer value1
       return value1
   
   @statistics_function(category="...", name="...", description="...")
   def original_function_name_key2(gtfs_data, **params):
       # Logique pour calculer value2
       return value2

3. Supprimez les warnings et ne retournez que la valeur

4. Pour les objets complexes (dictionnaires, listes), vous pouvez les retourner
   directement - ils seront stockés en JSON dans la base

EXEMPLE COMPLET:
================

# Ancienne fonction:
def agency_info(gtfs_data, **params):
    return {
        'count': len(gtfs_data.get('agency.txt', [])),
        'has_multiple': len(gtfs_data.get('agency.txt', [])) > 1
    }, []

# Nouvelles fonctions:
@statistics_function(category="agency", name="Count", description="...")
def agency_info_count(gtfs_data, **params):
    return len(gtfs_data.get('agency.txt', []))

@statistics_function(category="agency", name="Has multiple", description="...")  
def agency_info_has_multiple(gtfs_data, **params):
    return len(gtfs_data.get('agency.txt', [])) > 1
"""['agency_lang'].dropna().unique().tolist()

@statistics_function(
    category="agency",
    name="Nombre de langues",
    description="Nombre de langues différentes utilisées"
)
def agency_localization_language_count(gtfs_data, **params):
    """Nombre de langues différentes"""
    if 'agency.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['agency.txt']
    if 'agency_lang' not in df.columns:
        return 0
    
    return len(df['agency_lang'].dropna().unique())

@statistics_function(
    category="agency",
    name="Fuseaux horaires",
    description="Liste des fuseaux horaires utilisés"
)
def agency_localization_timezones(gtfs_data, **params):
    """Liste des fuseaux horaires"""
    if 'agency.txt' not in gtfs_data:
        return []
    
    df = gtfs_data['agency.txt']
    if 'agency_timezone' not in df.columns:
        return []
    
    return df

# Continuation des fonctions GTFS simplifiées

# =============================================
# STOPS - ACCESSIBILITY
# =============================================

@statistics_function(
    category="stops",
    name="Arrêts avec info accessibilité - Total",
    description="Nombre d'arrêts avec information d'accessibilité"
)
def stops_accessibility_total_with_info(gtfs_data, **params):
    """Nombre d'arrêts avec info d'accessibilité"""
    if 'stops.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['stops.txt']
    
    if 'wheelchair_boarding' not in df.columns:
        return 0
    
    return int(df['wheelchair_boarding'].notna().sum())

@statistics_function(
    category="stops",
    name="Arrêts accessibles",
    description="Nombre d'arrêts accessibles en fauteuil roulant"
)
def stops_accessibility_accessible(gtfs_data, **params):
    """Nombre d'arrêts accessibles"""
    if 'stops.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['stops.txt']
    
    if 'wheelchair_boarding' not in df.columns:
        return 0
    
    return int((df['wheelchair_boarding'] == 1).sum())

@statistics_function(
    category="stops",
    name="Arrêts non accessibles",
    description="Nombre d'arrêts non accessibles en fauteuil roulant"
)
def stops_accessibility_not_accessible(gtfs_data, **params):
    """Nombre d'arrêts non accessibles"""
    if 'stops.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['stops.txt']
    
    if 'wheelchair_boarding' not in df.columns:
        return 0
    
    return int((df['wheelchair_boarding'] == 2).sum())

@statistics_function(
    category="stops",
    name="Taux d'accessibilité",
    description="Pourcentage d'arrêts accessibles parmi ceux avec info"
)
def stops_accessibility_accessibility_rate(gtfs_data, **params):
    """Taux d'accessibilité des arrêts"""
    if 'stops.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['stops.txt']
    
    if 'wheelchair_boarding' not in df.columns:
        return 0.0
    
    with_info = df['wheelchair_boarding'].notna().sum()
    accessible = (df['wheelchair_boarding'] == 1).sum()
    
    if with_info == 0:
        return 0.0
    
    return round((accessible / with_info * 100), 1)

@statistics_function(
    category="stops",
    name="Taux de complétion info accessibilité",
    description="Pourcentage d'arrêts avec information d'accessibilité"
)
def stops_accessibility_info_completion_rate(gtfs_data, **params):
    """Taux de complétion des infos d'accessibilité"""
    if 'stops.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['stops.txt']
    total_stops = len(df)
    
    if total_stops == 0 or 'wheelchair_boarding' not in df.columns:
        return 0.0
    
    with_info = df['wheelchair_boarding'].notna().sum()
    return round((with_info / total_stops * 100), 1)

# =============================================
# STOPS - FARE ZONES
# =============================================

@statistics_function(
    category="stops",
    name="Zones tarifaires - Liste",
    description="Liste des zones tarifaires"
)
def stops_fare_zones_zones(gtfs_data, **params):
    """Liste des zones tarifaires"""
    if 'stops.txt' not in gtfs_data:
        return []
    
    df = gtfs_data['stops.txt']
    
    if 'zone_id' not in df.columns:
        return []
    
    zones = df['zone_id'].dropna().unique().tolist()
    zones.sort()
    return [str(z) for z in zones]

@statistics_function(
    category="stops",
    name="Zones tarifaires - Nombre",
    description="Nombre de zones tarifaires différentes"
)
def stops_fare_zones_zone_count(gtfs_data, **params):
    """Nombre de zones tarifaires"""
    if 'stops.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['stops.txt']
    
    if 'zone_id' not in df.columns:
        return 0
    
    return len(df['zone_id'].dropna().unique())

@statistics_function(
    category="stops",
    name="Zones tarifaires - Répartition",
    description="Répartition des arrêts par zone tarifaire"
)
def stops_fare_zones_by_zone(gtfs_data, **params):
    """Répartition par zone tarifaire"""
    if 'stops.txt' not in gtfs_data:
        return {}
    
    df = gtfs_data['stops.txt']
    
    if 'zone_id' not in df.columns:
        return {}
    
    zones = df['zone_id'].dropna().unique()
    zones.sort()
    
    by_zone = {}
    for zone in zones:
        count = (df['zone_id'] == zone).sum()
        by_zone[str(zone)] = int(count)
    
    return by_zone

@statistics_function(
    category="stops",
    name="Arrêts sans zone tarifaire",
    description="Nombre d'arrêts sans zone tarifaire définie"
)
def stops_fare_zones_no_zone(gtfs_data, **params):
    """Arrêts sans zone tarifaire"""
    if 'stops.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['stops.txt']
    
    if 'zone_id' not in df.columns:
        return len(df)
    
    return int(df['zone_id'].isna().sum())

# =============================================
# TRIPS - COUNT AND INFO
# =============================================

@statistics_function(
    category="trips",
    name="Nombre total de voyages",
    description="Nombre total de trips"
)
def trips_count_and_info_total(gtfs_data, **params):
    """Nombre total de trips"""
    if 'trips.txt' not in gtfs_data:
        return 0
    return len(gtfs_data['trips.txt'])

@statistics_function(
    category="trips",
    name="Voyages avec headsign",
    description="Nombre de trips avec headsign"
)
def trips_count_and_info_with_headsign(gtfs_data, **params):
    """Trips avec headsign"""
    if 'trips.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['trips.txt']
    
    if 'trip_headsign' not in df.columns:
        return 0
    
    return int(df['trip_headsign'].notna().sum())

@statistics_function(
    category="trips",
    name="Voyages avec tracé",
    description="Nombre de trips avec shape_id"
)
def trips_count_and_info_with_shape(gtfs_data, **params):
    """Trips avec shape"""
    if 'trips.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['trips.txt']
    
    if 'shape_id' not in df.columns:
        return 0
    
    return int(df['shape_id'].notna().sum())

@statistics_function(
    category="trips",
    name="Voyages avec direction",
    description="Nombre de trips avec direction_id"
)
def trips_count_and_info_with_direction(gtfs_data, **params):
    """Trips avec direction"""
    if 'trips.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['trips.txt']
    
    if 'direction_id' not in df.columns:
        return 0
    
    return int(df['direction_id'].notna().sum())

@statistics_function(
    category="trips",
    name="Voyages avec nom court",
    description="Nombre de trips avec trip_short_name"
)
def trips_count_and_info_with_short_name(gtfs_data, **params):
    """Trips avec nom court"""
    if 'trips.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['trips.txt']
    
    if 'trip_short_name' not in df.columns:
        return 0
    
    return int(df['trip_short_name'].notna().sum())

@statistics_function(
    category="trips",
    name="Taux de headsign",
    description="Pourcentage de trips avec headsign"
)
def trips_count_and_info_headsign_rate(gtfs_data, **params):
    """Taux de headsign"""
    if 'trips.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['trips.txt']
    total = len(df)
    
    if total == 0 or 'trip_headsign' not in df.columns:
        return 0.0
    
    with_headsign = df['trip_headsign'].notna().sum()
    return round((with_headsign / total * 100), 1)

@statistics_function(
    category="trips",
    name="Taux de tracés",
    description="Pourcentage de trips avec shape_id"
)
def trips_count_and_info_shape_rate(gtfs_data, **params):
    """Taux de shapes"""
    if 'trips.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['trips.txt']
    total = len(df)
    
    if total == 0 or 'shape_id' not in df.columns:
        return 0.0
    
    with_shape = df['shape_id'].notna().sum()
    return round((with_shape / total * 100), 1)

# =============================================
# TRIPS - PER ROUTE
# =============================================

@statistics_function(
    category="trips",
    name="Nombre total de routes",
    description="Nombre total de routes définies"
)
def trips_per_route_total_routes(gtfs_data, **params):
    """Nombre total de routes"""
    if 'routes.txt' not in gtfs_data:
        return 0
    return len(gtfs_data['routes.txt'])

@statistics_function(
    category="trips",
    name="Routes avec voyages",
    description="Nombre de routes ayant des trips"
)
def trips_per_route_routes_with_trips(gtfs_data, **params):
    """Routes avec trips"""
    if 'trips.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return 0
    
    trips_df = gtfs_data['trips.txt']
    
    if 'route_id' not in trips_df.columns:
        return 0
    
    return len(trips_df['route_id'].unique())

@statistics_function(
    category="trips",
    name="Routes sans voyages",
    description="Nombre de routes sans trips"
)
def trips_per_route_routes_without_trips(gtfs_data, **params):
    """Routes sans trips"""
    if 'trips.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return len(gtfs_data.get('routes.txt', []))
    
    trips_df = gtfs_data['trips.txt']
    routes_df = gtfs_data['routes.txt']
    
    if 'route_id' not in trips_df.columns:
        return len(routes_df)
    
    total_routes = len(routes_df)
    routes_with_trips = len(trips_df['route_id'].unique())
    
    return total_routes - routes_with_trips

@statistics_function(
    category="trips",
    name="Moyenne de voyages par route",
    description="Nombre moyen de trips par route"
)
def trips_per_route_avg_trips_per_route(gtfs_data, **params):
    """Moyenne trips par route"""
    if 'trips.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return 0.0
    
    trips_df = gtfs_data['trips.txt']
    
    if 'route_id' not in trips_df.columns:
        return 0.0
    
    trips_per_route = trips_df['route_id'].value_counts()
    
    if len(trips_per_route) == 0:
        return 0.0
    
    return round(trips_per_route.mean(), 1)

@statistics_function(
    category="trips",
    name="Minimum de voyages par route",
    description="Nombre minimum de trips par route"
)
def trips_per_route_min_trips(gtfs_data, **params):
    """Minimum trips par route"""
    if 'trips.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return 0
    
    trips_df = gtfs_data['trips.txt']
    
    if 'route_id' not in trips_df.columns:
        return 0
    
    trips_per_route = trips_df['route_id'].value_counts()
    
    if len(trips_per_route) == 0:
        return 0
    
    return int(trips_per_route.min())

@statistics_function(
    category="trips",
    name="Maximum de voyages par route",
    description="Nombre maximum de trips par route"
)
def trips_per_route_max_trips(gtfs_data, **params):
    """Maximum trips par route"""
    if 'trips.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return 0
    
    trips_df = gtfs_data['trips.txt']
    
    if 'route_id' not in trips_df.columns:
        return 0
    
    trips_per_route = trips_df['route_id'].value_counts()
    
    if len(trips_per_route) == 0:
        return 0
    
    return int(trips_per_route.max())

# =============================================
# TRIPS - DIRECTION ANALYSIS
# =============================================

@statistics_function(
    category="trips",
    name="Voyages avec ID de direction",
    description="Nombre de trips avec direction_id défini"
)
def trips_direction_analysis_with_direction_id(gtfs_data, **params):
    """Trips avec direction_id"""
    if 'trips.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['trips.txt']
    
    if 'direction_id' not in df.columns:
        return 0
    
    return int(df['direction_id'].notna().sum())

@statistics_function(
    category="trips",
    name="Voyages direction 0",
    description="Nombre de trips avec direction_id = 0"
)
def trips_direction_analysis_direction_0(gtfs_data, **params):
    """Trips direction 0"""
    if 'trips.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['trips.txt']
    
    if 'direction_id' not in df.columns:
        return 0
    
    return int((df['direction_id'] == 0).sum())

@statistics_function(
    category="trips",
    name="Voyages direction 1",
    description="Nombre de trips avec direction_id = 1"
)
def trips_direction_analysis_direction_1(gtfs_data, **params):
    """Trips direction 1"""
    if 'trips.txt' not in gtfs_data:
        return 0
    
    df = gtfs_data['trips.txt']
    
    if 'direction_id' not in df.columns:
        return 0
    
    return int((df['direction_id'] == 1).sum())

@statistics_function(
    category="trips",
    name="Directions équilibrées",
    description="Indique si les directions sont équilibrées"
)
def trips_direction_analysis_balanced(gtfs_data, **params):
    """Directions équilibrées"""
    if 'trips.txt' not in gtfs_data:
        return False
    
    df = gtfs_data['trips.txt']
    
    if 'direction_id' not in df.columns:
        return False
    
    direction_0 = (df['direction_id'] == 0).sum()
    direction_1 = (df['direction_id'] == 1).sum()
    
    if direction_0 == 0 or direction_1 == 0:
        return False
    
    ratio = min(direction_0, direction_1) / max(direction_0, direction_1)
    return ratio >= 0.9

@statistics_function(
    category="trips",
    name="Taux de complétion direction",
    description="Pourcentage de trips avec direction_id"
)
def trips_direction_analysis_direction_completion_rate(gtfs_data, **params):
    """Taux de complétion direction"""
    if 'trips.txt' not in gtfs_data:
        return 0.0
    
    df = gtfs_data['trips.txt']
    total = len(df)
    
    if total == 0 or 'direction_id' not in df.columns:
        return 0.0
    
    with_direction = df['direction_id'].notna().sum()
    return round((with_direction / total * 100), 1)

# =============================================
# CALENDAR - VALIDITY PERIOD
# =============================================

@statistics_function(
    category="calendar",
    name="Date de début de validité",
    description="Date de début de validité du GTFS"
)
def calendar_validity_period_start_date(gtfs_data, **params):
    """Date de début de validité"""
    start_date = None
    
    # Vérifier calendar.txt
    if 'calendar.txt' in gtfs_data:
        cal_df = gtfs_data['calendar.txt']
        if 'start_date' in cal_df.columns and len(cal_df) > 0:
            start_date = cal_df['start_date'].min()
    
    # Vérifier calendar_dates.txt
    if 'calendar_dates.txt' in gtfs_data:
        cal_dates_df = gtfs_data['calendar_dates.txt']
        if 'date' in cal_dates_df.columns and len(cal_dates_df) > 0:
            dates_min = cal_dates_df['date'].min()
            if start_date is None or dates_min < start_date:
                start_date = dates_min
    
    return str(start_date) if start_date else None

@statistics_function(
    category="calendar",
    name="Date de fin de validité",
    description="Date de fin de validité du GTFS"
)
def calendar_validity_period_end_date(gtfs_data, **params):
    """Date de fin de validité"""
    end_date = None
    
    # Vérifier calendar.txt
    if 'calendar.txt' in gtfs_data:
        cal_df = gtfs_data['calendar.txt']
        if 'end_date' in cal_df.columns and len(cal_df) > 0:
            end_date = cal_df['end_date'].max()
    
    # Vérifier calendar_dates.txt
    if 'calendar_dates.txt' in gtfs_data:
        cal_dates_df = gtfs_data['calendar_dates.txt']
        if 'date' in cal_dates_df.columns and len(cal_dates_df) > 0:
            dates_max = cal_dates_df['date'].max()
            if end_date is None or dates_max > end_date:
                end_date = dates_max
    
    return str(end_date) if end_date else None

@statistics_function(
    category="calendar",
    name="Durée de validité en jours",
    description="Durée de validité du GTFS en jours"
)
def calendar_validity_period_duration_days(gtfs_data, **params):
    """Durée de validité en jours"""
    start_date = None
    end_date = None
    
    # Calculer start_date et end_date (même logique que ci-dessus)
    if 'calendar.txt' in gtfs_data:
        cal_df = gtfs_data['calendar.txt']
        if 'start_date' in cal_df.columns and 'end_date' in cal_df.columns and len(cal_df) > 0:
            start_date = cal_df['start_date'].min()
            end_date = cal_df['end_date'].max()
    
    if 'calendar_dates.txt' in gtfs_data:
        cal_dates_df = gtfs_data['calendar_dates.txt']
        if 'date' in cal_dates_df.columns and len(cal_dates_df) > 0:
            dates_min = cal_dates_df['date'].min()
            dates_max = cal_dates_df['date'].max()
            
            if start_date is None or dates_min < start_date:
                start_date = dates_min
            if end_date is None or dates_max > end_date:
                end_date = dates_max
    
    if start_date and end_date:
        try:
            import pandas as pd
            start_dt = pd.to_datetime(str(start_date), format='%Y%m%d')
            end_dt = pd.to_datetime(str(end_date), format='%Y%m%d')
            return (end_dt - start_dt).days + 1
        except:
            return 0
    
    return 0

@statistics_function(
    category="calendar",
    name="Période de validité valide",
    description="Indique si la période de validité est définie"
)
def calendar_validity_period_has_valid_period(gtfs_data, **params):
    """Période de validité valide"""
    start_date = calendar_validity_period_start_date(gtfs_data, **params)
    end_date = calendar_validity_period_end_date(gtfs_data, **params)
    
    return start_date is not None and end_date is not None

# [Continue avec les autres catégories: CALENDAR, SCHEDULE, QUALITY, SHAPES, FARES, TRANSFERS, PATHWAYS, EXTENSIONS, SUMMARY...]
# Le pattern est le même pour toutes les fonctions restantes