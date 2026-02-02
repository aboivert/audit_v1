"""
Fonctions d'audit pour le file_type: za
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="za",
    name="stations_without_children",
    description="Stations (location_type=1) sans aucun arrêt enfant (location_type=0) référencé.",
    parameters={}
)
def stations_without_children(gtfs_data, **params):
    if 'stops' not in gtfs_data:
        return {}

    stops = gtfs_data['stops']
    stations = stops[stops['location_type'] == 1]
    children = stops[stops['location_type'] == 0]

    # Ensemble des parent_station valides
    valid_parents = set(children['parent_station'].dropna().unique())

    # Stations sans aucun enfant
    stations_without_kids = [s for s in stations['stop_id'] if s not in valid_parents]

    return {
        "stations_without_children_count": len(stations_without_kids),
        "stations_without_children": stations_without_kids
    }

@audit_function(
    file_type="za",
    name="stops_with_invalid_parent_station",
    description="Arrêts (location_type=0) avec parent_station inexistant ou non station.",
    parameters={}
)
def stops_with_invalid_parent_station(gtfs_data, **params):
    if 'stops' not in gtfs_data:
        return {}

    stops = gtfs_data['stops']
    stops_0 = stops[stops['location_type'] == 0]
    stations_1 = stops[stops['location_type'] == 1].set_index('stop_id')

    invalid_stops = []

    for idx, row in stops_0.iterrows():
        parent = row.get('parent_station')
        if parent:
            if parent not in stations_1.index:
                invalid_stops.append({
                    'stop_id': row['stop_id'],
                    'parent_station': parent,
                    'issue': 'Parent station not found'
                })
    
    return {
        'invalid_parent_count': len(invalid_stops),
        'invalid_parent_stops': invalid_stops
    }

@audit_function(
    file_type="za",
    name="child_stops_missing_parent_station_field",
    description="Arrêts enfants potentiels sans champ parent_station renseigné.",
    parameters={
        "proximity_threshold_m": {
            "type": "float",
            "description": "Distance max pour considérer un arrêt proche d'une station.",
            "default": 100.0
        }
    }
)
def child_stops_missing_parent_station_field(gtfs_data, proximity_threshold_m=100.0, **params):
    if 'stops' not in gtfs_data:
        return {}

    stops = gtfs_data['stops']
    stations = stops[stops['location_type'] == 1][['stop_id', 'stop_lat', 'stop_lon']]
    stops_0 = stops[(stops['location_type'] == 0) & (stops['parent_station'].isna())][['stop_id', 'stop_lat', 'stop_lon']]

    missing_parent_stops = []

    for idx, stop_row in stops_0.iterrows():
        stop_coord = (stop_row['stop_lat'], stop_row['stop_lon'])
        # Chercher station la plus proche
        nearest_station = None
        min_dist = float('inf')
        for _, station_row in stations.iterrows():
            station_coord = (station_row['stop_lat'], station_row['stop_lon'])
            dist = geodesic(stop_coord, station_coord).meters
            if dist < min_dist:
                min_dist = dist
                nearest_station = station_row['stop_id']
        if min_dist <= proximity_threshold_m:
            missing_parent_stops.append({
                'stop_id': stop_row['stop_id'],
                'nearest_station': nearest_station,
                'distance_m': round(min_dist, 2)
            })

    return {
        'missing_parent_count': len(missing_parent_stops),
        'missing_parent_stops': missing_parent_stops
    }

@audit_function(
    file_type="za",
    name="stations_with_children_too_far",
    description="Stations avec arrêts enfants géographiquement trop éloignés.",
    parameters={
        "distance_threshold_m": {
            "type": "float",
            "description": "Distance max entre station et enfant.",
            "default": 500.0
        }
    }
)
def stations_with_children_too_far(gtfs_data, distance_threshold_m=500.0, **params):
    if 'stops' not in gtfs_data:
        return {}

    stops = gtfs_data['stops']
    stations = stops[stops['location_type'] == 1].set_index('stop_id')
    children = stops[(stops['location_type'] == 0) & stops['parent_station'].notna()]

    far_children = []

    for idx, row in children.iterrows():
        parent_id = row['parent_station']
        if parent_id in stations.index:
            station_coord = (stations.loc[parent_id, 'stop_lat'], stations.loc[parent_id, 'stop_lon'])
            child_coord = (row['stop_lat'], row['stop_lon'])
            dist = geodesic(station_coord, child_coord).meters
            if dist > distance_threshold_m:
                far_children.append({
                    'child_stop_id': row['stop_id'],
                    'parent_station': parent_id,
                    'distance_m': round(dist, 2)
                })

    return {
        'far_children_count': len(far_children),
        'far_children': far_children
    }

@audit_function(
    file_type="za",
    name="children_stops_same_location_as_parent",
    description="Arrêts enfants ayant exactement la même coordonnée que leur station parente (doublons potentiels).",
    parameters={}
)
def children_stops_same_location_as_parent(gtfs_data, **params):
    if 'stops' not in gtfs_data:
        return {}

    stops = gtfs_data['stops']
    stations = stops[stops['location_type'] == 1].set_index('stop_id')
    children = stops[(stops['location_type'] == 0) & stops['parent_station'].notna()]

    duplicates = []

    for idx, row in children.iterrows():
        parent_id = row['parent_station']
        if parent_id in stations.index:
            station_coord = (stations.loc[parent_id, 'stop_lat'], stations.loc[parent_id, 'stop_lon'])
            child_coord = (row['stop_lat'], row['stop_lon'])
            if abs(station_coord[0] - child_coord[0]) < 1e-6 and abs(station_coord[1] - child_coord[1]) < 1e-6:
                duplicates.append({
                    'child_stop_id': row['stop_id'],
                    'parent_station': parent_id
                })

    return {
        'duplicates_count': len(duplicates),
        'duplicates': duplicates
    }

@audit_function(
    file_type="za",
    name="stations_with_parent_station_defined",
    description="Stations (location_type=1) qui ont un parent_station défini (incohérent).",
    parameters={}
)
def stations_with_parent_station_defined(gtfs_data, **params):
    if 'stops' not in gtfs_data:
        return {}

    stations = gtfs_data['stops']
    stations_with_parent = stations[(stations['location_type'] == 1) & (stations['parent_station'].notna())]

    return {
        'stations_with_parent_count': len(stations_with_parent),
        'stations_with_parent_stops': stations_with_parent['stop_id'].tolist()
    }

