"""
Fonctions d'audit pour le file_type: redondances
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="redondances",
    name="stop_id_coordinate_variation",
    description="Détecte les stop_id définis plusieurs fois avec des coordonnées légèrement différentes.",
    parameters={
        "tolerance_meters": {
            "type": "float",
            "description": "Distance maximale tolérée entre coordonnées (en mètres).",
            "default": 10.0
        }
    }
)
def stop_id_coordinate_variation(gtfs_data, tolerance_meters=10.0, **params):
    if 'stops.txt' not in gtfs_data:
        return 0, ["stops.txt missing"]

    stops_df = gtfs_data['stops.txt']
    # On va regrouper par stop_id et vérifier la variance géographique

    # Fonction pour calculer la distance en mètres entre deux lat/lon (haversine)
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371000  # rayon de la Terre en mètres
        phi1, phi2 = np.radians(lat1), np.radians(lat2)
        dphi = np.radians(lat2 - lat1)
        dlambda = np.radians(lon2 - lon1)
        a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
        return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    problematic_stop_ids = []
    grouped = stops_df.groupby('stop_id')

    for stop_id, group in grouped:
        coords = group[['stop_lat', 'stop_lon']].dropna().values
        if len(coords) <= 1:
            continue  # Un seul point, pas de problème
        # Comparaison de chaque paire de coordonnées
        for i in range(len(coords)):
            for j in range(i+1, len(coords)):
                dist = haversine(coords[i][0], coords[i][1], coords[j][0], coords[j][1])
                if dist > tolerance_meters:
                    problematic_stop_ids.append(stop_id)
                    break
            else:
                continue
            break

    if problematic_stop_ids:
        return 0, problematic_stop_ids
    return 1, []

@audit_function(
    file_type="redondances",
    name="duplicate_trips",
    description="Détecte les trips strictement identiques (même route_id, service_id, shape_id, horaires, etc.).",
    parameters={}
)
def duplicate_trips(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data or 'stop_times.txt' not in gtfs_data:
        return 0, ["trips.txt or stop_times.txt missing"]

    trips_df = gtfs_data['trips.txt']
    stop_times_df = gtfs_data['stop_times.txt']

    # Clé composant les champs statiques d'un trip (hors horaires)
    trips_key_fields = ['route_id', 'service_id', 'shape_id', 'trip_headsign', 'direction_id']

    # On prépare un dictionnaire clé->list of trip_ids
    key_to_trip_ids = {}

    for _, trip in trips_df.iterrows():
        key = tuple(trip.get(field, None) for field in trips_key_fields)

        # On récupère la séquence ordonnée des horaires et stop_ids pour ce trip
        trip_stop_times = stop_times_df[stop_times_df['trip_id'] == trip['trip_id']]
        trip_stop_times = trip_stop_times.sort_values(by='stop_sequence')

        # On crée une tuple d’horaires (arrival_time, departure_time) et stop_id
        times_and_stops = tuple(zip(
            trip_stop_times['stop_id'],
            trip_stop_times['arrival_time'],
            trip_stop_times['departure_time']
        ))

        # On inclut aussi cette info dans la clé pour la comparaison
        full_key = (key, times_and_stops)

        if full_key not in key_to_trip_ids:
            key_to_trip_ids[full_key] = []
        key_to_trip_ids[full_key].append(trip['trip_id'])

    # Chercher les doublons
    duplicates = [trip_ids for trip_ids in key_to_trip_ids.values() if len(trip_ids) > 1]

    if duplicates:
        # Flatten la liste des groupes de doublons
        flat_dups = [trip_id for group in duplicates for trip_id in group]
        return 0, flat_dups
    return 1, []

