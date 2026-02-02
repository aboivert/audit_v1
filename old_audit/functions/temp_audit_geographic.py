"""
Fonctions d'audit pour le file_type: geographic
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="geographic",
    name="shapes_cover_all_stops",
    description="Vérifie que tous les stops d'un trip sont géométriquement proches des shapes du même trip.",
    parameters={
        "max_distance_meters": {
            "type": "number",
            "description": "Distance max autorisée (en mètres) entre un stop et un point de shape.",
            "default": 100
        }
    }
)
def shapes_cover_all_stops(gtfs_data, max_distance_meters=100, **params):
    shapes = gtfs_data.get('shapes.txt')
    stop_times = gtfs_data.get('stop_times.txt')
    stops = gtfs_data.get('stops.txt')
    trips = gtfs_data.get('trips.txt')
    if shapes is None or stop_times is None or stops is None or trips is None:
        return {}
    
    # Mapper stop_id -> (lat, lon)
    stop_coords = stops.set_index('stop_id')[['stop_lat','stop_lon']].to_dict('index')
    # Mapper shape_id -> liste de points (lat, lon)
    shape_points = shapes.groupby('shape_id').apply(
        lambda g: list(zip(g['shape_pt_lat'], g['shape_pt_lon']))
    ).to_dict()
    
    # Trips avec shape_id
    trips_shape = trips.set_index('trip_id')['shape_id'].to_dict()
    # Stop_times groupés par trip_id avec la liste des stop_ids
    stop_times_grouped = stop_times.groupby('trip_id')['stop_id'].apply(list).to_dict()
    
    trips_with_issues = {}
    
    for trip_id, stops_list in stop_times_grouped.items():
        shape_id = trips_shape.get(trip_id)
        if shape_id is None or shape_id not in shape_points:
            continue
        shape_pts = shape_points[shape_id]
        for stop_id in stops_list:
            if stop_id not in stop_coords:
                continue
            stop_coord = (stop_coords[stop_id]['stop_lat'], stop_coords[stop_id]['stop_lon'])
            distances = [geodesic(stop_coord, pt).meters for pt in shape_pts]
            min_dist = min(distances) if distances else np.inf
            if min_dist > max_distance_meters:
                if trip_id not in trips_with_issues:
                    trips_with_issues[trip_id] = []
                trips_with_issues[trip_id].append({
                    "stop_id": stop_id,
                    "min_distance_meters": round(min_dist, 2)
                })
    return {
        "trips_with_stops_far_from_shape_count": len(trips_with_issues),
        "trips_with_stops_far_from_shape": trips_with_issues
    }

@audit_function(
    file_type="geographic",
    name="stops_outliers_coordinates",
    description="Détecte les stops dont les coordonnées sont extrêmes et éloignées de tous les autres stops.",
    parameters={
        "zscore_threshold": {
            "type": "number",
            "description": "Seuil Z-score pour détecter un stop extrême.",
            "default": 3.0
        }
    }
)
def stops_outliers_coordinates(gtfs_data, zscore_threshold=3.0, **params):
    stops = gtfs_data.get('stops.txt')
    if stops is None or len(stops) == 0:
        return {}
    
    lat_z = (stops['stop_lat'] - stops['stop_lat'].mean()) / stops['stop_lat'].std(ddof=0)
    lon_z = (stops['stop_lon'] - stops['stop_lon'].mean()) / stops['stop_lon'].std(ddof=0)
    
    outliers = stops[(lat_z.abs() > zscore_threshold) | (lon_z.abs() > zscore_threshold)]
    
    return {
        "outlier_stop_count": len(outliers),
        "outlier_stop_ids": outliers['stop_id'].tolist(),
        "outlier_coords": outliers[['stop_id','stop_lat','stop_lon']].to_dict('records')
    }

@audit_function(
    file_type="geographic",
    name="distance_between_stops_consistency",
    description="Vérifie que les distances entre stops consécutifs dans un trip sont réalistes.",
    parameters={
        "max_distance_km": {
            "type": "float",
            "description": "Distance maximale réaliste entre deux stops consécutifs.",
            "default": 10.0
        },
        "min_distance_m": {
            "type": "float",
            "description": "Distance minimale réaliste entre deux stops consécutifs.",
            "default": 10.0
        }
    }
)
def distance_between_stops_consistency(gtfs_data, max_distance_km=10.0, min_distance_m=10.0, **params):
    if 'stop_times' not in gtfs_data or 'stops' not in gtfs_data or 'trips' not in gtfs_data:
        return {}

    stop_times = gtfs_data['stop_times']
    stops = gtfs_data['stops']
    trips = gtfs_data['trips']

    # Fusion des coordonnées dans stop_times
    stops_coords = stops.set_index('stop_id')[['stop_lat', 'stop_lon']]
    stop_times = stop_times.merge(stops_coords, left_on='stop_id', right_index=True, how='left')
    
    # Vérification par trip_id
    inconsistencies = []
    for trip_id, group in stop_times.groupby('trip_id'):
        group = group.sort_values('stop_sequence')
        coords = list(zip(group['stop_lat'], group['stop_lon']))
        for i in range(len(coords) - 1):
            dist = geodesic(coords[i], coords[i+1]).meters
            if dist > max_distance_km * 1000 or dist < min_distance_m:
                inconsistencies.append({
                    'trip_id': trip_id,
                    'stop_seq_start': group.iloc[i]['stop_sequence'],
                    'stop_seq_end': group.iloc[i+1]['stop_sequence'],
                    'distance_m': dist
                })

    return {
        'inconsistencies_count': len(inconsistencies),
        'inconsistencies': inconsistencies
    }

@audit_function(
    file_type="geographic",
    name="duplicate_stops_geographically_close",
    description="Détecte des stops différents très proches géographiquement (doublons potentiels).",
    parameters={
        "distance_threshold_m": {
            "type": "float",
            "description": "Seuil en mètres pour considérer deux stops comme proches.",
            "default": 10.0
        }
    }
)
def duplicate_stops_geographically_close(gtfs_data, distance_threshold_m=10.0, **params):
    if 'stops' not in gtfs_data:
        return {}

    stops = gtfs_data['stops']
    stops['coords'] = list(zip(stops['stop_lat'], stops['stop_lon']))

    duplicates = []
    coords = stops['coords'].tolist()
    stop_ids = stops['stop_id'].tolist()
    
    for i in range(len(coords)):
        for j in range(i+1, len(coords)):
            dist = geodesic(coords[i], coords[j]).meters
            if dist <= distance_threshold_m:
                duplicates.append({
                    'stop_id_1': stop_ids[i],
                    'stop_id_2': stop_ids[j],
                    'distance_m': dist
                })

    return {
        'duplicates_count': len(duplicates),
        'duplicates': duplicates
    }

@audit_function(
    file_type="geographic",
    name="shape_orientation_check",
    description="Vérifie que les points de shape suivent une progression cohérente.",
    parameters={}
)
def shape_orientation_check(gtfs_data, **params):
    if 'shapes' not in gtfs_data:
        return {}

    shapes = gtfs_data['shapes']
    inconsistencies = []
    for shape_id, group in shapes.groupby('shape_id'):
        group = group.sort_values('shape_pt_sequence')
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))

        # Calcul des angles ou vérification de la continuité (simple check)
        for i in range(len(coords) - 2):
            p1 = coords[i]
            p2 = coords[i+1]
            p3 = coords[i+2]

            # On crée vecteurs p1->p2 et p2->p3
            v1 = (p2[0]-p1[0], p2[1]-p1[1])
            v2 = (p3[0]-p2[0], p3[1]-p2[1])

            # Produit scalaire pour détecter changement brutal de direction
            dot = v1[0]*v2[0] + v1[1]*v2[1]
            norm1 = (v1[0]**2 + v1[1]**2)**0.5
            norm2 = (v2[0]**2 + v2[1]**2)**0.5
            if norm1 > 0 and norm2 > 0:
                cos_angle = dot/(norm1*norm2)
                # angle en degrés
                angle = np.arccos(np.clip(cos_angle, -1, 1)) * 180 / np.pi
                if angle > 150:  # Angle très proche de 180°, c'est un "rebroussement" suspect
                    inconsistencies.append({
                        'shape_id': shape_id,
                        'point_sequence': group.iloc[i+1]['shape_pt_sequence'],
                        'angle_deg': round(angle, 2)
                    })

    return {
        'inconsistencies_count': len(inconsistencies),
        'inconsistencies': inconsistencies
    }

@audit_function(
    file_type="geographic",
    name="stops_far_from_shape_check",
    description="Vérifie que les stops d’un trip sont proches de sa shape (distance max en mètres).",
    parameters={
        "max_distance_m": {
            "type": "float",
            "description": "Distance maximale autorisée entre stop et shape.",
            "default": 100.0
        }
    }
)
def stops_far_from_shape_check(gtfs_data, max_distance_m=100.0, **params):
    if 'stop_times' not in gtfs_data or 'stops' not in gtfs_data or 'shapes' not in gtfs_data or 'trips' not in gtfs_data:
        return {}

    stop_times = gtfs_data['stop_times']
    stops = gtfs_data['stops']
    shapes = gtfs_data['shapes']
    trips = gtfs_data['trips']

    stops_coords = stops.set_index('stop_id')[['stop_lat', 'stop_lon']]

    inconsistencies = []

    for trip_id, trip_row in trips.iterrows():
        shape_id = trip_row.get('shape_id')
        if pd.isna(shape_id):
            continue

        shape_points = shapes[shapes['shape_id'] == shape_id].sort_values('shape_pt_sequence')
        if shape_points.empty:
            continue

        line = LineString([(lat, lon) for lat, lon in zip(shape_points['shape_pt_lat'], shape_points['shape_pt_lon'])])

        trip_stop_times = stop_times[stop_times['trip_id'] == trip_id]
        for _, stop_time_row in trip_stop_times.iterrows():
            stop_id = stop_time_row['stop_id']
            stop_coord = stops_coords.loc[stop_id]
            stop_point = Point(stop_coord['stop_lat'], stop_coord['stop_lon'])

            # Distance en degrés (approx)
            dist = line.distance(stop_point)
            # Approximation simple : on peut convertir degrés à mètres (~111000m par degré lat)
            dist_m = dist * 111000

            if dist_m > max_distance_m:
                inconsistencies.append({
                    'trip_id': trip_id,
                    'stop_id': stop_id,
                    'distance_m': round(dist_m, 2)
                })

    return {
        'inconsistencies_count': len(inconsistencies),
        'inconsistencies': inconsistencies
    }

@audit_function(
    file_type="geographic",
    name="shape_point_distance_check",
    description="Détecte les segments de shape avec des distances trop longues entre points consécutifs.",
    parameters={
        "max_distance_m": {
            "type": "float",
            "description": "Distance maximale acceptable entre points consécutifs de shape.",
            "default": 2000.0
        }
    }
)
def shape_point_distance_check(gtfs_data, max_distance_m=2000.0, **params):
    if 'shapes' not in gtfs_data:
        return {}

    shapes = gtfs_data['shapes']
    problems = []

    for shape_id, group in shapes.groupby('shape_id'):
        group = group.sort_values('shape_pt_sequence')
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        for i in range(len(coords) - 1):
            dist = geodesic(coords[i], coords[i+1]).meters
            if dist > max_distance_m:
                problems.append({
                    'shape_id': shape_id,
                    'pt_seq_start': group.iloc[i]['shape_pt_sequence'],
                    'pt_seq_end': group.iloc[i+1]['shape_pt_sequence'],
                    'distance_m': dist
                })

    return {
        'problems_count': len(problems),
        'problems': problems
    }

@audit_function(
    file_type="geographic",
    name="stops_outside_urban_area",
    description="Détecte les stops qui sont très éloignés de la zone urbaine de référence.",
    parameters={
        "urban_area_bbox": {
            "type": "list",
            "description": "Bounding box [min_lat, min_lon, max_lat, max_lon] représentant la zone urbaine.",
            "default": None
        }
    }
)
def stops_outside_urban_area(gtfs_data, urban_area_bbox=None, **params):
    if 'stops' not in gtfs_data or urban_area_bbox is None:
        return {}

    stops = gtfs_data['stops']

    min_lat, min_lon, max_lat, max_lon = urban_area_bbox

    outside_stops = stops[
        (stops['stop_lat'] < min_lat) |
        (stops['stop_lat'] > max_lat) |
        (stops['stop_lon'] < min_lon) |
        (stops['stop_lon'] > max_lon)
    ]

    return {
        'outside_stops_count': len(outside_stops),
        'outside_stops': outside_stops['stop_id'].tolist()
    }

@audit_function(
    file_type="geographic",
    name="isolated_stops_detection",
    description="Détecte les stops très éloignés de tous les autres stops (stop isolé).",
    parameters={
        "distance_threshold_m": {
            "type": "float",
            "description": "Distance minimale pour considérer un stop isolé.",
            "default": 1000.0
        }
    }
)
def isolated_stops_detection(gtfs_data, distance_threshold_m=1000.0, **params):
    if 'stops' not in gtfs_data:
        return {}

    stops = gtfs_data['stops']
    coords = stops[['stop_lat', 'stop_lon']].values
    stop_ids = stops['stop_id'].tolist()

    isolated = []

    for i, coord_i in enumerate(coords):
        min_dist = float('inf')
        for j, coord_j in enumerate(coords):
            if i == j:
                continue
            dist = geodesic(coord_i, coord_j).meters
            if dist < min_dist:
                min_dist = dist
        if min_dist > distance_threshold_m:
            isolated.append({
                'stop_id': stop_ids[i],
                'min_distance_to_other_stop_m': min_dist
            })

    return {
        'isolated_stops_count': len(isolated),
        'isolated_stops': isolated
    }

