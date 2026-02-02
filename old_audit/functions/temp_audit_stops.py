"""
Fonctions d'audit pour le file_type: stops
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="stops",
    name="check_required_columns",
    description="Vérifie la présence des colonnes obligatoires dans stops.txt",
    parameters={}
)
def check_required_columns_stops(gtfs_data, **params):
    required_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    missing_columns = [col for col in required_columns if col not in df.columns]
    return {"missing_columns": missing_columns, "all_present": len(missing_columns) == 0}

@audit_function(
    file_type="stops",
    name="check_duplicate_stop_id",
    description="Vérifie les doublons sur stop_id",
    parameters={}
)
def check_duplicate_stop_id(gtfs_data, **params):
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    duplicates = df['stop_id'][df['stop_id'].duplicated(keep=False)]
    return {
        "duplicate_stop_id_count": duplicates.nunique(),
        "duplicate_stop_ids": duplicates.unique().tolist()
    }

@audit_function(
    file_type="stops",
    name="validate_stop_lat_lon",
    description="Vérifie que stop_lat et stop_lon sont dans les limites géographiques valides",
    parameters={}
)
def validate_stop_lat_lon(gtfs_data, **params):
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    errors = []
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return {"missing_column": True}
    for idx, row in df.iterrows():
        lat = row['stop_lat']
        lon = row['stop_lon']
        if lat is None or lon is None:
            errors.append({"stop_id": row.get('stop_id', None), "error": "Latitude or Longitude missing"})
            continue
        if not (-90 <= lat <= 90):
            errors.append({"stop_id": row.get('stop_id', None), "error": f"Latitude out of range: {lat}"})
        if not (-180 <= lon <= 180):
            errors.append({"stop_id": row.get('stop_id', None), "error": f"Longitude out of range: {lon}"})
    return {
        "invalid_lat_lon_count": len(errors),
        "errors": errors
    }

@audit_function(
    file_type="stops",
    name="check_stop_name_uniqueness_per_location",
    description="Vérifie si plusieurs stops ont le même nom mais des coordonnées très différentes",
    parameters={
        "distance_threshold_meters": {
            "type": "number",
            "description": "Distance seuil en mètres pour considérer deux stops au même emplacement",
            "default": 50
        }
    }
)
def check_stop_name_uniqueness_per_location(gtfs_data, distance_threshold_meters=50, **params):
    import numpy as np
    from geopy.distance import geodesic

    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if not {'stop_name', 'stop_lat', 'stop_lon'}.issubset(df.columns):
        return {"missing_column": True}
    
    grouped = df.groupby('stop_name')
    suspicious = []
    for stop_name, group in grouped:
        coords = list(zip(group['stop_lat'], group['stop_lon']))
        # Compare pairwise distances
        for i in range(len(coords)):
            for j in range(i+1, len(coords)):
                dist = geodesic(coords[i], coords[j]).meters
                if dist > distance_threshold_meters:
                    suspicious.append({
                        "stop_name": stop_name,
                        "stop_id_1": group.iloc[i]['stop_id'],
                        "coords_1": coords[i],
                        "stop_id_2": group.iloc[j]['stop_id'],
                        "coords_2": coords[j],
                        "distance_m": dist
                    })
    return {
        "count_suspicious": len(suspicious),
        "details": suspicious
    }

@audit_function(
    file_type="stops",
    name="missing_values_stats",
    description="Calcule les valeurs manquantes par colonne dans stops.txt",
    parameters={}
)
def missing_values_stats_stops(gtfs_data, **params):
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    empty_counts = df.isna().sum().to_dict()
    empty_rate = {col: round((count / len(df)) * 100, 2) for col, count in empty_counts.items()}
    return {
        "empty_counts": empty_counts,
        "empty_rate": empty_rate,
        "has_missing_values": any(count > 0 for count in empty_counts.values())
    }

@audit_function(
    file_type="stops",
    name="validate_location_type",
    description="Vérifie que location_type, s'il est présent, respecte les valeurs autorisées (0,1,2,3)",
    parameters={}
)
def validate_location_type(gtfs_data, **params):
    allowed_values = {0, 1, 2, 3}
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'location_type' not in df.columns:
        return {"missing_column": True}
    invalid_vals = df.loc[~df['location_type'].isin(allowed_values), 'location_type'].dropna().unique().tolist()
    return {
        "invalid_location_type_count": len(invalid_vals),
        "invalid_location_types": invalid_vals
    }

@audit_function(
    file_type="stops",
    name="check_parent_station_consistency",
    description="Vérifie la cohérence entre parent_station et location_type",
    parameters={}
)
def check_parent_station_consistency(gtfs_data, **params):
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']

    if 'location_type' not in df.columns or 'parent_station' not in df.columns:
        return {"missing_column": True}
    
    inconsistent = []
    for idx, row in df.iterrows():
        loc_type = row.get('location_type', None)
        parent = row.get('parent_station', None)
        # location_type 1 = station, 2=entrance/exit, 3=generic node
        # Si location_type = 1, parent_station doit être vide
        if loc_type == 1 and parent:
            inconsistent.append({"stop_id": row['stop_id'], "error": "location_type=1 but parent_station is not empty"})
        # Si location_type in (2,3) parent_station doit être renseigné
        if loc_type in {2,3} and (not parent or pd.isna(parent)):
            inconsistent.append({"stop_id": row['stop_id'], "error": f"location_type={loc_type} but parent_station is empty"})
    return {
        "inconsistencies_count": len(inconsistent),
        "details": inconsistent
    }

@audit_function(
    file_type="stops",
    name="validate_wheelchair_boarding",
    description="Vérifie la validité du champ wheelchair_boarding",
    parameters={}
)
def validate_wheelchair_boarding(gtfs_data, **params):
    allowed_values = {0, 1, 2, 3}
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'wheelchair_boarding' not in df.columns:
        return {"missing_column": True}
    invalid_vals = df.loc[~df['wheelchair_boarding'].isin(allowed_values), 'wheelchair_boarding'].dropna().unique().tolist()
    return {
        "invalid_wheelchair_boarding_count": len(invalid_vals),
        "invalid_wheelchair_boarding_values": invalid_vals
    }

@audit_function(
    file_type="stops",
    name="check_stop_code_format",
    description="Vérifie le format et la présence du champ stop_code si présent (souvent codes courts utilisés par opérateurs)",
    parameters={}
)
def check_stop_code_format(gtfs_data, **params):
    import re
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'stop_code' not in df.columns:
        return {"missing_column": True}
    invalid_codes = []
    # Exemple: vérifier si stop_code est alphanumérique et non vide
    pattern = re.compile(r'^[A-Za-z0-9\-]+$')
    for idx, val in df['stop_code'].dropna().iteritems():
        if not pattern.match(str(val)):
            invalid_codes.append({"index": idx, "stop_code": val})
    return {
        "invalid_stop_code_count": len(invalid_codes),
        "invalid_stop_codes": invalid_codes
    }

@audit_function(
    file_type="stops",
    name="detect_nearby_duplicate_stops",
    description="Détecte les arrêts très proches géographiquement (moins de 10m), suspicion doublons",
    parameters={
        "distance_threshold_meters": {
            "type": "number",
            "description": "Distance seuil en mètres",
            "default": 10
        }
    }
)
def detect_nearby_duplicate_stops(gtfs_data, distance_threshold_meters=10, **params):
    from geopy.distance import geodesic
    import itertools

    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if not {'stop_id', 'stop_lat', 'stop_lon'}.issubset(df.columns):
        return {"missing_column": True}

    coords = list(zip(df['stop_lat'], df['stop_lon']))
    stop_ids = df['stop_id'].tolist()
    close_pairs = []
    # Compare chaque paire (complexité O(n^2) mais on suppose dataset raisonnable)
    for i, j in itertools.combinations(range(len(coords)), 2):
        dist = geodesic(coords[i], coords[j]).meters
        if dist <= distance_threshold_meters:
            close_pairs.append({
                "stop_id_1": stop_ids[i],
                "stop_id_2": stop_ids[j],
                "distance_m": round(dist, 2)
            })
    return {
        "count_close_pairs": len(close_pairs),
        "close_pairs": close_pairs
    }

@audit_function(
    file_type="stops",
    name="check_parent_station_cycles",
    description="Détecte des cycles dans la hiérarchie parent_station (ex: A->B->A)",
    parameters={}
)
def check_parent_station_cycles(gtfs_data, **params):
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'stop_id' not in df.columns or 'parent_station' not in df.columns:
        return {"missing_column": True}

    parent_map = df.set_index('stop_id')['parent_station'].to_dict()
    cycles = []
    
    def find_cycle(start_id):
        visited = set()
        current = start_id
        while current is not None and current in parent_map:
            if current in visited:
                return True
            visited.add(current)
            current = parent_map.get(current)
        return False
    
    for stop_id in parent_map.keys():
        if find_cycle(stop_id):
            cycles.append(stop_id)
    
    return {
        "cycle_count": len(cycles),
        "cycle_stop_ids": cycles
    }

@audit_function(
    file_type="stops",
    name="check_stop_timezone_validity",
    description="Vérifie la validité du champ stop_timezone, s'il est renseigné",
    parameters={}
)
def check_stop_timezone_validity(gtfs_data, **params):
    import pytz
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'stop_timezone' not in df.columns:
        return {"missing_column": True}
    invalid_timezones = []
    all_timezones = set(pytz.all_timezones)
    for idx, tz in df['stop_timezone'].dropna().iteritems():
        if tz not in all_timezones:
            invalid_timezones.append({"index": idx, "stop_timezone": tz})
    return {
        "invalid_stop_timezone_count": len(invalid_timezones),
        "invalid_timezones": invalid_timezones
    }

@audit_function(
    file_type="stops",
    name="check_stop_url_validity",
    description="Vérifie que stop_url, si présent, est une URL valide",
    parameters={}
)
def check_stop_url_validity(gtfs_data, **params):
    import validators
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'stop_url' not in df.columns:
        return {"missing_column": True}
    invalid_urls = []
    for idx, url in df['stop_url'].dropna().iteritems():
        if not validators.url(url):
            invalid_urls.append({"index": idx, "stop_url": url})
    return {
        "invalid_url_count": len(invalid_urls),
        "invalid_urls": invalid_urls
    }

@audit_function(
    file_type="stops",
    name="analyze_stop_code_uniqueness",
    description="Analyse l'unicité des stop_code (doivent idéalement être uniques, s'ils sont renseignés)",
    parameters={}
)
def analyze_stop_code_uniqueness(gtfs_data, **params):
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'stop_code' not in df.columns:
        return {"missing_column": True}
    code_counts = df['stop_code'].value_counts()
    duplicate_codes = code_counts[code_counts > 1].index.tolist()
    return {
        "duplicate_stop_code_count": len(duplicate_codes),
        "duplicate_stop_codes": duplicate_codes
    }

@audit_function(
    file_type="stops",
    name="check_stop_desc_quality",
    description="Analyse la qualité du champ stop_desc (longueur et présence de caractères spéciaux)",
    parameters={}
)
def check_stop_desc_quality(gtfs_data, **params):
    import re
    if 'stops.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stops.txt']
    if 'stop_desc' not in df.columns:
        return {"missing_column": True}
    issues = []
    special_char_pattern = re.compile(r'[^a-zA-Z0-9\s.,;:!?()-]')
    for idx, desc in df['stop_desc'].dropna().iteritems():
        if len(desc) > 500:
            issues.append({"index": idx, "issue": "Description too long"})
        if special_char_pattern.search(desc):
            issues.append({"index": idx, "issue": "Contains special characters"})
    return {
        "stop_desc_issues_count": len(issues),
        "issues": issues
    }

@audit_function(
    file_type="stops",
    name="stop_id_multiple_coords",
    description="Même stop_id défini à plusieurs coordonnées géographiques très différentes.",
    parameters={}
)
def stop_id_multiple_coords(gtfs_data, **params):
    import numpy as np
    stops = gtfs_data.get('stops.txt')
    if stops is None:
        return {}
    # Calculer la variance de lat/lon par stop_id
    grouped = stops.groupby('stop_id').agg({
        'stop_lat': ['min','max'],
        'stop_lon': ['min','max']
    })
    grouped.columns = ['lat_min','lat_max','lon_min','lon_max']
    # Distance approximative en degrés (pas hyper précis, mais indicatif)
    grouped['lat_diff'] = grouped['lat_max'] - grouped['lat_min']
    grouped['lon_diff'] = grouped['lon_max'] - grouped['lon_min']
    # Seuil arbitraire ~0.0005 ~ 50 mètres environ
    threshold = 0.0005
    inconsistent_stops = grouped[(grouped['lat_diff'] > threshold) | (grouped['lon_diff'] > threshold)]
    return {
        "inconsistent_stop_id_count": len(inconsistent_stops),
        "inconsistent_stop_ids": inconsistent_stops.index.tolist()
    }

@audit_function(
    file_type="stops",
    name="no_cycles_in_parent_station",
    description="Vérifie que la relation parent_station ne crée pas de cycles dans stops.txt.",
    parameters={}
)
def no_cycles_in_parent_station(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    graph = {}
    for _, row in df.iterrows():
        if pd.notna(row.get('parent_station')):
            graph[row['stop_id']] = row['parent_station']
    # Détection de cycles simple dans ce graphe parent->child
    def has_cycle(node, visited, rec_stack):
        visited.add(node)
        rec_stack.add(node)
        parent = graph.get(node)
        if parent:
            if parent not in visited:
                if has_cycle(parent, visited, rec_stack):
                    return True
            elif parent in rec_stack:
                return True
        rec_stack.remove(node)
        return False

    visited = set()
    for node in graph:
        if node not in visited:
            if has_cycle(node, visited, set()):
                return {"has_cycle": True}
    return {"has_cycle": False}

@audit_function(
    file_type="stops",
    name="children_zone_id_consistency",
    description="Vérifie que tous les arrêts enfants d'une station ont le même zone_id que la station.",
    parameters={}
)
def children_zone_id_consistency(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    stations = df[df['location_type'] == 1][['stop_id', 'zone_id']]
    children = df[df['location_type'] == 0][['stop_id', 'parent_station', 'zone_id']].dropna(subset=['parent_station'])
    
    inconsistencies = []
    zone_dict = stations.set_index('stop_id')['zone_id'].to_dict()
    
    for _, row in children.iterrows():
        parent_zone = zone_dict.get(row['parent_station'])
        if parent_zone != row['zone_id']:
            inconsistencies.append({
                "child_stop_id": row['stop_id'],
                "parent_station": row['parent_station'],
                "child_zone_id": row['zone_id'],
                "parent_zone_id": parent_zone
            })
    return {
        "inconsistencies_count": len(inconsistencies),
        "details": inconsistencies
    }

@audit_function(
    file_type="stops",
    name="location_type_validity",
    description="Vérifie que les location_type avec parent_station soient bien 0 ou 1 uniquement.",
    parameters={}
)
def location_type_validity(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    invalid_rows = df[(df['parent_station'].notna()) & (~df['location_type'].isin([0,1]))]
    return {
        "invalid_count": len(invalid_rows),
        "invalid_stop_ids": invalid_rows['stop_id'].tolist()
    }

@audit_function(
    file_type="stops",
    name="parent_station_geographic_proximity",
    description="Vérifie que les stations parentes et leurs arrêts enfants soient géographiquement proches (moins de 1km).",
    parameters={}
)
def parent_station_geographic_proximity(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    stations = df[df['location_type'] == 1].set_index('stop_id')
    children = df[(df['location_type'] == 0) & (df['parent_station'].notna())]
    
    far_children = []
    
    for _, row in children.iterrows():
        parent_id = row['parent_station']
        if parent_id not in stations.index:
            continue
        child_coords = (row['stop_lat'], row['stop_lon'])
        parent_coords = (stations.loc[parent_id]['stop_lat'], stations.loc[parent_id]['stop_lon'])
        try:
            dist = geodesic(child_coords, parent_coords).meters
        except Exception:
            dist = None
        if dist is None or dist > 1000:  # plus d'1 km considéré trop éloigné
            far_children.append({
                "child_stop_id": row['stop_id'],
                "parent_station": parent_id,
                "distance_m": dist
            })
    return {
        "too_far_count": len(far_children),
        "details": far_children
    }

@audit_function(
    file_type="stops",
    name="children_distance_stats",
    description="Calcule distance moyenne et écart-type des enfants aux stations parentes.",
    parameters={}
)
def children_distance_stats(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    stations = df[df['location_type'] == 1].set_index('stop_id')
    children = df[(df['location_type'] == 0) & (df['parent_station'].notna())]
    
    distances = []
    for _, row in children.iterrows():
        parent_id = row['parent_station']
        if parent_id not in stations.index:
            continue
        try:
            dist = geodesic((row['stop_lat'], row['stop_lon']), (stations.loc[parent_id]['stop_lat'], stations.loc[parent_id]['stop_lon'])).meters
            distances.append(dist)
        except Exception:
            continue
    
    if len(distances) == 0:
        return {"average_distance_m": None, "std_distance_m": None}
    return {
        "average_distance_m": round(np.mean(distances), 2),
        "std_distance_m": round(np.std(distances), 2)
    }

@audit_function(
    file_type="stops",
    name="children_municipality_consistency",
    description="Vérifie si les enfants d'une même station appartiennent à plusieurs communes différentes.",
    parameters={}
)
def children_municipality_consistency(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    if 'municipality' not in df.columns:
        return {"checked": False, "reason": "champ municipality absent"}
    
    stations = df[df['location_type'] == 1][['stop_id']]
    children = df[(df['location_type'] == 0) & (df['parent_station'].notna())][['stop_id', 'parent_station', 'municipality']]
    
    inconsistent_stations = []
    for station_id, group in children.groupby('parent_station'):
        communes = group['municipality'].dropna().unique()
        if len(communes) > 1:
            inconsistent_stations.append({
                "parent_station": station_id,
                "municipalities": list(communes)
            })
    return {
        "inconsistent_station_count": len(inconsistent_stations),
        "details": inconsistent_stations
    }

@audit_function(
    file_type="stops",
    name="parent_station_stop_id_validity",
    description="Vérifie que tous les parent_station référencés existent comme stop_id.",
    parameters={}
)
def parent_station_stop_id_validity(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    stop_ids = set(df['stop_id'].unique())
    parents = df['parent_station'].dropna().unique()
    invalid_parents = [p for p in parents if p not in stop_ids]
    return {
        "invalid_parent_stations_count": len(invalid_parents),
        "invalid_parent_stations": invalid_parents
    }

@audit_function(
    file_type="stops",
    name="stop_id_format_and_uniqueness",
    description="Vérifie le format des stop_id et leur unicité.",
    parameters={}
)
def stop_id_format_and_uniqueness(gtfs_data, **params):
    import re
    df = gtfs_data['stops.txt']
    pattern = re.compile(r"^[a-zA-Z0-9_\-]+$")
    invalid_format = df[~df['stop_id'].astype(str).apply(lambda x: bool(pattern.match(x)))]
    duplicated = df[df.duplicated(subset=['stop_id'], keep=False)]
    return {
        "invalid_format_count": len(invalid_format),
        "invalid_format_stop_ids": invalid_format['stop_id'].tolist(),
        "duplicated_count": len(duplicated),
        "duplicated_stop_ids": duplicated['stop_id'].tolist()
    }

@audit_function(
    file_type="stops",
    name="missing_coordinates_for_children",
    description="Détecte les arrêts enfants sans coordonnées valides.",
    parameters={}
)
def missing_coordinates_for_children(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    children = df[(df['location_type'] == 0) & (df['parent_station'].notna())]
    missing_coords = children[
        (children['stop_lat'].isna()) |
        (children['stop_lon'].isna()) |
        (children['stop_lat'] == 0) |
        (children['stop_lon'] == 0)
    ]
    return {
        "missing_coordinates_count": len(missing_coords),
        "missing_coordinates_stop_ids": missing_coords['stop_id'].tolist()
    }

@audit_function(
    file_type="stops",
    name="isolated_stations",
    description="Détecte les stations géographiquement isolées (pas d'autres stations proches dans 5km).",
    parameters={}
)
def isolated_stations(gtfs_data, **params):
    df = gtfs_data['stops.txt']
    stations = df[df['location_type'] == 1][['stop_id', 'stop_lat', 'stop_lon']]
    isolated = []
    coords_list = stations[['stop_lat', 'stop_lon']].values.tolist()
    for idx, row in stations.iterrows():
        ref_point = (row['stop_lat'], row['stop_lon'])
        count_close = 0
        for other in coords_list:
            if other == [row['stop_lat'], row['stop_lon']]:
                continue
            try:
                dist = geodesic(ref_point, (other[0], other[1])).meters
            except Exception:
                dist = None
            if dist is not None and dist <= 5000:
                count_close += 1
        if count_close == 0:
            isolated.append(row['stop_id'])
    return {
        "isolated_station_count": len(isolated),
        "isolated_station_ids": isolated
    }

