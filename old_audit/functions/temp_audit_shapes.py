"""
Fonctions d'audit pour le file_type: shapes
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="shapes",
    name="invalid_coordinates",
    description="Vérifie que les coordonnées lat/lon sont valides et dans des bornes terrestres",
    parameters={}
)
def invalid_coordinates(gtfs_data, **params):
    import pandas as pd
    if 'shapes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['shapes.txt']
    out_of_bounds = df[
        (df['shape_pt_lat'] < -90) | (df['shape_pt_lat'] > 90) |
        (df['shape_pt_lon'] < -180) | (df['shape_pt_lon'] > 180)
    ]
    return {
        "invalid_coords_count": len(out_of_bounds),
        "examples": out_of_bounds.head(5).to_dict(orient="records")
    }

@audit_function(
    file_type="shapes",
    name="non_monotonic_sequences",
    description="Détecte les shape_id où les shape_pt_sequence ne sont pas strictement croissantes",
    parameters={}
)
def non_monotonic_sequences(gtfs_data, **params):
    import pandas as pd
    if 'shapes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['shapes.txt']
    problems = []
    for shape_id, group in df.groupby('shape_id'):
        seqs = group.sort_values('shape_pt_sequence')['shape_pt_sequence'].values
        if not all(earlier < later for earlier, later in zip(seqs, seqs[1:])):
            problems.append(shape_id)
    return {"shape_ids_with_sequence_issues": problems, "count": len(problems)}

@audit_function(
    file_type="shapes",
    name="duplicate_points_in_shape",
    description="Détecte les points identiques (lat/lon/seq) dupliqués dans un même shape_id",
    parameters={}
)
def duplicate_points_in_shape(gtfs_data, **params):
    import pandas as pd
    if 'shapes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['shapes.txt']
    dups = df.duplicated(subset=['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'])
    return {
        "duplicate_points_count": int(dups.sum()),
        "examples": df[dups].head(5).to_dict(orient="records")
    }

@audit_function(
    file_type="shapes",
    name="minimal_distance_between_points",
    description="Détecte des segments trop courts (points très proches les uns des autres)",
    parameters={"min_distance_meters": {"type": "number", "default": 1.0}}
)
def minimal_distance_between_points(gtfs_data, min_distance_meters=1.0, **params):
    import pandas as pd
    from geopy.distance import geodesic
    if 'shapes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    issues = []
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        for i in range(1, len(coords)):
            dist = geodesic(coords[i-1], coords[i]).meters
            if dist < min_distance_meters:
                issues.append({"shape_id": shape_id, "sequence": i, "distance_m": dist})
    return {"very_short_segments": issues, "count": len(issues)}

@audit_function(
    file_type="shapes",
    name="shape_total_distance_stats",
    description="Calcule la distance totale parcourue par chaque shape_id",
    parameters={}
)
def shape_total_distance_stats(gtfs_data, **params):
    import pandas as pd
    from geopy.distance import geodesic
    import statistics
    if 'shapes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    distances = []
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        shape_dist = sum(geodesic(coords[i-1], coords[i]).meters for i in range(1, len(coords)))
        distances.append(shape_dist)
    return {
        "shape_distance_summary": {
            "min_meters": round(min(distances), 2),
            "max_meters": round(max(distances), 2),
            "avg_meters": round(statistics.mean(distances), 2)
        },
        "count": len(distances)
    }

@audit_function(
    file_type="shapes",
    name="closed_loop_shapes",
    description="Identifie les shape_id qui reviennent au point de départ (boucle fermée)",
    parameters={"tolerance_meters": {"type": "number", "default": 10.0}}
)
def closed_loop_shapes(gtfs_data, tolerance_meters=10.0, **params):
    from geopy.distance import geodesic
    df = gtfs_data['shapes.txt']
    loops = []
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        if len(coords) < 2:
            continue
        dist = geodesic(coords[0], coords[-1]).meters
        if dist <= tolerance_meters:
            loops.append({"shape_id": shape_id, "closure_distance": dist})
    return {
        "closed_loops_count": len(loops),
        "closed_loops": loops[:5]
    }

@audit_function(
    file_type="shapes",
    name="uniform_spacing_detection",
    description="Détecte les shapes avec distances quasi-identiques entre les points",
    parameters={"tolerance": {"type": "number", "default": 1.0}}
)
def uniform_spacing_detection(gtfs_data, tolerance=1.0, **params):
    from geopy.distance import geodesic
    import numpy as np
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    suspect_shapes = []
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        dists = [geodesic(coords[i], coords[i+1]).meters for i in range(len(coords)-1)]
        if len(dists) < 2:
            continue
        std = np.std(dists)
        if std < tolerance:
            suspect_shapes.append({"shape_id": shape_id, "std_dev": std})
    return {"uniformly_spaced_shapes": suspect_shapes, "count": len(suspect_shapes)}

@audit_function(
    file_type="shapes",
    name="abrupt_direction_changes",
    description="Identifie les shapes avec des changements d'angle très brusques",
    parameters={"angle_threshold_deg": {"type": "number", "default": 120.0}}
)
def abrupt_direction_changes(gtfs_data, angle_threshold_deg=120.0, **params):
    import numpy as np
    from math import atan2, degrees
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    issues = []

    def angle(p1, p2, p3):
        # Calculate angle between three points
        def bearing(a, b):
            return atan2(b[1]-a[1], b[0]-a[0])
        angle_rad = bearing(p1, p2) - bearing(p2, p3)
        return abs(degrees(angle_rad)) % 180

    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        for i in range(1, len(coords)-1):
            ang = angle(coords[i-1], coords[i], coords[i+1])
            if ang < (180 - angle_threshold_deg):
                issues.append({"shape_id": shape_id, "sequence": i, "angle_deg": round(ang, 2)})
    return {"abrupt_turns": issues[:10], "total_count": len(issues)}

@audit_function(
    file_type="shapes",
    name="shape_point_density",
    description="Analyse la densité des points par shape_id (nombre de points par km)",
    parameters={}
)
def shape_point_density(gtfs_data, **params):
    from geopy.distance import geodesic
    import statistics
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    density = {}
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        if len(coords) < 2:
            continue
        length = sum(geodesic(coords[i], coords[i+1]).meters for i in range(len(coords)-1)) / 1000
        density[shape_id] = round(len(coords) / length, 2) if length > 0 else None
    values = [v for v in density.values() if v]
    return {
        "average_density_points_per_km": round(statistics.mean(values), 2),
        "min_density": round(min(values), 2),
        "max_density": round(max(values), 2),
        "examples": dict(list(density.items())[:5])
    }

@audit_function(
    file_type="shapes",
    name="backtracking_detection",
    description="Détecte des retours en arrière sur l'axe lat/lon (forme non fluide)",
    parameters={"threshold_deg": {"type": "number", "default": 0.001}}
)
def backtracking_detection(gtfs_data, threshold_deg=0.001, **params):
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    problems = []
    
    for shape_id, group in df.groupby('shape_id'):
        lats = group['shape_pt_lat'].values
        lons = group['shape_pt_lon'].values
        
        lat_diffs = lats[1:] - lats[:-1]
        lon_diffs = lons[1:] - lons[:-1]
        
        # Vérifie si le sens change brutalement (produit de deux différences successives négatif)
        lat_backtrack = any((lat_diffs[i] * lat_diffs[i+1] < -threshold_deg) for i in range(len(lat_diffs)-1))
        lon_backtrack = any((lon_diffs[i] * lon_diffs[i+1] < -threshold_deg) for i in range(len(lon_diffs)-1))
        
        if lat_backtrack or lon_backtrack:
            problems.append(shape_id)
    
    return {
        "backtracking_shapes": problems,
        "count": len(problems)
    }

@audit_function(
    file_type="shapes",
    name="shape_linearity_ratio",
    description="Mesure la linéarité des shapes : distance droite vs distance réelle",
    parameters={}
)
def shape_linearity_ratio(gtfs_data, **params):
    from geopy.distance import geodesic
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    ratios = []
    
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        if len(coords) < 2:
            continue
        total_distance = sum(geodesic(coords[i], coords[i+1]).meters for i in range(len(coords)-1))
        direct_distance = geodesic(coords[0], coords[-1]).meters
        if total_distance == 0:
            continue
        ratio = round(direct_distance / total_distance, 4)
        ratios.append({"shape_id": shape_id, "linearity_ratio": ratio})
    
    return {
        "total_shapes": len(ratios),
        "linearity_ratios": ratios[:10],
        "min_ratio": min([r['linearity_ratio'] for r in ratios], default=None),
        "max_ratio": max([r['linearity_ratio'] for r in ratios], default=None)
    }

@audit_function(
    file_type="shapes",
    name="consecutive_duplicate_points",
    description="Détecte les points consécutifs identiques dans les shapes",
    parameters={}
)
def consecutive_duplicate_points(gtfs_data, **params):
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    issues = []
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        duplicates = sum(1 for i in range(len(coords)-1) if coords[i] == coords[i+1])
        if duplicates > 0:
            issues.append({"shape_id": shape_id, "consecutive_duplicates": duplicates})
    return {
        "shapes_with_duplicates": issues,
        "count": len(issues)
    }

@audit_function(
    file_type="shapes",
    name="isolated_shape_points",
    description="Cherche des points très éloignés de leurs voisins (erreur de géocodage)",
    parameters={"distance_threshold_m": {"type": "number", "default": 1000.0}}
)
def isolated_shape_points(gtfs_data, distance_threshold_m=1000.0, **params):
    from geopy.distance import geodesic
    df = gtfs_data['shapes.txt'].sort_values(['shape_id', 'shape_pt_sequence'])
    anomalies = []
    for shape_id, group in df.groupby('shape_id'):
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        for i in range(1, len(coords)-1):
            prev = geodesic(coords[i-1], coords[i]).meters
            next = geodesic(coords[i], coords[i+1]).meters
            if prev > distance_threshold_m and next > distance_threshold_m:
                anomalies.append({"shape_id": shape_id, "sequence": i, "distance_to_neighbors": (prev, next)})
    return {
        "isolated_points": anomalies[:10],
        "total_anomalies": len(anomalies)
    }

@audit_function(
    file_type="shapes",
    name="similar_shapes_detection",
    description="Détecte des shapes très similaires (parcourant quasiment la même trajectoire).",
    parameters={}
)
def similar_shapes_detection(gtfs_data, **params):
    import numpy as np

    df = gtfs_data.get('shapes')
    if df is None or df.empty:
        return {"similar_pairs": []}

    # Regroupement par shape_id, tri par shape_pt_sequence
    shapes_grouped = df.groupby('shape_id')

    shape_coords = {}
    for shape_id, group in shapes_grouped:
        coords = group.sort_values('shape_pt_sequence')[['shape_pt_lat', 'shape_pt_lon']].to_numpy()
        shape_coords[shape_id] = coords

    similar_pairs = []
    shape_ids = list(shape_coords.keys())

    def coords_distance(c1, c2):
        return np.linalg.norm(c1 - c2, axis=1).mean()

    threshold = 0.0005  # approx 50m tolérance

    for i in range(len(shape_ids)):
        for j in range(i + 1, len(shape_ids)):
            coords1 = shape_coords[shape_ids[i]]
            coords2 = shape_coords[shape_ids[j]]

            if len(coords1) != len(coords2):
                continue

            dist = coords_distance(coords1, coords2)
            if dist < threshold:
                similar_pairs.append({
                    "shape_id_1": shape_ids[i],
                    "shape_id_2": shape_ids[j],
                    "average_distance": dist
                })

    return {"similar_pairs": similar_pairs}

@audit_function(
    file_type="shapes",
    name="shapes_bad_sequence",
    description="Points de shape dont shape_pt_sequence n'est pas strictement croissant.",
    parameters={}
)
def shapes_bad_sequence(gtfs_data, **params):
    shapes = gtfs_data.get('shapes.txt')
    if shapes is None:
        return {}
    invalid_shapes = []
    for shape_id, group in shapes.groupby('shape_id'):
        seq = group['shape_pt_sequence']
        if not all(seq.iloc[i] < seq.iloc[i+1] for i in range(len(seq)-1)):
            invalid_shapes.append(shape_id)
    return {
        "invalid_shape_sequence_count": len(invalid_shapes),
        "invalid_shape_ids": invalid_shapes
    }

@audit_function(
    file_type="shapes",
    name="shapes_large_jumps",
    description="Distance trop grande entre deux points consécutifs dans un shape.",
    parameters={}
)
def shapes_large_jumps(gtfs_data, **params):
    import geopy.distance
    shapes = gtfs_data.get('shapes.txt')
    if shapes is None:
        return {}
    threshold_meters = 1000  # saut > 1 km considéré suspect
    suspicious_shapes = []
    for shape_id, group in shapes.groupby('shape_id'):
        group = group.sort_values('shape_pt_sequence')
        coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
        for i in range(len(coords)-1):
            dist = geopy.distance.distance(coords[i], coords[i+1]).meters
            if dist > threshold_meters:
                suspicious_shapes.append(shape_id)
                break
    return {
        "shapes_with_large_jumps_count": len(suspicious_shapes),
        "shapes_with_large_jumps": suspicious_shapes
    }

