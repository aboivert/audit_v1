"""
Fonctions d'audit pour le file_type: trips
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="trips",
    name="check_trip_id_uniqueness",
    description="Vérifie l'unicité de trip_id.",
    parameters={}
)
def check_trip_id_uniqueness(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    dup = df['trip_id'][df['trip_id'].duplicated()]
    return {"duplicate_trip_ids": dup.tolist(), "count_duplicates": dup.nunique()}

@audit_function(
    file_type="trips",
    name="check_required_columns",
    description="Vérifie la présence des colonnes obligatoires route_id, service_id, trip_id.",
    parameters={}
)
def check_required_columns_trips(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    needed = {'route_id', 'service_id', 'trip_id'}
    missing = list(needed - set(df.columns))
    return {"missing_columns": missing, "all_present": len(missing) == 0}

@audit_function(
    file_type="trips",
    name="headsign_completion_rate",
    description="Taux de complétude du champ trip_headsign.",
    parameters={}
)
def headsign_completion_rate(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    if 'trip_headsign' not in df.columns:
        return {"missing_column": True}
    total = len(df)
    present = df['trip_headsign'].notna().sum()
    rate = round(present / total * 100, 2) if total > 0 else 0
    return {"total": total, "present": present, "completion_rate": rate}

@audit_function(
    file_type="trips",
    name="validate_direction_id",
    description="Vérifie que direction_id est uniquement 0 ou 1.",
    parameters={}
)
def validate_direction_id(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    if 'direction_id' not in df.columns:
        return {"missing_column": True}
    invalid = df.loc[~df['direction_id'].isin([0,1]), 'direction_id']
    return {"invalid_direction_ids": invalid.tolist(), "count_invalid": len(invalid)}

@audit_function(
    file_type="trips",
    name="shape_id_distribution",
    description="Analyse de la distribution des trips par shape_id.",
    parameters={}
)
def shape_id_distribution(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    if 'shape_id' not in df.columns:
        return {"missing_column": True}
    counts = df['shape_id'].value_counts().to_dict()
    no_shape = df['shape_id'].isna().sum()
    return {"shape_counts": counts, "trips_without_shape": int(no_shape)}

@audit_function(
    file_type="trips",
    name="trips_without_shape",
    description="Compte le nombre de trips sans shape_id.",
    parameters={}
)
def trips_without_shape(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    missing = df['shape_id'].isna().sum() if 'shape_id' in df.columns else len(df)
    return {"trips_without_shape": int(missing)}

@audit_function(
    file_type="trips",
    name="service_id_variability",
    description="Analyse le nombre de trips par service_id.",
    parameters={}
)
def service_id_variability(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    counts = df['service_id'].value_counts().to_dict()
    return {"trips_per_service": counts, "unique_services": len(counts)}

@audit_function(
    file_type="trips",
    name="trip_name_field_completeness",
    description="Analyse la complétude et la longueur moyenne des champs trip_short_name et trip_long_name.",
    parameters={}
)
def trip_name_field_completeness(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    results = {}
    for col in ['trip_short_name', 'trip_long_name']:
        if col in df.columns:
            non_null = df[col].dropna()
            results[col] = {
                "completion_rate": round(len(non_null) / len(df) * 100, 2),
                "avg_length": round(non_null.str.len().mean(), 2) if not non_null.empty else 0,
                "unique_values": non_null.nunique()
            }
        else:
            results[col] = {"missing": True}
    return results

@audit_function(
    file_type="trips",
    name="trip_headsign_analysis",
    description="Analyse textuelle du champ trip_headsign.",
    parameters={}
)
def trip_headsign_analysis(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    if 'trip_headsign' not in df.columns:
        return {"missing_column": True}
    
    heads = df['trip_headsign'].dropna().astype(str)
    special_chars = heads[heads.str.contains(r'[^a-zA-Z0-9\s\-\/]', regex=True)]
    uppercase = heads[heads.str.isupper()]
    too_short = heads[heads.str.len() < 3]
    stats = {
        "count": len(heads),
        "avg_length": round(heads.str.len().mean(), 2),
        "min_length": heads.str.len().min(),
        "max_length": heads.str.len().max(),
        "uppercase_entries": len(uppercase),
        "short_entries": len(too_short),
        "entries_with_special_chars": len(special_chars)
    }
    return stats

@audit_function(
    file_type="trips",
    name="direction_variability_by_route",
    description="Vérifie que chaque route a des trips dans les deux directions (0 et 1).",
    parameters={}
)
def direction_variability_by_route(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    if 'route_id' not in df.columns or 'direction_id' not in df.columns:
        return {"missing_columns": True}
    
    summary = df.groupby('route_id')['direction_id'].nunique()
    one_direction_routes = summary[summary < 2].index.tolist()
    return {
        "routes_with_one_direction_only": one_direction_routes,
        "count": len(one_direction_routes)
    }

@audit_function(
    file_type="trips",
    name="trip_id_format_check",
    description="Détecte des formats de trip_id inhabituels ou non standardisés.",
    parameters={}
)
def trip_id_format_check(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt']
    trip_ids = df['trip_id'].astype(str)
    suspicious = trip_ids[trip_ids.str.contains(r'\s|[^a-zA-Z0-9_\-]', regex=True)]
    too_short = trip_ids[trip_ids.str.len() < 3]
    return {
        "trip_ids_with_spaces_or_special_chars": suspicious.tolist(),
        "short_trip_ids": too_short.tolist(),
        "total_suspicious": len(suspicious) + len(too_short)
    }

@audit_function(
    file_type="trips",
    name="trip_id_entropy",
    description="Mesure de l'entropie des trip_id (diversité de structure).",
    parameters={}
)
def trip_id_entropy(gtfs_data, **params):
    import math
    from collections import Counter

    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    trip_ids = gtfs_data['trips.txt']['trip_id'].dropna().astype(str)
    freq = Counter(trip_ids)
    total = sum(freq.values())
    entropy = -sum((count/total) * math.log2(count/total) for count in freq.values() if count > 0)
    return {"entropy": round(entropy, 4), "unique_trip_ids": len(freq)}

@audit_function(
    file_type="trips",
    name="redundant_trip_names",
    description="Détecte des redondances exactes entre les champs headsign, short_name et long_name.",
    parameters={}
)
def redundant_trip_names(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt'].copy()
    redundant = []
    for i, row in df.iterrows():
        names = [row.get('trip_headsign'), row.get('trip_short_name'), row.get('trip_long_name')]
        names = [str(n).strip().lower() for n in names if pd.notna(n)]
        if len(set(names)) == 1 and len(names) > 1:
            redundant.append(row['trip_id'])
    return {"redundant_trip_name_trip_ids": redundant, "count": len(redundant)}

@audit_function(
    file_type="trips",
    name="duplicate_trip_rows_without_id",
    description="Détecte des lignes dupliquées sans considérer trip_id.",
    parameters={}
)
def duplicate_trip_rows_without_id(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['trips.txt'].copy()
    if 'trip_id' in df.columns:
        df = df.drop(columns=['trip_id'])
    duplicate_rows = df[df.duplicated()]
    return {"duplicate_rows_count": len(duplicate_rows), "has_duplicates": len(duplicate_rows) > 0}

@audit_function(
    file_type="trips",
    name="duplicate_trip_ids",
    description="Détecte doublons de trip_id dans trips.txt.",
    parameters={}
)
def duplicate_trip_ids(gtfs_data, **params):
    df = gtfs_data.get('trips')
    if df is None or df.empty:
        return {"duplicate_count": 0}
    duplicated = df.duplicated(subset=['trip_id'])
    count = duplicated.sum()
    return {"duplicate_count": int(count), "has_duplicates": count > 0}

@audit_function(
    file_type="trips",
    name="conflicting_trip_id_contexts",
    description="Détecte un même trip_id utilisé dans deux contextes incohérents (ex: différents route_id ou shape_id).",
    parameters={}
)
def conflicting_trip_id_contexts(gtfs_data, **params):
    df = gtfs_data.get('trips')
    if df is None or df.empty:
        return {"conflicts": []}

    # On vérifie si un trip_id a plusieurs route_id ou shape_id différents
    conflicts = []
    grouped = df.groupby('trip_id')
    for trip_id, group in grouped:
        unique_route_ids = group['route_id'].nunique()
        unique_shape_ids = group['shape_id'].nunique() if 'shape_id' in group else 0
        if unique_route_ids > 1 or unique_shape_ids > 1:
            conflicts.append({
                "trip_id": trip_id,
                "route_id_count": unique_route_ids,
                "shape_id_count": unique_shape_ids
            })
    return {"conflicts": conflicts}

@audit_function(
    file_type="trips",
    name="duplicate_trip_ids",
    description="Détecte les trip_id dupliqués.",
    parameters={}
)
def duplicate_trip_ids(gtfs_data, **params):
    df = gtfs_data.get('trips.txt')
    if df is None:
        return {}
    duplicated = df[df.duplicated(subset=['trip_id'], keep=False)]
    return {
        "duplicated_trip_id_count": len(duplicated),
        "duplicated_trip_ids": duplicated['trip_id'].unique().tolist()
    }

