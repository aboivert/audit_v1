"""
Fonctions d'audit pour le file_type: frequencies
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="frequencies",
    name="required_fields_check",
    description="Vérifie que tous les champs requis sont présents dans le fichier.",
    parameters={}
)
def required_fields_check(gtfs_data, **params):
    required = {'trip_id', 'start_time', 'end_time', 'headway_secs'}
    df = gtfs_data['frequencies.txt']
    missing = required - set(df.columns)
    return {
        "missing_fields": list(missing),
        "has_all_required": len(missing) == 0
    }

@audit_function(
    file_type="frequencies",
    name="invalid_time_format",
    description="Détecte les start_time ou end_time mal formatées.",
    parameters={}
)
def invalid_time_format(gtfs_data, **params):
    import re
    df = gtfs_data['frequencies.txt']
    time_pattern = re.compile(r'^\d{1,2}:\d{2}:\d{2}$')
    invalid_rows = df[~df['start_time'].astype(str).str.match(time_pattern) |
                      ~df['end_time'].astype(str).str.match(time_pattern)]
    return {
        "invalid_time_entries": invalid_rows[['trip_id', 'start_time', 'end_time']].to_dict(orient='records'),
        "count": len(invalid_rows)
    }

@audit_function(
    file_type="frequencies",
    name="start_after_end_check",
    description="Vérifie que start_time est avant end_time.",
    parameters={}
)
def start_after_end_check(gtfs_data, **params):
    from datetime import datetime
    df = gtfs_data['frequencies.txt'].copy()
    
    def parse_time(t):
        try:
            return datetime.strptime(t, '%H:%M:%S')
        except:
            return None
    
    df['start'] = df['start_time'].apply(parse_time)
    df['end'] = df['end_time'].apply(parse_time)
    invalid = df[df['start'] >= df['end']]
    
    return {
        "invalid_duration_count": len(invalid),
        "invalid_entries": invalid[['trip_id', 'start_time', 'end_time']].to_dict(orient='records')
    }

@audit_function(
    file_type="frequencies",
    name="headway_outliers",
    description="Détecte des headway_secs en dehors des bornes raisonnables.",
    parameters={"min_secs": {"type": "number", "default": 60}, "max_secs": {"type": "number", "default": 3600}}
)
def headway_outliers(gtfs_data, min_secs=60, max_secs=3600, **params):
    df = gtfs_data['frequencies.txt']
    low = df[df['headway_secs'] < min_secs]
    high = df[df['headway_secs'] > max_secs]
    
    return {
        "too_frequent_count": len(low),
        "too_rare_count": len(high),
        "extremes": {
            "min_headway": df['headway_secs'].min(),
            "max_headway": df['headway_secs'].max()
        }
    }

@audit_function(
    file_type="frequencies",
    name="overlapping_intervals",
    description="Détecte des plages horaires qui se chevauchent pour un même trip.",
    parameters={}
)
def overlapping_intervals(gtfs_data, **params):
    from datetime import datetime
    df = gtfs_data['frequencies.txt'].copy()
    
    def to_seconds(t):
        try:
            h, m, s = map(int, t.split(":"))
            return h*3600 + m*60 + s
        except:
            return None
    
    df['start_sec'] = df['start_time'].apply(to_seconds)
    df['end_sec'] = df['end_time'].apply(to_seconds)
    
    overlaps = []
    for trip_id, group in df.groupby('trip_id'):
        sorted_group = group.sort_values('start_sec')
        for i in range(1, len(sorted_group)):
            if sorted_group.iloc[i]['start_sec'] < sorted_group.iloc[i-1]['end_sec']:
                overlaps.append(trip_id)
                break
    
    return {
        "trips_with_overlaps": overlaps,
        "count": len(overlaps)
    }

@audit_function(
    file_type="frequencies",
    name="invalid_exact_times",
    description="Vérifie que la colonne exact_times contient uniquement 0 ou 1.",
    parameters={}
)
def invalid_exact_times(gtfs_data, **params):
    df = gtfs_data['frequencies.txt']
    if 'exact_times' not in df:
        return {"status": "Column exact_times not present"}
    
    invalids = df[~df['exact_times'].isin([0, 1])]
    return {
        "invalid_rows": len(invalids),
        "invalid_trip_ids": invalids['trip_id'].tolist()
    }

@audit_function(
    file_type="frequencies",
    name="frequencies_gap_analysis",
    description="Détecte les trous de service entre les plages horaires de frequencies pour un même trip_id.",
    parameters={}
)
def frequencies_gap_analysis(gtfs_data, **params):
    from datetime import timedelta

    df = gtfs_data['frequencies.txt'].copy()
    
    def to_seconds(t):
        try:
            h, m, s = map(int, t.split(":"))
            return h * 3600 + m * 60 + s
        except:
            return None
    
    df['start_sec'] = df['start_time'].apply(to_seconds)
    df['end_sec'] = df['end_time'].apply(to_seconds)
    
    gaps = []

    for trip_id, group in df.groupby('trip_id'):
        sorted_group = group.sort_values('start_sec')
        for i in range(1, len(sorted_group)):
            prev_end = sorted_group.iloc[i-1]['end_sec']
            current_start = sorted_group.iloc[i]['start_sec']
            if prev_end is not None and current_start is not None and current_start > prev_end:
                gap_duration = current_start - prev_end
                if gap_duration >= 60:  # seuil minimal de 1 minute pour considérer un "trou"
                    gaps.append({
                        "trip_id": trip_id,
                        "gap_start": sorted_group.iloc[i-1]['end_time'],
                        "gap_end": sorted_group.iloc[i]['start_time'],
                        "gap_duration_secs": gap_duration
                    })

    return {
        "gap_count": len(gaps),
        "trips_with_gaps": len(set(g['trip_id'] for g in gaps)),
        "gaps": gaps
    }

@audit_function(
    file_type="frequencies",
    name="duplicate_frequencies_intervals",
    description="Détecte doublons dans frequencies.txt pour un même trip_id avec mêmes start_time, end_time et headway_secs.",
    parameters={}
)
def duplicate_frequencies_intervals(gtfs_data, **params):
    df = gtfs_data.get('frequencies')
    if df is None or df.empty:
        return {"duplicate_count": 0}
    duplicated = df.duplicated(subset=['trip_id', 'start_time', 'end_time', 'headway_secs'])
    count = duplicated.sum()
    return {"duplicate_count": int(count), "has_duplicates": count > 0}

