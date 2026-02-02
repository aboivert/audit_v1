"""
Fonctions d'audit pour le file_type: stop_times
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="stop_times",
    name="check_required_columns_stop_times",
    description="Vérifie la présence des colonnes obligatoires : trip_id, arrival_time, departure_time, stop_id, stop_sequence",
    parameters={}
)
def check_required_columns_stop_times(gtfs_data, **params):
    required = {'trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'}
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    missing = list(required - set(df.columns))
    return {"missing_columns": missing, "has_missing": len(missing) > 0}

@audit_function(
    file_type="stop_times",
    name="check_arrival_before_departure",
    description="Vérifie que arrival_time <= departure_time dans chaque stop_time",
    parameters={}
)
def check_arrival_before_departure(gtfs_data, **params):
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    errors = df[df['arrival_time'] > df['departure_time']]
    return {
        "errors_count": len(errors),
        "error_trip_ids": errors['trip_id'].unique().tolist()
    }

@audit_function(
    file_type="stop_times",
    name="check_non_monotonic_times",
    description="Vérifie que les temps pour un même trip_id sont monotones (temps croissants)",
    parameters={}
)
def check_non_monotonic_times(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    non_mono = []
    for trip_id, group in df.groupby('trip_id'):
        arr = pd.to_timedelta(group['arrival_time'])
        if not arr.is_monotonic_increasing:
            non_mono.append(trip_id)
    return {"non_monotonic_trip_ids": non_mono, "count": len(non_mono)}

@audit_function(
    file_type="stop_times",
    name="check_headway_extremes",
    description="Analyse les headways (écart entre départs consécutifs d'un même trip)",
    parameters={}
)
def check_headway_extremes(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    results = {}
    for trip_id, group in df.groupby('trip_id'):
        deps = pd.to_timedelta(group['departure_time'])
        diffs = deps.diff().dropna().dt.total_seconds()
        if not diffs.empty:
            results[trip_id] = {
                "min_headway_s": diffs.min(),
                "max_headway_s": diffs.max(),
                "avg_headway_s": round(diffs.mean(),2)
            }
    return {"headway_stats": results}

@audit_function(
    file_type="stop_times",
    name="identify_zero_or_negative_durations",
    description="Détecte des durées de segment nulles ou négatives entre arrêts d'un même trip",
    parameters={}
)
def identify_zero_or_negative_durations(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    flagged = []
    for trip_id, group in df.groupby('trip_id'):
        arr = pd.to_timedelta(group['arrival_time'])
        dep = pd.to_timedelta(group['departure_time']).shift(1)
        durations = (arr - dep).dropna().dt.total_seconds()
        invalid = durations[durations <= 0]
        if not invalid.empty:
            flagged.append({
                "trip_id": trip_id,
                "count_invalid": int((invalid <= 0).sum())
            })
    return {"invalid_duration_segments": flagged}

@audit_function(
    file_type="stop_times",
    name="check_stop_sequence_integrity",
    description="Vérifie que stop_sequence est croissant de manière continue (sans sauts)",
    parameters={}
)
def check_stop_sequence_integrity(gtfs_data, **params):
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    errors = []
    for trip_id, group in df.groupby('trip_id'):
        seq = list(group['stop_sequence'])
        if seq != sorted(seq) or any(b - a != 1 for a, b in zip(seq, seq[1:])):
            errors.append(trip_id)
    return {"trip_ids_with_sequence_errors": errors, "count": len(errors)}

@audit_function(
    file_type="stop_times",
    name="compute_average_stops_per_trip",
    description="Calcule le nombre moyen d'arrêts par trip (stop_sequence)",
    parameters={}
)
def compute_average_stops_per_trip(gtfs_data, **params):
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    counts = df.groupby('trip_id').size()
    return {
        "avg_stops": round(counts.mean(),2),
        "min": int(counts.min()),
        "max": int(counts.max())
    }

@audit_function(
    file_type="stop_times",
    name="identify_duplicate_stop_times",
    description="Détecte les stop_times dupliqués pour un même trip_id, stop_sequence, arrival_time, departure_time",
    parameters={}
)
def identify_duplicate_stop_times(gtfs_data, **params):
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    dup = df.duplicated(subset=['trip_id','stop_sequence','arrival_time','departure_time'])
    count = int(dup.sum())
    return {"duplicate_count": count, "has_duplicates": count > 0}

@audit_function(
    file_type="stop_times",
    name="detect_extreme_time_gaps",
    description="Détecte les écarts temps entre arrêts très longs (ex : > 2 h)",
    parameters={"gap_threshold_seconds": {"type": "number", "description": "Seuil en secondes", "default": 7200}}
)
def detect_extreme_time_gaps(gtfs_data, gap_threshold_seconds=7200, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    flags = []
    for trip_id, group in df.groupby('trip_id'):
        deps = pd.to_timedelta(group['departure_time'])
        diffs = deps.diff().dropna().dt.total_seconds()
        long_gaps = diffs[diffs >= gap_threshold_seconds]
        for idx, dur in long_gaps.iteritems():
            flags.append({"trip_id": trip_id, "stop_sequence": group.iloc[int(idx)]['stop_sequence'], "gap_seconds": dur})
    return {"flags": flags, "count": len(flags)}

@audit_function(
    file_type="stop_times",
    name="detect_back_to_back_zero_wait",
    description="Trips où arrival_time à stop N == departure_time à stop N+1 (zéro attente)",
    parameters={}
)
def detect_back_to_back_zero_wait(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    zero_wait = []
    for trip_id, group in df.groupby('trip_id'):
        arr = pd.to_timedelta(group['arrival_time'])
        dep_next = pd.to_timedelta(group['departure_time']).shift(-1)
        zeros = (dep_next - arr).dropna().dt.total_seconds()
        for idx, wait in zeros.iteritems():
            if wait == 0:
                zero_wait.append({"trip_id": trip_id, "stop_sequence": group.iloc[int(idx)]['stop_sequence']})
    return {"zero_wait_count": len(zero_wait), "details": zero_wait}

@audit_function(
    file_type="stop_times",
    name="check_consecutive_arrival_dep_mismatch",
    description="Toujours arrival <= departure même entre deux arrêts consécutifs",
    parameters={}
)
def check_consecutive_arrival_dep_mismatch(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    mismatch = []
    for trip_id, group in df.groupby('trip_id'):
        arr = pd.to_timedelta(group['arrival_time'])
        dep = pd.to_timedelta(group['departure_time'])
        if any(dep.shift(-1).dropna() < arr):
            mismatch.append(trip_id)
    return {"trips_with_mismatch": mismatch, "count": len(mismatch)}

@audit_function(
    file_type="stop_times",
    name="analyze_trip_duration_extremes",
    description="Analyse durée totale du trip : min, max, moyenne",
    parameters={}
)
def analyze_trip_duration_extremes(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    durations = {}
    for trip_id, group in df.groupby('trip_id'):
        arr = pd.to_timedelta(group['arrival_time'])
        dur = (arr.iloc[-1] - arr.iloc[0]).total_seconds()
        durations[trip_id] = dur
    vals = list(durations.values())
    if not vals:
        return {"count": 0}
    import statistics
    return {
        "trip_durations": {
            "min_seconds": min(vals),
            "max_seconds": max(vals),
            "avg_seconds": round(statistics.mean(vals),2)
        }
    }

@audit_function(
    file_type="stop_times",
    name="check_timepoint_flags_consistency",
    description="Analyse si arrival_time/departure_time alignés avec timepoint flag (si présent)",
    parameters={}
)
def check_timepoint_flags_consistency(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    if 'timepoint' not in df.columns:
        return {"missing_column": True}
    invalid = df.loc[(df['timepoint'] == 1) & df['arrival_time'].isna()]
    return {
        "invalid_timepoints_count": len(invalid),
        "invalid_trip_ids": invalid['trip_id'].unique().tolist()
    }

@audit_function(
    file_type="stop_times",
    name="detect_same_times_multiple_stops",
    description="Détecte stops consécutifs avec mêmes horaires (arrivée-départ identiques)",
    parameters={}
)
def detect_same_times_multiple_stops(gtfs_data, **params):
    import pandas as pd
    if 'stop_times.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['stop_times.txt']
    zero_same = []
    for trip_id, group in df.groupby('trip_id'):
        for i in range(len(group)-1):
            if group.iloc[i]['departure_time'] == group.iloc[i+1]['arrival_time']:
                zero_same.append({"trip_id": trip_id, "stop_sequence": group.iloc[i]['stop_sequence']})
    return {"same_time_segments": zero_same, "count": len(zero_same)}

@audit_function(
    file_type="stop_times",
    name="duplicate_stop_times_same_trip_same_stop_same_time",
    description="Détecte les lignes dupliquées dans stop_times.txt avec mêmes trip_id, stop_id, arrival_time et departure_time.",
    parameters={}
)
def duplicate_stop_times_same_trip_same_stop_same_time(gtfs_data, **params):
    df = gtfs_data.get('stop_times')
    if df is None or df.empty:
        return {"duplicate_count": 0, "has_duplicates": False}
    duplicated = df.duplicated(subset=['trip_id', 'stop_id', 'arrival_time', 'departure_time'])
    count = duplicated.sum()
    return {
        "duplicate_count": int(count),
        "has_duplicates": count > 0
    }

@audit_function(
    file_type="stop_times",
    name="duplicate_stop_times_trip_stop",
    description="Détecte doublons stricts de trip_id et stop_id dans stop_times.txt.",
    parameters={}
)
def duplicate_stop_times_trip_stop(gtfs_data, **params):
    df = gtfs_data.get('stop_times')
    if df is None or df.empty:
        return {"duplicate_count": 0}
    duplicated = df.duplicated(subset=['trip_id', 'stop_id'])
    count = duplicated.sum()
    return {"duplicate_count": int(count), "has_duplicates": count > 0}

@audit_function(
    file_type="stop_times",
    name="conflicting_stop_sequence_per_trip",
    description="Détecte plusieurs stop_times pour un même trip_id et stop_sequence (stop_sequence non unique dans un trip).",
    parameters={}
)
def conflicting_stop_sequence_per_trip(gtfs_data, **params):
    df = gtfs_data.get('stop_times')
    if df is None or df.empty:
        return {"conflicts": []}

    duplicated = df.duplicated(subset=['trip_id', 'stop_sequence'], keep=False)
    conflicted_rows = df[duplicated][['trip_id', 'stop_sequence', 'stop_id']].to_dict(orient='records')

    return {"conflicts": conflicted_rows}

@audit_function(
    file_type="stop_times",
    name="stop_times_temporal_order",
    description="Vérifie que arrival_time et departure_time sont croissants selon stop_sequence pour chaque trip.",
    parameters={}
)
def stop_times_temporal_order(gtfs_data, **params):
    df = gtfs_data.get('stop_times.txt')
    if df is None:
        return {}
    invalid_trips = []
    for trip_id, group in df.groupby('trip_id'):
        group_sorted = group.sort_values('stop_sequence')
        arr = pd.to_timedelta(group_sorted['arrival_time'], errors='coerce')
        dep = pd.to_timedelta(group_sorted['departure_time'], errors='coerce')
        # Vérifier que chaque temps est croissant ou égal au précédent
        if arr.isnull().any() or dep.isnull().any():
            invalid_trips.append(trip_id)
            continue
        if not (arr.diff().fillna(pd.Timedelta(seconds=0)) >= pd.Timedelta(seconds=0)).all():
            invalid_trips.append(trip_id)
        if not (dep.diff().fillna(pd.Timedelta(seconds=0)) >= pd.Timedelta(seconds=0)).all():
            invalid_trips.append(trip_id)
    return {
        "invalid_trips_count": len(invalid_trips),
        "invalid_trips": invalid_trips
    }

@audit_function(
    file_type="stop_times",
    name="stop_times_non_negative_dwell",
    description="Vérifie que departure_time >= arrival_time pour chaque arrêt (temps d'attente non négatif).",
    parameters={}
)
def stop_times_non_negative_dwell(gtfs_data, **params):
    df = gtfs_data.get('stop_times.txt')
    if df is None:
        return {}
    arr = pd.to_timedelta(df['arrival_time'], errors='coerce')
    dep = pd.to_timedelta(df['departure_time'], errors='coerce')
    invalid_count = ((dep - arr) < pd.Timedelta(seconds=0)).sum()
    return {
        "invalid_dwell_count": int(invalid_count)
    }

@audit_function(
    file_type="stop_times",
    name="stop_times_time_over_48h",
    description="Détecte les horaires dépassant 48 heures (limite raisonnable).",
    parameters={}
)
def stop_times_time_over_48h(gtfs_data, **params):
    df = gtfs_data.get('stop_times.txt')
    if df is None:
        return {}
    times = pd.to_timedelta(df['arrival_time'], errors='coerce')
    count_over_48h = (times > pd.Timedelta(hours=48)).sum()
    return {
        "count_times_over_48h": int(count_over_48h)
    }

