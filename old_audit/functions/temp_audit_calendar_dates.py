"""
Fonctions d'audit pour le file_type: calendar_dates
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="calendar_dates",
    name="invalid_dates",
    description="Vérifie que toutes les dates sont valides (format AAAAMMJJ).",
    parameters={}
)
def invalid_dates(gtfs_data, **params):
    df = gtfs_data['calendar_dates.txt']
    invalid = []
    for _, row in df.iterrows():
        try:
            pd.to_datetime(str(row['date']), format='%Y%m%d')
        except:
            invalid.append(row['service_id'])
    return {
        "services_with_invalid_dates": invalid,
        "count": len(invalid)
    }

@audit_function(
    file_type="calendar_dates",
    name="invalid_exception_types",
    description="Vérifie que exception_type est 1 ou 2.",
    parameters={}
)
def invalid_exception_types(gtfs_data, **params):
    df = gtfs_data['calendar_dates.txt']
    invalid_rows = df[~df['exception_type'].isin([1, 2])]
    return {
        "invalid_rows": invalid_rows.to_dict(orient='records'),
        "count": len(invalid_rows)
    }

@audit_function(
    file_type="calendar_dates",
    name="duplicate_calendar_dates",
    description="Détecte les doublons sur (service_id, date).",
    parameters={}
)
def duplicate_calendar_dates(gtfs_data, **params):
    df = gtfs_data['calendar_dates.txt']
    duplicated = df.duplicated(subset=['service_id', 'date'], keep=False)
    return {
        "duplicate_rows": df[duplicated].to_dict(orient='records'),
        "count": duplicated.sum()
    }

@audit_function(
    file_type="calendar_dates",
    name="conflicting_exceptions",
    description="Détecte les conflits pour un même service et une même date (ajout + suppression).",
    parameters={}
)
def conflicting_exceptions(gtfs_data, **params):
    df = gtfs_data.get('calendar_dates.txt')
    if df is None:
        return {"error": "calendar_dates.txt manquant"}

    grouped = df.groupby(['service_id', 'date'])
    conflicts = []
    for (sid, date), group in grouped:
        if group['exception_type'].nunique() > 1:
            conflicts.append({
                'service_id': sid,
                'date': date,
                'types': group['exception_type'].tolist()
            })
    return {
        "conflicting_exceptions": conflicts,
        "count": len(conflicts)
    }

@audit_function(
    file_type="calendar_dates",
    name="duplicate_calendar_dates",
    description="Détecte doublons dans calendar_dates sur service_id, date, exception_type.",
    parameters={}
)
def duplicate_calendar_dates(gtfs_data, **params):
    df = gtfs_data.get('calendar_dates')
    if df is None or df.empty:
        return {"duplicate_count": 0}
    duplicated = df.duplicated(subset=['service_id', 'date', 'exception_type'])
    count = duplicated.sum()
    return {"duplicate_count": int(count), "has_duplicates": count > 0}

@audit_function(
    file_type="calendar_dates",
    name="calendar_dates_conflicts_with_calendar",
    description=(
        "Détecte conflits entre calendar_dates.txt et calendar.txt : "
        "suppression d'un service actif, dates en exception incohérentes."
    ),
    parameters={}
)
def calendar_dates_conflicts_with_calendar(gtfs_data, **params):
    cal_df = gtfs_data.get('calendar')
    cal_dates_df = gtfs_data.get('calendar_dates')
    if cal_df is None or cal_df.empty or cal_dates_df is None or cal_dates_df.empty:
        return {"conflicts": []}

    # On crée un set de service_id actifs dans calendar.txt
    active_service_ids = set(cal_df['service_id'].unique())

    conflicts = []

    # Recherche d'exceptions de suppression (exception_type == 2) pour service_id non actifs
    deletions = cal_dates_df[cal_dates_df['exception_type'] == 2]

    for _, row in deletions.iterrows():
        service_id = row['service_id']
        if service_id not in active_service_ids:
            conflicts.append({
                "service_id": service_id,
                "date": row['date'],
                "issue": "Suppression d'un service_id absent de calendar.txt"
            })

    # Recherche d'exceptions d'ajout (exception_type == 1) pour service_id non dans calendar.txt
    additions = cal_dates_df[cal_dates_df['exception_type'] == 1]

    for _, row in additions.iterrows():
        service_id = row['service_id']
        if service_id not in active_service_ids:
            conflicts.append({
                "service_id": service_id,
                "date": row['date'],
                "issue": "Ajout d'un service_id absent de calendar.txt"
            })

    return {"conflicts": conflicts}

