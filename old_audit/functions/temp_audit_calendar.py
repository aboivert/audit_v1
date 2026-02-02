"""
Fonctions d'audit pour le file_type: calendar
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="calendar",
    name="invalid_or_inverted_dates",
    description="Vérifie que start_date et end_date sont valides et bien ordonnées.",
    parameters={}
)
def invalid_or_inverted_dates(gtfs_data, **params):
    df = gtfs_data['calendar.txt']
    invalid = []
    for _, row in df.iterrows():
        try:
            start = pd.to_datetime(str(row['start_date']), format='%Y%m%d')
            end = pd.to_datetime(str(row['end_date']), format='%Y%m%d')
            if start > end:
                invalid.append(row['service_id'])
        except:
            invalid.append(row['service_id'])
    return {
        "invalid_or_inverted_services": invalid,
        "count": len(invalid)
    }

@audit_function(
    file_type="calendar",
    name="inactive_services",
    description="Détecte les services qui ne sont actifs aucun jour de la semaine.",
    parameters={}
)
def inactive_services(gtfs_data, **params):
    df = gtfs_data['calendar.txt']
    weekday_cols = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    df['sum_days'] = df[weekday_cols].sum(axis=1)
    inactive = df[df['sum_days'] == 0]['service_id'].tolist()
    return {
        "inactive_service_ids": inactive,
        "count": len(inactive)
    }

@audit_function(
    file_type="calendar",
    name="excessive_duration_services",
    description="Identifie les services avec une durée d'activité > 2 ans.",
    parameters={}
)
def excessive_duration_services(gtfs_data, **params):
    df = gtfs_data['calendar.txt']
    long_services = []
    for _, row in df.iterrows():
        try:
            start = pd.to_datetime(str(row['start_date']), format='%Y%m%d')
            end = pd.to_datetime(str(row['end_date']), format='%Y%m%d')
            if (end - start).days > 730:
                long_services.append(row['service_id'])
        except:
            continue
    return {
        "long_services": long_services,
        "count": len(long_services)
    }

@audit_function(
    file_type="calendar",
    name="calendar_dates_service_not_in_calendar",
    description="Détecte les service_id présents dans calendar_dates.txt mais absents de calendar.txt.",
    parameters={}
)
def calendar_dates_service_not_in_calendar(gtfs_data, **params):
    calendar_df = gtfs_data.get('calendar.txt')
    calendar_dates_df = gtfs_data.get('calendar_dates.txt')
    
    if calendar_df is None or calendar_dates_df is None:
        return {"error": "Fichiers manquants"}

    calendar_services = set(calendar_df['service_id'].unique())
    calendar_dates_services = set(calendar_dates_df['service_id'].unique())
    
    extra_services = list(calendar_dates_services - calendar_services)

    return {
        "calendar_dates_only_services": extra_services,
        "count": len(extra_services)
    }

@audit_function(
    file_type="calendar",
    name="exceptions_outside_date_range",
    description="Détecte les dates dans calendar_dates.txt hors des plages définies dans calendar.txt.",
    parameters={}
)
def exceptions_outside_date_range(gtfs_data, **params):
    calendar_df = gtfs_data.get('calendar.txt')
    calendar_dates_df = gtfs_data.get('calendar_dates.txt')
    
    if calendar_df is None or calendar_dates_df is None:
        return {"error": "Fichiers manquants"}

    calendar_df['start_date'] = pd.to_datetime(calendar_df['start_date'], format='%Y%m%d', errors='coerce')
    calendar_df['end_date'] = pd.to_datetime(calendar_df['end_date'], format='%Y%m%d', errors='coerce')
    calendar_dates_df['date'] = pd.to_datetime(calendar_dates_df['date'], format='%Y%m%d', errors='coerce')

    outliers = []

    calendar_dict = calendar_df.set_index('service_id')[['start_date', 'end_date']].to_dict(orient='index')

    for _, row in calendar_dates_df.iterrows():
        sid = row['service_id']
        date = row['date']
        if sid in calendar_dict:
            start = calendar_dict[sid]['start_date']
            end = calendar_dict[sid]['end_date']
            if pd.notnull(date) and pd.notnull(start) and pd.notnull(end):
                if not (start <= date <= end):
                    outliers.append({'service_id': sid, 'date': date.strftime('%Y-%m-%d')})
    
    return {
        "exceptions_outside_range": outliers,
        "count": len(outliers)
    }

@audit_function(
    file_type="calendar",
    name="never_active_services",
    description="Détecte les services présents dans calendar.txt qui ne roulent aucun jour et n'ont aucune date d'exception.",
    parameters={}
)
def never_active_services(gtfs_data, **params):
    calendar_df = gtfs_data.get('calendar.txt')
    calendar_dates_df = gtfs_data.get('calendar_dates.txt')
    
    if calendar_df is None:
        return {"error": "calendar.txt manquant"}

    calendar_df['weekday_sum'] = calendar_df[['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']].sum(axis=1)
    inactive = calendar_df[calendar_df['weekday_sum'] == 0]

    if calendar_dates_df is not None:
        exceptions = set(calendar_dates_df['service_id'])
        inactive = inactive[~inactive['service_id'].isin(exceptions)]

    return {
        "never_active_services": inactive['service_id'].tolist(),
        "count": len(inactive)
    }

@audit_function(
    file_type="calendar",
    name="duplicate_service_id_days",
    description="Détecte doublons dans calendar.txt sur service_id et jours d'opération identiques.",
    parameters={}
)
def duplicate_service_id_days(gtfs_data, **params):
    df = gtfs_data.get('calendar')
    if df is None or df.empty:
        return {"duplicate_count": 0}

    day_cols = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
    df['days_signature'] = df[day_cols].astype(str).agg(''.join, axis=1)

    duplicated = df.duplicated(subset=['service_id', 'days_signature'])
    count = duplicated.sum()

    return {"duplicate_count": int(count), "has_duplicates": count > 0}

