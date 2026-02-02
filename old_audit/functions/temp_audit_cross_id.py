"""
Fonctions d'audit pour le file_type: cross_id
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="cross_id",
    name="trip_route_id_reference",
    description="Vérifie que tous les route_id dans trips.txt existent dans routes.txt.",
    parameters={}
)
def trip_route_id_reference(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return 0, []
    trips_df = gtfs_data['trips.txt']
    routes_df = gtfs_data['routes.txt']
    invalid_ids = trips_df[~trips_df['route_id'].isin(routes_df['route_id'])]['route_id'].unique().tolist()
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="trip_service_id_reference",
    description="Vérifie que chaque service_id de trips.txt est défini dans calendar.txt ou calendar_dates.txt.",
    parameters={}
)
def trip_service_id_reference(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data:
        return 0, []
    trips_df = gtfs_data['trips.txt']
    calendar_ids = set()
    if 'calendar.txt' in gtfs_data:
        calendar_ids.update(gtfs_data['calendar.txt']['service_id'].unique())
    if 'calendar_dates.txt' in gtfs_data:
        calendar_ids.update(gtfs_data['calendar_dates.txt']['service_id'].unique())
    invalid_ids = trips_df[~trips_df['service_id'].isin(calendar_ids)]['service_id'].unique().tolist()
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="stop_times_trip_id_reference",
    description="Vérifie que tous les trip_id dans stop_times.txt existent dans trips.txt.",
    parameters={}
)
def stop_times_trip_id_reference(gtfs_data, **params):
    if 'stop_times.txt' not in gtfs_data or 'trips.txt' not in gtfs_data:
        return 0, []
    stop_times_df = gtfs_data['stop_times.txt']
    trips_df = gtfs_data['trips.txt']
    invalid_ids = stop_times_df[~stop_times_df['trip_id'].isin(trips_df['trip_id'])]['trip_id'].unique().tolist()
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="stop_times_stop_id_reference",
    description="Vérifie que tous les stop_id dans stop_times.txt existent dans stops.txt.",
    parameters={}
)
def stop_times_stop_id_reference(gtfs_data, **params):
    if 'stop_times.txt' not in gtfs_data or 'stops.txt' not in gtfs_data:
        return 0, []
    stop_times_df = gtfs_data['stop_times.txt']
    stops_df = gtfs_data['stops.txt']
    invalid_ids = stop_times_df[~stop_times_df['stop_id'].isin(stops_df['stop_id'])]['stop_id'].unique().tolist()
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="fare_rules_route_id_reference",
    description="Vérifie que les route_id dans fare_rules.txt existent dans routes.txt.",
    parameters={}
)
def fare_rules_route_id_reference(gtfs_data, **params):
    if 'fare_rules.txt' not in gtfs_data or 'routes.txt' not in gtfs_data:
        return 0, []
    fare_rules_df = gtfs_data['fare_rules.txt']
    routes_df = gtfs_data['routes.txt']
    if 'route_id' not in fare_rules_df.columns:
        return 1, []
    invalid_ids = fare_rules_df[~fare_rules_df['route_id'].isin(routes_df['route_id'])]['route_id'].unique().tolist()
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="fare_rules_zone_id_reference",
    description="Vérifie que les origin_id, destination_id, contains_id dans fare_rules.txt existent dans stops.txt (zone_id).",
    parameters={}
)
def fare_rules_zone_id_reference(gtfs_data, **params):
    if 'fare_rules.txt' not in gtfs_data or 'stops.txt' not in gtfs_data:
        return 0, []
    fare_rules_df = gtfs_data['fare_rules.txt']
    stops_df = gtfs_data['stops.txt']
    if 'zone_id' not in stops_df.columns:
        return 1, []
    valid_zones = stops_df['zone_id'].dropna().unique()
    invalid = []
    for col in ['origin_id', 'destination_id', 'contains_id']:
        if col in fare_rules_df.columns:
            ids = fare_rules_df[~fare_rules_df[col].isin(valid_zones)][col].dropna().unique().tolist()
            invalid.extend(ids)
    return (1 if len(invalid) == 0 else 0), list(set(invalid))

@audit_function(
    file_type="cross_id",
    name="frequencies_trip_id_reference",
    description="Vérifie que tous les trip_id dans frequencies.txt existent dans trips.txt.",
    parameters={}
)
def frequencies_trip_id_reference(gtfs_data, **params):
    if 'frequencies.txt' not in gtfs_data or 'trips.txt' not in gtfs_data:
        return 0, []
    frequencies_df = gtfs_data['frequencies.txt']
    trips_df = gtfs_data['trips.txt']
    invalid_ids = frequencies_df[~frequencies_df['trip_id'].isin(trips_df['trip_id'])]['trip_id'].unique().tolist()
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="transfers_stop_id_reference",
    description="Vérifie que from_stop_id et to_stop_id dans transfers.txt existent dans stops.txt.",
    parameters={}
)
def transfers_stop_id_reference(gtfs_data, **params):
    if 'transfers.txt' not in gtfs_data or 'stops.txt' not in gtfs_data:
        return 0, []
    transfers_df = gtfs_data['transfers.txt']
    stops_df = gtfs_data['stops.txt']
    stop_ids = stops_df['stop_id'].unique()
    
    invalid_from = transfers_df[~transfers_df['from_stop_id'].isin(stop_ids)]['from_stop_id'].unique().tolist()
    invalid_to = transfers_df[~transfers_df['to_stop_id'].isin(stop_ids)]['to_stop_id'].unique().tolist()
    invalid_ids = list(set(invalid_from + invalid_to))
    
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="shapes_shape_id_reference",
    description="Vérifie que tous les shape_id dans trips.txt existent dans shapes.txt.",
    parameters={}
)
def shapes_shape_id_reference(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data or 'shapes.txt' not in gtfs_data:
        return 0, []
    trips_df = gtfs_data['trips.txt']
    shapes_df = gtfs_data['shapes.txt']
    if 'shape_id' not in trips_df.columns:
        return 1, []
    shape_ids_trips = trips_df['shape_id'].dropna().unique()
    shape_ids_shapes = shapes_df['shape_id'].unique()
    invalid_ids = [sid for sid in shape_ids_trips if sid not in shape_ids_shapes]
    return (1 if len(invalid_ids) == 0 else 0), invalid_ids

@audit_function(
    file_type="cross_id",
    name="routes_usage_in_trips",
    description="Vérifie que toutes les route_id de routes.txt sont utilisées dans trips.txt.",
    parameters={}
)
def routes_usage_in_trips(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data or 'trips.txt' not in gtfs_data:
        return 0, ["routes.txt or trips.txt missing"]

    routes_df = gtfs_data['routes.txt']
    trips_df = gtfs_data['trips.txt']

    unused_routes = set(routes_df['route_id']) - set(trips_df['route_id'])
    if unused_routes:
        return 0, list(unused_routes)
    return 1, []

@audit_function(
    file_type="cross_id",
    name="trips_have_stop_times",
    description="Vérifie que chaque trip_id de trips.txt a au moins une entrée dans stop_times.txt.",
    parameters={}
)
def trips_have_stop_times(gtfs_data, **params):
    if 'trips.txt' not in gtfs_data or 'stop_times.txt' not in gtfs_data:
        return 0, ["trips.txt or stop_times.txt missing"]

    trips_df = gtfs_data['trips.txt']
    stop_times_df = gtfs_data['stop_times.txt']

    trips_with_stop_times = set(stop_times_df['trip_id'])
    trips_without_stops = set(trips_df['trip_id']) - trips_with_stop_times

    if trips_without_stops:
        return 0, list(trips_without_stops)
    return 1, []

@audit_function(
    file_type="cross_id",
    name="stops_usage_check",
    description="Vérifie que chaque stop_id de stops.txt est utilisé au moins une fois dans stop_times.txt.",
    parameters={}
)
def stops_usage_check(gtfs_data, **params):
    if 'stops.txt' not in gtfs_data or 'stop_times.txt' not in gtfs_data:
        return 0, ["stops.txt or stop_times.txt missing"]

    stops_df = gtfs_data['stops.txt']
    stop_times_df = gtfs_data['stop_times.txt']

    used_stops = set(stop_times_df['stop_id'])
    unused_stops = set(stops_df['stop_id']) - used_stops

    if unused_stops:
        return 0, list(unused_stops)
    return 1, []

@audit_function(
    file_type="cross_id",
    name="shapes_usage_check",
    description="Vérifie que chaque shape_id de shapes.txt est utilisé au moins une fois dans trips.txt.",
    parameters={}
)
def shapes_usage_check(gtfs_data, **params):
    if 'shapes.txt' not in gtfs_data or 'trips.txt' not in gtfs_data:
        return 0, ["shapes.txt or trips.txt missing"]

    shapes_df = gtfs_data['shapes.txt']
    trips_df = gtfs_data['trips.txt']

    used_shapes = set(trips_df['shape_id'].dropna())
    unused_shapes = set(shapes_df['shape_id']) - used_shapes

    if unused_shapes:
        return 0, list(unused_shapes)
    return 1, []

@audit_function(
    file_type="cross_id",
    name="fare_attributes_usage",
    description="Vérifie que chaque fare_id de fare_attributes.txt est utilisé dans fare_rules.txt.",
    parameters={}
)
def fare_attributes_usage(gtfs_data, **params):
    if 'fare_attributes.txt' not in gtfs_data or 'fare_rules.txt' not in gtfs_data:
        return 0, ["fare_attributes.txt or fare_rules.txt missing"]

    fare_attr_df = gtfs_data['fare_attributes.txt']
    fare_rules_df = gtfs_data['fare_rules.txt']

    used_fares = set(fare_rules_df['fare_id'])
    unused_fares = set(fare_attr_df['fare_id']) - used_fares

    if unused_fares:
        return 0, list(unused_fares)
    return 1, []

@audit_function(
    file_type="cross_id",
    name="calendar_services_not_used_in_trips",
    description="Vérifie que chaque service_id de calendar.txt est utilisé dans trips.txt.",
    parameters={}
)
def calendar_services_not_used_in_trips(gtfs_data, **params):
    if 'calendar.txt' not in gtfs_data or 'trips.txt' not in gtfs_data:
        return 0, ["calendar.txt or trips.txt missing"]

    calendar_df = gtfs_data['calendar.txt']
    trips_df = gtfs_data['trips.txt']

    calendar_services = set(calendar_df['service_id'])
    trips_services = set(trips_df['service_id'])

    unused_services = calendar_services - trips_services

    if unused_services:
        return 0, list(unused_services)
    return 1, []

@audit_function(
    file_type="cross_id",
    name="trips_with_invalid_route_id",
    description="Trips référencent des route_id inexistants dans routes.txt.",
    parameters={}
)
def trips_with_invalid_route_id(gtfs_data, **params):
    trips = gtfs_data.get('trips.txt')
    routes = gtfs_data.get('routes.txt')
    if trips is None or routes is None:
        return {}
    valid_route_ids = set(routes['route_id'])
    invalid_trips = trips[~trips['route_id'].isin(valid_route_ids)]
    return {
        "invalid_route_id_count": len(invalid_trips),
        "invalid_trip_ids": invalid_trips['trip_id'].tolist()
    }

@audit_function(
    file_type="cross_id",
    name="calendar_dates_invalid_suppression",
    description="Exception_type=2 (suppression) pour un service_id non actif selon calendar.txt.",
    parameters={}
)
def calendar_dates_invalid_suppression(gtfs_data, **params):
    calendar = gtfs_data.get('calendar.txt')
    calendar_dates = gtfs_data.get('calendar_dates.txt')
    if calendar is None or calendar_dates is None:
        return {}
    active_services = set()
    # Construire l’ensemble des service_id actifs par date depuis calendar.txt
    for _, row in calendar.iterrows():
        service_id = row['service_id']
        days = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
        for day in days:
            if row.get(day, 0) == 1:
                active_services.add((service_id, day))
    invalid_suppressions = []
    for _, row in calendar_dates.iterrows():
        sid = row['service_id']
        ex_type = row['exception_type']
        date = row['date']
        # jour de la semaine
        try:
            day_name = pd.to_datetime(str(date), format='%Y%m%d').day_name().lower()
        except:
            continue
        if ex_type == 2 and (sid, day_name) not in active_services:
            invalid_suppressions.append({
                "service_id": sid,
                "date": date
            })
    return {
        "invalid_suppressions_count": len(invalid_suppressions),
        "invalid_suppressions": invalid_suppressions
    }

@audit_function(
    file_type="cross_id",
    name="stop_times_with_missing_stop",
    description="Stop_times référencent des stop_id inexistants dans stops.txt.",
    parameters={}
)
def stop_times_with_missing_stop(gtfs_data, **params):
    stops = gtfs_data.get('stops.txt')
    stop_times = gtfs_data.get('stop_times.txt')
    if stops is None or stop_times is None:
        return {}
    valid_stop_ids = set(stops['stop_id'])
    missing_stops = stop_times[~stop_times['stop_id'].isin(valid_stop_ids)]
    return {
        "missing_stop_id_count": len(missing_stops),
        "missing_stop_times_indices": missing_stops.index.tolist()
    }

@audit_function(
    file_type="cross_id",
    name="fare_rules_with_invalid_fare_id",
    description="Fare_rules référencent des fare_id non présents dans fare_attributes.",
    parameters={}
)
def fare_rules_with_invalid_fare_id(gtfs_data, **params):
    fare_attributes = gtfs_data.get('fare_attributes.txt')
    fare_rules = gtfs_data.get('fare_rules.txt')
    if fare_attributes is None or fare_rules is None:
        return {}
    valid_fare_ids = set(fare_attributes['fare_id'])
    invalid_rules = fare_rules[~fare_rules['fare_id'].isin(valid_fare_ids)]
    return {
        "invalid_fare_rules_count": len(invalid_rules),
        "invalid_fare_ids": invalid_rules['fare_id'].unique().tolist()
    }

@audit_function(
    file_type="cross_id",
    name="duplicate_ids_global",
    description="IDs censés être uniques (route_id, trip_id, stop_id, fare_id) apparaissent plusieurs fois avec définitions différentes.",
    parameters={}
)
def duplicate_ids_global(gtfs_data, **params):
    duplicates = {}
    for fname, col in [('routes.txt', 'route_id'),
                       ('trips.txt', 'trip_id'),
                       ('stops.txt', 'stop_id'),
                       ('fare_attributes.txt', 'fare_id')]:
        df = gtfs_data.get(fname)
        if df is None:
            continue
        duplicated = df[df.duplicated(subset=[col], keep=False)]
        if not duplicated.empty:
            duplicates[col] = duplicated[col].unique().tolist()
    return {
        "duplicate_ids_count": sum(len(v) for v in duplicates.values()),
        "duplicate_ids": duplicates
    }

@audit_function(
    file_type="cross_id",
    name="missing_primary_keys",
    description="Clés primaires obligatoires manquantes ou vides.",
    parameters={}
)
def missing_primary_keys(gtfs_data, **params):
    missing = {}
    primary_keys = {
        'agency.txt': ['agency_id'],
        'stops.txt': ['stop_id'],
        'routes.txt': ['route_id'],
        'trips.txt': ['trip_id'],
        'fare_attributes.txt': ['fare_id']
    }
    for fname, keys in primary_keys.items():
        df = gtfs_data.get(fname)
        if df is None:
            continue
        for key in keys:
            missing_rows = df[df[key].isnull() | (df[key].astype(str).str.strip() == "")]
            if not missing_rows.empty:
                missing[fname.replace('.txt','')] = missing.get(fname.replace('.txt',''), []) + missing_rows.index.tolist()
    return {
        "missing_primary_keys": missing
    }

