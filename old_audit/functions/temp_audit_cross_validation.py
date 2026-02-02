"""
Fonctions d'audit pour le file_type: cross_validation
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="cross_validation",
    name="frequencies_no_time_overlap",
    description="Vérifie qu'il n'y a pas de chevauchement des intervalles horaires dans frequencies.txt par trip_id.",
    parameters={}
)
def frequencies_no_time_overlap(gtfs_data, **params):
    import pandas as pd

    if 'frequencies.txt' not in gtfs_data:
        return 0, []
    
    freq_df = gtfs_data['frequencies.txt']

    def time_to_seconds(t):
        try:
            h, m, s = map(int, t.split(':'))
            return h * 3600 + m * 60 + s
        except:
            return None

    invalid_trip_ids = []

    for trip_id, group in freq_df.groupby('trip_id'):
        intervals = []
        for _, row in group.iterrows():
            start_sec = time_to_seconds(row['start_time'])
            end_sec = time_to_seconds(row['end_time'])
            if start_sec is None or end_sec is None or start_sec > end_sec:
                invalid_trip_ids.append(trip_id)
                break
            intervals.append((start_sec, end_sec))
        
        if trip_id in invalid_trip_ids:
            continue
        
        # Trier les intervalles par start_time
        intervals.sort(key=lambda x: x[0])
        for i in range(len(intervals) - 1):
            if intervals[i][1] > intervals[i+1][0]:
                invalid_trip_ids.append(trip_id)
                break

    invalid_trip_ids = list(set(invalid_trip_ids))
    return (1 if len(invalid_trip_ids) == 0 else 0), invalid_trip_ids

@audit_function(
    file_type="cross_validation",
    name="stop_times_within_frequencies_intervals",
    description="Vérifie que les horaires dans stop_times.txt sont inclus dans les intervalles de frequencies.txt pour chaque trip_id.",
    parameters={}
)
def stop_times_within_frequencies_intervals(gtfs_data, **params):
    import pandas as pd

    if 'stop_times.txt' not in gtfs_data or 'frequencies.txt' not in gtfs_data:
        return 0, []

    stop_times_df = gtfs_data['stop_times.txt']
    freq_df = gtfs_data['frequencies.txt']

    def time_to_seconds(t):
        try:
            h, m, s = map(int, t.split(':'))
            return h * 3600 + m * 60 + s
        except:
            return None

    invalid_trip_ids = set()

    # Regrouper fréquences par trip_id avec listes d'intervalles en secondes
    freq_intervals = {}
    for trip_id, group in freq_df.groupby('trip_id'):
        intervals = []
        for _, row in group.iterrows():
            start_sec = time_to_seconds(row['start_time'])
            end_sec = time_to_seconds(row['end_time'])
            if start_sec is not None and end_sec is not None and start_sec <= end_sec:
                intervals.append((start_sec, end_sec))
        freq_intervals[trip_id] = intervals

    # Vérifier chaque trip_id dans stop_times
    for trip_id, group in stop_times_df.groupby('trip_id'):
        if trip_id not in freq_intervals or not freq_intervals[trip_id]:
            # Pas d'intervalle fréquence correspondant -> signaler
            invalid_trip_ids.add(trip_id)
            continue
        
        intervals = freq_intervals[trip_id]
        # Pour chaque arrival_time dans stop_times
        for arrival in group['arrival_time']:
            arrival_sec = time_to_seconds(arrival)
            if arrival_sec is None:
                invalid_trip_ids.add(trip_id)
                break
            
            # Vérifier si l'arrivée est dans au moins un intervalle frequencies
            in_any_interval = any(start <= arrival_sec <= end for (start, end) in intervals)
            if not in_any_interval:
                invalid_trip_ids.add(trip_id)
                break

    invalid_trip_ids = list(invalid_trip_ids)
    return (1 if len(invalid_trip_ids) == 0 else 0), invalid_trip_ids

@audit_function(
    file_type="cross_validation",
    name="feed_info_date_coverage",
    description="Vérifie que l'intervalle start_date/end_date de feed_info.txt couvre toutes les dates de calendar.txt et calendar_dates.txt.",
    parameters={}
)
def feed_info_date_coverage(gtfs_data, **params):
    import pandas as pd
    from datetime import datetime

    # Vérifications de présence
    if 'feed_info.txt' not in gtfs_data:
        return 0, ["feed_info.txt missing"]
    if 'calendar.txt' not in gtfs_data and 'calendar_dates.txt' not in gtfs_data:
        return 0, ["calendar.txt and calendar_dates.txt missing"]

    feed_df = gtfs_data['feed_info.txt']
    calendar_df = gtfs_data.get('calendar.txt', pd.DataFrame())
    calendar_dates_df = gtfs_data.get('calendar_dates.txt', pd.DataFrame())

    errors = []

    # Vérifier présence colonnes start_date et end_date dans feed_info
    if 'start_date' not in feed_df.columns or 'end_date' not in feed_df.columns:
        return 0, ["start_date or end_date missing in feed_info.txt"]

    # Prendre la première ligne (généralement une seule ligne dans feed_info)
    feed_start_str = feed_df.iloc[0]['start_date']
    feed_end_str = feed_df.iloc[0]['end_date']

    try:
        feed_start = datetime.strptime(str(feed_start_str), "%Y%m%d").date()
        feed_end = datetime.strptime(str(feed_end_str), "%Y%m%d").date()
    except Exception as e:
        return 0, [f"Invalid date format in feed_info.txt: {e}"]

    # Collecter toutes les dates valides de calendar.txt
    calendar_dates = set()
    if not calendar_df.empty:
        for idx, row in calendar_df.iterrows():
            try:
                start_date = datetime.strptime(str(row['start_date']), "%Y%m%d").date()
                end_date = datetime.strptime(str(row['end_date']), "%Y%m%d").date()
                # Ajouter toutes les dates entre start_date et end_date incluses
                delta = (end_date - start_date).days
                for i in range(delta + 1):
                    calendar_dates.add(start_date + pd.Timedelta(days=i))
            except Exception as e:
                errors.append(f"Invalid calendar.txt date format at row {idx}: {e}")

    # Ajouter aussi les dates spécifiques de calendar_dates.txt
    if not calendar_dates_df.empty and 'date' in calendar_dates_df.columns:
        for idx, date_val in enumerate(calendar_dates_df['date']):
            try:
                d = datetime.strptime(str(date_val), "%Y%m%d").date()
                calendar_dates.add(d)
            except Exception as e:
                errors.append(f"Invalid calendar_dates.txt date format at row {idx}: {e}")

    # Vérifier que toutes les dates sont dans l'intervalle feed_info
    out_of_range_dates = [d.strftime("%Y-%m-%d") for d in calendar_dates if d < feed_start or d > feed_end]

    if errors:
        return 0, errors

    if out_of_range_dates:
        return 0, [f"Dates out of feed_info range: {out_of_range_dates}"]

    return 1, []

@audit_function(
    file_type="cross_validation",
    name="service_dates_within_feed_info",
    description="Vérifie qu'aucun service ne roule en dehors de la plage start_date/end_date de feed_info.txt.",
    parameters={}
)
def service_dates_within_feed_info(gtfs_data, **params):
    import pandas as pd
    from datetime import datetime

    if 'feed_info.txt' not in gtfs_data:
        return 0, ["feed_info.txt missing"]
    if 'calendar.txt' not in gtfs_data and 'calendar_dates.txt' not in gtfs_data:
        return 0, ["calendar.txt and calendar_dates.txt missing"]

    feed_df = gtfs_data['feed_info.txt']
    calendar_df = gtfs_data.get('calendar.txt', pd.DataFrame())
    calendar_dates_df = gtfs_data.get('calendar_dates.txt', pd.DataFrame())

    errors = []

    # Vérifier colonnes feed_info
    if 'start_date' not in feed_df.columns or 'end_date' not in feed_df.columns:
        return 0, ["start_date or end_date missing in feed_info.txt"]

    try:
        feed_start = datetime.strptime(str(feed_df.iloc[0]['start_date']), "%Y%m%d").date()
        feed_end = datetime.strptime(str(feed_df.iloc[0]['end_date']), "%Y%m%d").date()
    except Exception as e:
        return 0, [f"Invalid date format in feed_info.txt: {e}"]

    out_of_range_services = []

    # Pour calendar.txt : vérifier chaque jour de service
    if not calendar_df.empty:
        for idx, row in calendar_df.iterrows():
            try:
                start_date = datetime.strptime(str(row['start_date']), "%Y%m%d").date()
                end_date = datetime.strptime(str(row['end_date']), "%Y%m%d").date()
                if start_date < feed_start or end_date > feed_end:
                    out_of_range_services.append(f"calendar.txt row {idx}: service dates {start_date} to {end_date} out of feed_info range")
            except Exception as e:
                errors.append(f"Invalid calendar.txt date format at row {idx}: {e}")

    # Pour calendar_dates.txt : vérifier chaque date
    if not calendar_dates_df.empty and 'date' in calendar_dates_df.columns:
        for idx, date_val in enumerate(calendar_dates_df['date']):
            try:
                d = datetime.strptime(str(date_val), "%Y%m%d").date()
                if d < feed_start or d > feed_end:
                    out_of_range_services.append(f"calendar_dates.txt row {idx}: date {d} out of feed_info range")
            except Exception as e:
                errors.append(f"Invalid calendar_dates.txt date format at row {idx}: {e}")

    if errors:
        return 0, errors

    if out_of_range_services:
        return 0, out_of_range_services

    return 1, []

