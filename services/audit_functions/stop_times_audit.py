"""
Fonctions d'audit pour le fichier trips.txt
"""
import pandas as pd
from datetime import datetime
from services.gtfs_handler import GTFSHandler

from .generic_functions import check_required_field
from .generic_functions import check_required_fields_summary
from .generic_functions import check_format_field
from .generic_functions import calculate_summary
from .generic_functions import calculate_validity_score

format = {'pickup_type':{'genre':'optional','description':"Validité du champ pickup_type",'type':'listing', 'valid_fields':['0', '1', '2', '3']},
          'drop_off_type':{'genre':'optional','description':"Validité du champ drop_off_type",'type':'listing', 'valid_fields':['0', '1', '2', '3']},
          'departure_time':{'genre':'required','description':"Validité des horaires de départ d\'un arrêt",'type':'time'},
          'arrival_time':{'genre':'required','description':"Validité des horaires d\'arrivée à un arrêt",'type':'time'},
}

def audit_stop_times_file(project_id, progress_callback = None):
    """
    Audit complet du fichier stops.txt
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Résultats de l'audit selon la structure définie
    """

    if progress_callback:
        progress_callback(15, "Chargement du fichier stop_times.txt...", "loading")

    stop_times_df = GTFSHandler.get_gtfs_data(project_id, 'stop_times.txt')
    
    if stop_times_df is None or stop_times_df.empty:
        if progress_callback:
            progress_callback(0, "Fichier stop_times.txt introuvable", "error")
        return {
            "file": "stop_times.txt",
            "status": "missing",
            "message": "Fichier stop_times.txt non trouvé ou vide",
            "severity": "critical",
            "timestamp": datetime.now().isoformat()
        }
    
    if progress_callback:
        progress_callback(25, "Vérification des champs obligatoires...", "required_fields")
    
    results = {
        "file": "stop_times.txt",
        "status": "processed",
        "total_rows": len(stop_times_df),
        "timestamp": datetime.now().isoformat(),
        "required_fields": _check_required_fields(stop_times_df, project_id)
    }

    if progress_callback:
        progress_callback(40, "Vérification du format des donnés...", "data_format")

    results["data_format"] = _check_data_format(stop_times_df)

    if progress_callback:
        progress_callback(55, "Analyse des cohérences temporelles...", "temporal_analysis")

    results["temporal_analysis"] = _check_temporal_analysis(stop_times_df)

    if progress_callback:
        progress_callback(70, "Génération des statistiques...", "statistics")

    results["statistics"] = _generate_statistics(stop_times_df, project_id)


    if progress_callback:
        progress_callback(85, "Calcul du résumé...", "summary")

    # Calculer le résumé global
    results["summary"] = calculate_summary(results, ['required_fields','data_format', 'temporal_analysis', 'data_consistency'])
    return results
    
def _check_required_fields(df, project_id):
    """Vérifications des champs obligatoires"""
    checks = []
        
    # 1. Vérifier service_id requis
    stop_times_fields_check = check_required_fields_summary(df, ['trip_id', 'stop_sequence'], 'trip_id')
    checks.append(stop_times_fields_check)

    # 2. Vérifier unicité du couple (trip_id, stop_sequence)
    if 'trip_id' in df.columns and 'stop_sequence' in df.columns and not df['trip_id'].isna().all() and not df['stop_sequence'].isna().all():
        uniqueness_check = {
            "check_name": "trip_id_stop_sequence_unique",
            "description": "Unicité des couples (trip_id, stop_sequence)",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        # Vérifier les doublons sur le couple (trip_id, stop_sequence)
        duplicates = df[df.duplicated(['trip_id', 'stop_sequence'], keep=False) & 
                        df['trip_id'].notna() & df['stop_sequence'].notna()]
        if not duplicates.empty:
            # Grouper les doublons par couple (trip_id, stop_sequence)
            duplicate_details = []
            duplicate_couples = duplicates[['trip_id', 'stop_sequence']].drop_duplicates()
            
            for _, row in duplicate_couples.iterrows():
                trip_id = row['trip_id']
                stop_sequence = row['stop_sequence']
                dup_rows = duplicates[(duplicates['trip_id'] == trip_id) & 
                                    (duplicates['stop_sequence'] == stop_sequence)]
                
                duplicate_details.append({
                    "trip_id": str(trip_id),
                    "stop_sequence": str(stop_sequence),
                    "occurrences": len(dup_rows),
                    "rows": dup_rows.index.tolist()
                })
            
            uniqueness_check.update({
                "status": "error",
                "message": f"{len(duplicate_couples)} couples (trip_id, stop_sequence) dupliqués",
                "details": {
                    "duplicate_couples": len(duplicate_couples),
                    "duplicate_rows": duplicates.index.tolist(),
                    "duplicate_details": duplicate_details
                }
            })
        else:
            uniqueness_check["message"] = "Tous les couples (trip_id, stop_sequence) sont uniques"
        
        checks.append(uniqueness_check)

    # Vérifier que les trip_id référencés existent dans trips.txt
    trip_exists_check = {
        "check_name": "trip_id_exists",
        "description": "Les trip_id référencés existent dans trips.txt",
        "status": "pass",
        "message": "",
        "details": {}
    }

    try:
        trips_df = GTFSHandler.get_gtfs_data(project_id, 'trips.txt')
        
        if trips_df is not None and 'trip_id' in df.columns and 'trip_id' in trips_df.columns:
            # Récupérer les trip_id valides
            valid_trip_ids = set(trips_df['trip_id'].dropna().unique())
            stop_times_trip_ids = set(df['trip_id'].dropna().unique())
            
            # Trouver les trip_id dans stop_times mais pas dans trips
            invalid_trip_ids = stop_times_trip_ids - valid_trip_ids
            
            if invalid_trip_ids:
                invalid_stop_times = []
                for trip_id in invalid_trip_ids:
                    stop_times_with_invalid_trip = df[df['trip_id'] == trip_id]
                    for idx, row in stop_times_with_invalid_trip.iterrows():
                        stop_id = df.loc[idx, 'stop_id'] if 'stop_id' in df.columns else 'N/A'
                        stop_sequence = df.loc[idx, 'stop_sequence'] if 'stop_sequence' in df.columns else 'N/A'
                        invalid_stop_times.append({
                            "trip_id": str(trip_id),
                            "stop_id": str(stop_id),
                            "stop_sequence": str(stop_sequence),
                            "row": idx
                        })
                
                invalid_trip_group = {}
                for item in invalid_stop_times:
                    trip_id = str(item["trip_id"])
                    
                    if trip_id not in invalid_trip_group:
                        invalid_trip_group[trip_id] = []
                    
                    invalid_trip_group[trip_id].append({
                        "stop_id": item["stop_id"],
                        "stop_sequence": item["stop_sequence"],
                        "row": item["row"]
                    })

                trip_exists_check.update({
                    "status": "error",
                    "message": f"{len(invalid_trip_ids)} trip_id inexistants référencés",
                    "details": {
                        "invalid_trip_ids": list(invalid_trip_ids),
                        "invalid_stop_times": invalid_trip_group,
                        "total_invalid_entries": len(invalid_stop_times)
                    }
                })
            else:
                trip_exists_check["message"] = "Tous les trip_id référencés existent"
        else:
            trip_exists_check.update({
                "status": "info",
                "message": "Impossible de vérifier l'existence des trip_id",
                "details": {"reason": "missing_trips_file_or_columns"}
            })

    except Exception as e:
        trip_exists_check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les trip_id: {str(e)}",
            "details": {"error": str(e)}
        })

    checks.append(trip_exists_check)
    
    # 1. Vérifier service_id requis
    required_fields_check = check_required_fields_summary(df, ['stop_id', 'departure_time', 'arrival_time'], 'trip_id')
    checks.append(required_fields_check)

    # Vérifier que les stop_id référencés existent dans stops.txt
    stop_exists_check = {
        "check_name": "stop_id_exists",
        "description": "Les stop_id référencés existent dans stops.txt",
        "status": "pass",
        "message": "",
        "details": {}
    }

    try:
        stops_df = GTFSHandler.get_gtfs_data(project_id, 'stops.txt')
        
        if stops_df is not None and 'stop_id' in df.columns and 'stop_id' in stops_df.columns:
            # Récupérer les stop_id valides
            valid_stop_ids = set(stops_df['stop_id'].dropna().unique())
            stop_times_stop_ids = set(df['stop_id'].dropna().unique())
            
            # Trouver les stop_id dans stop_times mais pas dans stops
            invalid_stop_ids = stop_times_stop_ids - valid_stop_ids
            
            if invalid_stop_ids:
                invalid_stop_times = []
                for stop_id in invalid_stop_ids:
                    stop_times_with_invalid_stop = df[df['stop_id'] == stop_id]
                    for idx, row in stop_times_with_invalid_stop.iterrows():
                        trip_id = df.loc[idx, 'trip_id'] if 'trip_id' in df.columns else 'N/A'
                        stop_sequence = df.loc[idx, 'stop_sequence'] if 'stop_sequence' in df.columns else 'N/A'
                        invalid_stop_times.append({
                            "stop_id": str(stop_id),
                            "trip_id": str(trip_id),
                            "stop_sequence": str(stop_sequence),
                            "row": idx
                        })
                
                invalid_stop_group = {}
                for item in invalid_stop_times:
                    stop_id = str(item["stop_id"])
                    
                    if stop_id not in invalid_stop_group:
                        invalid_stop_group[stop_id] = []
                    
                    invalid_stop_group[stop_id].append({
                        "trip_id": item["trip_id"],
                        "stop_sequence": item["stop_sequence"],
                        "row": item["row"]
                    })

                stop_exists_check.update({
                    "status": "error",
                    "message": f"{len(invalid_stop_ids)} stop_id inexistants référencés",
                    "details": {
                        "invalid_stop_ids": list(invalid_stop_ids),
                        "invalid_stop_times": invalid_stop_group,
                        "total_invalid_entries": len(invalid_stop_times)
                    }
                })
            else:
                stop_exists_check["message"] = "Tous les stop_id référencés existent"
        else:
            stop_exists_check.update({
                "status": "info",
                "message": "Impossible de vérifier l'existence des stop_id",
                "details": {"reason": "missing_stops_file_or_columns"}
            })

    except Exception as e:
        stop_exists_check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les stop_id: {str(e)}",
            "details": {"error": str(e)}
        })

    checks.append(stop_exists_check)

    # Déterminer le statut global
    if not checks:
        overall_status = "pass"
    else:
        statuses = [check["status"] for check in checks]
        if "error" in statuses:
            overall_status = "error"
        elif "warning" in statuses:
            overall_status = "warning"
        else:
            overall_status = "pass"
    
    # NOUVEAU : Calculer le score de validité
    score_result = calculate_validity_score(checks)
    
    return {
        "status": overall_status,
        "checks": checks,
        "score": score_result["score"],
        "grade": score_result["grade"], 
        "percentage": score_result["percentage"]
    }

def _check_data_format(df):
    """Vérifications des champs optionnels"""
    checks = []
    
    # 1. Vérifier route_type
    pick_up_check = check_format_field(df, 'pickup_type', format['pickup_type'], 'trip_id')
    checks.append(pick_up_check) 

    drop_off_check = check_format_field(df, 'drop_off_type', format['drop_off_type'], 'trip_id')
    checks.append(drop_off_check) 

    # Déterminer le statut global
    if not checks:
        overall_status = "pass"
    else:
        statuses = [check["status"] for check in checks]
        if "error" in statuses:
            overall_status = "error"
        elif "warning" in statuses:
            overall_status = "warning"
        else:
            overall_status = "pass"
    
    # NOUVEAU : Calculer le score de validité
    score_result = calculate_validity_score(checks)
    
    return {
        "status": overall_status,
        "checks": checks,
        "score": score_result["score"],
        "grade": score_result["grade"], 
        "percentage": score_result["percentage"]
    }


def _generate_statistics(df, project_id):
    """Génère les statistiques du fichier"""
    checks = []
    # Métriques de base
    repartition = _calculate_stop_times_repartition(df, project_id)

    repartition_check = {
        "check_name": "stop_repartition",
        "description": "Analyse de la répartition des temps d\'arrêts",
        "status": "info",
        "message": f"Analyse de {len(df)} temps d'arrêts",
        "details": {"repartition": repartition}
    }
    checks.append(repartition_check)
    
    return {
        "status": "info",
        "checks": checks,
        "repartition":repartition,
    }

def _calculate_stop_times_repartition(df, project_id):
    """Calcule les métriques de base"""
    
    # 1. Nombre total de routes
    total_services = len(df)
    # 2. Répartition par type de transport
    stats_pickup = {}
    if 'pickup_type' in df.columns:
        type_counts = df['pickup_type'].value_counts().to_dict()

        
        for pickup_type, count in type_counts.items():
            if pd.notna(pickup_type):
                stats_pickup[str(int(pickup_type))] = {
                    "count": int(count),
                    "percentage": round((count / total_services) * 100, 1),
                    "type_name": pickup_type_names.get(int(pickup_type), f"Type {int(pickup_type)}")
                }

    stats_dropoff = {}
    if 'drop_off_type' in df.columns:
        type_counts = df['drop_off_type'].value_counts().to_dict()

        
        for drop_off_type, count in type_counts.items():
            if pd.notna(drop_off_type):
                stats_dropoff[str(int(drop_off_type))] = {
                    "count": int(count),
                    "percentage": round((count / total_services) * 100, 1),
                    "type_name": drop_off_type_names.get(int(drop_off_type), f"Type {int(drop_off_type)}")
                }    

    return {
        "stats_dropoff": stats_dropoff,
        "stats_pickup": stats_pickup
    }


pickup_type_names = {
    0: "Montée régulière",
    1: "Pas de montée autorisée",
    2: "Appeler l’agence pour monter",
    3: "Se coordonner avec le conducteur pour monter"
}

drop_off_type_names = {
    0: "Descente régulière",
    1: "Pas de descente autorisée",
    2: "Appeler l’agence pour descendre",
    3: "Se coordonner avec le conducteur pour descendre"
}

def _check_temporal_analysis(df):
    """Analyse des cohérences temporelles"""
    return analyze_temporal_consistency(
        df,
        'arrival_time',
        'departure_time', 
        'trip_id',
        'stop_sequence'
    )

def analyze_temporal_consistency(df, arrival_field, departure_field, trip_field, sequence_field):
    """
    Analyse complète des cohérences temporelles
    """
    # 1. Validation technique des formats
    technical_validation = _validate_time_formats(df, arrival_field, departure_field, trip_field)
    
    # 2. Cohérence intra-arrêt (arrivée <= départ)
    intra_stop_checks = _validate_intra_stop_consistency(df, arrival_field, departure_field, trip_field, sequence_field)
    
    # 3. Cohérence séquentielle par voyage
    sequential_checks = _validate_sequential_consistency(df, arrival_field, departure_field, trip_field, sequence_field)
    
    # 4. Métriques métier
    business_metrics = _calculate_temporal_metrics(df, arrival_field, departure_field, trip_field, sequence_field)
    
    # 5. Recommandations
    recommendations = _generate_temporal_recommendations(technical_validation, intra_stop_checks, sequential_checks, business_metrics)
    
    # 6. Compiler tous les checks
    all_checks = [technical_validation] + intra_stop_checks + sequential_checks
    
    # 7. Statut global
    statuses = [check["status"] for check in all_checks]
    if "error" in statuses:
        overall_status = "error"
    elif "warning" in statuses:
        overall_status = "warning"
    else:
        overall_status = "pass"
    
    # 8. Score global
    score_result = calculate_validity_score(all_checks)
    
    return {
        "status": overall_status,
        "checks": all_checks,
        "score": score_result["score"],
        "grade": score_result["grade"],
        "percentage": score_result["percentage"],
        
        # Données détaillées pour l'affichage
        "technical_validation": technical_validation,
        "intra_stop_checks": intra_stop_checks,
        "sequential_checks": sequential_checks,
        "business_metrics": business_metrics,
        "recommendations": recommendations,
        "field_info": {
            "arrival_field": arrival_field,
            "departure_field": departure_field,
            "trip_field": trip_field,
            "sequence_field": sequence_field
        }
    }

def _validate_time_formats(df, arrival_field, departure_field, trip_field):
    """Valide le format des horaires"""
    import re
    
    # Pattern pour HH:MM:SS (accepte > 24h)
    time_pattern = re.compile(r'^([0-9]{1,2}):([0-5][0-9]):([0-5][0-9])$')
    
    total_records = len(df)
    invalid_arrivals = 0
    invalid_departures = 0
    
    if arrival_field in df.columns:
        arrival_series = df[arrival_field].dropna()
        invalid_arrivals = sum(1 for time_str in arrival_series if not time_pattern.match(str(time_str)))
    
    if departure_field in df.columns:
        departure_series = df[departure_field].dropna()
        invalid_departures = sum(1 for time_str in departure_series if not time_pattern.match(str(time_str)))
    
    total_invalid = invalid_arrivals + invalid_departures
    
    if total_invalid == 0:
        status = "pass"
        message = "Tous les horaires sont au format valide"
    elif total_invalid < total_records * 0.1:  # Moins de 10%
        status = "warning"
        message = f"{total_invalid} horaires au format invalide"
    else:
        status = "error"
        message = f"{total_invalid} horaires au format invalide ({round(total_invalid/total_records*100, 1)}%)"
    
    return {
        "check_name": "time_format_validation",
        "description": "Validation du format des horaires",
        "status": status,
        "message": message,
        "details": {
            "total_records": int(total_records),
            "invalid_arrivals": int(invalid_arrivals),
            "invalid_departures": int(invalid_departures),
            "total_invalid": int(total_invalid)
        }
    }

def _validate_intra_stop_consistency(df, arrival_field, departure_field, trip_field, sequence_field):
    """Valide que arrival_time <= departure_time pour chaque arrêt"""
    checks = []
    
    if arrival_field not in df.columns or departure_field not in df.columns:
        return checks
    
    # Convertir les horaires en minutes depuis minuit
    def time_to_minutes(time_str):
        try:
            if pd.isna(time_str):
                return None
            parts = str(time_str).split(':')
            if len(parts) != 3:
                return None
            return int(parts[0]) * 60 + int(parts[1])
        except:
            return None
    
    df_copy = df.copy()
    df_copy['arrival_minutes'] = df_copy[arrival_field].apply(time_to_minutes)
    df_copy['departure_minutes'] = df_copy[departure_field].apply(time_to_minutes)
    
    # Trouver les incohérences
    inconsistent = df_copy[
        (df_copy['arrival_minutes'].notna()) & 
        (df_copy['departure_minutes'].notna()) &
        (df_copy['arrival_minutes'] > df_copy['departure_minutes'])
    ]
    
    intra_check = {
        "check_name": "arrival_before_departure",
        "description": "Arrivée doit être antérieure ou égale au départ",
        "status": "pass" if inconsistent.empty else "error",
        "message": f"{len(inconsistent)} arrêts avec arrivée > départ" if not inconsistent.empty else "Toutes les arrivées sont cohérentes",
        "details": {
            "inconsistent_count": int(len(inconsistent)),
            "inconsistent_trips": inconsistent[trip_field].tolist()[:20] if not inconsistent.empty else []
        }
    }
    checks.append(intra_check)
    
    # Vérifier les temps d'arrêt anormalement longs
    if not inconsistent.empty:
        df_valid = df_copy[
            (df_copy['arrival_minutes'].notna()) & 
            (df_copy['departure_minutes'].notna()) &
            (df_copy['arrival_minutes'] <= df_copy['departure_minutes'])
        ]
    else:
        df_valid = df_copy[
            (df_copy['arrival_minutes'].notna()) & 
            (df_copy['departure_minutes'].notna())
        ]
    
    if not df_valid.empty:
        df_valid['stop_duration'] = df_valid['departure_minutes'] - df_valid['arrival_minutes']
        long_stops = df_valid[df_valid['stop_duration'] > 60]  # Plus d'1h
        
        duration_check = {
            "check_name": "excessive_stop_duration",
            "description": "Temps d'arrêt excessifs (> 1h)",
            "status": "warning" if not long_stops.empty else "pass",
            "message": f"{len(long_stops)} arrêts avec temps > 1h" if not long_stops.empty else "Tous les temps d'arrêt sont raisonnables",
            "details": {
                "long_stops_count": int(len(long_stops)),
                "max_duration_minutes": int(df_valid['stop_duration'].max()) if not df_valid.empty else 0,
                "avg_duration_minutes": round(df_valid['stop_duration'].mean(), 1) if not df_valid.empty else 0
            }
        }
        checks.append(duration_check)
    
    return checks

def _validate_sequential_consistency(df, arrival_field, departure_field, trip_field, sequence_field):
   """Valide la cohérence séquentielle par voyage"""
   checks = []
   
   if not all(col in df.columns for col in [arrival_field, departure_field, trip_field, sequence_field]):
       return checks
   
   def time_to_minutes(time_str):
       try:
           if pd.isna(time_str):
               return None
           parts = str(time_str).split(':')
           if len(parts) != 3:
               return None
           return int(parts[0]) * 60 + int(parts[1])
       except:
           return None
   
   df_copy = df.copy()
   df_copy['arrival_minutes'] = df_copy[arrival_field].apply(time_to_minutes)
   df_copy['departure_minutes'] = df_copy[departure_field].apply(time_to_minutes)
   
   problematic_trips = []
   
   # Analyser voyage par voyage
   for trip_id, trip_data in df_copy.groupby(trip_field):
       if len(trip_data) < 2:
           continue
           
       # Trier par stop_sequence
       trip_sorted = trip_data.sort_values(sequence_field)
       
       for i in range(len(trip_sorted) - 1):
           current = trip_sorted.iloc[i]
           next_stop = trip_sorted.iloc[i + 1]
           
           current_dep = current['departure_minutes']
           next_arr = next_stop['arrival_minutes']
           
           if pd.notna(current_dep) and pd.notna(next_arr):
               if current_dep > next_arr:
                   problematic_trips.append({
                       "trip_id": str(trip_id),
                       "current_sequence": int(current[sequence_field]),
                       "next_sequence": int(next_stop[sequence_field]),
                       "current_departure": str(current[departure_field]),
                       "next_arrival": str(next_stop[arrival_field])
                   })
   
   sequential_check = {
       "check_name": "sequential_time_consistency",
       "description": "Cohérence temporelle séquentielle des voyages",
       "status": "pass" if not problematic_trips else "error",
       "message": f"{len(problematic_trips)} incohérences séquentielles détectées" if problematic_trips else "Toutes les séquences temporelles sont cohérentes",
       "details": {
           "problematic_trips_count": int(len(problematic_trips)),
           "problematic_trips": problematic_trips  # Limiter l'affichage
       }
   }
   checks.append(sequential_check)
   
   return checks

def _calculate_temporal_metrics(df, arrival_field, departure_field, trip_field, sequence_field):
    """Calcule les métriques temporelles"""
    def time_to_minutes(time_str):
        try:
            if pd.isna(time_str):
                return None
            parts = str(time_str).split(':')
            if len(parts) != 3:
                return None
            return int(parts[0]) * 60 + int(parts[1])
        except:
            return None
    
    df_copy = df.copy()
    df_copy['arrival_minutes'] = df_copy[arrival_field].apply(time_to_minutes)
    df_copy['departure_minutes'] = df_copy[departure_field].apply(time_to_minutes)
    
    # Temps d'arrêt
    valid_stops = df_copy[
        (df_copy['arrival_minutes'].notna()) & 
        (df_copy['departure_minutes'].notna()) &
        (df_copy['arrival_minutes'] <= df_copy['departure_minutes'])
    ]
    
    if not valid_stops.empty:
        valid_stops['stop_duration'] = valid_stops['departure_minutes'] - valid_stops['arrival_minutes']
        avg_stop_duration = round(valid_stops['stop_duration'].mean(), 1)
        max_stop_duration = int(valid_stops['stop_duration'].max())
    else:
        avg_stop_duration = max_stop_duration = 0
    
    # Durées de voyage
    trip_durations = []
    for trip_id, trip_data in df_copy.groupby(trip_field):
        if len(trip_data) < 2:
            continue
        trip_sorted = trip_data.sort_values(sequence_field)
        first_arrival = trip_sorted.iloc[0]['arrival_minutes']
        last_departure = trip_sorted.iloc[-1]['departure_minutes']
        
        if pd.notna(first_arrival) and pd.notna(last_departure) and last_departure >= first_arrival:
            trip_durations.append(last_departure - first_arrival)
    
    if trip_durations:
        avg_trip_duration = round(sum(trip_durations) / len(trip_durations), 1)
        max_trip_duration = max(trip_durations)
    else:
        avg_trip_duration = max_trip_duration = 0
    
    return {
        "total_stops": int(len(df)),
        "valid_stops_count": int(len(valid_stops)),
        "avg_stop_duration_minutes": float(avg_stop_duration),
        "max_stop_duration_minutes": int(max_stop_duration),
        "total_trips": int(df[trip_field].nunique()),
        "avg_trip_duration_minutes": float(avg_trip_duration),
        "max_trip_duration_minutes": int(max_trip_duration),
        "trips_with_duration": int(len(trip_durations))
    }

def _generate_temporal_recommendations(technical_validation, intra_checks, sequential_checks, metrics):
    """Génère les recommandations temporelles"""
    recommendations = []
    
    # Recommandations techniques
    if technical_validation["status"] == "error":
        recommendations.append({
            "type": "critical",
            "message": "Formats d'horaires invalides détectés",
            "description": "Corrigez les horaires au format HH:MM:SS pour assurer la cohérence des données."
        })
    
    # Recommandations de cohérence
    for check in intra_checks:
        if check["check_name"] == "arrival_before_departure" and check["status"] == "error":
            recommendations.append({
                "type": "critical",
                "message": "Incohérences arrivée/départ détectées",
                "description": "Certains arrêts ont une heure d'arrivée postérieure au départ. Vérifiez les données."
            })
        elif check["check_name"] == "excessive_stop_duration" and check["status"] == "warning":
            recommendations.append({
                "type": "warning",
                "message": "Temps d'arrêt très longs détectés",
                "description": f"Durée moyenne: {metrics['avg_stop_duration_minutes']}min. Vérifiez les arrêts de plus d'1h."
            })
    
    for check in sequential_checks:
        if check["status"] == "error":
            recommendations.append({
                "type": "critical",
                "message": "Voyages incohérents temporellement",
                "description": "Certains voyages 'remontent dans le temps'. Vérifiez les séquences d'arrêts."
            })
    
    # Recommandations métriques
    if metrics["avg_trip_duration_minutes"] > 300:  # Plus de 5h
        recommendations.append({
            "type": "info",
            "message": "Voyages très longs détectés",
            "description": f"Durée moyenne: {metrics['avg_trip_duration_minutes']}min. Vérifiez la cohérence du réseau."
        })
    
    return recommendations