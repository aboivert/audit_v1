"""
Fonctions d'audit pour le fichier trips.txt
"""
import pandas as pd
import re
from datetime import datetime
from urllib.parse import urlparse
from services.gtfs_handler import GTFSHandler

from .generic_functions import is_truly_empty
from .generic_functions import check_required_field
from .generic_functions import check_required_fields_summary
from .generic_functions import calculate_score_from_checks
from .generic_functions import check_format_field
from .generic_functions import check_orphan_id
from .generic_functions import check_unused_id
from .generic_functions import calculate_summary
from .generic_functions import calculate_validity_score
from .generic_functions import analyze_accessibility_field

import pytz
format = {'cars_allowed':{'genre':'optional','description':"Validité de l'autorisation de prendre une voiture à bord",'type':'listing', 'valid_fields':['0', '1','2']},
          'bikes_allowed':{'genre':'optional','description':"Validité de l'autorisation de prendre un vélo à bord",'type':'listing', 'valid_fields':['0', '1','2']},
          'wheelchair_accessible':{'genre':'optional','description':"Validité des embarquements UFR",'type':'listing', 'valid_fields':['0', '1','2']},
          'direction_id':{'genre':'optional','description':"Validité des sens de direction",'type':'listing', 'valid_fields':['0','1']},
}

def audit_trips_file(project_id, progress_callback = None):
    """
    Audit complet du fichier stops.txt
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Résultats de l'audit selon la structure définie
    """

    if progress_callback:
        progress_callback(15, "Chargement du fichier trips.txt...", "loading")

    trips_df = GTFSHandler.get_gtfs_data(project_id, 'trips.txt')
    
    if trips_df is None or trips_df.empty:
        if progress_callback:
            progress_callback(0, "Fichier trips.txt introuvable", "error")
        return {
            "file": "trips.txt",
            "status": "missing",
            "message": "Fichier trips.txt non trouvé ou vide",
            "severity": "critical",
            "timestamp": datetime.now().isoformat()
        }
    
    if progress_callback:
        progress_callback(25, "Vérification des champs obligatoires...", "required_fields")
    
    results = {
        "file": "trips.txt",
        "status": "processed",
        "total_rows": len(trips_df),
        "timestamp": datetime.now().isoformat(),
        "required_fields": _check_required_fields(trips_df, project_id)
    }

    if progress_callback:
        progress_callback(35, "Vérification du format des donnés...", "data_format")

    results["data_format"] = _check_data_format(trips_df)

    if progress_callback:
        progress_callback(45, "Vérification de la cohérence des données...", "data_consistency")

    results["data_consistency"] = _check_data_consistency(trips_df, project_id)


    if progress_callback:
        progress_callback(55, "Analyse UFR spécialisée...", "ufr_analysis")

    results["ufr_analysis"] = _check_ufr_analysis(trips_df)


    if progress_callback:
        progress_callback(65, "Génération de statistiques...", "statistics")

    results["statistics"] = _generate_statistics(trips_df, project_id)

    if progress_callback:
        progress_callback(80, "Calcul du résumé...", "summary")

    # Calculer le résumé global
    results["summary"] = calculate_summary(results, ['required_fields','data_format', 'data_consistency', 'ufr_analysis', 'statistics'])
    return results
    
def _check_required_fields(df, project_id):
    """Vérifications des champs obligatoires"""
    checks = []
    
    # 1. Vérifier trip_id
    trip_id_check = check_required_field(df, 'trip_id', 'trips_df')
    checks.append(trip_id_check)
    
    # 2. Vérifier unicité des route_id
    if 'trip_id' in df.columns and not df['trip_id'].isna().all():
        uniqueness_check = {
            "check_name": "trip_id_unique",
            "description": "Unicité des trip_id",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        duplicates = df[df.duplicated('trip_id', keep=False) & df['trip_id'].notna()]
        if not duplicates.empty:
            duplicate_ids = duplicates['trip_id'].unique().tolist()
            duplicate_rows = duplicates.index.tolist()
            
            # Ajouter les noms des routes pour plus de clarté
            duplicate_details = []
            for dup_id in duplicate_ids:
                dup_rows = duplicates[duplicates['trip_id'] == dup_id]
                detail = {
                    "trip_id": str(dup_id),
                    "occurrences": len(dup_rows),
                    "rows": dup_rows.index.tolist(),
                }
            
            uniqueness_check.update({
                "status": "error",
                "message": f"{len(duplicate_ids)} trip_id dupliqués",
                "details": {
                    "duplicate_ids": [str(x) for x in duplicate_ids], 
                    "duplicate_rows": duplicate_rows,
                    "duplicate_details": duplicate_details
                }
            })
        else:
            uniqueness_check["message"] = "Tous les trip_id sont uniques"
            
        checks.append(uniqueness_check)
    
    required_fields_check = check_required_fields_summary(df, ['route_id', 'service_id'], 'trip_id')
    checks.append(required_fields_check)

    # 7. Vérifier que les agency_id référencés existent
    route_exists_check = {
        "check_name": "route_id_exists",
        "description": "Les route_id référencés existent dans routes.txt",
        "status": "pass",
        "message": "",
        "details": {}
    }
    
    try:
        route_df = GTFSHandler.get_gtfs_data(project_id, 'routes.txt')
        
        if route_df is not None and 'route_id' in df.columns and 'route_id' in route_df.columns:
            # Récupérer les route_id valides
            valid_route_ids = set(route_df['route_id'].dropna().unique())
            trips_route_ids = set(df['route_id'].dropna().unique())
            
            # Trouver les agency_id dans routes mais pas dans agency
            invalid_route_ids = trips_route_ids - valid_route_ids
            
            if invalid_route_ids:
                invalid_trips = []
                for route_id in invalid_route_ids:
                    trips_with_invalid_route = df[df['route_id'] == route_id]
                    for idx, row in trips_with_invalid_route.iterrows():
                        trip_id = df.loc[idx, 'trip_id'] if 'trip_id' in df.columns else 'N/A'
                        invalid_trips.append({
                            "route_id": str(route_id),
                            "trip_id": str(trip_id),
                        })
                invalid_trip_group = {}
                for item in invalid_trips:
                    route_id = str(item["route_id"])
                    trip_id = str(item["trip_id"])
                    
                    if route_id not in invalid_trip_group:
                        invalid_trip_group[route_id] = []
                    
                    invalid_trip_group[route_id].append(trip_id)


                route_exists_check.update({
                    "status": "error",
                    "message": f"{len(invalid_route_ids)} route_id inexistants référencés",
                    "details": {
                        "invalid_route_ids": list(invalid_route_ids),
                        "invalid_trips": invalid_trip_group,
                        #"valid_agency_ids": list(valid_agency_ids)
                    }
                })
            else:
                route_exists_check["message"] = "Tous les route_id référencés existent"
        else:
            route_exists_check.update({
                "status": "info",
                "message": "Impossible de vérifier l'existence des route_id",
                "details": {"reason": "missing_route_file_or_columns"}
            })
    
    except Exception as e:
        route_exists_check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les route_id: {str(e)}",
            "details": {"error": str(e)}
        })
    
    checks.append(route_exists_check)

    service_exists_check = {
        "check_name": "service_id_exists",
        "description": "Les service_id référencés existent dans calendar.txt et/ou calendar_dates.txt",
        "status": "pass",
        "message": "",
        "details": {}
    }

    try:
        calendar_df = GTFSHandler.get_gtfs_data(project_id, 'calendar.txt')
        calendar_dates_df = GTFSHandler.get_gtfs_data(project_id, 'calendar_dates.txt')
        
        if 'service_id' in df.columns:
            # Récupérer les service_id valides selon la logique spécifiée
            valid_service_ids = set()
            
            # Si calendar.txt existe et contient service_id
            calendar_exists = calendar_df is not None and 'service_id' in calendar_df.columns
            # Si calendar_dates.txt existe et contient service_id  
            calendar_dates_exists = calendar_dates_df is not None and 'service_id' in calendar_dates_df.columns
            
            if calendar_exists and calendar_dates_exists:
                # Les deux existent : union des service_id
                calendar_service_ids = set(calendar_df['service_id'].dropna().unique())
                calendar_dates_service_ids = set(calendar_dates_df['service_id'].dropna().unique())
                valid_service_ids = calendar_service_ids.union(calendar_dates_service_ids)
            elif calendar_exists:
                # Seul calendar existe
                valid_service_ids = set(calendar_df['service_id'].dropna().unique())
            elif calendar_dates_exists:
                # Seul calendar_dates existe
                valid_service_ids = set(calendar_dates_df['service_id'].dropna().unique())
            else:
                # Aucun des deux fichiers n'existe ou ne contient service_id
                service_exists_check.update({
                    "status": "info",
                    "message": "Impossible de vérifier l'existence des service_id",
                    "details": {"reason": "missing_calendar_files_or_columns"}
                })
                return service_exists_check
            
            trips_service_ids = set(df['service_id'].dropna().unique())
            
            # Trouver les service_id dans trips mais pas dans calendar/calendar_dates
            invalid_service_ids = trips_service_ids - valid_service_ids
            
            if invalid_service_ids:
                invalid_trips = []
                for service_id in invalid_service_ids:
                    trips_with_invalid_service = df[df['service_id'] == service_id]
                    for idx, row in trips_with_invalid_service.iterrows():
                        trip_id = df.loc[idx, 'trip_id'] if 'trip_id' in df.columns else 'N/A'
                        invalid_trips.append({
                            "service_id": str(service_id),
                            "trip_id": str(trip_id),
                        })
                
                invalid_trip_group = {}
                for item in invalid_trips:
                    service_id = str(item["service_id"])
                    trip_id = str(item["trip_id"])
                    
                    if service_id not in invalid_trip_group:
                        invalid_trip_group[service_id] = []
                    
                    invalid_trip_group[service_id].append(trip_id)

                service_exists_check.update({
                    "status": "error",
                    "message": f"{len(invalid_service_ids)} service_id inexistants référencés",
                    "details": {
                        "invalid_service_ids": list(invalid_service_ids),
                        "invalid_trips": invalid_trip_group,
                    }
                })
            else:
                service_exists_check["message"] = "Tous les service_id référencés existent"
        else:
            service_exists_check.update({
                "status": "info", 
                "message": "Colonne service_id manquante dans trips.txt",
                "details": {"reason": "missing_service_id_column"}
            })

    except Exception as e:
        service_exists_check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les service_id: {str(e)}",
            "details": {"error": str(e)}
        })

    checks.append(service_exists_check)

    name_check = {
        "check_name": "trip_short_name_or_headsign",
        "description": "Au moins trip_short_name ou trip_headsign doit être présent",
        "status": "pass",
        "message": "",
        "details": {}
    }

    has_short = 'trip_short_name' in df.columns
    has_headsign = 'trip_headsign' in df.columns

    if not has_short and not has_headsign:
        name_check.update({
            "status": "error",
            "message": "Ni trip_short_name ni trip_headsign ne sont présents",
            "details": {"missing_both_columns": True}
        })
    else:
        # Vérifier pour chaque ligne qu'au moins un nom est présent
        issues = []
        for idx, row in df.iterrows():
            short_empty = not has_short or is_truly_empty(row.get('trip_short_name'))
            headsign_empty = not has_headsign or is_truly_empty(row.get('trip_headsign'))
            
            if short_empty and headsign_empty:
                # Récupérer les identifiants du trip
                trip_id = df.loc[idx, 'trip_id'] if 'trip_id' in df.columns else 'N/A'
                issues.append({
                    "row": idx,
                    "trip_id": str(trip_id)
                })
        
        if issues:
            name_check.update({
                "status": "error",
                "message": f"{len(issues)} trips sans nom court ni headsign",
                "details": {
                    "missing_both_names": len(issues),
                    "affected_trips": issues
                }
            })
        else:
            name_check["message"] = "Tous les trips ont au moins un nom (court ou headsign)"

    checks.append(name_check)
    
    # Déterminer le statut global des champs requis
    statuses = [check["status"] for check in checks]
    if "error" in statuses:
        overall_status = "error"
    elif "warning" in statuses:
        overall_status = "warning"
    else:
        overall_status = "pass"
    score_result = calculate_score_from_checks(checks, overall_status)
    return {
        "status": overall_status,
        "checks": checks,
        "score": score_result["score"],           # 85.0
        "grade": score_result["grade"],           # "B"
        "percentage": score_result["percentage"], # 85.0
        "score_breakdown": score_result["breakdown"]  # Détails complets
    }

def _check_data_format(df):
    """Vérifications des champs optionnels"""
    checks = []
    
    # 1. Vérifier route_type
    direction_id = check_format_field(df, 'direction_id', format['direction_id'], 'trip_id')
    checks.append(direction_id)   

    bikes_allowed = check_format_field(df, 'bikes_allowed', format['bikes_allowed'], 'trip_id')
    checks.append(bikes_allowed)   

    '''cars_allowed = check_format_field(df, 'cars_allowed', format['cars_allowed'], 'trip_id')
    checks.append(cars_allowed)'''

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

def _check_data_consistency(df, project_id):
    """Vérifications de cohérence des données"""
    checks = []

    shapes_df = GTFSHandler.get_gtfs_data(project_id, 'shapes.txt')
    if shapes_df is not None:
        shape_exists_check = check_orphan_id(shapes_df, 'shape_id', 'trips.txt', project_id)
        # Note: on passe shapes_df comme DataFrame source et 'trips.txt' comme target_file
        # mais en réalité on veut vérifier l'inverse, donc on peut adapter :
        
        shape_exists_check = check_orphan_id(df, 'shape_id', 'shapes.txt', project_id)
        checks.append(shape_exists_check)
    else:
        # Si shapes.txt n'existe pas, créer un check informatif
        shape_exists_check = {
            "check_name": "no_orphan_shape_id_in_shapes",
            "description": "shape_id référencés dans shapes.txt",
            "status": "info",
            "message": "Fichier shapes.txt absent - vérification impossible",
            "details": {"reason": "missing_shapes_file"}
        }
        checks.append(shape_exists_check)
    
    # Déterminer le statut global
    statuses = [check["status"] for check in checks]
    if "error" in statuses:
        overall_status = "error"
    elif "warning" in statuses:
        overall_status = "warning"
    else:
        overall_status = "pass"

    # NOUVEAU : Calculer le score de validité (même méthode que data_format)
    score_result = calculate_validity_score(checks)
    
    return {
        "status": overall_status,
        "checks": checks,
        "score": score_result["score"],
        "grade": score_result["grade"], 
        "percentage": score_result["percentage"]
    }

def _check_ufr_analysis(df):
    """Analyse UFR spécifique aux arrêts (wheelchair_accessible)"""
    return analyze_accessibility_field(
        df, 
        'wheelchair_accessible', 
        format['wheelchair_accessible'],
        'trip_id',
        wheelchair_accessible_names
    )

def _generate_statistics(df, project_id):
    """Génère les statistiques du fichier"""
    checks = []
    # Métriques de base
    repartition = _calculate_trip_repartition(df, project_id)

    repartition_check = {
        "check_name": "stop_repartition",
        "description": "Analyse de la répartition des trajets",
        "status": "info",
        "message": f"Analyse de {len(df)} trajets",
        "details": {"repartition": repartition}
    }
    checks.append(repartition_check)
    
    return {
        "status": "info",
        "checks": checks,
        "repartition":repartition,
    }

def _calculate_trip_repartition(df, project_id):
    """Calcule les métriques de base"""
    
    # 1. Nombre total de routes
    total_stops = len(df)
    # 2. Répartition par type de transport
    stops_by_direction_id = {}
    if 'direction_id' in df.columns:
        type_counts = df['direction_id'].value_counts().to_dict()

        
        for direction_id, count in type_counts.items():
            if pd.notna(direction_id):
                stops_by_direction_id[str(int(direction_id))] = {
                    "count": int(count),
                    "percentage": round((count / total_stops) * 100, 1),
                    "type_name": direction_id_names.get(int(direction_id), f"Type {int(direction_id)}")
                }

    stops_by_bikes_allowed = {}
    if 'bikes_allowed' in df.columns:
        type_counts = df['bikes_allowed'].value_counts().to_dict()

        
        for bikes_allowed, count in type_counts.items():
            if pd.notna(bikes_allowed):
                stops_by_bikes_allowed[str(int(bikes_allowed))] = {
                    "count": int(count),
                    "percentage": round((count / total_stops) * 100, 1),
                    "type_name": bikes_allowed_names.get(int(bikes_allowed), f"Type {int(bikes_allowed)}")
                }

    '''stops_by_cars_allowed = {}
    if 'cars_allowed' in df.columns:
        type_counts = df['cars_allowed'].value_counts().to_dict()

        
        for cars_allowed, count in type_counts.items():
            if pd.notna(cars_allowed):
                stops_by_cars_allowed[str(int(cars_allowed))] = {
                    "count": int(count),
                    "percentage": round((count / total_stops) * 100, 1),
                    "type_name": cars_allowed_names.get(int(cars_allowed), f"Type {int(cars_allowed)}")
                }'''
    
    return {
        "stops_by_direction_id": stops_by_direction_id,
        "stops_by_bikes_allowed": stops_by_bikes_allowed,
        #"stops_by_cars_allowed": stops_by_cars_allowed
    }


direction_id_names = {
    0: "Aller (ou direction par défaut)",
    1: "Retour (ou direction opposée)"
}

bikes_allowed_names = {
    0: "Pas d'information sur l'accessibilité aux vélos pour le trajet",
    1: "Vélos autorisés à bord pour le trajet",
    2: "Vélos non autorisés à bord pour le trajet"
}

cars_allowed_names = {
    0: "Pas d'information sur l'accessibilité aux voitures pour le trajet",
    1: "Voitures autorisées à bord ou sur le trajet",
    2: "Voitures non autorisées à bord ou sur le trajet"
}

wheelchair_accessible_names = {
    0: "Pas d'information sur l'accessibilité UFR du trajet",
    1: "Trajet accessible aux UFR",
    2: "Trajet non accessible aux UFR"
}