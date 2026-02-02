"""
Fonctions d'audit pour le fichier stops.txt
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
format = {'stop_timezone':{'genre':'optional','description':"Validité des fuseaux horaires", 'type':'listing', 'valid_fields':set(pytz.all_timezones)},
          'stop_url':{'genre':'optional','description':"Validité des URL",'type':'url'},
          'stop_lat':{'genre':'required','description':"Validité des latitudes",'type':'coordinates'},
          'stop_lon':{'genre':'required','description':"Validité des longitudes",'type':'coordinates'},
          'wheelchair_boarding':{'genre':'optional','description':"Validité des embarquements UFR",'type':'listing', 'valid_fields':['0', '1','2']},
          'location_type':{'genre':'optional','description':"Validité des types de location",'type':'listing', 'valid_fields':['0','1','2', '3', '4']},
}

def audit_stops_file(project_id, progress_callback = None):
    """
    Audit complet du fichier stops.txt
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Résultats de l'audit selon la structure définie
    """

    if progress_callback:
        progress_callback(15, "Chargement du fichier stops.txt...", "loading")

    stops_df = GTFSHandler.get_gtfs_data(project_id, 'stops.txt')
    
    if stops_df is None or stops_df.empty:
        if progress_callback:
            progress_callback(0, "Fichier stops.txt introuvable", "error")
        return {
            "file": "stops.txt",
            "status": "missing",
            "message": "Fichier stops.txt non trouvé ou vide",
            "severity": "critical",
            "timestamp": datetime.now().isoformat()
        }
    
    if progress_callback:
        progress_callback(25, "Vérification des champs obligatoires...", "required_fields")
    
    results = {
        "file": "stops.txt",
        "status": "processed",
        "total_rows": len(stops_df),
        "timestamp": datetime.now().isoformat(),
        "required_fields": _check_required_fields(stops_df, project_id)
    }

    if progress_callback:
        progress_callback(35, "Vérification du format des donnés...", "data_format")

    results["data_format"] = _check_data_format(stops_df)

    if progress_callback:
        progress_callback(45, "Vérification de la cohérence des données...", "data_consistency")

    results["data_consistency"] = _check_data_consistency(stops_df, project_id)


    if progress_callback:
        progress_callback(55, "Analyse UFR spécialisée...", "ufr_analysis")

    results["ufr_analysis"] = _check_ufr_analysis(stops_df)

    if progress_callback:
        progress_callback(65, "Analyse des zones d'arrêt...", "hierarchy_analysis")

    results["hierarchy_analysis"] = _check_hierarchy_analysis(stops_df)


    if progress_callback:
        progress_callback(75, "Génération de statistiques...", "statistics")

    results["statistics"] = _generate_statistics(stops_df, project_id)

    if progress_callback:
        progress_callback(85, "Calcul du résumé...", "summary")

    # Calculer le résumé global
    results["summary"] = calculate_summary(results, ['required_fields','data_format','data_consistency', 'ufr_analysis', 'hierarchy_analysis', 'statistics'])
    return results
    
def _check_required_fields(df, project_id):
    """Vérifications des champs obligatoires"""
    checks = []
    
    # 1. Vérifier route_id
    stop_id_check = check_required_field(df, 'stop_id', 'stop_id')
    checks.append(stop_id_check)
    
    # 2. Vérifier unicité des route_id
    if 'stop_id' in df.columns and not df['stop_id'].isna().all():
        uniqueness_check = {
            "check_name": "stop_id_unique",
            "description": "Unicité des stop_id",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        duplicates = df[df.duplicated('stop_id', keep=False) & df['stop_id'].notna()]
        if not duplicates.empty:
            duplicate_ids = duplicates['stop_id'].unique().tolist()
            duplicate_rows = duplicates.index.tolist()
            
            # Ajouter les noms des routes pour plus de clarté
            duplicate_details = []
            for dup_id in duplicate_ids:
                dup_rows = duplicates[duplicates['stop_id'] == dup_id]
                detail = {
                    "stop_id": str(dup_id),
                    "occurrences": len(dup_rows),
                    "rows": dup_rows.index.tolist(),
                }
                if 'stop_name' in df.columns:
                    detail["stop_names"] = dup_rows['stop_names'].fillna('N/A').astype(str).tolist()
                duplicate_details.append(detail)
            
            uniqueness_check.update({
                "status": "error",
                "message": f"{len(duplicate_ids)} stop_id dupliqués",
                "details": {
                    "duplicate_ids": [str(x) for x in duplicate_ids], 
                    "duplicate_rows": duplicate_rows,
                    "duplicate_details": duplicate_details
                }
            })
        else:
            uniqueness_check["message"] = "Tous les stop_id sont uniques"
            
        checks.append(uniqueness_check)
    
    required_fields_check = check_required_fields_summary(df, ['stop_name', 'stop_lat', 'stop_lon'], 'stop_id')
    checks.append(required_fields_check)
    
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
    stop_url = check_format_field(df, 'stop_url', format['stop_url'], 'stop_id')
    checks.append(stop_url)   

    stop_timezone = check_format_field(df, 'stop_timezone', format['stop_timezone'], 'stop_id')
    checks.append(stop_timezone)   

    stop_lat = check_format_field(df, 'stop_lat', format['stop_lat'], 'stop_id')
    checks.append(stop_lat)   

    stop_lon = check_format_field(df, 'stop_lon', format['stop_lon'], 'stop_id')
    checks.append(stop_lon)   
    
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
        
    # stop_id référencé dans stop_times.txt mais absent de stops.txt
    orphan_stops_in_stop_times = check_orphan_id(df, 'stop_id', 'stop_times.txt', project_id)
    checks.append(orphan_stops_in_stop_times)
    # stop_id référencé dans transfers.txt (from_stop_id) mais absent de stops.txt  
    orphan_stops_in_transfers_from = check_orphan_id(df, 'stop_id', 'transfers.txt', project_id, target_field='from_stop_id')
    checks.append(orphan_stops_in_transfers_from)

    # stop_id référencé dans transfers.txt (to_stop_id) mais absent de stops.txt
    orphan_stops_in_transfer_to = check_orphan_id(df, 'stop_id', 'transfers.txt', project_id, target_field='to_stop_id')
    checks.append(orphan_stops_in_transfer_to)

    # stop_id défini dans stops.txt mais jamais utilisé dans stop_times.txt
    unused_stops_in_stop_times = check_unused_id(df, 'stop_id', 'stop_times.txt', project_id)
    checks.append(unused_stops_in_stop_times)

    # stop_id défini mais jamais utilisé comme origine dans transfers.txt
    unused_stops_in_transfers_from = check_unused_id(df, 'stop_id', 'transfers.txt', project_id, target_field='from_stop_id')
    checks.append(unused_stops_in_transfers_from)

    # stop_id défini mais jamais utilisé comme destination dans transfers.txt  
    unused_stops_in_transfers_to = check_unused_id(df, 'stop_id', 'transfers.txt', project_id, target_field='to_stop_id')
    checks.append(unused_stops_in_transfers_to)

    # zone_id référencé dans stops.txt mais absent de fare_rules.txt
    orphan_stops_in_fare_origin = check_orphan_id(df, 'zone_id', 'fare_rules.txt', project_id, target_field='origin_id')
    checks.append(orphan_stops_in_fare_origin)

    orphan_stops_in_fare_destination = check_orphan_id(df, 'zone_id', 'fare_rules.txt', project_id, target_field='destination_id')
    checks.append(orphan_stops_in_fare_destination)    
    
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
    """Analyse UFR spécifique aux arrêts (wheelchair_boarding)"""
    return analyze_accessibility_field(
        df, 
        'wheelchair_boarding', 
        format['wheelchair_boarding'],
        'stop_id',
        wheelchair_boarding_names
    )

def _generate_statistics(df, project_id):
    """Génère les statistiques du fichier"""
    checks = []
    # Métriques de base
    repartition = _calculate_stop_repartition(df, project_id)

    repartition_check = {
        "check_name": "stop_repartition",
        "description": "Analyse de la répartition des arrêts",
        "status": "info",
        "message": f"Analyse de {len(df)} arrêts",
        "details": {"repartition": repartition}
    }
    checks.append(repartition_check)
    
    return {
        "status": "info",
        "checks": checks,
        "repartition":repartition,
    }

def _calculate_stop_repartition(df, project_id):
    """Calcule les métriques de base"""
    
    # 1. Nombre total de routes
    total_stops = len(df)
    # 2. Répartition par type de transport
    stops_by_location_type = {}
    if 'location_type' in df.columns:
        type_counts = df['location_type'].value_counts().to_dict()

        
        for location_type, count in type_counts.items():
            if pd.notna(location_type):
                stops_by_location_type[str(int(location_type))] = {
                    "count": int(count),
                    "percentage": round((count / total_stops) * 100, 1),
                    "type_name": location_type_names.get(int(location_type), f"Type {int(location_type)}")
                }
    
    return {
        "stops_by_location_type": stops_by_location_type,
    }


location_type_names = {
    # Types de base GTFS
    0: "Point d'arrêt",
    1: "Zone d'arrêt",
    2: "Entrée/Sortie",
    3: "Noeud générique",
    4: "Zone d'embarquement",
}

wheelchair_boarding_names = {
    # Types de base GTFS
    0: "Pas d'information d'accessibilité UFR pour l'arrêt",
    1: "Embarquement UFR possible pour l'arrêt",
    2: "Pas d'embarquement UFR possible pour l'arrêt"
}

def _check_hierarchy_analysis(df):
    """Analyse de la hiérarchie location_type et parent_station"""
    
    # Vérifier que la colonne location_type existe
    if 'location_type' not in df.columns:
        return {
            "status": "info",
            "score": 0,
            "grade": "N/A",
            "percentage": 0,
            "checks" : [],
            "field_info": {
                "field_name": "location_type",
                "parent_field": "parent_station",
                "id_field": "stop_id"
            },
            "technical_validation": {
                "check_name": "missing_location_type",
                "description": "Colonne location_type manquante",
                "status": "info",
                "message": "La colonne location_type n'est pas présente dans le fichier"
            },
            "business_metrics": None,
            "consistency_checks": [],
            "geographic_analysis": None,
            "repartition": {},
            "recommendations": [{
                "type": "info",
                "message": "Colonne location_type absente",
                "description": "L'analyse de la hiérarchie nécessite la colonne location_type."
            }]
        }
    
    # NOUVELLE VÉRIFICATION : Colonne location_type vide
    if df['location_type'].isna().all() or (df['location_type'] == '').all():
        return {
            "status": "error",
            "score": 0,
            "grade": "F",
            "percentage": 0,
            "checks": [],
            "message": "Colonne location_type vide - analyse impossible"
        }
    
    # NOUVELLE VÉRIFICATION : Aucune zone d'arrêt (location_type = 1)
    zones_count = len(df[df['location_type'] == 1])
    if zones_count == 0:
        return {
            "status": "error", 
            "score": 0,
            "grade": "F",
            "percentage": 0,
            "checks": [],
            "message": "Aucune zone d'arrêt (location_type=1) définie - analyse impossible"
        }
    
    # Si parent_station n'existe pas, analyse partielle
    if 'parent_station' not in df.columns:
        return {
            "status": "info",
            "score": 50,  # Score partiel car on peut analyser location_type
            "grade": "C",
            "percentage": 50,
            "checks":[],
            "field_info": {
                "field_name": "location_type",
                "parent_field": "parent_station",
                "id_field": "stop_id"
            },
            "technical_validation": check_format_field(df, 'location_type', format['location_type'], 'stop_id'),
            "business_metrics": _calculate_hierarchy_metrics_partial(df),
            "consistency_checks": [],
            "geographic_analysis": None,
            "repartition": _calculate_hierarchy_repartition(df, 'location_type', location_type_names),
            "recommendations": [{
                "type": "warning",
                "message": "Colonne parent_station absente",
                "description": "L'analyse complète de la hiérarchie nécessite la colonne parent_station pour valider les relations entre points et zones d'arrêt."
            }]
        }
    
    # Analyse complète si tout est OK
    return analyze_hierarchy_system(
        df, 
        'location_type',
        'parent_station', 
        format['location_type'],
        'stop_id',
        location_type_names
    )

def _calculate_hierarchy_metrics_partial(df):
    """Calcule les métriques de base quand parent_station n'existe pas"""
    total_records = len(df)
    
    # Compter par type
    stop_points = df[df['location_type'] == 0] if 'location_type' in df.columns else pd.DataFrame()
    stop_zones = df[df['location_type'] == 1] if 'location_type' in df.columns else pd.DataFrame()
    
    return {
        "total_records": total_records,
        "stop_points_count": len(stop_points),
        "stop_zones_count": len(stop_zones),
        "parent_completion_rate": 0,  # Pas de parent_station
        "points_with_parent_count": 0,
        "used_zones_count": 0,
        "unused_zones_count": len(stop_zones),
        "points_per_zone_avg": 0,
        "hierarchy_usage_rate": 0
    }

def analyze_hierarchy_system(df, location_field, parent_field, field_format, id_field, type_names):
    """
    Analyse complète du système de hiérarchie location_type/parent_station
    """
    # 1. Validation technique
    technical_validation = check_format_field(df, location_field, field_format, id_field)
    
    # 2. Métriques métier
    business_metrics = _calculate_hierarchy_metrics(df, location_field, parent_field, id_field)
    
    # 3. Validation de cohérence
    consistency_checks = _validate_hierarchy_consistency(df, location_field, parent_field, id_field)
    
    # 4. Analyse géographique (toujours inclure, même si info)
    geographic_analysis = _analyze_geographic_distances(df, location_field, parent_field, id_field)
    
    # 5. Répartition
    repartition = _calculate_hierarchy_repartition(df, location_field, type_names)
    
    # 7. Compiler tous les checks pour calculate_summary
    all_checks = [technical_validation] + consistency_checks + [geographic_analysis]
    
    # 8. Déterminer le statut global
    statuses = [check["status"] for check in all_checks]
    if "error" in statuses:
        overall_status = "error"
    elif "warning" in statuses:
        overall_status = "warning"  
    else:
        overall_status = "pass"
    
    # 9. Calcul du score global
    score_result = calculate_validity_score(all_checks)
    
    # 6. Recommandations
    recommendations = _generate_hierarchy_recommendations(business_metrics, consistency_checks, geographic_analysis)
    
    return {
        # STRUCTURE REQUISE POUR calculate_summary
        "status": overall_status,           # ← REQUIS
        "checks": all_checks,              # ← REQUIS
        
        # Score (comme les autres catégories)
        "score": score_result["score"],
        "grade": score_result["grade"],
        "percentage": score_result["percentage"],
        
        # Données détaillées pour l'affichage HTML (comme UFR)
        "technical_validation": technical_validation,
        "business_metrics": business_metrics,
        "consistency_checks": consistency_checks,
        "geographic_analysis": geographic_analysis,
        "repartition": repartition,
        "recommendations": recommendations,
        "field_info": {
            "field_name": location_field,
            "parent_field": parent_field,
            "id_field": id_field
        }
    }

def _calculate_hierarchy_metrics(df, location_field, parent_field, id_field):
    """Calcule les métriques métier de la hiérarchie"""
    total_records = len(df)
    
    # Compter par type
    stop_points = df[df[location_field] == 0] if location_field in df.columns else pd.DataFrame()
    stop_zones = df[df[location_field] == 1] if location_field in df.columns else pd.DataFrame()
    
    stop_points_count = len(stop_points)
    stop_zones_count = len(stop_zones)
    
    # Taux de renseignement du parent_station
    if parent_field in df.columns and not stop_points.empty:
        points_with_parent = stop_points[stop_points[parent_field].notna() & (stop_points[parent_field] != '')]
        parent_completion_rate = round((len(points_with_parent) / stop_points_count) * 100, 1) if stop_points_count > 0 else 0
    else:
        parent_completion_rate = 0
        points_with_parent = pd.DataFrame()
    
    # Zones utilisées
    if not points_with_parent.empty:
        used_zones = points_with_parent[parent_field].nunique()
    else:
        used_zones = 0
    
    # Ratio points/zones
    if stop_zones_count > 0:
        points_per_zone_avg = round(stop_points_count / stop_zones_count, 1)
    else:
        points_per_zone_avg = 0
    
    return {
        "total_records": total_records,
        "stop_points_count": stop_points_count,
        "stop_zones_count": stop_zones_count,
        "parent_completion_rate": parent_completion_rate,
        "points_with_parent_count": len(points_with_parent),
        "used_zones_count": used_zones,
        "unused_zones_count": stop_zones_count - used_zones,
        "points_per_zone_avg": points_per_zone_avg,
        "hierarchy_usage_rate": round((used_zones / stop_zones_count) * 100, 1) if stop_zones_count > 0 else 0
    }


def _validate_hierarchy_consistency(df, location_field, parent_field, id_field):
    """Valide la cohérence de la hiérarchie"""
    checks = []
    
    if location_field not in df.columns or parent_field not in df.columns:
        return checks
    
    # Règle 1: Les zones d'arrêt doivent avoir parent_station vide
    zones_with_parent = df[(df[location_field] == 1) & 
                          (df[parent_field].notna()) & 
                          (df[parent_field] != '')]
    
    rule1_check = {
        "check_name": "zones_empty_parent",
        "description": "Les zones d'arrêt doivent avoir parent_station vide",
        "status": "pass" if zones_with_parent.empty else "error",
        "message": f"{len(zones_with_parent)} zones d'arrêt avec parent_station renseigné" if not zones_with_parent.empty else "Toutes les zones ont parent_station vide",
        "details": {
            "invalid_zones": zones_with_parent[id_field].tolist() if not zones_with_parent.empty else []
        }
    }
    checks.append(rule1_check)
    
    # Règle 2: parent_station doit pointer vers une zone existante
    points_with_parent = df[(df[location_field] == 0) & 
                           (df[parent_field].notna()) & 
                           (df[parent_field] != '')]
    
    if not points_with_parent.empty:
        existing_zones = set(df[df[location_field] == 1][id_field].tolist())
        invalid_references = points_with_parent[~points_with_parent[parent_field].isin(existing_zones)]
        
        rule2_check = {
            "check_name": "valid_parent_references",
            "description": "parent_station doit pointer vers une zone d'arrêt existante",
            "status": "pass" if invalid_references.empty else "error",
            "message": f"{len(invalid_references)} références invalides vers des zones inexistantes" if not invalid_references.empty else "Toutes les références parent_station sont valides",
            "details": {
                "invalid_points": invalid_references[id_field].tolist() if not invalid_references.empty else [],
                "invalid_parent_refs": invalid_references[parent_field].tolist() if not invalid_references.empty else []
            }
        }
        checks.append(rule2_check)
        
        # Règle 3: parent_station ne doit pas pointer vers un point d'arrêt
        points_ids = set(df[df[location_field] == 0][id_field].tolist())
        points_to_points = points_with_parent[points_with_parent[parent_field].isin(points_ids)]
        
        rule3_check = {
            "check_name": "no_point_to_point",
            "description": "parent_station ne doit pas pointer vers un point d'arrêt",
            "status": "pass" if points_to_points.empty else "error",
            "message": f"{len(points_to_points)} points d'arrêt pointent vers d'autres points" if not points_to_points.empty else "Aucun point ne pointe vers un autre point",
            "details": {
                "invalid_points": points_to_points[id_field].tolist() if not points_to_points.empty else []
            }
        }
        checks.append(rule3_check)
    
    # Règle 4: Zones orphelines (non utilisées)
    if not points_with_parent.empty:
        used_zones = set(points_with_parent[parent_field].tolist())
        all_zones = set(df[df[location_field] == 1][id_field].tolist())
        orphaned_zones = all_zones - used_zones
    else:
        all_zones = set(df[df[location_field] == 1][id_field].tolist())
        orphaned_zones = all_zones
    
    rule4_check = {
        "check_name": "orphaned_zones",
        "description": "Zones d'arrêt non utilisées comme parent_station",
        "status": "warning" if orphaned_zones else "pass",
        "message": f"{len(orphaned_zones)} zones d'arrêt orphelines" if orphaned_zones else "Toutes les zones sont utilisées",
        "details": {
            "orphaned_zones": list(orphaned_zones)
        }
    }
    checks.append(rule4_check)
    
    return checks


def _analyze_geographic_distances(df, location_field, parent_field, id_field):
    """Analyse les distances géographiques entre points et zones"""
    
    # Vérifier la présence des coordonnées
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return {
            "check_name": "geographic_distances",
            "description": "Analyse des distances points/zones", 
            "status": "info",
            "message": "Coordonnées non disponibles pour l'analyse géographique",
            "details": {}
        }
    
    # Points d'arrêt avec parent_station renseigné
    points_with_parent = df[(df[location_field] == 0) & 
                           (df[parent_field].notna()) & 
                           (df[parent_field] != '') &
                           (df['stop_lat'].notna()) & 
                           (df['stop_lon'].notna())]
    
    if points_with_parent.empty:
        return {
            "check_name": "geographic_distances",
            "description": "Analyse des distances points/zones",
            "status": "info", 
            "message": "Aucun point d'arrêt avec parent_station et coordonnées",
            "details": {}
        }
    
    # Créer un dictionnaire des zones avec leurs coordonnées
    zones_coords = {}
    zones_data = df[(df[location_field] == 1) & 
                   (df['stop_lat'].notna()) & 
                   (df['stop_lon'].notna())]
    
    for _, zone in zones_data.iterrows():
        zones_coords[zone[id_field]] = (zone['stop_lat'], zone['stop_lon'])
    
    # Calculer les distances
    distances = []
    points_too_far = []
    points_without_zone_coords = []
    
    for _, point in points_with_parent.iterrows():
        parent_id = point[parent_field]
        point_id = point[id_field]
        
        if parent_id in zones_coords:
            point_coords = (point['stop_lat'], point['stop_lon'])
            zone_coords = zones_coords[parent_id]
            
            # Calcul distance haversine en mètres
            distance = _calculate_haversine_distance(point_coords, zone_coords)
            distances.append(distance)
            
            # Seuil de 500m
            if distance > 500:
                points_too_far.append({
                    "point_id": point_id,
                    "zone_id": parent_id,
                    "distance_m": round(distance, 1)
                })
        else:
            points_without_zone_coords.append(point_id)
    
    # Statistiques
    if distances:
        avg_distance = round(sum(distances) / len(distances), 1)
        max_distance = round(max(distances), 1)
        distances_over_500 = len([d for d in distances if d > 500])
        distances_over_1000 = len([d for d in distances if d > 1000])
    else:
        avg_distance = max_distance = distances_over_500 = distances_over_1000 = 0
    
    # Déterminer le statut
    if distances_over_1000 > 0:
        status = "error"
        message = f"{distances_over_1000} points à plus de 1km de leur zone"
    elif distances_over_500 > 0:
        status = "warning"
        message = f"{distances_over_500} points à plus de 500m de leur zone"
    else:
        status = "pass"
        message = "Toutes les distances sont acceptables"
    
    return {
        "check_name": "geographic_distances",
        "description": "Distances entre points d'arrêt et zones",
        "status": status,
        "message": message,
        "details": {
            "total_analyzed": len(distances),
            "avg_distance_m": avg_distance,
            "max_distance_m": max_distance,
            "points_over_500m": distances_over_500,
            "points_over_1000m": distances_over_1000,
            "points_too_far": points_too_far[:10],  # Limiter à 10 pour l'affichage
            "points_without_zone_coords": points_without_zone_coords
        }
    }

def _calculate_haversine_distance(coord1, coord2):
    """Calcule la distance haversine entre deux points en mètres"""
    import math
    
    lat1, lon1 = math.radians(coord1[0]), math.radians(coord1[1])
    lat2, lon2 = math.radians(coord2[0]), math.radians(coord2[1])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Rayon de la Terre en mètres
    earth_radius_m = 6371000
    
    return earth_radius_m * c

def _calculate_hierarchy_repartition(df, location_field, type_names):
    """Calcule la répartition des location_type"""
    if location_field not in df.columns:
        return {}
    
    total = len(df)
    repartition = {}
    
    type_counts = df[location_field].value_counts().to_dict()
    
    for location_type, count in type_counts.items():
        if pd.notna(location_type):
            repartition[str(int(location_type))] = {
                "count": int(count),
                "percentage": round((count / total) * 100, 1),
                "type_name": type_names.get(int(location_type), f"Type {int(location_type)}")
            }
    
    return repartition

def _generate_hierarchy_recommendations(business_metrics, consistency_checks, geographic_analysis):
    """Génère les recommandations pour la hiérarchie"""
    recommendations = []
    
    # Recommandations basées sur les métriques
    if business_metrics["stop_zones_count"] == 0:
        recommendations.append({
            "type": "critical",
            "message": "Aucune zone d'arrêt définie",
            "description": "Considérez l'ajout de zones d'arrêt (location_type=1) pour structurer votre réseau."
        })
    
    elif business_metrics["parent_completion_rate"] < 50:
        recommendations.append({
            "type": "warning",
            "message": f"Faible utilisation du système parent_station ({business_metrics['parent_completion_rate']}%)",
            "description": "Améliorer le rattachement des points d'arrêt aux zones pour une meilleure organisation."
        })
    
    if business_metrics["unused_zones_count"] > 0:
        recommendations.append({
            "type": "warning",
            "message": f"{business_metrics['unused_zones_count']} zones d'arrêt orphelines",
            "description": "Ces zones ne sont utilisées par aucun point d'arrêt. Vérifiez leur utilité."
        })
    
    # Recommandations basées sur la cohérence
    error_checks = [c for c in consistency_checks if c["status"] == "error"]
    if error_checks:
        recommendations.append({
            "type": "critical",
            "message": "Erreurs de cohérence détectées",
            "description": "Corrigez les références parent_station invalides pour assurer l'intégrité des données."
        })
    
    # Recommandations géographiques
    if geographic_analysis and geographic_analysis.get("details", {}).get("points_over_1000m", 0) > 0:
        recommendations.append({
            "type": "error",
            "message": "Points d'arrêt très éloignés de leur zone",
            "description": "Vérifiez la géolocalisation des zones d'arrêt ou la cohérence des rattachements."
        })
    
    elif geographic_analysis and geographic_analysis.get("details", {}).get("points_over_500m", 0) > 0:
        recommendations.append({
            "type": "warning",
            "message": "Points d'arrêt éloignés de leur zone",
            "description": "Distances importantes détectées. Vérifiez la précision géographique."
        })
    
    # Recommandations sur la répartition
    if business_metrics["points_per_zone_avg"] > 20:
        recommendations.append({
            "type": "info",
            "message": "Zones d'arrêt très chargées",
            "description": f"En moyenne {business_metrics['points_per_zone_avg']} points par zone. Considérez un découpage plus fin."
        })
    
    return recommendations