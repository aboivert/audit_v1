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
format = {'monday':{'genre':'required','description':"Validité du champ monday",'type':'listing', 'valid_fields':['0', '1',]},
          'tuesday':{'genre':'required','description':"Validité du champ tuesday",'type':'listing', 'valid_fields':['0', '1',]},
          'wednesday':{'genre':'required','description':"Validité du champ wednesday",'type':'listing', 'valid_fields':['0', '1',]},
          'thursday':{'genre':'required','description':"Validité du champ thursday",'type':'listing', 'valid_fields':['0', '1',]},
          'friday':{'genre':'required','description':"Validité du champ friday",'type':'listing', 'valid_fields':['0', '1',]},
          'saturday':{'genre':'required','description':"Validité du champ saturday",'type':'listing', 'valid_fields':['0', '1',]},
          'sunday':{'genre':'required','description':"Validité du champ sunday",'type':'listing', 'valid_fields':['0', '1',]},
          'start_date':{'genre':'required','description':"Validité des dates de début de calendrier",'type':'date'},
          'end_date':{'genre':'required','description':"Validité des dates de fin de calendrier",'type':'date'},
}

def audit_calendar_file(project_id, progress_callback = None):
    """
    Audit complet du fichier stops.txt
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Résultats de l'audit selon la structure définie
    """

    if progress_callback:
        progress_callback(15, "Chargement du fichier calendar.txt...", "loading")

    calendar_df = GTFSHandler.get_gtfs_data(project_id, 'calendar.txt')
    
    if calendar_df is None or calendar_df.empty:
        if progress_callback:
            progress_callback(0, "Fichier calendar.txt introuvable", "error")
        return {
            "file": "calendar.txt",
            "status": "missing",
            "message": "Fichier calendar.txt non trouvé ou vide",
            "severity": "critical",
            "timestamp": datetime.now().isoformat()
        }
    
    if progress_callback:
        progress_callback(25, "Vérification des champs obligatoires...", "required_fields")
    
    results = {
        "file": "calendar.txt",
        "status": "processed",
        "total_rows": len(calendar_df),
        "timestamp": datetime.now().isoformat(),
        "required_fields": _check_required_fields(calendar_df, project_id)
    }

    if progress_callback:
        progress_callback(45, "Vérification du format des donnés...", "data_format")

    results["data_format"] = _check_data_format(calendar_df)

    if progress_callback:
        progress_callback(65, "Vérification de la cohérence des données...", "data_consistency")

    results["data_consistency"] = _check_data_consistency(calendar_df, project_id)


    if progress_callback:
        progress_callback(85, "Calcul du résumé...", "summary")

    # Calculer le résumé global
    results["summary"] = calculate_summary(results, ['required_fields','data_format', 'data_consistency'])
    return results
    
def _check_required_fields(df, project_id):
    """Vérifications des champs obligatoires"""
    checks = []
    
    # 1. Vérifier trip_id
    service_id_check = check_required_field(df, 'service_id', 'calendar_df')
    checks.append(service_id_check)
    
    # 2. Vérifier unicité des route_id
    if 'service_id' in df.columns and not df['service_id'].isna().all():
        uniqueness_check = {
            "check_name": "service_id_unique",
            "description": "Unicité des service_id",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        duplicates = df[df.duplicated('service_id', keep=False) & df['service_id'].notna()]
        if not duplicates.empty:
            duplicate_ids = duplicates['service_id'].unique().tolist()
            duplicate_rows = duplicates.index.tolist()
            
            # Ajouter les noms des routes pour plus de clarté
            duplicate_details = []
            for dup_id in duplicate_ids:
                dup_rows = duplicates[duplicates['service_id'] == dup_id]
                detail = {
                    "service_id": str(dup_id),
                    "occurrences": len(dup_rows),
                    "rows": dup_rows.index.tolist(),
                }
            
            uniqueness_check.update({
                "status": "error",
                "message": f"{len(duplicate_ids)} service_id dupliqués",
                "details": {
                    "duplicate_ids": [str(x) for x in duplicate_ids], 
                    "duplicate_rows": duplicate_rows,
                    "duplicate_details": duplicate_details
                }
            })
        else:
            uniqueness_check["message"] = "Tous les service_id sont uniques"
            
        checks.append(uniqueness_check)
    
    required_fields_check = check_required_fields_summary(df, ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'], 'service_id')
    checks.append(required_fields_check)

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

    # Vérifier les routes non utilisées (dans routes mais pas dans trips)
    unused_check = check_unused_id(df, 'service_id', 'trips.txt', project_id)
    checks.append(unused_check)
    
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

def _check_data_format(df):
    """Vérifications des champs optionnels"""
    checks = []
    
    # 1. Vérifier route_type
    monday_check = check_format_field(df, 'monday', format['monday'], 'service_id')
    checks.append(monday_check) 

    tuesday_check = check_format_field(df, 'tuesday', format['tuesday'], 'service_id')
    checks.append(tuesday_check)  

    wednesday_check = check_format_field(df, 'wednesday', format['wednesday'], 'service_id')
    checks.append(wednesday_check)  

    thursday_check = check_format_field(df, 'thursday', format['thursday'], 'service_id')
    checks.append(thursday_check)  

    friday_check = check_format_field(df, 'friday', format['friday'], 'service_id')
    checks.append(friday_check)  

    saturday_check = check_format_field(df, 'saturday', format['saturday'], 'service_id')
    checks.append(saturday_check)  

    sunday_check = check_format_field(df, 'sunday', format['sunday'], 'service_id')
    checks.append(sunday_check)    

    start_date_check = check_format_field(df, 'start_date', format['start_date'], 'service_id')
    checks.append(start_date_check)

    end_date_check = check_format_field(df, 'end_date', format['end_date'], 'service_id')
    checks.append(end_date_check)

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