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

format = {'exception_type':{'genre':'required','description':"Validité du champ exception_type",'type':'listing', 'valid_fields':['1', '2',]},
          'date':{'genre':'required','description':"Validité des dates",'type':'date'},
}

def audit_calendar_dates_file(project_id, progress_callback = None):
    """
    Audit complet du fichier stops.txt
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Résultats de l'audit selon la structure définie
    """

    if progress_callback:
        progress_callback(15, "Chargement du fichier calendar_dates.txt...", "loading")

    calendar_dates_df = GTFSHandler.get_gtfs_data(project_id, 'calendar_dates.txt')
    
    if calendar_dates_df is None or calendar_dates_df.empty:
        if progress_callback:
            progress_callback(0, "Fichier calendar_dates.txt introuvable", "error")
        return {
            "file": "calendar_dates.txt",
            "status": "missing",
            "message": "Fichier calendar_dates.txt non trouvé ou vide",
            "severity": "critical",
            "timestamp": datetime.now().isoformat()
        }
    
    if progress_callback:
        progress_callback(25, "Vérification des champs obligatoires...", "required_fields")
    
    results = {
        "file": "calendar_dates.txt",
        "status": "processed",
        "total_rows": len(calendar_dates_df),
        "timestamp": datetime.now().isoformat(),
        "required_fields": _check_required_fields(calendar_dates_df, project_id)
    }

    if progress_callback:
        progress_callback(45, "Vérification du format des donnés...", "data_format")

    results["data_format"] = _check_data_format(calendar_dates_df)

    if progress_callback:
        progress_callback(65, "Génération des statistiques...", "statistics")

    results["statistics"] = _generate_statistics(calendar_dates_df, project_id)


    if progress_callback:
        progress_callback(85, "Calcul du résumé...", "summary")

    # Calculer le résumé global
    results["summary"] = calculate_summary(results, ['required_fields','data_format', 'data_consistency'])
    return results
    
def _check_required_fields(df, project_id):
    """Vérifications des champs obligatoires"""
    checks = []
        
    # 1. Vérifier service_id requis
    service_date_fields_check = check_required_fields_summary(df, ['service_id', 'date'], 'service_id')
    checks.append(service_date_fields_check)

    # 2. Vérifier unicité du couple (service_id, date)
    if 'service_id' in df.columns and 'date' in df.columns and not df['service_id'].isna().all() and not df['date'].isna().all():
        uniqueness_check = {
            "check_name": "service_id_date_unique",
            "description": "Unicité des couples (service_id, date)",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        # Vérifier les doublons sur le couple (service_id, date)
        duplicates = df[df.duplicated(['service_id', 'date'], keep=False) & 
                        df['service_id'].notna() & df['date'].notna()]
        if not duplicates.empty:
            # Grouper les doublons par couple (service_id, date)
            duplicate_details = []
            duplicate_couples = duplicates[['service_id', 'date']].drop_duplicates()
            
            for _, row in duplicate_couples.iterrows():
                service_id = row['service_id']
                date = row['date']
                dup_rows = duplicates[(duplicates['service_id'] == service_id) & 
                                    (duplicates['date'] == date)]
                
                duplicate_details.append({
                    "service_id": str(service_id),
                    "date": str(date),
                    "occurrences": len(dup_rows),
                    "rows": dup_rows.index.tolist()
                })
            
            uniqueness_check.update({
                "status": "error",
                "message": f"{len(duplicate_couples)} couples (service_id, date) dupliqués",
                "details": {
                    "duplicate_couples": len(duplicate_couples),
                    "duplicate_rows": duplicates.index.tolist(),
                    "duplicate_details": duplicate_details
                }
            })
        else:
            uniqueness_check["message"] = "Tous les couples (service_id, date) sont uniques"
        
        checks.append(uniqueness_check)
    # 3. Vérifier cohérence avec calendar.txt (si il existe)
    calendar_df = GTFSHandler.get_gtfs_data(project_id, 'calendar.txt')
    if calendar_df is not None and not calendar_df.empty and 'service_id' in calendar_df.columns and 'service_id' in df.columns:
        calendar_consistency_check = {
            "check_name": "service_id_exists_in_calendar",
            "description": "Les service_id de calendar_dates existent dans calendar.txt",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        # Récupérer les service_id valides de calendar
        valid_service_ids = set(calendar_df['service_id'].dropna().unique())
        calendar_dates_service_ids = set(df['service_id'].dropna().unique())
        
        # Trouver les service_id dans calendar_dates mais pas dans calendar
        invalid_service_ids = calendar_dates_service_ids - valid_service_ids
        
        if invalid_service_ids:
            # Trouver toutes les lignes concernées
            invalid_entries = []
            for service_id in invalid_service_ids:
                entries_with_invalid_service = df[df['service_id'] == service_id]
                for idx, row in entries_with_invalid_service.iterrows():
                    date_value = df.loc[idx, 'date'] if 'date' in df.columns else 'N/A'
                    invalid_entries.append({
                        "service_id": str(service_id),
                        "date": str(date_value),
                        "row": idx
                    })
            
            # Grouper par service_id pour faciliter la lecture
            invalid_service_group = {}
            for item in invalid_entries:
                service_id = item["service_id"]
                if service_id not in invalid_service_group:
                    invalid_service_group[service_id] = []
                invalid_service_group[service_id].append({
                    "date": item["date"],
                    "row": item["row"]
                })
            
            calendar_consistency_check.update({
                "status": "error",
                "message": f"{len(invalid_service_ids)} service_id de calendar_dates absents de calendar.txt",
                "details": {
                    "invalid_service_ids": list(invalid_service_ids),
                    "invalid_entries": invalid_service_group,
                    "total_invalid_entries": len(invalid_entries)
                }
            })
        else:
            calendar_consistency_check["message"] = "Tous les service_id de calendar_dates existent dans calendar.txt"
        
        checks.append(calendar_consistency_check)
    else:
        # Si calendar.txt n'existe pas, c'est informatif
        calendar_consistency_check = {
            "check_name": "service_id_exists_in_calendar",
            "description": "Les service_id de calendar_dates existent dans calendar.txt",
            "status": "info",
            "message": "calendar.txt absent - vérification de cohérence impossible",
            "details": {"reason": "missing_calendar_file"}
        }
        checks.append(calendar_consistency_check)
    
    date_check = check_required_field(df, 'date', 'service_id')
    checks.append(date_check)

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
    exception_type_check = check_format_field(df, 'exception_type', format['exception_type'], 'service_id')
    checks.append(exception_type_check) 

    date_check = check_format_field(df, 'date', format['date'], 'service_id')
    checks.append(date_check)  

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
    repartition = _calculate_service__repartition(df, project_id)

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

def _calculate_service__repartition(df, project_id):
    """Calcule les métriques de base"""
    
    # 1. Nombre total de routes
    total_services = len(df)
    # 2. Répartition par type de transport
    stats_exception_type = {}
    if 'exception_type' in df.columns:
        type_counts = df['exception_type'].value_counts().to_dict()

        
        for exception_type, count in type_counts.items():
            if pd.notna(exception_type):
                stats_exception_type[str(int(exception_type))] = {
                    "count": int(count),
                    "percentage": round((count / total_services) * 100, 1),
                    "type_name": exception_type_names.get(int(exception_type), f"Type {int(exception_type)}")
                }
    
    return {
        "stats_exception_type": stats_exception_type,
    }


exception_type_names = {
    1: "Ajout de service",
    2: "Suppression de service"
}