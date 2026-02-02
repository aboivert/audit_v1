"""
Fonctions d'audit pour le fichier agency.txt
"""
import pandas as pd
import re
from datetime import datetime
from services.gtfs_handler import GTFSHandler

from .generic_functions import clean_for_json
from .generic_functions import check_required_field
from .generic_functions import check_required_fields_summary
from .generic_functions import calculate_score_from_checks
from .generic_functions import check_format_field
from .generic_functions import calculate_summary
from .generic_functions import check_unused_id
from .generic_functions import check_orphan_id

import pytz
format = {'agency_timezone':{'genre':'required','description':"Validité des fuseaux horaires", 'type':'listing', 'valid_fields':set(pytz.all_timezones)},
          'agency_lang':{'genre':'optional','description':"Validité des langues", 'type':'listing','valid_fields':{'en', 'fr', 'es', 'de', 'it', 'pt', 'nl', 'sv', 'da', 'no', 'fi', 'ru', 'zh', 'ja', 'ko', 'ar','EN', 'FR', 'ES', 'DE', 'IT', 'PT', 'NL', 'SV', 'DA', 'NO', 'FI', 'RU', 'ZH', 'JA', 'KO', 'AR'}},
          'agency_url':{'genre':'required','description':"Validité des URL",'type':'url'},
          'agency_fare_url':{'genre':'optional','description':"Validité des URL de tarif",'type':'url'},
          'agency_phone':{'genre':'optional','description':"Validité des numéros de téléphone",'type':'regex','pattern':re.compile(r'^[\+]?[\s\-\(\)0-9]{8,}$')},
          'agency_email':{'genre':'optional','description':"Validity des mails",'type':'regex','pattern':re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')}
}


def audit_agency_file(project_id, progress_callback=None):
    """
    Audit complet du fichier agency.txt
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Résultats de l'audit selon la structure définie
    """

    if progress_callback:
        progress_callback(15, "Chargement du fichier agency.txt...", "loading")

    agency_df = GTFSHandler.get_gtfs_data(project_id, 'agency.txt')
    
    if agency_df is None or agency_df.empty:
        if progress_callback:
            progress_callback(0, "Fichier agency.txt introuvable", "error")
        return {
            "file": "agency.txt",
            "status": "missing",
            "message": "Fichier agency.txt non trouvé ou vide",
            "severity": "critical",
            "timestamp": datetime.now().isoformat()
        }
    
    if progress_callback:
        progress_callback(25, "Vérification des champs obligatoires...", "required_fields")
    
    results = {
        "file": "agency.txt",
        "status": "processed",
        "total_rows": len(agency_df),
        "timestamp": datetime.now().isoformat(),
        "required_fields": _check_requirements(agency_df),
    }
    
    if progress_callback:
        progress_callback(45, "Vérification du format des données...", "data_format")
    
    results["data_format"] = _check_data_format(agency_df)
    
    if progress_callback:
        progress_callback(65, "Vérification de la cohérence des données...", "data_consistency")
    
    results["data_consistency"] = _check_data_consistency(agency_df, project_id)
    
    if progress_callback:
        progress_callback(80, "Calcul du résumé...", "summary")
    
    # Calculer le résumé global
    results["summary"] = calculate_summary(results, ['required_fields', 'data_format', 'data_consistency'])
    
    # Nettoyer les types de données pour la sérialisation JSON
    results = clean_for_json(results)
    
    return results

def _check_requirements(df):
    """Vérifications des champs obligatoires"""
    checks = []
    
    # 1. Vérifier agency_id (obligatoire sauf si une seule agence)
    agency_id_check = {
        "check_name": "agency_id_present",
        "description": "Présence du champ agency_id",
        "status": "pass",
        "message": "",
        "details": {}
    }
    
    if len(df) > 1:  # Si plus d'une agence, agency_id obligatoire
        if 'agency_id' not in df.columns:
            agency_id_check.update({
                "status": "error",
                "message": "Champ agency_id manquant (obligatoire avec plusieurs agences)",
                "details": {"missing_column": True}
            })
        else:
            missing_ids = df['agency_id'].isna().sum()
            if missing_ids > 0:
                missing_rows = df[df['agency_id'].isna()].index.tolist()
                if 'agency_name' in df.columns:
                    agency_name = df.iloc[missing_rows]["agency_name"].tolist()
                else:
                    agency_name = 'not able to find agency_name'
                agency_id_check.update({
                    "status": "error",
                    "message": f"{missing_ids} valeurs manquantes pour agency_id",
                    "details": {"index_row_missing_id": missing_rows, "agency_name":agency_name}
                })
            else:
                agency_id_check["message"] = "Tous les agency_id sont présents"
    else:
        agency_id_check["message"] = "agency_id optionnel (une seule agence)"
    
    checks.append(agency_id_check)
    
    # 2. Vérifier unicité des agency_id
    if 'agency_id' in df.columns and not df['agency_id'].isna().all():
        uniqueness_check = {
            "check_name": "agency_id_unique",
            "description": "Unicité des agency_id",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        duplicates = df[df.duplicated('agency_id', keep=False) & df['agency_id'].notna()]
        if not duplicates.empty:
            duplicate_ids = duplicates['agency_id'].unique().tolist()
            duplicate_rows = duplicates.index.tolist()
            
            # Créer un détail par ID dupliqué
            duplicate_details = []
            for dup_id in duplicate_ids:
                dup_rows = duplicates[duplicates['agency_id'] == dup_id]
                detail = {
                    "agency_id": str(dup_id),
                    "occurrences": len(dup_rows),
                }
                if 'agency_name' in df.columns:
                    detail["agency_names"] = dup_rows['agency_name'].fillna('N/A').astype(str).tolist()
                duplicate_details.append(detail)
            
            uniqueness_check.update({
                "status": "error",
                "message": f"{len(duplicate_ids)} agency_id dupliqués",
                "details": {
                    "duplicate_ids": [str(x) for x in duplicate_ids], 
                    "duplicate_details": duplicate_details
                }
            })
        else:
            uniqueness_check["message"] = "Tous les agency_id sont uniques"
            
        checks.append(uniqueness_check)

    required_fields_check = check_required_fields_summary(df, ['agency_name', 'agency_url', 'agency_timezone'], 'agency_id')
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
    
    # 1. Vérifier agency_lang
    lang_check = check_format_field(df, 'agency_lang', format['agency_lang'], 'agency_id')
    checks.append(lang_check)
    
    # 2. Vérifier agency_phone
    phone_check = check_format_field(df, 'agency_phone', format['agency_phone'], 'agency_id')
    checks.append(phone_check)
    
    # 3. Vérifier agency_fare_url
    fare_url_check = check_format_field(df, 'agency_fare_url', format['agency_fare_url'], 'agency_id')
    checks.append(fare_url_check)
    
    # 4. Vérifier agency_email
    email_check = check_format_field(df, 'agency_email', format['agency_email'], 'agency_id')
    checks.append(email_check)

    # 4. Vérifier agency_email
    timezone_check = check_format_field(df, 'agency_timezone', format['agency_timezone'], 'agency_id')
    checks.append(timezone_check)

    # 4. Vérifier agency_email
    url_check = check_format_field(df, 'agency_url', format['agency_url'], 'agency_id')
    checks.append(url_check)
    
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
    return {
        "status": overall_status,
        "checks": checks
    }

    
def _check_data_consistency(df, project_id):
    """Vérifications de cohérence des données"""
    checks = []
    
    # Vérifier les agences orphelines (référencées dans routes mais absentes)
    orphan_check = check_orphan_id(df, 'agency_id','routes.txt',project_id)
    checks.append(orphan_check)

    # Vérifier les agences non utilisées (dans agency mais pas dans routes)
    unused_check = check_unused_id(df, 'agency_id', 'routes.txt', project_id)
    checks.append(unused_check)
    
    # Vérifier l'encodage UTF-8
    #encoding_check = _check_encoding(df)
    #checks.append(encoding_check)
    
    # Déterminer le statut global
    statuses = [check["status"] for check in checks]
    if "error" in statuses:
        overall_status = "error"
    elif "warning" in statuses:
        overall_status = "warning"
    else:
        overall_status = "pass"
    
    return {
        "status": overall_status,
        "checks": checks
    }



def get_agency_audit_info():
    """
    Retourne les informations sur les audits disponibles pour agency.txt
    
    Returns:
        dict: Informations sur les checks disponibles
    """
    return {
        "file": "agency.txt",
        "description": "Audit du fichier des agences de transport",
        "categories": {
            "required_fields": {
                "name": "Champs obligatoires",
                "description": "Vérification des champs requis par la spécification GTFS",
                "checks": [
                    "agency_id_present",
                    "agency_id_unique", 
                    "agency_name_present",
                    "agency_url_present",
                    "agency_timezone_valid"
                ]
            },
            "optional_fields": {
                "name": "Champs optionnels",
                "description": "Vérification du format des champs optionnels",
                "checks": [
                    "agency_lang_format",
                    "agency_phone_format",
                    "agency_fare_url_format",
                    "agency_email_format"
                ]
            },
            "data_consistency": {
                "name": "Cohérence des données",
                "description": "Vérifications de cohérence inter-fichiers",
                "checks": [
                    "no_orphan_agencies",
                    "no_unused_agencies",
                    "utf8_encoding"
                ]
            }
        },
        "statistics": {
            "total_agencies": "Nombre total d'agences",
            "fields_completion": "Taux de complétude des champs optionnels",
            "richness_analysis": "Analyse de richesse informationnelle",  
            "connectivity_analysis": "Métriques de connectivité réseau",   
            "global_completion": "Score de complétude global" 
        }
    }