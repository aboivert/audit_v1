import pandas as pd
from urllib.parse import urlparse
from services.gtfs_handler import GTFSHandler
from datetime import datetime
import re



def clean_for_json(data):
    """
    Nettoie récursivement les données pour la sérialisation JSON
    Convertit les types numpy/pandas en types Python natifs
    """
    import numpy as np
    
    if isinstance(data, dict):
        return {key: clean_for_json(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_for_json(item) for item in data]
    elif isinstance(data, (np.integer, pd.Int64Dtype)):
        return int(data)
    elif isinstance(data, (np.floating, pd.Float64Dtype)):
        return float(data)
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif pd.isna(data):
        return None
    else:
        return data
    

def check_required_field(df, field_name, id_gathering):
    """Vérifie un champ obligatoire standard"""
    check = {
        "check_name": f"{field_name}_present",
        "description": f"Présence du champ {field_name}",
        "status": "pass",
        "message": "",
        "details": {}
    }
    
    if field_name not in df.columns:
        check.update({
            "status": "error",
            "message": f"Champ {field_name} manquant",
            "details": {"missing_column": True}
        })
    else:
        missing_count = df[field_name].isna().sum()
        empty_count = (df[field_name] == '').sum() if df[field_name].dtype == 'object' else 0
        total_issues = missing_count + empty_count
        
        if total_issues > 0:
            affected_rows = df[(df[field_name].isna()) | (df[field_name] == '')].index.tolist()
            affected_ids = []
            if id_gathering in df.columns:
                affected_ids = df.loc[affected_rows, id_gathering].fillna('N/A').astype(str).tolist()
            else:
                affected_ids = ['Not able to determine ID of missing fields']
            
            check.update({
                "status": "error",
                "message": f"{total_issues} valeurs manquantes/vides pour {field_name}",
                "details": {
                    "affected_ids": affected_ids,
                }
            })
        else:
            check["message"] = f"Toutes les valeurs {field_name} sont présentes"
    
    return check


def check_required_fields_summary(df, fields, id_field, summary_name="required_fields"):
    """
    Vérifie la présence de plusieurs champs obligatoires et retourne un seul check global
    
    Args:
        df (pd.DataFrame): Le DataFrame à vérifier
        fields (list): Liste des noms de champs à vérifier
        id_field (str): Nom du champ utilisé pour identifier les lignes
        summary_name (str): Nom du check global
    
    Returns:
        dict: Un seul check avec le résumé global
    """
    
    check = {
        "check_name": summary_name,
        "description": "Présence des champs obligatoires",
        "status": "pass",
        "message": "",
        "details": {}
    }
    
    missing_fields = []
    field_details = []
    total_issues = 0
    
    for field_name in fields:
        field_info = {
            "field_name": field_name,
            "status": "pass"
        }
        
        if field_name not in df.columns:
            field_info.update({
                "status": "error",
                "issue": "missing_column",
                "message": f"Colonne {field_name} manquante"
            })
            missing_fields.append(field_name)
            total_issues += 1
        else:
            missing_count = df[field_name].isna().sum()
            empty_count = (df[field_name] == '').sum() if df[field_name].dtype == 'object' else 0
            field_total_issues = missing_count + empty_count
            
            if field_total_issues > 0:
                affected_rows = df[(df[field_name].isna()) | (df[field_name] == '')].index.tolist()
                affected_ids = []
                if id_field in df.columns:
                    affected_ids = df.loc[affected_rows, id_field].fillna('N/A').astype(str).tolist()
                else:
                    affected_ids = ['ID non disponible']
                
                field_info.update({
                    "status": "error",
                    #"issue": "missing_values",
                    "message": f"{field_total_issues} valeurs manquantes/vides",
                    #"missing_count": int(missing_count),
                    #"empty_count": int(empty_count),
                    #"affected_rows": affected_rows,
                    "affected_ids": affected_ids
                })
                missing_fields.append(field_name)
                total_issues += field_total_issues
        
        # On n'ajoute aux détails que les champs qui ont des problèmes
        if field_info["status"] != "pass":
            field_details.append(field_info)
    
    # Construire le message et les détails du check global
    if total_issues > 0:
        check.update({
            "status": "error",
            "message": f"{len(missing_fields)} champ(s) obligatoire(s) avec des problèmes : {', '.join(missing_fields)}",
            "details": {
                #"total_issues": total_issues,
                #"fields_with_issues": len(missing_fields),
                "problematic_fields": missing_fields,
                "field_details": field_details  # Seulement les champs avec problèmes
            }
        })
    else:
        check["message"] = f"Tous les champs obligatoires sont présents ({', '.join(fields)})"
    
    return check

def check_format_field(df, field, format, id_gathering):
    """Vérifie la validité du formattage du champ field"""
    check = {
        "check_name": f"{field}_valid",
        "description": format['description'], 
        "status": "pass",
        "message": "Tous les formats sont valides",
        "details": {},
        "type": format['genre']
    }
    invalid_data = []
    empty_data = []
    
    if field not in df.columns:
        check.update({
            "status": "error",
            "message": f"Champ {field} manquant",
            "details": {"missing_column": True}
        })
    else:
        if format['type'] == 'listing':
            for idx, data in df[field].items():
                problematic_id = df.loc[idx, id_gathering] if id_gathering in df.columns else 'N/A'
                if is_truly_empty(data):
                    empty_data.append(f"{problematic_id}")
                elif str(data) not in format['valid_fields']:
                    invalid_data.append(f"{problematic_id}:{data}")
        elif format['type'] == 'url':
            for idx, data in df[field].items():
                problematic_id = df.loc[idx, id_gathering] if id_gathering in df.columns else 'N/A'
                try:
                    if is_truly_empty(data):
                        empty_data.append(f"{problematic_id}")
                    else:
                        parsed = urlparse(str(data))
                        if not all([parsed.scheme, parsed.netloc]):
                            invalid_data.append(f"{problematic_id}:{data}")
                except:
                        invalid_data.append(f"{problematic_id}:{data}")
        elif format['type'] == 'regex':
            for idx, data in df[field].items():
                problematic_id = df.loc[idx, id_gathering] if id_gathering in df.columns else 'N/A'
                if is_truly_empty(data):
                    empty_data.append(f"{problematic_id}")
                elif not format['pattern'].match(str(data)):
                    invalid_data.append(f"{problematic_id}:{data}")
        elif format['type'] == 'coordinates':
            coord_type = format.get('coord_type', 'latitude')
            for idx, data in df[field].items():
                problematic_id = df.loc[idx, id_gathering] if id_gathering in df.columns else 'N/A'
                if is_truly_empty(data):
                    empty_data.append(f"{problematic_id}")
                else:
                    try:
                        coord_value = float(data)
                        if coord_type == 'latitude':
                            # Latitude doit être entre -90 et 90
                            if not (-90 <= coord_value <= 90):
                                invalid_data.append(f"{problematic_id}:{data}")
                        elif coord_type == 'longitude':
                            # Longitude doit être entre -180 et 180
                            if not (-180 <= coord_value <= 180):
                                invalid_data.append(f"{problematic_id}:{data}")
                        else:
                            # Type de coordonnée non reconnu
                            invalid_data.append(f"{problematic_id}:{data}")
                    except (ValueError, TypeError):
                        # Impossible de convertir en float
                        invalid_data.append(f"{problematic_id}:{data}")
        elif format['type'] == 'date':
            date_format = format.get('date_format', '%Y%m%d')  # Format GTFS par défaut YYYYMMDD
            for idx, data in df[field].items():
                problematic_id = df.loc[idx, id_gathering] if id_gathering in df.columns else 'N/A'
                if is_truly_empty(data):
                    empty_data.append(f"{problematic_id}")
                else:
                    try:
                        # Convertir en string pour le parsing
                        date_str = str(data).strip()
                        # Essayer de parser la date selon le format spécifié
                        datetime.strptime(date_str, date_format)
                    except ValueError:
                        # Format de date invalide
                        invalid_data.append(f"{problematic_id}:{data}")
                    except Exception:
                        # Autre erreur lors du parsing
                        invalid_data.append(f"{problematic_id}:{data}")
        elif format['type'] == 'time':
            for idx, data in df[field].items():
                problematic_id = df.loc[idx, id_gathering] if id_gathering in df.columns else 'N/A'
                if is_truly_empty(data):
                    empty_data.append(f"{problematic_id}")
                else:
                    try:
                        # Convertir en string pour le parsing
                        time_str = str(data).strip()
                        
                        # Vérifier le format GTFS pour les horaires : HH:MM:SS ou H:MM:SS
                        # GTFS permet aussi des heures > 23 (ex: 25:30:00 pour 1h30 le jour suivant)
                        if not re.match(r'^\d{1,2}:\d{2}:\d{2}$', time_str):
                            invalid_data.append(f"{problematic_id}:{data}")
                            continue
                            
                        # Séparer les composants
                        parts = time_str.split(':')
                        hours = int(parts[0])
                        minutes = int(parts[1])
                        seconds = int(parts[2])
                        
                        # Valider les minutes et secondes (0-59)
                        if not (0 <= minutes <= 59) or not (0 <= seconds <= 59):
                            invalid_data.append(f"{problematic_id}:{data}")
                        # Heures peuvent dépasser 23 en GTFS (pas de validation upper bound)
                        elif hours < 0:
                            invalid_data.append(f"{problematic_id}:{data}")
                            
                    except (ValueError, IndexError):
                        # Erreur de conversion ou format invalide
                        invalid_data.append(f"{problematic_id}:{data}")
                    except Exception:
                        # Autre erreur lors du parsing
                        invalid_data.append(f"{problematic_id}:{data}")
    
    if invalid_data or empty_data:
        check.update({
            "status": "warning",
            "message": f"{len(invalid_data)} lignes au format invalide et {len(empty_data)} lignes vides",
            "details": {"invalid_data": invalid_data, "empty_data": empty_data},
            "statistics": {'number': len(df), 'invalid': len(invalid_data), 'empty': len(empty_data)}
        })
    return check

def check_unused_id(df, objet_source, target_file, project_id, target_field=None):
    """
    Vérifie les objets 'source' présents dans df mais non utilisés dans target_file
    
    Args:
        df: DataFrame source
        objet_source: Nom du champ dans df à vérifier
        target_file: Fichier cible où chercher l'utilisation
        project_id: ID du projet
        target_field: Nom du champ dans target_file (par défaut = objet_source)
    """
    field_to_check = target_field or objet_source
    
    check = {
        "check_name": f"no_unused_{objet_source}_in_{target_file.replace('.txt', '')}",
        "description": f"{objet_source} définis mais non utilisés dans {target_file}{'.' + target_field if target_field else ''}",
        "status": "pass",
        "message": f"Tous les {objet_source} sont utilisés",
        "details": {}
    }
    
    try:
        target_df = GTFSHandler.get_gtfs_data(project_id, target_file)
        
        if target_df is not None and field_to_check in target_df.columns and objet_source in df.columns:
            df_objet_source = set(df[objet_source].dropna().unique())
            target_df_objet_source = set(target_df[field_to_check].dropna().unique())
            unused_objects = df_objet_source - target_df_objet_source
            
            if unused_objects:
                check.update({
                    "status": "warning",
                    "message": f"{len(unused_objects)} {objet_source} définis mais non utilisés dans {target_file}.{field_to_check}",
                    "details": {"unused_objects": list(unused_objects)}
                })
        else:
            check.update({
                "status": "info",
                "message": f"Impossible de vérifier l'utilisation ({target_file} manquant ou champ {field_to_check} absent)",
                "details": {"reason": "missing_file_or_field"}
            })
    except Exception as e:
        check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les {objet_source} non utilisés: {str(e)}",
            "details": {"error": str(e)}
        })
    
    return check


def check_orphan_id(df, objet_source, target_file, project_id, target_field=None):
    """
    Vérifie les objets 'source' orphelins (référencés dans target_file mais absents de df)
    
    Args:
        df: DataFrame source
        objet_source: Nom du champ dans df à vérifier
        target_file: Fichier cible où chercher les références
        project_id: ID du projet
        target_field: Nom du champ dans target_file (par défaut = objet_source)
    """
    field_to_check = target_field or objet_source
    
    check = {
        "check_name": f"no_orphan_{objet_source}_in_{target_file.replace('.txt', '')}",
        "description": f"{objet_source} référencés dans {target_file}{'.' + target_field if target_field else ''}",
        "status": "pass",
        "message": f"Tous les {objet_source} sont référencés correctement",
        "details": {}
    }
    
    try:
        target_df = GTFSHandler.get_gtfs_data(project_id, target_file)
        
        if target_df is not None and field_to_check in target_df.columns and objet_source in df.columns:
            target_df_objet_source = set(target_df[field_to_check].dropna().unique())
            df_objet_source = set(df[objet_source].dropna().unique())
            
            orphan_objects = target_df_objet_source - df_objet_source
            
            if orphan_objects:
                check.update({
                    "status": "error",
                    "message": f"{len(orphan_objects)} {objet_source} référencés dans {target_file}.{field_to_check} mais absents",
                    "details": {"orphan_objects": list(orphan_objects)}
                })
        else:
            check.update({
                "status": "info",
                "message": f"Impossible de vérifier les références croisées ({target_file} manquant ou champ {field_to_check} absent)",
                "details": {"reason": "missing_file_or_field"}
            })
    except Exception as e:
        check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les références croisées: {str(e)}",
            "details": {"error": str(e)}
        })
    
    return check

def calculate_summary(results, categories):
    """Calcule le résumé global de l'audit"""
    all_checks = []
    # Collecter tous les checks
    for category in categories:
        if category in results:
            all_checks.extend(results[category]['checks'])

    # Compter par statut
    total_checks = len(all_checks)
    passed_checks = sum(1 for check in all_checks if check['status'] == 'pass')
    warning_checks = sum(1 for check in all_checks if check['status'] == 'warning')
    error_checks = sum(1 for check in all_checks if check['status'] == 'error')
    critical_checks = sum(1 for check in all_checks if check['status'] == 'critical')
    
    # Déterminer le statut global
    if critical_checks > 0 or results['status'] == 'missing':
        overall_status = 'critical'
    elif error_checks > 0:
        overall_status = 'error'
    elif warning_checks > 0:
        overall_status = 'warning'
    else:
        overall_status = 'pass'
    
    return {
        "overall_status": overall_status,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "warning_checks": warning_checks,
        "error_checks": error_checks,
        "critical_checks": critical_checks
    }

def calculate_score_from_checks(checks, overall_status, scoring_config=None):
    """
    Calcule un score basé sur une liste de checks et le statut global
    
    Args:
        checks (list): Liste des checks individuels
        overall_status (str): Statut global ("pass", "warning", "error")
        scoring_config (dict): Configuration optionnelle du scoring
    
    Returns:
        dict: Score détaillé avec breakdown
    """
    
    # Configuration par défaut
    default_config = {
        "max_score": 100,
        "penalties": {
            "error": 20,      # -20 points par erreur
            "warning": 5,     # -5 points par warning
            "pass": 0         # 0 point de pénalité
        },
        "weights": {
            # Poids par type de check (optionnel)
            #"agency_id_present": 1.5,      # Plus important
            #"agency_id_unique": 1.5,       # Plus important
            #"agency_required_fields": 1.0, # Standard
            "default": 1.0                 # Poids par défaut
        },
        "bonus_all_pass": 5,  # Bonus si tout est OK
        "min_score": 0        # Score minimum
    }
    
    config = scoring_config or default_config
    
    # Initialisation
    max_score = config["max_score"]
    current_score = max_score
    penalties = config["penalties"]
    weights = config["weights"]
    
    # Détails du scoring
    score_breakdown = {
        "initial_score": max_score,
        "checks_evaluated": len(checks),
        "penalties_applied": [],
        "total_penalty": 0,
        "bonus_applied": 0,
        "final_score": max_score
    }
    
    # Analyser chaque check
    error_count = 0
    warning_count = 0
    pass_count = 0
    
    for check in checks:
        check_name = check.get("check_name", "unknown")
        check_status = check.get("status", "unknown")
        
        # Déterminer le poids
        weight = weights.get(check_name, weights.get("default", 1.0))
        
        # Calculer la pénalité
        base_penalty = penalties.get(check_status, 0)
        weighted_penalty = base_penalty * weight
        
        # Appliquer la pénalité
        current_score -= weighted_penalty
        
        # Enregistrer dans le breakdown
        penalty_detail = {
            "check_name": check_name,
            "check_description": check.get("description", ""),
            "status": check_status,
            "base_penalty": base_penalty,
            "weight": weight,
            "weighted_penalty": weighted_penalty,
            "message": check.get("message", "")
        }
        
        # Ajouter des détails spécifiques selon le type d'erreur
        if check_status == "error" and check.get("details"):
            details = check["details"]
            if "total_issues" in details:
                penalty_detail["issues_count"] = details["total_issues"]
            if "duplicate_ids" in details:
                penalty_detail["duplicate_count"] = len(details["duplicate_ids"])
            if "missing_column" in details:
                penalty_detail["missing_column"] = True
        
        score_breakdown["penalties_applied"].append(penalty_detail)
        score_breakdown["total_penalty"] += weighted_penalty
        
        # Compter les statuts
        if check_status == "error":
            error_count += 1
        elif check_status == "warning":
            warning_count += 1
        elif check_status == "pass":
            pass_count += 1
    
    # Bonus si tout est OK
    if error_count == 0 and warning_count == 0 and pass_count > 0:
        bonus = config.get("bonus_all_pass", 0)
        current_score += bonus
        score_breakdown["bonus_applied"] = bonus
    
    # Appliquer le score minimum
    min_score = config.get("min_score", 0)
    current_score = max(current_score, min_score)
    
    # Finaliser le breakdown
    score_breakdown.update({
        "final_score": round(current_score, 1),
        "score_percentage": round((current_score / max_score) * 100, 1),
        "status_counts": {
            "errors": error_count,
            "warnings": warning_count,
            "passes": pass_count
        },
        "overall_status": overall_status
    })
    return {
        "score": round(current_score, 1),
        "max_score": max_score,
        "percentage": round((current_score / max_score) * 100, 1),
        "grade": _calculate_grade(current_score, max_score),
        "breakdown": score_breakdown
    }


def _calculate_grade(score, max_score):
    """Calcule une note littérale basée sur le score"""
    percentage = (score / max_score) * 100
    
    if percentage >= 95:
        return "A+"
    elif percentage >= 90:
        return "A"
    elif percentage >= 85:
        return "B+"
    elif percentage >= 80:
        return "B"
    elif percentage >= 75:
        return "C+"
    elif percentage >= 70:
        return "C"
    elif percentage >= 60:
        return "D"
    else:
        return "F"
    
def is_truly_empty(value):
    """Vérifie si une valeur est vraiment vide (NaN, None, '', 'nan', etc.)"""
    if pd.isna(value):
        return True
    
    str_value = str(value).strip().lower()
    
    # Valeurs considérées comme vides
    empty_values = {'', 'nan', 'none', 'null', 'n/a', 'na', '#n/a'}
    
    return str_value in empty_values

def calculate_validity_score(checks):
    """Calcule le score de validité basé sur les statistiques des checks"""
    total_validity_percentages = []
    has_errors = False
    has_warnings = False
    
    for check in checks:
        # Compter les statuts pour déterminer le statut global
        if check['status'] == 'error':
            has_errors = True
        if check['status'] == 'warning':
            has_warnings = True
        
        # Vérifier si c'est un champ optionnel avec colonne manquante
        is_missing_column = check.get('details', {}).get('missing_column', False)
        is_optional = check.get('type') == 'optional'
        
        if is_missing_column and is_optional:
            # Champ optionnel avec colonne manquante → EXCLU du calcul
            continue
        
        if 'statistics' in check:
            # Champ avec statistiques détaillées
            stats = check['statistics']
            total = stats.get('number', 0)
            invalid = stats.get('invalid', 0)
            empty = stats.get('empty', 0)
            valid = total - invalid - empty
            
            if total > 0:
                validity_percentage = (valid / total) * 100
                total_validity_percentages.append(validity_percentage)
        elif check['status'] == 'pass':
            # Champ sans statistiques mais avec statut "pass" = 100%
            total_validity_percentages.append(100)
        elif is_missing_column and check.get('type') == 'required':
            # Champ requis avec colonne manquante = 0%
            total_validity_percentages.append(0)
        elif check['status'] == 'error':
            # Autres erreurs = 0%
            total_validity_percentages.append(0)
    
    if len(total_validity_percentages) == 0:
        return {"score": 0, "percentage": 0, "grade": "F"}
    
    # Calculer la moyenne des pourcentages de validité
    average_validity = sum(total_validity_percentages) / len(total_validity_percentages)
    grade = _calculate_grade_from_percentage(average_validity)
    
    return {
        "score": round(average_validity, 1),
        "percentage": round(average_validity),
        "grade": grade
    }

def _calculate_grade_from_percentage(percentage):
    """Calcule une note littérale basée sur le pourcentage"""
    if percentage >= 95:
        return "A+"
    elif percentage >= 90:
        return "A"
    elif percentage >= 85:
        return "B+"
    elif percentage >= 80:
        return "B"
    elif percentage >= 75:
        return "C+"
    elif percentage >= 70:
        return "C"
    elif percentage >= 60:
        return "D"
    else:
        return "F"
    
def analyze_accessibility_field(df, field_name, field_config, id_field, accessibility_mapping):
    """
    Analyse générique d'un champ d'accessibilité UFR
    
    Args:
        df (pd.DataFrame): DataFrame à analyser
        field_name (str): Nom du champ ('wheelchair_boarding' ou 'wheelchair_accessible')
        field_config (dict): Configuration du format depuis le dict format
        id_field (str): Nom du champ ID ('stop_id', 'trip_id', etc.)
        accessibility_mapping (dict): Mapping des valeurs vers les libellés
    
    Returns:
        dict: Résultats complets avec validation technique + analyse métier + répartition
    """
    
    # 1. VALIDATION TECHNIQUE - Réutilise la fonction existante
    technical_validation = check_format_field(df, field_name, field_config, id_field)
    
    # 2. ANALYSE MÉTIER
    business_analysis = _calculate_ufr_business_metrics(df, field_name, id_field, accessibility_mapping)
    
    # 3. RÉPARTITION POUR GRAPHIQUE
    repartition = _calculate_ufr_repartition(df, field_name, accessibility_mapping)
    
    # 4. RECOMMANDATIONS AUTOMATIQUES
    recommendations = _generate_ufr_recommendations(business_analysis, len(df))
    
    # 5. DÉTAILS (liste des IDs problématiques)
    details = _get_ufr_details(df, field_name, id_field)
    
    # 6. SCORE ET STATUT GLOBAL
    overall_status = _determine_ufr_status(technical_validation, business_analysis)
    score_result = _calculate_ufr_score(technical_validation, business_analysis)
    
    return {
        "status": overall_status,
        "score": score_result["score"],
        "grade": score_result["grade"],
        "percentage": score_result["percentage"],
        "technical_validation": technical_validation,
        "business_metrics": business_analysis,
        "repartition": repartition,
        "recommendations": recommendations,
        "details": details,
        "field_info": {
            "field_name": field_name,
            "id_field": id_field,
            "total_records": len(df)
        },
        "checks": [
            technical_validation,  # Le check technique existant
            {
                "check_name": "ufr_business_analysis", 
                "description": "Analyse métier UFR",
                "status": overall_status,
                "message": f"Taux de renseignement: {business_analysis['completion_rate']}%"
            }
        ]
    }


def _calculate_ufr_business_metrics(df, field_name, id_field, accessibility_mapping):
    """Calcule les métriques métier UFR"""
    
    total_records = len(df)
    metrics = {
        "total_records": total_records,
        "completion_rate": 0.0,
        "accessibility_rate": 0.0,
        "no_info_count": 0,
        "accessible_count": 0,
        "not_accessible_count": 0,
        "unknown_values_count": 0
    }
    
    if field_name not in df.columns:
        # Colonne manquante = 0% partout
        return metrics
    
    # Compter les différentes valeurs
    value_counts = df[field_name].value_counts(dropna=False)
    
    for value, count in value_counts.items():
        if pd.isna(value) or is_truly_empty(value):
            metrics["no_info_count"] += count
        else:
            try:
                int_value = int(float(value))
                if int_value == 0:
                    metrics["no_info_count"] += count
                elif int_value == 1:
                    metrics["accessible_count"] += count
                elif int_value == 2:
                    metrics["not_accessible_count"] += count
                else:
                    # Valeurs inconnues (3, 4, etc. selon la spec)
                    metrics["unknown_values_count"] += count
            except (ValueError, TypeError):
                metrics["unknown_values_count"] += count
    
    # Calculer les taux
    records_with_explicit_info = metrics["accessible_count"] + metrics["not_accessible_count"]
    
    # Taux de renseignement = % avec info explicite (valeurs 1 ou 2)
    if total_records > 0:
        metrics["completion_rate"] = round((records_with_explicit_info / total_records) * 100, 1)
        
        # Taux d'accessibilité = % accessibles sur le total
        metrics["accessibility_rate"] = round((metrics["accessible_count"] / total_records) * 100, 1)
    
    return metrics


def _calculate_ufr_repartition(df, field_name, accessibility_mapping):
    """Calcule la répartition pour le graphique"""
    
    if field_name not in df.columns:
        return {}
    
    total_records = len(df)
    repartition = {}
    
    # Compter les valeurs, y compris NaN/vides
    value_counts = df[field_name].fillna('0').astype(str).value_counts()
    
    for value_str, count in value_counts.items():
        try:
            # Convertir en int pour le mapping
            if value_str.lower() in ['nan', '', 'none']:
                int_value = 0
            else:
                int_value = int(float(value_str))
                
            # Utiliser le mapping pour le nom
            type_name = accessibility_mapping.get(int_value, f"Valeur inconnue ({int_value})")
            
            repartition[str(int_value)] = {
                "count": int(count),
                "percentage": round((count / total_records) * 100, 1),
                "type_name": type_name
            }
            
        except (ValueError, TypeError):
            # Valeurs vraiment problématiques
            repartition[value_str] = {
                "count": int(count),
                "percentage": round((count / total_records) * 100, 1),
                "type_name": f"Valeur invalide ({value_str})"
            }
    
    return repartition


def _generate_ufr_recommendations(business_metrics, total_records):
    """Génère les recommandations automatiques"""
    
    recommendations = []
    completion_rate = business_metrics["completion_rate"]
    accessibility_rate = business_metrics["accessibility_rate"]
    no_info_count = business_metrics["no_info_count"]
    
    # Cas spécial : tous les arrêts en valeur 0
    if no_info_count == total_records:
        recommendations.append({
            "type": "critical",
            "message": "L'accessibilité UFR n'est pas renseignée sur ce réseau",
            "description": "Tous les enregistrements ont la valeur 0 (pas d'information)",
            "priority": "high"
        })
        return recommendations
    
    # Recommandations basées sur les seuils
    if completion_rate < 80:
        recommendations.append({
            "type": "warning",
            "message": "Améliorer la couverture d'information UFR",
            "description": f"Seulement {completion_rate}% des enregistrements ont une information explicite d'accessibilité",
            "priority": "medium"
        })
    
    if accessibility_rate < 50:
        recommendations.append({
            "type": "info", 
            "message": "Évaluer l'accessibilité du réseau",
            "description": f"Seulement {accessibility_rate}% des enregistrements sont déclarés accessibles UFR",
            "priority": "medium"
        })
    
    if no_info_count > 0 and completion_rate > 20:  # Pas si tout est à 0
        recommendations.append({
            "type": "warning",
            "message": "Auditer et qualifier les enregistrements non documentés", 
            "description": f"{no_info_count} enregistrements sans information d'accessibilité (valeur 0)",
            "priority": "low"
        })
    
    # Message positif si tout va bien
    if completion_rate >= 90 and accessibility_rate >= 70:
        recommendations.append({
            "type": "success",
            "message": "Excellente couverture d'accessibilité UFR",
            "description": f"Couverture d'information: {completion_rate}%, Accessibilité: {accessibility_rate}%",
            "priority": "info"
        })
    
    return recommendations


def _get_ufr_details(df, field_name, id_field):
    """Récupère les détails (IDs des enregistrements problématiques)"""
    
    details = {
        "no_info_ids": [],
        "accessible_ids": [],
        "not_accessible_ids": [],
        "invalid_ids": []
    }
    
    if field_name not in df.columns:
        return details
    
    if id_field not in df.columns:
        # Pas d'ID disponible, utiliser les index
        id_field = df.index.name or "row_index"
        df_with_ids = df.reset_index()
    else:
        df_with_ids = df
    
    for idx, row in df_with_ids.iterrows():
        value = row[field_name]
        record_id = str(row[id_field]) if id_field in row else str(idx)
        
        if pd.isna(value) or is_truly_empty(value):
            details["no_info_ids"].append(record_id)
        else:
            try:
                int_value = int(float(value))
                if int_value == 0:
                    details["no_info_ids"].append(record_id)
                elif int_value == 1:
                    details["accessible_ids"].append(record_id)
                elif int_value == 2:
                    details["not_accessible_ids"].append(record_id)
                else:
                    details["invalid_ids"].append(f"{record_id}:{value}")
            except (ValueError, TypeError):
                details["invalid_ids"].append(f"{record_id}:{value}")
    
    return details


def _determine_ufr_status(technical_validation, business_metrics):
    """Détermine le statut global UFR"""
    
    # Priorité au technique
    tech_status = technical_validation["status"]
    if tech_status == "error":
        return "error"
    
    # Ensuite analyser le métier
    completion_rate = business_metrics["completion_rate"]
    total_records = business_metrics["total_records"]
    no_info_count = business_metrics["no_info_count"]
    
    # Cas critique : tout est à 0
    if no_info_count == total_records and total_records > 0:
        return "warning"  # Pas d'erreur technique, mais problème métier
    
    # Cas warning : faible couverture
    if completion_rate < 50:
        return "warning"
    
    # Sinon, reprendre le statut technique
    return tech_status


def _calculate_ufr_score(technical_validation, business_metrics):
    """Calcule le score UFR combiné technique + métier"""
    
    # Score technique (0-100)
    tech_score = technical_validation.get("statistics", {}).get("number", 0)
    if tech_score > 0:
        tech_invalid = technical_validation.get("statistics", {}).get("invalid", 0)
        tech_empty = technical_validation.get("statistics", {}).get("empty", 0)
        tech_valid = tech_score - tech_invalid - tech_empty
        tech_percentage = (tech_valid / tech_score) * 100
    else:
        tech_percentage = 0 if technical_validation["status"] == "error" else 100
    
    # Score métier (0-100) basé sur le taux de renseignement
    business_score = business_metrics["completion_rate"]
    
    # Score combiné : 70% technique + 30% métier
    combined_score = (tech_percentage * 0.7) + (business_score * 0.3)
    
    # Grade basé sur le score combiné
    grade = _calculate_grade_from_percentage(combined_score)
    
    return {
        "score": round(combined_score, 1),
        "percentage": round(combined_score),
        "grade": grade,
        "breakdown": {
            "technical_score": round(tech_percentage, 1),
            "business_score": round(business_score, 1),
            "weight_technical": 70,
            "weight_business": 30
        }
    }

def calculate_gtfs_statistics(project_id):
    """
    Calcule les statistiques globales d'un GTFS
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Statistiques complètes du GTFS
    """
    from services.gtfs_handler import GTFSHandler
    import pandas as pd
    from datetime import datetime
    
    try:
        # Vérifier si le GTFS existe
        if not GTFSHandler.has_gtfs_data(project_id):
            return None
            
        # Lire les données GTFS
        print(f"🔍 DEBUG: Chargement données GTFS...")
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        print(f"🔍 DEBUG: gtfs_data keys = {list(gtfs_data.keys()) if gtfs_data else 'None'}")
        
        if not gtfs_data:
            print("❌ gtfs_data est None ou vide")
            return None
        
        # === ANALYSE DES FICHIERS ===
        fichiers_obligatoires = ['agency.txt', 'routes.txt', 'trips.txt', 'stops.txt', 'stop_times.txt']
        fichiers_optionnels = ['calendar.txt', 'calendar_dates.txt', 'shapes.txt', 'transfers.txt', 'feed_info.txt', 'frequencies.txt']
        
        fichiers_presents = list(gtfs_data.keys())
        obligatoires_presents = [f for f in fichiers_obligatoires if f in fichiers_presents]
        obligatoires_manquants = [f for f in fichiers_obligatoires if f not in fichiers_presents]
        optionnels_presents = [f for f in fichiers_optionnels if f in fichiers_presents]
        
        conformite_fichiers = len(obligatoires_manquants) == 0
        
        # === COMPTAGES ===
        statistiques = {
            'nombre_fichiers': len(fichiers_presents),
            'fichiers_presents': fichiers_presents,
            'fichiers_obligatoires': {
                'presents': obligatoires_presents,
                'manquants': obligatoires_manquants,
                'conforme': conformite_fichiers
            },
            'fichiers_optionnels_presents': optionnels_presents,
            
            # Comptages des entités
            'nombre_agences': len(gtfs_data.get('agency.txt', [])),
            'nombre_routes': len(gtfs_data.get('routes.txt', [])),
            'nombre_trips': len(gtfs_data.get('trips.txt', [])),
            'nombre_stop_times': len(gtfs_data.get('stop_times.txt', [])),
            'nombre_shapes': len(gtfs_data.get('shapes.txt', [])) if 'shapes.txt' in gtfs_data else 0,
        }
        
        # === ANALYSE DES ARRÊTS ===
        if 'stops.txt' in gtfs_data:
            stops_df = gtfs_data['stops.txt']  # C'est déjà un DataFrame
            statistiques['nombre_stops_total'] = len(stops_df)
            
            if 'location_type' in stops_df.columns:
                location_counts = stops_df['location_type'].value_counts()
                statistiques['stops_par_type'] = {
                    'points_arret': int(location_counts.get(0, 0)),  # location_type = 0
                    'zones_arret': int(location_counts.get(1, 0)),   # location_type = 1
                    'autres': int(sum(location_counts.get(i, 0) for i in [2, 3, 4]))
                }
            else:
                # Si pas de colonne location_type, tous sont des points d'arrêt
                statistiques['stops_par_type'] = {
                    'points_arret': len(stops_df),
                    'zones_arret': 0,
                    'autres': 0
                }
        else:
            statistiques.update({
                'nombre_stops_total': 0,
                'stops_par_type': {'points_arret': 0, 'zones_arret': 0, 'autres': 0}
            })
        
        # === PÉRIODE DE VALIDITÉ ===
        periode = calculate_service_period(gtfs_data)
        statistiques['periode_validite'] = periode
        
        # === MÉTADONNÉES ===
        statistiques['date_calcul'] = datetime.utcnow().isoformat()
        
        return statistiques
        
    except Exception as e:
        print(f"Erreur calcul statistiques: {e}")
        return None


def calculate_service_period(gtfs_data):
    """
    Calcule la période de validité du service à partir des calendriers
    
    Args:
        gtfs_data (dict): Données GTFS chargées
        
    Returns:
        dict: Période de validité avec dates min/max
    """
    import pandas as pd
    from datetime import datetime
    
    dates = []
    
    # Analyser calendar.txt
    if 'calendar.txt' in gtfs_data:
        calendar_df = gtfs_data['calendar.txt']  # Déjà un DataFrame
        if 'start_date' in calendar_df.columns:
            dates.extend(pd.to_datetime(calendar_df['start_date'], format='%Y%m%d'))
        if 'end_date' in calendar_df.columns:
            dates.extend(pd.to_datetime(calendar_df['end_date'], format='%Y%m%d'))
    
    # Analyser calendar_dates.txt
    if 'calendar_dates.txt' in gtfs_data:
        calendar_dates_df = gtfs_data['calendar_dates.txt']  # Déjà un DataFrame
        if 'date' in calendar_dates_df.columns:
            dates.extend(pd.to_datetime(calendar_dates_df['date'], format='%Y%m%d'))
    
    if not dates:
        return {
            'date_debut': None,
            'date_fin': None,
            'duree_jours': 0,
            'source': 'aucune'
        }
    
    date_min = min(dates)
    date_max = max(dates)
    duree = (date_max - date_min).days + 1
    
    sources = []
    if 'calendar.txt' in gtfs_data:
        sources.append('calendar')
    if 'calendar_dates.txt' in gtfs_data:
        sources.append('calendar_dates')
    
    return {
        'date_debut': date_min.strftime('%Y-%m-%d'),
        'date_fin': date_max.strftime('%Y-%m-%d'),
        'duree_jours': duree,
        'source': ', '.join(sources)
    }