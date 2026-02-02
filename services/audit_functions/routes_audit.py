"""
Fonctions d'audit pour le fichier routes.txt
"""
import pandas as pd
import re
from datetime import datetime
from urllib.parse import urlparse
from services.gtfs_handler import GTFSHandler

from .generic_functions import is_truly_empty
from .generic_functions import check_required_field
from .generic_functions import calculate_score_from_checks
from .generic_functions import check_format_field
from .generic_functions import check_orphan_id
from .generic_functions import check_unused_id
from .generic_functions import calculate_summary
from .generic_functions import calculate_validity_score

format = {'route_type':{'genre':'required','description':"Validité des types de route", 'type':'listing', 'valid_fields':{"0", "1", "2", "3", "4", "5", "6", "7", "11", "12", "100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "200", "201", "202", "203", "204", "205", "206", "207", "208", "209", "300", "301", "302", "400", "401", "402", "403", "404", "405", "406", "407", "408", "409", "410", "411", "412", "413", "414", "415", "416", "417", "500", "600", "601", "602", "603", "604", "605", "606", "607","700", "701", "702", "703", "704", "705", "706", "800", "900", "1000", "1100", "1200", "1300", "1400", "1500", "1600", "1700" }},
          'route_color':{'genre':'optional','description':"Validité des couleurs de route", 'type':'regex', 'pattern':re.compile(r'^[0-9A-Fa-f]{6}$')},
          'route_text_color':{'genre':'optional','description':"Validité des couleurs de texte de route", 'type':'regex', 'pattern':re.compile(r'^[0-9A-Fa-f]{6}$')},
          'route_url':{'genre':'optional','description':"Validité des URL", 'type':'url'},
          'continuous_pickup':{'genre':'optional','description':"Validité des continuous_pickup", 'type':'listing', 'valid_fields':{'0', '1', '2', '3'}},
          'continuous_drop_off':{'genre':'optional','description':"Validité des continuous_drop_off", 'type':'listing', 'valid_fields':{'0', '1', '2', '3'}},
}

def audit_routes_file(project_id, progress_callback = None):
    """
    Audit complet du fichier routes.txt
    
    Args:
        project_id (str): ID du projet
        
    Returns:
        dict: Résultats de l'audit selon la structure définie
    """

    if progress_callback:
        progress_callback(15, "Chargement du fichier routes.txt...", "loading")

    routes_df = GTFSHandler.get_gtfs_data(project_id, 'routes.txt')
    
    if routes_df is None or routes_df.empty:
        if progress_callback:
            progress_callback(0, "Fichier routes.txt introuvable", "error")
        return {
            "file": "routes.txt",
            "status": "missing",
            "message": "Fichier routes.txt non trouvé ou vide",
            "severity": "critical",
            "timestamp": datetime.now().isoformat()
        }
    
    if progress_callback:
        progress_callback(25, "Vérification des champs obligatoires...", "required_fields")
    
    results = {
        "file": "routes.txt",
        "status": "processed",
        "total_rows": len(routes_df),
        "timestamp": datetime.now().isoformat(),
        "required_fields": _check_required_fields(routes_df, project_id)
    }

    if progress_callback:
        progress_callback(35, "Vérification du format des donnés...", "data_format")

    results["data_format"] = _check_data_format(routes_df)

    if progress_callback:
        progress_callback(45, "Vérification de la cohérence des données...", "data_consistency")

    results["data_consistency"] = _check_data_consistency(routes_df, project_id)

    if progress_callback:
        progress_callback(55, "Vérification de l'accessibilité de la donnée...", "accessiblity")

    results["accessiblity"] = _check_accessibility(routes_df)

    if progress_callback:
        progress_callback(65, "Génération de statistiques...", "statistics")

    results["statistics"] = _generate_statistics(routes_df, project_id)

    if progress_callback:
        progress_callback(80, "Calcul du résumé...", "summary")

    # Calculer le résumé global
    results["summary"] = calculate_summary(results, ['required_fields','data_format','data_consistency', 'accessiblity','statistics'])
    print(results['statistics'])
    return results

def _check_required_fields(df, project_id):
    """Vérifications des champs obligatoires"""
    checks = []
    
    # 1. Vérifier route_id
    route_id_check = check_required_field(df, 'route_id', 'route_id')
    checks.append(route_id_check)
    
    # 2. Vérifier unicité des route_id
    if 'route_id' in df.columns and not df['route_id'].isna().all():
        uniqueness_check = {
            "check_name": "route_id_unique",
            "description": "Unicité des route_id",
            "status": "pass",
            "message": "",
            "details": {}
        }
        
        duplicates = df[df.duplicated('route_id', keep=False) & df['route_id'].notna()]
        if not duplicates.empty:
            duplicate_ids = duplicates['route_id'].unique().tolist()
            duplicate_rows = duplicates.index.tolist()
            
            # Ajouter les noms des routes pour plus de clarté
            duplicate_details = []
            for dup_id in duplicate_ids:
                dup_rows = duplicates[duplicates['route_id'] == dup_id]
                detail = {
                    "route_id": str(dup_id),
                    "occurrences": len(dup_rows),
                    "rows": dup_rows.index.tolist(),
                }
                if 'route_short_name' in df.columns:
                    detail["route_short_names"] = dup_rows['route_short_name'].fillna('N/A').astype(str).tolist()
                if 'route_long_name' in df.columns:
                    detail["route_long_names"] = dup_rows['route_long_name'].fillna('N/A').astype(str).tolist()
                duplicate_details.append(detail)
            
            uniqueness_check.update({
                "status": "error",
                "message": f"{len(duplicate_ids)} route_id dupliqués",
                "details": {
                    "duplicate_ids": [str(x) for x in duplicate_ids], 
                    "duplicate_rows": duplicate_rows,
                    "duplicate_details": duplicate_details
                }
            })
        else:
            uniqueness_check["message"] = "Tous les route_id sont uniques"
            
        checks.append(uniqueness_check)
    
    # 3. Vérifier qu'au moins route_short_name ou route_long_name est présent
    name_check = {
        "check_name": "route_short_name_or_long_name",
        "description": "Au moins route_short_name ou route_long_name doit être présent",
        "status": "pass",
        "message": "",
        "details": {}
    }
    
    has_short = 'route_short_name' in df.columns
    has_long = 'route_long_name' in df.columns
    
    if not has_short and not has_long:
        name_check.update({
            "status": "error",
            "message": "Ni route_short_name ni route_long_name ne sont présents",
            "details": {"missing_both_columns": True}
        })
    else:
        # Vérifier pour chaque ligne qu'au moins un nom est présent
        issues = []
        for idx, row in df.iterrows():
            short_empty = not has_short or is_truly_empty(row.get('route_short_name'))
            long_empty = not has_long or is_truly_empty(row.get('route_long_name'))
            
            if short_empty and long_empty:
                # Récupérer les identifiants de la route
                route_id = df.loc[idx, 'route_id'] if 'route_id' in df.columns else 'N/A'
                issues.append({
                    "row": idx,
                    "route_id": str(route_id)
                })
        
        if issues:
            name_check.update({
                "status": "error",
                "message": f"{len(issues)} routes sans nom court ni nom long",
                "details": {
                    "missing_both_names": len(issues),
                    "affected_routes": issues
                }
            })
        else:
            name_check["message"] = "Toutes les routes ont au moins un nom (court ou long)"
    
    checks.append(name_check)
    
    # 4. Vérifier route_type
    type_check = check_required_field(df, 'route_type', 'route_id')
    checks.append(type_check)
     
    # 6. Vérifier agency_id si plusieurs agences
    agency_multiple_check = {
        "check_name": "agency_id_present_if_multiple",
        "description": "agency_id obligatoire si plusieurs agences dans agency.txt",
        "status": "pass",
        "message": "",
        "details": {}
    }
    
    try:
        agency_df = GTFSHandler.get_gtfs_data(project_id, 'agency.txt')
        
        if agency_df is not None and len(agency_df) > 1:
            # Plusieurs agences, agency_id obligatoire
            if 'agency_id' not in df.columns:
                agency_multiple_check.update({
                    "status": "error",
                    "message": "Champ agency_id manquant (obligatoire avec plusieurs agences)",
                    "details": {"missing_column": True, "agency_count": len(agency_df)}
                })
            else:
                missing_count = df['agency_id'].isna().sum()
                if missing_count > 0:
                    missing_rows = df[df['agency_id'].isna()].index.tolist()
                    # Récupérer les identifiants des routes concernées
                    affected_route_ids = []
                    affected_route_short_names = []
                    affected_route_long_names = []
                    if 'route_id' in df.columns:
                        affected_route_ids = df.loc[missing_rows, 'route_id'].fillna('N/A').astype(str).tolist()
                    if 'route_short_name' in df.columns:
                        affected_route_short_names = df.loc[missing_rows, 'route_short_name'].fillna('N/A').astype(str).tolist()
                    if 'route_long_name' in df.columns:
                        affected_route_long_names = df.loc[missing_rows, 'route_long_name'].fillna('N/A').astype(str).tolist()
                    
                    agency_multiple_check.update({
                        "status": "error",
                        "message": f"{missing_count} valeurs agency_id manquantes",
                        "details": {
                            "missing_count": missing_count,
                            "affected_rows": missing_rows,
                            "affected_route_ids": affected_route_ids,
                            "affected_route_short_names": affected_route_short_names,
                            "affected_route_long_names": affected_route_long_names,
                            "agency_count": len(agency_df)
                        }
                    })
                else:
                    agency_multiple_check["message"] = f"Tous les agency_id sont présents ({len(agency_df)} agences)"
        else:
            agency_count = len(agency_df) if agency_df is not None else 0
            agency_multiple_check["message"] = f"agency_id optionnel ({agency_count} agence)"
    
    except Exception as e:
        agency_multiple_check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les agences multiples: {str(e)}",
            "details": {"error": str(e)}
        })
    
    checks.append(agency_multiple_check)
    
    # 7. Vérifier que les agency_id référencés existent
    agency_exists_check = {
        "check_name": "agency_id_exists",
        "description": "Les agency_id référencés existent dans agency.txt",
        "status": "pass",
        "message": "",
        "details": {}
    }
    
    try:
        agency_df = GTFSHandler.get_gtfs_data(project_id, 'agency.txt')
        
        if agency_df is not None and 'agency_id' in df.columns and 'agency_id' in agency_df.columns:
            # Récupérer les agency_id valides
            valid_agency_ids = set(agency_df['agency_id'].dropna().unique())
            route_agency_ids = set(df['agency_id'].dropna().unique())
            
            # Trouver les agency_id dans routes mais pas dans agency
            invalid_agency_ids = route_agency_ids - valid_agency_ids
            
            if invalid_agency_ids:
                invalid_routes = []
                for agency_id in invalid_agency_ids:
                    routes_with_invalid_agency = df[df['agency_id'] == agency_id]
                    for idx, row in routes_with_invalid_agency.iterrows():
                        route_id = df.loc[idx, 'route_id'] if 'route_id' in df.columns else 'N/A'
                        route_short_name = df.loc[idx, 'route_short_name'] if 'route_short_name' in df.columns else 'N/A'
                        route_long_name = df.loc[idx, 'route_long_name'] if 'route_long_name' in df.columns else 'N/A'
                        invalid_routes.append({
                            #"row": idx,
                            "agency_id": str(agency_id),
                            "route_id": str(route_id),
                            #"route_short_name": str(route_short_name),
                            #"route_long_name": str(route_long_name)
                        })
                invalid_route_group = {}
                for item in invalid_routes:
                    agency_id = str(item["agency_id"])
                    route_id = str(item["route_id"])
                    
                    if agency_id not in invalid_route_group:
                        invalid_route_group[agency_id] = []
                    
                    invalid_route_group[agency_id].append(route_id)


                agency_exists_check.update({
                    "status": "error",
                    "message": f"{len(invalid_agency_ids)} agency_id inexistants référencés",
                    "details": {
                        "invalid_agency_ids": list(invalid_agency_ids),
                        "invalid_routes": invalid_route_group,
                        #"valid_agency_ids": list(valid_agency_ids)
                    }
                })
            else:
                agency_exists_check["message"] = "Tous les agency_id référencés existent"
        else:
            agency_exists_check.update({
                "status": "info",
                "message": "Impossible de vérifier l'existence des agency_id",
                "details": {"reason": "missing_agency_file_or_columns"}
            })
    
    except Exception as e:
        agency_exists_check.update({
            "status": "warning",
            "message": f"Impossible de vérifier les agency_id: {str(e)}",
            "details": {"error": str(e)}
        })
    
    checks.append(agency_exists_check)
    
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
    route_type_check = check_format_field(df, 'route_type', format['route_type'], 'route_id')
    checks.append(route_type_check)   

    route_color_check = check_format_field(df, 'route_color', format['route_color'], 'route_id')
    checks.append(route_color_check)   

    route_text_color_check = check_format_field(df, 'route_text_color', format['route_text_color'], 'route_id')
    checks.append(route_text_color_check)   

    route_url = check_format_field(df, 'route_url', format['route_url'], 'route_id')
    checks.append(route_url)   

    continuous_pickup = check_format_field(df, 'continuous_pickup', format['continuous_pickup'], 'route_id')
    checks.append(continuous_pickup)   

    continuous_drop_off = check_format_field(df, 'continuous_drop_off', format['continuous_drop_off'], 'route_id')
    checks.append(continuous_drop_off)   
    
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
    
    # Vérifier les routes orphelines (référencées dans trips mais absentes)
    orphan_check = check_orphan_id(df, 'route_id', 'trips.txt', project_id)
    checks.append(orphan_check)

    # Vérifier les routes non utilisées (dans routes mais pas dans trips)
    unused_check = check_unused_id(df, 'route_id', 'trips.txt', project_id)
    checks.append(unused_check)
    
    # Vérifier le contraste des couleurs
    duplicate_names_check = _check_duplicate_names(df)
    checks.append(duplicate_names_check)
    
    # Vérifier les trous dans route_sort_order
    sort_order_gaps_check = _check_sort_order_gaps(df)
    checks.append(sort_order_gaps_check)
    
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

def _check_accessibility(df):
    checks = []
    
    # Vérifier le contraste des couleurs
    contrast_check = _check_color_contrast(df)
    checks.append(contrast_check)
    
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

def _generate_statistics(df, project_id):
    """Génère les statistiques du fichier"""
    checks = []
    # Métriques de base
    repartition = _calculate_route_repartition(df, project_id)

    repartition_check = {
        "check_name": "route_repartition",
        "description": "Analyse de la répartition des routes",
        "status": "info",
        "message": f"Analyse de {len(df)} routes",
        "details": {"repartition": repartition}
    }
    checks.append(repartition_check)
    
    return {
        "status": "info",
        "checks": checks,
        "repartition":repartition,
    }
def _check_color_contrast(df):
    """Vérifie le contraste entre route_color et route_text_color"""
    check = {
        "check_name": "color_contrast_check",
        "description": "Contraste suffisant entre route_color et route_text_color",
        "status": "pass",
        "message": "Contrastes de couleurs suffisants",
        "details": {},
        "type": "optional"
    }
    
    poor_contrasts = []        # Contrastes insuffisants (< 4.5:1)
    invalid_contrasts = []     # Calcul impossible (toutes les raisons)
    empty_count = 0
    invalid_count = 0
    total_rows = len(df)
    
    if 'route_color' in df.columns and 'route_text_color' in df.columns:
        for idx, row in df.iterrows():
            bg_color = str(row.get('route_color', '')).strip()
            text_color = str(row.get('route_text_color', '')).strip()
            route_id = df.loc[idx, 'route_id'] if 'route_id' in df.columns else 'N/A'
            
            # Vérifier si les couleurs sont disponibles et valides
            bg_empty = is_truly_empty(bg_color)
            text_empty = is_truly_empty(text_color)
            bg_valid_format = len(bg_color) == 6 if not bg_empty else False
            text_valid_format = len(text_color) == 6 if not text_empty else False
            
            if bg_empty or text_empty or not bg_valid_format or not text_valid_format:
                # Calcul impossible → ajouter aux invalid_contrasts
                empty_count += 1
                reason = []
                if bg_empty:
                    reason.append("route_color manquant")
                if text_empty:
                    reason.append("route_text_color manquant")
                if not bg_empty and not bg_valid_format:
                    reason.append("route_color format invalide")
                if not text_empty and not text_valid_format:
                    reason.append("route_text_color format invalide")
                
                invalid_contrasts.append({
                    "route_id": str(route_id),
                    "bg_color": bg_color if not bg_empty else "N/A",
                    "text_color": text_color if not text_empty else "N/A",
                    "reason": ", ".join(reason)
                })
            else:
                # Couleurs présentes et format valide → calculer le contraste
                try:
                    contrast_ratio = _calculate_color_contrast(bg_color, text_color)
                    # WCAG recommande un ratio d'au moins 4.5:1 pour le texte normal
                    if contrast_ratio < 4.5:
                        invalid_count += 1
                        poor_contrasts.append(f"{route_id}:{contrast_ratio}")
                except Exception as e:
                    # Erreur de calcul → aussi dans invalid_contrasts
                    empty_count += 1
                    invalid_contrasts.append({
                        "route_id": str(route_id),
                        "bg_color": bg_color,
                        "text_color": text_color,
                        "reason": f"Erreur de calcul: {str(e)}"
                    })
    else:
        # Colonnes manquantes → toutes les lignes sont dans invalid_contrasts
        empty_count = total_rows
        for idx, row in df.iterrows():
            route_id = df.loc[idx, 'route_id'] if 'route_id' in df.columns else 'N/A'
            invalid_contrasts.append({
                "route_id": str(route_id),
                "reason": "Colonnes route_color et/ou route_text_color manquantes"
            })

    # Calculer les valides
    valid_count = total_rows - invalid_count - empty_count
    
    # Ajouter les statistiques
    check["statistics"] = {
        'number': total_rows,      # Toutes les lignes
        'invalid': invalid_count,  # Contrastes insuffisants
        'empty': empty_count       # Calcul impossible
    }
    invalid_contrast_group = {}
    for item in invalid_contrasts:
        reason = str(item["reason"])
        route_id = str(item["route_id"])
        
        if reason not in invalid_contrast_group:
            invalid_contrast_group[reason] = []
        
        invalid_contrast_group[reason].append(route_id)

    
    # Mettre à jour le message et les détails
    if empty_count == total_rows:
        check.update({
            "status": "info",
            "message": "Aucun contraste calculable",
            "details": {"invalid_contrasts": invalid_contrast_group}
        })
    elif invalid_count > 0:
        check.update({
            "status": "warning", 
            "message": f"{invalid_count} contrastes insuffisants, {empty_count} non calculables sur {total_rows} routes",
            "details": {
                "poor_contrasts": poor_contrasts,
                "invalid_contrasts": invalid_contrast_group
            }
        })
    elif empty_count > 0:
        check.update({
            "status": "info",
            "message": f"Tous les contrastes calculables sont suffisants ({valid_count} valides, {empty_count} non calculables)",
            "details": {"invalid_contrasts": invalid_contrast_group}
        })
    else:
        check.update({
            "message": f"Tous les contrastes sont suffisants ({valid_count}/{total_rows} routes)"
        })
    
    return check

def _check_duplicate_names(df):
    """Détecte les noms identiques avec des IDs différents, groupé par agency_id"""
    check = {
        "check_name": "duplicate_names_check",
        "description": "Détection des noms identiques avec des IDs différents (par agence)",
        "status": "pass",
        "message": "Pas de noms dupliqués",
        "details": {}
    }
    
    # NOUVEAU : Skip si pas d'agency_id
    if 'agency_id' not in df.columns:
        check.update({
            "status": "info",
            "message": "Vérification non effectuée (pas d'agency_id dans les données)",
            "details": {"reason": "missing_agency_id_column"}
        })
        return check
    
    duplicate_issues = []
    
    # Grouper par agency_id
    agency_groups = df.groupby('agency_id')
    
    for agency_id, agency_df in agency_groups:
        agency_duplicates = []
        
        # Vérifier les noms courts dans cette agence
        if 'route_short_name' in agency_df.columns:
            short_name_groups = agency_df.groupby('route_short_name')
            for name, group in short_name_groups:
                if not is_truly_empty(name) and len(group) > 1:
                    routes = []
                    for idx, row in group.iterrows():
                        routes.append({
                            "row": idx,
                            "route_id": str(row.get('route_id', 'N/A')),
                            "route_long_name": str(row.get('route_long_name', 'N/A'))
                        })
                    
                    agency_duplicates.append({
                        "type": "short_name",
                        "name": str(name),
                        "count": len(group),
                        "routes": routes
                    })
        
        # Vérifier les noms longs dans cette agence
        if 'route_long_name' in agency_df.columns:
            long_name_groups = agency_df.groupby('route_long_name')
            for name, group in long_name_groups:
                if not is_truly_empty(name) and len(group) > 1:
                    routes = []
                    for idx, row in group.iterrows():
                        routes.append({
                            "row": idx,
                            "route_id": str(row.get('route_id', 'N/A')),
                            "route_short_name": str(row.get('route_short_name', 'N/A'))
                        })
                    
                    agency_duplicates.append({
                        "type": "long_name", 
                        "name": str(name),
                        "count": len(group),
                        "routes": routes
                    })
        
        # Ajouter les duplicatas de cette agence s'il y en a
        if agency_duplicates:
            duplicate_issues.append({
                "agency_id": str(agency_id),
                "agency_name": str(agency_df.iloc[0].get('agency_name', 'N/A')) if 'agency_name' in agency_df.columns else 'N/A',
                "duplicate_count": len(agency_duplicates),
                "duplicates": agency_duplicates
            })
    
    if duplicate_issues:
        total_duplicates = sum(agency['duplicate_count'] for agency in duplicate_issues)
        check.update({
            "status": "warning",
            "message": f"{total_duplicates} noms dupliqués détectés dans {len(duplicate_issues)} agence(s)",
            "details": {"duplicate_names_by_agency": duplicate_issues}
        })
    
    return check

def _check_sort_order_gaps(df):
    """Détecte les trous dans la séquence route_sort_order"""
    check = {
        "check_name": "sort_order_gaps",
        "description": "Détection des trous dans la séquence route_sort_order",
        "status": "pass",
        "message": "Séquence d'ordre de tri cohérente",
        "details": {}
    }
    
    if 'route_sort_order' in df.columns:
        # Récupérer les valeurs non nulles et numériques
        valid_orders = []
        for idx, order in df['route_sort_order'].items():
            if not is_truly_empty(order):
                try:
                    valid_orders.append(int(order))
                except:
                    pass
        
        if len(valid_orders) > 1:
            valid_orders.sort()
            gaps = []
            
            for i in range(1, len(valid_orders)):
                if valid_orders[i] - valid_orders[i-1] > 1:
                    gaps.append({
                        "start": valid_orders[i-1],
                        "end": valid_orders[i],
                        "gap_size": valid_orders[i] - valid_orders[i-1] - 1
                    })
            
            if gaps:
                check.update({
                    "status": "info",
                    "message": f"{len(gaps)} trous détectés dans la séquence route_sort_order",
                    "details": {
                        "gaps": gaps,
                        "total_orders": len(valid_orders),
                        "min_order": min(valid_orders),
                        "max_order": max(valid_orders)
                    }
                })
    
    return check

# ===== FONCTION UTILITAIRE POUR LE CONTRASTE =====

def _calculate_color_contrast(bg_color, text_color):
    """Calcule le ratio de contraste entre deux couleurs"""
    def hex_to_rgb(hex_color):
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    
    def relative_luminance(rgb):
        r, g, b = [x/255.0 for x in rgb]
        r = r/12.92 if r <= 0.03928 else pow((r+0.055)/1.055, 2.4)
        g = g/12.92 if g <= 0.03928 else pow((g+0.055)/1.055, 2.4)
        b = b/12.92 if b <= 0.03928 else pow((b+0.055)/1.055, 2.4)
        return 0.2126 * r + 0.7152 * g + 0.0722 * b
    
    bg_rgb = hex_to_rgb(bg_color)
    text_rgb = hex_to_rgb(text_color)
    
    bg_luminance = relative_luminance(bg_rgb)
    text_luminance = relative_luminance(text_rgb)
    
    # Le contraste est le ratio entre la luminance la plus forte et la plus faible
    lighter = max(bg_luminance, text_luminance)
    darker = min(bg_luminance, text_luminance)
    
    return (lighter + 0.05) / (darker + 0.05)

def _calculate_route_repartition(df, project_id):
    """Calcule les métriques de base"""
    
    # 1. Nombre total de routes
    total_routes = len(df)
    # 2. Répartition par type de transport
    routes_by_type = {}
    if 'route_type' in df.columns:
        type_counts = df['route_type'].value_counts().to_dict()

        
        for route_type, count in type_counts.items():
            if pd.notna(route_type):
                routes_by_type[str(int(route_type))] = {
                    "count": int(count),
                    "percentage": round((count / total_routes) * 100, 1),
                    "type_name": type_names.get(int(route_type), f"Type {int(route_type)}")
                }
    
    # 3. Répartition par agence
    routes_by_agency = {}
    if 'agency_id' in df.columns:
        agency_counts = df['agency_id'].value_counts().to_dict()
        
        # Récupérer les noms des agences si possible
        try:
            agency_df = GTFSHandler.get_gtfs_data(project_id, 'agency.txt')
            agency_names = {}
            if agency_df is not None and 'agency_name' in agency_df.columns:
                for _, agency in agency_df.iterrows():
                    agency_id = agency.get('agency_id')
                    agency_name = agency.get('agency_name')
                    if pd.notna(agency_id) and pd.notna(agency_name):
                        agency_names[agency_id] = str(agency_name)
        except:
            agency_names = {}
        
        for agency_id, count in agency_counts.items():
            if pd.notna(agency_id):
                routes_by_agency[str(agency_id)] = {
                    "count": int(count),
                    "percentage": round((count / total_routes) * 100, 1),
                    "agency_name": agency_names.get(agency_id, 'N/A')
                }

    return {
        "routes_by_type": routes_by_type,
        "routes_by_agency": routes_by_agency,
    }


type_names = {
    # Types de base GTFS
    0: "Tramway",
    1: "Métro",
    2: "Train de banlieue",
    3: "Autobus",
    4: "Ferry",
    5: "Tramway urbain",
    6: "Téléphérique",
    7: "Funiculaire",
    11: "Trolleybus",
    12: "Monorail",

    # Types étendus GTFS
    100: "Train à grande vitesse",
    101: "Train interurbain",
    102: "Train régional",
    103: "Train suburbain",
    104: "Train de banlieue express",
    105: "Train local",
    106: "Train couchette",
    107: "Train touristique",
    108: "Train de navette",
    109: "Autre service ferroviaire",

    200: "Autocar express",
    201: "Autobus interurbain",
    202: "Navette aéroport",
    203: "Minibus",
    204: "Bus scolaire",
    205: "Bus de nuit",
    206: "Bus touristique",
    207: "Bus à la demande",
    208: "Bus régional",
    209: "Autre service de bus",

    300: "Métro régional",
    301: "Train léger sur rail (Light Rail)",
    302: "Train urbain automatique",

    400: "Tramway",
    401: "Tramway urbain",
    402: "Tramway express",
    403: "Tram-train",
    404: "Métro léger",
    405: "Tramway historique",
    406: "Tramway touristique",
    407: "Navette de centre-ville",
    408: "Tramway de montagne",
    409: "Autre type de tramway",
    410: "Tramway autonome",
    411: "Tramway à hydrogène",
    412: "Tramway à batterie",
    413: "Tramway hybride",
    414: "Tramway sur pneus",
    415: "Navette urbaine autonome",
    416: "Tramway régional",
    417: "Tramway à grande capacité",

    500: "Téléphérique urbain",
    
    600: "Funiculaire urbain",
    601: "Funiculaire touristique",
    602: "Funiculaire de montagne",
    603: "Ascenseur public",
    604: "Tapis roulant (mobilité douce)",
    605: "Téléphérique touristique",
    606: "Téléphérique de montagne",
    607: "Téléphérique à va-et-vient",

    700: "Bus à haut niveau de service (BHNS)",
    701: "Bus électrique",
    702: "Bus hybride",
    703: "Bus au gaz naturel",
    704: "Bus articulé",
    705: "Bus double étage",
    706: "Bus autonome",

    800: "Navette fluviale",
    
    900: "Trolleybus",
    
    1000: "Téléphérique / Remontée mécanique",
    1100: "Navette autonome",
    1200: "Train touristique",
    1300: "Bateau touristique",
    1400: "Vélo en libre-service",
    1500: "Taxi",
    1600: "Covoiturage",
    1700: "Véhicule autonome"
}