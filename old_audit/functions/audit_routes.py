"""
Fonctions d'audit pour le file_type: routes
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="routes",
    name="check_routes_required_columns",
    genre="validity",
    description="Vérifie la présence des colonnes obligatoires dans routes.txt.",
    parameters={}
)
def check_routes_required_columns(gtfs_data, **params):
    required_cols = {"route_id", "route_type", "route_short_name", "route_long_name"}
    
    df = gtfs_data.get('routes.txt')  # ← enlever .txt, cohérence
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier routes.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Vérifie la présence des colonnes obligatoires dans le fichier routes.txt selon la norme GTFS."
            },
            "recommendations": ["Fournir un fichier routes.txt valide contenant les colonnes obligatoires."]
        }
    
    # Analyse des colonnes présentes vs requises
    present_cols = set(df.columns)
    missing_cols = list(required_cols - present_cols)
    present_required = list(required_cols & present_cols)
    total_required = len(required_cols)
    present_count = len(present_required)
    missing_count = len(missing_cols)
    
    # Score de complétude
    completeness_score = (present_count / total_required) * 100
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if missing_count > 0:
        issues.append({
            "type": "missing_column",
            "field": "required_columns",
            "count": missing_count,
            "affected_ids": [],
            "details": missing_cols,
            "required_columns": list(required_cols),
            "message": f"{missing_count} colonnes obligatoires manquantes: {', '.join(missing_cols)}"
        })
    
    # Status basé sur la complétude
    if completeness_score == 100:
        status = "success"
    elif completeness_score >= 75:  # Au moins 3/4 des colonnes requises
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_required_columns": total_required,
            "present_columns": present_count,
            "missing_columns": missing_count,
            "completeness_score": round(completeness_score, 1),
            "column_analysis": {
                "present_required": present_required,
                "missing_required": missing_cols,
                "all_columns": list(present_cols)
            }
        },
        "explanation": {
            "purpose": "Vérifie la présence des colonnes obligatoires dans routes.txt selon la spécification GTFS.",
            "required_fields": {
                "route_id": "Identifiant unique de la ligne de transport",
                "route_type": "Type de transport (bus, métro, train, etc.)",
                "route_short_name": "Nom court de la ligne (numéro, code)",
                "route_long_name": "Nom complet descriptif de la ligne"
            },
            "compliance_status": f"Structure conforme à {completeness_score:.1f}% - {present_count}/{total_required} colonnes requises présentes",
            "validation_result": "Toutes les colonnes obligatoires sont présentes" if missing_count == 0 else f"Colonnes manquantes: {', '.join(missing_cols)}"
        },
        "recommendations": [
            rec for rec in [
                f"Ajouter les colonnes manquantes: {', '.join(missing_cols)} selon la spécification GTFS." if missing_count > 0 else None,
                "Consulter la documentation GTFS officielle pour le format exact des colonnes requises." if missing_count > 0 else None,
                "Vérifier que les colonnes présentes contiennent des données valides et non vides." if present_count > 0 and missing_count == 0 else None,
                "Considérer l'ajout de colonnes optionnelles (route_color, route_text_color, etc.) pour enrichir les données." if missing_count == 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="routes",
    name="check_route_id_uniqueness",
    genre="validity",
    description="Vérifie que les identifiants route_id sont uniques.",
    parameters={}
)
def check_route_id_uniqueness(gtfs_data, **params):
    df = gtfs_data.get('routes.txt')  # ← enlever .txt, cohérence
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier routes.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Vérifie l'unicité des identifiants de ligne (route_id) dans routes.txt."
            },
            "recommendations": ["Fournir un fichier routes.txt valide."]
        }
    
    # Vérification de la présence de la colonne route_id
    if 'route_id' not in df.columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_column",
                "field": "route_id",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne route_id manquante"
            }],
            "result": {"total_routes": len(df), "unique_route_ids": 0, "duplicate_count": 0},
            "explanation": {"purpose": "Vérifie l'unicité des identifiants de ligne.", "validation_status": "Impossible de vérifier sans colonne route_id."},
            "recommendations": ["Ajouter la colonne route_id obligatoire dans routes.txt."]
        }
    
    # Analyse des doublons
    total_routes = len(df)
    duplicated_mask = df['route_id'].duplicated(keep=False)
    duplicated_rows = df[duplicated_mask]
    duplicated_ids = duplicated_rows['route_id'].unique().tolist()
    num_duplicate_ids = len(duplicated_ids)
    num_duplicate_rows = len(duplicated_rows)
    unique_route_ids = len(df['route_id'].unique())
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = 100 if num_duplicate_ids == 0 else compute_score(num_duplicate_ids, unique_route_ids)
    
    # Analyse détaillée des doublons
    duplicate_details = []
    if num_duplicate_ids > 0:
        for route_id in duplicated_ids:
            count = (df['route_id'] == route_id).sum()
            duplicate_details.append({
                'route_id': route_id,
                'occurrence_count': count,
                'row_indices': df[df['route_id'] == route_id].index.tolist()
            })
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if num_duplicate_ids > 0:
        issues.append({
            "type": "duplicate_identifier",
            "field": "route_id",
            "count": num_duplicate_ids,
            "affected_ids": duplicated_ids,
            "details": duplicate_details,
            "total_duplicate_rows": num_duplicate_rows,
            "message": f"{num_duplicate_ids} route_id dupliqués affectant {num_duplicate_rows} lignes"
        })
    
    # Status basé sur la criticité (identifiants dupliqués = très grave)
    if num_duplicate_ids == 0:
        status = "success"
    else:
        status = "error"  # Toujours error car les IDs dupliqués cassent l'intégrité
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_routes": total_routes,
            "unique_route_ids": unique_route_ids,
            "duplicate_ids_count": num_duplicate_ids,
            "duplicate_rows_count": num_duplicate_rows,
            "uniqueness_score": score,
            "integrity_analysis": {
                "uniqueness_rate": f"{unique_route_ids}/{total_routes} identifiants uniques",
                "duplication_impact": f"{num_duplicate_rows} lignes affectées par la duplication" if num_duplicate_ids > 0 else "Aucune duplication détectée"
            }
        },
        "explanation": {
            "purpose": "Vérifie que chaque ligne de transport possède un identifiant unique (route_id) selon la norme GTFS.",
            "uniqueness_requirement": "Les route_id doivent être uniques pour éviter les conflits de référence dans le GTFS.",
            "validation_result": f"Tous les route_id sont uniques" if num_duplicate_ids == 0 else f"Doublons détectés: {', '.join(duplicated_ids)}",
            "data_integrity": f"Intégrité des données: {'Conforme' if num_duplicate_ids == 0 else 'Compromise'}",
            "impact_assessment": "Les identifiants dupliqués peuvent causer des erreurs de référence dans d'autres fichiers GTFS" if num_duplicate_ids > 0 else "Structure d'identifiants conforme"
        },
        "recommendations": [
            rec for rec in [
                f"Corriger immédiatement les {num_duplicate_ids} route_id dupliqués pour garantir l'intégrité des données." if num_duplicate_ids > 0 else None,
                f"Examiner les {num_duplicate_rows} lignes concernées pour identifier les erreurs de saisie ou d'import." if num_duplicate_rows > 0 else None,
                "Mettre en place des contraintes d'unicité lors de la création des données routes." if num_duplicate_ids > 0 else None,
                "Vérifier que les doublons ne proviennent pas d'une fusion incorrecte de fichiers." if num_duplicate_ids > 0 else None,
                "Valider l'intégrité référentielle avec les autres fichiers GTFS après correction." if num_duplicate_ids > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="routes",
    name="check_route_type_validity",
    genre="validity",
    description="Vérifie que les valeurs de route_type sont conformes à la liste officielle GTFS (0-11).",
    parameters={}
)
def check_route_type_validity(gtfs_data, **params):
    df = gtfs_data.get('routes.txt')  # ← enlever .txt, cohérence
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier routes.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Vérifie que les types de transport (route_type) respectent la norme GTFS."
            },
            "recommendations": ["Fournir un fichier routes.txt valide."]
        }
    
    # Définition des types valides selon GTFS avec leurs descriptions
    valid_types = {
        0: "Tramway, streetcar, light rail",
        1: "Métro, subway",
        2: "Rail (intercity/long-distance)",
        3: "Bus",
        4: "Ferry",
        5: "Cable tram",
        6: "Aerial lift (gondola, funicular)",
        7: "Funicular",
        11: "Trolleybus",
        12: "Monorail"
    }
    valid_type_codes = set(valid_types.keys())
    
    # Vérification de la présence de la colonne
    if 'route_type' not in df.columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_column",
                "field": "route_type",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne route_type obligatoire manquante"
            }],
            "result": {
                "total_routes": len(df),
                "valid_types": 0,
                "invalid_types": 0,
                "type_distribution": {}
            },
            "explanation": {
                "purpose": "Vérifie que les types de transport respectent la norme GTFS.",
                "validation_status": "Impossible de vérifier sans colonne route_type."
            },
            "recommendations": ["Ajouter la colonne route_type obligatoire selon la spécification GTFS."]
        }
    
    total_routes = len(df)
    
    # Analyse des types valides/invalides
    invalid_mask = ~df['route_type'].isin(valid_type_codes)
    invalid_routes = df[invalid_mask]
    valid_routes = df[~invalid_mask]
    
    count_invalid = len(invalid_routes)
    count_valid = len(valid_routes)
    
    # Récupération des IDs problématiques
    invalid_ids = invalid_routes['route_id'].tolist() if 'route_id' in invalid_routes.columns else invalid_routes.index.tolist()
    
    # Distribution des types utilisés
    type_distribution = df['route_type'].value_counts().to_dict()
    
    # Analyse des valeurs invalides spécifiques
    invalid_values = invalid_routes['route_type'].value_counts().to_dict()
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = 100 if count_invalid == 0 else compute_score(count_invalid, total_routes)
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if count_invalid > 0:
        issues.append({
            "type": "invalid_value",
            "field": "route_type",
            "count": count_invalid,
            "affected_ids": invalid_ids,
            "invalid_values": invalid_values,
            "valid_range": list(valid_type_codes),
            "message": f"{count_invalid} valeurs route_type invalides (hors plage 0-12)"
        })
    
    # Status basé sur la conformité
    if count_invalid == 0:
        status = "success"
    elif count_invalid / total_routes <= 0.05:  # ≤ 5% d'erreurs
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_routes": total_routes,
            "valid_routes": count_valid,
            "invalid_routes": count_invalid,
            "validity_score": score,
            "type_analysis": {
                "type_distribution": type_distribution,
                "used_types": {k: valid_types[k] for k in type_distribution.keys() if k in valid_types},
                "invalid_values": invalid_values,
                "compliance_rate": f"{count_valid}/{total_routes} routes conformes"
            }
        },
        "explanation": {
            "purpose": "Vérifie que tous les types de transport (route_type) utilisent les codes GTFS officiels.",
            "valid_types_reference": "Types valides: " + ", ".join([f"{k}={v}" for k, v in list(valid_types.items())[:5]]) + "...",
            "validation_result": f"Tous les route_type sont valides" if count_invalid == 0 else f"Valeurs invalides détectées: {list(invalid_values.keys())}",
            "distribution_overview": f"Types les plus utilisés: {dict(list(type_distribution.items())[:3])}" if type_distribution else "Aucune donnée valide",
            "compliance_status": f"Taux de conformité: {(count_valid/total_routes*100):.1f}%" if total_routes > 0 else "N/A"
        },
        "recommendations": [
            rec for rec in [
                f"Corriger les {count_invalid} valeurs route_type invalides selon la norme GTFS." if count_invalid > 0 else None,
                f"Remplacer les valeurs interdites: {', '.join(map(str, invalid_values.keys()))} par des codes valides (0-12)." if invalid_values else None,
                "Consulter la spécification GTFS pour choisir le type approprié à chaque mode de transport." if count_invalid > 0 else None,
                "Vérifier la cohérence entre route_type et les autres attributs des lignes (noms, couleurs, etc.)." if count_valid > 0 else None,
                f"Considérer l'utilisation de types plus spécifiques si toutes les routes utilisent le type {max(type_distribution, key=type_distribution.get) if type_distribution else 'N/A'}." if len(type_distribution) == 1 and total_routes > 5 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="routes",
    name="validate_route_colors",
    genre="quality",
    description="Vérifie le format hexadécimal des couleurs route_color et route_text_color.",
    parameters={}
)
def validate_route_colors(gtfs_data, **params):
    df = gtfs_data.get('routes.txt')  # ← enlever .txt, cohérence
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier routes.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Valide le format hexadécimal des couleurs de ligne et de texte."
            },
            "recommendations": ["Fournir un fichier routes.txt valide."]
        }
    
    total = len(df)
    if total == 0:
        return {
            "status": "success",  # ← ok → success
            "issues": [],
            "result": {"total_routes": 0, "color_analysis": {}},
            "explanation": {
                "purpose": "Valide le format hexadécimal des couleurs de ligne et de texte.",
                "data_status": "Le fichier routes.txt est vide."
            },
            "recommendations": []
        }

    # Utiliser ta fonction _hex_color_pattern depuis __init__.py
    hex_pattern = hex_color_pattern()
    
    # Analyse des couleurs de ligne (route_color)
    invalid_route_color = []
    route_color_stats = {"present": 0, "valid": 0, "invalid": 0, "missing": 0}
    
    if 'route_color' in df.columns:
        route_color_data = df['route_color']
        route_color_stats["present"] = (route_color_data.notna()).sum()
        route_color_stats["missing"] = (route_color_data.isna()).sum()
        
        invalid_route_color = df[
            (route_color_data.notna()) & (~route_color_data.str.match(hex_pattern, na=False))
        ]
        route_color_stats["invalid"] = len(invalid_route_color)
        route_color_stats["valid"] = route_color_stats["present"] - route_color_stats["invalid"]
    else:
        route_color_stats["missing"] = total

    # Analyse des couleurs de texte (route_text_color)
    invalid_text_color = []
    text_color_stats = {"present": 0, "valid": 0, "invalid": 0, "missing": 0}
    
    if 'route_text_color' in df.columns:
        text_color_data = df['route_text_color']
        text_color_stats["present"] = (text_color_data.notna()).sum()
        text_color_stats["missing"] = (text_color_data.isna()).sum()
        
        invalid_text_color = df[
            (text_color_data.notna()) & (~text_color_data.str.match(hex_pattern, na=False))
        ]
        text_color_stats["invalid"] = len(invalid_text_color)
        text_color_stats["valid"] = text_color_stats["present"] - text_color_stats["invalid"]
    else:
        text_color_stats["missing"] = total

    # Collecte des IDs problématiques
    invalid_route_color_ids = invalid_route_color['route_id'].tolist() if 'route_id' in df.columns else []
    invalid_text_color_ids = invalid_text_color['route_id'].tolist() if 'route_id' in df.columns else []
    all_problem_ids = list(set(invalid_route_color_ids + invalid_text_color_ids))
    
    total_invalid = len(all_problem_ids)
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = compute_score(total_invalid, total)
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if len(invalid_route_color) > 0:
        issues.append({
            "type": "invalid_format",
            "field": "route_color",
            "count": len(invalid_route_color),
            "affected_ids": invalid_route_color_ids,
            "format_requirement": "Format hexadécimal 6 caractères (ex: FF0000)",
            "message": f"{len(invalid_route_color)} couleurs de ligne invalides"
        })
    
    if len(invalid_text_color) > 0:
        issues.append({
            "type": "invalid_format",
            "field": "route_text_color", 
            "count": len(invalid_text_color),
            "affected_ids": invalid_text_color_ids,
            "format_requirement": "Format hexadécimal 6 caractères (ex: FFFFFF)",
            "message": f"{len(invalid_text_color)} couleurs de texte invalides"
        })
    
    # Status basé sur la conformité
    if total_invalid == 0:
        status = "success"
    elif total_invalid / total <= 0.1:  # ≤ 10% d'erreurs
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_routes": total,
            "color_validation_score": score,
            "route_color_analysis": route_color_stats,
            "text_color_analysis": text_color_stats,
            "overall_summary": {
                "total_invalid_colors": total_invalid,
                "routes_with_valid_colors": total - total_invalid,
                "color_compliance_rate": f"{total - total_invalid}/{total} routes conformes"
            }
        },
        "explanation": {
            "purpose": "Valide que les couleurs de ligne et de texte respectent le format hexadécimal GTFS (6 caractères).",
            "format_specification": "Format requis: RRGGBB en hexadécimal (ex: FF0000=rouge, 00FF00=vert, 0000FF=bleu)",
            "validation_scope": f"Vérification de route_color et route_text_color sur {total} lignes",
            "color_usage": f"Couleurs définies: {route_color_stats['present']} lignes avec route_color, {text_color_stats['present']} avec route_text_color",
            "quality_assessment": f"Score de conformité: {score}/100"
        },
        "recommendations": [
            rec for rec in [
                f"Corriger les {len(invalid_route_color)} couleurs de ligne invalides au format hexadécimal 6 caractères." if len(invalid_route_color) > 0 else None,
                f"Corriger les {len(invalid_text_color)} couleurs de texte invalides au format hexadécimal 6 caractères." if len(invalid_text_color) > 0 else None,
                "Utiliser des couleurs contrastées entre route_color et route_text_color pour l'accessibilité." if route_color_stats["valid"] > 0 and text_color_stats["valid"] > 0 else None,
                f"Considérer l'ajout de couleurs pour les {route_color_stats['missing']} lignes sans route_color définie." if route_color_stats["missing"] > 0 and route_color_stats["missing"] < total else None,
                "Vérifier que les couleurs choisies respectent l'identité visuelle du réseau de transport." if total_invalid == 0 else None,
                "Tester l'affichage des couleurs sur différents supports (cartes, applications, etc.)." if total_invalid == 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="routes",
    name="check_routes_name_completeness",
    genre="completeness",
    description="Vérifie que chaque route a au moins un nom court ou long renseigné.",
    parameters={}
)
def check_routes_name_completeness(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data:
        return {
            "status": "error",
            "score": 0,
            "issues": ["Fichier routes.txt manquant."],
            "summary": {},
            "problem_ids": [],
            "explanation": "Impossible de vérifier la complétude des noms sans routes.txt.",
            "recommendations": ["Fournir un fichier routes.txt valide."]
        }
    df = gtfs_data['routes.txt']
    total = len(df)
    missing_names = df[(df['route_short_name'].fillna('') == '') & (df['route_long_name'].fillna('') == '')]
    missing_ids = missing_names['route_id'].tolist()
    missing_count = len(missing_ids)
    completeness_pct = 100 * (total - missing_count) / total if total > 0 else 0

    status = "ok" if missing_count == 0 else "warning"
    issues = [f"{missing_count} route(s) sans nom court ni long."] if missing_count > 0 else []

    explanation = (
        f"{completeness_pct:.1f}% des routes possèdent au moins un nom (court ou long)."
        if total > 0 else "routes.txt est vide."
    )
    recommendations = []
    if missing_count > 0:
        recommendations.append("Saisir au moins route_short_name ou route_long_name pour toutes les routes.")

    return {
        "status": status,
        "score": round(completeness_pct, 1),
        "issues": issues,
        "summary": {
            "total_routes": total,
            "routes_missing_names_count": missing_count,
            "completeness_pct": round(completeness_pct, 1)
        },
        "problem_ids": missing_ids,
        "explanation": explanation,
        "recommendations": recommendations
    }

@audit_function(
    file_type="routes",
    name="check_duplicate_route_names",
    genre="quality",
    description="Détecte les noms courts et longs dupliqués dans routes.txt.",
    parameters={}
)
def check_duplicate_route_names(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data:
        return {
            "status": "error",
            "score": 0,
            "issues": ["Fichier routes.txt manquant."],
            "summary": {},
            "problem_ids": [],
            "explanation": "Impossible de vérifier doublons noms sans routes.txt.",
            "recommendations": ["Fournir un fichier routes.txt valide."]
        }
    df = gtfs_data['routes.txt']

    short_dups = df['route_short_name'][df['route_short_name'].duplicated(keep=False)].dropna().unique()
    long_dups = df['route_long_name'][df['route_long_name'].duplicated(keep=False)].dropna().unique()

    problem_ids_short = df[df['route_short_name'].isin(short_dups)]['route_id'].tolist()
    problem_ids_long = df[df['route_long_name'].isin(long_dups)]['route_id'].tolist()

    total_dups = len(problem_ids_short) + len(problem_ids_long)
    total_routes = len(df)
    score = compute_score(total_dups, total_routes)
    status = "ok" if total_dups == 0 else "warning"

    issues = []
    if len(short_dups) > 0:
        issues.append(f"{len(short_dups)} nom(s) court(s) dupliqué(s).")
    if len(long_dups) > 0:
        issues.append(f"{len(long_dups)} nom(s) long(s) dupliqué(s).")

    explanation = (
        "Des noms de routes (court ou long) apparaissent plusieurs fois." if total_dups > 0
        else "Aucun doublon détecté dans les noms."
    )
    recommendations = []
    if total_dups > 0:
        recommendations.append("Assurer l'unicité des noms courts et longs pour éviter confusions.")

    return {
        "status": status,
        "score": score,
        "issues": issues,
        "summary": {
            "duplicate_short_name_count": len(short_dups),
            "duplicate_long_name_count": len(long_dups)
        },
        "problem_ids": list(set(problem_ids_short + problem_ids_long)),
        "explanation": explanation,
        "recommendations": recommendations
    }

@audit_function(
    file_type="routes",
    name="routes_count_by_type",
    genre='statistics',
    description="Nombre total de routes par type de transport."
)
def routes_count_by_type(gtfs_data, **params):
    """
    Analyse la répartition des lignes par type de transport selon la classification GTFS
    """
    df = gtfs_data.get('routes.txt')
    if df is None:
        return {
            "status": "error",  # ← ERROR car fichier obligatoire
            "issues": [{
                "type": "missing_file",
                "field": "routes.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Le fichier routes.txt est obligatoire pour analyser les types de transport"
            }],
            "result": {None},  # ← None au lieu d'objet vide
            "explanation": {
                "purpose": "Analyse la répartition des lignes par type de transport selon la classification GTFS."
            },
            "recommendations": ["Fournir le fichier routes.txt obligatoire selon la spécification GTFS."]
        }
    
    # Définition complète des types GTFS (étendue)
    transport_types = {
        0: 'Tramway/LRT', 1: 'Métro', 2: 'Train', 3: 'Bus', 4: 'Ferry',
        5: 'Téléphérique', 6: 'Télécabine', 7: 'Funiculaire', 11: 'Trolleybus', 12: 'Monorail'
    }
    
    total = len(df)
    if total == 0:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "routes.txt", 
                "count": 0,
                "affected_ids": [],
                "message": "Le fichier routes.txt est vide"
            }],
            "result": {
                "total_routes": 0,
                "distribution_by_type": {},
                "type_diversity": 0
            },
            "explanation": {
                "purpose": "Analyse la répartition des lignes par type de transport selon la classification GTFS.",
                "context": "Fichier routes.txt vide - aucune ligne à analyser"
            },
            "recommendations": ["Ajouter des lignes dans routes.txt pour constituer votre réseau de transport."]
        }
    
    # Vérification colonne route_type obligatoire
    if 'route_type' not in df.columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "route_type",
                "count": total,
                "affected_ids": df['route_id'].tolist() if 'route_id' in df.columns else [],
                "message": "La colonne route_type est obligatoire dans routes.txt"
            }],
            "result": {
                "total_routes": total,
                "distribution_by_type": {},
                "type_diversity": 0
            },
            "explanation": {
                "purpose": "Analyse la répartition des lignes par type de transport selon la classification GTFS.",
                "context": "Colonne route_type obligatoire manquante",
                "impact": "Impossible de classifier les lignes de transport"
            },
            "recommendations": ["Ajouter la colonne route_type obligatoire selon la spécification GTFS."]
        }
    
    # Analyse optimisée de la distribution
    type_counts = df['route_type'].value_counts()
    null_count = df['route_type'].isna().sum()
    
    distribution_by_type = {}
    distribution_by_name = {}
    unknown_types = []
    
    for route_type, count in type_counts.items():
        if pd.isna(route_type):
            continue
        count = int(count)
        
        if route_type in transport_types:
            type_name = transport_types[route_type]
            distribution_by_type[route_type] = count
            distribution_by_name[type_name] = count
        else:
            unknown_types.append(route_type)
            distribution_by_type[route_type] = count
            distribution_by_name[f'Type inconnu ({route_type})'] = count
    
    # Métriques avancées optimisées
    type_diversity = len(distribution_by_type)
    valid_total = total - null_count
    # Calcul indice de diversité Shannon corrigé
    diversity_index = 0
    if valid_total > 0:
        import math
        for count in type_counts.values():
            if pd.isna(count) or count <= 0:
                continue
            proportion = count / valid_total
            diversity_index -= proportion * math.log2(proportion)
    print(diversity_index)
    # Analyse de concentration
    dominant_type = max(distribution_by_name.items(), key=lambda x: x[1]) if distribution_by_name else None
    concentration_rate = round(dominant_type[1] / valid_total * 100, 2) if dominant_type and valid_total > 0 else 0
    
    # Construction des issues
    issues = []
    
    if null_count > 0:
        null_ids = df.loc[df['route_type'].isna(), 'route_id'].tolist() if 'route_id' in df.columns else []
        issues.append({
            "type": "missing_data",
            "field": "route_type", 
            "count": null_count,
            "affected_ids": null_ids[:100],
            "message": f"{null_count} lignes ont un route_type manquant"
        })
    
    if unknown_types:
        unknown_ids = df.loc[df['route_type'].isin(unknown_types), 'route_id'].tolist() if 'route_id' in df.columns else []
        issues.append({
            "type": "invalid_format",
            "field": "route_type",
            "count": len(unknown_types),
            "affected_ids": unknown_ids[:100],
            "message": f"{len(unknown_types)} types non-standard GTFS détectés: {unknown_types[:5]}"
        })
    
    # Status intelligent
    error_rate = (null_count + len([t for t in unknown_types])) / total
    if error_rate == 0:
        status = "success"
    elif error_rate <= 0.05:  # ≤5% d'erreurs
        status = "warning" 
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_routes": total,
            "valid_routes": valid_total,
            "distribution_by_type": distribution_by_type,
            "distribution_by_name": distribution_by_name,
            "transport_analysis": {
                "type_diversity": type_diversity,
                "diversity_index": round(diversity_index, 3),
                "dominant_type": dominant_type[0] if dominant_type else None,
                "concentration_rate": concentration_rate,
                "network_balance": (
                    "équilibré" if concentration_rate < 60
                    else "concentré" if concentration_rate < 80  
                    else "mono-modal"
                )
            },
            "data_quality": {
                "valid_types": len([t for t in type_counts.index if t in transport_types]),
                "unknown_types": len(unknown_types),
                "missing_values": null_count,
                "compliance_rate": round((valid_total - len(unknown_types)) / total * 100, 2) if total > 0 else 0
            }
        },
        "explanation": {
            "purpose": "Analyse la diversité et répartition des lignes par type de transport selon la classification GTFS",
            "context": f"Analyse de {total} lignes avec {type_diversity} types de transport différents",
            "network_profile": f"Réseau {'diversifié' if type_diversity >= 4 else 'modéré' if type_diversity >= 2 else 'simple'} - Mode dominant: {dominant_type[0] if dominant_type else 'N/A'} ({concentration_rate}%)",
            "data_quality_summary": f"Conformité GTFS: {round((valid_total - len(unknown_types)) / total * 100, 1)}%" if total > 0 else "N/A",
            "impact": (
                f"Classification claire de {valid_total} lignes sur {type_diversity} modes de transport" if status == "success"
                else f"Problèmes de classification : {null_count} manquants, {len(unknown_types)} types invalides"
            )
        },
        "recommendations": [
            rec for rec in [
                f"URGENT: Corriger {null_count} route_type manquants" if null_count > 0 else None,
                f"Standardiser {len(unknown_types)} types non-GTFS: {unknown_types[:3]}" if unknown_types else None,
                f"Rééquilibrer le réseau si {dominant_type[0]} dépasse 80% des lignes" if concentration_rate > 80 else None,
                "Enrichir le réseau avec d'autres modes si pertinent" if type_diversity == 1 and total > 5 else None,
                f"Valider la pertinence de {len([c for c in distribution_by_name.values() if c == 1])} types à ligne unique" if len([c for c in distribution_by_name.values() if c == 1]) > 2 else None,
                "Documenter les spécificités de chaque mode pour l'exploitation" if type_diversity >= 3 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="routes",
    name="duplicate_routes_detection",
    genre='redondances',
    description="Détecte les routes ayant les mêmes paramètres (route_short_name, route_long_name, route_type).",
    parameters={}
)
def duplicate_routes_detection(gtfs_data, **params):
    routes = gtfs_data.get('routes.txt')  # ← Déjà sans .txt, OK
    if routes is None or routes.empty:
        return {
            "status": "error",
            "issues": ["Fichier routes.txt manquant ou vide."],
            "result": {},  # ← Nouvelle structure
            "explanation": {
                "purpose": "Détecte les lignes de transport ayant des caractéristiques identiques (potentiels doublons)."
            },
            "recommendations": ["Fournir un fichier routes.txt valide avec des données."]
        }

    # Champs clés pour la détection de doublons
    key_fields = ['route_short_name', 'route_long_name', 'route_type']
    available_fields = [field for field in key_fields if field in routes.columns]
    
    if not available_fields:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_column",
                "field": "route_comparison_fields",
                "count": len(key_fields),
                "affected_ids": [],
                "message": "Colonnes nécessaires pour la détection de doublons manquantes"
            }],
            "result": {
                "total_routes": len(routes),
                "duplicate_groups": 0,
                "affected_routes": 0
            },
            "explanation": {
                "purpose": "Détecte les doublons de routes.",
                "validation_status": "Impossible de détecter sans les colonnes de comparaison."
            },
            "recommendations": ["Ajouter les colonnes route_short_name, route_long_name et route_type."]
        }

    total = len(routes)
    
    # Détection des groupes de doublons
    grouped = routes.groupby(available_fields, dropna=False)
    duplicate_groups = []
    duplicated_route_ids = []
    
    for group_key, group_df in grouped:
        if len(group_df) > 1:
            route_ids = group_df['route_id'].tolist() if 'route_id' in group_df.columns else group_df.index.tolist()
            duplicated_route_ids.extend(route_ids)
            
            # Détails du groupe de doublons
            group_details = {
                'group_characteristics': dict(zip(available_fields, group_key)),
                'route_count': len(group_df),
                'route_ids': route_ids,
                'routes_data': group_df.to_dict(orient='records')
            }
            duplicate_groups.append(group_details)
    
    duplicate_group_count = len(duplicate_groups)
    affected_routes_count = len(duplicated_route_ids)
    
    # Score basé sur le nombre de groupes et routes affectées
    if duplicate_group_count == 0:
        score = 100
    else:
        # Pénalité progressive selon l'impact
        penalty = min(50, duplicate_group_count * 5 + (affected_routes_count - duplicate_group_count) * 2)
        score = max(0, 100 - penalty)
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if duplicate_group_count > 0:
        issues.append({
            "type": "duplicate_entity",
            "field": "route_definition",
            "count": duplicate_group_count,
            "affected_ids": duplicated_route_ids,
            "details": duplicate_groups,
            "comparison_fields": available_fields,
            "message": f"{duplicate_group_count} groupes de routes dupliquées affectant {affected_routes_count} lignes"
        })
    
    # Status basé sur l'impact
    if duplicate_group_count == 0:
        status = "success"
    elif affected_routes_count / total <= 0.1:  # ≤ 10% de routes affectées
        status = "warning"
    else:
        status = "error"

    return {
        "status": status,
        "issues": issues,
        "result": {  # ← Nouvelle structure, enlever score/problem_ids
            "total_routes": total,
            "unique_routes": total - affected_routes_count,
            "duplicate_groups": duplicate_group_count,
            "affected_routes": affected_routes_count,
            "uniqueness_score": score,
            "duplication_analysis": {
                "comparison_fields": available_fields,
                "largest_group": max(duplicate_groups, key=lambda x: x['route_count']) if duplicate_groups else None,
                "duplication_rate": f"{affected_routes_count}/{total} routes affectées",
                "average_group_size": round(affected_routes_count / duplicate_group_count, 1) if duplicate_group_count > 0 else 0
            }
        },
        "explanation": {
            "purpose": "Identifie les lignes de transport ayant des caractéristiques identiques (nom court, nom long, type) qui pourraient être des doublons.",
            "detection_criteria": f"Comparaison basée sur: {', '.join(available_fields)}",
            "duplication_impact": "Les doublons de routes peuvent créer des confusions pour les utilisateurs et compliquer la maintenance des données",
            "analysis_summary": f"Détection sur {total} routes: {duplicate_group_count} groupes de doublons identifiés" if duplicate_group_count > 0 else f"Aucun doublon détecté parmi {total} routes",
            "quality_assessment": f"Score d'unicité: {score}/100"
        },
        "recommendations": [
            rec for rec in [
                f"Examiner et fusionner les {duplicate_group_count} groupes de routes dupliquées." if duplicate_group_count > 0 else None,
                f"Prioriser le groupe le plus important: {duplicate_groups[0]['route_count']} routes identiques" if duplicate_groups and max(g['route_count'] for g in duplicate_groups) > 2 else None,
                "Vérifier que les doublons ne correspondent pas à des lignes différentes avec des noms similaires." if duplicate_group_count > 0 else None,
                "Mettre en place des contrôles d'unicité lors de la création de nouvelles lignes." if duplicate_group_count > 0 else None,
                "Considérer l'ajout de suffixes directionnels ou géographiques pour différencier les lignes similaires." if duplicate_group_count > 0 else None,
                f"Nettoyer en priorité les {len([g for g in duplicate_groups if g['route_count'] > 3])} groupes avec plus de 3 routes." if len([g for g in duplicate_groups if g['route_count'] > 3]) > 0 else None
            ] if rec is not None
        ]
    }