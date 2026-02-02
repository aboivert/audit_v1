#A FAIRE
"""
Fonctions d'audit pour le file_type: stops
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="stops",
    name="stop_id_coordinate_variation",
    genre="redondances",
    description="Détecte les stop_id définis plusieurs fois avec des coordonnées géographiques différentes au-delà d'une tolérance.",
    parameters={
        "tolerance_meters": {
            "type": "float",
            "description": "Distance maximale tolérée entre coordonnées (en mètres).",
            "default": 10.0
        }
    }
)
def stop_id_coordinate_variation(gtfs_data, tolerance_meters=10.0, **params):
    """
    Analyse les variations de coordonnées pour chaque stop_id.
    Détecte les stop_id ayant des coordonnées inconsistantes entre différentes occurrences.
    """
    
    # Vérification de la présence du fichier
    if gtfs_data.get('stops.txt') is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant pour l'analyse des coordonnées"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser les variations de coordonnées GPS pour chaque stop_id afin de détecter les incohérences géographiques"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide avec les données d'arrêts"]
        }

    stops_df = gtfs_data.get('stops.txt')
    
    # Vérification des colonnes requises
    required_columns = ['stop_id', 'stop_lat', 'stop_lon']
    missing_columns = [col for col in required_columns if col not in stops_df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": ', '.join(missing_columns),
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes manquantes dans stops.txt: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser les variations de coordonnées GPS pour chaque stop_id",
                "missing_data": f"Impossible d'effectuer l'analyse sans les colonnes: {', '.join(missing_columns)}"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Analyse des variations par stop_id
    issues = []
    problematic_stops = []
    coordinate_variations = {}
    
    grouped = stops_df.groupby('stop_id')
    total_stops = len(grouped)
    stops_with_multiple_coords = 0
    
    for stop_id, group in grouped:
        # Filtrer les coordonnées valides
        valid_coords = group[['stop_lat', 'stop_lon']].dropna()
        
        if len(valid_coords) < 1:
            continue
            
        if len(valid_coords) == 1:
            continue
            
        stops_with_multiple_coords += 1
        coords_list = valid_coords.values
        max_distance = 0
        coord_pairs = []
        
        # Calculer toutes les distances entre paires de coordonnées
        for i in range(len(coords_list)):
            for j in range(i + 1, len(coords_list)):
                lat1, lon1 = coords_list[i]
                lat2, lon2 = coords_list[j]
                distance = calculate_distance(lat1, lon1, lat2, lon2)
                max_distance = max(max_distance, distance)
                coord_pairs.append({
                    'coord1': (lat1, lon1),
                    'coord2': (lat2, lon2),
                    'distance': distance
                })
        
        coordinate_variations[stop_id] = {
            'max_distance': max_distance,
            'coordinate_count': len(coords_list),
            'coord_pairs': coord_pairs[:5]  # Limiter à 5 paires pour performance
        }
        
        # Vérifier si la variation dépasse la tolérance
        if max_distance > tolerance_meters:
            problematic_stops.append({
                'stop_id': stop_id,
                'max_distance': round(max_distance, 2),
                'coordinate_count': len(coords_list)
            })

    # Création des issues
    if problematic_stops:
        # Limiter les IDs affectés à 100 pour performance
        affected_ids = [stop['stop_id'] for stop in problematic_stops[:100]]
        
        issues.append({
            "type": "coordinate_inconsistency",
            "field": "stop_lat, stop_lon",
            "count": len(problematic_stops),
            "affected_ids": affected_ids,
            "message": f"{len(problematic_stops)} stop_id(s) avec variations de coordonnées > {tolerance_meters}m"
        })

    # Détermination du status
    if not issues:
        status = "success"
    elif len(problematic_stops) / total_stops > 0.1:  # Plus de 10% problématiques
        status = "error"
    else:
        status = "warning"

    # Calcul des statistiques
    distances = [var['max_distance'] for var in coordinate_variations.values()]
    
    result = {
        "total_unique_stops": total_stops,
        "stops_with_multiple_coordinates": stops_with_multiple_coords,
        "stops_with_variations_over_tolerance": len(problematic_stops),
        "tolerance_meters": tolerance_meters,
        "statistics": {
            "max_variation_distance": round(max(distances), 2) if distances else 0,
            "avg_variation_distance": round(sum(distances) / len(distances), 2) if distances else 0,
            "variation_rate": round(len(problematic_stops) / total_stops * 100, 2)
        },
        "most_problematic_stops": sorted(problematic_stops, key=lambda x: x['max_distance'], reverse=True)[:10],
        "quality_assessment": (
            "excellent" if len(problematic_stops) == 0 else
            "good" if len(problematic_stops) / total_stops <= 0.02 else
            "fair" if len(problematic_stops) / total_stops <= 0.05 else
            "poor"
        )
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Harmoniser les coordonnées pour {len(problematic_stops)} stop_id(s) avec variations > {tolerance_meters}m" if len(problematic_stops) > 0 else None,
        f"Examiner en priorité le stop_id '{problematic_stops[0]['stop_id']}' avec une variation de {problematic_stops[0]['max_distance']}m" if problematic_stops else None,
        "Implémenter un processus de validation des coordonnées lors de la saisie des données" if len(problematic_stops) > total_stops * 0.05 else None,
        f"Considérer ajuster la tolérance (actuellement {tolerance_meters}m) selon le contexte opérationnel" if len(problematic_stops) > total_stops * 0.1 else None,
        "Documenter les raisons des variations de coordonnées légitimes (arrêts multiples, repositionnements)" if stops_with_multiple_coords > 0 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser les variations de coordonnées GPS pour chaque stop_id afin de détecter les incohérences géographiques",
            "methodology": f"Calcul des distances géodésiques entre toutes les paires de coordonnées par stop_id avec tolérance de {tolerance_meters}m",
            "interpretation": f"Les stop_id avec variations > {tolerance_meters}m peuvent indiquer des erreurs de saisie ou des besoins de consolidation"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }


@audit_function(
    file_type="stops",
    name="duplicate_stops_by_name_and_location",
    genre='redondances',
    description="Détecte les arrêts ayant le même nom et localisés très proche (doublons probables).",
    parameters={"distance_threshold_m": {"type": "float", "default": 10.0}}
)
def duplicate_stops_by_name_and_location(gtfs_data, distance_threshold_m=10.0, **params):
    """
    Détecte les arrêts potentiellement dupliqués basés sur le nom et la proximité géographique.
    Identifie les stops avec le même nom situés à moins de distance_threshold_m mètres.
    """
    
    stops_df = gtfs_data.get('stops.txt')
    
    if stops_df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecter les arrêts potentiellement dupliqués basés sur le nom et la proximité géographique"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    if stops_df.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt vide"
            }],
            "result": {
                "total_stops": 0,
                "valid_stops": 0,
                "duplicate_pairs": [],
                "analysis_performed": False
            },
            "explanation": {
                "purpose": "Détecter les arrêts dupliqués par nom et localisation",
                "issue": "Aucun arrêt à analyser"
            },
            "recommendations": ["Remplir le fichier stops.txt avec des données d'arrêts valides"]
        }

    # Vérification des colonnes requises
    required_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
    missing_columns = [col for col in required_columns if col not in stops_df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": ', '.join(missing_columns),
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes manquantes dans stops.txt: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecter les arrêts dupliqués par nom et localisation",
                "missing_data": f"Colonnes requises manquantes: {', '.join(missing_columns)}"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Filtrer les arrêts avec données complètes
    valid_stops = stops_df.dropna(subset=['stop_name', 'stop_lat', 'stop_lon']).copy()
    
    if valid_stops.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "invalid_data",
                "field": "stop_name, stop_lat, stop_lon",
                "count": len(stops_df),
                "affected_ids": [],
                "message": "Aucun arrêt avec nom et coordonnées valides"
            }],
            "result": {
                "total_stops": len(stops_df),
                "valid_stops": 0,
                "duplicate_pairs": [],
                "analysis_performed": False
            },
            "explanation": {
                "purpose": "Détecter les arrêts dupliqués par nom et localisation",
                "issue": "Tous les arrêts ont des données incomplètes (nom ou coordonnées manquantes)"
            },
            "recommendations": ["Compléter les données manquantes pour les arrêts (nom, latitude, longitude)"]
        }

    # Normaliser les noms pour améliorer la détection
    valid_stops['stop_name_normalized'] = valid_stops['stop_name'].str.strip().str.lower()
    
    # Grouper par nom normalisé pour optimiser la recherche
    duplicates = []
    duplicate_groups = []
    processed_pairs = set()
    
    name_groups = valid_stops.groupby('stop_name_normalized')
    
    for normalized_name, group in name_groups:
        if len(group) < 2:
            continue
            
        stops_in_group = group[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].values.tolist()
        group_duplicates = []
        
        # Comparer tous les arrêts dans ce groupe de nom
        for i in range(len(stops_in_group)):
            id1, name1, lat1, lon1 = stops_in_group[i]
            for j in range(i + 1, len(stops_in_group)):
                id2, name2, lat2, lon2 = stops_in_group[j]
                
                # Éviter les comparaisons déjà effectuées
                pair_key = tuple(sorted([id1, id2]))
                if pair_key in processed_pairs:
                    continue
                processed_pairs.add(pair_key)
                
                # Calculer la distance
                distance = calculate_distance(lat1, lon1, lat2, lon2)
                
                if distance <= distance_threshold_m:
                    duplicate_pair = {
                        "stop_id_1": id1,
                        "stop_id_2": id2,
                        "stop_name": name1,  # Nom original (non normalisé)
                        "distance_m": round(distance, 2)
                    }
                    duplicates.append(duplicate_pair)
                    group_duplicates.append(duplicate_pair)
        
        if group_duplicates:
            duplicate_groups.append({
                "normalized_name": normalized_name,
                "original_name": group['stop_name'].iloc[0],
                "duplicate_count": len(group_duplicates),
                "stop_ids_involved": list(set([d["stop_id_1"] for d in group_duplicates] + 
                                            [d["stop_id_2"] for d in group_duplicates]))
            })

    # Création des issues
    issues = []
    if duplicates:
        # Limiter les IDs affectés à 100 pour performance
        all_affected_ids = list(set([d["stop_id_1"] for d in duplicates] + 
                                   [d["stop_id_2"] for d in duplicates]))
        
        issues.append({
            "type": "duplicate_data",
            "field": "stop_name, stop_lat, stop_lon",
            "count": len(duplicates),
            "affected_ids": all_affected_ids[:100],
            "message": f"{len(duplicates)} paires d'arrêts dupliqués détectées (même nom, distance ≤ {distance_threshold_m}m)"
        })

    # Détermination du status
    total_stops = len(valid_stops)
    duplicate_rate = len(duplicates) / total_stops if total_stops > 0 else 0
    
    if not duplicates:
        status = "success"
    elif duplicate_rate > 0.1:  # Plus de 10% de doublons
        status = "error"
    else:
        status = "warning"

    # Calcul des statistiques
    distances = [d['distance_m'] for d in duplicates] if duplicates else []
    
    result = {
        "total_stops": len(stops_df),
        "valid_stops_analyzed": len(valid_stops),
        "duplicate_pairs_found": len(duplicates),
        "distance_threshold_m": distance_threshold_m,
        "duplicate_groups": len(duplicate_groups),
        "statistics": {
            "min_distance": round(min(distances), 2) if distances else 0,
            "max_distance": round(max(distances), 2) if distances else 0,
            "avg_distance": round(sum(distances) / len(distances), 2) if distances else 0,
            "duplicate_rate": round(duplicate_rate * 100, 2)
        },
        "duplicate_pairs": duplicates[:20],  # Limiter à 20 pour l'affichage
        "duplicate_groups_summary": duplicate_groups[:10],  # Top 10 groupes
        "quality_assessment": (
            "excellent" if len(duplicates) == 0 else
            "good" if duplicate_rate <= 0.02 else
            "fair" if duplicate_rate <= 0.05 else
            "poor"
        )
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Examiner et fusionner {len(duplicates)} paires d'arrêts potentiellement dupliqués" if len(duplicates) > 0 else None,
        f"Prioriser le groupe '{duplicate_groups[0]['original_name']}' avec {duplicate_groups[0]['duplicate_count']} doublons" if duplicate_groups else None,
        f"Considérer ajuster le seuil de distance (actuellement {distance_threshold_m}m) selon le contexte urbain" if len(duplicates) > total_stops * 0.1 else None,
        "Implémenter un processus de validation lors de la création d'arrêts pour éviter les doublons" if len(duplicates) > 5 else None,
        "Standardiser la nomenclature des noms d'arrêts pour améliorer la détection automatique" if len(duplicate_groups) > 3 else None,
        f"Vérifier la cohérence des {len(stops_df) - len(valid_stops)} arrêts avec données incomplètes" if len(valid_stops) < len(stops_df) else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Détecter les arrêts potentiellement dupliqués basés sur le nom et la proximité géographique",
            "methodology": f"Comparaison des arrêts avec noms identiques (normalisés) dans un rayon de {distance_threshold_m}m",
            "interpretation": f"Les paires détectées nécessitent une vérification manuelle pour confirmer s'il s'agit de vrais doublons"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="check_required_columns",
    genre="validity",
    description="Vérifie la présence des colonnes obligatoires dans stops.txt",
    parameters={}
)
def check_required_columns(gtfs_data, **params):
    """
    Vérifie la présence des colonnes obligatoires dans le fichier stops.txt.
    Contrôle que tous les champs requis par la spécification GTFS sont présents.
    """
    required_columns = {'stop_id', 'stop_name', 'stop_lat', 'stop_lon'}
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier la présence des colonnes obligatoires dans stops.txt selon la spécification GTFS"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt conforme aux spécifications GTFS"]
        }

    # Vérification des colonnes manquantes
    present_columns = set(df.columns)
    missing_columns = list(required_columns - present_columns)
    
    # Création des issues
    issues = []
    if missing_columns:
        issues.append({
            "type": "missing_field",
            "field": ', '.join(sorted(missing_columns)),
            "count": len(missing_columns),
            "affected_ids": [],
            "message": f"Colonnes obligatoires manquantes: {', '.join(sorted(missing_columns))}"
        })

    # Détermination du status
    status = "success" if not missing_columns else "error"

    # Résultats détaillés
    result = {
        "required_columns": sorted(list(required_columns)),
        "present_columns": sorted(list(present_columns)),
        "missing_columns": sorted(missing_columns),
        "all_required_present": len(missing_columns) == 0,
        "compliance_rate": round((len(required_columns) - len(missing_columns)) / len(required_columns) * 100, 2),
        "total_columns_in_file": len(present_columns),
        "validation_status": "compliant" if not missing_columns else "non_compliant"
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Ajouter les colonnes manquantes: {', '.join(sorted(missing_columns))}" if missing_columns else None,
        "Vérifier que les colonnes ajoutées respectent les formats GTFS spécifiés" if missing_columns else None,
        "Valider le contenu des colonnes obligatoires (valeurs non nulles, formats corrects)" if not missing_columns else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Vérifier la présence des colonnes obligatoires dans stops.txt selon la spécification GTFS",
            "required_fields": "stop_id (identifiant), stop_name (nom), stop_lat (latitude), stop_lon (longitude)",
            "compliance": "Toutes les colonnes requises sont présentes" if not missing_columns else f"{len(missing_columns)} colonnes manquantes sur {len(required_columns)} requises"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }


@audit_function(
    file_type="stops",
    name="check_duplicate_stop_id",
    genre="quality",
    description="Vérifie l’unicité des stop_id",
    parameters={}
)
def check_duplicate_stop_id(gtfs_data, **params):
    """
    Vérifie l'unicité des identifiants stop_id dans le fichier stops.txt.
    Détecte les doublons qui violent la contrainte d'unicité GTFS.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier l'unicité des identifiants stop_id selon la contrainte GTFS"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    # Vérification de la colonne stop_id
    if 'stop_id' not in df.columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "stop_id",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne stop_id manquante dans stops.txt"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier l'unicité des identifiants stop_id",
                "missing_data": "Colonne stop_id requise pour l'analyse"
            },
            "recommendations": ["Ajouter la colonne stop_id obligatoire"]
        }

    # Détection des doublons
    duplicates_mask = df['stop_id'].duplicated(keep=False)
    duplicate_stops = df[duplicates_mask]
    unique_duplicate_ids = duplicate_stops['stop_id'].unique().tolist()
    
    # Création des issues
    issues = []
    if unique_duplicate_ids:
        # Limiter les IDs affectés à 100 pour performance
        affected_ids = unique_duplicate_ids[:100]
        
        issues.append({
            "type": "duplicate_data",
            "field": "stop_id",
            "count": len(unique_duplicate_ids),
            "affected_ids": affected_ids,
            "message": f"{len(unique_duplicate_ids)} stop_id(s) dupliqués détectés (violation contrainte GTFS)"
        })

    # Détermination du status
    total_stops = len(df)
    duplicate_rate = len(unique_duplicate_ids) / total_stops if total_stops > 0 else 0
    
    if not unique_duplicate_ids:
        status = "success"
    elif duplicate_rate > 0.05:  # Plus de 5% de doublons = critique
        status = "error"
    else:
        status = "warning"

    # Analyse des patterns de duplication
    duplication_analysis = {}
    if unique_duplicate_ids:
        for stop_id in unique_duplicate_ids:
            occurrences = duplicate_stops[duplicate_stops['stop_id'] == stop_id]
            duplication_analysis[stop_id] = {
                'occurrence_count': len(occurrences),
                'row_indices': occurrences.index.tolist()[:10]  # Limiter à 10 indices
            }

    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "unique_stop_ids": len(df['stop_id'].unique()),
        "duplicate_stop_ids": len(unique_duplicate_ids),
        "total_duplicate_rows": len(duplicate_stops),
        "uniqueness_rate": round((1 - duplicate_rate) * 100, 2),
        "duplicate_rate": round(duplicate_rate * 100, 2),
        "most_duplicated": sorted(
            [(sid, duplication_analysis[sid]['occurrence_count']) 
             for sid in unique_duplicate_ids], 
            key=lambda x: x[1], reverse=True
        )[:10] if unique_duplicate_ids else [],
        "compliance_status": "compliant" if not unique_duplicate_ids else "violation",
        "data_integrity": "preserved" if not unique_duplicate_ids else "compromised"
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Corriger {len(unique_duplicate_ids)} stop_id(s) dupliqués pour respecter la contrainte GTFS" if unique_duplicate_ids else None,
        f"Examiner en priorité '{unique_duplicate_ids[0]}' avec {duplication_analysis[unique_duplicate_ids[0]]['occurrence_count']} occurrences" if unique_duplicate_ids else None,
        "Implémenter une validation d'unicité lors de la création/import des arrêts" if len(unique_duplicate_ids) > 5 else None,
        "Vérifier si les doublons correspondent à des erreurs de saisie ou des besoins légitimes" if unique_duplicate_ids else None,
        "Nettoyer la base de données pour éliminer les doublons avant publication GTFS" if duplicate_rate > 0.02 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Vérifier l'unicité des identifiants stop_id selon la contrainte GTFS obligatoire",
            "gtfs_requirement": "Chaque stop_id doit être unique dans le fichier stops.txt",
            "impact": "Les doublons compromettent l'intégrité référentielle et peuvent causer des erreurs"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }


@audit_function(
    file_type="stops",
    name="validate_stop_lat_lon",
    genre="validit",
    description="Valide les coordonnées GPS des stops (limites valides).",
    parameters={}
)
def validate_stop_lat_lon(gtfs_data, **params):
    """
    Valide les coordonnées géographiques des arrêts dans le fichier stops.txt.
    Vérifie que les latitudes et longitudes sont dans les plages valides.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valider les coordonnées géographiques des arrêts selon les limites terrestres"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    # Vérification des colonnes requises
    required_columns = ['stop_lat', 'stop_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": ', '.join(missing_columns),
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes manquantes: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valider les coordonnées géographiques des arrêts",
                "missing_data": f"Colonnes requises manquantes: {', '.join(missing_columns)}"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Validation des coordonnées
    issues = []
    coordinate_errors = []
    
    # Vérifier les valeurs manquantes
    missing_lat = df['stop_lat'].isna()
    missing_lon = df['stop_lon'].isna()
    missing_coords = missing_lat | missing_lon
    
    if missing_coords.any():
        missing_stop_ids = df[missing_coords]['stop_id'].dropna().tolist()[:100]
        coordinate_errors.extend([{
            "stop_id": stop_id,
            "error": "coordinate_missing",
            "details": "Latitude ou longitude manquante"
        } for stop_id in missing_stop_ids])

    # Vérifier les plages valides pour les coordonnées non manquantes
    valid_coords = df[~missing_coords].copy()
    
    if not valid_coords.empty:
        # Latitude hors limites (-90 à +90)
        invalid_lat = (valid_coords['stop_lat'] < -90) | (valid_coords['stop_lat'] > 90)
        if invalid_lat.any():
            invalid_lat_stops = valid_coords[invalid_lat]['stop_id'].dropna().tolist()[:100]
            coordinate_errors.extend([{
                "stop_id": stop_id,
                "error": "latitude_out_of_range",
                "details": "Latitude hors limites (-90° à +90°)"
            } for stop_id in invalid_lat_stops])
        
        # Longitude hors limites (-180 à +180)
        invalid_lon = (valid_coords['stop_lon'] < -180) | (valid_coords['stop_lon'] > 180)
        if invalid_lon.any():
            invalid_lon_stops = valid_coords[invalid_lon]['stop_id'].dropna().tolist()[:100]
            coordinate_errors.extend([{
                "stop_id": stop_id,
                "error": "longitude_out_of_range",
                "details": "Longitude hors limites (-180° à +180°)"
            } for stop_id in invalid_lon_stops])
        
        # Coordonnées exactement (0, 0) - souvent une erreur
        zero_coords = (valid_coords['stop_lat'] == 0) & (valid_coords['stop_lon'] == 0)
        if zero_coords.any():
            zero_coord_stops = valid_coords[zero_coords]['stop_id'].dropna().tolist()[:100]
            coordinate_errors.extend([{
                "stop_id": stop_id,
                "error": "suspicious_zero_coordinates",
                "details": "Coordonnées (0,0) suspectes"
            } for stop_id in zero_coord_stops])

    # Création des issues par type d'erreur
    error_types = {}
    for error in coordinate_errors:
        error_type = error["error"]
        if error_type not in error_types:
            error_types[error_type] = []
        error_types[error_type].append(error["stop_id"])

    for error_type, stop_ids in error_types.items():
        issues.append({
            "type": "invalid_format",
            "field": "stop_lat, stop_lon",
            "count": len(stop_ids),
            "affected_ids": stop_ids[:100],
            "message": f"{len(stop_ids)} arrêts avec {error_type.replace('_', ' ')}"
        })

    # Détermination du status
    total_stops = len(df)
    total_errors = len(coordinate_errors)
    error_rate = total_errors / total_stops if total_stops > 0 else 0
    
    if total_errors == 0:
        status = "success"
    elif error_rate > 0.1:  # Plus de 10% d'erreurs
        status = "error"
    else:
        status = "warning"

    # Calcul des statistiques
    valid_stops = df[~missing_coords & 
                    (df['stop_lat'].between(-90, 90)) & 
                    (df['stop_lon'].between(-180, 180))]
    
    result = {
        "total_stops": total_stops,
        "valid_coordinates": len(valid_stops),
        "invalid_coordinates": total_errors,
        "missing_coordinates": missing_coords.sum(),
        "coordinates_validation": {
            "completeness_rate": round((1 - missing_coords.sum() / total_stops) * 100, 2),
            "validity_rate": round((len(valid_stops) / total_stops) * 100, 2),
            "error_rate": round(error_rate * 100, 2)
        },
        "coordinate_ranges": {
            "latitude_min": float(valid_stops['stop_lat'].min()) if not valid_stops.empty else None,
            "latitude_max": float(valid_stops['stop_lat'].max()) if not valid_stops.empty else None,
            "longitude_min": float(valid_stops['stop_lon'].min()) if not valid_stops.empty else None,
            "longitude_max": float(valid_stops['stop_lon'].max()) if not valid_stops.empty else None
        },
        "error_breakdown": {error_type: len(stop_ids) for error_type, stop_ids in error_types.items()},
        "quality_assessment": (
            "excellent" if total_errors == 0 else
            "good" if error_rate <= 0.02 else
            "fair" if error_rate <= 0.05 else
            "poor"
        )
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Corriger {total_errors} coordonnées invalides pour assurer la géolocalisation" if total_errors > 0 else None,
        f"Priorité: résoudre {missing_coords.sum()} coordonnées manquantes" if missing_coords.sum() > 0 else None,
        f"Vérifier les {error_types.get('latitude_out_of_range', [])} latitudes hors limites (-90° à +90°)" if 'latitude_out_of_range' in error_types else None,
        f"Vérifier les {error_types.get('longitude_out_of_range', [])} longitudes hors limites (-180° à +180°)" if 'longitude_out_of_range' in error_types else None,
        f"Examiner les {error_types.get('suspicious_zero_coordinates', [])} coordonnées (0,0) potentiellement erronées" if 'suspicious_zero_coordinates' in error_types else None,
        "Implémenter une validation des coordonnées lors de la saisie des données" if error_rate > 0.05 else None,
        "Utiliser un système de géocodage pour valider et corriger les coordonnées" if total_errors > 10 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Valider les coordonnées géographiques des arrêts selon les limites terrestres standard",
            "validation_rules": "Latitude: -90° à +90°, Longitude: -180° à +180°, valeurs non nulles",
            "impact": "Des coordonnées invalides empêchent la géolocalisation correcte des arrêts"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="stops_count_by_type",
    genre="statistics",
    description="Nombre total d'arrêts par type de localisation"
)
def stops_count_by_type(gtfs_data, **params):
    """
    Analyse la distribution des arrêts par type selon la classification GTFS.
    Fournit des statistiques sur les types d'arrêts et leur géolocalisation.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la distribution des arrêts par type selon la classification GTFS"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    if df.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt vide"
            }],
            "result": {
                "total_stops": 0,
                "distribution_by_type": {},
                "coordinate_analysis": {}
            },
            "explanation": {
                "purpose": "Analyser la distribution des arrêts par type",
                "issue": "Aucun arrêt à analyser"
            },
            "recommendations": ["Ajouter des données d'arrêts dans le fichier stops.txt"]
        }

    # Définition des types GTFS
    location_types = {
        0: 'Arrêts/Quais',
        1: 'Stations',
        2: 'Entrées/Sorties',
        3: 'Nœuds génériques',
        4: 'Aires d\'embarquement'
    }

    total_stops = len(df)
    df_analysis = df.copy()
    
    # Gestion du champ location_type (optionnel dans GTFS)
    if 'location_type' in df_analysis.columns:
        df_analysis['location_type'] = df_analysis['location_type'].fillna(0)
        has_location_type_column = True
    else:
        df_analysis['location_type'] = 0  # Par défaut = Arrêts/Quais
        has_location_type_column = False

    # Distribution par type
    type_counts = df_analysis['location_type'].value_counts().sort_index()
    distribution_by_type = {}
    
    for loc_type, count in type_counts.items():
        type_name = location_types.get(int(loc_type), f'Type non standard {int(loc_type)}')
        distribution_by_type[type_name] = {
            "count": int(count),
            "percentage": round((count / total_stops) * 100, 2),
            "type_code": int(loc_type)
        }

    # Analyse des coordonnées
    coordinate_analysis = {}
    if 'stop_lat' in df.columns and 'stop_lon' in df.columns:
        valid_coords = df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())]
        coords_count = len(valid_coords)
        coordinate_analysis = {
            "stops_with_coordinates": coords_count,
            "stops_without_coordinates": total_stops - coords_count,
            "coordinate_completion_rate": round((coords_count / total_stops) * 100, 2),
            "coordinate_availability": "available"
        }
        
        # Analyse par type avec coordonnées
        coord_by_type = {}
        for loc_type, type_info in distribution_by_type.items():
            type_code = type_info["type_code"]
            type_stops = df_analysis[df_analysis['location_type'] == type_code]
            type_with_coords = type_stops[(type_stops['stop_lat'].notna()) & 
                                        (type_stops['stop_lon'].notna())]
            coord_by_type[loc_type] = {
                "total": len(type_stops),
                "with_coordinates": len(type_with_coords),
                "coordinate_rate": round((len(type_with_coords) / len(type_stops)) * 100, 2) if len(type_stops) > 0 else 0
            }
        coordinate_analysis["by_type"] = coord_by_type
    else:
        coordinate_analysis = {
            "coordinate_availability": "unavailable",
            "message": "Colonnes stop_lat/stop_lon manquantes"
        }

    # Détection des issues
    issues = []
    
    # Types non standard
    non_standard_types = [code for code in type_counts.index if code not in location_types]
    if non_standard_types:
        issues.append({
            "type": "invalid_format",
            "field": "location_type",
            "count": len(non_standard_types),
            "affected_ids": [],
            "message": f"Types non standard détectés: {non_standard_types}"
        })

    # Manque de diversité des types
    if len(distribution_by_type) == 1 and total_stops > 10:
        issues.append({
            "type": "data_quality",
            "field": "location_type",
            "count": 1,
            "affected_ids": [],
            "message": "Tous les arrêts ont le même type (manque de diversité)"
        })

    # Coordonnées manquantes
    if 'coordinate_analysis' in locals() and coordinate_analysis.get("coordinate_completion_rate", 0) < 90:
        missing_coords = coordinate_analysis.get("stops_without_coordinates", 0)
        issues.append({
            "type": "missing_data",
            "field": "stop_lat, stop_lon",
            "count": missing_coords,
            "affected_ids": [],
            "message": f"{missing_coords} arrêts sans coordonnées ({100 - coordinate_analysis.get('coordinate_completion_rate', 0):.1f}%)"
        })

    # Détermination du status
    if not issues:
        status = "success"
    elif any(issue["type"] in ["invalid_format", "missing_data"] for issue in issues):
        status = "warning"
    else:
        status = "success"

    # Résultats enrichis
    result = {
        "total_stops": total_stops,
        "distribution_by_type": distribution_by_type,
        "coordinate_analysis": coordinate_analysis,
        "type_diversity": {
            "unique_types": len(distribution_by_type),
            "has_location_type_column": has_location_type_column,
            "most_common_type": max(distribution_by_type.items(), key=lambda x: x[1]["count"])[0] if distribution_by_type else None,
            "type_distribution_quality": (
                "excellent" if len(distribution_by_type) >= 3 else
                "good" if len(distribution_by_type) == 2 else
                "basic"
            )
        },
        "gtfs_compliance": {
            "valid_location_types": all(code in location_types for code in type_counts.index),
            "uses_standard_types": has_location_type_column
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"Examiner les types non standard: {non_standard_types}" if non_standard_types else None,
        "Diversifier les types d'arrêts si approprié (stations, entrées, etc.)" if len(distribution_by_type) == 1 and total_stops > 10 else None,
        f"Compléter les coordonnées pour {coordinate_analysis.get('stops_without_coordinates', 0)} arrêts" if coordinate_analysis.get("coordinate_completion_rate", 100) < 90 else None,
        "Ajouter la colonne location_type pour une classification plus précise" if not has_location_type_column and total_stops > 50 else None,
        "Documenter la justification des types d'arrêts utilisés" if len(distribution_by_type) > 3 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser la distribution des arrêts par type selon la classification GTFS",
            "gtfs_types": "0=Arrêts/Quais, 1=Stations, 2=Entrées/Sorties, 3=Nœuds génériques, 4=Aires d'embarquement",
            "summary": f"{total_stops} arrêts répartis en {len(distribution_by_type)} type(s) différent(s)"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }



@audit_function(
    file_type="stops",
    name="stop_id_format_and_uniqueness",
    genre="validity",
    description="Vérifie le format des stop_id et leur unicité.",
    parameters={}
)
def stop_id_format_and_uniqueness(gtfs_data, **params):
    """
    Valide le format et l'unicité des identifiants stop_id dans le fichier stops.txt.
    Vérifie la conformité aux conventions GTFS et détecte les doublons.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valider le format et l'unicité des identifiants stop_id selon les conventions GTFS"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    # Vérification de la colonne stop_id
    if 'stop_id' not in df.columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "stop_id",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne stop_id manquante dans stops.txt"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valider les identifiants stop_id",
                "missing_data": "Colonne stop_id requise pour l'analyse"
            },
            "recommendations": ["Ajouter la colonne stop_id obligatoire"]
        }

    # Pattern de validation GTFS pour stop_id
    pattern = re.compile(r"^[a-zA-Z0-9_\-]+$")
    
    # Validation du format
    stop_ids_str = df['stop_id'].astype(str)
    invalid_format = df[~stop_ids_str.apply(lambda x: bool(pattern.match(x)))]
    
    # Validation de l'unicité
    duplicates = df[df['stop_id'].duplicated(keep=False)]
    unique_duplicate_ids = duplicates['stop_id'].unique().tolist()
    
    # Création des issues
    issues = []
    
    if not invalid_format.empty:
        # Limiter les IDs affectés à 100 pour performance
        invalid_ids = invalid_format['stop_id'].tolist()[:100]
        issues.append({
            "type": "invalid_format",
            "field": "stop_id",
            "count": len(invalid_format),
            "affected_ids": invalid_ids,
            "message": f"{len(invalid_format)} stop_id(s) avec format invalide (caractères non autorisés)"
        })

    if unique_duplicate_ids:
        # Limiter les IDs affectés à 100 pour performance
        duplicate_ids = unique_duplicate_ids[:100]
        issues.append({
            "type": "duplicate_data",
            "field": "stop_id",
            "count": len(unique_duplicate_ids),
            "affected_ids": duplicate_ids,
            "message": f"{len(unique_duplicate_ids)} stop_id(s) dupliqués (violation contrainte unicité)"
        })

    # Détermination du status
    total_stops = len(df)
    total_problems = len(invalid_format) + len(unique_duplicate_ids)
    problem_rate = total_problems / total_stops if total_stops > 0 else 0
    
    if not issues:
        status = "success"
    elif problem_rate > 0.05:  # Plus de 5% de problèmes = critique
        status = "error"
    else:
        status = "warning"

    # Analyse des patterns d'erreurs
    format_analysis = {}
    if not invalid_format.empty:
        # Analyser les types d'erreurs de format
        invalid_chars = set()
        for stop_id in invalid_format['stop_id'].astype(str):
            for char in stop_id:
                if not re.match(r"[a-zA-Z0-9_\-]", char):
                    invalid_chars.add(char)
        
        format_analysis = {
            "invalid_characters_found": sorted(list(invalid_chars)),
            "common_issues": []
        }
        
        # Identifier les problèmes courants
        if ' ' in invalid_chars:
            format_analysis["common_issues"].append("Espaces détectés")
        if any(char in invalid_chars for char in ".,;:"):
            format_analysis["common_issues"].append("Ponctuation non autorisée")
        if any(char in invalid_chars for char in "éèàùç"):
            format_analysis["common_issues"].append("Caractères accentués")

    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "valid_stop_ids": total_stops - len(invalid_format) - len(duplicates),
        "format_validation": {
            "valid_format": len(df) - len(invalid_format),
            "invalid_format": len(invalid_format),
            "format_compliance_rate": round((1 - len(invalid_format) / total_stops) * 100, 2)
        },
        "uniqueness_validation": {
            "unique_stop_ids": len(df['stop_id'].unique()),
            "duplicate_stop_ids": len(unique_duplicate_ids),
            "total_duplicate_rows": len(duplicates),
            "uniqueness_rate": round((1 - len(unique_duplicate_ids) / total_stops) * 100, 2)
        },
        "overall_compliance": {
            "compliant_stop_ids": total_stops - total_problems,
            "compliance_rate": round((1 - problem_rate) * 100, 2),
            "gtfs_standard_met": total_problems == 0
        },
        "format_analysis": format_analysis,
        "quality_assessment": (
            "excellent" if total_problems == 0 else
            "good" if problem_rate <= 0.02 else
            "fair" if problem_rate <= 0.05 else
            "poor"
        )
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Corriger {len(invalid_format)} stop_id(s) avec format invalide" if not invalid_format.empty else None,
        f"URGENT: Résoudre {len(unique_duplicate_ids)} stop_id(s) dupliqués" if unique_duplicate_ids else None,
        f"Éliminer les caractères invalides: {', '.join(format_analysis.get('invalid_characters_found', []))}" if format_analysis.get('invalid_characters_found') else None,
        "Utiliser uniquement: lettres (a-z, A-Z), chiffres (0-9), tirets (-) et underscores (_)" if not invalid_format.empty else None,
        "Implémenter une validation des stop_id lors de la création des arrêts" if total_problems > 5 else None,
        "Nettoyer la base de données pour éliminer les doublons avant publication" if len(unique_duplicate_ids) > 2 else None,
        "Standardiser la nomenclature des identifiants d'arrêts" if problem_rate > 0.1 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Valider le format et l'unicité des identifiants stop_id selon les conventions GTFS",
            "format_rules": "Format autorisé: lettres, chiffres, tirets (-) et underscores (_) uniquement",
            "uniqueness_requirement": "Chaque stop_id doit être unique dans le fichier stops.txt"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }



@audit_function(
    file_type="stops",
    name="check_duplicate_stop_code",
    genre="quality",
    description="Détecte les doublons dans stop_code, s’il est renseigné.",
    parameters={}
)
def check_duplicate_stop_code(gtfs_data, **params):
    """
    Vérifie l'unicité des codes d'arrêts (stop_code) dans le fichier stops.txt.
    Détecte les doublons dans le champ optionnel stop_code utilisé pour l'affichage public.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier l'unicité des codes d'arrêts stop_code utilisés pour l'affichage public"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    # Vérification de la colonne stop_code (optionnelle dans GTFS)
    if 'stop_code' not in df.columns:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "stop_code_column_present": False,
                "analysis_performed": False,
                "message": "Colonne stop_code absente (champ optionnel GTFS)"
            },
            "explanation": {
                "purpose": "Vérifier l'unicité des codes d'arrêts stop_code",
                "gtfs_note": "Le champ stop_code est optionnel dans la spécification GTFS"
            },
            "recommendations": ["Considérer ajouter la colonne stop_code pour améliorer l'identification des arrêts par les usagers"]
        }

    # Analyse des stop_code
    total_stops = len(df)
    stop_codes = df['stop_code'].dropna()
    
    if stop_codes.empty:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "stop_code_column_present": True,
                "total_stops": total_stops,
                "stops_with_stop_code": 0,
                "completion_rate": 0.0,
                "analysis_performed": False,
                "message": "Colonne stop_code présente mais aucune valeur renseignée"
            },
            "explanation": {
                "purpose": "Vérifier l'unicité des codes d'arrêts stop_code",
                "observation": "Tous les stop_code sont vides ou null"
            },
            "recommendations": ["Renseigner les stop_code pour améliorer l'identification des arrêts"]
        }

    # Détection des doublons
    value_counts = stop_codes.value_counts()
    duplicates = value_counts[value_counts > 1]
    duplicate_codes = duplicates.index.tolist()
    
    # Création des issues
    issues = []
    if duplicate_codes:
        # Limiter les codes affectés à 100 pour performance
        affected_codes = duplicate_codes[:100]
        
        issues.append({
            "type": "duplicate_data",
            "field": "stop_code",
            "count": len(duplicate_codes),
            "affected_ids": affected_codes,
            "message": f"{len(duplicate_codes)} stop_code(s) dupliqués détectés"
        })

    # Détermination du status
    completion_rate = len(stop_codes) / total_stops
    duplicate_rate = len(duplicate_codes) / len(stop_codes) if len(stop_codes) > 0 else 0
    
    if not duplicate_codes:
        status = "success"
    elif duplicate_rate > 0.05:  # Plus de 5% de doublons
        status = "warning"  # Warning car stop_code est optionnel
    else:
        status = "success"

    # Analyse détaillée des doublons
    duplication_analysis = {}
    if duplicate_codes:
        for code in duplicate_codes:
            stops_with_code = df[df['stop_code'] == code]
            duplication_analysis[code] = {
                'occurrence_count': len(stops_with_code),
                'stop_ids': stops_with_code['stop_id'].tolist()[:10] if 'stop_id' in stops_with_code.columns else []
            }

    # Résultats détaillés
    result = {
        "stop_code_column_present": True,
        "total_stops": total_stops,
        "stops_with_stop_code": len(stop_codes),
        "unique_stop_codes": len(stop_codes.unique()),
        "duplicate_stop_codes": len(duplicate_codes),
        "completion_rate": round(completion_rate * 100, 2),
        "uniqueness_rate": round((1 - duplicate_rate) * 100, 2) if len(stop_codes) > 0 else 100,
        "most_duplicated": sorted(
            [(code, duplicates[code]) for code in duplicate_codes],
            key=lambda x: x[1], reverse=True
        )[:10] if duplicate_codes else [],
        "usage_statistics": {
            "avg_stop_code_length": round(stop_codes.astype(str).str.len().mean(), 1) if not stop_codes.empty else 0,
            "stop_code_patterns": "analysis_available" if not stop_codes.empty else "no_data"
        },
        "quality_assessment": (
            "excellent" if not duplicate_codes and completion_rate > 0.8 else
            "good" if not duplicate_codes and completion_rate > 0.5 else
            "fair" if duplicate_rate <= 0.02 else
            "poor"
        )
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"Corriger {len(duplicate_codes)} stop_code(s) dupliqués pour éviter la confusion" if duplicate_codes else None,
        f"Examiner en priorité le code '{duplicate_codes[0]}' utilisé {duplicates[duplicate_codes[0]]} fois" if duplicate_codes else None,
        f"Améliorer le taux de completion des stop_code ({completion_rate*100:.1f}% actuellement)" if completion_rate < 0.7 else None,
        "Implémenter une validation d'unicité lors de la saisie des stop_code" if len(duplicate_codes) > 3 else None,
        "Standardiser le format des stop_code pour améliorer la cohérence" if len(stop_codes.unique()) > 50 else None,
        "Les stop_code dupliqués peuvent créer de la confusion pour les usagers" if duplicate_codes else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Vérifier l'unicité des codes d'arrêts stop_code utilisés pour l'affichage public",
            "gtfs_note": "Le champ stop_code est optionnel mais doit être unique s'il est utilisé",
            "user_impact": "Les stop_code dupliqués peuvent créer de la confusion lors de la recherche d'arrêts"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }


@audit_function(
    file_type="stops",
    name="missing_values_stats_stops",
    genre="completeness",
    description="Calcule les valeurs manquantes par colonne dans stops.txt",
    parameters={}
)
def missing_values_stats_stops(gtfs_data, **params):
    """
    Analyse les valeurs manquantes dans le fichier stops.txt.
    Fournit des statistiques détaillées sur la complétude des données d'arrêts.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la complétude des données dans le fichier stops.txt"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    if df.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt vide"
            }],
            "result": {
                "total_stops": 0,
                "completeness_analysis": {}
            },
            "explanation": {
                "purpose": "Analyser la complétude des données d'arrêts",
                "issue": "Aucune donnée à analyser"
            },
            "recommendations": ["Ajouter des données d'arrêts dans le fichier stops.txt"]
        }

    # Classification des colonnes GTFS
    required_columns = {'stop_id', 'stop_name', 'stop_lat', 'stop_lon'}
    optional_columns = {'stop_code', 'stop_desc', 'zone_id', 'stop_url', 'location_type', 
                       'parent_station', 'stop_timezone', 'wheelchair_boarding', 
                       'level_id', 'platform_code'}
    
    total_stops = len(df)
    missing_counts = df.isna().sum().to_dict()
    missing_rates = {col: round((count / total_stops * 100), 2) 
                    for col, count in missing_counts.items()} if total_stops > 0 else {}
    
    # Analyse par catégorie de colonnes
    required_missing = {col: missing_counts.get(col, 0) for col in required_columns 
                       if col in df.columns}
    optional_missing = {col: missing_counts.get(col, 0) for col in optional_columns 
                       if col in df.columns}
    
    # Création des issues
    issues = []
    
    # Issues critiques pour colonnes obligatoires
    critical_missing = {col: count for col, count in required_missing.items() if count > 0}
    if critical_missing:
        issues.append({
            "type": "missing_data",
            "field": ', '.join(critical_missing.keys()),
            "count": sum(critical_missing.values()),
            "affected_ids": [],
            "message": f"Valeurs manquantes dans colonnes obligatoires: {', '.join(critical_missing.keys())}"
        })

    # Issues pour colonnes optionnelles avec fort taux de manquement
    high_missing_optional = {col: count for col, count in optional_missing.items() 
                           if count > 0 and (count / total_stops) > 0.5}
    if high_missing_optional:
        issues.append({
            "type": "data_quality",
            "field": ', '.join(high_missing_optional.keys()),
            "count": len(high_missing_optional),
            "affected_ids": [],
            "message": f"Colonnes optionnelles avec >50% de valeurs manquantes: {', '.join(high_missing_optional.keys())}"
        })

    # Détermination du status
    if critical_missing:
        status = "error"  # Colonnes obligatoires manquantes = critique
    elif high_missing_optional:
        status = "warning"  # Colonnes optionnelles très incomplètes
    else:
        status = "success"

    # Calcul des métriques de complétude
    total_missing = sum(missing_counts.values())
    total_cells = total_stops * len(df.columns)
    overall_completeness = round((1 - total_missing / total_cells) * 100, 2) if total_cells > 0 else 100
    
    # Analyse détaillée par colonne
    column_analysis = {}
    for col in df.columns:
        missing_count = missing_counts.get(col, 0)
        missing_rate = missing_rates.get(col, 0)
        
        column_analysis[col] = {
            "missing_count": missing_count,
            "missing_rate": missing_rate,
            "present_count": total_stops - missing_count,
            "completeness": round(100 - missing_rate, 2),
            "column_type": (
                "required" if col in required_columns else
                "optional" if col in optional_columns else
                "custom"
            ),
            "quality_level": (
                "excellent" if missing_rate == 0 else
                "good" if missing_rate <= 5 else
                "fair" if missing_rate <= 20 else
                "poor"
            )
        }

    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "total_columns": len(df.columns),
        "overall_completeness": overall_completeness,
        "completeness_by_category": {
            "required_columns": {
                "present_columns": len([col for col in required_columns if col in df.columns]),
                "missing_columns": len([col for col in required_columns if col not in df.columns]),
                "avg_completeness": round(sum(100 - missing_rates.get(col, 0) 
                                           for col in required_columns if col in df.columns) / 
                                       len([col for col in required_columns if col in df.columns]), 2) 
                                       if any(col in df.columns for col in required_columns) else 0
            },
            "optional_columns": {
                "present_columns": len([col for col in optional_columns if col in df.columns]),
                "avg_completeness": round(sum(100 - missing_rates.get(col, 0) 
                                           for col in optional_columns if col in df.columns) / 
                                       len([col for col in optional_columns if col in df.columns]), 2) 
                                       if any(col in df.columns for col in optional_columns) else 0
            }
        },
        "column_analysis": column_analysis,
        "most_incomplete_columns": sorted(
            [(col, missing_rates[col]) for col in df.columns if missing_rates[col] > 0],
            key=lambda x: x[1], reverse=True
        )[:10],
        "data_quality_summary": {
            "excellent_columns": len([col for col in df.columns if missing_rates.get(col, 0) == 0]),
            "good_columns": len([col for col in df.columns if 0 < missing_rates.get(col, 0) <= 5]),
            "fair_columns": len([col for col in df.columns if 5 < missing_rates.get(col, 0) <= 20]),
            "poor_columns": len([col for col in df.columns if missing_rates.get(col, 0) > 20])
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Compléter les valeurs manquantes dans les colonnes obligatoires: {', '.join(critical_missing.keys())}" if critical_missing else None,
        f"Améliorer la complétude de '{list(high_missing_optional.keys())[0]}' ({missing_rates.get(list(high_missing_optional.keys())[0], 0):.1f}% manquant)" if high_missing_optional else None,
        f"Considérer compléter les colonnes optionnelles pour enrichir les données (complétude globale: {overall_completeness}%)" if overall_completeness < 80 else None,
        "Implémenter des validations de saisie pour réduire les valeurs manquantes" if total_missing > total_stops else None,
        f"Examiner les colonnes avec >50% de valeurs manquantes: {', '.join(high_missing_optional.keys())}" if high_missing_optional else None,
        "Documenter les raisons des valeurs manquantes légitimes" if len([col for col in df.columns if missing_rates.get(col, 0) > 10]) > 3 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser la complétude des données dans le fichier stops.txt selon les standards GTFS",
            "methodology": "Classification des colonnes (obligatoires/optionnelles) et calcul des taux de complétude",
            "overall_assessment": f"Complétude globale de {overall_completeness}% sur {total_stops} arrêts"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }


@audit_function(
    file_type="stops",
    name="validate_wheelchair_boarding",
    genre="validity",
    description="Vérifie la validité du champ wheelchair_boarding.",
    parameters={}
)
def validate_wheelchair_boarding(gtfs_data, **params):
    """
    Valide les valeurs du champ wheelchair_boarding dans le fichier stops.txt.
    Vérifie la conformité aux valeurs GTFS autorisées (0, 1, 2).
    """
    
    # Valeurs autorisées selon GTFS (0=inconnu, 1=accessible, 2=non accessible)
    allowed_values = {0, 1, 2}
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valider les valeurs wheelchair_boarding selon la spécification GTFS"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    # Vérification de la colonne wheelchair_boarding (optionnelle dans GTFS)
    if 'wheelchair_boarding' not in df.columns:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "wheelchair_boarding_column_present": False,
                "analysis_performed": False,
                "accessibility_data_available": False
            },
            "explanation": {
                "purpose": "Valider les valeurs wheelchair_boarding selon GTFS",
                "gtfs_note": "Le champ wheelchair_boarding est optionnel dans la spécification GTFS"
            },
            "recommendations": ["Considérer ajouter le champ wheelchair_boarding pour améliorer l'accessibilité"]
        }

    total_stops = len(df)
    wheelchair_data = df['wheelchair_boarding'].dropna()
    
    # Si toutes les valeurs sont nulles
    if wheelchair_data.empty:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "wheelchair_boarding_column_present": True,
                "total_stops": total_stops,
                "stops_with_accessibility_info": 0,
                "completion_rate": 0.0,
                "analysis_performed": False
            },
            "explanation": {
                "purpose": "Valider les valeurs wheelchair_boarding",
                "observation": "Colonne présente mais aucune valeur renseignée"
            },
            "recommendations": ["Renseigner les informations d'accessibilité wheelchair_boarding"]
        }

    # Validation des valeurs
    invalid_mask = ~wheelchair_data.isin(allowed_values)
    invalid_values = wheelchair_data[invalid_mask].unique().tolist()
    invalid_stops = df[~df['wheelchair_boarding'].isin(allowed_values) & df['wheelchair_boarding'].notna()]
    
    # Création des issues
    issues = []
    if invalid_values:
        # Limiter les IDs affectés à 100 pour performance
        affected_ids = invalid_stops['stop_id'].tolist()[:100] if 'stop_id' in invalid_stops.columns else []
        
        issues.append({
            "type": "invalid_format",
            "field": "wheelchair_boarding",
            "count": len(invalid_stops),
            "affected_ids": affected_ids,
            "message": f"{len(invalid_stops)} arrêts avec valeurs wheelchair_boarding invalides: {invalid_values}"
        })

    # Détermination du status
    completion_rate = len(wheelchair_data) / total_stops
    error_rate = len(invalid_stops) / total_stops if total_stops > 0 else 0
    
    if not invalid_values:
        status = "success"
    elif error_rate > 0.05:  # Plus de 5% d'erreurs
        status = "error"
    else:
        status = "warning"

    # Analyse de la distribution des valeurs
    value_distribution = {}
    gtfs_labels = {0: "Inconnu", 1: "Accessible", 2: "Non accessible"}
    
    for value in allowed_values:
        count = (wheelchair_data == value).sum()
        value_distribution[f"{gtfs_labels[value]} ({value})"] = {
            "count": int(count),
            "percentage": round((count / len(wheelchair_data)) * 100, 2) if len(wheelchair_data) > 0 else 0
        }

    # Résultats détaillés
    result = {
        "wheelchair_boarding_column_present": True,
        "total_stops": total_stops,
        "stops_with_accessibility_info": len(wheelchair_data),
        "stops_with_invalid_values": len(invalid_stops),
        "completion_rate": round(completion_rate * 100, 2),
        "validity_rate": round((1 - error_rate) * 100, 2),
        "value_distribution": value_distribution,
        "invalid_values_found": invalid_values,
        "gtfs_compliance": {
            "all_values_valid": len(invalid_values) == 0,
            "allowed_values": list(allowed_values),
            "value_meanings": gtfs_labels
        },
        "accessibility_coverage": {
            "accessible_stops": int((wheelchair_data == 1).sum()),
            "non_accessible_stops": int((wheelchair_data == 2).sum()),
            "unknown_accessibility": int((wheelchair_data == 0).sum()),
            "coverage_quality": (
                "excellent" if completion_rate > 0.9 and len(invalid_values) == 0 else
                "good" if completion_rate > 0.7 and len(invalid_values) == 0 else
                "fair" if completion_rate > 0.5 else
                "poor"
            )
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Corriger {len(invalid_stops)} valeurs wheelchair_boarding invalides" if invalid_values else None,
        f"Valeurs autorisées uniquement: 0 (inconnu), 1 (accessible), 2 (non accessible)" if invalid_values else None,
        f"Améliorer le taux de completion des données d'accessibilité ({completion_rate*100:.1f}% actuellement)" if completion_rate < 0.8 else None,
        f"Réduire le nombre d'arrêts avec accessibilité 'inconnue' ({(wheelchair_data == 0).sum()} arrêts)" if (wheelchair_data == 0).sum() > len(wheelchair_data) * 0.5 else None,
        "Effectuer un audit terrain pour documenter l'accessibilité des arrêts" if completion_rate < 0.5 else None,
        "Mettre en place un processus de validation pour les nouvelles saisies" if len(invalid_values) > 0 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Valider les valeurs wheelchair_boarding selon la spécification GTFS",
            "gtfs_values": "0 = Accessibilité inconnue, 1 = Accessible, 2 = Non accessible",
            "importance": "Les données d'accessibilité sont cruciales pour les personnes à mobilité réduite"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="stops_accessibility_summary",
    genre="statistics",
    description="Résumé de l’accessibilité des arrêts basée sur wheelchair_boarding.",
    parameters={}
)
def stops_accessibility_summary(gtfs_data, **params):
    """
    Fournit un résumé complet de l'accessibilité des arrêts basé sur wheelchair_boarding.
    Analyse la couverture et la répartition des informations d'accessibilité.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la couverture et répartition des informations d'accessibilité des arrêts"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    total_stops = len(df)
    
    if total_stops == 0:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt vide"
            }],
            "result": {
                "total_stops": 0,
                "accessibility_summary": {}
            },
            "explanation": {
                "purpose": "Analyser l'accessibilité des arrêts",
                "issue": "Aucune donnée d'arrêt à analyser"
            },
            "recommendations": ["Ajouter des données d'arrêts dans le fichier stops.txt"]
        }

    # Vérification de la colonne wheelchair_boarding
    if 'wheelchair_boarding' not in df.columns:
        return {
            "status": "warning",
            "issues": [{
                "type": "missing_field",
                "field": "wheelchair_boarding",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne wheelchair_boarding manquante (données d'accessibilité indisponibles)"
            }],
            "result": {
                "total_stops": total_stops,
                "accessibility_data_available": False,
                "coverage_analysis": {
                    "message": "Aucune donnée d'accessibilité disponible"
                }
            },
            "explanation": {
                "purpose": "Analyser l'accessibilité des arrêts",
                "limitation": "Impossible d'analyser sans la colonne wheelchair_boarding"
            },
            "recommendations": [
                "Ajouter la colonne wheelchair_boarding pour documenter l'accessibilité",
                "Effectuer un audit d'accessibilité des arrêts"
            ]
        }

    # Analyse des données d'accessibilité
    wheelchair_data = df['wheelchair_boarding'].dropna()
    stops_with_info = len(wheelchair_data)
    
    # Comptage par catégorie
    unknown_accessibility = (wheelchair_data == 0).sum()
    accessible_stops = (wheelchair_data == 1).sum()
    not_accessible_stops = (wheelchair_data == 2).sum()
    invalid_values = wheelchair_data[~wheelchair_data.isin({0, 1, 2})].count()
    
    # Calcul des taux
    info_completion_rate = (stops_with_info / total_stops * 100) if total_stops > 0 else 0
    accessibility_rate = (accessible_stops / stops_with_info * 100) if stops_with_info > 0 else 0
    
    # Création des issues
    issues = []
    
    # Issue si faible couverture d'information
    if info_completion_rate < 70:
        issues.append({
            "type": "missing_data",
            "field": "wheelchair_boarding",
            "count": total_stops - stops_with_info,
            "affected_ids": [],
            "message": f"Faible couverture des données d'accessibilité ({info_completion_rate:.1f}%)"
        })
    
    # Issue si trop d'accessibilité inconnue
    if unknown_accessibility > stops_with_info * 0.5:
        issues.append({
            "type": "data_quality",
            "field": "wheelchair_boarding",
            "count": int(unknown_accessibility),
            "affected_ids": [],
            "message": f"Trop d'arrêts avec accessibilité 'inconnue' ({unknown_accessibility} sur {stops_with_info})"
        })
    
    # Issue si valeurs invalides
    if invalid_values > 0:
        issues.append({
            "type": "invalid_format",
            "field": "wheelchair_boarding",
            "count": int(invalid_values),
            "affected_ids": [],
            "message": f"{invalid_values} valeurs wheelchair_boarding invalides détectées"
        })

    # Détermination du status
    if info_completion_rate < 50:
        status = "error"  # Couverture très faible
    elif info_completion_rate < 70 or unknown_accessibility > stops_with_info * 0.5:
        status = "warning"  # Couverture insuffisante ou trop d'inconnus
    else:
        status = "success"

    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "accessibility_data_available": True,
        "coverage_analysis": {
            "stops_with_accessibility_info": int(stops_with_info),
            "stops_without_info": total_stops - stops_with_info,
            "info_completion_rate": round(info_completion_rate, 1),
            "coverage_quality": (
                "excellent" if info_completion_rate >= 90 else
                "good" if info_completion_rate >= 70 else
                "fair" if info_completion_rate >= 50 else
                "poor"
            )
        },
        "accessibility_breakdown": {
            "accessible_stops": int(accessible_stops),
            "not_accessible_stops": int(not_accessible_stops),
            "unknown_accessibility": int(unknown_accessibility),
            "invalid_values": int(invalid_values)
        },
        "accessibility_rates": {
            "accessibility_rate": round(accessibility_rate, 1),
            "non_accessibility_rate": round((not_accessible_stops / stops_with_info * 100), 1) if stops_with_info > 0 else 0,
            "unknown_rate": round((unknown_accessibility / stops_with_info * 100), 1) if stops_with_info > 0 else 0
        },
        "quality_indicators": {
            "data_completeness": "sufficient" if info_completion_rate >= 70 else "insufficient",
            "accessibility_documentation": "adequate" if unknown_accessibility <= stops_with_info * 0.3 else "needs_improvement",
            "overall_accessibility_score": round(
                (accessible_stops / total_stops * 100), 1
            ) if total_stops > 0 else 0
        },
        "distribution_analysis": {
            "most_common_status": (
                "accessible" if accessible_stops >= max(not_accessible_stops, unknown_accessibility) else
                "not_accessible" if not_accessible_stops >= unknown_accessibility else
                "unknown"
            ),
            "balance_assessment": (
                "well_documented" if unknown_accessibility <= stops_with_info * 0.2 else
                "partially_documented" if unknown_accessibility <= stops_with_info * 0.5 else
                "poorly_documented"
            )
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"Compléter les données d'accessibilité pour {total_stops - stops_with_info} arrêts ({100 - info_completion_rate:.1f}% manquants)" if info_completion_rate < 90 else None,
        f"Documenter l'accessibilité réelle de {unknown_accessibility} arrêts actuellement marqués comme 'inconnus'" if unknown_accessibility > stops_with_info * 0.3 else None,
        f"Effectuer un audit d'accessibilité terrain pour améliorer la documentation" if info_completion_rate < 70 else None,
        f"Corriger {invalid_values} valeurs wheelchair_boarding invalides" if invalid_values > 0 else None,
        f"Priorité: rendre accessibles les {not_accessible_stops} arrêts non accessibles si possible" if not_accessible_stops > accessible_stops else None,
        "Mettre en place un processus de mise à jour régulière des données d'accessibilité" if info_completion_rate >= 70 else None,
        f"Excellent taux d'accessibilité ({accessibility_rate:.1f}%) - maintenir ce niveau" if accessibility_rate > 80 and info_completion_rate > 80 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser la couverture et répartition des informations d'accessibilité des arrêts",
            "summary": f"{stops_with_info} arrêts avec info d'accessibilité ({info_completion_rate:.1f}%), dont {accessible_stops} accessibles ({accessibility_rate:.1f}%)",
            "impact": "Les données d'accessibilité sont essentielles pour les personnes à mobilité réduite"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="check_stop_url_validity",
    genre="validity",
    description="Vérifie la validité des URLs dans stop_url, si la colonne est présente.",
    parameters={}
)
def check_stop_url_validity(gtfs_data, **params):
    """
    Valide le format des URLs dans le champ stop_url du fichier stops.txt.
    Vérifie que les URLs sont bien formées selon les standards web.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valider le format des URLs dans le champ stop_url"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    total_stops = len(df)
    stop_ids = df.get("stop_id", [f"stop_{i}" for i in range(total_stops)])
    urls = df.get("stop_url", [""] * total_stops)

    # On vérifie uniquement les URLs non vides
    checked_urls = [(sid, url) for sid, url in zip(stop_ids, urls) if url and str(url).strip() != ""]

    invalid_urls_stop_ids = [sid for sid, url in checked_urls if not is_valid_url(str(url))]
    empty_urls_stop_ids = [sid for sid, url in zip(stop_ids, urls) if not url or str(url).strip() == ""]

    invalid_urls_count = len(invalid_urls_stop_ids)
    checked_urls_count = len(checked_urls)
    empty_urls_count = len(empty_urls_stop_ids)
    valid_urls_count = checked_urls_count - invalid_urls_count

    issues = []
    if invalid_urls_count > 0:
        issues.append({
            "type": "invalid_format",
            "field": "stop_url", 
            "count": invalid_urls_count,
            "affected_ids": invalid_urls_stop_ids[:100],  # Limiter à 100
            "message": f"{invalid_urls_count} URL(s) mal formée(s)"
        })
    if empty_urls_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "stop_url",
            "count": empty_urls_count, 
            "affected_ids": empty_urls_stop_ids[:100],  # Limiter à 100
            "message": f"{empty_urls_count} arrêt(s) sans URL"
        })

    # Détermination du status
    if total_stops == 0:
        status = "error"
    elif invalid_urls_count > 0:
        status = "error"  # Dès qu'il y a une URL invalide
    elif empty_urls_count > 0:
        status = "warning"  # Des URLs manquantes mais celles renseignées sont valides
    else:
        status = "success"  # Tout est parfait

    # Calcul du score de validité
    validity_score = (valid_urls_count / checked_urls_count * 100) if checked_urls_count > 0 else 0

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_stops": total_stops,
            "checked_urls": checked_urls_count,
            "valid_urls": valid_urls_count,
            "invalid_urls": invalid_urls_count,
            "empty_urls": empty_urls_count,
            "validity_score": round(validity_score, 1),
            "completion_rate": round((checked_urls_count / total_stops * 100), 2) if total_stops > 0 else 0
        },
        "explanation": {
            "purpose": "Valider le format des URLs dans le champ stop_url selon les standards web",
            "validation_rules": "Une URL est considérée comme valide si elle respecte le format standard (http/https)",
            "score_interpretation": f"{validity_score:.1f}% des URLs renseignées sont valides",
            "coverage": f"{checked_urls_count}/{total_stops} arrêts ont une URL renseignée"
        },
        "recommendations": [
            rec for rec in [
                "Corriger les URLs mal formées pour les arrêts concernés" if invalid_urls_count > 0 else None,
                "Ajouter des URLs d'information pour les arrêts qui n'en ont pas" if empty_urls_count > 0 else None,
                "Vérifier que les URLs pointent vers des pages web actives" if valid_urls_count > 0 else None,
                "Utiliser uniquement des URLs complètes avec protocole (http:// ou https://)" if invalid_urls_count > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="stops",
    name="stops_geographic_coverage",
    genre='statistics',
    description="Zone géographique couverte par les arrêts"
)
def stops_geographic_coverage(gtfs_data, **params):
    """
    Analyse la couverture géographique des arrêts basée sur leurs coordonnées.
    Calcule la zone englobante et les dimensions du réseau de transport.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la couverture géographique des arrêts du réseau de transport"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    # Vérification des colonnes de coordonnées
    if 'stop_lat' not in df.columns or 'stop_lon' not in df.columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "stop_lat, stop_lon",
                "count": 2,
                "affected_ids": [],
                "message": "Colonnes stop_lat et/ou stop_lon manquantes"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la couverture géographique des arrêts",
                "missing_data": "Coordonnées requises pour l'analyse géographique"
            },
            "recommendations": ["Ajouter les colonnes stop_lat et stop_lon avec coordonnées valides"]
        }

    # Filtrer les coordonnées valides
    valid_coords = df[(df['stop_lat'].notna()) & (df['stop_lon'].notna())]
    total_stops = len(df)
    stops_with_coords = len(valid_coords)
    
    if valid_coords.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_data",
                "field": "stop_lat, stop_lon",
                "count": total_stops,
                "affected_ids": [],
                "message": "Aucune coordonnée valide trouvée"
            }],
            "result": {
                "total_stops": total_stops,
                "stops_with_coordinates": 0,
                "geographic_analysis_possible": False
            },
            "explanation": {
                "purpose": "Analyser la couverture géographique des arrêts",
                "issue": "Impossible d'effectuer l'analyse sans coordonnées valides"
            },
            "recommendations": ["Renseigner les coordonnées GPS pour tous les arrêts"]
        }

    # Calcul de la zone englobante
    lat_min = valid_coords['stop_lat'].min()
    lat_max = valid_coords['stop_lat'].max()
    lon_min = valid_coords['stop_lon'].min()
    lon_max = valid_coords['stop_lon'].max()
    
    # Calcul du centre géographique
    center_lat = (lat_min + lat_max) / 2
    center_lon = (lon_min + lon_max) / 2
    
    # Calcul des dimensions approximatives en km
    lat_span_km = (lat_max - lat_min) * 111  # 1° de latitude ≈ 111 km
    lon_span_km = (lon_max - lon_min) * 111 * math.cos(math.radians(center_lat))  # Correction longitude
    
    # Calcul de la surface approximative en km²
    surface_km2 = lat_span_km * lon_span_km
    
    # Analyse de la densité
    density_stops_per_km2 = stops_with_coords / surface_km2 if surface_km2 > 0 else 0
    
    # Détection des issues
    issues = []
    
    # Couverture incomplète
    coverage_rate = stops_with_coords / total_stops
    if coverage_rate < 0.9:
        issues.append({
            "type": "missing_data",
            "field": "stop_lat, stop_lon",
            "count": total_stops - stops_with_coords,
            "affected_ids": [],
            "message": f"{total_stops - stops_with_coords} arrêts sans coordonnées ({(1-coverage_rate)*100:.1f}%)"
        })
    
    # Zone très étendue (potentiellement anormale)
    if lat_span_km > 500 or lon_span_km > 500:
        issues.append({
            "type": "data_quality",
            "field": "stop_lat, stop_lon",
            "count": 1,
            "affected_ids": [],
            "message": f"Zone géographique très étendue ({lat_span_km:.1f}×{lon_span_km:.1f}km) - vérifier coordonnées aberrantes"
        })
    
    # Zone très compacte (potentiellement incomplète)
    if lat_span_km < 1 and lon_span_km < 1 and stops_with_coords > 10:
        issues.append({
            "type": "data_quality",
            "field": "stop_lat, stop_lon",
            "count": 1,
            "affected_ids": [],
            "message": f"Zone géographique très compacte ({lat_span_km:.1f}×{lon_span_km:.1f}km) - réseau potentiellement incomplet"
        })

    # Détermination du status
    if coverage_rate < 0.5:
        status = "error"  # Moins de 50% des arrêts géolocalisés
    elif coverage_rate < 0.9 or any(issue["type"] == "data_quality" for issue in issues):
        status = "warning"
    else:
        status = "success"

    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "stops_with_coordinates": stops_with_coords,
        "coordinate_coverage_rate": round(coverage_rate * 100, 2),
        "bounding_box": {
            "lat_min": round(lat_min, 6),
            "lat_max": round(lat_max, 6),
            "lon_min": round(lon_min, 6),
            "lon_max": round(lon_max, 6)
        },
        "geographic_center": {
            "latitude": round(center_lat, 6),
            "longitude": round(center_lon, 6)
        },
        "coverage_dimensions": {
            "latitude_span_km": round(lat_span_km, 1),
            "longitude_span_km": round(lon_span_km, 1),
            "approximate_surface_km2": round(surface_km2, 1)
        },
        "density_analysis": {
            "stops_per_km2": round(density_stops_per_km2, 2),
            "density_category": (
                "very_dense" if density_stops_per_km2 > 10 else
                "dense" if density_stops_per_km2 > 5 else
                "moderate" if density_stops_per_km2 > 1 else
                "sparse"
            )
        },
        "geographic_quality": {
            "coverage_completeness": (
                "excellent" if coverage_rate >= 0.95 else
                "good" if coverage_rate >= 0.85 else
                "fair" if coverage_rate >= 0.70 else
                "poor"
            ),
            "extent_assessment": (
                "very_large" if lat_span_km > 100 or lon_span_km > 100 else
                "large" if lat_span_km > 50 or lon_span_km > 50 else
                "medium" if lat_span_km > 10 or lon_span_km > 10 else
                "compact"
            )
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"Géolocaliser {total_stops - stops_with_coords} arrêts manquants pour compléter la couverture" if coverage_rate < 0.9 else None,
        f"Vérifier les coordonnées aberrantes dans une zone de {lat_span_km:.0f}×{lon_span_km:.0f}km" if lat_span_km > 500 or lon_span_km > 500 else None,
        f"Examiner si le réseau est complet dans cette zone compacte de {lat_span_km:.1f}×{lon_span_km:.1f}km" if lat_span_km < 1 and lon_span_km < 1 and stops_with_coords > 10 else None,
        f"Optimiser la densité d'arrêts ({density_stops_per_km2:.1f} arrêts/km²)" if density_stops_per_km2 < 0.5 else None,
        "Documenter les zones de service et limites géographiques du réseau" if lat_span_km > 50 else None,
        f"Excellent maillage géographique avec {stops_with_coords} arrêts couvrant {surface_km2:.0f}km²" if coverage_rate >= 0.95 and 1 <= density_stops_per_km2 <= 20 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser la couverture géographique des arrêts du réseau de transport",
            "methodology": "Calcul de la zone englobante et des dimensions approximatives basées sur les coordonnées GPS",
            "geographic_summary": f"Réseau couvrant {lat_span_km:.1f}×{lon_span_km:.1f}km centré sur ({center_lat:.4f}, {center_lon:.4f})",
            "coverage_assessment": f"{stops_with_coords}/{total_stops} arrêts géolocalisés ({coverage_rate*100:.1f}%)"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="stops_fare_zones_distribution",
    genre='statistics',
    description="Répartition des arrêts par zones tarifaires."
)
def stops_fare_zones_distribution(gtfs_data, **params):
    """
    Analyse la répartition des arrêts par zones tarifaires basée sur le champ zone_id.
    Fournit des statistiques sur la couverture et distribution des zones tarifaires.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la répartition des arrêts par zones tarifaires"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    total_stops = len(df)
    
    if total_stops == 0:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt vide"
            }],
            "result": {
                "total_stops": 0,
                "fare_zone_analysis": {}
            },
            "explanation": {
                "purpose": "Analyser les zones tarifaires des arrêts",
                "issue": "Aucune donnée d'arrêt à analyser"
            },
            "recommendations": ["Ajouter des données d'arrêts dans le fichier stops.txt"]
        }

    # Vérification de la colonne zone_id (optionnelle dans GTFS)
    if 'zone_id' not in df.columns:
        return {
            "status": "warning",
            "issues": [{
                "type": "missing_field",
                "field": "zone_id",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne zone_id manquante (système tarifaire non défini)"
            }],
            "result": {
                "total_stops": total_stops,
                "zone_system_available": False,
                "stops_without_zone": total_stops,
                "zone_coverage_rate": 0.0
            },
            "explanation": {
                "purpose": "Analyser les zones tarifaires des arrêts",
                "gtfs_note": "Le champ zone_id est optionnel - absence indique un système tarifaire uniforme ou non défini"
            },
            "recommendations": [
                "Ajouter la colonne zone_id si le réseau utilise un système de zones tarifaires",
                "Documenter le système tarifaire utilisé (uniforme vs zones)"
            ]
        }

    # Analyse des zones tarifaires
    zone_data = df['zone_id'].dropna()
    stops_with_zones = len(zone_data)
    stops_without_zones = total_stops - stops_with_zones
    
    if zone_data.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "missing_data",
                "field": "zone_id",
                "count": total_stops,
                "affected_ids": [],
                "message": "Aucune zone tarifaire renseignée"
            }],
            "result": {
                "total_stops": total_stops,
                "zone_system_available": True,
                "stops_with_zones": 0,
                "stops_without_zones": total_stops,
                "zone_coverage_rate": 0.0
            },
            "explanation": {
                "purpose": "Analyser les zones tarifaires des arrêts",
                "observation": "Colonne zone_id présente mais aucune zone définie"
            },
            "recommendations": ["Renseigner les zones tarifaires pour tous les arrêts concernés"]
        }

    # Extraction et analyse des zones
    unique_zones = sorted(zone_data.unique().tolist())
    zone_distribution = {}
    
    for zone in unique_zones:
        count = (df['zone_id'] == zone).sum()
        zone_distribution[str(zone)] = {
            "stop_count": int(count),
            "percentage": round((count / total_stops) * 100, 2)
        }

    # Création des issues
    issues = []
    zone_coverage_rate = stops_with_zones / total_stops
    
    # Issue si couverture incomplète
    if stops_without_zones > 0:
        issues.append({
            "type": "missing_data",
            "field": "zone_id",
            "count": stops_without_zones,
            "affected_ids": [],
            "message": f"{stops_without_zones} arrêts sans zone tarifaire définie"
        })
    
    # Issue si déséquilibre important entre zones
    if len(unique_zones) > 1:
        zone_counts = [zone_distribution[str(z)]["stop_count"] for z in unique_zones]
        max_count = max(zone_counts)
        min_count = min(zone_counts)
        if max_count > min_count * 5:  # Déséquilibre > 5x
            issues.append({
                "type": "data_quality",
                "field": "zone_id",
                "count": 1,
                "affected_ids": [],
                "message": f"Déséquilibre important entre zones (ratio {max_count//min_count}:1)"
            })

    # Détermination du status
    if zone_coverage_rate < 0.8:
        status = "warning"  # Couverture insuffisante
    elif len(unique_zones) == 1 and total_stops > 50:
        status = "warning"  # Une seule zone pour un grand réseau
    else:
        status = "success"

    # Analyse de la répartition
    zone_stats = {
        "most_populated_zone": max(unique_zones, key=lambda z: zone_distribution[str(z)]["stop_count"]) if unique_zones else None,
        "least_populated_zone": min(unique_zones, key=lambda z: zone_distribution[str(z)]["stop_count"]) if unique_zones else None,
        "average_stops_per_zone": round(stops_with_zones / len(unique_zones), 1) if unique_zones else 0,
        "zone_balance_quality": (
            "balanced" if len(unique_zones) <= 1 else
            "well_balanced" if max(zone_counts) <= min(zone_counts) * 2 else
            "moderately_balanced" if max(zone_counts) <= min(zone_counts) * 5 else
            "unbalanced"
        )
    }

    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "zone_system_available": True,
        "stops_with_zones": stops_with_zones,
        "stops_without_zones": stops_without_zones,
        "zone_coverage_rate": round(zone_coverage_rate * 100, 2),
        "fare_zone_overview": {
            "unique_zones": [str(z) for z in unique_zones],
            "zone_count": len(unique_zones),
            "zone_distribution": zone_distribution
        },
        "zone_statistics": zone_stats,
        "system_assessment": {
            "coverage_quality": (
                "excellent" if zone_coverage_rate >= 0.95 else
                "good" if zone_coverage_rate >= 0.85 else
                "fair" if zone_coverage_rate >= 0.70 else
                "poor"
            ),
            "zone_complexity": (
                "simple" if len(unique_zones) <= 2 else
                "moderate" if len(unique_zones) <= 5 else
                "complex"
            ),
            "system_completeness": zone_coverage_rate >= 0.9
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"Définir des zones tarifaires pour {stops_without_zones} arrêts sans zone" if stops_without_zones > 0 else None,
        f"Rééquilibrer la répartition entre zones (zone '{zone_stats['most_populated_zone']}' surdimensionnée)" if zone_stats["zone_balance_quality"] == "unbalanced" else None,
        f"Vérifier la pertinence d'avoir une seule zone pour {total_stops} arrêts" if len(unique_zones) == 1 and total_stops > 50 else None,
        f"Documenter la logique tarifaire des {len(unique_zones)} zones définies" if len(unique_zones) > 3 else None,
        "Valider que toutes les zones définies correspondent à des tarifs effectifs" if len(unique_zones) > 0 else None,
        f"Système tarifaire bien structuré avec {len(unique_zones)} zones équilibrées" if zone_stats["zone_balance_quality"] in ["balanced", "well_balanced"] and zone_coverage_rate >= 0.9 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser la répartition des arrêts par zones tarifaires selon le système de tarification",
            "zone_system": f"{len(unique_zones)} zones identifiées avec {zone_coverage_rate*100:.1f}% de couverture",
            "distribution_summary": f"De {zone_distribution[str(zone_stats['least_populated_zone'])]['stop_count']} à {zone_distribution[str(zone_stats['most_populated_zone'])]['stop_count']} arrêts par zone" if len(unique_zones) > 1 else f"Zone unique avec {stops_with_zones} arrêts"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="isolated_stops_detection",
    genre='geographic',
    description="Détecte les stops très éloignés de tout autre stop (isolés).",
    parameters={
        "distance_threshold_m": {"type": "float", "default": 1000.0}
    }
)
def isolated_stops_detection(gtfs_data, distance_threshold_m=1000.0, **params):
    """
    Détecte les arrêts isolés géographiquement par rapport aux autres arrêts du réseau.
    Identifie les arrêts situés au-delà du seuil de distance configuré.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecter les arrêts isolés géographiquement dans le réseau de transport"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    if df.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt vide"
            }],
            "result": {
                "total_stops": 0,
                "isolation_analysis": {}
            },
            "explanation": {
                "purpose": "Détecter les arrêts isolés géographiquement",
                "issue": "Aucune donnée d'arrêt à analyser"
            },
            "recommendations": ["Ajouter des données d'arrêts dans le fichier stops.txt"]
        }

    # Vérification des colonnes requises
    required_columns = ['stop_id', 'stop_lat', 'stop_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": ', '.join(missing_columns),
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes manquantes: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecter les arrêts isolés géographiquement",
                "missing_data": f"Colonnes requises manquantes: {', '.join(missing_columns)}"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Filtrer les arrêts avec coordonnées valides
    valid_stops = df.dropna(subset=['stop_lat', 'stop_lon']).copy()
    total_stops = len(df)
    valid_stops_count = len(valid_stops)
    
    if valid_stops_count < 2:
        return {
            "status": "warning",
            "issues": [{
                "type": "insufficient_data",
                "field": "stop_lat, stop_lon",
                "count": 1,
                "affected_ids": [],
                "message": f"Moins de 2 arrêts avec coordonnées valides ({valid_stops_count})"
            }],
            "result": {
                "total_stops": total_stops,
                "valid_stops": valid_stops_count,
                "isolation_analysis_possible": False
            },
            "explanation": {
                "purpose": "Détecter les arrêts isolés géographiquement",
                "limitation": "Au moins 2 arrêts avec coordonnées valides requis pour l'analyse"
            },
            "recommendations": ["Compléter les coordonnées manquantes pour permettre l'analyse d'isolation"]
        }

    # Détection des arrêts isolés
    isolated_stops = []
    distance_matrix = []
    
    coords = valid_stops[['stop_lat', 'stop_lon']].values
    stop_ids = valid_stops['stop_id'].tolist()
    
    for i, coord_i in enumerate(coords):
        min_distance = float('inf')
        nearest_stop_id = None
        
        for j, coord_j in enumerate(coords):
            if i == j:
                continue
            
            # Calcul de distance géodésique
            distance = calculate_distance(coord_i[0], coord_i[1], coord_j[0], coord_j[1])
            
            if distance < min_distance:
                min_distance = distance
                nearest_stop_id = stop_ids[j]
        
        distance_matrix.append({
            "stop_id": stop_ids[i],
            "nearest_stop_id": nearest_stop_id,
            "distance_to_nearest_m": round(min_distance, 2)
        })
        
        # Vérifier si l'arrêt est isolé
        if min_distance > distance_threshold_m:
            isolated_stops.append({
                "stop_id": stop_ids[i],
                "nearest_stop_id": nearest_stop_id,
                "distance_to_nearest_m": round(min_distance, 2)
            })

    # Création des issues
    issues = []
    if isolated_stops:
        # Limiter les IDs affectés à 100 pour performance
        affected_ids = [stop["stop_id"] for stop in isolated_stops[:100]]
        
        issues.append({
            "type": "isolated_data",
            "field": "stop_lat, stop_lon",
            "count": len(isolated_stops),
            "affected_ids": affected_ids,
            "message": f"{len(isolated_stops)} arrêt(s) isolé(s) au-delà de {distance_threshold_m}m"
        })

    # Détermination du status
    isolation_rate = len(isolated_stops) / valid_stops_count
    
    if not isolated_stops:
        status = "success"
    elif isolation_rate > 0.1:  # Plus de 10% d'arrêts isolés
        status = "error"
    else:
        status = "warning"

    # Analyse des distances
    distances = [item["distance_to_nearest_m"] for item in distance_matrix]
    isolated_distances = [stop["distance_to_nearest_m"] for stop in isolated_stops]
    
    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "valid_stops_analyzed": valid_stops_count,
        "isolated_stops_count": len(isolated_stops),
        "isolation_rate": round(isolation_rate * 100, 2),
        "distance_threshold_m": distance_threshold_m,
        "distance_statistics": {
            "min_distance_to_nearest": round(min(distances), 2) if distances else 0,
            "max_distance_to_nearest": round(max(distances), 2) if distances else 0,
            "avg_distance_to_nearest": round(sum(distances) / len(distances), 2) if distances else 0,
            "median_distance_to_nearest": round(sorted(distances)[len(distances)//2], 2) if distances else 0
        },
        "isolated_stops_details": sorted(isolated_stops, key=lambda x: x["distance_to_nearest_m"], reverse=True)[:20],
        "most_isolated_stop": max(isolated_stops, key=lambda x: x["distance_to_nearest_m"]) if isolated_stops else None,
        "network_density_assessment": {
            "avg_nearest_neighbor_distance": round(sum(distances) / len(distances), 2) if distances else 0,
            "density_category": (
                "very_dense" if sum(distances) / len(distances) < 200 else
                "dense" if sum(distances) / len(distances) < 500 else
                "moderate" if sum(distances) / len(distances) < 1000 else
                "sparse"
            ) if distances else "unknown",
            "connectivity_quality": (
                "excellent" if isolation_rate == 0 else
                "good" if isolation_rate <= 0.02 else
                "fair" if isolation_rate <= 0.05 else
                "poor"
            )
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Examiner {len(isolated_stops)} arrêt(s) isolé(s) au-delà de {distance_threshold_m}m" if isolated_stops else None,
        f"Priorité: vérifier l'arrêt '{isolated_stops[0]['stop_id']}' isolé à {isolated_stops[0]['distance_to_nearest_m']}m" if isolated_stops else None,
        f"Considérer ajuster le seuil d'isolation (actuellement {distance_threshold_m}m) selon le contexte urbain/rural" if len(isolated_stops) > valid_stops_count * 0.2 else None,
        "Vérifier la cohérence géographique et l'accessibilité des arrêts isolés" if isolated_stops else None,
        f"Analyser si les arrêts isolés correspondent à des terminus ou points d'échange justifiés" if len(isolated_stops) > 2 else None,
        f"Optimiser le maillage du réseau - distance moyenne entre arrêts: {sum(distances) / len(distances):.0f}m" if distances and sum(distances) / len(distances) > 800 else None,
        f"Excellent maillage du réseau - tous les arrêts connectés dans un rayon de {distance_threshold_m}m" if not isolated_stops and valid_stops_count > 10 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Détecter les arrêts isolés géographiquement pour évaluer la connectivité du réseau",
            "methodology": f"Calcul de la distance au plus proche voisin avec seuil d'isolation de {distance_threshold_m}m",
            "network_assessment": f"Réseau de {valid_stops_count} arrêts avec {len(isolated_stops)} arrêt(s) isolé(s)"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="stops_geographical_quality",
    genre='geographic',
    description="Détecte stops éloignés (outliers) et doublons géographiquement proches",
    parameters={
        "zscore_threshold": {"type": "number", "default": 3.0},
        "duplicate_distance_threshold_m": {"type": "number", "default": 10.0}
    }
)
def stops_geographical_quality(gtfs_data, zscore_threshold=3.0, duplicate_distance_threshold_m=10.0, **params):
    """
    Analyse la qualité géographique des arrêts : détection d'outliers et de doublons géographiques.
    Identifie les coordonnées aberrantes et les arrêts potentiellement dupliqués par proximité.
    """
    
    df = gtfs_data.get('stops.txt')
    
    if df is None:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt manquant"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la qualité géographique des arrêts (outliers et doublons géographiques)"
            },
            "recommendations": ["URGENT: Fournir un fichier stops.txt valide"]
        }

    if df.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "empty_file",
                "field": "stops.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stops.txt vide"
            }],
            "result": {
                "total_stops": 0,
                "geographical_quality_analysis": {}
            },
            "explanation": {
                "purpose": "Analyser la qualité géographique des arrêts",
                "issue": "Aucune donnée d'arrêt à analyser"
            },
            "recommendations": ["Ajouter des données d'arrêts dans le fichier stops.txt"]
        }

    # Vérification des colonnes requises
    required_columns = ['stop_id', 'stop_lat', 'stop_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": ', '.join(missing_columns),
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes manquantes: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la qualité géographique des arrêts",
                "missing_data": f"Colonnes requises manquantes: {', '.join(missing_columns)}"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Filtrer les arrêts avec coordonnées valides
    valid_stops = df.dropna(subset=['stop_lat', 'stop_lon']).copy()
    total_stops = len(df)
    valid_stops_count = len(valid_stops)
    
    if valid_stops_count < 3:
        return {
            "status": "warning",
            "issues": [{
                "type": "insufficient_data",
                "field": "stop_lat, stop_lon",
                "count": 1,
                "affected_ids": [],
                "message": f"Pas assez d'arrêts avec coordonnées valides pour l'analyse ({valid_stops_count})"
            }],
            "result": {
                "total_stops": total_stops,
                "valid_stops": valid_stops_count,
                "geographical_analysis_possible": False
            },
            "explanation": {
                "purpose": "Analyser la qualité géographique des arrêts",
                "limitation": "Au moins 3 arrêts avec coordonnées valides requis pour l'analyse statistique"
            },
            "recommendations": ["Compléter les coordonnées manquantes pour permettre l'analyse de qualité"]
        }

    # 1. Détection des outliers géographiques via Z-score
    lat_mean = valid_stops['stop_lat'].mean()
    lat_std = valid_stops['stop_lat'].std(ddof=0)
    lon_mean = valid_stops['stop_lon'].mean()
    lon_std = valid_stops['stop_lon'].std(ddof=0)
    
    # Éviter division par zéro si tous les points sont identiques
    if lat_std == 0 or lon_std == 0:
        lat_z = pd.Series([0] * len(valid_stops), index=valid_stops.index)
        lon_z = pd.Series([0] * len(valid_stops), index=valid_stops.index)
    else:
        lat_z = (valid_stops['stop_lat'] - lat_mean) / lat_std
        lon_z = (valid_stops['stop_lon'] - lon_mean) / lon_std
    
    outlier_mask = (lat_z.abs() > zscore_threshold) | (lon_z.abs() > zscore_threshold)
    outliers = valid_stops[outlier_mask]
    outlier_count = len(outliers)
    
    # 2. Détection des doublons géographiques proches
    geographic_duplicates = []
    coords = list(zip(valid_stops['stop_lat'], valid_stops['stop_lon']))
    stop_ids = valid_stops['stop_id'].tolist()
    
    for i in range(len(coords)):
        for j in range(i + 1, len(coords)):
            distance = calculate_distance(coords[i][0], coords[i][1], coords[j][0], coords[j][1])
            
            if distance <= duplicate_distance_threshold_m:
                geographic_duplicates.append({
                    "stop_id_1": stop_ids[i],
                    "stop_id_2": stop_ids[j],
                    "distance_m": round(distance, 2)
                })

    duplicates_count = len(geographic_duplicates)

    # Création des issues
    issues = []
    
    if outlier_count > 0:
        # Limiter les IDs affectés à 100 pour performance
        outlier_ids = outliers['stop_id'].tolist()[:100]
        issues.append({
            "type": "geographical_outlier",
            "field": "stop_lat, stop_lon",
            "count": outlier_count,
            "affected_ids": outlier_ids,
            "message": f"{outlier_count} arrêt(s) géographiquement aberrant(s) (Z-score > {zscore_threshold})"
        })
    
    if duplicates_count > 0:
        # Limiter les paires affectées à 100 pour performance
        duplicate_ids = list(set([d["stop_id_1"] for d in geographic_duplicates] + 
                                [d["stop_id_2"] for d in geographic_duplicates]))[:100]
        issues.append({
            "type": "geographical_duplicate",
            "field": "stop_lat, stop_lon",
            "count": duplicates_count,
            "affected_ids": duplicate_ids,
            "message": f"{duplicates_count} paire(s) d'arrêts géographiquement proches (< {duplicate_distance_threshold_m}m)"
        })

    # Détermination du status
    quality_issues_rate = (outlier_count + duplicates_count) / valid_stops_count
    
    if quality_issues_rate == 0:
        status = "success"
    elif quality_issues_rate > 0.1:  # Plus de 10% de problèmes de qualité
        status = "error"
    else:
        status = "warning"

    # Analyse statistique des coordonnées
    coordinate_stats = {
        "latitude_range": {
            "min": round(valid_stops['stop_lat'].min(), 6),
            "max": round(valid_stops['stop_lat'].max(), 6),
            "mean": round(lat_mean, 6),
            "std": round(lat_std, 6)
        },
        "longitude_range": {
            "min": round(valid_stops['stop_lon'].min(), 6),
            "max": round(valid_stops['stop_lon'].max(), 6),
            "mean": round(lon_mean, 6),
            "std": round(lon_std, 6)
        }
    }

    # Résultats détaillés
    result = {
        "total_stops": total_stops,
        "valid_stops_analyzed": valid_stops_count,
        "geographical_quality_metrics": {
            "outliers_detected": outlier_count,
            "geographic_duplicates_detected": duplicates_count,
            "quality_issues_rate": round(quality_issues_rate * 100, 2),
            "zscore_threshold_used": zscore_threshold,
            "duplicate_distance_threshold_m": duplicate_distance_threshold_m
        },
        "outlier_analysis": {
            "outlier_stop_ids": outliers['stop_id'].tolist()[:20] if outlier_count > 0 else [],
            "most_extreme_outlier": {
                "stop_id": outliers.loc[outliers.index[lat_z.abs().idxmax() if lat_z.abs().max() > lon_z.abs().max() else lon_z.abs().idxmax()], 'stop_id'],
                "max_zscore": round(max(lat_z.abs().max(), lon_z.abs().max()), 2)
            } if outlier_count > 0 else None
        },
        "duplicate_analysis": {
            "duplicate_pairs": geographic_duplicates[:20],  # Limiter à 20 paires
            "closest_pair": min(geographic_duplicates, key=lambda x: x["distance_m"]) if duplicates_count > 0 else None
        },
        "coordinate_statistics": coordinate_stats,
        "overall_quality_assessment": {
            "geographical_consistency": (
                "excellent" if quality_issues_rate == 0 else
                "good" if quality_issues_rate <= 0.02 else
                "fair" if quality_issues_rate <= 0.05 else
                "poor"
            ),
            "data_integrity": "preserved" if quality_issues_rate <= 0.02 else "compromised"
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Examiner {outlier_count} arrêt(s) géographiquement aberrant(s)" if outlier_count > 0 else None,
        f"URGENT: Vérifier {duplicates_count} paire(s) d'arrêts potentiellement dupliqués" if duplicates_count > 0 else None,
        f"Corriger l'arrêt le plus aberrant: '{result['outlier_analysis']['most_extreme_outlier']['stop_id']}'" if outlier_count > 0 else None,
        f"Examiner la paire la plus proche: {geographic_duplicates[0]['stop_id_1']} / {geographic_duplicates[0]['stop_id_2']} ({geographic_duplicates[0]['distance_m']}m)" if duplicates_count > 0 else None,
        f"Considérer ajuster le seuil Z-score (actuellement {zscore_threshold}) selon le contexte géographique" if outlier_count > valid_stops_count * 0.1 else None,
        f"Revoir le seuil de duplication (actuellement {duplicate_distance_threshold_m}m) selon la densité urbaine" if duplicates_count > valid_stops_count * 0.05 else None,
        "Implémenter des contrôles de qualité géographique lors de la saisie des données" if quality_issues_rate > 0.02 else None,
        f"Excellente qualité géographique - {valid_stops_count} arrêts sans anomalie détectée" if quality_issues_rate == 0 and valid_stops_count > 10 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser la qualité géographique des arrêts pour détecter coordonnées aberrantes et doublons",
            "methodology": f"Détection outliers par Z-score (seuil {zscore_threshold}) et doublons par distance (seuil {duplicate_distance_threshold_m}m)",
            "quality_assessment": f"Qualité géographique: {result['overall_quality_assessment']['geographical_consistency']} ({quality_issues_rate*100:.1f}% d'anomalies)"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }

@audit_function(
    file_type="stops",
    name="shapes_cover_all_stops",
    genre='geographic',
    description="Vérifie que tous les stops d'un trip sont proches des shapes du même trip.",
    parameters={
        "max_distance_meters": {"type": "number", "default": 100}
    }
)
def shapes_cover_all_stops(gtfs_data, max_distance_meters=100, **params):
    """
    Vérifie que tous les arrêts sont correctement couverts par leurs formes géométriques associées.
    Analyse l'alignement géographique entre stops et shapes selon les trips.
    """
    
    # Vérification des fichiers requis
    required_tables = ['shapes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    missing_tables = []
    
    for table in required_tables:
        if gtfs_data.get(table) is None:
            missing_tables.append(table)
    
    if missing_tables:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": ', '.join(missing_tables),
                "count": len(missing_tables),
                "affected_ids": [],
                "message": f"Fichiers manquants: {', '.join(missing_tables)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier l'alignement géographique entre arrêts et formes géométriques des trajets"
            },
            "recommendations": [f"URGENT: Fournir les fichiers manquants: {', '.join(missing_tables)}"]
        }

    # Récupération des données
    shapes_df = gtfs_data.get('shapes.txt')
    stop_times_df = gtfs_data.get('stop_times.txt')
    stops_df = gtfs_data.get('stops.txt')
    trips_df = gtfs_data.get('trips.txt')
    
    # Vérification que les DataFrames ne sont pas vides
    empty_tables = []
    for name, df in [('shapes.txt', shapes_df), ('stop_times.txt', stop_times_df), 
                     ('stops.txt', stops_df), ('trips.txt', trips_df)]:
        if df.empty:
            empty_tables.append(name)
    
    if empty_tables:
        return {
            "status": "error",
            "issues": [{
                "type": "empty_file",
                "field": ', '.join(empty_tables),
                "count": len(empty_tables),
                "affected_ids": [],
                "message": f"Fichiers vides: {', '.join(empty_tables)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier l'alignement géographique entre arrêts et shapes",
                "issue": "Impossible d'effectuer l'analyse avec des fichiers vides"
            },
            "recommendations": ["Remplir les fichiers requis avec des données valides"]
        }

    # Vérification des colonnes requises
    required_columns = {
        'shapes.txt': ['shape_id', 'shape_pt_lat', 'shape_pt_lon'],
        'stops.txt': ['stop_id', 'stop_lat', 'stop_lon'],
        'trips.txt': ['trip_id', 'shape_id'],
        'stop_times.txt': ['trip_id', 'stop_id']
    }
    
    missing_columns_issues = []
    for table_name, columns in required_columns.items():
        df = gtfs_data.get(table_name)
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            missing_columns_issues.append(f"{table_name}: {', '.join(missing_cols)}")
    
    if missing_columns_issues:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "colonnes requises",
                "count": len(missing_columns_issues),
                "affected_ids": [],
                "message": f"Colonnes manquantes - {'; '.join(missing_columns_issues)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier l'alignement géographique entre arrêts et shapes",
                "missing_data": "Colonnes requises manquantes pour l'analyse"
            },
            "recommendations": ["Ajouter les colonnes manquantes dans les fichiers concernés"]
        }

    # Préparation des données
    try:
        # Dictionnaire des coordonnées des arrêts
        stop_coords = stops_df.set_index('stop_id')[['stop_lat', 'stop_lon']].to_dict('index')
        
        # Groupement des points de forme par shape_id
        shape_points = shapes_df.groupby('shape_id').apply(
            lambda g: list(zip(g['shape_pt_lat'], g['shape_pt_lon']))
        ).to_dict()
        
        # Mapping trip_id -> shape_id
        trips_shape = trips_df.set_index('trip_id')['shape_id'].to_dict()
        
        # Groupement des stops par trip_id
        stop_times_grouped = stop_times_df.groupby('trip_id')['stop_id'].apply(list).to_dict()
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "data_processing_error",
                "field": "données GTFS",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors du traitement des données: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifier l'alignement géographique entre arrêts et shapes",
                "error": "Problème lors du traitement des données"
            },
            "recommendations": ["Vérifier l'intégrité et la cohérence des données GTFS"]
        }

    # Analyse de la couverture
    trips_with_issues = {}
    stops_analysis = {}
    total_stop_checks = 0
    
    for trip_id, stop_ids_list in stop_times_grouped.items():
        shape_id = trips_shape.get(trip_id)
        
        # Ignorer les trips sans shape_id ou avec shape inexistante
        if shape_id is None or shape_id not in shape_points:
            continue
            
        shape_pts = shape_points[shape_id]
        trip_issues = []
        
        for stop_id in stop_ids_list:
            coord = stop_coords.get(stop_id)
            if coord is None:
                continue
                
            stop_coord = (coord['stop_lat'], coord['stop_lon'])
            total_stop_checks += 1
            
            # Calcul de la distance minimale au shape
            distances = [calculate_distance(stop_coord[0], stop_coord[1], pt[0], pt[1]) 
                        for pt in shape_pts]
            min_dist = min(distances) if distances else float('inf')
            
            # Enregistrement pour analyse globale
            if stop_id not in stops_analysis:
                stops_analysis[stop_id] = []
            stops_analysis[stop_id].append({
                'trip_id': trip_id,
                'shape_id': shape_id,
                'distance_to_shape': min_dist
            })
            
            # Vérification du seuil
            if min_dist > max_distance_meters:
                trip_issues.append({
                    "stop_id": stop_id,
                    "shape_id": shape_id,
                    "min_distance_meters": round(min_dist, 2)
                })
        
        if trip_issues:
            trips_with_issues[trip_id] = trip_issues

    # Création des issues
    issues = []
    if trips_with_issues:
        # Limiter les IDs affectés à 100 pour performance
        affected_trip_ids = list(trips_with_issues.keys())[:100]
        
        issues.append({
            "type": "geometric_alignment",
            "field": "shapes, stops",
            "count": len(trips_with_issues),
            "affected_ids": affected_trip_ids,
            "message": f"{len(trips_with_issues)} trip(s) avec arrêts éloignés de leur forme géométrique (> {max_distance_meters}m)"
        })

    # Détermination du status
    coverage_issues_rate = len(trips_with_issues) / len(stop_times_grouped) if stop_times_grouped else 0
    
    if not trips_with_issues:
        status = "success"
    elif coverage_issues_rate > 0.1:  # Plus de 10% des trips ont des problèmes
        status = "error"
    else:
        status = "warning"

    # Calcul des statistiques de distance
    all_distances = []
    for stop_analysis in stops_analysis.values():
        for analysis in stop_analysis:
            all_distances.append(analysis['distance_to_shape'])

    # Analyse des arrêts les plus problématiques
    problematic_stops = {}
    for trip_id, issues_list in trips_with_issues.items():
        for issue in issues_list:
            stop_id = issue['stop_id']
            if stop_id not in problematic_stops:
                problematic_stops[stop_id] = []
            problematic_stops[stop_id].append({
                'trip_id': trip_id,
                'distance': issue['min_distance_meters']
            })

    # Résultats détaillés
    result = {
        "total_trips_analyzed": len(stop_times_grouped),
        "trips_with_coverage_issues": len(trips_with_issues),
        "total_stop_shape_checks": total_stop_checks,
        "coverage_quality_rate": round((1 - coverage_issues_rate) * 100, 2),
        "distance_threshold_meters": max_distance_meters,
        "distance_statistics": {
            "min_distance_to_shape": round(min(all_distances), 2) if all_distances else 0,
            "max_distance_to_shape": round(max(all_distances), 2) if all_distances else 0,
            "avg_distance_to_shape": round(sum(all_distances) / len(all_distances), 2) if all_distances else 0,
            "distances_over_threshold": len([d for d in all_distances if d > max_distance_meters])
        },
        "problematic_trips_details": {
            trip_id: issues_list for trip_id, issues_list in list(trips_with_issues.items())[:10]
        },
        "most_problematic_stops": sorted(
            [(stop_id, max(data, key=lambda x: x['distance'])['distance']) 
             for stop_id, data in problematic_stops.items()],
            key=lambda x: x[1], reverse=True
        )[:10],
        "alignment_quality_assessment": {
            "geometric_precision": (
                "excellent" if not trips_with_issues else
                "good" if coverage_issues_rate <= 0.02 else
                "fair" if coverage_issues_rate <= 0.05 else
                "poor"
            ),
            "shapes_coverage_completeness": round((1 - coverage_issues_rate) * 100, 2)
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Corriger l'alignement pour {len(trips_with_issues)} trip(s) avec arrêts mal positionnés" if trips_with_issues else None,
        f"Examiner en priorité le trip '{list(trips_with_issues.keys())[0]}' avec {len(trips_with_issues[list(trips_with_issues.keys())[0]])} arrêts problématiques" if trips_with_issues else None,
        f"Revoir l'arrêt '{result['most_problematic_stops'][0][0]}' éloigné de {result['most_problematic_stops'][0][1]}m" if result['most_problematic_stops'] else None,
        f"Considérer ajuster le seuil de tolérance (actuellement {max_distance_meters}m) selon le contexte urbain" if len(trips_with_issues) > len(stop_times_grouped) * 0.2 else None,
        "Améliorer la précision géométrique des shapes pour un meilleur alignement" if result['distance_statistics']['avg_distance_to_shape'] > max_distance_meters / 2 else None,
        "Vérifier la cohérence entre les tracés théoriques (shapes) et les arrêts réels" if trips_with_issues else None,
        f"Excellente couverture géométrique - tous les arrêts alignés dans un rayon de {max_distance_meters}m" if not trips_with_issues and len(stop_times_grouped) > 10 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Vérifier que tous les arrêts sont correctement couverts par leurs formes géométriques associées",
            "methodology": f"Calcul de la distance minimale entre chaque arrêt et sa forme géométrique (seuil {max_distance_meters}m)",
            "coverage_assessment": f"{len(trips_with_issues)}/{len(stop_times_grouped)} trips avec problèmes d'alignement"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }


@audit_function(
    file_type="stops",
    name="distance_between_stops_consistency",
    genre='geographic',
    description="Vérifie que distances entre stops consécutifs dans un trip sont réalistes.",
    parameters={
        "max_distance_km": {"type": "float", "default": 10.0},
        "min_distance_m": {"type": "float", "default": 10.0}
    }
)
def distance_between_stops_consistency(gtfs_data, max_distance_km=10.0, min_distance_m=10.0, **params):
    """
    Analyse la cohérence des distances entre arrêts consécutifs dans les trajets.
    Détecte les distances aberrantes (trop grandes ou trop petites) entre arrêts successifs.
    """
    
    # Vérification des fichiers requis
    required_tables = ['stop_times.txt', 'stops.txt', 'trips.txt']
    missing_tables = []
    
    for table in required_tables:
        if gtfs_data.get(table) is None:
            missing_tables.append(table)
    
    if missing_tables:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": ', '.join(missing_tables),
                "count": len(missing_tables),
                "affected_ids": [],
                "message": f"Fichiers manquants: {', '.join(missing_tables)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la cohérence des distances entre arrêts consécutifs dans les trajets"
            },
            "recommendations": [f"URGENT: Fournir les fichiers manquants: {', '.join(missing_tables)}"]
        }

    # Récupération des données
    stop_times_df = gtfs_data.get('stop_times.txt')
    stops_df = gtfs_data.get('stops.txt')
    trips_df = gtfs_data.get('trips.txt')
    
    # Vérification que les DataFrames ne sont pas vides
    empty_tables = []
    for name, df in [('stop_times.txt', stop_times_df), ('stops.txt', stops_df), ('trips.txt', trips_df)]:
        if df.empty:
            empty_tables.append(name)
    
    if empty_tables:
        return {
            "status": "error",
            "issues": [{
                "type": "empty_file",
                "field": ', '.join(empty_tables),
                "count": len(empty_tables),
                "affected_ids": [],
                "message": f"Fichiers vides: {', '.join(empty_tables)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la cohérence des distances entre arrêts consécutifs",
                "issue": "Impossible d'effectuer l'analyse avec des fichiers vides"
            },
            "recommendations": ["Remplir les fichiers requis avec des données valides"]
        }

    # Vérification des colonnes requises
    required_columns = {
        'stop_times.txt': ['trip_id', 'stop_id', 'stop_sequence'],
        'stops.txt': ['stop_id', 'stop_lat', 'stop_lon'],
        'trips.txt': ['trip_id']
    }
    
    missing_columns_issues = []
    for table_name, columns in required_columns.items():
        df = gtfs_data.get(table_name)
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            missing_columns_issues.append(f"{table_name}: {', '.join(missing_cols)}")
    
    if missing_columns_issues:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "colonnes requises",
                "count": len(missing_columns_issues),
                "affected_ids": [],
                "message": f"Colonnes manquantes - {'; '.join(missing_columns_issues)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la cohérence des distances entre arrêts consécutifs",
                "missing_data": "Colonnes requises manquantes pour l'analyse"
            },
            "recommendations": ["Ajouter les colonnes manquantes dans les fichiers concernés"]
        }

    # Préparation des données
    try:
        # Fusion avec les coordonnées des arrêts
        stops_coords = stops_df.set_index('stop_id')[['stop_lat', 'stop_lon']]
        stop_times_merged = stop_times_df.merge(stops_coords, left_on='stop_id', right_index=True, how='left')
        
        # Filtrer les données avec coordonnées valides
        valid_data = stop_times_merged.dropna(subset=['stop_lat', 'stop_lon'])
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "data_processing_error",
                "field": "données GTFS",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors du traitement des données: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyser la cohérence des distances entre arrêts consécutifs",
                "error": "Problème lors du traitement des données"
            },
            "recommendations": ["Vérifier l'intégrité et la cohérence des données GTFS"]
        }

    if valid_data.empty:
        return {
            "status": "warning",
            "issues": [{
                "type": "insufficient_data",
                "field": "stop_lat, stop_lon",
                "count": 1,
                "affected_ids": [],
                "message": "Aucune donnée valide avec coordonnées pour l'analyse"
            }],
            "result": {
                "total_trips": len(trips_df),
                "distance_analysis_possible": False
            },
            "explanation": {
                "purpose": "Analyser la cohérence des distances entre arrêts consécutifs",
                "limitation": "Données de coordonnées insuffisantes"
            },
            "recommendations": ["Compléter les coordonnées manquantes dans stops.txt"]
        }

    # Analyse des distances entre arrêts consécutifs
    inconsistencies = []
    distance_stats = []
    trips_analyzed = 0
    
    for trip_id, group in valid_data.groupby('trip_id'):
        if len(group) < 2:
            continue  # Besoin d'au moins 2 arrêts pour calculer des distances
            
        trips_analyzed += 1
        group_sorted = group.sort_values('stop_sequence')
        coords = list(zip(group_sorted['stop_lat'], group_sorted['stop_lon']))
        stop_ids = group_sorted['stop_id'].tolist()
        
        for i in range(len(coords) - 1):
            # Calcul de la distance entre arrêts consécutifs
            distance_m = calculate_distance(coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1])
            distance_stats.append(distance_m)
            
            # Vérification des seuils
            is_too_far = distance_m > max_distance_km * 1000
            is_too_close = distance_m < min_distance_m
            
            if is_too_far or is_too_close:
                inconsistencies.append({
                    "trip_id": trip_id,
                    "stop_id_from": stop_ids[i],
                    "stop_id_to": stop_ids[i + 1],
                    "distance_meters": round(distance_m, 2),
                    "issue_type": "too_far" if is_too_far else "too_close",
                    "stop_sequence_from": group_sorted.iloc[i]['stop_sequence'],
                    "stop_sequence_to": group_sorted.iloc[i + 1]['stop_sequence']
                })

    # Création des issues
    issues = []
    
    # Issues pour distances trop grandes
    too_far_issues = [inc for inc in inconsistencies if inc["issue_type"] == "too_far"]
    if too_far_issues:
        affected_trips = list(set([inc["trip_id"] for inc in too_far_issues]))[:100]
        issues.append({
            "type": "distance_too_large",
            "field": "stop_sequence",
            "count": len(too_far_issues),
            "affected_ids": affected_trips,
            "message": f"{len(too_far_issues)} distances entre arrêts > {max_distance_km}km"
        })
    
    # Issues pour distances trop petites
    too_close_issues = [inc for inc in inconsistencies if inc["issue_type"] == "too_close"]
    if too_close_issues:
        affected_trips = list(set([inc["trip_id"] for inc in too_close_issues]))[:100]
        issues.append({
            "type": "distance_too_small",
            "field": "stop_sequence",
            "count": len(too_close_issues),
            "affected_ids": affected_trips,
            "message": f"{len(too_close_issues)} distances entre arrêts < {min_distance_m}m"
        })

    # Détermination du status
    total_segments = len(distance_stats)
    inconsistency_rate = len(inconsistencies) / total_segments if total_segments > 0 else 0
    
    if not inconsistencies:
        status = "success"
    elif inconsistency_rate > 0.1:  # Plus de 10% d'incohérences
        status = "error"
    else:
        status = "warning"

    # Résultats détaillés
    result = {
        "total_trips_analyzed": trips_analyzed,
        "total_stop_segments": total_segments,
        "inconsistencies_detected": len(inconsistencies),
        "inconsistency_rate": round(inconsistency_rate * 100, 2),
        "distance_thresholds": {
            "max_distance_km": max_distance_km,
            "min_distance_m": min_distance_m
        },
        "distance_statistics": {
            "min_distance_m": round(min(distance_stats), 2) if distance_stats else 0,
            "max_distance_m": round(max(distance_stats), 2) if distance_stats else 0,
            "avg_distance_m": round(sum(distance_stats) / len(distance_stats), 2) if distance_stats else 0,
            "median_distance_m": round(sorted(distance_stats)[len(distance_stats)//2], 2) if distance_stats else 0
        },
        "inconsistency_breakdown": {
            "too_far_count": len(too_far_issues),
            "too_close_count": len(too_close_issues),
            "most_extreme_distance": max(inconsistencies, key=lambda x: x["distance_meters"]) if inconsistencies else None,
            "closest_stops": min(inconsistencies, key=lambda x: x["distance_meters"]) if inconsistencies else None
        },
        "problematic_trips_details": inconsistencies[:20],  # Limiter à 20 pour l'affichage
        "distance_quality_assessment": {
            "consistency_level": (
                "excellent" if inconsistency_rate == 0 else
                "good" if inconsistency_rate <= 0.02 else
                "fair" if inconsistency_rate <= 0.05 else
                "poor"
            ),
            "network_coherence": "maintained" if inconsistency_rate <= 0.05 else "compromised"
        }
    }

    # Génération des recommandations conditionnelles
    recommendations = [
        f"URGENT: Examiner {len(inconsistencies)} incohérences de distances entre arrêts consécutifs" if inconsistencies else None,
        f"Corriger {len(too_far_issues)} distances > {max_distance_km}km (possibles erreurs de séquence)" if too_far_issues else None,
        f"Vérifier {len(too_close_issues)} distances < {min_distance_m}m (arrêts potentiellement dupliqués)" if too_close_issues else None,
        f"Examiner en priorité le trip '{inconsistencies[0]['trip_id']}' avec distance de {inconsistencies[0]['distance_meters']}m" if inconsistencies else None,
        f"Revoir les seuils de distance (max: {max_distance_km}km, min: {min_distance_m}m) selon le type de réseau" if len(inconsistencies) > total_segments * 0.2 else None,
        "Vérifier l'ordre des séquences d'arrêts dans les trips problématiques" if too_far_issues else None,
        "Implémenter des contrôles de cohérence géographique lors de la création des itinéraires" if inconsistency_rate > 0.05 else None,
        f"Excellente cohérence des distances - {total_segments} segments analysés sans anomalie" if not inconsistencies and total_segments > 50 else None
    ]

    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyser la cohérence des distances entre arrêts consécutifs pour détecter les anomalies géographiques",
            "methodology": f"Calcul des distances entre arrêts successifs avec seuils {min_distance_m}m < distance < {max_distance_km}km",
            "consistency_assessment": f"{len(inconsistencies)}/{total_segments} segments avec distances aberrantes"
        },
        "recommendations": [rec for rec in recommendations if rec is not None]
    }