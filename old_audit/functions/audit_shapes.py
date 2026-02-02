"""
Fonctions d'audit pour le file_type: shapes
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="shapes",
    name="invalid_coordinates",
    genre="validity",
    description="Vérifie que les coordonnées lat/lon sont dans les limites terrestres",
    parameters={}
)
def invalid_coordinates(gtfs_data, **params):
    """
    Valide que les coordonnées des points de forme respectent les limites géographiques valides
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour valider les coordonnées géographiques"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Valide que les coordonnées des points de forme respectent les limites géographiques valides (-90≤lat≤90, -180≤lon≤180)."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide pour analyser les coordonnées géographiques."]
        }

    total_points = len(df)
    
    # Cas fichier vide
    if total_points == 0:
        return {
            "status": "warning",
            "issues": [
                {
                    "type": "empty_file",
                    "field": "shapes.txt",
                    "count": 0,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est vide - aucun point de forme défini"
                }
            ],
            "result": {
                "total_points": 0,
                "geographic_coverage": "none"
            },
            "explanation": {
                "purpose": "Valide que les coordonnées des points de forme respectent les limites géographiques valides",
                "context": "Aucun point de forme défini",
                "impact": "Pas de géométrie disponible pour les parcours"
            },
            "recommendations": [
                "Ajouter des points de forme dans shapes.txt pour définir la géométrie des parcours"
            ]
        }

    # Vérification des colonnes requises
    required_columns = ['shape_pt_lat', 'shape_pt_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "coordinate_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes de coordonnées manquantes dans shapes.txt: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Valide que les coordonnées des points de forme respectent les limites géographiques valides",
                "context": "Colonnes de coordonnées manquantes",
                "impact": "Impossible de valider les coordonnées géographiques"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Définition des limites géographiques valides
    lat_min, lat_max = -90.0, 90.0
    lon_min, lon_max = -180.0, 180.0
    
    # Analyse des coordonnées invalides
    invalid_conditions = {
        'lat_below_min': df['shape_pt_lat'] < lat_min,
        'lat_above_max': df['shape_pt_lat'] > lat_max,
        'lon_below_min': df['shape_pt_lon'] < lon_min,
        'lon_above_max': df['shape_pt_lon'] > lon_max,
        'lat_null': df['shape_pt_lat'].isna(),
        'lon_null': df['shape_pt_lon'].isna()
    }
    
    # Points avec au moins une coordonnée invalide
    any_invalid = pd.Series([False] * len(df))
    for condition in invalid_conditions.values():
        any_invalid |= condition
    
    invalid_points = df[any_invalid]
    invalid_count = len(invalid_points)
    
    # Analyse détaillée des types d'erreurs
    error_breakdown = {}
    for error_type, condition in invalid_conditions.items():
        error_count = condition.sum()
        if error_count > 0:
            error_breakdown[error_type] = {
                "count": int(error_count),
                "percentage": round(error_count / total_points * 100, 2)
            }
    
    # Analyse géographique des coordonnées valides
    valid_points = df[~any_invalid]
    geographic_analysis = {}
    
    if len(valid_points) > 0:
        valid_lats = valid_points['shape_pt_lat']
        valid_lons = valid_points['shape_pt_lon']
        
        geographic_analysis = {
            "valid_points": len(valid_points),
            "coordinate_bounds": {
                "min_latitude": round(float(valid_lats.min()), 6),
                "max_latitude": round(float(valid_lats.max()), 6),
                "min_longitude": round(float(valid_lons.min()), 6),
                "max_longitude": round(float(valid_lons.max()), 6)
            },
            "geographic_span": {
                "latitude_range": round(float(valid_lats.max() - valid_lats.min()), 6),
                "longitude_range": round(float(valid_lons.max() - valid_lons.min()), 6)
            },
            "coordinate_precision": {
                "avg_lat_precision": len(str(valid_lats.iloc[0]).split('.')[-1]) if len(valid_lats) > 0 and '.' in str(valid_lats.iloc[0]) else 0,
                "avg_lon_precision": len(str(valid_lons.iloc[0]).split('.')[-1]) if len(valid_lons) > 0 and '.' in str(valid_lons.iloc[0]) else 0
            }
        }
    
    # Calcul des métriques de qualité
    validity_rate = round((total_points - invalid_count) / total_points * 100, 2) if total_points > 0 else 100
    
    # Identification des shape_id problématiques
    problematic_shapes = []
    if 'shape_id' in invalid_points.columns:
        problematic_shape_ids = invalid_points['shape_id'].unique()
        for shape_id in problematic_shape_ids:
            shape_invalid_count = invalid_points[invalid_points['shape_id'] == shape_id].shape[0]
            problematic_shapes.append({
                "shape_id": shape_id,
                "invalid_points": shape_invalid_count
            })
    worst_shape = max(problematic_shapes, key=lambda x: x['invalid_points']) if problematic_shapes else None

    # Détermination du statut
    if invalid_count == 0:
        status = "success"
    elif validity_rate >= 99:  # ≥99% valides
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    
    if invalid_count > 0:
        affected_shape_ids = [str(shape['shape_id']) for shape in problematic_shapes] if problematic_shapes else []
        issues.append({
            "type": "invalid_coordinates",
            "field": "geographic_bounds",
            "count": invalid_count,
            "affected_ids": affected_shape_ids[:100],
            "message": f"{invalid_count} points ont des coordonnées hors limites géographiques (-90≤lat≤90, -180≤lon≤180)"
        })
        
        # Issues spécifiques par type d'erreur
        for error_type, error_data in error_breakdown.items():
            if error_data['count'] > 0:
                error_descriptions = {
                    'lat_below_min': f"{error_data['count']} points avec latitude < -90°",
                    'lat_above_max': f"{error_data['count']} points avec latitude > 90°",
                    'lon_below_min': f"{error_data['count']} points avec longitude < -180°",
                    'lon_above_max': f"{error_data['count']} points avec longitude > 180°",
                    'lat_null': f"{error_data['count']} points avec latitude manquante",
                    'lon_null': f"{error_data['count']} points avec longitude manquante"
                }
                
                if error_type in ['lat_null', 'lon_null']:
                    issue_type = "missing_data"
                else:
                    issue_type = "out_of_bounds"
                
                issues.append({
                    "type": issue_type,
                    "field": error_type,
                    "count": error_data['count'],
                    "affected_ids": [],
                    "message": error_descriptions[error_type]
                })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_points": total_points,
            "valid_points": total_points - invalid_count,
            "invalid_points": invalid_count,
            "validity_rate": validity_rate,
            "error_breakdown": error_breakdown,
            "geographic_analysis": geographic_analysis,
            "problematic_shapes": problematic_shapes[:20],  # Top 20 shapes avec erreurs
            "coordinate_validation": {
                "latitude_bounds": {"min": lat_min, "max": lat_max},
                "longitude_bounds": {"min": lon_min, "max": lon_max},
                "total_shapes_affected": len(problematic_shapes),
                "worst_shape": max(problematic_shapes, key=lambda x: x['invalid_points']) if problematic_shapes else None
            },
            "data_quality": {
                "geographic_integrity": invalid_count == 0,
                "coordinate_completeness": error_breakdown.get('lat_null', {}).get('count', 0) + error_breakdown.get('lon_null', {}).get('count', 0) == 0,
                "precision_level": geographic_analysis.get('coordinate_precision', {})
            }
        },
        "explanation": {
            "purpose": "Valide que les coordonnées des points de forme respectent les limites géographiques standards pour assurer la cohérence cartographique",
            "geographic_standards": "Limites valides: latitude [-90°, 90°], longitude [-180°, 180°]",
            "context": f"Analyse de {total_points} points de forme avec {validity_rate}% de coordonnées valides",
            "error_summary": f"Erreurs détectées: {len(error_breakdown)} types différents" if error_breakdown else "Aucune erreur géographique",
            "geographic_scope": f"Couverture: {geographic_analysis.get('coordinate_bounds', {}).get('min_latitude', 'N/A')}° à {geographic_analysis.get('coordinate_bounds', {}).get('max_latitude', 'N/A')}° lat, {geographic_analysis.get('coordinate_bounds', {}).get('min_longitude', 'N/A')}° à {geographic_analysis.get('coordinate_bounds', {}).get('max_longitude', 'N/A')}° lon" if geographic_analysis.get('coordinate_bounds') else "N/A",
            "impact": (
                f"Toutes les coordonnées respectent les standards géographiques" if status == "success"
                else f"Problèmes géographiques : {invalid_count} points avec coordonnées invalides affectant {len(problematic_shapes)} formes"
            )
        },
# Avant les recommendations, ajouter :

# Puis dans les recommendations :
        "recommendations": [
            rec for rec in [
                f"URGENT: Corriger {invalid_count} coordonnées hors limites géographiques" if invalid_count > 0 else None,
                f"Vérifier les {error_breakdown.get('lat_above_max', {}).get('count', 0)} latitudes > 90° (possibles erreurs d'unité)" if error_breakdown.get('lat_above_max', {}).get('count', 0) > 0 else None,
                f"Vérifier les {error_breakdown.get('lon_above_max', {}).get('count', 0)} longitudes > 180° (possibles erreurs d'unité)" if error_breakdown.get('lon_above_max', {}).get('count', 0) > 0 else None,
                f"Renseigner {error_breakdown.get('lat_null', {}).get('count', 0) + error_breakdown.get('lon_null', {}).get('count', 0)} coordonnées manquantes" if error_breakdown.get('lat_null', {}).get('count', 0) + error_breakdown.get('lon_null', {}).get('count', 0) > 0 else None,
                f"Examiner en priorité la forme {worst_shape['shape_id']} ({worst_shape['invalid_points']} points invalides)" if worst_shape else None,
                "Valider la cohérence du système de coordonnées utilisé (WGS84 attendu)" if invalid_count > total_points * 0.1 else None,
                "Implémenter une validation géographique dans votre processus de génération shapes.txt" if invalid_count > 0 else None,
                "Maintenir cette intégrité géographique pour assurer la qualité cartographique" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="shapes",
    name="non_monotonic_sequences",
    genre="quality",
    description="Détecte les shape_id où shape_pt_sequence n'est pas strictement croissante",
    parameters={}
)
def non_monotonic_sequences(gtfs_data, **params):
    """
    Détecte les formes avec des séquences shape_pt_sequence non strictement croissantes
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour valider les séquences des points de forme"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Détecte les formes avec des séquences shape_pt_sequence non strictement croissantes pour assurer l'ordre correct des points."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide pour analyser les séquences de points."]
        }

    total_points = len(df)
    
    # Cas fichier vide
    if total_points == 0:
        return {
            "status": "warning",
            "issues": [
                {
                    "type": "empty_file",
                    "field": "shapes.txt",
                    "count": 0,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est vide - aucune séquence à valider"
                }
            ],
            "result": {
                "total_shapes": 0,
                "total_points": 0,
                "sequence_validation": "not_applicable"
            },
            "explanation": {
                "purpose": "Détecte les formes avec des séquences shape_pt_sequence non strictement croissantes",
                "context": "Aucune forme définie",
                "impact": "Validation non applicable"
            },
            "recommendations": ["Ajouter des formes dans shapes.txt si nécessaire."]
        }

    # Vérification des colonnes requises
    required_columns = ['shape_id', 'shape_pt_sequence']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes dans shapes.txt: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les formes avec des séquences shape_pt_sequence non strictement croissantes",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de valider les séquences"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Analyse des séquences par shape_id
    total_shapes = df['shape_id'].nunique()
    problematic_shapes = []
    sequence_analysis = {}
    
    for shape_id, group in df.groupby('shape_id'):
        # Tri par shape_pt_sequence pour analyser l'ordre
        sorted_group = group.sort_values('shape_pt_sequence')
        sequences = sorted_group['shape_pt_sequence'].values
        
        # Analyse de la monotonie stricte
        is_strictly_increasing = all(sequences[i] < sequences[i+1] for i in range(len(sequences)-1))
        
        # Détection des problèmes spécifiques
        sequence_issues = []
        duplicates = []
        decreases = []
        
        if not is_strictly_increasing:
            # Identification des doublons
            unique_sequences = set()
            for i, seq in enumerate(sequences):
                if seq in unique_sequences:
                    duplicates.append({"position": i, "sequence": seq})
                unique_sequences.add(seq)
            
            # Identification des diminutions
            for i in range(len(sequences)-1):
                if sequences[i] >= sequences[i+1]:
                    decreases.append({
                        "position": i,
                        "current_sequence": sequences[i],
                        "next_sequence": sequences[i+1]
                    })
            
            # Compilation des problèmes
            if duplicates:
                sequence_issues.append("duplicate_sequences")
            if decreases:
                sequence_issues.append("non_increasing")
            
            problematic_shapes.append({
                "shape_id": shape_id,
                "total_points": len(sequences),
                "issues": sequence_issues,
                "duplicate_count": len(duplicates),
                "decrease_count": len(decreases),
                "first_sequence": int(sequences[0]) if len(sequences) > 0 else None,
                "last_sequence": int(sequences[-1]) if len(sequences) > 0 else None,
                "sequence_range": int(sequences[-1] - sequences[0]) if len(sequences) > 1 else 0
            })
        
        # Statistiques de séquence pour toutes les formes
        sequence_analysis[shape_id] = {
            "points_count": len(sequences),
            "is_valid": is_strictly_increasing,
            "min_sequence": int(sequences.min()) if len(sequences) > 0 else None,
            "max_sequence": int(sequences.max()) if len(sequences) > 0 else None,
            "sequence_gaps": len(sequences) - 1 - (sequences.max() - sequences.min()) if len(sequences) > 1 else 0,
            "avg_sequence_step": round((sequences.max() - sequences.min()) / (len(sequences) - 1), 2) if len(sequences) > 1 else 0
        }

    # Calcul des métriques globales
    problematic_count = len(problematic_shapes)
    valid_shapes = total_shapes - problematic_count
    validity_rate = round(valid_shapes / total_shapes * 100, 2) if total_shapes > 0 else 100
    
    # Analyse globale des patterns d'erreur
    total_duplicates = sum(shape['duplicate_count'] for shape in problematic_shapes)
    total_decreases = sum(shape['decrease_count'] for shape in problematic_shapes)
    
    # Détermination du statut
    if problematic_count == 0:
        status = "success"
    elif validity_rate >= 95:  # ≥95% de formes valides
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    
    if problematic_count > 0:
        problematic_shape_ids = [shape['shape_id'] for shape in problematic_shapes]
        issues.append({
            "type": "invalid_sequence",
            "field": "shape_pt_sequence",
            "count": problematic_count,
            "affected_ids": problematic_shape_ids[:100],
            "message": f"{problematic_count} formes ont des séquences shape_pt_sequence non strictement croissantes"
        })
        
        # Issues spécifiques par type de problème
        if total_duplicates > 0:
            shapes_with_duplicates = [shape['shape_id'] for shape in problematic_shapes if shape['duplicate_count'] > 0]
            issues.append({
                "type": "duplicate_sequence",
                "field": "shape_pt_sequence",
                "count": total_duplicates,
                "affected_ids": shapes_with_duplicates[:50],
                "message": f"{total_duplicates} séquences dupliquées détectées dans {len(shapes_with_duplicates)} formes"
            })
        
        if total_decreases > 0:
            shapes_with_decreases = [shape['shape_id'] for shape in problematic_shapes if shape['decrease_count'] > 0]
            issues.append({
                "type": "decreasing_sequence",
                "field": "shape_pt_sequence",
                "count": total_decreases,
                "affected_ids": shapes_with_decreases[:50],
                "message": f"{total_decreases} diminutions de séquence détectées dans {len(shapes_with_decreases)} formes"
            })

    # Analyse des performances des séquences valides
    valid_sequence_stats = {}
    if valid_shapes > 0:
        valid_analysis = {k: v for k, v in sequence_analysis.items() if v['is_valid']}
        if valid_analysis:
            avg_points_per_shape = round(sum(data['points_count'] for data in valid_analysis.values()) / len(valid_analysis), 1)
            avg_sequence_step = round(sum(data['avg_sequence_step'] for data in valid_analysis.values()) / len(valid_analysis), 2)
            
            valid_sequence_stats = {
                "avg_points_per_shape": avg_points_per_shape,
                "avg_sequence_step": avg_sequence_step,
                "min_shape_points": min(data['points_count'] for data in valid_analysis.values()),
                "max_shape_points": max(data['points_count'] for data in valid_analysis.values())
            }

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_points": total_points,
            "valid_shapes": valid_shapes,
            "problematic_shapes": problematic_count,
            "validity_rate": validity_rate,
            "sequence_problems": {
                "shapes_with_duplicates": len([s for s in problematic_shapes if s['duplicate_count'] > 0]),
                "shapes_with_decreases": len([s for s in problematic_shapes if s['decrease_count'] > 0]),
                "total_duplicate_sequences": total_duplicates,
                "total_sequence_decreases": total_decreases
            },
            "problematic_shapes_details": problematic_shapes[:20],  # Top 20 shapes problématiques
            "valid_sequence_stats": valid_sequence_stats,
            "sequence_quality": {
                "monotonic_compliance": problematic_count == 0,
                "sequence_integrity": total_duplicates == 0 and total_decreases == 0,
                "overall_quality": (
                    "excellent" if validity_rate == 100
                    else "good" if validity_rate >= 95
                    else "poor"
                )
            }
        },
        "explanation": {
            "purpose": "Détecte les formes avec des séquences shape_pt_sequence non strictement croissantes pour assurer l'ordre correct et l'intégrité des parcours",
            "sequence_requirement": "Les shape_pt_sequence doivent être strictement croissantes pour définir l'ordre des points le long du parcours",
            "context": f"Analyse de {total_shapes} formes avec {total_points} points, {validity_rate}% de formes valides",
            "problem_summary": f"Problèmes détectés: {total_duplicates} doublons, {total_decreases} diminutions de séquence",
            "impact": (
                f"Toutes les séquences de formes sont correctement ordonnées" if status == "success"
                else f"Problèmes d'ordre détectés : {problematic_count} formes avec séquences incorrectes"
            )
        },
        "recommendations": [
            rec for rec in [
                f"URGENT: Corriger {problematic_count} formes avec séquences non monotones" if problematic_count > 0 else None,
                f"Éliminer {total_duplicates} séquences dupliquées dans les formes" if total_duplicates > 0 else None,
                f"Corriger {total_decreases} diminutions de séquence (ordre inversé)" if total_decreases > 0 else None,
                f"Examiner en priorité les formes avec le plus de problèmes: {max(problematic_shapes, key=lambda x: x['duplicate_count'] + x['decrease_count'])['shape_id'] if problematic_shapes else 'N/A'}" if problematic_shapes else None,
                "Renuméroter les séquences pour assurer une progression strictement croissante (ex: 0, 1, 2, 3...)" if problematic_count > 0 else None,
                "Vérifier l'ordre de saisie des points lors de la création des formes" if total_decreases > 0 else None,
                "Implémenter une validation de monotonie dans votre processus de génération shapes.txt" if problematic_count > 0 else None,
                "Maintenir cette intégrité des séquences pour assurer la qualité des parcours" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="shapes",
    name="duplicate_points_in_shape",
    genre="quality",
    description="Détecte points identiques dupliqués dans un même shape_id",
    parameters={}
)
def duplicate_points_in_shape(gtfs_data, **params):
    """
    Détecte les points dupliqués dans les formes basés sur shape_id, coordonnées et séquence
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour détecter les points dupliqués"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Détecte les points dupliqués dans les formes pour optimiser la géométrie des parcours."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide pour analyser les doublons de points."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "warning",
            "issues": [
                {
                    "type": "empty_file",
                    "field": "shapes.txt",
                    "count": 0,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est vide"
                }
            ],
            "result": {
                "total_points": 0,
                "duplicate_analysis": "not_applicable"
            },
            "explanation": {
                "purpose": "Détecte les points dupliqués dans les formes pour optimiser la géométrie des parcours",
                "context": "Aucun point de forme défini",
                "impact": "Validation non applicable"
            },
            "recommendations": ["Ajouter des points de forme dans shapes.txt si nécessaire."]
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les points dupliqués dans les formes pour optimiser la géométrie des parcours",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les doublons"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Détection des doublons stricts
    duplicated_mask = df.duplicated(subset=['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'], keep=False)
    duplicated_points = df[duplicated_mask]
    duplicate_count = len(duplicated_points)
    
    # Analyse par forme
    shapes_with_duplicates = []
    duplicate_groups = {}
    
    if duplicate_count > 0:
        for shape_id, group in duplicated_points.groupby('shape_id'):
            duplicate_sets = group.groupby(['shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence']).size()
            duplicate_details = []
            
            for (lat, lon, seq), count in duplicate_sets.items():
                if count > 1:
                    duplicate_details.append({
                        "coordinates": [lat, lon],
                        "sequence": seq,
                        "duplicate_count": count
                    })
            
            if duplicate_details:
                shapes_with_duplicates.append({
                    "shape_id": shape_id,
                    "total_duplicates": len(group),
                    "duplicate_sets": len(duplicate_details),
                    "details": duplicate_details
                })
                
                duplicate_groups[shape_id] = duplicate_details

    # Analyse des coordonnées dupliquées sans séquence
    coordinate_duplicates = df.duplicated(subset=['shape_id', 'shape_pt_lat', 'shape_pt_lon'], keep=False)
    coord_duplicate_count = coordinate_duplicates.sum()
    
    # Calcul des métriques
    total_shapes = df['shape_id'].nunique()
    affected_shapes = len(shapes_with_duplicates)
    duplication_rate = round(duplicate_count / total_points * 100, 2) if total_points > 0 else 0
    efficiency_gain = duplicate_count - len(shapes_with_duplicates) if duplicate_count > 0 else 0

    if duplicate_count == 0:
        status = "success"
    elif duplication_rate <= 1:
        status = "warning"
    else:
        status = "error"

    issues = []
    
    if duplicate_count > 0:
        affected_shape_ids = [shape['shape_id'] for shape in shapes_with_duplicates]
        issues.append({
            "type": "duplicate_data",
            "field": "shape_points",
            "count": duplicate_count,
            "affected_ids": affected_shape_ids[:100],
            "message": f"{duplicate_count} points strictement dupliqués dans {affected_shapes} formes"
        })
    
    if coord_duplicate_count > duplicate_count:
        issues.append({
            "type": "coordinate_duplicate",
            "field": "coordinates",
            "count": coord_duplicate_count - duplicate_count,
            "affected_ids": [],
            "message": f"{coord_duplicate_count - duplicate_count} points avec coordonnées identiques mais séquences différentes"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_points": total_points,
            "total_shapes": total_shapes,
            "duplicate_points": duplicate_count,
            "unique_points": total_points - duplicate_count,
            "affected_shapes": affected_shapes,
            "duplication_rate": duplication_rate,
            "duplicate_analysis": {
                "shapes_with_duplicates": shapes_with_duplicates[:10],
                "coordinate_duplicates": int(coord_duplicate_count),
                "strict_duplicates": duplicate_count,
                "efficiency_gain": efficiency_gain
            },
            "optimization_potential": {
                "removable_points": duplicate_count,
                "optimized_size": total_points - duplicate_count,
                "compression_ratio": round((total_points - duplicate_count) / total_points, 3) if total_points > 0 else 1,
                "storage_savings_percentage": round(duplicate_count / total_points * 100, 2) if total_points > 0 else 0
            },
            "geometry_quality": {
                "point_uniqueness": duplicate_count == 0,
                "coordinate_precision": coord_duplicate_count == 0,
                "redundancy_level": (
                    "none" if duplicate_count == 0
                    else "minimal" if duplication_rate <= 1
                    else "significant"
                )
            }
        },
        "explanation": {
            "purpose": "Détecte les points strictement dupliqués (même shape_id, coordonnées et séquence) pour optimiser la géométrie et réduire la redondance",
            "context": f"Analyse de {total_points} points dans {total_shapes} formes avec {duplication_rate}% de redondance",
            "duplication_summary": f"Points dupliqués: {duplicate_count} affectant {affected_shapes} formes",
            "optimization_impact": f"Potentiel d'optimisation: {round(duplicate_count / total_points * 100, 2)}% d'espace récupérable",
            "impact": (
                f"Géométrie optimisée sans redondance de points" if status == "success"
                else f"Redondance détectée : {duplicate_count} points dupliqués réductibles"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Supprimer {duplicate_count} points strictement dupliqués pour optimiser la géométrie" if duplicate_count > 0 else None,
                f"Examiner les {coord_duplicate_count - duplicate_count} points avec coordonnées identiques mais séquences différentes" if coord_duplicate_count > duplicate_count else None,
                f"Prioriser la forme {max(shapes_with_duplicates, key=lambda x: x['total_duplicates'])['shape_id']} ({max(shapes_with_duplicates, key=lambda x: x['total_duplicates'])['total_duplicates']} doublons)" if shapes_with_duplicates else None,
                f"Récupérer {round(duplicate_count / total_points * 100, 2)}% d'espace de stockage en éliminant les doublons" if duplicate_count > total_points * 0.05 else None,
                "Implémenter une validation de déduplication dans votre processus de génération shapes.txt" if duplicate_count > 0 else None,
                "Maintenir cette efficacité géométrique sans redondance" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="shapes",
    name="shape_total_distance_stats",
    genre="statistics",
    description="Calcule la distance totale parcourue par chaque shape_id",
    parameters={}
)
def shape_total_distance_stats(gtfs_data, **params):
    """
    Calcule les statistiques de distance totale pour chaque forme géométrique
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour calculer les distances des formes"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Calcule les statistiques de distance totale pour analyser la géométrie des parcours."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide pour analyser les distances."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "warning",
            "issues": [
                {
                    "type": "empty_file",
                    "field": "shapes.txt",
                    "count": 0,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est vide"
                }
            ],
            "result": {
                "total_shapes": 0,
                "distance_analysis": "not_applicable"
            },
            "explanation": {
                "purpose": "Calcule les statistiques de distance totale pour analyser la géométrie des parcours",
                "context": "Aucune forme définie",
                "impact": "Calcul de distance impossible"
            },
            "recommendations": ["Ajouter des formes dans shapes.txt pour analyser les distances."]
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Calcule les statistiques de distance totale pour analyser la géométrie des parcours",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de calculer les distances"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Tri des données par forme et séquence
    df_sorted = df.sort_values(['shape_id', 'shape_pt_sequence'])
    
    # Calcul des distances par forme
    shape_distances = []
    shape_details = []
    processing_errors = []
    
    for shape_id, group in df_sorted.groupby('shape_id'):
        try:
            # Vérification de la validité des coordonnées
            valid_coords = group.dropna(subset=['shape_pt_lat', 'shape_pt_lon'])
            if len(valid_coords) < 2:
                processing_errors.append(f"{shape_id}: moins de 2 points valides")
                continue
            
            # Extraction des coordonnées
            coords = list(zip(valid_coords['shape_pt_lat'], valid_coords['shape_pt_lon']))
            
            # Calcul de la distance totale
            from geopy.distance import geodesic
            total_distance = 0
            segment_distances = []
            
            for i in range(1, len(coords)):
                try:
                    segment_dist = geodesic(coords[i-1], coords[i]).meters
                    segment_distances.append(segment_dist)
                    total_distance += segment_dist
                except Exception as e:
                    processing_errors.append(f"{shape_id}: erreur segment {i}")
                    continue
            
            shape_distances.append(total_distance)
            
            # Détails de la forme
            shape_details.append({
                "shape_id": shape_id,
                "total_distance_meters": round(total_distance, 2),
                "total_distance_km": round(total_distance / 1000, 3),
                "point_count": len(coords),
                "segment_count": len(segment_distances),
                "avg_segment_length": round(sum(segment_distances) / len(segment_distances), 2) if segment_distances else 0,
                "max_segment_length": round(max(segment_distances), 2) if segment_distances else 0,
                "min_segment_length": round(min(segment_distances), 2) if segment_distances else 0
            })
            
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Calcul des statistiques globales
    total_shapes = df_sorted['shape_id'].nunique()
    successful_calculations = len(shape_distances)
    
    if not shape_distances:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "calculation_error",
                    "field": "distance_computation",
                    "count": len(processing_errors),
                    "affected_ids": [],
                    "message": f"Aucune distance calculable - {len(processing_errors)} erreurs de traitement"
                }
            ],
            "result": {
                "total_shapes": total_shapes,
                "successful_calculations": 0,
                "processing_errors": processing_errors[:10]
            },
            "explanation": {
                "purpose": "Calcule les statistiques de distance totale pour analyser la géométrie des parcours",
                "context": "Erreurs de calcul pour toutes les formes",
                "impact": "Aucune statistique de distance disponible"
            },
            "recommendations": [
                "Vérifier la validité des coordonnées dans shapes.txt",
                "Corriger les formes avec moins de 2 points valides"
            ]
        }

    # Statistiques descriptives
    import statistics
    distance_stats = {
        "count": successful_calculations,
        "min_meters": round(min(shape_distances), 2),
        "max_meters": round(max(shape_distances), 2),
        "avg_meters": round(statistics.mean(shape_distances), 2),
        "median_meters": round(statistics.median(shape_distances), 2),
        "total_network_meters": round(sum(shape_distances), 2),
        "total_network_km": round(sum(shape_distances) / 1000, 2),
        "std_deviation": round(statistics.stdev(shape_distances), 2) if len(shape_distances) > 1 else 0
    }

    # Classification des formes par distance
    distance_distribution = {
        "very_short": len([d for d in shape_distances if d < 1000]),     # < 1 km
        "short": len([d for d in shape_distances if 1000 <= d < 5000]),  # 1-5 km
        "medium": len([d for d in shape_distances if 5000 <= d < 20000]), # 5-20 km
        "long": len([d for d in shape_distances if 20000 <= d < 50000]),  # 20-50 km
        "very_long": len([d for d in shape_distances if d >= 50000])      # > 50 km
    }

    # Détermination du statut
    success_rate = round(successful_calculations / total_shapes * 100, 2) if total_shapes > 0 else 0
    
    if processing_errors and success_rate < 90:
        status = "error"
    elif processing_errors:
        status = "warning"
    else:
        status = "success"

    # Construction des issues
    issues = []
    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "distance_calculation",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} formes n'ont pas pu être analysées"
        })
    longest_shape = max(shape_details, key=lambda x: x['total_distance_meters']) if shape_details else None
    shortest_shape = min(shape_details, key=lambda x: x['total_distance_meters']) if shape_details else None
    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_points": total_points,
            "successful_calculations": successful_calculations,
            "calculation_success_rate": success_rate,
            "distance_statistics": distance_stats,
            "distance_distribution": distance_distribution,
            "shape_details": shape_details[:20],  # Top 20 formes
            "network_analysis": {
                "longest_shape": max(shape_details, key=lambda x: x['total_distance_meters']) if shape_details else None,
                "shortest_shape": min(shape_details, key=lambda x: x['total_distance_meters']) if shape_details else None,
                "avg_points_per_shape": round(sum(s['point_count'] for s in shape_details) / len(shape_details), 1) if shape_details else 0,
                "total_segments": sum(s['segment_count'] for s in shape_details)
            },
            "quality_metrics": {
                "processing_reliability": success_rate,
                "data_completeness": len(processing_errors) == 0,
                "geometric_diversity": len([v for v in distance_distribution.values() if v > 0])
            }
        },
        "explanation": {
            "purpose": "Calcule les statistiques de distance totale des formes géométriques pour évaluer l'étendue et la diversité du réseau",
            "context": f"Analyse de {total_shapes} formes avec {success_rate}% de calculs réussis",
            "distance_summary": f"Réseau total: {distance_stats['total_network_km']} km, moyenne: {distance_stats['avg_meters']}m par forme",
            "network_scope": f"Formes de {distance_stats['min_meters']}m à {distance_stats['max_meters']}m",
            "impact": (
                f"Analyse complète du réseau géométrique: {distance_stats['total_network_km']} km couverts" if status == "success"
                else f"Analyse partielle: {len(processing_errors)} formes non analysables"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Corriger {len(processing_errors)} formes avec erreurs de calcul" if processing_errors else None,
                f"Examiner la forme la plus longue: {longest_shape['shape_id']} ({longest_shape['total_distance_km']} km)" if longest_shape else None,
                f"Vérifier les {distance_distribution['very_short']} formes très courtes (<1km)" if distance_distribution['very_short'] > total_shapes * 0.3 else None,
                f"Optimiser les formes avec segments irréguliers (écart-type: {distance_stats['std_deviation']}m)" if distance_stats['std_deviation'] > distance_stats['avg_meters'] else None,
                "Valider la précision géographique pour améliorer les calculs de distance" if len(processing_errors) > 0 else None,
                "Maintenir cette qualité géométrique pour des analyses de distance fiables" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="shapes",
    name="closed_loop_shapes",
    genre="quality",
    description="Identifie les shape_id qui reviennent au point de départ (boucle fermée)",
    parameters={"tolerance_meters": {"type": "number", "default": 10.0}}
)
def closed_loop_shapes(gtfs_data, tolerance_meters=10.0, **params):
    """
    Détecte les formes géométriques fermées (boucles) avec tolérance configurable
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour détecter les boucles fermées"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Détecte les formes géométriques fermées (boucles) pour identifier les parcours circulaires."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide pour analyser les boucles."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "total_shapes": 0,
                "closed_loops": [],
                "closed_loops_count": 0
            },
            "explanation": {
                "purpose": "Détecte les formes géométriques fermées (boucles) pour identifier les parcours circulaires",
                "context": "Aucune forme définie",
                "impact": "Analyse non applicable"
            },
            "recommendations": []
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les formes géométriques fermées (boucles) pour identifier les parcours circulaires",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les boucles"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Analyse des boucles fermées
    closed_loops = []
    processing_errors = []
    total_shapes = df['shape_id'].nunique()
    
    for shape_id, group in df.groupby('shape_id'):
        try:
            # Tri par séquence si disponible
            if 'shape_pt_sequence' in group.columns:
                group = group.sort_values('shape_pt_sequence')
            
            coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
            
            if len(coords) < 3:  # Une boucle nécessite au moins 3 points
                continue
            
            # Calcul de la distance entre premier et dernier point
            from geopy.distance import geodesic
            start_point = coords[0]
            end_point = coords[-1]
            
            loop_distance = geodesic(start_point, end_point).meters
            
            if loop_distance <= tolerance_meters:
                # Calcul de la longueur totale de la forme
                total_distance = 0
                for i in range(1, len(coords)):
                    total_distance += geodesic(coords[i-1], coords[i]).meters
                
                closed_loops.append({
                    "shape_id": shape_id,
                    "start_point": list(start_point),
                    "end_point": list(end_point),
                    "closure_distance_meters": round(loop_distance, 2),
                    "total_length_meters": round(total_distance, 2),
                    "total_length_km": round(total_distance / 1000, 3),
                    "point_count": len(coords),
                    "closure_ratio": round(loop_distance / total_distance * 100, 4) if total_distance > 0 else 0
                })
                
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Calcul des métriques
    loops_count = len(closed_loops)
    loop_rate = round(loops_count / total_shapes * 100, 2) if total_shapes > 0 else 0
    
    # Analyse des boucles détectées
    loop_analysis = {}
    if closed_loops:
        closure_distances = [loop['closure_distance_meters'] for loop in closed_loops]
        total_lengths = [loop['total_length_meters'] for loop in closed_loops]
        
        loop_analysis = {
            "avg_closure_distance": round(sum(closure_distances) / len(closure_distances), 2),
            "max_closure_distance": round(max(closure_distances), 2),
            "avg_loop_length": round(sum(total_lengths) / len(total_lengths), 2),
            "longest_loop": max(closed_loops, key=lambda x: x['total_length_meters']),
            "tightest_closure": min(closed_loops, key=lambda x: x['closure_distance_meters'])
        }

    # Détermination du statut
    if processing_errors and len(processing_errors) > total_shapes * 0.1:
        status = "error"
    elif processing_errors:
        status = "warning"
    else:
        status = "success"

    # Construction des issues
    issues = []
    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "loop_detection",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} formes n'ont pas pu être analysées"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_points": total_points,
            "closed_loops_count": loops_count,
            "closed_loops": closed_loops,
            "loop_rate": loop_rate,
            "tolerance_meters": tolerance_meters,
            "loop_analysis": loop_analysis,
            "processing_stats": {
                "successfully_analyzed": total_shapes - len(processing_errors),
                "processing_errors": len(processing_errors),
                "analysis_coverage": round((total_shapes - len(processing_errors)) / total_shapes * 100, 2) if total_shapes > 0 else 0
            }
        },
        "explanation": {
            "purpose": "Détecte les formes géométriques fermées (boucles) pour identifier les parcours circulaires avec tolérance configurable",
            "tolerance_setting": f"Tolérance de fermeture: {tolerance_meters}m",
            "context": f"Analyse de {total_shapes} formes avec {loop_rate}% de boucles fermées détectées",
            "loop_summary": f"Boucles trouvées: {loops_count} avec fermeture moyenne de {loop_analysis.get('avg_closure_distance', 0)}m",
            "impact": (
                f"Détection complète: {loops_count} boucles fermées identifiées" if status == "success"
                else f"Analyse partielle: {len(processing_errors)} formes non analysables"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Corriger {len(processing_errors)} formes avec erreurs d'analyse" if processing_errors else None,
                f"Examiner la boucle la plus longue: {loop_analysis.get('longest_loop', {}).get('shape_id')} ({loop_analysis.get('longest_loop', {}).get('total_length_km')} km)" if loop_analysis.get('longest_loop') else None,
                f"Vérifier la fermeture la plus lâche: {loop_analysis.get('tightest_closure', {}).get('shape_id')} ({loop_analysis.get('tightest_closure', {}).get('closure_distance_meters')}m)" if loop_analysis.get('tightest_closure') else None,
                f"Considérer ajuster la tolérance (actuellement {tolerance_meters}m) selon vos besoins" if loops_count > 0 else None,
                "Valider que les boucles correspondent à des parcours circulaires réels" if loops_count > 0 else None,
                "Maintenir cette qualité géométrique pour une détection fiable des boucles" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="shapes",
    name="large_jumps_in_shapes",
    genre="quality",
    description=" Détecte les sauts importants entre points consécutifs dans les formes géométriques",
    parameters={"tolerance_meters": {"type": "number", "default": 1000.0}}
)
def large_jumps_in_shapes(gtfs_data, distance_threshold=1000.0, **params):
    """
    Détecte les sauts importants entre points consécutifs dans les formes géométriques
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour détecter les sauts de distance"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Détecte les sauts importants entre points consécutifs pour identifier les anomalies géométriques."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide pour analyser les sauts de distance."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "total_shapes": 0,
                "shapes_with_jumps": [],
                "shapes_with_large_jumps_count": 0
            },
            "explanation": {
                "purpose": "Détecte les sauts importants entre points consécutifs pour identifier les anomalies géométriques",
                "context": "Aucune forme définie",
                "impact": "Analyse non applicable"
            },
            "recommendations": []
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les sauts importants entre points consécutifs pour identifier les anomalies géométriques",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les sauts"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Analyse des sauts de distance
    shapes_with_jumps = []
    all_jump_details = []
    processing_errors = []
    total_shapes = df['shape_id'].nunique()
    
    for shape_id, group in df.groupby('shape_id'):
        try:
            # Tri par séquence si disponible
            if 'shape_pt_sequence' in group.columns:
                group = group.sort_values('shape_pt_sequence')
            
            coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
            
            if len(coords) < 2:
                continue
            
            # Calcul des distances entre points consécutifs
            from geopy.distance import geodesic
            jumps_in_shape = []
            
            for i in range(1, len(coords)):
                segment_distance = geodesic(coords[i-1], coords[i]).meters
                
                if segment_distance > distance_threshold:
                    jump_detail = {
                        "segment_index": i,
                        "from_point": list(coords[i-1]),
                        "to_point": list(coords[i]),
                        "distance_meters": round(segment_distance, 2),
                        "distance_km": round(segment_distance / 1000, 3)
                    }
                    jumps_in_shape.append(jump_detail)
                    all_jump_details.append({
                        "shape_id": shape_id,
                        **jump_detail
                    })
            
            if jumps_in_shape:
                max_jump = max(jumps_in_shape, key=lambda x: x['distance_meters'])
                total_shape_length = sum(geodesic(coords[i-1], coords[i]).meters for i in range(1, len(coords)))
                
                shapes_with_jumps.append({
                    "shape_id": shape_id,
                    "jump_count": len(jumps_in_shape),
                    "max_jump_meters": max_jump['distance_meters'],
                    "max_jump_km": max_jump['distance_km'],
                    "total_jump_distance": round(sum(j['distance_meters'] for j in jumps_in_shape), 2),
                    "total_shape_length": round(total_shape_length, 2),
                    "jump_ratio": round(sum(j['distance_meters'] for j in jumps_in_shape) / total_shape_length * 100, 2) if total_shape_length > 0 else 0,
                    "jumps_details": jumps_in_shape
                })
                
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Calcul des métriques
    shapes_with_jumps_count = len(shapes_with_jumps)
    jump_rate = round(shapes_with_jumps_count / total_shapes * 100, 2) if total_shapes > 0 else 0
    total_jumps = len(all_jump_details)
    
    # Analyse des sauts détectés
    jump_analysis = {}
    if all_jump_details:
        jump_distances = [jump['distance_meters'] for jump in all_jump_details]
        
        jump_analysis = {
            "total_jumps": total_jumps,
            "avg_jump_distance": round(sum(jump_distances) / len(jump_distances), 2),
            "max_jump_distance": round(max(jump_distances), 2),
            "min_jump_distance": round(min(jump_distances), 2),
            "worst_shape": max(shapes_with_jumps, key=lambda x: x['max_jump_meters']) if shapes_with_jumps else None,
            "jump_distribution": {
                "moderate": len([d for d in jump_distances if distance_threshold <= d < distance_threshold * 2]),
                "large": len([d for d in jump_distances if distance_threshold * 2 <= d < distance_threshold * 5]),
                "extreme": len([d for d in jump_distances if d >= distance_threshold * 5])
            }
        }

    # Détermination du statut
    if total_jumps > 0:
        status = "error"
    elif processing_errors:
        status = "warning"
    else:
        status = "success"

    # Construction des issues
    issues = []
    
    if shapes_with_jumps_count > 0:
        affected_shape_ids = [shape['shape_id'] for shape in shapes_with_jumps]
        issues.append({
            "type": "large_distance_jump",
            "field": "segment_continuity",
            "count": total_jumps,
            "affected_ids": affected_shape_ids[:100],
            "message": f"{total_jumps} sauts de distance >{distance_threshold}m détectés dans {shapes_with_jumps_count} formes"
        })
    
    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "jump_detection",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} formes n'ont pas pu être analysées"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_points": total_points,
            "shapes_with_large_jumps_count": shapes_with_jumps_count,
            "shapes_with_jumps": shapes_with_jumps[:20],  # Top 20 formes avec sauts
            "jump_rate": jump_rate,
            "distance_threshold": distance_threshold,
            "jump_analysis": jump_analysis,
            "all_jumps": all_jump_details[:50],  # Top 50 sauts les plus importants
            "processing_stats": {
                "successfully_analyzed": total_shapes - len(processing_errors),
                "processing_errors": len(processing_errors),
                "analysis_coverage": round((total_shapes - len(processing_errors)) / total_shapes * 100, 2) if total_shapes > 0 else 0
            }
        },
        "explanation": {
            "purpose": "Détecte les sauts importants entre points consécutifs pour identifier les anomalies géométriques et discontinuités",
            "threshold_setting": f"Seuil de détection: {distance_threshold}m",
            "context": f"Analyse de {total_shapes} formes avec {jump_rate}% présentant des sauts significatifs",
            "jump_summary": f"Sauts détectés: {total_jumps} avec distance moyenne de {jump_analysis.get('avg_jump_distance', 0)}m",
            "impact": (
                f"Géométrie continue : aucun saut significatif détecté" if status == "success"
                else f"Discontinuités géométriques : {total_jumps} sauts dans {shapes_with_jumps_count} formes"
            )
        },
        "recommendations": [
            rec for rec in [
                f"URGENT: Corriger {total_jumps} sauts de distance >{distance_threshold}m" if total_jumps > 0 else None,
                f"Examiner la forme la plus problématique: {jump_analysis.get('worst_shape', {}).get('shape_id')} (saut max: {jump_analysis.get('worst_shape', {}).get('max_jump_km')} km)" if jump_analysis.get('worst_shape') else None,
                f"Traiter en priorité les {jump_analysis.get('jump_distribution', {}).get('extreme', 0)} sauts extrêmes (>{distance_threshold * 5}m)" if jump_analysis.get('jump_distribution', {}).get('extreme', 0) > 0 else None,
                f"Vérifier la continuité géographique des {shapes_with_jumps_count} formes affectées" if shapes_with_jumps_count > 0 else None,
                f"Considérer ajuster le seuil de détection (actuellement {distance_threshold}m)" if total_jumps > total_shapes * 0.5 else None,
                f"Corriger {len(processing_errors)} formes avec erreurs d'analyse" if processing_errors else None,
                "Maintenir cette continuité géométrique pour assurer la qualité des parcours" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="shapes",
    name="backtracking_detection",
    genre="quality",
    description="Détecte retours en arrière dans lat/lon (changements de sens brusques)",
    parameters={"threshold_deg": {"type": "number", "default": 0.001}}
)
def backtracking_detection(gtfs_data, threshold_deg=0.001, **params):
    """
    Détecte les formes avec des segments de retour en arrière (backtracking) basé sur les changements de direction
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour détecter le backtracking"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Détecte les formes avec des segments de retour en arrière (backtracking) pour identifier les anomalies de parcours."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide pour analyser le backtracking."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "total_shapes": 0,
                "backtracking_shapes": [],
                "count": 0
            },
            "explanation": {
                "purpose": "Détecte les formes avec des segments de retour en arrière (backtracking) pour identifier les anomalies de parcours",
                "context": "Aucune forme définie",
                "impact": "Analyse non applicable"
            },
            "recommendations": []
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les formes avec des segments de retour en arrière (backtracking) pour identifier les anomalies de parcours",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter le backtracking"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Tri des données par forme et séquence
    if 'shape_pt_sequence' in df.columns:
        df_sorted = df.sort_values(['shape_id', 'shape_pt_sequence'])
    else:
        df_sorted = df.sort_values('shape_id')

    # Analyse du backtracking par forme
    problematic_shapes = []
    backtracking_details = []
    processing_errors = []
    total_shapes = df_sorted['shape_id'].nunique()
    
    for shape_id, group in df_sorted.groupby('shape_id'):
        try:
            if len(group) < 3:  # Besoin d'au moins 3 points pour détecter backtracking
                continue
            
            lats = group['shape_pt_lat'].values
            lons = group['shape_pt_lon'].values
            
            # Calcul des différences consécutives
            lat_diffs = lats[1:] - lats[:-1]
            lon_diffs = lons[1:] - lons[:-1]
            
            # Détection du backtracking par changement de signe significatif
            lat_backtracks = []
            lon_backtracks = []
            
            for i in range(len(lat_diffs) - 1):
                # Backtracking latitude
                if (lat_diffs[i] * lat_diffs[i+1] < 0 and 
                    abs(lat_diffs[i]) > threshold_deg and 
                    abs(lat_diffs[i+1]) > threshold_deg):
                    lat_backtracks.append({
                        "segment_index": i + 1,
                        "lat_change_1": round(lat_diffs[i], 6),
                        "lat_change_2": round(lat_diffs[i+1], 6),
                        "reversal_magnitude": round(abs(lat_diffs[i]) + abs(lat_diffs[i+1]), 6)
                    })
                
                # Backtracking longitude
                if (lon_diffs[i] * lon_diffs[i+1] < 0 and 
                    abs(lon_diffs[i]) > threshold_deg and 
                    abs(lon_diffs[i+1]) > threshold_deg):
                    lon_backtracks.append({
                        "segment_index": i + 1,
                        "lon_change_1": round(lon_diffs[i], 6),
                        "lon_change_2": round(lon_diffs[i+1], 6),
                        "reversal_magnitude": round(abs(lon_diffs[i]) + abs(lon_diffs[i+1]), 6)
                    })
            
            # Si backtracking détecté
            if lat_backtracks or lon_backtracks:
                # Calcul de la sévérité du backtracking
                total_backtracks = len(lat_backtracks) + len(lon_backtracks)
                max_lat_reversal = max([bt['reversal_magnitude'] for bt in lat_backtracks]) if lat_backtracks else 0
                max_lon_reversal = max([bt['reversal_magnitude'] for bt in lon_backtracks]) if lon_backtracks else 0
                
                shape_detail = {
                    "shape_id": shape_id,
                    "total_points": len(group),
                    "lat_backtracks": len(lat_backtracks),
                    "lon_backtracks": len(lon_backtracks),
                    "total_backtracks": total_backtracks,
                    "max_lat_reversal": max_lat_reversal,
                    "max_lon_reversal": max_lon_reversal,
                    "severity": "high" if max(max_lat_reversal, max_lon_reversal) > threshold_deg * 10 else "medium" if max(max_lat_reversal, max_lon_reversal) > threshold_deg * 5 else "low",
                    "lat_backtrack_details": lat_backtracks,
                    "lon_backtrack_details": lon_backtracks
                }
                
                problematic_shapes.append(shape_detail)
                backtracking_details.extend([
                    {"shape_id": shape_id, "type": "latitude", **bt} for bt in lat_backtracks
                ])
                backtracking_details.extend([
                    {"shape_id": shape_id, "type": "longitude", **bt} for bt in lon_backtracks
                ])
                
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Calcul des métriques
    problematic_count = len(problematic_shapes)
    backtracking_rate = round(problematic_count / total_shapes * 100, 2) if total_shapes > 0 else 0
    total_backtracks = len(backtracking_details)
    
    # Analyse de sévérité
    severity_analysis = {}
    if problematic_shapes:
        severity_counts = {"low": 0, "medium": 0, "high": 0}
        for shape in problematic_shapes:
            severity_counts[shape["severity"]] += 1
        
        severity_analysis = {
            "severity_distribution": severity_counts,
            "worst_shape": max(problematic_shapes, key=lambda x: x["total_backtracks"]),
            "avg_backtracks_per_shape": round(sum(s["total_backtracks"] for s in problematic_shapes) / len(problematic_shapes), 2),
            "max_reversal_magnitude": max(max(s["max_lat_reversal"], s["max_lon_reversal"]) for s in problematic_shapes)
        }

    # Détermination du statut
    if problematic_count > 0:
        if severity_analysis.get("severity_distribution", {}).get("high", 0) > 0:
            status = "error"
        else:
            status = "warning"
    elif processing_errors:
        status = "warning"
    else:
        status = "success"

    # Construction des issues
    issues = []
    
    if problematic_count > 0:
        problematic_shape_ids = [shape['shape_id'] for shape in problematic_shapes]
        issues.append({
            "type": "backtracking_detected",
            "field": "shape_direction",
            "count": total_backtracks,
            "affected_ids": problematic_shape_ids[:100],
            "message": f"{total_backtracks} segments de backtracking détectés dans {problematic_count} formes"
        })
        
        # Issue spécifique pour backtracking sévère
        high_severity_shapes = [s for s in problematic_shapes if s["severity"] == "high"]
        if high_severity_shapes:
            issues.append({
                "type": "severe_backtracking",
                "field": "shape_direction",
                "count": len(high_severity_shapes),
                "affected_ids": [s['shape_id'] for s in high_severity_shapes][:50],
                "message": f"{len(high_severity_shapes)} formes avec backtracking sévère (>{threshold_deg * 10}°)"
            })
    
    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "backtrack_detection",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} formes n'ont pas pu être analysées"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_points": total_points,
            "backtracking_shapes": [s['shape_id'] for s in problematic_shapes],
            "count": problematic_count,
            "backtracking_rate": backtracking_rate,
            "threshold_degrees": threshold_deg,
            "severity_analysis": severity_analysis,
            "problematic_shapes_details": problematic_shapes[:20],  # Top 20 formes problématiques
            "backtracking_details": backtracking_details[:50],  # Top 50 backtracks
            "processing_stats": {
                "successfully_analyzed": total_shapes - len(processing_errors),
                "processing_errors": len(processing_errors),
                "analysis_coverage": round((total_shapes - len(processing_errors)) / total_shapes * 100, 2) if total_shapes > 0 else 0
            },
            "direction_analysis": {
                "total_direction_reversals": total_backtracks,
                "latitude_reversals": len([bt for bt in backtracking_details if bt['type'] == 'latitude']),
                "longitude_reversals": len([bt for bt in backtracking_details if bt['type'] == 'longitude']),
                "avg_reversal_magnitude": round(sum(bt.get('reversal_magnitude', 0) for bt in backtracking_details) / len(backtracking_details), 6) if backtracking_details else 0
            }
        },
        "explanation": {
            "purpose": "Détecte les segments de retour en arrière (backtracking) dans les formes pour identifier les anomalies de parcours et trajets non optimaux",
            "detection_method": f"Analyse des changements de direction significatifs (>{threshold_deg}°) entre segments consécutifs",
            "context": f"Analyse de {total_shapes} formes avec {backtracking_rate}% présentant du backtracking",
            "severity_breakdown": f"Sévérité: {severity_analysis.get('severity_distribution', {})} formes par niveau" if severity_analysis else "Aucun backtracking détecté",
            "impact": (
                f"Trajectoires optimales : aucun backtracking détecté" if status == "success"
                else f"Anomalies de parcours : {total_backtracks} segments de backtracking dans {problematic_count} formes"
            )
        },
        "recommendations": [
            rec for rec in [
                f"URGENT: Corriger {len([s for s in problematic_shapes if s['severity'] == 'high'])} formes avec backtracking sévère" if any(s['severity'] == 'high' for s in problematic_shapes) else None,
                f"Examiner la forme la plus problématique: {severity_analysis.get('worst_shape', {}).get('shape_id')} ({severity_analysis.get('worst_shape', {}).get('total_backtracks')} backtracks)" if severity_analysis.get('worst_shape') else None,
                f"Traiter {total_backtracks} segments de retour en arrière pour optimiser les parcours" if total_backtracks > 0 else None,
                f"Ajuster le seuil de détection (actuellement {threshold_deg}°) si nécessaire" if problematic_count > total_shapes * 0.5 else None,
                "Vérifier la logique de tri des points par shape_pt_sequence" if total_backtracks > 0 else None,
                f"Corriger {len(processing_errors)} formes avec erreurs d'analyse" if processing_errors else None,
                "Maintenir cette qualité directionnelle pour assurer des parcours optimaux" if status == "success" else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="shapes",
    name="similar_shapes_detection",
    genre="quality",
    description="Détecte shapes très similaires par trajectoire proche",
    parameters={"distance_threshold": {"type": "number", "default": 0.0005}}  # env. 50m tolérance
)
def similar_shapes_detection(gtfs_data, distance_threshold=0.0005, **params):
    """
    Détecte les formes géométriques similaires basées sur la distance moyenne entre points correspondants
    """
    df = gtfs_data.get('shapes.txt')
    if df is None or df.empty:
        return {
            "status": "error" if df is None else "warning",
            "issues": [
                {
                    "type": "missing_file" if df is None else "empty_file",
                    "field": "shapes.txt",
                    "count": 1 if df is None else 0,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour détecter les formes similaires" if df is None else "Le fichier shapes.txt est vide"
                }
            ],
            "result": {
                "similar_pairs": [],
                "total_shapes": 0
            },
            "explanation": {
                "purpose": "Détecte les formes géométriques similaires pour identifier les doublons potentiels et optimiser les données."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide." if df is None else "Ajouter des formes dans shapes.txt."]
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les formes géométriques similaires pour identifier les doublons potentiels",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les similarités"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Extraction des coordonnées par forme
    df_sorted = df.sort_values(['shape_id', 'shape_pt_sequence'])
    shape_coords = {}
    processing_errors = []
    
    for shape_id, group in df_sorted.groupby('shape_id'):
        try:
            coords = group[['shape_pt_lat', 'shape_pt_lon']].values
            if len(coords) > 0:
                shape_coords[shape_id] = coords
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Comparaison des formes
    similar_pairs = []
    shape_ids = list(shape_coords.keys())
    total_comparisons = len(shape_ids) * (len(shape_ids) - 1) // 2
    
    import numpy as np
    
    def mean_coord_distance(c1, c2):
        """Calcule la distance moyenne entre points correspondants"""
        return np.linalg.norm(c1 - c2, axis=1).mean()

    for i in range(len(shape_ids)):
        for j in range(i + 1, len(shape_ids)):
            try:
                coords1 = shape_coords[shape_ids[i]]
                coords2 = shape_coords[shape_ids[j]]
                
                # Comparaison seulement si même nombre de points
                if len(coords1) != len(coords2):
                    continue
                
                mean_distance = mean_coord_distance(coords1, coords2)
                
                if mean_distance < distance_threshold:
                    similarity_score = 1 - (mean_distance / distance_threshold)
                    
                    similar_pairs.append({
                        "shape_1": shape_ids[i],
                        "shape_2": shape_ids[j],
                        "mean_distance": round(mean_distance, 6),
                        "similarity_score": round(similarity_score, 4),
                        "point_count": len(coords1),
                        "similarity_level": (
                            "very_high" if similarity_score > 0.95
                            else "high" if similarity_score > 0.8
                            else "medium"
                        )
                    })
            except Exception as e:
                processing_errors.append(f"Comparison {shape_ids[i]}-{shape_ids[j]}: {str(e)}")

    # Métriques de similarité
    total_shapes = len(shape_coords)
    similarity_rate = round(len(similar_pairs) / total_comparisons * 100, 2) if total_comparisons > 0 else 0
    
    # Analyse des groupes similaires
    similarity_analysis = {}
    if similar_pairs:
        # Regroupement des formes très similaires
        very_similar = [p for p in similar_pairs if p['similarity_level'] == 'very_high']
        
        similarity_analysis = {
            "very_similar_pairs": len(very_similar),
            "high_similar_pairs": len([p for p in similar_pairs if p['similarity_level'] == 'high']),
            "medium_similar_pairs": len([p for p in similar_pairs if p['similarity_level'] == 'medium']),
            "most_similar_pair": min(similar_pairs, key=lambda x: x['mean_distance']),
            "avg_similarity_score": round(sum(p['similarity_score'] for p in similar_pairs) / len(similar_pairs), 4)
        }

    # Détermination du statut
    if len(very_similar) > 0:
        status = "warning"  # Formes très similaires détectées
    elif processing_errors:
        status = "warning"
    else:
        status = "success"

    # Construction des issues
    issues = []
    if similar_pairs:
        similar_shape_ids = list(set([p['shape_1'] for p in similar_pairs] + [p['shape_2'] for p in similar_pairs]))
        issues.append({
            "type": "similar_shapes",
            "field": "shape_geometry",
            "count": len(similar_pairs),
            "affected_ids": similar_shape_ids[:100],
            "message": f"{len(similar_pairs)} paires de formes similaires détectées (distance < {distance_threshold})"
        })

    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "similarity_detection",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} erreurs lors de la comparaison des formes"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_comparisons": total_comparisons,
            "similar_pairs": similar_pairs,
            "similarity_count": len(similar_pairs),
            "similarity_rate": similarity_rate,
            "distance_threshold": distance_threshold,
            "similarity_analysis": similarity_analysis,
            "processing_stats": {
                "successfully_analyzed": total_shapes - len([e for e in processing_errors if ':' not in e]),
                "processing_errors": len(processing_errors),
                "comparison_coverage": round((total_comparisons - len([e for e in processing_errors if 'Comparison' in e])) / total_comparisons * 100, 2) if total_comparisons > 0 else 0
            }
        },
        "explanation": {
            "purpose": "Détecte les formes géométriques similaires pour identifier les doublons potentiels et optimiser les données",
            "detection_method": f"Comparaison par distance moyenne entre points correspondants (seuil: {distance_threshold})",
            "context": f"Analyse de {total_shapes} formes avec {total_comparisons} comparaisons possibles",
            "similarity_summary": f"Paires similaires: {len(similar_pairs)} avec {similarity_analysis.get('avg_similarity_score', 0)} score moyen",
            "impact": (
                f"Formes géométriques uniques sans doublons détectés" if status == "success"
                else f"Similarités détectées : {len(similar_pairs)} paires de formes potentiellement redondantes"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Examiner {len([p for p in similar_pairs if p['similarity_level'] == 'very_high'])} paires très similaires pour déduplication" if any(p['similarity_level'] == 'very_high' for p in similar_pairs) else None,
                f"Analyser la paire la plus similaire: {similarity_analysis.get('most_similar_pair', {}).get('shape_1')} - {similarity_analysis.get('most_similar_pair', {}).get('shape_2')} (distance: {similarity_analysis.get('most_similar_pair', {}).get('mean_distance')})" if similarity_analysis.get('most_similar_pair') else None,
                f"Considérer fusionner les formes avec score de similarité > 0.95" if len([p for p in similar_pairs if p['similarity_score'] > 0.95]) > 0 else None,
                f"Ajuster le seuil de détection (actuellement {distance_threshold}) selon vos besoins" if len(similar_pairs) > total_shapes * 0.5 else None,
                f"Corriger {len(processing_errors)} erreurs de traitement" if processing_errors else None,
                "Maintenir cette diversité géométrique sans redondance" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="shapes",
    name="consecutive_duplicates_in_shape",
    genre="quality",
    description="Détecte les points consécutifs identiques dans les formes géométriques",
    parameters={}
)
def consecutive_duplicates_in_shape(gtfs_data, **params):
    """
    Détecte les points consécutifs identiques dans les formes géométriques
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour détecter les doublons consécutifs"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Détecte les points consécutifs identiques pour optimiser la géométrie des formes."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "total_shapes": 0,
                "shapes_with_duplicates": [],
                "count": 0
            },
            "explanation": {
                "purpose": "Détecte les points consécutifs identiques pour optimiser la géométrie des formes",
                "context": "Aucune forme définie",
                "impact": "Analyse non applicable"
            },
            "recommendations": []
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_points": total_points,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les points consécutifs identiques pour optimiser la géométrie des formes",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les doublons consécutifs"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Tri par forme et séquence
    if 'shape_pt_sequence' in df.columns:
        df_sorted = df.sort_values(['shape_id', 'shape_pt_sequence'])
    else:
        df_sorted = df.sort_values('shape_id')

    # Détection des doublons consécutifs
    shapes_with_duplicates = []
    all_duplicate_details = []
    processing_errors = []
    total_shapes = df_sorted['shape_id'].nunique()

    for shape_id, group in df_sorted.groupby('shape_id'):
        try:
            if len(group) < 2:
                continue
            
            coords = group[['shape_pt_lat', 'shape_pt_lon']].values
            consecutive_duplicates = []
            
            for i in range(1, len(coords)):
                if (coords[i][0] == coords[i-1][0] and 
                    coords[i][1] == coords[i-1][1]):
                    consecutive_duplicates.append({
                        "position": i,
                        "coordinates": [coords[i][0], coords[i][1]],
                        "previous_position": i-1
                    })
            
            if consecutive_duplicates:
                shapes_with_duplicates.append({
                    "shape_id": shape_id,
                    "total_points": len(group),
                    "consecutive_duplicates": len(consecutive_duplicates),
                    "duplicate_positions": [d["position"] for d in consecutive_duplicates],
                    "efficiency_gain": len(consecutive_duplicates),
                    "details": consecutive_duplicates
                })
                
                all_duplicate_details.extend([
                    {"shape_id": shape_id, **dup} for dup in consecutive_duplicates
                ])
                
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Calcul des métriques
    affected_shapes = len(shapes_with_duplicates)
    total_duplicates = len(all_duplicate_details)
    duplicate_rate = round(affected_shapes / total_shapes * 100, 2) if total_shapes > 0 else 0
    
    # Analyse des duplicatas
    duplicate_analysis = {}
    if shapes_with_duplicates:
        duplicate_counts = [s["consecutive_duplicates"] for s in shapes_with_duplicates]
        duplicate_analysis = {
            "avg_duplicates_per_shape": round(sum(duplicate_counts) / len(duplicate_counts), 2),
            "max_duplicates_in_shape": max(duplicate_counts),
            "total_removable_points": sum(duplicate_counts),
            "worst_shape": max(shapes_with_duplicates, key=lambda x: x["consecutive_duplicates"]),
            "efficiency_gain_percentage": round(sum(duplicate_counts) / total_points * 100, 2) if total_points > 0 else 0
        }

    # Détermination du statut
    if total_duplicates == 0:
        status = "success"
    elif duplicate_rate <= 5:  # ≤5% de formes affectées
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    
    if affected_shapes > 0:
        affected_shape_ids = [shape['shape_id'] for shape in shapes_with_duplicates]
        issues.append({
            "type": "consecutive_duplicates",
            "field": "shape_geometry",
            "count": total_duplicates,
            "affected_ids": affected_shape_ids[:100],
            "message": f"{total_duplicates} points consécutifs dupliqués dans {affected_shapes} formes"
        })
    
    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "duplicate_detection",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} formes n'ont pas pu être analysées"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_points": total_points,
            "shapes_with_duplicates": shapes_with_duplicates[:20],
            "count": affected_shapes,
            "duplicate_rate": duplicate_rate,
            "duplicate_analysis": duplicate_analysis,
            "all_duplicates": all_duplicate_details[:50],
            "optimization_potential": {
                "removable_points": duplicate_analysis.get("total_removable_points", 0),
                "storage_savings": duplicate_analysis.get("efficiency_gain_percentage", 0),
                "optimized_size": total_points - duplicate_analysis.get("total_removable_points", 0)
            }
        },
        "explanation": {
            "purpose": "Détecte les points consécutifs identiques dans les formes pour optimiser la géométrie et réduire la redondance",
            "context": f"Analyse de {total_shapes} formes avec {duplicate_rate}% présentant des doublons consécutifs",
            "duplicate_summary": f"Points dupliqués: {total_duplicates} dans {affected_shapes} formes",
            "optimization_impact": f"Potentiel d'optimisation: {duplicate_analysis.get('efficiency_gain_percentage', 0)}% de réduction possible",
            "impact": (
                f"Géométrie optimisée sans points consécutifs dupliqués" if status == "success"
                else f"Redondance détectée : {total_duplicates} points consécutifs supprimables"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Supprimer {duplicate_analysis.get('total_removable_points', 0)} points consécutifs dupliqués" if total_duplicates > 0 else None,
                f"Prioriser la forme {duplicate_analysis.get('worst_shape', {}).get('shape_id')} ({duplicate_analysis.get('worst_shape', {}).get('consecutive_duplicates')} doublons)" if duplicate_analysis.get('worst_shape') else None,
                f"Récupérer {duplicate_analysis.get('efficiency_gain_percentage', 0)}% d'espace de stockage" if duplicate_analysis.get('efficiency_gain_percentage', 0) > 1 else None,
                "Implémenter une déduplication automatique lors de la génération des formes" if total_duplicates > 0 else None,
                f"Corriger {len(processing_errors)} formes avec erreurs de traitement" if processing_errors else None,
                "Maintenir cette efficacité géométrique sans redondance" if status == "success" else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="shapes",
    name="isolated_shape_points",
    genre="quality",
    description="Cherche points très éloignés de voisins (erreurs de géocodage)",
    parameters={"distance_threshold_m": {"type": "number", "default": 1000.0}}
)
def isolated_shape_points(gtfs_data, distance_threshold_m=1000.0, **params):
    """
    Détecte les points isolés très éloignés de leurs voisins (possibles erreurs de géocodage)
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour détecter les points isolés"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Détecte les points isolés très éloignés de leurs voisins pour identifier les erreurs de géocodage."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "isolated_points": [],
                "total_anomalies": 0
            },
            "explanation": {
                "purpose": "Détecte les points isolés très éloignés de leurs voisins",
                "context": "Aucune forme définie",
                "impact": "Analyse non applicable"
            },
            "recommendations": []
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les points isolés très éloignés de leurs voisins",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les points isolés"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Tri par forme et séquence
    if 'shape_pt_sequence' in df.columns:
        df_sorted = df.sort_values(['shape_id', 'shape_pt_sequence'])
    else:
        df_sorted = df.sort_values('shape_id')

    # Détection des points isolés
    isolated_points = []
    processing_errors = []
    total_shapes = df_sorted['shape_id'].nunique()

    from geopy.distance import geodesic

    for shape_id, group in df_sorted.groupby('shape_id'):
        try:
            if len(group) < 3:  # Besoin d'au moins 3 points pour détecter isolation
                continue
            
            coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
            
            # Analyse des points intermédiaires (pas premier ni dernier)
            for i in range(1, len(coords) - 1):
                prev_dist = geodesic(coords[i-1], coords[i]).meters
                next_dist = geodesic(coords[i], coords[i+1]).meters
                
                # Point isolé si éloigné des deux voisins
                if prev_dist > distance_threshold_m and next_dist > distance_threshold_m:
                    isolation_score = min(prev_dist, next_dist)
                    
                    isolated_points.append({
                        "shape_id": shape_id,
                        "point_index": i,
                        "coordinates": list(coords[i]),
                        "distance_to_previous": round(prev_dist, 2),
                        "distance_to_next": round(next_dist, 2),
                        "min_neighbor_distance": round(isolation_score, 2),
                        "isolation_severity": (
                            "extreme" if isolation_score > distance_threshold_m * 5
                            else "high" if isolation_score > distance_threshold_m * 2
                            else "moderate"
                        )
                    })
                    
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Calcul des métriques
    total_anomalies = len(isolated_points)
    affected_shapes = len(set(point['shape_id'] for point in isolated_points))
    isolation_rate = round(total_anomalies / total_points * 100, 4) if total_points > 0 else 0

    # Analyse des anomalies
    anomaly_analysis = {}
    if isolated_points:
        isolation_distances = [point['min_neighbor_distance'] for point in isolated_points]
        severity_counts = {"moderate": 0, "high": 0, "extreme": 0}
        
        for point in isolated_points:
            severity_counts[point["isolation_severity"]] += 1
        
        anomaly_analysis = {
            "severity_distribution": severity_counts,
            "avg_isolation_distance": round(sum(isolation_distances) / len(isolation_distances), 2),
            "max_isolation_distance": round(max(isolation_distances), 2),
            "most_isolated_point": max(isolated_points, key=lambda x: x['min_neighbor_distance']),
            "shapes_affected": affected_shapes
        }

    # Détermination du statut
    if total_anomalies == 0:
        status = "success"
    elif severity_counts.get("extreme", 0) > 0:
        status = "error"
    else:
        status = "warning"

    # Construction des issues
    issues = []
    
    if total_anomalies > 0:
        affected_shape_ids = list(set(point['shape_id'] for point in isolated_points))
        issues.append({
            "type": "isolated_points",
            "field": "point_positioning",
            "count": total_anomalies,
            "affected_ids": affected_shape_ids[:100],
            "message": f"{total_anomalies} points isolés détectés (distance >{distance_threshold_m}m des voisins)"
        })
        
        # Issue spécifique pour points extrêmement isolés
        extreme_points = [p for p in isolated_points if p["isolation_severity"] == "extreme"]
        if extreme_points:
            issues.append({
                "type": "extreme_isolation",
                "field": "geocoding_errors",
                "count": len(extreme_points),
                "affected_ids": [p['shape_id'] for p in extreme_points][:50],
                "message": f"{len(extreme_points)} points extrêmement isolés (>{distance_threshold_m * 5}m) - possibles erreurs de géocodage"
            })

    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "isolation_detection",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} formes n'ont pas pu être analysées"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_points": total_points,
            "total_shapes": total_shapes,
            "isolated_points": isolated_points[:10],  # Top 10 points les plus isolés
            "total_anomalies": total_anomalies,
            "isolation_rate": isolation_rate,
            "distance_threshold_m": distance_threshold_m,
            "anomaly_analysis": anomaly_analysis,
            "processing_stats": {
                "successfully_analyzed": total_shapes - len(processing_errors),
                "processing_errors": len(processing_errors)
            }
        },
        "explanation": {
            "purpose": "Détecte les points isolés très éloignés de leurs voisins pour identifier les possibles erreurs de géocodage ou anomalies de positionnement",
            "detection_method": f"Analyse des distances aux voisins (seuil: {distance_threshold_m}m)",
            "context": f"Analyse de {total_points} points avec {total_anomalies} anomalies détectées",
            "severity_breakdown": f"Sévérité: {anomaly_analysis.get('severity_distribution', {})} points par niveau",
            "impact": (
                f"Positionnement cohérent : aucun point isolé détecté" if status == "success"
                else f"Anomalies de positionnement : {total_anomalies} points isolés dans {affected_shapes} formes"
            )
        },
        "recommendations": [
            rec for rec in [
                f"URGENT: Corriger {len([p for p in isolated_points if p['isolation_severity'] == 'extreme'])} points extrêmement isolés" if any(p['isolation_severity'] == 'extreme' for p in isolated_points) else None,
                f"Examiner le point le plus isolé: {anomaly_analysis.get('most_isolated_point', {}).get('shape_id')} position {anomaly_analysis.get('most_isolated_point', {}).get('point_index')} ({anomaly_analysis.get('most_isolated_point', {}).get('min_neighbor_distance')}m)" if anomaly_analysis.get('most_isolated_point') else None,
                f"Vérifier les données de géocodage pour {total_anomalies} points suspects" if total_anomalies > 0 else None,
                f"Ajuster le seuil de détection (actuellement {distance_threshold_m}m) selon votre contexte géographique" if total_anomalies > total_points * 0.1 else None,
                f"Corriger {len(processing_errors)} formes avec erreurs de traitement" if processing_errors else None,
                "Maintenir cette qualité de positionnement géographique" if status == "success" else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="shapes",
    name="shape_linearity_ratio",
    genre="quality",
    description="Mesure la linéarité d'un shape (distance directe / distance réelle)",
    parameters={}
)
def shape_linearity_ratio(gtfs_data, **params):
    """
    Calcule le ratio de linéarité des formes (distance directe / distance totale du parcours)
    """
    df = gtfs_data.get('shapes.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "shapes.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier shapes.txt est requis pour calculer la linéarité"
                }
            ],
            "result": {},
            "explanation": {
                "purpose": "Calcule le ratio de linéarité des formes pour évaluer la directivité des parcours."
            },
            "recommendations": ["Fournir un fichier shapes.txt valide."]
        }

    total_points = len(df)
    
    if total_points == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "total_shapes": 0,
                "linearity_ratios": [],
                "min_ratio": None,
                "max_ratio": None
            },
            "explanation": {
                "purpose": "Calcule le ratio de linéarité des formes pour évaluer la directivité des parcours",
                "context": "Aucune forme définie",
                "impact": "Analyse non applicable"
            },
            "recommendations": []
        }

    required_columns = ['shape_id', 'shape_pt_lat', 'shape_pt_lon']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "required_columns",
                    "count": len(missing_columns),
                    "affected_ids": [],
                    "message": f"Colonnes obligatoires manquantes: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Calcule le ratio de linéarité des formes pour évaluer la directivité des parcours",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de calculer la linéarité"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Tri par forme et séquence
    if 'shape_pt_sequence' in df.columns:
        df_sorted = df.sort_values(['shape_id', 'shape_pt_sequence'])
    else:
        df_sorted = df.sort_values('shape_id')

    # Calcul des ratios de linéarité
    linearity_ratios = []
    processing_errors = []
    total_shapes = df_sorted['shape_id'].nunique()

    from geopy.distance import geodesic

    for shape_id, group in df_sorted.groupby('shape_id'):
        try:
            if len(group) < 2:
                continue
            
            coords = list(zip(group['shape_pt_lat'], group['shape_pt_lon']))
            
            # Distance totale (somme des segments)
            total_distance = sum(
                geodesic(coords[i], coords[i+1]).meters 
                for i in range(len(coords)-1)
            )
            
            if total_distance == 0:
                continue
            
            # Distance directe (vol d'oiseau)
            direct_distance = geodesic(coords[0], coords[-1]).meters
            
            # Ratio de linéarité
            linearity_ratio = direct_distance / total_distance
            
            # Classification de la linéarité
            if linearity_ratio >= 0.9:
                linearity_class = "very_linear"
            elif linearity_ratio >= 0.7:
                linearity_class = "linear"
            elif linearity_ratio >= 0.5:
                linearity_class = "moderate"
            elif linearity_ratio >= 0.3:
                linearity_class = "winding"
            else:
                linearity_class = "very_winding"

            linearity_ratios.append({
                "shape_id": shape_id,
                "linearity_ratio": round(linearity_ratio, 4),
                "total_distance_m": round(total_distance, 2),
                "direct_distance_m": round(direct_distance, 2),
                "point_count": len(coords),
                "linearity_class": linearity_class,
                "detour_factor": round(1 / linearity_ratio if linearity_ratio > 0 else float('inf'), 2)
            })
            
        except Exception as e:
            processing_errors.append(f"{shape_id}: {str(e)}")

    # Calcul des statistiques globales
    successful_calculations = len(linearity_ratios)
    
    if not linearity_ratios:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "calculation_error",
                    "field": "linearity_computation",
                    "count": len(processing_errors),
                    "affected_ids": [],
                    "message": f"Aucun ratio de linéarité calculable - {len(processing_errors)} erreurs"
                }
            ],
            "result": {
                "total_shapes": total_shapes,
                "successful_calculations": 0,
                "processing_errors": processing_errors[:10]
            },
            "explanation": {
                "purpose": "Calcule le ratio de linéarité des formes pour évaluer la directivité des parcours",
                "context": "Erreurs de calcul pour toutes les formes",
                "impact": "Aucune mesure de linéarité disponible"
            },
            "recommendations": [
                "Vérifier la validité des coordonnées dans shapes.txt",
                "Corriger les formes avec moins de 2 points valides"
            ]
        }

    # Statistiques descriptives
    ratios = [lr['linearity_ratio'] for lr in linearity_ratios]
    
    linearity_stats = {
        "count": successful_calculations,
        "min_ratio": round(min(ratios), 4),
        "max_ratio": round(max(ratios), 4),
        "avg_ratio": round(sum(ratios) / len(ratios), 4),
        "median_ratio": round(sorted(ratios)[len(ratios)//2], 4),
    }

    # Distribution par classe de linéarité
    linearity_distribution = {
        "very_linear": len([lr for lr in linearity_ratios if lr['linearity_class'] == 'very_linear']),
        "linear": len([lr for lr in linearity_ratios if lr['linearity_class'] == 'linear']),
        "moderate": len([lr for lr in linearity_ratios if lr['linearity_class'] == 'moderate']),
        "winding": len([lr for lr in linearity_ratios if lr['linearity_class'] == 'winding']),
        "very_winding": len([lr for lr in linearity_ratios if lr['linearity_class'] == 'very_winding'])
    }

    # Analyse de qualité
    quality_analysis = {
        "most_linear": max(linearity_ratios, key=lambda x: x['linearity_ratio']),
        "most_winding": min(linearity_ratios, key=lambda x: x['linearity_ratio']),
        "avg_detour_factor": round(sum(lr['detour_factor'] for lr in linearity_ratios if lr['detour_factor'] != float('inf')) / len([lr for lr in linearity_ratios if lr['detour_factor'] != float('inf')]), 2),
        "network_linearity": linearity_stats['avg_ratio']
    }

    # Détermination du statut
    success_rate = round(successful_calculations / total_shapes * 100, 2) if total_shapes > 0 else 0
    
    if processing_errors and success_rate < 90:
        status = "error"
    elif processing_errors:
        status = "warning"
    else:
        status = "success"

    # Construction des issues
    issues = []
    if processing_errors:
        issues.append({
            "type": "processing_error",
            "field": "linearity_calculation",
            "count": len(processing_errors),
            "affected_ids": [],
            "message": f"{len(processing_errors)} formes n'ont pas pu être analysées"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_shapes": total_shapes,
            "total_points": total_points,
            "successful_calculations": successful_calculations,
            "calculation_success_rate": success_rate,
            "linearity_ratios": linearity_ratios[:10],  # Top 10 formes
            "linearity_stats": linearity_stats,
            "linearity_distribution": linearity_distribution,
            "quality_analysis": quality_analysis,
            "network_metrics": {
                "overall_linearity": linearity_stats['avg_ratio'],
                "linearity_variance": round(sum((r - linearity_stats['avg_ratio'])**2 for r in ratios) / len(ratios), 6),
                "efficient_routes": linearity_distribution['very_linear'] + linearity_distribution['linear'],
                "inefficient_routes": linearity_distribution['winding'] + linearity_distribution['very_winding']
            }
        },
        "explanation": {
            "purpose": "Calcule le ratio de linéarité des formes (distance directe/distance totale) pour évaluer l'efficacité et la directivité des parcours",
            "linearity_interpretation": "Ratio proche de 1 = parcours direct, ratio faible = parcours sinueux avec détours",
            "context": f"Analyse de {total_shapes} formes avec {success_rate}% de calculs réussis",
            "network_assessment": f"Linéarité moyenne du réseau: {linearity_stats['avg_ratio']} (facteur détour moyen: {quality_analysis['avg_detour_factor']})",
            "quality_range": f"Formes de {linearity_stats['min_ratio']} (très sinueuse) à {linearity_stats['max_ratio']} (très directe)",
            "impact": (
                f"Analyse complète de la linéarité : réseau avec {linearity_stats['avg_ratio']} de directivité moyenne" if status == "success"
                else f"Analyse partielle: {len(processing_errors)} formes non analysables"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Corriger {len(processing_errors)} formes avec erreurs de calcul" if processing_errors else None,
                f"Examiner la forme la plus sinueuse: {quality_analysis['most_winding']['shape_id']} (ratio: {quality_analysis['most_winding']['linearity_ratio']}, détour: {quality_analysis['most_winding']['detour_factor']}x)" if quality_analysis.get('most_winding') else None,
                f"Optimiser {linearity_distribution['very_winding']} formes très sinueuses (ratio < 0.3)" if linearity_distribution['very_winding'] > 0 else None,
                f"Analyser {linearity_distribution['winding']} formes sinueuses pour identifier les améliorations possibles" if linearity_distribution['winding'] > successful_calculations * 0.3 else None,
                f"Valoriser {linearity_distribution['very_linear']} formes très directes comme références" if linearity_distribution['very_linear'] > 0 else None,
                f"Améliorer la linéarité globale du réseau (actuellement {linearity_stats['avg_ratio']})" if linearity_stats['avg_ratio'] < 0.6 else None,
                "Maintenir cette qualité de linéarité pour assurer l'efficacité des parcours" if status == "success" and linearity_stats['avg_ratio'] >= 0.7 else None
            ] if rec is not None
        ]
    }