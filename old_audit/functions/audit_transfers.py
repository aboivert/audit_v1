"""
Fonctions d'audit pour le file_type: agency
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="transfers",
    name="invalid_stop_ids",
    genre="validity",
    description="Détecte les stop_ids (from_stop_id et to_stop_id) absents dans stops.txt.",
    parameters={}
)
def invalid_stop_ids(gtfs_data, **params):
    transfers = gtfs_data.get('transfers.txt')
    stops = gtfs_data.get('stops.txt')

    if transfers is None:
        return {
            "status": "error",
            "score": 0,
            "issues": ["Fichier transfers.txt manquant."],
            "summary": {},
            "problem_ids": [],
            "explanation": "Impossible d'analyser sans transfers.txt.",
            "recommendations": ["Fournir un fichier transfers.txt valide."]
        }
    if stops is None:
        return {
            "status": "warning",
            "score": 50,
            "issues": ["Fichier stops.txt manquant, impossibilité de valider les stop_ids."],
            "summary": {},
            "problem_ids": [],
            "explanation": "Le fichier stops.txt est nécessaire pour vérifier l'existence des stop_ids.",
            "recommendations": ["Fournir stops.txt pour valider les liens de correspondance."]
        }

    invalid_from = set(transfers['from_stop_id']) - set(stops['stop_id'])
    invalid_to = set(transfers['to_stop_id']) - set(stops['stop_id'])
    total_invalid = len(invalid_from) + len(invalid_to)
    total_checks = len(set(transfers['from_stop_id']).union(set(transfers['to_stop_id'])))

    score = 100 if total_checks == 0 else max(0, 100 - (total_invalid / total_checks * 100))
    status = "ok" if total_invalid == 0 else "error"
    issues = []
    if total_invalid > 0:
        issues.append(f"{total_invalid} stop_id(s) invalide(s) détecté(s) dans transfers.txt.")

    explanation = (
        "Tous les stop_ids référencés existent dans stops.txt." if status == "ok"
        else f"{total_invalid} stop_id(s) dans transfers.txt ne sont pas référencés dans stops.txt."
    )
    recommendations = []
    if status == "error":
        recommendations.append("Corriger ou supprimer les stop_ids invalides dans transfers.txt.")

    return {
        "status": status,
        "score": round(score,1),
        "issues": issues,
        "summary": {
            "invalid_from_stop_ids_count": len(invalid_from),
            "invalid_to_stop_ids_count": len(invalid_to),
            "total_invalid_stop_ids": total_invalid
        },
        "problem_ids": list(invalid_from.union(invalid_to)),
        "explanation": explanation,
        "recommendations": recommendations
    }


@audit_function(
    file_type="transfers",
    name="invalid_transfer_type_values",
    genre="validity",
    description="Détecte les valeurs invalides dans le champ transfer_type.",
    parameters={}
)
def invalid_transfer_type_values(gtfs_data, **params):
    """
    Valide que les valeurs transfer_type dans transfers.txt respectent la spécification GTFS
    """
    df = gtfs_data.get('transfers.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "transfers.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier transfers.txt est requis pour valider les types de correspondance"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Valide que les valeurs transfer_type respectent la spécification GTFS pour assurer la cohérence des correspondances."
            },
            "recommendations": ["Fournir un fichier transfers.txt valide pour analyser les types de correspondance."]
        }

    total_transfers = len(df)
    
    # Vérification de la présence de la colonne transfer_type
    if 'transfer_type' not in df.columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "transfer_type",
                    "count": total_transfers,
                    "affected_ids": [],
                    "message": "La colonne transfer_type est obligatoire dans transfers.txt"
                }
            ],
            "result": {
                "total_transfers": total_transfers,
                "validation_possible": False
            },
            "explanation": {
                "purpose": "Valide que les valeurs transfer_type respectent la spécification GTFS pour assurer la cohérence des correspondances",
                "context": "Colonne transfer_type obligatoire manquante",
                "impact": "Impossible de valider les types de correspondance"
            },
            "recommendations": ["Ajouter la colonne transfer_type obligatoire dans transfers.txt."]
        }

    # Valeurs autorisées selon la spécification GTFS
    allowed_values = {0, 1, 2, 3, 4}
    transfer_type_meanings = {
        0: "Correspondance recommandée entre lignes",
        1: "Correspondance chronométrée (connexion garantie)",
        2: "Temps de correspondance minimum requis",
        3: "Correspondance non possible",
        4: "Correspondance possible avec transfert en station"
    }

    # Analyse des valeurs
    valid_mask = df['transfer_type'].isin(allowed_values)
    null_mask = df['transfer_type'].isna()
    
    valid_count = valid_mask.sum()
    null_count = null_mask.sum()
    invalid_count = total_transfers - valid_count - null_count
    
    validation_rate = round(valid_count / total_transfers * 100, 2) if total_transfers > 0 else 0

    # Identification des valeurs invalides
    invalid_mask = ~valid_mask & ~null_mask
    invalid_values = df.loc[invalid_mask, 'transfer_type'].unique().tolist()
    invalid_transfer_indices = df.loc[invalid_mask].index.tolist()
    
    # Analyse de la distribution des types valides
    type_distribution = df.loc[valid_mask, 'transfer_type'].value_counts().to_dict()
    
    # Identification des IDs des correspondances problématiques (si colonnes disponibles)
    problematic_transfer_ids = []
    if 'from_stop_id' in df.columns and 'to_stop_id' in df.columns:
        problematic_transfers = df.loc[invalid_mask]
        problematic_transfer_ids = [
            f"{row['from_stop_id']}->{row['to_stop_id']}" 
            for _, row in problematic_transfers.iterrows()
        ]

    # Détermination du statut
    if invalid_count == 0 and null_count == 0:
        status = "success"
    elif invalid_count == 0 and null_count <= total_transfers * 0.05:  # ≤5% nulls
        status = "warning"
    elif validation_rate >= 95:
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    
    if invalid_count > 0:
        issues.append({
            "type": "invalid_format",
            "field": "transfer_type",
            "count": invalid_count,
            "affected_ids": problematic_transfer_ids[:100] if problematic_transfer_ids else invalid_transfer_indices[:100],
            "message": f"{invalid_count} correspondances ont des transfer_type invalides: {invalid_values}"
        })
    
    if null_count > 0:
        null_indices = df.loc[null_mask].index.tolist()
        issues.append({
            "type": "missing_data",
            "field": "transfer_type",
            "count": null_count,
            "affected_ids": null_indices[:100],
            "message": f"{null_count} correspondances ont des transfer_type manquants (null/vide)"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_transfers": total_transfers,
            "valid_transfer_types": int(valid_count),
            "invalid_transfer_types": int(invalid_count),
            "missing_transfer_types": int(null_count),
            "validation_rate": validation_rate,
            "type_distribution": {
                "valid_types": type_distribution,
                "invalid_values": invalid_values,
                "most_common_type": max(type_distribution.items(), key=lambda x: x[1]) if type_distribution else None
            },
            "specification_compliance": {
                "allowed_values": list(allowed_values),
                "type_meanings": transfer_type_meanings,
                "compliance_rate": round((valid_count / total_transfers) * 100, 2) if total_transfers > 0 else 0
            },
            "transfer_analysis": {
                "recommended_transfers": type_distribution.get(0, 0),
                "timed_transfers": type_distribution.get(1, 0),
                "min_time_transfers": type_distribution.get(2, 0),
                "not_possible_transfers": type_distribution.get(3, 0),
                "in_station_transfers": type_distribution.get(4, 0)
            }
        },
        "explanation": {
            "purpose": "Valide que les valeurs transfer_type respectent la spécification GTFS pour assurer la cohérence et l'interprétation correcte des correspondances",
            "specification": "Valeurs autorisées: 0=recommandée, 1=chronométrée, 2=temps minimum, 3=non possible, 4=en station",
            "context": f"Analyse de {total_transfers} correspondances avec {validation_rate}% de types valides",
            "distribution_analysis": f"Type principal: {max(type_distribution.items(), key=lambda x: x[1]) if type_distribution else 'N/A'}",
            "impact": (
                f"Tous les types de correspondance sont conformes à la spécification GTFS" if status == "success"
                else f"Problèmes de conformité : {invalid_count} types invalides, {null_count} manquants"
            )
        },
        "recommendations": [
            rec for rec in [
                f"URGENT: Corriger {invalid_count} transfer_type invalides (utiliser 0, 1, 2, 3 ou 4 uniquement)" if invalid_count > 0 else None,
                f"Renseigner {null_count} transfer_type manquants" if null_count > 0 else None,
                f"Remplacer les valeurs invalides {invalid_values} par des valeurs GTFS conformes" if invalid_values else None,
                "Vérifier que les types de correspondance reflètent bien la réalité opérationnelle" if invalid_count > 0 else None,
                "Consulter la documentation GTFS pour le choix approprié des transfer_type" if invalid_count > 0 else None,
                f"Valider l'usage de {type_distribution.get(3, 0)} correspondances 'non possibles' (type 3)" if type_distribution.get(3, 0) > total_transfers * 0.1 else None,
                "Maintenir cette conformité aux spécifications GTFS pour assurer l'interopérabilité" if status == "success" else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="transfers",
    name="duplicate_transfers",
    genre="quality",
    description="Détecte les doublons sur from_stop_id et to_stop_id.",
    parameters={}
)
def duplicate_transfers(gtfs_data, **params):
    """
    Détecte les correspondances dupliquées basées sur from_stop_id/to_stop_id dans transfers.txt
    """
    df = gtfs_data.get('transfers.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "transfers.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier transfers.txt est requis pour détecter les doublons de correspondance"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Détecte les correspondances dupliquées pour éviter les ambiguïtés dans les règles de transfert."
            },
            "recommendations": ["Fournir un fichier transfers.txt valide pour analyser les doublons."]
        }

    total_transfers = len(df)
    
    # Vérification des colonnes requises
    required_columns = ['from_stop_id', 'to_stop_id']
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
                    "message": f"Colonnes obligatoires manquantes dans transfers.txt: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_transfers": total_transfers,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les correspondances dupliquées pour éviter les ambiguïtés dans les règles de transfert",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les doublons"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Détection des doublons basée sur from_stop_id/to_stop_id
    duplicates_mask = df.duplicated(subset=['from_stop_id', 'to_stop_id'], keep=False)
    duplicates_df = df[duplicates_mask]
    duplicate_count = len(duplicates_df)
    
    # Analyse des paires dupliquées
    duplicate_pairs = duplicates_df[['from_stop_id', 'to_stop_id']].drop_duplicates()
    unique_duplicate_pairs = len(duplicate_pairs)
    
    # Analyse détaillée des conflits
    conflict_analysis = {}
    if duplicate_count > 0:
        # Groupement par paire pour analyser les conflits
        grouped_duplicates = duplicates_df.groupby(['from_stop_id', 'to_stop_id'])
        
        conflict_details = []
        for (from_stop, to_stop), group in grouped_duplicates:
            conflict_info = {
                "from_stop_id": from_stop,
                "to_stop_id": to_stop,
                "duplicate_count": len(group),
                "transfer_types": group['transfer_type'].tolist() if 'transfer_type' in group.columns else [],
                "min_transfer_times": group['min_transfer_time'].tolist() if 'min_transfer_time' in group.columns else []
            }
            conflict_details.append(conflict_info)
        
        conflict_analysis = {
            "unique_pairs_with_duplicates": unique_duplicate_pairs,
            "total_duplicate_entries": duplicate_count,
            "conflict_details": conflict_details[:10],  # Top 10 exemples
            "max_duplicates_for_pair": max(len(group) for _, group in grouped_duplicates),
            "avg_duplicates_per_pair": round(duplicate_count / unique_duplicate_pairs, 2) if unique_duplicate_pairs > 0 else 0
        }

    # Calcul des métriques
    duplication_rate = round(duplicate_count / total_transfers * 100, 2) if total_transfers > 0 else 0
    unique_transfers = total_transfers - duplicate_count + unique_duplicate_pairs

    # Détermination du statut
    if duplicate_count == 0:
        status = "success"
    elif duplication_rate <= 5:  # ≤5% de doublons
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    if duplicate_count > 0:
        duplicate_pair_ids = [f"{row['from_stop_id']}->{row['to_stop_id']}" for _, row in duplicate_pairs.iterrows()]
        issues.append({
            "type": "duplicate_data",
            "field": "transfer_pairs",
            "count": duplicate_count,
            "affected_ids": duplicate_pair_ids[:100],
            "message": f"{duplicate_count} correspondances dupliquées détectées sur {unique_duplicate_pairs} paires from_stop_id/to_stop_id"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_transfers": total_transfers,
            "duplicate_transfers": duplicate_count,
            "unique_duplicate_pairs": unique_duplicate_pairs,
            "unique_transfers": unique_transfers,
            "duplication_rate": duplication_rate,
            "conflict_analysis": conflict_analysis,
            "data_quality": {
                "transfer_efficiency": round((unique_transfers / total_transfers) * 100, 2) if total_transfers > 0 else 0,
                "redundancy_level": (
                    "none" if duplicate_count == 0
                    else "low" if duplication_rate <= 5
                    else "high"
                )
            }
        },
        "explanation": {
            "purpose": "Détecte les correspondances dupliquées basées sur from_stop_id/to_stop_id pour éviter les ambiguïtés et optimiser les données",
            "context": f"Analyse de {total_transfers} correspondances avec {unique_duplicate_pairs} paires dupliquées",
            "duplication_summary": f"Taux de duplication: {duplication_rate}% ({duplicate_count} entrées redondantes)",
            "conflict_impact": f"Impact: {duplicate_count - unique_duplicate_pairs} entrées supprimables" if duplicate_count > 0 else "Aucune redondance détectée",
            "impact": (
                f"Données de correspondance optimisées sans doublons" if status == "success"
                else f"Redondances détectées : {duplicate_count} correspondances dupliquées sur {unique_duplicate_pairs} paires"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Supprimer {duplicate_count - unique_duplicate_pairs} correspondances redondantes pour optimiser les données" if duplicate_count > 0 else None,
                f"Examiner les {unique_duplicate_pairs} paires avec multiples définitions (conflits potentiels)" if unique_duplicate_pairs > 0 else None,
                "Fusionner les correspondances dupliquées en conservant les paramètres les plus appropriés" if duplicate_count > 0 else None,
                f"Vérifier les conflits de transfer_type pour les paires dupliquées" if conflict_analysis.get('conflict_details') else None,
                "Implémenter une validation d'unicité dans votre processus de génération transfers.txt" if duplicate_count > 0 else None,
                "Maintenir cette efficacité des données de correspondance sans redondance" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="transfers",
    name="missing_min_transfer_time",
    genre="validity",
    description="Détecte les transferts avec transfer_type=2 sans min_transfer_time renseigné.",
    parameters={}
)
def missing_min_transfer_time(gtfs_data, **params):
    """
    Détecte les correspondances de type 2 (temps minimum requis) sans min_transfer_time défini
    """
    df = gtfs_data.get('transfers.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "transfers.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier transfers.txt est requis pour valider min_transfer_time"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Vérifie que les correspondances de type 2 ont un min_transfer_time défini selon la spécification GTFS."
            },
            "recommendations": ["Fournir un fichier transfers.txt valide pour analyser les temps de correspondance."]
        }

    total_transfers = len(df)
    
    # Vérification des colonnes requises
    required_columns = ['transfer_type', 'min_transfer_time']
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
                    "message": f"Colonnes obligatoires manquantes dans transfers.txt: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_transfers": total_transfers,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Vérifie que les correspondances de type 2 ont un min_transfer_time défini selon la spécification GTFS",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de valider les temps de correspondance"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Analyse des correspondances de type 2
    type_2_transfers = df[df['transfer_type'] == 2]
    total_type_2 = len(type_2_transfers)
    
    if total_type_2 == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "total_transfers": total_transfers,
                "type_2_transfers": 0,
                "missing_min_transfer_time": 0,
                "compliance_rate": 100.0
            },
            "explanation": {
                "purpose": "Vérifie que les correspondances de type 2 ont un min_transfer_time défini selon la spécification GTFS",
                "context": "Aucune correspondance de type 2 (temps minimum requis) trouvée",
                "impact": "Validation non applicable - aucune correspondance nécessitant un temps minimum"
            },
            "recommendations": []
        }

    # Identification des correspondances de type 2 sans min_transfer_time
    missing_time_mask = type_2_transfers['min_transfer_time'].isna()
    missing_df = type_2_transfers[missing_time_mask]
    missing_count = len(missing_df)
    
    # Analyse des temps de correspondance pour les correspondances valides
    valid_type_2 = type_2_transfers[~missing_time_mask]
    time_analysis = {}
    if len(valid_type_2) > 0:
        times = valid_type_2['min_transfer_time'].dropna()
        if len(times) > 0:
            time_analysis = {
                "avg_transfer_time": round(times.mean(), 1),
                "min_transfer_time": int(times.min()),
                "max_transfer_time": int(times.max()),
                "median_transfer_time": round(times.median(), 1)
            }

    # Calcul des métriques
    compliance_rate = round((total_type_2 - missing_count) / total_type_2 * 100, 2) if total_type_2 > 0 else 100
    
    # IDs des correspondances problématiques
    problematic_transfer_ids = []
    if 'from_stop_id' in missing_df.columns and 'to_stop_id' in missing_df.columns:
        problematic_transfer_ids = [
            f"{row['from_stop_id']}->{row['to_stop_id']}" 
            for _, row in missing_df.iterrows()
        ]

    # Détermination du statut
    if missing_count == 0:
        status = "success"
    elif compliance_rate >= 90:
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    if missing_count > 0:
        issues.append({
            "type": "missing_required_data",
            "field": "min_transfer_time",
            "count": missing_count,
            "affected_ids": problematic_transfer_ids[:100] if problematic_transfer_ids else missing_df.index.tolist()[:100],
            "message": f"{missing_count} correspondances de type 2 sans min_transfer_time défini (requis par GTFS)"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_transfers": total_transfers,
            "type_2_transfers": total_type_2,
            "valid_type_2_transfers": total_type_2 - missing_count,
            "missing_min_transfer_time": missing_count,
            "compliance_rate": compliance_rate,
            "time_analysis": time_analysis,
            "transfer_type_distribution": df['transfer_type'].value_counts().to_dict() if 'transfer_type' in df.columns else {},
            "specification_compliance": {
                "type_2_requirement": "min_transfer_time obligatoire pour transfer_type=2",
                "missing_percentage": round(missing_count / total_type_2 * 100, 2) if total_type_2 > 0 else 0
            }
        },
        "explanation": {
            "purpose": "Vérifie que les correspondances de type 2 (temps minimum requis) ont un min_transfer_time défini selon la spécification GTFS",
            "specification": "transfer_type=2 nécessite min_transfer_time pour définir le temps de correspondance minimum",
            "context": f"Analyse de {total_type_2} correspondances de type 2 sur {total_transfers} correspondances totales",
            "compliance_summary": f"Taux de conformité: {compliance_rate}% ({missing_count} non conformes)",
            "impact": (
                f"Toutes les correspondances de type 2 respectent la spécification GTFS" if status == "success"
                else f"Violation GTFS : {missing_count} correspondances de type 2 sans temps minimum défini"
            )
        },
        "recommendations": [
            rec for rec in [
                f"URGENT: Renseigner min_transfer_time pour {missing_count} correspondances de type 2 (requis GTFS)" if missing_count > 0 else None,
                f"Utiliser les temps observés (moyenne: {time_analysis.get('avg_transfer_time', 'N/A')}s) comme référence" if time_analysis and missing_count > 0 else None,
                "Évaluer sur le terrain les temps de correspondance réels pour définir des min_transfer_time appropriés" if missing_count > 0 else None,
                "Considérer changer vers transfer_type=0 (recommandée) si le temps minimum n'est pas critique" if missing_count > total_type_2 * 0.5 else None,
                "Implémenter une validation GTFS dans votre processus de génération transfers.txt" if missing_count > 0 else None,
                "Maintenir cette conformité GTFS pour assurer l'interopérabilité" if status == "success" else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="transfers",
    name="missing_symmetric_transfers",
    genre="quality",
    description="Détecte les transferts où le transfert inverse n’existe pas.",
    parameters={}
)
def missing_symmetric_transfers(gtfs_data, **params):
    """
    Détecte les correspondances asymétriques où le transfert inverse n'existe pas
    """
    df = gtfs_data.get('transfers.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "transfers.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier transfers.txt est requis pour analyser la symétrie des correspondances"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Détecte les correspondances asymétriques pour améliorer la cohérence du réseau de correspondances."
            },
            "recommendations": ["Fournir un fichier transfers.txt valide pour analyser les correspondances symétriques."]
        }

    total_transfers = len(df)
    
    # Vérification des colonnes requises
    required_columns = ['from_stop_id', 'to_stop_id']
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
                    "message": f"Colonnes obligatoires manquantes dans transfers.txt: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_transfers": total_transfers,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les correspondances asymétriques pour améliorer la cohérence du réseau de correspondances",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible d'analyser la symétrie des correspondances"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Analyse de la symétrie des correspondances
    existing_pairs = set(tuple(x) for x in df[['from_stop_id', 'to_stop_id']].values)
    missing_symmetric = []
    
    for from_id, to_id in existing_pairs:
        reverse_pair = (to_id, from_id)
        if reverse_pair not in existing_pairs:
            missing_symmetric.append({
                "missing_from": to_id,
                "missing_to": from_id,
                "existing_pair": f"{from_id}->{to_id}"
            })

    missing_count = len(missing_symmetric)
    total_unique_pairs = len(existing_pairs)
    
    # Analyse des types de correspondances asymétriques
    asymmetry_analysis = {}
    if missing_count > 0 and 'transfer_type' in df.columns:
        # Analyse des types de correspondances pour les paires asymétriques
        type_distribution = {}
        for missing in missing_symmetric:
            # Trouver le type de la correspondance existante
            existing_transfers = df[
                (df['from_stop_id'] == missing['existing_pair'].split('->')[0]) & 
                (df['to_stop_id'] == missing['existing_pair'].split('->')[1])
            ]
            if not existing_transfers.empty:
                transfer_type = existing_transfers.iloc[0]['transfer_type']
                type_distribution[transfer_type] = type_distribution.get(transfer_type, 0) + 1
        
        asymmetry_analysis = {
            "asymmetric_by_type": type_distribution,
            "most_asymmetric_type": max(type_distribution.items(), key=lambda x: x[1]) if type_distribution else None
        }

    # Calcul des métriques
    symmetry_rate = round((total_unique_pairs - missing_count) / total_unique_pairs * 100, 2) if total_unique_pairs > 0 else 100
    potential_pairs = total_unique_pairs + missing_count  # Si tous étaient symétriques
    
    # Détermination du statut
    if missing_count == 0:
        status = "success"
    elif symmetry_rate >= 80:  # ≥80% symétrique
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    if missing_count > 0:
        missing_pair_ids = [f"{item['missing_from']}->{item['missing_to']}" for item in missing_symmetric]
        issues.append({
            "type": "asymmetric_data",
            "field": "transfer_symmetry",
            "count": missing_count,
            "affected_ids": missing_pair_ids[:100],
            "message": f"{missing_count} correspondances asymétriques détectées (transfert inverse manquant)"
        })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_transfers": total_transfers,
            "unique_transfer_pairs": total_unique_pairs,
            "symmetric_pairs": total_unique_pairs - missing_count,
            "asymmetric_pairs": missing_count,
            "symmetry_rate": symmetry_rate,
            "missing_symmetric_details": missing_symmetric[:20],  # Top 20 exemples
            "asymmetry_analysis": asymmetry_analysis,
            "network_completeness": {
                "current_coverage": round((total_unique_pairs / potential_pairs) * 100, 2) if potential_pairs > 0 else 0,
                "potential_total_pairs": potential_pairs,
                "optimization_potential": missing_count
            }
        },
        "explanation": {
            "purpose": "Détecte les correspondances asymétriques pour améliorer la cohérence et la complétude du réseau de correspondances",
            "context": f"Analyse de {total_unique_pairs} paires de correspondances avec {missing_count} asymétries",
            "symmetry_analysis": f"Taux de symétrie: {symmetry_rate}% ({missing_count} correspondances inverses manquantes)",
            "network_impact": f"Complétude réseau: {round((total_unique_pairs / potential_pairs) * 100, 2)}% des correspondances potentielles" if potential_pairs > 0 else "N/A",
            "impact": (
                f"Réseau de correspondances parfaitement symétrique" if status == "success"
                else f"Asymétries détectées : {missing_count} correspondances inverses manquantes sur {total_unique_pairs} paires"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Ajouter {missing_count} correspondances inverses pour améliorer la symétrie du réseau" if missing_count > 0 else None,
                f"Prioriser les correspondances de type {asymmetry_analysis.get('most_asymmetric_type', [None, 0])[0]} (plus asymétriques)" if asymmetry_analysis.get('most_asymmetric_type') else None,
                "Vérifier que les asymétries correspondent à des contraintes opérationnelles réelles" if missing_count > 0 else None,
                "Évaluer l'impact des correspondances asymétriques sur l'expérience voyageur" if symmetry_rate < 70 else None,
                f"Optimiser la complétude du réseau (actuellement {round((total_unique_pairs / potential_pairs) * 100, 2)}%)" if potential_pairs > 0 and missing_count > total_unique_pairs * 0.2 else None,
                "Implémenter une validation de symétrie dans votre processus de génération transfers.txt" if missing_count > 0 else None,
                "Maintenir cette symétrie parfaite pour assurer la cohérence des correspondances" if status == "success" else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="transfers",
    name="duplicate_transfers_pairs",
    genre="quality",
    description="Détecte les doublons sur from_stop_id, to_stop_id et transfer_type.",
    parameters={}
)
def duplicate_transfers_pairs(gtfs_data, **params):
    """
    Détecte les correspondances strictement identiques basées sur from_stop_id, to_stop_id et transfer_type
    """
    df = gtfs_data.get('transfers.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "transfers.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier transfers.txt est requis pour détecter les doublons stricts"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Détecte les correspondances strictement identiques pour éliminer les redondances exactes."
            },
            "recommendations": ["Fournir un fichier transfers.txt valide pour analyser les doublons stricts."]
        }

    total_transfers = len(df)
    
    # Cas fichier vide
    if total_transfers == 0:
        return {
            "status": "success",
            "issues": [],
            "result": {
                "total_transfers": 0,
                "duplicate_transfers": 0,
                "unique_transfers": 0,
                "duplication_rate": 0.0
            },
            "explanation": {
                "purpose": "Détecte les correspondances strictement identiques pour éliminer les redondances exactes",
                "context": "Fichier transfers.txt vide",
                "impact": "Aucune donnée à analyser"
            },
            "recommendations": ["Ajouter des correspondances dans transfers.txt si nécessaire."]
        }

    # Vérification des colonnes requises pour la détection de doublons
    required_columns = ['from_stop_id', 'to_stop_id', 'transfer_type']
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
                    "message": f"Colonnes obligatoires manquantes pour détecter les doublons: {', '.join(missing_columns)}"
                }
            ],
            "result": {
                "total_transfers": total_transfers,
                "missing_columns": missing_columns
            },
            "explanation": {
                "purpose": "Détecte les correspondances strictement identiques pour éliminer les redondances exactes",
                "context": "Colonnes obligatoires manquantes",
                "impact": "Impossible de détecter les doublons stricts"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }

    # Détection des doublons stricts (même from_stop_id, to_stop_id ET transfer_type)
    duplicated_mask = df.duplicated(subset=['from_stop_id', 'to_stop_id', 'transfer_type'], keep=False)
    duplicated_df = df[duplicated_mask]
    duplicate_count = len(duplicated_df)
    
    # Analyse des doublons stricts
    duplicate_groups = []
    strict_redundancy_analysis = {}
    
    if duplicate_count > 0:
        # Groupement par triplet identique
        grouped = duplicated_df.groupby(['from_stop_id', 'to_stop_id', 'transfer_type'])
        
        for (from_stop, to_stop, transfer_type), group in grouped:
            group_size = len(group)
            group_info = {
                "from_stop_id": from_stop,
                "to_stop_id": to_stop,
                "transfer_type": transfer_type,
                "duplicate_count": group_size,
                "redundant_entries": group_size - 1,  # Entrées supprimables
                "indices": group.index.tolist()
            }
            
            # Analyse des différences dans les autres champs
            if 'min_transfer_time' in group.columns:
                unique_times = group['min_transfer_time'].dropna().unique()
                if len(unique_times) > 1:
                    group_info["conflicting_min_transfer_times"] = unique_times.tolist()
                    
            duplicate_groups.append(group_info)
        
        # Analyse globale de la redondance stricte
        total_redundant = sum(group['redundant_entries'] for group in duplicate_groups)
        unique_duplicate_triplets = len(duplicate_groups)
        
        strict_redundancy_analysis = {
            "unique_duplicate_triplets": unique_duplicate_triplets,
            "total_redundant_entries": total_redundant,
            "max_duplicates_per_triplet": max(group['duplicate_count'] for group in duplicate_groups),
            "avg_duplicates_per_triplet": round(duplicate_count / unique_duplicate_triplets, 2) if unique_duplicate_triplets > 0 else 0,
            "conflicting_parameters": len([g for g in duplicate_groups if 'conflicting_min_transfer_times' in g])
        }

    # Calcul des métriques
    duplication_rate = round(duplicate_count / total_transfers * 100, 2) if total_transfers > 0 else 0
    efficiency_after_cleanup = total_transfers - strict_redundancy_analysis.get('total_redundant_entries', 0)
    efficiency_gain = round(strict_redundancy_analysis.get('total_redundant_entries', 0) / total_transfers * 100, 2) if total_transfers > 0 else 0

    # Détermination du statut
    if duplicate_count == 0:
        status = "success"
    elif duplication_rate <= 2:  # ≤2% de doublons stricts
        status = "warning"
    else:
        status = "error"

    # Construction des issues
    issues = []
    if duplicate_count > 0:
        duplicate_triplet_ids = [f"{g['from_stop_id']}->{g['to_stop_id']} (type {g['transfer_type']})" for g in duplicate_groups]
        issues.append({
            "type": "strict_duplicate",
            "field": "transfer_triplet",
            "count": duplicate_count,
            "affected_ids": duplicate_triplet_ids[:100],
            "message": f"{duplicate_count} correspondances strictement identiques détectées sur {len(duplicate_groups)} triplets"
        })
        
        # Issue spécifique pour les conflits de paramètres
        if strict_redundancy_analysis.get('conflicting_parameters', 0) > 0:
            issues.append({
                "type": "parameter_conflict",
                "field": "min_transfer_time",
                "count": strict_redundancy_analysis['conflicting_parameters'],
                "affected_ids": [f"{g['from_stop_id']}->{g['to_stop_id']}" for g in duplicate_groups if 'conflicting_min_transfer_times' in g][:50],
                "message": f"{strict_redundancy_analysis['conflicting_parameters']} triplets ont des paramètres conflictuels (min_transfer_time différents)"
            })

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_transfers": total_transfers,
            "duplicate_transfers": duplicate_count,
            "unique_transfers": total_transfers - duplicate_count,
            "duplication_rate": duplication_rate,
            "strict_redundancy_analysis": strict_redundancy_analysis,
            "duplicate_groups": duplicate_groups[:10],  # Top 10 exemples
            "optimization_potential": {
                "removable_entries": strict_redundancy_analysis.get('total_redundant_entries', 0),
                "efficiency_gain_percentage": efficiency_gain,
                "optimized_size": efficiency_after_cleanup,
                "compression_ratio": round(efficiency_after_cleanup / total_transfers, 3) if total_transfers > 0 else 1
            },
            "data_quality": {
                "strict_uniqueness": duplicate_count == 0,
                "parameter_consistency": strict_redundancy_analysis.get('conflicting_parameters', 0) == 0,
                "redundancy_level": (
                    "none" if duplicate_count == 0
                    else "minimal" if duplication_rate <= 2
                    else "significant"
                )
            }
        },
        "explanation": {
            "purpose": "Détecte les correspondances strictement identiques (même from_stop_id, to_stop_id ET transfer_type) pour éliminer les redondances exactes",
            "context": f"Analyse de {total_transfers} correspondances avec détection de doublons stricts sur 3 champs clés",
            "duplication_summary": f"Redondance stricte: {duplication_rate}% ({duplicate_count} entrées dupliquées)",
            "optimization_impact": f"Potentiel d'optimisation: {efficiency_gain}% d'espace récupérable ({strict_redundancy_analysis.get('total_redundant_entries', 0)} entrées supprimables)",
            "impact": (
                f"Données de correspondance parfaitement optimisées sans redondance stricte" if status == "success"
                else f"Redondances strictes détectées : {duplicate_count} correspondances identiques sur {len(duplicate_groups)} triplets"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Supprimer {strict_redundancy_analysis.get('total_redundant_entries', 0)} entrées strictement redondantes pour optimiser les données" if duplicate_count > 0 else None,
                f"Résoudre {strict_redundancy_analysis.get('conflicting_parameters', 0)} conflits de min_transfer_time dans les doublons" if strict_redundancy_analysis.get('conflicting_parameters', 0) > 0 else None,
                f"Examiner le triplet avec {strict_redundancy_analysis.get('max_duplicates_per_triplet', 0)} doublons (cause possible: erreur d'import)" if strict_redundancy_analysis.get('max_duplicates_per_triplet', 0) > 3 else None,
                "Conserver une seule entrée par triplet unique en gardant les paramètres les plus appropriés" if duplicate_count > 0 else None,
                f"Optimiser l'efficacité des données (gain possible: {efficiency_gain}%)" if efficiency_gain > 5 else None,
                "Implémenter une validation d'unicité stricte dans votre processus de génération transfers.txt" if duplicate_count > 0 else None,
                "Maintenir cette efficacité parfaite des données de correspondance" if status == "success" else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="transfers",
    name="transfer_rules",
    genre='statistics',
    description="Analyse des règles de correspondance définies dans transfers.txt."
)
def transfer_rules(gtfs_data, **params):
    """
    Analyse statistique complète des règles de correspondance dans transfers.txt
    """
    df = gtfs_data.get('transfers.txt')
    if df is None:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "transfers.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier transfers.txt est requis pour analyser les règles de correspondance"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Analyse statistique complète des règles de correspondance pour évaluer la couverture et configuration du réseau."
            },
            "recommendations": ["Fournir un fichier transfers.txt pour analyser les règles de correspondance."]
        }

    total_transfers = len(df)
    
    # Cas fichier vide
    if total_transfers == 0:
        return {
            "status": "warning",
            "issues": [
                {
                    "type": "empty_file",
                    "field": "transfers.txt",
                    "count": 0,
                    "affected_ids": [],
                    "message": "Le fichier transfers.txt est vide - aucune règle de correspondance définie"
                }
            ],
            "result": {
                "total_transfers": 0,
                "transfer_coverage": "none",
                "network_connectivity": "undefined"
            },
            "explanation": {
                "purpose": "Analyse statistique complète des règles de correspondance pour évaluer la couverture et configuration du réseau",
                "context": "Aucune règle de correspondance définie",
                "impact": "Réseau sans correspondances explicites - connectivité réduite"
            },
            "recommendations": [
                "Définir des règles de correspondance pour améliorer la connectivité du réseau",
                "Identifier les points de correspondance stratégiques entre lignes"
            ]
        }

    # Analyse des types de correspondance
    transfer_type_mapping = {
        0: 'Recommended',      # Correspondance recommandée
        1: 'Timed',           # Correspondance chronométrée
        2: 'Minimum time',    # Temps minimum requis
        3: 'Not possible',    # Correspondance non possible
        4: 'In-seat'          # Correspondance en place (même véhicule)
    }
    
    type_distribution = {
        'Recommended': 0,
        'Timed': 0,
        'Minimum time': 0,
        'Not possible': 0,
        'In-seat': 0,
        'Unknown': 0
    }
    
    # Analyse détaillée par type
    type_analysis = {}
    issues = []
    
    if 'transfer_type' in df.columns:
        type_counts = df['transfer_type'].value_counts()
        unknown_types = []
        
        for transfer_type, count in type_counts.items():
            if pd.isna(transfer_type):
                type_distribution['Unknown'] += int(count)
            elif transfer_type in transfer_type_mapping:
                type_name = transfer_type_mapping[transfer_type]
                type_distribution[type_name] = int(count)
            else:
                unknown_types.append(transfer_type)
                type_distribution['Unknown'] += int(count)
        
        # Issues pour types inconnus
        if unknown_types:
            issues.append({
                "type": "invalid_format",
                "field": "transfer_type",
                "count": len(unknown_types),
                "affected_ids": unknown_types,
                "message": f"Types de correspondance non-standard détectés: {unknown_types}"
            })
        
        # Analyse spécialisée par type
        for type_code, type_name in transfer_type_mapping.items():
            type_subset = df[df['transfer_type'] == type_code]
            if len(type_subset) > 0:
                type_analysis[type_name] = {
                    "count": len(type_subset),
                    "percentage": round(len(type_subset) / total_transfers * 100, 2),
                    "with_min_time": type_subset['min_transfer_time'].notna().sum() if 'min_transfer_time' in type_subset.columns else 0
                }
                
                # Analyse spécifique selon le type
                if type_code == 2 and 'min_transfer_time' in type_subset.columns:
                    # Type 2 doit avoir min_transfer_time
                    missing_time = type_subset['min_transfer_time'].isna().sum()
                    if missing_time > 0:
                        type_analysis[type_name]["missing_required_time"] = int(missing_time)
                
                if 'min_transfer_time' in type_subset.columns:
                    valid_times = type_subset['min_transfer_time'].dropna()
                    if len(valid_times) > 0:
                        type_analysis[type_name]["avg_transfer_time"] = round(valid_times.mean(), 1)
                        type_analysis[type_name]["min_transfer_time_range"] = [int(valid_times.min()), int(valid_times.max())]
    else:
        issues.append({
            "type": "missing_field",
            "field": "transfer_type",
            "count": total_transfers,
            "affected_ids": [],
            "message": "La colonne transfer_type est manquante dans transfers.txt"
        })

    # Analyse des temps de correspondance
    time_analysis = {}
    if 'min_transfer_time' in df.columns:
        with_min_time = df['min_transfer_time'].notna().sum()
        valid_times = df['min_transfer_time'].dropna()
        
        if len(valid_times) > 0:
            time_analysis = {
                "transfers_with_time": int(with_min_time),
                "transfers_without_time": total_transfers - with_min_time,
                "time_coverage_rate": round(with_min_time / total_transfers * 100, 2),
                "avg_transfer_time": round(valid_times.mean(), 1),
                "median_transfer_time": round(valid_times.median(), 1),
                "min_transfer_time": int(valid_times.min()),
                "max_transfer_time": int(valid_times.max()),
                "time_distribution": {
                    "under_60s": (valid_times < 60).sum(),
                    "60_180s": ((valid_times >= 60) & (valid_times < 180)).sum(),
                    "180_300s": ((valid_times >= 180) & (valid_times < 300)).sum(),
                    "over_300s": (valid_times >= 300).sum()
                }
            }
        else:
            time_analysis = {
                "transfers_with_time": 0,
                "transfers_without_time": total_transfers,
                "time_coverage_rate": 0.0
            }
    else:
        time_analysis = {
            "column_available": False,
            "transfers_with_time": 0,
            "transfers_without_time": total_transfers,
            "time_coverage_rate": 0.0
        }

    # Analyse de la connectivité du réseau
    network_analysis = {}
    if 'from_stop_id' in df.columns and 'to_stop_id' in df.columns:
        unique_stops = set(df['from_stop_id']).union(set(df['to_stop_id']))
        network_analysis = {
            "unique_stops_in_transfers": len(unique_stops),
            "total_transfer_pairs": total_transfers,
            "avg_transfers_per_stop": round(total_transfers * 2 / len(unique_stops), 2) if len(unique_stops) > 0 else 0,
            "connectivity_density": round(total_transfers / (len(unique_stops) * (len(unique_stops) - 1)) * 100, 4) if len(unique_stops) > 1 else 0
        }

    # Évaluation de la qualité globale
    quality_assessment = {
        "transfer_diversity": len([count for count in type_distribution.values() if count > 0]),
        "time_completeness": time_analysis.get("time_coverage_rate", 0),
        "type_balance": max(type_distribution.values()) / total_transfers * 100 if total_transfers > 0 else 0,
        "network_coverage": "comprehensive" if total_transfers > 50 else "limited" if total_transfers > 10 else "minimal"
    }

    # Détermination du statut
    if total_transfers == 0:
        status = "warning"
    elif len(issues) > 0:
        status = "error"
    elif quality_assessment["transfer_diversity"] >= 3 and time_analysis.get("time_coverage_rate", 0) >= 50:
        status = "success"
    else:
        status = "warning"

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_transfers": total_transfers,
            "type_distribution": type_distribution,
            "type_analysis": type_analysis,
            "time_analysis": time_analysis,
            "network_analysis": network_analysis,
            "quality_assessment": quality_assessment,
            "transfer_statistics": {
                "most_common_type": max(type_distribution.items(), key=lambda x: x[1])[0] if any(type_distribution.values()) else None,
                "dominant_type_percentage": round(max(type_distribution.values()) / total_transfers * 100, 2) if total_transfers > 0 else 0,
                "forbidden_transfers": type_distribution.get('Not possible', 0),
                "automated_transfers": type_distribution.get('In-seat', 0)
            }
        },
        "explanation": {
            "purpose": "Analyse statistique complète des règles de correspondance pour évaluer la couverture, configuration et qualité du réseau de correspondances",
            "context": f"Analyse de {total_transfers} règles de correspondance avec {quality_assessment['transfer_diversity']} types différents",
            "type_distribution": f"Type principal: {max(type_distribution.items(), key=lambda x: x[1])[0] if any(type_distribution.values()) else 'N/A'} ({round(max(type_distribution.values()) / total_transfers * 100, 2) if total_transfers > 0 else 0}%)",
            "time_coverage": f"Couverture temporelle: {time_analysis.get('time_coverage_rate', 0)}% des correspondances ont un temps défini",
            "network_scope": f"Connectivité: {network_analysis.get('unique_stops_in_transfers', 0)} arrêts impliqués dans les correspondances",
            "impact": (
                f"Réseau de correspondances bien configuré avec {quality_assessment['transfer_diversity']} types et {time_analysis.get('time_coverage_rate', 0)}% de couverture temporelle" if status == "success"
                else f"Configuration des correspondances à améliorer : diversité limitée ou couverture temporelle insuffisante"
            )
        },
        "recommendations": [
            rec for rec in [
                f"Corriger les types de correspondance non-standard: {[issue['affected_ids'] for issue in issues if issue['type'] == 'invalid_format']}" if any(issue['type'] == 'invalid_format' for issue in issues) else None,
                f"Ajouter la colonne transfer_type pour classifier les {total_transfers} correspondances" if any(issue['field'] == 'transfer_type' for issue in issues) else None,
                f"Améliorer la couverture temporelle ({time_analysis.get('time_coverage_rate', 0)}% actuellement)" if time_analysis.get('time_coverage_rate', 0) < 50 else None,
                f"Diversifier les types de correspondance (seulement {quality_assessment['transfer_diversity']} types utilisés)" if quality_assessment['transfer_diversity'] < 3 else None,
                f"Renseigner min_transfer_time pour les {type_analysis.get('Minimum time', {}).get('missing_required_time', 0)} correspondances de type 2" if type_analysis.get('Minimum time', {}).get('missing_required_time', 0) > 0 else None,
                f"Équilibrer la distribution des types (type dominant: {round(max(type_distribution.values()) / total_transfers * 100, 2) if total_transfers > 0 else 0}%)" if quality_assessment['type_balance'] > 80 else None,
                f"Étendre le réseau de correspondances ({quality_assessment['network_coverage']} couverture actuelle)" if quality_assessment['network_coverage'] in ['limited', 'minimal'] else None,
                "Maintenir cette configuration équilibrée des correspondances" if status == "success" else None
            ] if rec is not None
        ]
    }