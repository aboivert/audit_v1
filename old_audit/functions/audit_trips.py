"""
Fonctions d'audit pour le file_type: trips
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="trips",
    name="trips_general_stats",
    genre="statistics",
    description="Statistiques descriptives générales sur les trips sans contrôles de complétude."
)
def trips_general_stats(gtfs_data, **params):
    df = gtfs_data.get('trips.txt')  # ← enlever .txt, cohérence
    if df is None:
        return {
            "status": "error",  # ← Ajouter status
            "issues": [{
                "type": "no_data",
                "field": "trips",
                "count": 0,
                "affected_ids": [],
                "message": "Fichier trips.txt manquant"
            }],
            "result": {},  # ← Nouvelle structure
            "explanation": {
                "purpose": "Analyse statistique complète des voyages (trips) du réseau de transport."
            },
            "recommendations": ["Fournir un fichier trips.txt pour analyser les voyages."]
        }

    total_trips = len(df)
    if total_trips == 0:
        return {
            "status": "warning",
            "issues": [],
            "result": {"total_trips": 0, "statistics": {}},
            "explanation": {
                "purpose": "Analyse statistique des voyages.",
                "data_status": "Le fichier trips.txt est vide."
            },
            "recommendations": []
        }

    # Vérification des colonnes essentielles
    essential_columns = ['trip_id', 'route_id', 'service_id']
    missing_columns = [col for col in essential_columns if col not in df.columns]
    
    # --- Statistiques de base: route_id, service_id, direction_id ---
    route_counts = df['route_id'].value_counts().to_dict() if 'route_id' in df.columns else {}
    service_counts = df['service_id'].value_counts().to_dict() if 'service_id' in df.columns else {}
    direction_counts = df['direction_id'].value_counts().to_dict() if 'direction_id' in df.columns else {}

    # --- Statistiques shapes ---
    if 'shape_id' in df.columns:
        shape_counts = df['shape_id'].value_counts().to_dict()
        trips_with_shape = df['shape_id'].notna().sum()
        trips_without_shape = total_trips - trips_with_shape
        shape_coverage = (trips_with_shape / total_trips * 100) if total_trips > 0 else 0
    else:
        shape_counts = {}
        trips_with_shape = 0
        trips_without_shape = total_trips
        shape_coverage = 0

    # --- Statistiques champs texte ---
    text_fields = ['trip_headsign', 'trip_short_name', 'trip_long_name']
    text_stats = {}
    for col in text_fields:
        if col in df.columns:
            non_null = df[col].dropna().astype(str)
            non_empty = non_null[non_null.str.strip() != '']
            text_stats[col] = {
                "total_entries": len(df),
                "non_null_count": len(non_null),
                "non_empty_count": len(non_empty),
                "coverage_pct": round((len(non_empty) / total_trips * 100), 1) if total_trips > 0 else 0,
                "avg_length": round(non_empty.str.len().mean(), 2) if len(non_empty) > 0 else 0,
                "length_range": {
                    "min": int(non_empty.str.len().min()) if len(non_empty) > 0 else 0,
                    "max": int(non_empty.str.len().max()) if len(non_empty) > 0 else 0
                },
                "unique_values": non_empty.nunique()
            }
        else:
            text_stats[col] = {"column_missing": True}

    # --- Statistiques trip_id ---
    trip_id_analysis = {}
    if 'trip_id' in df.columns:
        trip_ids = df['trip_id'].dropna().astype(str)
        unique_trip_ids = trip_ids.nunique()
        
        # Calcul de l'entropie
        entropy = 0
        if len(trip_ids) > 0:
            freq = Counter(trip_ids)
            total = sum(freq.values())
            entropy = -sum((count / total) * math.log2(count / total) for count in freq.values() if count > 0)
        
        trip_id_analysis = {
            "total_trip_ids": len(trip_ids),
            "unique_trip_ids": int(unique_trip_ids),
            "uniqueness_rate": round((unique_trip_ids / len(trip_ids) * 100), 1) if len(trip_ids) > 0 else 0,
            "avg_length": round(trip_ids.str.len().mean(), 2) if len(trip_ids) > 0 else 0,
            "length_distribution": {
                "min": int(trip_ids.str.len().min()) if len(trip_ids) > 0 else 0,
                "max": int(trip_ids.str.len().max()) if len(trip_ids) > 0 else 0,
                "std": round(trip_ids.str.len().std(), 2) if len(trip_ids) > 0 else 0
            },
            "entropy": round(entropy, 4)
        }

    # Issues pour les problèmes détectés
    issues = []
    if missing_columns:
        issues.append({
            "type": "missing_column",
            "field": "essential_columns",
            "count": len(missing_columns),
            "affected_ids": [],
            "details": missing_columns,
            "message": f"Colonnes essentielles manquantes: {', '.join(missing_columns)}"
        })
    
    if 'trip_id' in df.columns and trip_id_analysis["uniqueness_rate"] < 100:
        duplicate_count = len(trip_ids) - unique_trip_ids
        issues.append({
            "type": "duplicate_identifier",
            "field": "trip_id",
            "count": duplicate_count,
            "affected_ids": [],
            "message": f"{duplicate_count} trip_id dupliqués détectés"
        })

    # Status basé sur la qualité des données
    if missing_columns or (trip_id_analysis.get("uniqueness_rate", 100) < 100):
        status = "error" if missing_columns else "warning"
    elif shape_coverage < 50:  # Moins de 50% des trips ont des shapes
        status = "warning"
    else:
        status = "success"

    return {
        "status": status,
        "issues": issues,
        "result": {  # ← Nouvelle structure unifiée
            "total_trips": total_trips,
            "data_quality": {
                "essential_columns_present": len(essential_columns) - len(missing_columns),
                "missing_columns": missing_columns,
                "trip_id_uniqueness": trip_id_analysis.get("uniqueness_rate", 0)
            },
            "distribution_analysis": {
                "routes": {
                    "unique_routes": len(route_counts),
                    "trips_per_route": route_counts,
                    "most_frequent_route": max(route_counts.items(), key=lambda x: x[1]) if route_counts else None
                },
                "services": {
                    "unique_services": len(service_counts),
                    "trips_per_service": service_counts,
                    "most_used_service": max(service_counts.items(), key=lambda x: x[1]) if service_counts else None
                },
                "directions": {
                    "direction_distribution": direction_counts,
                    "has_direction_info": len(direction_counts) > 0
                }
            },
            "shape_analysis": {
                "shape_coverage_pct": round(shape_coverage, 1),
                "trips_with_shape": int(trips_with_shape),
                "trips_without_shape": int(trips_without_shape),
                "unique_shapes": len(shape_counts),
                "most_used_shape": max(shape_counts.items(), key=lambda x: x[1]) if shape_counts else None
            },
            "text_content_analysis": text_stats,
            "identifier_analysis": trip_id_analysis
        },
        "explanation": {
            "purpose": "Fournit une analyse statistique complète des voyages (trips) pour évaluer la richesse et la qualité des données.",
            "data_overview": f"Analyse de {total_trips} voyages répartis sur {len(route_counts)} routes et {len(service_counts)} services",
            "quality_indicators": {
                "structural_completeness": f"{len(essential_columns) - len(missing_columns)}/{len(essential_columns)} colonnes essentielles présentes",
                "shape_coverage": f"{shape_coverage:.1f}% des voyages ont des tracés géographiques définis",
                "identifier_quality": f"Unicité des trip_id: {trip_id_analysis.get('uniqueness_rate', 0):.1f}%"
            },
            "distribution_insights": f"Route la plus fréquente: {max(route_counts.items(), key=lambda x: x[1])[0] if route_counts else 'N/A'} ({max(route_counts.values()) if route_counts else 0} voyages)"
        },
        "recommendations": [
            rec for rec in [
                f"Ajouter les colonnes manquantes: {', '.join(missing_columns)} selon la spécification GTFS." if missing_columns else None,
                f"Corriger les {len(trip_ids) - unique_trip_ids if 'trip_id' in df.columns else 0} trip_id dupliqués pour garantir l'unicité." if trip_id_analysis.get("uniqueness_rate", 100) < 100 else None,
                f"Améliorer la couverture géographique en ajoutant des shapes aux {trips_without_shape} voyages sans tracé." if shape_coverage < 80 else None,
                "Enrichir les informations textuelles (headsign, noms) pour améliorer l'expérience utilisateur." if any(stats.get("coverage_pct", 0) < 50 for stats in text_stats.values() if isinstance(stats, dict)) else None,
                f"Rééquilibrer la distribution des voyages si certaines routes sont sur-représentées." if route_counts and max(route_counts.values()) > total_trips * 0.3 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="trips",
    name="check_trip_id_uniqueness",
    genre="validity",
    description="Vérifie l'unicité de trip_id.",
    parameters={}
)
def check_trip_id_uniqueness(gtfs_data, **params):
   """
   Vérifie l'unicité des trip_id dans trips.txt (contrainte GTFS obligatoire)
   """
   df = gtfs_data.get('trips.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "trips.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier trips.txt est requis pour vérifier l'unicité des trip_id"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Vérifie l'unicité des trip_id selon la contrainte obligatoire GTFS."
           },
           "recommendations": ["Fournir un fichier trips.txt valide."]
       }
   
   total_trips = len(df)
   
   # Vérification de la présence de la colonne trip_id
   if 'trip_id' not in df.columns:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "trip_id",
                   "count": total_trips,
                   "affected_ids": [],
                   "message": "La colonne trip_id est obligatoire dans trips.txt"
               }
           ],
           "result": {
               "total_trips": total_trips,
               "unique_trip_ids": 0,
               "duplicate_trip_ids": [],
               "duplicate_count": 0
           },
           "explanation": {
               "purpose": "Vérifie l'unicité des trip_id selon la contrainte obligatoire GTFS",
               "context": "Colonne trip_id obligatoire manquante",
               "impact": "Impossible d'identifier les trips - structure GTFS invalide"
           },
           "recommendations": [
               "Ajouter la colonne trip_id obligatoire dans trips.txt",
               "Générer des identifiants uniques pour chaque trip"
           ]
       }
   
   # Détection des trip_id dupliqués
   duplicated_mask = df['trip_id'].duplicated(keep=False)
   duplicated_trip_ids = df.loc[duplicated_mask, 'trip_id'].unique().tolist()
   duplicate_count = len(duplicated_trip_ids)
   total_duplicated_rows = duplicated_mask.sum()
   unique_trip_ids = total_trips - total_duplicated_rows + duplicate_count
   
   # Analyse détaillée des doublons
   duplicate_analysis = {}
   if duplicate_count > 0:
       # Comptage des occurrences par trip_id dupliqué
       duplicate_counts = df[df['trip_id'].isin(duplicated_trip_ids)]['trip_id'].value_counts()
       duplicate_analysis = {
           "max_occurrences": int(duplicate_counts.max()),
           "min_occurrences": int(duplicate_counts.min()),
           "avg_occurrences": round(duplicate_counts.mean(), 2),
           "distribution": duplicate_counts.to_dict(),
           "worst_offenders": duplicate_counts.head(5).to_dict()  # Top 5 des plus dupliqués
       }
   
   # Calcul du taux de duplication
   duplication_rate = round(total_duplicated_rows / total_trips * 100, 2) if total_trips > 0 else 0
   
   # Détermination du statut
   if duplicate_count == 0:
       status = "success"
   else:
       status = "error"  # Violation grave de la contrainte GTFS
   
   # Construction des issues
   issues = []
   if duplicate_count > 0:
       issues.append({
           "type": "duplicate_key",
           "field": "trip_id",
           "count": duplicate_count,
           "affected_ids": duplicated_trip_ids[:100],
           "message": f"{duplicate_count} trip_id dupliqués détectés ({total_duplicated_rows} lignes concernées)"
       })
   
   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_trips": total_trips,
           "unique_trip_ids": unique_trip_ids,
           "duplicate_trip_ids": duplicated_trip_ids,
           "duplicate_count": duplicate_count,
           "total_duplicated_rows": int(total_duplicated_rows),
           "duplication_rate": duplication_rate,
           "duplicate_analysis": duplicate_analysis,
           "integrity_status": {
               "primary_key_valid": duplicate_count == 0,
               "data_consistency": "valid" if duplicate_count == 0 else "compromised",
               "gtfs_compliance": "compliant" if duplicate_count == 0 else "non_compliant"
           }
       },
       "explanation": {
           "purpose": "Vérifie l'unicité des trip_id selon la contrainte de clé primaire obligatoire GTFS",
           "constraint": "trip_id doit être unique dans trips.txt selon la spécification GTFS",
           "context": f"Analyse de {total_trips} trips avec {unique_trip_ids} identifiants uniques",
           "duplication_summary": f"Taux de duplication: {duplication_rate}% ({total_duplicated_rows} lignes dupliquées)" if duplicate_count > 0 else "Aucune duplication détectée",
           "impact": (
               "Contrainte d'unicité respectée - intégrité GTFS assurée" if duplicate_count == 0
               else f"Violation critique : {duplicate_count} trip_id dupliqués compromettent l'intégrité des données"
           )
       },
       "recommendations": [
           rec for rec in [
               f"URGENT: Corriger {duplicate_count} trip_id dupliqués pour restaurer la conformité GTFS" if duplicate_count > 0 else None,
               f"Examiner les {duplicate_analysis.get('max_occurrences', 0)} occurrences du trip_id le plus dupliqué" if duplicate_count > 0 and duplicate_analysis.get('max_occurrences', 0) > 2 else None,
               "Implémenter une validation d'unicité dans votre processus de génération GTFS" if duplicate_count > 0 else None,
               "Générer des suffixes uniques pour résoudre les conflits (ex: trip_001, trip_002)" if duplicate_count > 0 else None,
               "Vérifier la source des doublons (fusion de fichiers, erreur de process?)" if duplicate_count > 0 else None,
               "Maintenir cette contrainte d'unicité pour assurer l'intégrité référentielle" if duplicate_count == 0 else None
           ] if rec is not None
       ]
   }
    
@audit_function(
    file_type="trips",
    name="check_required_columns",
    genre="validity",
    description="Vérifie la présence des colonnes obligatoires route_id, service_id, trip_id.",
    parameters={}
)
def check_required_columns(gtfs_data, **params):
    df = gtfs_data.get('trips.txt')  # ← enlever .txt, cohérence
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier trips.txt manquant."],
            "result": {},  # ← Nouvelle structure unifiée
            "explanation": {
                "purpose": "Vérifie la présence des colonnes obligatoires dans le fichier trips.txt selon la norme GTFS."
            },
            "recommendations": ["Fournir un fichier trips.txt valide."]
        }
    
    # Colonnes obligatoires selon GTFS
    required_columns = {'route_id', 'service_id', 'trip_id'}
    present_columns = set(df.columns)
    missing_columns = list(required_columns - present_columns)
    present_required = list(required_columns & present_columns)
    
    total_required = len(required_columns)
    present_count = len(present_required)
    missing_count = len(missing_columns)
    
    # Score de complétude structurelle
    completeness_score = (present_count / total_required) * 100
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if missing_count > 0:
        issues.append({
            "type": "missing_column",
            "field": "required_columns",
            "count": missing_count,
            "affected_ids": [],
            "details": missing_columns,
            "required_columns": list(required_columns),
            "message": f"{missing_count} colonnes obligatoires manquantes: {', '.join(missing_columns)}"
        })
    
    # Status basé sur la complétude
    if completeness_score == 100:
        status = "success"
    elif completeness_score >= 67:  # Au moins 2/3 des colonnes requises (2/3)
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_required_columns": total_required,
            "present_columns": present_count,
            "missing_columns": missing_count,
            "completeness_score": round(completeness_score, 1),
            "column_analysis": {
                "present_required": present_required,
                "missing_required": missing_columns,
                "all_columns": list(present_columns)
            },
            "structural_compliance": completeness_score == 100
        },
        "explanation": {
            "purpose": "Vérifie la présence des colonnes obligatoires dans trips.txt selon la spécification GTFS.",
            "required_fields": {
                "route_id": "Référence à la ligne de transport (clé étrangère vers routes.txt)",
                "service_id": "Référence au calendrier de service (clé étrangère vers calendar.txt)",
                "trip_id": "Identifiant unique du voyage"
            },
            "compliance_status": f"Structure conforme à {completeness_score:.1f}% - {present_count}/{total_required} colonnes requises présentes",
            "validation_result": "Toutes les colonnes obligatoires sont présentes" if missing_count == 0 else f"Colonnes manquantes: {', '.join(missing_columns)}",
            "structural_impact": "Les colonnes manquantes empêchent le bon fonctionnement du GTFS" if missing_count > 0 else "Structure conforme pour les références GTFS"
        },
        "recommendations": [
            rec for rec in [
                f"Ajouter immédiatement les colonnes manquantes: {', '.join(missing_columns)} selon la spécification GTFS." if missing_count > 0 else None,
                "Consulter la documentation GTFS officielle pour le format exact des colonnes requises." if missing_count > 0 else None,
                "Vérifier l'intégrité référentielle des colonnes présentes avec les autres fichiers GTFS." if present_count > 0 and missing_count == 0 else None,
                "Considérer l'ajout de colonnes optionnelles (trip_headsign, direction_id, shape_id, etc.) pour enrichir les données." if missing_count == 0 else None,
                "Valider que les colonnes présentes contiennent des données valides et non vides." if missing_count == 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="trips",
    name="headsign_completion_rate",
    genre="completeness",
    description="Taux de complétude du champ trip_headsign.",
    parameters={}
)
def headsign_completion_rate(gtfs_data, **params):
    """
    Analyse le taux de complétude du champ trip_headsign dans trips.txt
    """
    # Vérification de la présence du fichier
    if 'trips.txt' not in gtfs_data:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "trips.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier trips.txt est requis pour analyser les headsigns"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Évalue le taux de complétude du champ trip_headsign qui améliore l'information voyageur",
                "context": "Fichier trips.txt manquant"
            },
            "recommendations": [
                "Fournir le fichier trips.txt obligatoire"
            ]
        }
    
    df = gtfs_data['trips.txt']
    
    # Vérification de la présence de la colonne
    if 'trip_headsign' not in df.columns:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "trip_headsign",
                    "count": len(df),
                    "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                    "message": "La colonne trip_headsign est absente du fichier trips.txt"
                }
            ],
            "result": {
                "total_trips": len(df),
                "trips_with_headsign": 0,
                "completion_rate": 0.0
            },
            "explanation": {
                "purpose": "Évalue le taux de complétude du champ trip_headsign qui améliore l'information voyageur",
                "context": "Colonne trip_headsign manquante"
            },
            "recommendations": [
                "Ajouter la colonne trip_headsign dans trips.txt pour améliorer l'expérience voyageur"
            ]
        }
    
    # Calcul des métriques
    total = len(df)
    present = df['trip_headsign'].notna().sum()
    missing = total - present
    rate = round(present / total * 100, 2) if total > 0 else 0
    
    # Détermination du statut selon des seuils intelligents
    if rate == 100:
        status = "success"
    elif rate >= 80:
        status = "warning"
    else:
        status = "error"
    
    # Construction des issues
    issues = []
    if missing > 0:
        # Récupération des IDs des trips sans headsign
        missing_ids = df[df['trip_headsign'].isna()]['trip_id'].tolist() if 'trip_id' in df.columns else []
        
        issues.append({
            "type": "missing_data",
            "field": "trip_headsign",
            "count": missing,
            "affected_ids": missing_ids[:100],  # Limiter à 100 IDs pour éviter la surcharge
            "message": f"{missing} trips ({100-rate:.1f}%) n'ont pas de headsign défini"
        })
    
    # Résultat structuré
    result = {
        "total_trips": total,
        "trips_with_headsign": present,
        "trips_without_headsign": missing,
        "completion_rate": rate,
        "quality_level": (
            "excellent" if rate == 100 
            else "good" if rate >= 80 
            else "poor"
        )
    }
    
    # Explication enrichie
    explanation = {
        "purpose": "Évalue le taux de complétude du champ trip_headsign qui améliore l'information voyageur en affichant la destination",
        "context": f"Analyse de {total} trips dans le fichier trips.txt",
        "impact": (
            "Information voyageur optimale avec tous les headsigns renseignés" if rate == 100
            else f"Information voyageur incomplète : {missing} trips sans indication de destination"
        )
    }
    
    # Recommandations conditionnelles
    recommendations = [
        rec for rec in [
            None if rate == 100 else f"Compléter le champ trip_headsign pour {missing} trips manquants",
            "Vérifier la cohérence des headsigns avec les destinations réelles" if rate < 90 else None,
            "Considérer l'utilisation de stop_headsign dans stop_times.txt comme alternative" if rate < 50 else None
        ] if rec is not None
    ]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": explanation,
        "recommendations": recommendations
    }

@audit_function(
    file_type="trips",
    name="validate_direction_id",
    genre="validity",
    description="Vérifie que direction_id contient uniquement 0 ou 1.",
    parameters={}
)
def validate_direction_id(gtfs_data, **params):
    """
    Valide que les valeurs direction_id sont conformes (0 ou 1) dans trips.txt
    """
    # Vérification de la présence du fichier
    if 'trips.txt' not in gtfs_data:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "trips.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier trips.txt est requis pour valider les direction_id"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Valide que les direction_id respectent la spécification GTFS (0 ou 1 uniquement)",
                "context": "Fichier trips.txt manquant"
            },
            "recommendations": [
                "Fournir le fichier trips.txt obligatoire"
            ]
        }
    
    df = gtfs_data['trips.txt']
    
    # Vérification de la présence de la colonne
    if 'direction_id' not in df.columns:
        return {
            "status": "warning",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "direction_id",
                    "count": len(df),
                    "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                    "message": "La colonne direction_id est absente (champ optionnel mais recommandé)"
                }
            ],
            "result": {
                "total_trips": len(df),
                "valid_direction_ids": 0,
                "invalid_direction_ids": 0,
                "missing_direction_ids": len(df),
                "validation_rate": 0.0
            },
            "explanation": {
                "purpose": "Valide que les direction_id respectent la spécification GTFS (0 ou 1 uniquement)",
                "context": "Colonne direction_id absente du fichier trips.txt",
                "impact": "Impossible de distinguer les sens de circulation des trips"
            },
            "recommendations": [
                "Ajouter la colonne direction_id avec des valeurs 0 ou 1 pour distinguer les sens de circulation"
            ]
        }
    
    # Analyse des valeurs direction_id
    total = len(df)
    valid_values = [0, 1]
    
    # Calcul des métriques détaillées
    valid_mask = df['direction_id'].isin(valid_values)
    null_mask = df['direction_id'].isna()
    
    valid_count = valid_mask.sum()
    null_count = null_mask.sum()
    invalid_count = total - valid_count - null_count
    
    validation_rate = round(valid_count / total * 100, 2) if total > 0 else 0
    
    # Récupération des IDs problématiques
    invalid_ids = df.loc[~valid_mask & ~null_mask, 'trip_id'].tolist() if 'trip_id' in df.columns else []
    null_ids = df.loc[null_mask, 'trip_id'].tolist() if 'trip_id' in df.columns else []
    
    # Détermination du statut
    if invalid_count == 0 and null_count == 0:
        status = "success"
    elif invalid_count == 0 and null_count > 0:
        status = "warning"  # Valeurs nulles acceptables mais sous-optimales
    else:
        status = "error"  # Valeurs invalides détectées
    
    # Construction des issues
    issues = []
    
    if invalid_count > 0:
        # Analyse des valeurs invalides détectées
        invalid_values = df.loc[~valid_mask & ~null_mask, 'direction_id'].unique()
        
        issues.append({
            "type": "invalid_format",
            "field": "direction_id",
            "count": invalid_count,
            "affected_ids": invalid_ids[:100],  # Limiter à 100 IDs
            "message": f"{invalid_count} trips ont des direction_id invalides: {list(invalid_values)}"
        })
    
    if null_count > 0:
        issues.append({
            "type": "missing_data",
            "field": "direction_id",
            "count": null_count,
            "affected_ids": null_ids[:100],
            "message": f"{null_count} trips ont des direction_id manquants (null/vide)"
        })
    
    # Résultat structuré avec distribution des valeurs
    direction_distribution = df['direction_id'].value_counts().to_dict() if valid_count > 0 else {}
    
    result = {
        "total_trips": total,
        "valid_direction_ids": valid_count,
        "invalid_direction_ids": invalid_count,
        "missing_direction_ids": null_count,
        "validation_rate": validation_rate,
        "direction_distribution": {
            "outbound": direction_distribution.get(0, 0),
            "inbound": direction_distribution.get(1, 0)
        },
        "quality_level": (
            "excellent" if validation_rate == 100 and null_count == 0
            else "good" if validation_rate >= 95
            else "poor"
        )
    }
    
    # Explication enrichie
    explanation = {
        "purpose": "Valide que les direction_id respectent la spécification GTFS (0=outbound, 1=inbound) pour distinguer les sens de circulation",
        "context": f"Analyse de {total} trips dans le fichier trips.txt",
        "specification": "Les valeurs autorisées sont 0 (aller) ou 1 (retour), ou null si non spécifié",
        "impact": (
            "Direction des trips correctement spécifiée" if status == "success"
            else f"Problèmes de direction détectés : {invalid_count} invalides, {null_count} manquants"
        )
    }
    
    # Recommandations conditionnelles
    recommendations = [
        rec for rec in [
            f"Corriger {invalid_count} valeurs direction_id invalides (utiliser 0 ou 1 uniquement)" if invalid_count > 0 else None,
            f"Considérer renseigner les {null_count} direction_id manquants pour améliorer la qualité" if null_count > 0 else None,
            "Vérifier que 0=aller et 1=retour correspondent bien à votre logique métier" if invalid_count > 0 else None,
            "Harmoniser les direction_id avec les patterns de stops pour assurer la cohérence" if validation_rate < 90 else None
        ] if rec is not None
    ]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": explanation,
        "recommendations": recommendations
    }


@audit_function(
    file_type="trips",
    name="shape_id_distribution",
    genre="statistics",
    description="Analyse la distribution des trips par shape_id.",
    parameters={}
)
def shape_id_distribution(gtfs_data, **params):
    """
    Analyse la distribution des shape_id dans trips.txt et détecte les trips sans forme géométrique
    """
    # Vérification de la présence du fichier
    if 'trips.txt' not in gtfs_data:
        return {
            "status": "error",
            "issues": [
                {
                    "type": "missing_file",
                    "field": "trips.txt",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Le fichier trips.txt est requis pour analyser la distribution des shape_id"
                }
            ],
            "result": {None},
            "explanation": {
                "purpose": "Analyse la distribution des shape_id pour évaluer la couverture géométrique des trips",
                "context": "Fichier trips.txt manquant"
            },
            "recommendations": [
                "Fournir le fichier trips.txt obligatoire"
            ]
        }
    
    df = gtfs_data['trips.txt']
    
    # Vérification de la présence de la colonne
    if 'shape_id' not in df.columns:
        return {
            "status": "warning",
            "issues": [
                {
                    "type": "missing_field",
                    "field": "shape_id",
                    "count": len(df),
                    "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                    "message": "La colonne shape_id est absente (champ optionnel mais recommandé pour la géométrie)"
                }
            ],
            "result": {
                "total_trips": len(df),
                "trips_with_shape": 0,
                "trips_without_shape": len(df),
                "coverage_rate": 0.0,
                "unique_shapes": 0,
                "shape_distribution": {}
            },
            "explanation": {
                "purpose": "Analyse la distribution des shape_id pour évaluer la couverture géométrique des trips",
                "context": "Colonne shape_id absente du fichier trips.txt",
                "impact": "Aucune information géométrique disponible pour tracer les parcours"
            },
            "recommendations": [
                "Ajouter la colonne shape_id pour permettre la représentation géométrique des parcours",
                "Créer des formes dans shapes.txt si nécessaire"
            ]
        }
    
    # Analyse de la distribution des shape_id
    total = len(df)
    
    # Calcul des métriques
    has_shape_mask = df['shape_id'].notna()
    trips_with_shape = has_shape_mask.sum()
    trips_without_shape = total - trips_with_shape
    coverage_rate = round(trips_with_shape / total * 100, 2) if total > 0 else 0
    
    # Distribution des shapes
    shape_counts = df['shape_id'].value_counts().to_dict()
    unique_shapes = len(shape_counts)
    
    # IDs des trips sans shape
    trips_without_shape_ids = df.loc[~has_shape_mask, 'trip_id'].tolist() if 'trip_id' in df.columns else []
    
    # Analyse statistique de la distribution
    if unique_shapes > 0:
        shape_usage_counts = list(shape_counts.values())
        avg_trips_per_shape = round(sum(shape_usage_counts) / len(shape_usage_counts), 2)
        max_usage = max(shape_usage_counts)
        min_usage = min(shape_usage_counts)
        
        # Shapes peu utilisées (utilisées par 1 seul trip)
        underused_shapes = [shape_id for shape_id, count in shape_counts.items() if count == 1]
        overused_shapes = [shape_id for shape_id, count in shape_counts.items() if count > avg_trips_per_shape * 2]
    else:
        avg_trips_per_shape = 0
        max_usage = 0
        min_usage = 0
        underused_shapes = []
        overused_shapes = []
    
    # Détermination du statut
    if trips_without_shape == 0:
        status = "success"
    elif coverage_rate >= 80:
        status = "warning"
    else:
        status = "error"
    
    # Construction des issues
    issues = []
    
    if trips_without_shape > 0:
        issues.append({
            "type": "missing_data",
            "field": "shape_id",
            "count": trips_without_shape,
            "affected_ids": trips_without_shape_ids[:100],
            "message": f"{trips_without_shape} trips ({100-coverage_rate:.1f}%) n'ont pas de shape_id défini"
        })
    
    # Issue pour les shapes sous-utilisées (optionnel, selon le contexte)
    if len(underused_shapes) > unique_shapes * 0.3 and unique_shapes > 10:  # Si >30% des shapes ne servent qu'à 1 trip
        issues.append({
            "type": "inefficient_data",
            "field": "shape_id",
            "count": len(underused_shapes),
            "affected_ids": underused_shapes[:50],
            "message": f"{len(underused_shapes)} shapes ne sont utilisées que par un seul trip (possibles doublons)"
        })
    
    # Résultat structuré
    result = {
        "total_trips": total,
        "trips_with_shape": trips_with_shape,
        "trips_without_shape": trips_without_shape,
        "coverage_rate": coverage_rate,
        "unique_shapes": unique_shapes,
        "shape_distribution": {
            "top_10_shapes": dict(list(shape_counts.items())[:10]),
            "total_shapes": len(shape_counts),
            "avg_trips_per_shape": avg_trips_per_shape,
            "max_trips_per_shape": max_usage,
            "min_trips_per_shape": min_usage
        },
        "shape_analysis": {
            "underused_shapes": len(underused_shapes),
            "overused_shapes": len(overused_shapes),
            "single_use_shapes": len([s for s in shape_counts.values() if s == 1])
        },
        "quality_level": (
            "excellent" if coverage_rate == 100
            else "good" if coverage_rate >= 80
            else "poor"
        )
    }
    
    # Explication enrichie
    explanation = {
        "purpose": "Analyse la distribution des shape_id pour évaluer la couverture géométrique des trips et optimiser les formes",
        "context": f"Analyse de {total} trips avec {unique_shapes} formes géométriques uniques",
        "coverage_analysis": f"Taux de couverture géométrique: {coverage_rate}%",
        "impact": (
            f"Couverture géométrique complète pour tous les trips" if coverage_rate == 100
            else f"Couverture partielle : {trips_without_shape} trips sans représentation géométrique"
        )
    }
    
    # Recommandations conditionnelles
    recommendations = [
        rec for rec in [
            f"Compléter les shape_id pour {trips_without_shape} trips manquants" if trips_without_shape > 0 else None,
            f"Créer les formes géométriques dans shapes.txt si manquantes" if coverage_rate < 100 else None,
            f"Examiner les {len(underused_shapes)} shapes utilisées une seule fois (possibles doublons)" if len(underused_shapes) > unique_shapes * 0.2 else None,
            "Vérifier la cohérence entre shapes.txt et les shape_id référencés" if 'shapes' in gtfs_data and coverage_rate > 0 else None,
            "Optimiser la réutilisation des shapes pour des parcours similaires" if avg_trips_per_shape < 2 and unique_shapes > 50 else None
        ] if rec is not None
    ]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": explanation,
        "recommendations": recommendations
    }

@audit_function(
    file_type="trips",
    name="trips_without_shape",
    genre="completeness",
    description="Compte le nombre de trips sans shape_id.",
    parameters={}
)
def trips_without_shape(gtfs_data, **params):
   """
   Identifie les trips sans shape_id défini dans trips.txt
   """
   # Vérification de la présence du fichier
   if 'trips.txt' not in gtfs_data:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "trips.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier trips.txt est requis pour identifier les trips sans forme géométrique"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Identifie les trips sans shape_id pour évaluer la complétude de la géométrie des parcours",
               "context": "Fichier trips.txt manquant"
           },
           "recommendations": [
               "Fournir le fichier trips.txt obligatoire"
           ]
       }
   
   df = gtfs_data['trips.txt']
   total = len(df)
   
   # Vérification de la présence de la colonne shape_id
   if 'shape_id' not in df.columns:
       return {
           "status": "warning",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "shape_id",
                   "count": total,
                   "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                   "message": "La colonne shape_id est absente - tous les trips sont sans forme géométrique"
               }
           ],
           "result": {
               "total_trips": total,
               "trips_without_shape": total,
               "trips_with_shape": 0,
               "missing_rate": 100.0
           },
           "explanation": {
               "purpose": "Identifie les trips sans shape_id pour évaluer la complétude de la géométrie des parcours",
               "context": "Colonne shape_id absente du fichier trips.txt",
               "impact": "Aucune information géométrique disponible pour tous les trips"
           },
           "recommendations": [
               "Ajouter la colonne shape_id dans trips.txt",
               "Créer des formes géométriques dans shapes.txt"
           ]
       }
   
   # Analyse des trips sans shape_id
   missing_mask = df['shape_id'].isna()
   missing_count = missing_mask.sum()
   with_shape_count = total - missing_count
   missing_rate = round(missing_count / total * 100, 2) if total > 0 else 0
   
   # IDs des trips sans shape
   missing_trip_ids = df.loc[missing_mask, 'trip_id'].tolist() if 'trip_id' in df.columns else []
   
   # Détermination du statut
   if missing_count == 0:
       status = "success"
   elif missing_rate <= 10:
       status = "warning"
   else:
       status = "error"
   
   # Construction des issues
   issues = []
   if missing_count > 0:
       issues.append({
           "type": "missing_data",
           "field": "shape_id",
           "count": missing_count,
           "affected_ids": missing_trip_ids[:100],  # Limiter à 100 IDs
           "message": f"{missing_count} trips ({missing_rate}%) n'ont pas de shape_id défini"
       })
   
   # Résultat structuré
   result = {
       "total_trips": total,
       "trips_without_shape": int(missing_count),
       "trips_with_shape": int(with_shape_count),
       "missing_rate": missing_rate,
       "completion_rate": round(100 - missing_rate, 2),
       "quality_level": (
           "excellent" if missing_count == 0
           else "good" if missing_rate <= 10
           else "poor"
       )
   }
   
   # Explication enrichie
   explanation = {
       "purpose": "Identifie les trips sans shape_id pour évaluer la complétude de la géométrie des parcours",
       "context": f"Analyse de {total} trips dans le fichier trips.txt",
       "impact": (
           "Tous les trips ont une forme géométrique définie" if missing_count == 0
           else f"{missing_count} trips sans représentation géométrique affectent la visualisation des parcours"
       )
   }
   
   # Recommandations conditionnelles
   recommendations = [
       rec for rec in [
           f"Compléter les shape_id pour {missing_count} trips manquants" if missing_count > 0 else None,
           "Créer les formes correspondantes dans shapes.txt si nécessaire" if missing_count > 0 else None,
           "Prioriser les lignes principales pour l'ajout de formes géométriques" if missing_rate > 50 else None,
           "Vérifier la cohérence entre trips.txt et shapes.txt" if missing_count > 0 and 'shapes' in gtfs_data else None
       ] if rec is not None
   ]
   
   return {
       "status": status,
       "issues": issues,
       "result": result,
       "explanation": explanation,
       "recommendations": recommendations
   }



@audit_function(
    file_type="trips",
    name="service_id_variability",
    genre="statistics",
    description="Analyse le nombre de trips par service_id.",
    parameters={}
)
def service_id_variability(gtfs_data, **params):
   """
   Analyse la variabilité et distribution des service_id dans trips.txt
   """
   # Vérification de la présence du fichier
   if 'trips.txt' not in gtfs_data:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "trips.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier trips.txt est requis pour analyser la variabilité des service_id"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Analyse la distribution des service_id pour évaluer la diversité des services de transport",
               "context": "Fichier trips.txt manquant"
           },
           "recommendations": [
               "Fournir le fichier trips.txt obligatoire"
           ]
       }
   
   df = gtfs_data['trips.txt']
   total = len(df)
   
   # Vérification de la présence de la colonne service_id
   if 'service_id' not in df.columns:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "service_id",
                   "count": total,
                   "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                   "message": "La colonne service_id est obligatoire dans trips.txt"
               }
           ],
           "result": {
               "total_trips": total,
               "unique_services": 0,
               "service_distribution": {}
           },
           "explanation": {
               "purpose": "Analyse la distribution des service_id pour évaluer la diversité des services de transport",
               "context": "Colonne service_id obligatoire manquante",
               "impact": "Impossible d'associer les trips aux calendriers de service"
           },
           "recommendations": [
               "Ajouter la colonne service_id obligatoire dans trips.txt",
               "Créer les services correspondants dans calendar.txt ou calendar_dates.txt"
           ]
       }
   
   # Analyse des service_id
   service_counts = df['service_id'].value_counts().to_dict()
   null_services = df['service_id'].isna().sum()
   valid_services = total - null_services
   unique_services = len(service_counts)
   
   # IDs des trips sans service_id
   trips_without_service = df.loc[df['service_id'].isna(), 'trip_id'].tolist() if 'trip_id' in df.columns else []
   
   # Analyse statistique de la distribution
   if unique_services > 0:
       service_usage = list(service_counts.values())
       avg_trips_per_service = round(sum(service_usage) / len(service_usage), 2)
       max_trips = max(service_usage)
       min_trips = min(service_usage)
       
       # Services peu utilisés (≤ 5 trips) et très utilisés
       underused_services = [sid for sid, count in service_counts.items() if count <= 5]
       overused_services = [sid for sid, count in service_counts.items() if count > avg_trips_per_service * 3]
       
       # Coefficient de variation pour mesurer l'équilibre
       import statistics
       cv = round(statistics.stdev(service_usage) / statistics.mean(service_usage) * 100, 2) if len(service_usage) > 1 else 0
   else:
       avg_trips_per_service = 0
       max_trips = 0
       min_trips = 0
       underused_services = []
       overused_services = []
       cv = 0
   
   # Détermination du statut
   if null_services > 0:
       status = "error"
   elif unique_services == 0:
       status = "error"
   elif unique_services == 1:
       status = "warning"  # Un seul service peut être problématique
   else:
       status = "success"
   
   # Construction des issues
   issues = []
   
   if null_services > 0:
       issues.append({
           "type": "missing_data",
           "field": "service_id",
           "count": null_services,
           "affected_ids": trips_without_service[:100],
           "message": f"{null_services} trips n'ont pas de service_id défini"
       })
   
   if unique_services == 1 and null_services == 0:
       issues.append({
           "type": "insufficient_variety",
           "field": "service_id",
           "count": 1,
           "affected_ids": [],
           "message": "Un seul service_id utilisé - diversité des services limitée"
       })
   
   if len(underused_services) > unique_services * 0.3 and unique_services > 5:
       issues.append({
           "type": "inefficient_data",
           "field": "service_id",
           "count": len(underused_services),
           "affected_ids": underused_services[:50],
           "message": f"{len(underused_services)} services peu utilisés (≤5 trips) - possible sur-segmentation"
       })
   
   # Résultat structuré
   result = {
       "total_trips": total,
       "trips_with_service": valid_services,
       "trips_without_service": int(null_services),
       "unique_services": unique_services,
       "service_distribution": {
           "top_10_services": dict(list(service_counts.items())[:10]),
           "avg_trips_per_service": avg_trips_per_service,
           "max_trips_per_service": max_trips,
           "min_trips_per_service": min_trips,
           "coefficient_variation": cv
       },
       "service_analysis": {
           "underused_services": len(underused_services),
           "overused_services": len(overused_services),
           "balance_score": max(0, 100 - cv)  # Score d'équilibre (100 = parfait)
       },
       "quality_level": (
           "excellent" if unique_services > 1 and null_services == 0 and cv < 50
           else "good" if unique_services > 1 and null_services == 0
           else "poor"
       )
   }
   
   # Explication enrichie
   explanation = {
       "purpose": "Analyse la distribution des service_id pour évaluer la diversité et l'équilibre des services de transport",
       "context": f"Analyse de {total} trips répartis sur {unique_services} services différents",
       "variability_analysis": f"Coefficient de variation: {cv}% ({'équilibré' if cv < 50 else 'déséquilibré'})",
       "impact": (
           f"Distribution équilibrée avec {unique_services} services" if status == "success" and cv < 50
           else f"Distribution déséquilibrée ou problématique : {null_services} trips sans service"
       )
   }
   
   # Recommandations conditionnelles
   recommendations = [
       rec for rec in [
           f"Corriger les {null_services} trips sans service_id" if null_services > 0 else None,
           "Diversifier les services pour couvrir différentes périodes/types de desserte" if unique_services == 1 else None,
           f"Réévaluer les {len(underused_services)} services peu utilisés (possibles regroupements)" if len(underused_services) > unique_services * 0.3 else None,
           f"Équilibrer la répartition des trips entre services (CV={cv}%)" if cv > 75 else None,
           "Vérifier la cohérence avec calendar.txt et calendar_dates.txt" if unique_services > 0 else None,
           "Optimiser l'organisation des services selon les besoins opérationnels" if len(overused_services) > 3 else None
       ] if rec is not None
   ]
   
   return {
       "status": status,
       "issues": issues,
       "result": result,
       "explanation": explanation,
       "recommendations": recommendations
   }

@audit_function(
    file_type="trips",
    name="trip_name_field_completeness",
    genre="completeness",
    description="Analyse la complétude et la longueur moyenne des champs trip_short_name et trip_long_name.",
    parameters={}
)
def trip_name_field_completeness(gtfs_data, **params):
   """
   Analyse la complétude des champs de nommage des trips (trip_short_name, trip_long_name)
   """
   df = gtfs_data.get('trips.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "trips.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier trips.txt est requis pour analyser les noms de trips"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Analyse la complétude des champs de nommage des trips pour améliorer l'information voyageur."
           },
           "recommendations": ["Fournir un fichier trips.txt valide."]
       }
   
   total_trips = len(df)
   name_fields = ['trip_short_name', 'trip_long_name']
   field_results = {}
   issues = []
   overall_missing_count = 0
   
   # Analyse par champ de nommage
   for field in name_fields:
       if field in df.columns:
           non_null = df[field].dropna()
           empty_strings = df[field].eq('').sum()  # Compter les chaînes vides
           valid_data = df[field].notna() & df[field].ne('')
           
           completion_rate = round(valid_data.sum() / total_trips * 100, 2)
           missing_count = total_trips - valid_data.sum()
           avg_length = round(non_null.str.len().mean(), 2) if not non_null.empty else 0
           unique_values = non_null.nunique()
           
           # IDs des trips avec données manquantes
           missing_ids = df.loc[~valid_data, 'trip_id'].tolist() if 'trip_id' in df.columns else []
           
           field_results[field] = {
               "present": True,
               "completion_rate": completion_rate,
               "missing_count": missing_count,
               "average_length": avg_length,
               "unique_values": unique_values,
               "empty_strings": int(empty_strings),
               "quality_level": (
                   "excellent" if completion_rate == 100
                   else "good" if completion_rate >= 75
                   else "poor"
               )
           }
           
           # Ajout d'issues si nécessaire
           if missing_count > 0:
               issues.append({
                   "type": "missing_data",
                   "field": field,
                   "count": missing_count,
                   "affected_ids": missing_ids[:100],
                   "message": f"{missing_count} trips ({100-completion_rate:.1f}%) n'ont pas de {field} valide"
               })
               overall_missing_count += missing_count
       else:
           field_results[field] = {
               "present": False,
               "completion_rate": 0.0,
               "missing_count": total_trips,
               "average_length": 0,
               "unique_values": 0,
               "empty_strings": 0,
               "quality_level": "missing"
           }
           
           issues.append({
               "type": "missing_field",
               "field": field,
               "count": total_trips,
               "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
               "message": f"La colonne {field} est absente (champ optionnel)"
           })
   
   # Calcul de métriques globales
   present_fields = sum(1 for field in name_fields if field_results[field]["present"])
   avg_completion = round(sum(field_results[field]["completion_rate"] for field in name_fields) / len(name_fields), 2)
   
   # Détermination du statut global
   if present_fields == 0:
       status = "warning"  # Pas d'erreur car champs optionnels
   elif avg_completion >= 90:
       status = "success"
   elif avg_completion >= 50:
       status = "warning"
   else:
       status = "error"
   
   # Analyse de la qualité du nommage
   naming_analysis = {
       "fields_present": present_fields,
       "fields_total": len(name_fields),
       "avg_completion_rate": avg_completion,
       "total_missing_values": overall_missing_count,
       "naming_strategy": (
           "comprehensive" if present_fields == 2 and avg_completion >= 75
           else "partial" if present_fields >= 1 and avg_completion >= 50
           else "minimal"
       )
   }
   
   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_trips": total_trips,
           "field_analysis": field_results,
           "naming_analysis": naming_analysis,
           "quality_summary": {
               "best_field": max(name_fields, key=lambda x: field_results[x]["completion_rate"]) if present_fields > 0 else None,
               "worst_field": min(name_fields, key=lambda x: field_results[x]["completion_rate"]) if present_fields > 0 else None,
               "overall_quality": (
                   "excellent" if avg_completion >= 90
                   else "good" if avg_completion >= 50
                   else "poor"
               )
           }
       },
       "explanation": {
           "purpose": "Analyse la complétude des champs de nommage des trips pour améliorer l'information voyageur et la lisibilité des données",
           "field_descriptions": {
               "trip_short_name": "Nom court du voyage (ex: numéro de train)",
               "trip_long_name": "Nom descriptif du voyage (ex: destination complète)"
           },
           "analysis_summary": f"{present_fields}/{len(name_fields)} champs de nommage présents avec {avg_completion}% de complétude moyenne",
           "impact": (
               "Information de nommage complète pour une bonne expérience utilisateur" if status == "success"
               else f"Information de nommage incomplète : {overall_missing_count} valeurs manquantes au total"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Ajouter la colonne {field}" for field in name_fields if not field_results[field]["present"]
           ] + [
               f"Compléter les {field_results[field]['missing_count']} valeurs manquantes dans {field}" 
               for field in name_fields 
               if field_results[field]["present"] and field_results[field]["missing_count"] > 0
           ] + [
               "Standardiser le format des noms de trips pour améliorer la cohérence" if avg_completion > 0 and avg_completion < 90 else None,
               "Vérifier la pertinence des noms courts vs noms longs selon votre contexte métier" if present_fields == 2 else None,
               "Considérer l'utilisation de trip_headsign comme alternative pour l'affichage voyageur" if avg_completion < 50 else None
           ] if rec is not None
       ]
   }

@audit_function(
    file_type="trips",
    name="check_block_id_presence",
    genre="statistics",
    description="Vérifie la présence et complétude du champ block_id (optionnel).",
    parameters={}
)
def check_block_id_presence(gtfs_data, **params):
   """
   Analyse la présence et complétude du champ block_id dans trips.txt
   """
   df = gtfs_data.get('trips.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "trips.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier trips.txt est requis pour analyser les block_id"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Analyse la présence des block_id pour évaluer le groupement opérationnel des trips."
           },
           "recommendations": ["Fournir un fichier trips.txt valide."]
       }
   
   total_trips = len(df)
   
   # Vérification de la présence de la colonne
   if 'block_id' not in df.columns:
       return {
           "status": "warning",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "block_id",
                   "count": total_trips,
                   "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                   "message": "La colonne block_id est absente (champ optionnel pour le groupement de trips)"
               }
           ],
           "result": {
               "total_trips": total_trips,
               "trips_with_block": 0,
               "trips_without_block": total_trips,
               "completion_rate": 0.0,
               "unique_blocks": 0
           },
           "explanation": {
               "purpose": "Analyse la présence des block_id pour évaluer le groupement opérationnel des trips",
               "context": "Colonne block_id absente du fichier trips.txt",
               "impact": "Aucun groupement de trips possible - optimisation opérationnelle limitée"
           },
           "recommendations": [
               "Ajouter la colonne block_id si vous souhaitez grouper des trips consécutifs",
               "Utiliser block_id pour optimiser l'enchaînement des services et la planification véhicules"
           ]
       }
   
   # Analyse des block_id
   has_block_mask = df['block_id'].notna() & df['block_id'].ne('')
   trips_with_block = has_block_mask.sum()
   trips_without_block = total_trips - trips_with_block
   completion_rate = round(trips_with_block / total_trips * 100, 2) if total_trips > 0 else 0
   
   # Analyse de la distribution des blocks
   block_counts = df.loc[has_block_mask, 'block_id'].value_counts().to_dict()
   unique_blocks = len(block_counts)
   
   # IDs des trips sans block_id
   trips_without_block_ids = df.loc[~has_block_mask, 'trip_id'].tolist() if 'trip_id' in df.columns else []
   
   # Statistiques des blocks
   if unique_blocks > 0:
       block_sizes = list(block_counts.values())
       avg_trips_per_block = round(sum(block_sizes) / len(block_sizes), 2)
       max_block_size = max(block_sizes)
       min_block_size = min(block_sizes)
       single_trip_blocks = sum(1 for size in block_sizes if size == 1)
   else:
       avg_trips_per_block = 0
       max_block_size = 0
       min_block_size = 0
       single_trip_blocks = 0
   
   # Détermination du statut
   if completion_rate == 0:
       status = "warning"  # Pas d'erreur car optionnel
   elif completion_rate == 100:
       status = "success"
   elif completion_rate >= 75:
       status = "warning"
   else:
       status = "error"
   
   # Construction des issues
   issues = []
   if trips_without_block > 0:
       issues.append({
           "type": "missing_data",
           "field": "block_id",
           "count": trips_without_block,
           "affected_ids": trips_without_block_ids[:100],
           "message": f"{trips_without_block} trips ({100-completion_rate:.1f}%) n'ont pas de block_id défini"
       })
   
   # Issue pour blocks inefficaces (nombreux blocks à 1 trip)
   if single_trip_blocks > unique_blocks * 0.7 and unique_blocks > 10:
       issues.append({
           "type": "inefficient_data",
           "field": "block_id",
           "count": single_trip_blocks,
           "affected_ids": [bid for bid, count in block_counts.items() if count == 1][:50],
           "message": f"{single_trip_blocks} blocks ne contiennent qu'un seul trip - groupement sous-optimal"
       })
   
   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_trips": total_trips,
           "trips_with_block": int(trips_with_block),
           "trips_without_block": int(trips_without_block),
           "completion_rate": completion_rate,
           "block_analysis": {
               "unique_blocks": unique_blocks,
               "avg_trips_per_block": avg_trips_per_block,
               "max_block_size": max_block_size,
               "min_block_size": min_block_size,
               "single_trip_blocks": single_trip_blocks,
               "top_10_blocks": dict(list(block_counts.items())[:10])
           },
           "operational_efficiency": {
               "grouping_rate": completion_rate,
               "efficiency_score": max(0, 100 - (single_trip_blocks / max(unique_blocks, 1) * 100)) if unique_blocks > 0 else 0,
               "optimization_potential": (
                   "high" if completion_rate < 50
                   else "medium" if single_trip_blocks > unique_blocks * 0.5
                   else "low"
               )
           },
           "quality_level": (
               "excellent" if completion_rate == 100 and single_trip_blocks < unique_blocks * 0.3
               else "good" if completion_rate >= 75
               else "poor"
           )
       },
       "explanation": {
           "purpose": "Analyse la présence des block_id pour évaluer le groupement opérationnel des trips et l'optimisation des ressources",
           "context": f"Analyse de {total_trips} trips avec {unique_blocks} blocks définis",
           "block_concept": "Les block_id groupent des trips consécutifs utilisant le même véhicule pour optimiser l'exploitation",
           "efficiency_analysis": f"Taux de groupement: {completion_rate}% - {avg_trips_per_block:.1f} trips/block en moyenne",
           "impact": (
               f"Groupement opérationnel optimal avec tous les trips organisés en blocks" if completion_rate == 100
               else f"Potentiel d'optimisation : {trips_without_block} trips non groupés"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Définir des block_id pour {trips_without_block} trips non groupés" if trips_without_block > 0 else None,
               f"Optimiser {single_trip_blocks} blocks à trip unique en les regroupant si possible" if single_trip_blocks > unique_blocks * 0.3 else None,
               "Utiliser block_id pour planifier l'enchaînement optimal des services véhicules" if completion_rate < 100 else None,
               "Vérifier la cohérence temporelle et géographique des trips dans chaque block" if unique_blocks > 0 else None,
               "Exploiter les block_id pour optimiser la rotation du matériel roulant" if completion_rate >= 75 else None
           ] if rec is not None
       ]
   }

@audit_function(
    file_type="trips",
    name="validate_wheelchair_accessible",
    genre="accessibility",
    description="Vérifie que wheelchair_accessible contient uniquement 0, 1 ou 2 si présent.",
    parameters={}
)
def validate_wheelchair_accessible(gtfs_data, **params):
   """
   Valide les valeurs wheelchair_accessible dans trips.txt selon la spécification GTFS
   """
   df = gtfs_data.get('trips.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "trips.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier trips.txt est requis pour valider l'accessibilité fauteuil roulant"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Valide les valeurs wheelchair_accessible pour assurer la conformité GTFS et l'information d'accessibilité."
           },
           "recommendations": ["Fournir un fichier trips.txt valide."]
       }
   
   total_trips = len(df)
   
   # Vérification de la présence de la colonne
   if 'wheelchair_accessible' not in df.columns:
       return {
           "status": "warning",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "wheelchair_accessible",
                   "count": total_trips,
                   "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                   "message": "La colonne wheelchair_accessible est absente (champ optionnel mais important pour l'accessibilité)"
               }
           ],
           "result": {
               "total_trips": total_trips,
               "valid_values": 0,
               "invalid_values": 0,
               "missing_values": total_trips,
               "validation_rate": 0.0,
               "accessibility_coverage": 0.0
           },
           "explanation": {
               "purpose": "Valide les valeurs wheelchair_accessible pour assurer la conformité GTFS et l'information d'accessibilité",
               "context": "Colonne wheelchair_accessible absente du fichier trips.txt",
               "impact": "Aucune information d'accessibilité disponible pour les voyageurs à mobilité réduite"
           },
           "recommendations": [
               "Ajouter la colonne wheelchair_accessible avec les valeurs GTFS (0=inconnu, 1=accessible, 2=non accessible)",
               "Évaluer l'accessibilité de votre flotte pour renseigner cette information cruciale"
           ]
       }
   
   # Validation des valeurs selon GTFS
   valid_values = [0, 1, 2]  # 0=inconnu, 1=accessible, 2=non accessible
   
   # Analyse des valeurs
   valid_mask = df['wheelchair_accessible'].isin(valid_values)
   null_mask = df['wheelchair_accessible'].isna()
   
   valid_count = valid_mask.sum()
   null_count = null_mask.sum()
   invalid_count = total_trips - valid_count - null_count
   
   validation_rate = round(valid_count / total_trips * 100, 2) if total_trips > 0 else 0
   
   # IDs des trips problématiques
   invalid_ids = df.loc[~valid_mask & ~null_mask, 'trip_id'].tolist() if 'trip_id' in df.columns else []
   null_ids = df.loc[null_mask, 'trip_id'].tolist() if 'trip_id' in df.columns else []
   
   # Analyse de la distribution d'accessibilité
   accessibility_distribution = df.loc[valid_mask, 'wheelchair_accessible'].value_counts().to_dict()
   accessible_trips = accessibility_distribution.get(1, 0)
   non_accessible_trips = accessibility_distribution.get(2, 0)
   unknown_trips = accessibility_distribution.get(0, 0)
   
   accessibility_coverage = round(accessible_trips / total_trips * 100, 2) if total_trips > 0 else 0
   
   # Détermination du statut
   if invalid_count > 0:
       status = "error"
   elif null_count > total_trips * 0.1:  # Plus de 10% de valeurs nulles
       status = "warning"
   elif validation_rate == 100:
       status = "success"
   else:
       status = "warning"
   
   # Construction des issues
   issues = []
   
   if invalid_count > 0:
       invalid_values = df.loc[~valid_mask & ~null_mask, 'wheelchair_accessible'].unique()
       issues.append({
           "type": "invalid_format",
           "field": "wheelchair_accessible",
           "count": invalid_count,
           "affected_ids": invalid_ids[:100],
           "message": f"{invalid_count} trips ont des valeurs wheelchair_accessible invalides: {list(invalid_values)}"
       })
   
   if null_count > 0:
       issues.append({
           "type": "missing_data",
           "field": "wheelchair_accessible",
           "count": null_count,
           "affected_ids": null_ids[:100],
           "message": f"{null_count} trips ont des valeurs wheelchair_accessible manquantes"
       })
   
   # Alerte si trop de valeurs "inconnues" (0)
   if unknown_trips > total_trips * 0.5 and valid_count > 0:
       issues.append({
           "type": "insufficient_data",
           "field": "wheelchair_accessible",
           "count": unknown_trips,
           "affected_ids": df.loc[df['wheelchair_accessible'] == 0, 'trip_id'].tolist()[:100] if 'trip_id' in df.columns else [],
           "message": f"{unknown_trips} trips ont un statut d'accessibilité inconnu (valeur 0)"
       })
   
   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_trips": total_trips,
           "valid_values": int(valid_count),
           "invalid_values": int(invalid_count),
           "missing_values": int(null_count),
           "validation_rate": validation_rate,
           "accessibility_analysis": {
               "accessible_trips": int(accessible_trips),
               "non_accessible_trips": int(non_accessible_trips),
               "unknown_accessibility": int(unknown_trips),
               "accessibility_coverage": accessibility_coverage,
               "accessibility_distribution": {
                   "accessible": accessible_trips,
                   "non_accessible": non_accessible_trips,
                   "unknown": unknown_trips
               }
           },
           "compliance_level": {
               "data_quality": (
                   "excellent" if validation_rate == 100 and null_count == 0
                   else "good" if validation_rate >= 95
                   else "poor"
               ),
               "accessibility_info": (
                   "comprehensive" if unknown_trips < total_trips * 0.2
                   else "partial" if unknown_trips < total_trips * 0.5
                   else "insufficient"
               )
           }
       },
       "explanation": {
           "purpose": "Valide les valeurs wheelchair_accessible pour assurer la conformité GTFS et fournir une information d'accessibilité fiable",
           "specification": "Valeurs autorisées: 0=accessibilité inconnue, 1=accessible aux fauteuils roulants, 2=non accessible",
           "context": f"Analyse de {total_trips} trips avec {validation_rate}% de données valides",
           "accessibility_summary": f"Couverture accessibilité: {accessibility_coverage}% de trips accessibles déclarés",
           "impact": (
               f"Information d'accessibilité fiable pour {accessible_trips} trips accessibles" if status == "success"
               else f"Problèmes de données : {invalid_count} invalides, {null_count} manquantes, {unknown_trips} inconnues"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Corriger {invalid_count} valeurs wheelchair_accessible invalides (utiliser 0, 1 ou 2 uniquement)" if invalid_count > 0 else None,
               f"Renseigner {null_count} valeurs wheelchair_accessible manquantes" if null_count > 0 else None,
               f"Préciser le statut d'accessibilité de {unknown_trips} trips marqués comme 'inconnu' (0)" if unknown_trips > total_trips * 0.3 else None,
               "Effectuer un audit d'accessibilité de votre flotte pour améliorer la précision des données" if unknown_trips > total_trips * 0.5 else None,
               "Coordonner avec les équipes opérationnelles pour maintenir à jour les informations d'accessibilité" if accessible_trips > 0 else None,
               "Communiquer clairement sur l'accessibilité dans les applications voyageurs" if accessibility_coverage > 20 else None
           ] if rec is not None
       ]
   }


@audit_function(
    file_type="trips",
    name="check_shape_dist_traveled_format",
    genre="quality",
    description="Vérifie le format numérique (float) de 'shape_dist_traveled' si présent.",
    parameters={}
)
def check_shape_dist_traveled_format(gtfs_data, **params):
   """
   Valide le format des valeurs shape_dist_traveled dans trips.txt
   """
   df = gtfs_data.get('trips.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "trips.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier trips.txt est requis pour valider shape_dist_traveled"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Valide le format des valeurs shape_dist_traveled pour assurer la cohérence des distances parcourues."
           },
           "recommendations": ["Fournir un fichier trips.txt valide."]
       }
   
   total_trips = len(df)
   
   # Vérification de la présence de la colonne
   if 'shape_dist_traveled' not in df.columns:
       return {
           "status": "warning",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "shape_dist_traveled",
                   "count": total_trips,
                   "affected_ids": df['trip_id'].tolist() if 'trip_id' in df.columns else [],
                   "message": "La colonne shape_dist_traveled est absente (champ optionnel pour les distances cumulées)"
               }
           ],
           "result": {
               "total_trips": total_trips,
               "valid_values": 0,
               "invalid_values": 0,
               "missing_values": total_trips,
               "validation_rate": 0.0
           },
           "explanation": {
               "purpose": "Valide le format des valeurs shape_dist_traveled pour assurer la cohérence des distances parcourues",
               "context": "Colonne shape_dist_traveled absente du fichier trips.txt",
               "impact": "Aucune information de distance cumulée disponible pour le calcul des trajets"
           },
           "recommendations": [
               "Ajouter la colonne shape_dist_traveled si vous utilisez des formes géométriques détaillées",
               "Calculer les distances cumulées le long des shapes pour améliorer la précision"
           ]
       }
   
   # Validation du format des valeurs
   def is_valid_distance(value):
       """Vérifie si une valeur est un nombre positif ou égal à zéro"""
       if pd.isna(value):
           return True  # Les valeurs nulles sont acceptables
       try:
           num_value = float(value)
           return num_value >= 0
       except (ValueError, TypeError):
           return False
   
   # Analyse des valeurs
   valid_mask = df['shape_dist_traveled'].apply(is_valid_distance)
   null_mask = df['shape_dist_traveled'].isna()
   
   valid_count = valid_mask.sum()
   null_count = null_mask.sum()
   invalid_count = total_trips - valid_count
   
   validation_rate = round(valid_count / total_trips * 100, 2) if total_trips > 0 else 0
   
   # IDs des trips avec valeurs invalides
   invalid_rows = df.loc[~valid_mask]
   invalid_ids = invalid_rows['trip_id'].tolist() if 'trip_id' in df.columns else invalid_rows.index.tolist()
   
   # Analyse des valeurs valides (non nulles)
   valid_non_null = df.loc[valid_mask & ~null_mask, 'shape_dist_traveled']
   if len(valid_non_null) > 0:
       try:
           distances = pd.to_numeric(valid_non_null, errors='coerce')
           min_distance = float(distances.min())
           max_distance = float(distances.max())
           avg_distance = round(float(distances.mean()), 2)
           zero_distances = (distances == 0).sum()
       except:
           min_distance = max_distance = avg_distance = zero_distances = 0
   else:
       min_distance = max_distance = avg_distance = zero_distances = 0
   
   # Détection d'anomalies dans les valeurs valides
   anomalies = []
   if len(valid_non_null) > 0:
       # Valeurs négatives (déjà filtrées mais on vérifie)
       negative_count = sum(1 for v in valid_non_null if float(v) < 0)
       
       # Distances excessivement grandes (>1000km)
       excessive_distances = sum(1 for v in valid_non_null if float(v) > 1000000)  # >1000km en mètres
       
       if excessive_distances > 0:
           anomalies.append(f"{excessive_distances} distances > 1000km (possibles erreurs d'unité)")
   
   # Détermination du statut
   if invalid_count > 0:
       status = "error"
   elif null_count == total_trips:
       status = "warning"  # Toutes les valeurs sont nulles
   elif validation_rate == 100:
       status = "success"
   else:
       status = "warning"
   
   # Construction des issues
   issues = []
   
   if invalid_count > 0:
       # Analyse des types d'erreurs
       invalid_samples = []
       for _, row in invalid_rows.head(5).iterrows():
           invalid_samples.append(str(row['shape_dist_traveled']))
       
       issues.append({
           "type": "invalid_format",
           "field": "shape_dist_traveled",
           "count": invalid_count,
           "affected_ids": invalid_ids[:100],
           "message": f"{invalid_count} trips ont des valeurs shape_dist_traveled invalides (exemples: {', '.join(invalid_samples[:3])})"
       })
   
   if zero_distances > len(valid_non_null) * 0.5 and len(valid_non_null) > 10:
       issues.append({
           "type": "suspicious_data",
           "field": "shape_dist_traveled",
           "count": int(zero_distances),
           "affected_ids": df.loc[df['shape_dist_traveled'] == 0, 'trip_id'].tolist()[:50] if 'trip_id' in df.columns else [],
           "message": f"{zero_distances} trips ont une distance shape_dist_traveled de zéro (possibles données incomplètes)"
       })
   
   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_trips": total_trips,
           "valid_values": int(valid_count),
           "invalid_values": int(invalid_count),
           "missing_values": int(null_count),
           "validation_rate": validation_rate,
           "distance_analysis": {
               "trips_with_distance": int(len(valid_non_null)),
               "min_distance": min_distance,
               "max_distance": max_distance,
               "avg_distance": avg_distance,
               "zero_distances": int(zero_distances),
               "unit_assumed": "mètres (selon spécification GTFS)"
           },
           "data_quality": {
               "completeness": round((len(valid_non_null) / total_trips) * 100, 2),
               "anomalies_detected": len(anomalies),
               "quality_level": (
                   "excellent" if validation_rate == 100 and len(valid_non_null) > total_trips * 0.8
                   else "good" if validation_rate >= 95
                   else "poor"
               )
           }
       },
       "explanation": {
           "purpose": "Valide le format des valeurs shape_dist_traveled pour assurer la cohérence des distances parcourues le long des formes géométriques",
           "specification": "Les valeurs doivent être des nombres positifs ou égaux à zéro (en mètres) ou null si non spécifiées",
           "context": f"Analyse de {total_trips} trips avec {validation_rate}% de valeurs valides",
           "distance_summary": f"Distances valides: {min_distance}-{max_distance}m (moyenne: {avg_distance}m)" if len(valid_non_null) > 0 else "Aucune distance valide trouvée",
           "impact": (
               f"Données de distance cohérentes pour {len(valid_non_null)} trips" if status == "success"
               else f"Problèmes de format détectés : {invalid_count} valeurs invalides"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Corriger {invalid_count} valeurs shape_dist_traveled invalides (utiliser nombres positifs en mètres)" if invalid_count > 0 else None,
               f"Vérifier les {zero_distances} distances à zéro (début de parcours ou données manquantes?)" if zero_distances > len(valid_non_null) * 0.3 else None,
               "Convertir les unités en mètres si nécessaire (spécification GTFS)" if len(anomalies) > 0 else None,
               "Calculer automatiquement les distances cumulées à partir des coordonnées shapes" if len(valid_non_null) < total_trips * 0.5 else None,
               "Valider la cohérence avec les données de shapes.txt" if len(valid_non_null) > 0 and 'shapes' in gtfs_data else None,
               "Utiliser ces distances pour améliorer la précision des estimations de temps de parcours" if validation_rate >= 90 else None
           ] if rec is not None
       ]
   }

@audit_function(
    file_type="trips",
    name="duplicate_trips",
    genre='redondances',
    description="Détecte les trips strictement identiques (même route_id, service_id, shape_id, horaires, etc.).",
    parameters={}
)
def duplicate_trips(gtfs_data, **params):
   """
   Détecte les trips strictement dupliqués basés sur métadonnées et horaires identiques
   """
   print(gtfs_data)
   # Vérification des fichiers requis
   trips_df = gtfs_data.get('trips.txt')
   stop_times_df = gtfs_data.get('stop_times.txt')
   missing_files = []
   if trips_df is None:
       missing_files.append('trips.txt')
   if stop_times_df is None:
       missing_files.append('stop_times.txt')
   if missing_files:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "required_files",
                   "count": len(missing_files),
                   "affected_ids": [],
                   "message": f"Fichiers manquants requis pour la détection de doublons: {', '.join(missing_files)}"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Détecte les trips strictement identiques pour éliminer les redondances dans les données GTFS."
           },
           "recommendations": [f"Fournir les fichiers manquants: {', '.join(missing_files)}"]
       }
   total_trips = len(trips_df)

   # Champs clés pour la comparaison de trips
   key_fields = ['route_id', 'service_id', 'shape_id', 'trip_headsign', 'direction_id']
   # Groupement des trips par signature complète
   key_to_trip_ids = {}
   processing_errors = []

   for _, trip in trips_df.iterrows():
       try:
           # Signature métadonnées
           metadata_key = tuple(trip.get(f, None) for f in key_fields) 
           
           
           # Récupération des stop_times pour ce trip
           trip_stop_times = stop_times_df[stop_times_df['trip_id'] == trip['trip_id']]
           
           if len(trip_stop_times) == 0:
               processing_errors.append(trip['trip_id'])
               continue
               
           # Signature horaires (stops + horaires)
           trip_stop_times = trip_stop_times.sort_values('stop_sequence')
           schedule_signature = tuple(zip(
               trip_stop_times['stop_id'],
               trip_stop_times['arrival_time'],
               trip_stop_times['departure_time']
           ))
           
           # Signature complète
           full_signature = (metadata_key, schedule_signature)
           key_to_trip_ids.setdefault(full_signature, []).append(trip['trip_id'])
           
       except Exception as e:
           processing_errors.append(trip['trip_id'])

   # Identification des groupes de doublons
   duplicate_groups = [trip_ids for trip_ids in key_to_trip_ids.values() if len(trip_ids) > 1]
   duplicate_count = len(duplicate_groups)
   duplicated_trip_ids = [tid for group in duplicate_groups for tid in group]
   
   # Analyse détaillée des doublons
   duplicate_analysis = {}
   if duplicate_groups:
       group_sizes = [len(group) for group in duplicate_groups]
       duplicate_analysis = {
           "largest_group_size": max(group_sizes),
           "avg_group_size": round(sum(group_sizes) / len(group_sizes), 2),
           "total_redundant_trips": len(duplicated_trip_ids) - len(duplicate_groups),  # Trips en trop
           "groups_by_size": {size: group_sizes.count(size) for size in set(group_sizes)}
       }

   # Détermination du statut
   redundancy_rate = round(len(duplicated_trip_ids) / total_trips * 100, 2) if total_trips > 0 else 0
   print(redundancy_rate)
   if duplicate_count == 0:
       status = "success"
   elif redundancy_rate <= 5:
       status = "warning"
   else:
       status = "error"

   # Construction des issues
   issues = []
   
   if duplicate_count > 0:
       issues.append({
           "type": "duplicate_data",
           "field": "trip_signature",
           "count": duplicate_count,
           "affected_ids": duplicated_trip_ids[:100],
           "message": f"{duplicate_count} groupes de trips strictement dupliqués détectés ({len(duplicated_trip_ids)} trips concernés)"
       })

   if processing_errors:
       issues.append({
           "type": "processing_error",
           "field": "trip_analysis",
           "count": len(processing_errors),
           "affected_ids": processing_errors[:50],
           "message": f"{len(processing_errors)} trips n'ont pas pu être analysés (stop_times manquants ou erreurs)"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_trips": total_trips,
           "duplicate_groups": duplicate_count,
           "duplicated_trips": len(duplicated_trip_ids),
           "unique_trips": total_trips - len(duplicated_trip_ids) + duplicate_count,
           "redundancy_rate": redundancy_rate,
           "duplicate_analysis": duplicate_analysis,
           "processing_stats": {
               "successfully_analyzed": total_trips - len(processing_errors),
               "processing_errors": len(processing_errors),
               "analysis_coverage": round((total_trips - len(processing_errors)) / total_trips * 100, 2) if total_trips > 0 else 0
           },
           "efficiency_impact": {
               "redundant_trips": len(duplicated_trip_ids) - duplicate_count if duplicate_count > 0 else 0,
               "storage_waste": round(redundancy_rate, 2),
               "optimization_potential": (
                   "high" if redundancy_rate > 10
                   else "medium" if redundancy_rate > 5
                   else "low"
               )
           }
       },
       "explanation": {
           "purpose": "Détecte les trips strictement identiques (même route, service, horaires) pour éliminer les redondances et optimiser les données GTFS",
           "detection_method": "Comparaison par signature complète: métadonnées trip + séquence stop_times complète",
           "context": f"Analyse de {total_trips} trips avec détection de {duplicate_count} groupes de doublons",
           "redundancy_analysis": f"Taux de redondance: {redundancy_rate}% ({len(duplicated_trip_ids)} trips dupliqués)",
           "impact": (
               "Aucune redondance détectée - données optimisées" if duplicate_count == 0
               else f"Redondance détectée : {len(duplicated_trip_ids) - duplicate_count} trips supprimables"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Supprimer {len(duplicated_trip_ids) - duplicate_count} trips redondants pour optimiser les données" if duplicate_count > 0 else None,
               f"Examiner les {duplicate_count} groupes de doublons pour identifier la cause (erreur import, process?)" if duplicate_count > 0 else None,
               "Implémenter une validation de déduplication dans votre pipeline de génération GTFS" if redundancy_rate > 5 else None,
               f"Traiter les {len(processing_errors)} trips avec erreurs d'analyse (stop_times manquants)" if processing_errors else None,
               "Vérifier que les doublons ne correspondent pas à des besoins opérationnels légitimes" if duplicate_count > 0 else None,
               "Maintenir cette qualité de données sans redondance pour optimiser les performances" if duplicate_count == 0 else None
           ] if rec is not None
       ]
   }