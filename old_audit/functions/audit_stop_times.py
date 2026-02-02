"""
Fonctions d'audit pour le file_type: stop_times
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="stop_times",
    name="validate_required_structure",
    genre="validity",
    description="Vérifie la présence des colonnes obligatoires et la validité des types de données",
    parameters={}
)
def validate_required_structure(gtfs_data, **params):
    df, error = get_stop_times_or_error(gtfs_data)
    if error:
        return error
    
    # Colonnes obligatoires selon GTFS
    required_columns = {'trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'}
    
    total_stop_times = len(df)
    issues = []
    score = 100
    
    # 1. Vérification colonnes obligatoires
    missing_columns = list(required_columns - set(df.columns))
    if missing_columns:
        score -= 40
        issues.append(f"Colonnes obligatoires manquantes : {', '.join(missing_columns)}")
    
    # 2. Validation formats de temps
    time_columns = ['arrival_time', 'departure_time']
    converted_times, invalid_time_formats = convert_times_safely(df, time_columns)
    
    if invalid_time_formats:
        score -= 20
        for col, count in invalid_time_formats.items():
            issues.append(f"{count} formats de temps invalides dans {col}")
    
    # 3. Vérification valeurs vides dans champs obligatoires
    empty_required_fields = {}
    for col in required_columns:
        if col in df.columns:
            empty_count = df[col].isna().sum() + (df[col] == '').sum()
            if empty_count > 0:
                empty_required_fields[col] = int(empty_count)
                score -= min(15, empty_count // 100)  # Pénalité progressive
                issues.append(f"{empty_count} valeurs vides dans {col}")
    
    # 4. Validation types de données
    if 'stop_sequence' in df.columns:
        try:
            non_numeric_seq = pd.to_numeric(df['stop_sequence'], errors='coerce').isna().sum()
            if non_numeric_seq > 0:
                score -= 10
                issues.append(f"{non_numeric_seq} valeurs non-numériques dans stop_sequence")
        except Exception:
            score -= 10
            issues.append("Impossible de valider stop_sequence")
    
    # Calcul des IDs problématiques
    problematic_trip_ids = []
    if missing_columns or invalid_time_formats or empty_required_fields:
        # Identifier les trips avec des problèmes structurels
        if 'trip_id' in df.columns:
            mask = pd.Series([False] * len(df))
            
            for col in required_columns:
                if col in df.columns:
                    mask |= df[col].isna() | (df[col] == '')
            
            if 'arrival_time' in df.columns and 'arrival_time' in invalid_time_formats:
                mask |= converted_times['arrival_time'].isna() & df['arrival_time'].notna()
            
            if 'departure_time' in df.columns and 'departure_time' in invalid_time_formats:
                mask |= converted_times['departure_time'].isna() & df['departure_time'].notna()
            
            problematic_trip_ids = df[mask]['trip_id'].unique().tolist()
    
    # Explications
    explanation = {
        "structure_check": f"Structure validée sur {total_stop_times} enregistrements.",
        "required_columns": f"{len(required_columns - set(missing_columns))}/{len(required_columns)} colonnes obligatoires présentes.",
        "time_formats": "Formats de temps conformes." if not invalid_time_formats else f"Problèmes de format détectés dans {len(invalid_time_formats)} colonnes."
    }
    
    # Recommandations
    recommendations = []
    if missing_columns:
        recommendations.append(f"Ajouter les colonnes obligatoires : {', '.join(missing_columns)}")
    if invalid_time_formats:
        recommendations.append("Corriger les formats de temps (format attendu : HH:MM:SS)")
    if empty_required_fields:
        recommendations.append("Remplir tous les champs obligatoires")
    if not recommendations:
        recommendations.append("Structure conforme aux spécifications GTFS")
    
    return {
        "status": "ok" if not issues else "error",
        "issues": issues,
        "summary": {
            "total_stop_times": total_stop_times,
            "missing_required_columns": missing_columns,
            "invalid_time_formats": invalid_time_formats,
            "empty_required_fields": empty_required_fields,
            "problematic_trip_ids": problematic_trip_ids[:50],  # Limiter pour éviter surcharge
            "structure_validity_score": max(0, score)
        },
        "explanation": explanation,
        "recommendations": recommendations
    }


@audit_function(
    file_type="stop_times",
    name="validate_temporal_consistency",
    genre="validity",
    description="Vérifie la cohérence temporelle : arrival_time ≤ departure_time et ordre chronologique par trip",
    parameters={}
)
def validate_temporal_consistency(gtfs_data, **params):
    """
    Valide la cohérence temporelle dans stop_times.txt selon les règles GTFS.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide la cohérence temporelle entre arrival_time et departure_time dans stop_times.txt"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt avec les colonnes temporelles"]
        }
    
    total_stop_times = len(df)
    issues = []
    
    # Vérification présence colonnes temporelles
    required_time_columns = ['arrival_time', 'departure_time']
    missing_time_columns = [col for col in required_time_columns if col not in df.columns]
    
    if missing_time_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "time_columns",
                "count": len(missing_time_columns),
                "affected_ids": [],
                "message": f"Colonnes temporelles manquantes: {', '.join(missing_time_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide la cohérence temporelle entre arrival_time et departure_time dans stop_times.txt",
                "missing_columns": f"Impossible de valider sans: {', '.join(missing_time_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes manquantes: {', '.join(missing_time_columns)}"]
        }
    
    # Conversion des temps avec gestion d'erreurs robuste
    try:
        converted_times, invalid_formats = convert_times_safely(df, ['arrival_time', 'departure_time'])
        arrival_times = converted_times.get('arrival_time', pd.Series())
        departure_times = converted_times.get('departure_time', pd.Series())
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "conversion_error",
                "field": "time_conversion",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors de la conversion des temps: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide la cohérence temporelle entre arrival_time et departure_time dans stop_times.txt"
            },
            "recommendations": ["Vérifier les formats de temps dans arrival_time et departure_time"]
        }
    
    # 1. Validation arrival_time ≤ departure_time
    invalid_arrival_departure_mask = (arrival_times > departure_times) & arrival_times.notna() & departure_times.notna()
    invalid_arrival_departure_count = int(invalid_arrival_departure_mask.sum())
    
    if invalid_arrival_departure_count > 0:
        # Trouver les trip_ids concernés
        affected_trips = []
        if 'trip_id' in df.columns:
            affected_trips = df[invalid_arrival_departure_mask]['trip_id'].unique().tolist()[:100]
        
        issues.append({
            "type": "temporal_inconsistency",
            "field": "arrival_departure_order",
            "count": invalid_arrival_departure_count,
            "affected_ids": affected_trips,
            "message": f"Arrêts avec arrival_time > departure_time (violation GTFS)"
        })
    
    # 2. Validation ordre chronologique par trip
    non_monotonic_trips = []
    time_regression_details = []
    total_regressions = 0
    
    if 'trip_id' in df.columns:
        for trip_id, group in df.groupby('trip_id'):
            # Trier par stop_sequence si disponible
            if 'stop_sequence' in df.columns:
                try:
                    group = group.sort_values('stop_sequence')
                except:
                    pass  # Ignorer si problème de tri
            
            # Extraire les temps convertis pour ce trip
            trip_indices = group.index
            trip_arrivals = arrival_times.loc[trip_indices]
            trip_departures = departure_times.loc[trip_indices]
            
            # Détecter régressions temporelles dans arrival_time
            arrival_diff = trip_arrivals.diff()
            arrival_regressions = (arrival_diff < pd.Timedelta(0)).sum()
            
            # Détecter régressions temporelles dans departure_time
            departure_diff = trip_departures.diff()
            departure_regressions = (departure_diff < pd.Timedelta(0)).sum()
            
            trip_total_regressions = arrival_regressions + departure_regressions
            
            if trip_total_regressions > 0:
                non_monotonic_trips.append(trip_id)
                total_regressions += trip_total_regressions
                time_regression_details.append({
                    'trip_id': trip_id,
                    'arrival_regressions': arrival_regressions,
                    'departure_regressions': departure_regressions
                })
    
    if len(non_monotonic_trips) > 0:
        issues.append({
            "type": "non_monotonic_sequence",
            "field": "chronological_order",
            "count": len(non_monotonic_trips),
            "affected_ids": non_monotonic_trips[:100],
            "message": f"Trips avec ordre temporel non-monotone ({total_regressions} régressions détectées)"
        })
    
    # 3. Détection d'incohérences temporelles extrêmes
    extreme_durations = []
    if arrival_times.notna().any() and departure_times.notna().any():
        # Durées d'arrêt (departure - arrival)
        stop_durations = departure_times - arrival_times
        # Identifier durées > 2 heures (potentiellement problématiques)
        extreme_mask = stop_durations > pd.Timedelta(hours=2)
        extreme_count = extreme_mask.sum()
        
        if extreme_count > 0:
            affected_trips = []
            if 'trip_id' in df.columns:
                affected_trips = df[extreme_mask]['trip_id'].unique().tolist()[:100]
            
            issues.append({
                "type": "extreme_duration",
                "field": "stop_duration",
                "count": int(extreme_count),
                "affected_ids": affected_trips,
                "message": f"Arrêts avec durées extrêmes (>2h entre arrivée et départ)"
            })
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "conversion_error"]])
    temporal_violations = len([issue for issue in issues if issue["type"] == "temporal_inconsistency"])
    moderate_issues = len([issue for issue in issues if issue["type"] in ["non_monotonic_sequence", "extreme_duration"]])
    
    if critical_issues > 0:
        status = "error"
    elif temporal_violations > 0:
        status = "error"  # Violations GTFS sont critiques
    elif moderate_issues > 0:
        # Vérifier la proportion de problèmes
        total_problematic = sum(issue["count"] for issue in issues)
        if total_problematic > total_stop_times * 0.05:  # >5% problématique
            status = "error"
        else:
            status = "warning"
    else:
        status = "success"
    
    # Calcul des métriques de qualité
    arrival_departure_compliance = 100
    if total_stop_times > 0:
        arrival_departure_compliance = max(0, 100 - (invalid_arrival_departure_count / total_stop_times * 100))
    
    chronological_compliance = 100
    total_trips = len(df.groupby('trip_id')) if 'trip_id' in df.columns else 0
    if total_trips > 0:
        chronological_compliance = max(0, 100 - (len(non_monotonic_trips) / total_trips * 100))
    
    # Construction du result
    result = {
        "temporal_analysis": {
            "total_records": total_stop_times,
            "valid_arrival_departure_pairs": total_stop_times - invalid_arrival_departure_count,
            "arrival_departure_compliance_percent": round(arrival_departure_compliance, 1)
        },
        "chronological_analysis": {
            "total_trips_analyzed": total_trips,
            "monotonic_trips": total_trips - len(non_monotonic_trips),
            "chronological_compliance_percent": round(chronological_compliance, 1),
            "total_time_regressions": total_regressions
        },
        "issue_summary": {
            "temporal_violations": invalid_arrival_departure_count,
            "non_monotonic_trips": len(non_monotonic_trips),
            "extreme_durations": len([issue for issue in issues if issue["type"] == "extreme_duration"])
        }
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations urgentes pour violations temporelles
    if invalid_arrival_departure_count > 0:
        recommendations.append("URGENT: Corriger les arrêts où arrival_time > departure_time (violation spécification GTFS)")
    
    # Recommendations pour ordre chronologique
    if len(non_monotonic_trips) > 0:
        worst_trip = max(time_regression_details, key=lambda x: x['arrival_regressions'] + x['departure_regressions']) if time_regression_details else None
        if worst_trip:
            recommendations.append(f"Priorité: Réordonner les horaires du trip '{worst_trip['trip_id']}' et {len(non_monotonic_trips)-1} autres")
        else:
            recommendations.append("Réordonner les horaires pour assurer un ordre chronologique cohérent par trip")
    
    # Recommendations pour durées extrêmes
    if any(issue["type"] == "extreme_duration" for issue in issues):
        recommendations.append("Vérifier les durées d'arrêt extrêmes (>2h) qui peuvent indiquer des erreurs de saisie")
    
    # Recommendations pour optimisation
    if status == "success":
        recommendations.append("Cohérence temporelle parfaitement conforme aux spécifications GTFS")
    elif status == "warning":
        recommendations.append("Cohérence temporelle globalement bonne, quelques optimisations mineures possibles")
    
    # Recommendations pour amélioration des données
    if 'stop_sequence' not in df.columns:
        recommendations.append("Ajouter la colonne stop_sequence pour améliorer la validation de l'ordre chronologique")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Valide la cohérence temporelle entre arrival_time et departure_time dans stop_times.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements sur {total_trips} trips",
            "validation_rules": "arrival_time ≤ departure_time, ordre chronologique par trip, détection durées extrêmes",
            "gtfs_compliance": "Respect strict des règles temporelles GTFS"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="validate_sequence_integrity",
    genre="validity", 
    description="Vérifie que stop_sequence est croissant et continu pour chaque trip",
    parameters={}
)
def validate_sequence_integrity(gtfs_data, **params):
    """
    Valide l'intégrité des séquences stop_sequence dans stop_times.txt selon les règles GTFS.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide l'intégrité et la continuité des séquences stop_sequence par trip"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt avec les colonnes requises"]
        }
    
    issues = []
    
    # Vérification présence colonne stop_sequence
    if 'stop_sequence' not in df.columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "stop_sequence",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne stop_sequence manquante (obligatoire GTFS)"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide l'intégrité et la continuité des séquences stop_sequence par trip",
                "missing_requirement": "stop_sequence est obligatoire pour ordonner les arrêts"
            },
            "recommendations": ["URGENT: Ajouter la colonne stop_sequence avec des valeurs numériques croissantes"]
        }
    
    # Vérification présence colonne trip_id
    if 'trip_id' not in df.columns:
        return {
            "status": "error", 
            "issues": [{
                "type": "missing_field",
                "field": "trip_id",
                "count": 1,
                "affected_ids": [],
                "message": "Colonne trip_id manquante (obligatoire GTFS)"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide l'intégrité et la continuité des séquences stop_sequence par trip",
                "missing_requirement": "trip_id est nécessaire pour grouper les séquences par voyage"
            },
            "recommendations": ["URGENT: Ajouter la colonne trip_id pour identifier les voyages"]
        }
    
    # Initialisation des compteurs et listes
    trips_with_gaps = []
    trips_with_duplicates = []
    trips_with_non_monotonic = []
    trips_with_non_numeric = []
    problematic_details = []
    
    total_trips = df['trip_id'].nunique()
    total_records = len(df)
    
    # Analyse par trip
    for trip_id, group in df.groupby('trip_id'):
        sequences = group['stop_sequence'].dropna()
        trip_issues = []
        
        if len(sequences) == 0:
            continue
        
        # 1. Vérification conversion numérique
        try:
            numeric_sequences = pd.to_numeric(sequences, errors='coerce')
            non_numeric_count = numeric_sequences.isna().sum()
            
            if non_numeric_count > 0:
                trips_with_non_numeric.append(trip_id)
                trip_issues.append(f"{non_numeric_count} valeurs non-numériques")
                continue
                
            numeric_sequences = numeric_sequences.dropna()
            
        except Exception as e:
            trips_with_non_numeric.append(trip_id)
            trip_issues.append(f"Erreur conversion: {str(e)}")
            continue
        
        # 2. Vérification doublons
        unique_sequences = numeric_sequences.unique()
        if len(numeric_sequences) != len(unique_sequences):
            trips_with_duplicates.append(trip_id)
            duplicate_count = len(numeric_sequences) - len(unique_sequences)
            trip_issues.append(f"{duplicate_count} doublons")
        
        # 3. Vérification ordre croissant (monotonie)
        sequences_sorted = numeric_sequences.sort_values()
        if not numeric_sequences.equals(sequences_sorted):
            trips_with_non_monotonic.append(trip_id)
            # Compter les violations d'ordre
            violations = (numeric_sequences.diff() <= 0).sum()
            trip_issues.append(f"{violations} violations d'ordre")
        
        # 4. Vérification continuité (gaps)
        if len(unique_sequences) > 1:
            min_seq = int(min(unique_sequences))
            max_seq = int(max(unique_sequences))
            expected_count = max_seq - min_seq + 1
            actual_count = len(unique_sequences)
            
            if actual_count != expected_count:
                trips_with_gaps.append(trip_id)
                gap_count = expected_count - actual_count
                trip_issues.append(f"{gap_count} gaps dans la séquence")
        
        # Enregistrer les détails pour ce trip si problématique
        if trip_issues:
            problematic_details.append({
                'trip_id': trip_id,
                'issues': trip_issues,
                'sequence_count': len(sequences),
                'range': f"{min(numeric_sequences)}-{max(numeric_sequences)}" if len(numeric_sequences) > 0 else "N/A"
            })
    
    # Construction des issues
    if trips_with_non_numeric:
        issues.append({
            "type": "invalid_format",
            "field": "stop_sequence",
            "count": len(trips_with_non_numeric),
            "affected_ids": trips_with_non_numeric[:100],
            "message": "Trips avec valeurs stop_sequence non-numériques"
        })
    
    if trips_with_duplicates:
        issues.append({
            "type": "duplicate_data",
            "field": "stop_sequence",
            "count": len(trips_with_duplicates),
            "affected_ids": trips_with_duplicates[:100],
            "message": "Trips avec valeurs stop_sequence dupliquées"
        })
    
    if trips_with_non_monotonic:
        issues.append({
            "type": "sequence_disorder",
            "field": "stop_sequence",
            "count": len(trips_with_non_monotonic),
            "affected_ids": trips_with_non_monotonic[:100],
            "message": "Trips avec stop_sequence non-croissantes"
        })
    
    if trips_with_gaps:
        issues.append({
            "type": "sequence_gaps",
            "field": "stop_sequence",
            "count": len(trips_with_gaps),
            "affected_ids": trips_with_gaps[:100],
            "message": "Trips avec discontinuités dans stop_sequence"
        })
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "invalid_format"]])
    data_integrity_issues = len([issue for issue in issues if issue["type"] in ["duplicate_data", "sequence_disorder", "sequence_gaps"]])
    
    total_problematic_trips = len(set(trips_with_gaps + trips_with_duplicates + trips_with_non_monotonic + trips_with_non_numeric))
    
    if critical_issues > 0:
        status = "error"
    elif total_problematic_trips > total_trips * 0.1:  # >10% trips problématiques
        status = "error"
    elif data_integrity_issues > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul des métriques de qualité
    integrity_score = 100
    if total_trips > 0:
        problematic_ratio = total_problematic_trips / total_trips
        integrity_score = max(0, 100 - (problematic_ratio * 100))
    
    numeric_compliance = 100
    if total_trips > 0:
        numeric_compliance = max(0, 100 - (len(trips_with_non_numeric) / total_trips * 100))
    
    monotonic_compliance = 100
    if total_trips > 0:
        monotonic_compliance = max(0, 100 - (len(trips_with_non_monotonic) / total_trips * 100))
    
    # Construction du result
    result = {
        "sequence_analysis": {
            "total_trips": total_trips,
            "total_records": total_records,
            "integrity_score_percent": round(integrity_score, 1),
            "valid_trips": total_trips - total_problematic_trips
        },
        "compliance_metrics": {
            "numeric_compliance_percent": round(numeric_compliance, 1),
            "monotonic_compliance_percent": round(monotonic_compliance, 1),
            "continuity_compliance_percent": round(100 - (len(trips_with_gaps) / total_trips * 100) if total_trips > 0 else 100, 1),
            "uniqueness_compliance_percent": round(100 - (len(trips_with_duplicates) / total_trips * 100) if total_trips > 0 else 100, 1)
        },
        "issue_breakdown": {
            "non_numeric_sequences": len(trips_with_non_numeric),
            "duplicate_sequences": len(trips_with_duplicates),
            "non_monotonic_sequences": len(trips_with_non_monotonic),
            "sequence_gaps": len(trips_with_gaps)
        }
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations urgentes pour valeurs non-numériques
    if trips_with_non_numeric:
        recommendations.append("URGENT: Convertir toutes les valeurs stop_sequence en nombres entiers")
    
    # Recommendations pour doublons
    if trips_with_duplicates:
        worst_duplicate_trip = max(problematic_details, 
                                 key=lambda x: len([i for i in x['issues'] if 'doublons' in i]),
                                 default={'trip_id': trips_with_duplicates[0]})
        recommendations.append(f"Priorité: Éliminer les doublons stop_sequence dans le trip '{worst_duplicate_trip['trip_id']}' et {len(trips_with_duplicates)-1} autres")
    
    # Recommendations pour ordre
    if trips_with_non_monotonic:
        recommendations.append("Réordonner les stop_sequence en ordre strictement croissant (ex: 1,2,3,4...)")
    
    # Recommendations pour gaps
    if trips_with_gaps:
        recommendations.append("Combler les discontinuités dans stop_sequence pour assurer la continuité")
    
    # Recommendations pour amélioration globale
    if total_problematic_trips > total_trips * 0.05:  # >5%
        recommendations.append(f"Amélioration système: {total_problematic_trips} trips ({total_problematic_trips/total_trips*100:.1f}%) nécessitent une correction")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Intégrité des séquences parfaitement conforme aux spécifications GTFS")
    elif status == "warning":
        recommendations.append("Intégrité globalement bonne, quelques optimisations mineures possibles")
    
    # Recommendations préventives
    if status in ["success", "warning"]:
        recommendations.append("Maintenir la validation automatique des séquences lors des mises à jour")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Valide l'intégrité et la continuité des séquences stop_sequence par trip",
            "scope": f"Analyse de {total_records} enregistrements sur {total_trips} trips",
            "validation_rules": "Valeurs numériques, unicité, ordre croissant, continuité sans gaps",
            "gtfs_requirement": "stop_sequence doit définir l'ordre des arrêts de manière cohérente"
        },
        "recommendations": recommendations
    }


@audit_function(
    file_type="stop_times",
    name="validate_cross_references",
    genre="validity",
    description="Vérifie que trip_id et stop_id référencent des entités existantes dans trips.txt et stops.txt",
    parameters={} 
)
def validate_cross_references(gtfs_data, **params):
    """
    Valide l'intégrité référentielle entre stop_times.txt et les fichiers trips.txt/stops.txt.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide l'intégrité référentielle entre stop_times.txt et les fichiers de référence"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    
    # Vérification colonnes nécessaires
    required_columns = ['trip_id', 'stop_id']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "reference_columns",
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes de référence manquantes: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Valide l'intégrité référentielle entre stop_times.txt et les fichiers de référence",
                "missing_requirement": f"Colonnes obligatoires pour validation croisée: {', '.join(missing_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }
    
    # Variables de tracking
    orphaned_trip_ids = []
    orphaned_stop_ids = []
    invalid_trip_references = 0
    invalid_stop_references = 0
    missing_reference_files = []
    
    # 1. Validation trip_id avec trips.txt
    trips_df = gtfs_data.get('trips.txt')
    if trips_df is None or trips_df.empty:
        missing_reference_files.append('trips.txt')
        issues.append({
            "type": "missing_reference_file",
            "field": "trips.txt",
            "count": 1,
            "affected_ids": [],
            "message": "Fichier trips.txt manquant pour validation des trip_id"
        })
    elif 'trip_id' not in trips_df.columns:
        issues.append({
            "type": "missing_field",
            "field": "trip_id_in_trips",
            "count": 1,
            "affected_ids": [],
            "message": "Colonne trip_id manquante dans trips.txt"
        })
    else:
        # Validation effective des trip_id
        valid_trip_ids = set(trips_df['trip_id'].dropna().astype(str))
        stop_times_trip_ids = set(df['trip_id'].dropna().astype(str))
        orphaned_trip_ids = list(stop_times_trip_ids - valid_trip_ids)
        
        if orphaned_trip_ids:
            # Compter les enregistrements affectés
            invalid_trip_mask = df['trip_id'].astype(str).isin(orphaned_trip_ids)
            invalid_trip_references = int(invalid_trip_mask.sum())
            
            issues.append({
                "type": "invalid_reference",
                "field": "trip_id",
                "count": len(orphaned_trip_ids),
                "affected_ids": orphaned_trip_ids[:100],
                "message": f"trip_ids sans correspondance dans trips.txt ({invalid_trip_references} enregistrements affectés)"
            })
    
    # 2. Validation stop_id avec stops.txt
    stops_df = gtfs_data.get('stops.txt')
    if stops_df is None or stops_df.empty:
        missing_reference_files.append('stops.txt')
        issues.append({
            "type": "missing_reference_file",
            "field": "stops.txt",
            "count": 1,
            "affected_ids": [],
            "message": "Fichier stops.txt manquant pour validation des stop_id"
        })
    elif 'stop_id' not in stops_df.columns:
        issues.append({
            "type": "missing_field",
            "field": "stop_id_in_stops",
            "count": 1,
            "affected_ids": [],
            "message": "Colonne stop_id manquante dans stops.txt"
        })
    else:
        # Validation effective des stop_id
        valid_stop_ids = set(stops_df['stop_id'].dropna().astype(str))
        stop_times_stop_ids = set(df['stop_id'].dropna().astype(str))
        orphaned_stop_ids = list(stop_times_stop_ids - valid_stop_ids)
        
        if orphaned_stop_ids:
            # Compter les enregistrements affectés
            invalid_stop_mask = df['stop_id'].astype(str).isin(orphaned_stop_ids)
            invalid_stop_references = int(invalid_stop_mask.sum())
            
            issues.append({
                "type": "invalid_reference",
                "field": "stop_id",
                "count": len(orphaned_stop_ids),
                "affected_ids": orphaned_stop_ids[:100],
                "message": f"stop_ids sans correspondance dans stops.txt ({invalid_stop_references} enregistrements affectés)"
            })
    
    # 3. Détection des références nulles/vides
    null_trip_ids = df['trip_id'].isna().sum() + (df['trip_id'] == '').sum()
    null_stop_ids = df['stop_id'].isna().sum() + (df['stop_id'] == '').sum()
    
    if null_trip_ids > 0:
        issues.append({
            "type": "missing_value",
            "field": "trip_id",
            "count": int(null_trip_ids),
            "affected_ids": [],
            "message": f"Valeurs trip_id manquantes ou vides"
        })
    
    if null_stop_ids > 0:
        issues.append({
            "type": "missing_value",
            "field": "stop_id",
            "count": int(null_stop_ids),
            "affected_ids": [],
            "message": f"Valeurs stop_id manquantes ou vides"
        })
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "missing_reference_file"]])
    reference_violations = len([issue for issue in issues if issue["type"] == "invalid_reference"])
    data_quality_issues = len([issue for issue in issues if issue["type"] == "missing_value"])
    
    total_invalid_records = invalid_trip_references + invalid_stop_references + null_trip_ids + null_stop_ids
    
    if critical_issues > 0:
        status = "error"
    elif total_invalid_records > total_stop_times * 0.1:  # >10% des enregistrements problématiques
        status = "error"
    elif reference_violations > 0 or data_quality_issues > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul des métriques de qualité
    trip_reference_integrity = 100
    stop_reference_integrity = 100
    
    unique_trip_ids = len(df['trip_id'].dropna().unique()) if 'trip_id' in df.columns else 0
    unique_stop_ids = len(df['stop_id'].dropna().unique()) if 'stop_id' in df.columns else 0
    
    if unique_trip_ids > 0:
        trip_reference_integrity = max(0, 100 - (len(orphaned_trip_ids) / unique_trip_ids * 100))
    
    if unique_stop_ids > 0:
        stop_reference_integrity = max(0, 100 - (len(orphaned_stop_ids) / unique_stop_ids * 100))
    
    overall_integrity = (trip_reference_integrity + stop_reference_integrity) / 2
    
    # Construction du result
    result = {
        "reference_analysis": {
            "total_records": total_stop_times,
            "unique_trip_ids": unique_trip_ids,
            "unique_stop_ids": unique_stop_ids,
            "overall_integrity_percent": round(overall_integrity, 1)
        },
        "integrity_metrics": {
            "trip_reference_integrity_percent": round(trip_reference_integrity, 1),
            "stop_reference_integrity_percent": round(stop_reference_integrity, 1),
            "valid_trip_references": unique_trip_ids - len(orphaned_trip_ids),
            "valid_stop_references": unique_stop_ids - len(orphaned_stop_ids)
        },
        "validation_coverage": {
            "trips_file_available": trips_df is not None and not trips_df.empty,
            "stops_file_available": stops_df is not None and not stops_df.empty,
            "can_validate_trips": trips_df is not None and 'trip_id' in trips_df.columns,
            "can_validate_stops": stops_df is not None and 'stop_id' in stops_df.columns
        },
        "issue_details": {
            "orphaned_trip_ids_count": len(orphaned_trip_ids),
            "orphaned_stop_ids_count": len(orphaned_stop_ids),
            "invalid_trip_records": invalid_trip_references,
            "invalid_stop_records": invalid_stop_references,
            "null_references": null_trip_ids + null_stop_ids
        }
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations urgentes pour fichiers manquants
    if missing_reference_files:
        recommendations.append(f"URGENT: Fournir les fichiers de référence manquants: {', '.join(missing_reference_files)}")
    
    # Recommendations pour références orphelines (trip_id)
    if orphaned_trip_ids:
        if len(orphaned_trip_ids) <= 5:
            recommendations.append(f"Priorité: Corriger les trip_ids invalides: {', '.join(orphaned_trip_ids[:5])}")
        else:
            recommendations.append(f"Priorité: Corriger {len(orphaned_trip_ids)} trip_ids invalides (ex: {', '.join(orphaned_trip_ids[:3])}...)")
        
        # Suggestion d'action spécifique
        if invalid_trip_references < total_stop_times * 0.05:  # <5%
            recommendations.append("Option: Supprimer les enregistrements avec trip_ids invalides")
        else:
            recommendations.append("Option: Ajouter les trips manquants dans trips.txt")
    
    # Recommendations pour références orphelines (stop_id)
    if orphaned_stop_ids:
        if len(orphaned_stop_ids) <= 5:
            recommendations.append(f"Priorité: Corriger les stop_ids invalides: {', '.join(orphaned_stop_ids[:5])}")
        else:
            recommendations.append(f"Priorité: Corriger {len(orphaned_stop_ids)} stop_ids invalides (ex: {', '.join(orphaned_stop_ids[:3])}...)")
        
        # Suggestion d'action spécifique
        if invalid_stop_references < total_stop_times * 0.05:  # <5%
            recommendations.append("Option: Supprimer les enregistrements avec stop_ids invalides")
        else:
            recommendations.append("Option: Ajouter les arrêts manquants dans stops.txt")
    
    # Recommendations pour valeurs nulles
    if null_trip_ids > 0 or null_stop_ids > 0:
        recommendations.append("Compléter toutes les valeurs trip_id et stop_id manquantes")
    
    # Recommendations pour amélioration globale
    if total_invalid_records > total_stop_times * 0.02:  # >2%
        recommendations.append(f"Amélioration système: {total_invalid_records} enregistrements ({total_invalid_records/total_stop_times*100:.1f}%) ont des problèmes de référence")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Intégrité référentielle parfaitement conforme aux spécifications GTFS")
    elif status == "warning":
        recommendations.append("Intégrité référentielle globalement bonne, optimisations mineures possibles")
    
    # Recommendations préventives
    if status in ["success", "warning"]:
        recommendations.append("Maintenir la cohérence lors des ajouts/suppressions dans trips.txt et stops.txt")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Valide l'intégrité référentielle entre stop_times.txt et les fichiers de référence",
            "scope": f"Analyse de {total_stop_times} enregistrements avec {unique_trip_ids} trips uniques et {unique_stop_ids} arrêts uniques",
            "validation_targets": "Correspondance trip_id avec trips.txt, stop_id avec stops.txt",
            "gtfs_requirement": "Toutes les références doivent pointer vers des entités existantes"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="detect_data_duplicates",
    genre="quality",
    description="Détecte tous types de doublons dans stop_times : complets, trip+stop, trip+sequence",
    parameters={}
)
def detect_data_duplicates(gtfs_data, **params):
    """
    Détecte différents types de doublons dans stop_times.txt pour optimiser la qualité des données.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte et analyse différents types de doublons dans stop_times.txt"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    duplicate_trip_ids = set()
    duplicate_analyses = {}
    
    # 1. Doublons complets (toutes colonnes identiques)
    complete_duplicates_mask = df.duplicated(keep=False)
    complete_duplicates_count = int(complete_duplicates_mask.sum())
    
    if complete_duplicates_count > 0:
        # Identifier les trips concernés
        affected_trips = df[complete_duplicates_mask]['trip_id'].unique().tolist() if 'trip_id' in df.columns else []
        duplicate_trip_ids.update(affected_trips)
        
        # Analyser les groupes de doublons
        duplicate_groups = df[complete_duplicates_mask].groupby(df.columns.tolist()).size()
        max_group_size = duplicate_groups.max() if len(duplicate_groups) > 0 else 0
        
        duplicate_analyses['complete'] = {
            'total_duplicates': complete_duplicates_count,
            'unique_groups': len(duplicate_groups),
            'largest_group': int(max_group_size),
            'affected_trips': affected_trips[:100]
        }
        
        issues.append({
            "type": "complete_duplicate",
            "field": "all_columns",
            "count": complete_duplicates_count,
            "affected_ids": affected_trips[:100],
            "message": f"Enregistrements complètement identiques ({len(duplicate_groups)} groupes distincts)"
        })
    
    # 2. Doublons trip_id + stop_id (même arrêt visité plusieurs fois)
    trip_stop_duplicates_count = 0
    if 'trip_id' in df.columns and 'stop_id' in df.columns:
        trip_stop_duplicates_mask = df.duplicated(subset=['trip_id', 'stop_id'], keep=False)
        trip_stop_duplicates_count = int(trip_stop_duplicates_mask.sum())
        
        if trip_stop_duplicates_count > 0:
            affected_trips = df[trip_stop_duplicates_mask]['trip_id'].unique().tolist()
            duplicate_trip_ids.update(affected_trips)
            
            # Analyser les patterns de visite multiple
            multiple_visits = df[trip_stop_duplicates_mask].groupby(['trip_id', 'stop_id']).size()
            max_visits = multiple_visits.max() if len(multiple_visits) > 0 else 0
            
            duplicate_analyses['trip_stop'] = {
                'total_duplicates': trip_stop_duplicates_count,
                'unique_combinations': len(multiple_visits),
                'max_visits_same_stop': int(max_visits),
                'affected_trips': affected_trips[:100]
            }
            
            issues.append({
                "type": "trip_stop_duplicate",
                "field": "trip_stop_combination",
                "count": trip_stop_duplicates_count,
                "affected_ids": affected_trips[:100],
                "message": f"Trips visitant le même arrêt plusieurs fois ({len(multiple_visits)} combinaisons)"
            })
    
    # 3. Doublons trip_id + stop_sequence (même position dans le trip)
    trip_sequence_duplicates_count = 0
    if 'trip_id' in df.columns and 'stop_sequence' in df.columns:
        trip_sequence_duplicates_mask = df.duplicated(subset=['trip_id', 'stop_sequence'], keep=False)
        trip_sequence_duplicates_count = int(trip_sequence_duplicates_mask.sum())
        
        if trip_sequence_duplicates_count > 0:
            affected_trips = df[trip_sequence_duplicates_mask]['trip_id'].unique().tolist()
            duplicate_trip_ids.update(affected_trips)
            
            # Analyser les conflits de séquence
            sequence_conflicts = df[trip_sequence_duplicates_mask].groupby(['trip_id', 'stop_sequence']).size()
            max_conflicts = sequence_conflicts.max() if len(sequence_conflicts) > 0 else 0
            
            duplicate_analyses['trip_sequence'] = {
                'total_duplicates': trip_sequence_duplicates_count,
                'unique_conflicts': len(sequence_conflicts),
                'max_conflicts_same_position': int(max_conflicts),
                'affected_trips': affected_trips[:100]
            }
            
            issues.append({
                "type": "sequence_duplicate",
                "field": "trip_sequence_combination",
                "count": trip_sequence_duplicates_count,
                "affected_ids": affected_trips[:100],
                "message": f"Conflits de position stop_sequence dans les trips ({len(sequence_conflicts)} conflits)"
            })
    
    # 4. Doublons temporels (trip_id + horaires identiques)
    time_duplicates_count = 0
    if all(col in df.columns for col in ['trip_id', 'arrival_time', 'departure_time']):
        time_duplicates_mask = df.duplicated(subset=['trip_id', 'arrival_time', 'departure_time'], keep=False)
        time_duplicates_count = int(time_duplicates_mask.sum())
        
        if time_duplicates_count > 0:
            affected_trips = df[time_duplicates_mask]['trip_id'].unique().tolist()
            duplicate_trip_ids.update(affected_trips)
            
            # Analyser les patterns temporels
            time_patterns = df[time_duplicates_mask].groupby(['trip_id', 'arrival_time', 'departure_time']).size()
            max_same_time = time_patterns.max() if len(time_patterns) > 0 else 0
            
            duplicate_analyses['temporal'] = {
                'total_duplicates': time_duplicates_count,
                'unique_time_patterns': len(time_patterns),
                'max_same_schedule': int(max_same_time),
                'affected_trips': affected_trips[:100]
            }
            
            issues.append({
                "type": "temporal_duplicate",
                "field": "trip_time_combination",
                "count": time_duplicates_count,
                "affected_ids": affected_trips[:100],
                "message": f"Horaires identiques dans les trips ({len(time_patterns)} patterns temporels)"
            })
    
    # 5. Nouvelle détection: Doublons suspects basés sur proximité temporelle
    suspicious_temporal_count = 0
    if all(col in df.columns for col in ['trip_id', 'stop_id', 'arrival_time', 'departure_time']):
        try:
            # Convertir les temps pour analyse de proximité
            from datetime import datetime, timedelta
            import pandas as pd
            
            # Grouper par trip_id et stop_id pour trouver des horaires très proches
            suspicious_groups = []
            for (trip_id, stop_id), group in df.groupby(['trip_id', 'stop_id']):
                if len(group) > 1:
                    # Vérifier si les horaires sont très proches (< 1 minute)
                    times = pd.to_datetime(group['arrival_time'], format='%H:%M:%S', errors='coerce')
                    if times.notna().sum() > 1:
                        time_diffs = times.sort_values().diff().dt.total_seconds()
                        if (time_diffs < 60).any():  # Différence < 1 minute
                            suspicious_groups.append((trip_id, stop_id))
            
            if suspicious_groups:
                suspicious_temporal_count = len(suspicious_groups)
                affected_trips = [trip_id for trip_id, _ in suspicious_groups]
                
                issues.append({
                    "type": "suspicious_temporal",
                    "field": "temporal_proximity",
                    "count": suspicious_temporal_count,
                    "affected_ids": list(set(affected_trips))[:100],
                    "message": f"Horaires suspects (différence <1min pour même arrêt)"
                })
        except Exception:
            pass  # Ignorer si problème de conversion temporelle
    
    # Détermination du status
    total_duplicate_count = sum([
        complete_duplicates_count,
        trip_stop_duplicates_count,
        trip_sequence_duplicates_count,
        time_duplicates_count
    ])
    
    critical_duplicates = complete_duplicates_count + trip_sequence_duplicates_count
    moderate_duplicates = trip_stop_duplicates_count + time_duplicates_count
    
    duplicate_ratio = total_duplicate_count / total_stop_times if total_stop_times > 0 else 0
    
    if critical_duplicates > 0 and duplicate_ratio > 0.05:  # >5% duplicates critiques
        status = "error"
    elif total_duplicate_count > 0 and duplicate_ratio > 0.1:  # >10% duplicates au total
        status = "error"
    elif total_duplicate_count > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul des métriques de qualité
    data_uniqueness = max(0, 100 - (duplicate_ratio * 100))
    critical_integrity = max(0, 100 - (critical_duplicates / total_stop_times * 100)) if total_stop_times > 0 else 100
    
    # Construction du result
    result = {
        "duplicate_analysis": {
            "total_records": total_stop_times,
            "unique_records": total_stop_times - total_duplicate_count,
            "data_uniqueness_percent": round(data_uniqueness, 1),
            "critical_integrity_percent": round(critical_integrity, 1)
        },
        "duplicate_breakdown": {
            "complete_duplicates": complete_duplicates_count,
            "trip_stop_duplicates": trip_stop_duplicates_count,
            "sequence_duplicates": trip_sequence_duplicates_count,
            "temporal_duplicates": time_duplicates_count,
            "suspicious_temporal": suspicious_temporal_count
        },
        "impact_analysis": {
            "affected_trips_count": len(duplicate_trip_ids),
            "duplicate_ratio_percent": round(duplicate_ratio * 100, 2),
            "largest_duplicate_group": max([analysis.get('largest_group', 0) for analysis in duplicate_analyses.values()], default=0)
        },
        "detailed_analysis": duplicate_analyses
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations urgentes pour doublons critiques
    if complete_duplicates_count > 0:
        largest_group = duplicate_analyses['complete']['largest_group']
        recommendations.append(f"URGENT: Supprimer {complete_duplicates_count} doublons complets (groupe max: {largest_group} copies)")
    
    if trip_sequence_duplicates_count > 0:
        conflicts = duplicate_analyses['trip_sequence']['unique_conflicts']
        recommendations.append(f"URGENT: Résoudre {conflicts} conflits de stop_sequence (positions identiques)")
    
    # Recommendations pour doublons modérés
    if trip_stop_duplicates_count > 0:
        max_visits = duplicate_analyses['trip_stop'].get('max_visits_same_stop', 0)
        if max_visits > 3:
            recommendations.append(f"Vérifier les trips avec {max_visits} visites du même arrêt (possibles erreurs de saisie)")
        else:
            recommendations.append("Analyser la légitimité des visites multiples du même arrêt par trip")
    
    if time_duplicates_count > 0:
        time_patterns = duplicate_analyses['temporal']['unique_time_patterns']
        recommendations.append(f"Examiner {time_patterns} patterns d'horaires identiques (optimisation possible)")
    
    # Recommendations pour suspects temporels
    if suspicious_temporal_count > 0:
        recommendations.append(f"Vérifier {suspicious_temporal_count} horaires suspects avec différences <1min")
    
    # Recommendations pour optimisation
    if duplicate_ratio > 0.02:  # >2%
        recommendations.append(f"Optimisation données: {total_duplicate_count} doublons ({duplicate_ratio*100:.1f}%) à traiter pour améliorer la performance")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Excellente qualité des données - aucun doublon détecté")
    elif status == "warning" and duplicate_ratio < 0.01:
        recommendations.append("Qualité globalement excellente, doublons mineurs détectés")
    
    # Recommendations préventives
    if len(duplicate_trip_ids) > 0:
        recommendations.append(f"Audit ciblé recommandé sur {len(duplicate_trip_ids)} trips identifiés")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Détecte et analyse différents types de doublons dans stop_times.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements sur {len(duplicate_trip_ids)} trips avec doublons",
            "detection_types": "Doublons complets, trip-stop, séquence, temporels, proximité temporelle",
            "quality_impact": "Les doublons dégradent la performance et peuvent créer des incohérences"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="detect_temporal_anomalies", 
    genre="qualitéy",
    description="Détecte les anomalies temporelles : gaps extrêmes, durées nulles/négatives, temps > 48h",
    parameters={
        "extreme_gap_threshold_minutes": {"type": "number", "description": "Seuil gap extrême en minutes", "default": 120},
        "max_reasonable_hours": {"type": "number", "description": "Limite raisonnable en heures", "default": 48}
    }
)
def detect_temporal_anomalies(gtfs_data, extreme_gap_threshold_minutes=120, max_reasonable_hours=48, **params):
    """
    Détecte les anomalies temporelles dans stop_times.txt pour identifier les problèmes de qualité des horaires.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        extreme_gap_threshold_minutes: Seuil en minutes pour détecter les gaps extrêmes (défaut: 120)
        max_reasonable_hours: Limite raisonnable en heures pour les horaires (défaut: 48)
        **params: Paramètres additionnels
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les anomalies temporelles dans les horaires stop_times.txt"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt avec les colonnes temporelles"]
        }
    
    total_stop_times = len(df)
    issues = []
    
    # Vérification colonnes temporelles
    required_time_columns = ['arrival_time', 'departure_time']
    missing_time_columns = [col for col in required_time_columns if col not in df.columns]
    
    if missing_time_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "time_columns",
                "count": len(missing_time_columns),
                "affected_ids": [],
                "message": f"Colonnes temporelles manquantes: {', '.join(missing_time_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les anomalies temporelles dans les horaires stop_times.txt",
                "missing_requirement": f"Colonnes obligatoires pour analyse temporelle: {', '.join(missing_time_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes temporelles: {', '.join(missing_time_columns)}"]
        }
    
    # Variables de tracking
    anomalous_trip_ids = set()
    extreme_gap_details = []
    temporal_statistics = {}
    
    # Conversion des temps avec gestion d'erreurs robuste
    try:
        arrival_times = pd.to_timedelta(df['arrival_time'], errors='coerce')
        departure_times = pd.to_timedelta(df['departure_time'], errors='coerce')
        
        # Vérifier taux de conversion réussi
        arrival_conversion_rate = arrival_times.notna().sum() / len(df) if len(df) > 0 else 0
        departure_conversion_rate = departure_times.notna().sum() / len(df) if len(df) > 0 else 0
        
        temporal_statistics['conversion_rates'] = {
            'arrival_time_success_percent': round(arrival_conversion_rate * 100, 1),
            'departure_time_success_percent': round(departure_conversion_rate * 100, 1)
        }
        
        # Si taux de conversion trop faible, signaler comme problème
        if arrival_conversion_rate < 0.9 or departure_conversion_rate < 0.9:
            conversion_failures = len(df) - arrival_times.notna().sum() - departure_times.notna().sum()
            issues.append({
                "type": "conversion_error",
                "field": "time_format",
                "count": int(conversion_failures),
                "affected_ids": [],
                "message": f"Échec conversion temporelle ({(1-min(arrival_conversion_rate, departure_conversion_rate))*100:.1f}% d'erreurs)"
            })
            
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "conversion_error",
                "field": "time_parsing",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur critique de conversion temporelle: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les anomalies temporelles dans les horaires stop_times.txt"
            },
            "recommendations": ["Vérifier le format des colonnes temporelles (format GTFS: HH:MM:SS)"]
        }
    
    # 1. Détection durées négatives (arrival > departure)
    negative_duration_mask = (arrival_times > departure_times) & arrival_times.notna() & departure_times.notna()
    zero_negative_durations = int(negative_duration_mask.sum())
    
    if zero_negative_durations > 0:
        affected_trips = []
        if 'trip_id' in df.columns:
            affected_trips = df[negative_duration_mask]['trip_id'].unique().tolist()[:100]
            anomalous_trip_ids.update(affected_trips)
        
        issues.append({
            "type": "negative_duration",
            "field": "arrival_departure_order",
            "count": zero_negative_durations,
            "affected_ids": affected_trips,
            "message": "Durées négatives détectées (arrival_time > departure_time)"
        })
    
    # 2. Détection horaires extrêmes dépassant les limites raisonnables
    max_limit = pd.Timedelta(hours=max_reasonable_hours)
    over_limit_mask = (arrival_times > max_limit) | (departure_times > max_limit)
    times_over_limit = int(over_limit_mask.sum())
    
    if times_over_limit > 0:
        affected_trips = []
        if 'trip_id' in df.columns:
            affected_trips = df[over_limit_mask]['trip_id'].unique().tolist()[:100]
            anomalous_trip_ids.update(affected_trips)
        
        # Analyser les valeurs extrêmes
        max_arrival = arrival_times.max()
        max_departure = departure_times.max()
        extreme_hours = max(max_arrival.total_seconds(), max_departure.total_seconds()) / 3600 if pd.notna(max_arrival) and pd.notna(max_departure) else 0
        
        issues.append({
            "type": "extreme_time",
            "field": "time_limits",
            "count": times_over_limit,
            "affected_ids": affected_trips,
            "message": f"Horaires extrêmes dépassant {max_reasonable_hours}h (max détecté: {extreme_hours:.1f}h)"
        })
    
    # 3. Analyse des gaps extrêmes entre arrêts par trip
    extreme_gaps_count = 0
    temporal_inconsistencies = 0
    gap_statistics = {'max_gap_minutes': 0, 'avg_gap_minutes': 0, 'total_gaps_analyzed': 0}
    
    if 'trip_id' in df.columns:
        gap_threshold_seconds = extreme_gap_threshold_minutes * 60
        all_gaps = []
        
        for trip_id, group in df.groupby('trip_id'):
            if len(group) < 2:
                continue
                
            # Trier par stop_sequence si disponible
            if 'stop_sequence' in df.columns:
                try:
                    group = group.sort_values('stop_sequence')
                except:
                    pass  # Ignorer erreurs de tri
            
            group_arrivals = pd.to_timedelta(group['arrival_time'], errors='coerce')
            group_departures = pd.to_timedelta(group['departure_time'], errors='coerce')
            
            # Analyser gaps entre départ et arrivée suivante
            for i in range(len(group) - 1):
                try:
                    current_dep = group_departures.iloc[i]
                    next_arr = group_arrivals.iloc[i + 1]
                    
                    if pd.notna(current_dep) and pd.notna(next_arr):
                        gap_seconds = (next_arr - current_dep).total_seconds()
                        all_gaps.append(gap_seconds / 60)  # Convertir en minutes
                        
                        # Gap extrême
                        if gap_seconds > gap_threshold_seconds:
                            extreme_gaps_count += 1
                            anomalous_trip_ids.add(trip_id)
                            
                            extreme_gap_details.append({
                                "trip_id": trip_id,
                                "from_stop_sequence": group.iloc[i].get('stop_sequence', i),
                                "to_stop_sequence": group.iloc[i + 1].get('stop_sequence', i + 1),
                                "gap_minutes": round(gap_seconds / 60, 1)
                            })
                        
                        # Incohérence temporelle (départ après arrivée suivante)
                        elif gap_seconds < -60:  # Plus de 1 min de chevauchement
                            temporal_inconsistencies += 1
                            anomalous_trip_ids.add(trip_id)
                            
                except Exception:
                    continue
        
        # Calculer statistiques des gaps
        if all_gaps:
            gap_statistics = {
                'max_gap_minutes': round(max(all_gaps), 1),
                'avg_gap_minutes': round(sum(all_gaps) / len(all_gaps), 1),
                'total_gaps_analyzed': len(all_gaps)
            }
    
    if extreme_gaps_count > 0:
        # Trier par gap le plus important
        extreme_gap_details.sort(key=lambda x: x['gap_minutes'], reverse=True)
        worst_gap_trips = [detail['trip_id'] for detail in extreme_gap_details[:100]]
        
        issues.append({
            "type": "extreme_gap",
            "field": "inter_stop_timing",
            "count": extreme_gaps_count,
            "affected_ids": worst_gap_trips,
            "message": f"Gaps extrêmes entre arrêts (>{extreme_gap_threshold_minutes}min, max: {gap_statistics['max_gap_minutes']}min)"
        })
    
    if temporal_inconsistencies > 0:
        affected_trips = list(anomalous_trip_ids)[:100]
        issues.append({
            "type": "temporal_inconsistency",
            "field": "chronological_order",
            "count": temporal_inconsistencies,
            "affected_ids": affected_trips,
            "message": "Incohérences chronologiques entre arrêts consécutifs"
        })
    
    # 4. Nouvelle détection: Durées d'arrêt anormalement longues
    long_stop_durations = 0
    if arrival_times.notna().any() and departure_times.notna().any():
        stop_durations = departure_times - arrival_times
        long_duration_mask = stop_durations > pd.Timedelta(hours=1)  # >1h d'arrêt
        long_stop_durations = int(long_duration_mask.sum())
        
        if long_stop_durations > 0:
            affected_trips = []
            if 'trip_id' in df.columns:
                affected_trips = df[long_duration_mask]['trip_id'].unique().tolist()[:100]
            
            max_duration_hours = stop_durations.max().total_seconds() / 3600 if pd.notna(stop_durations.max()) else 0
            
            issues.append({
                "type": "excessive_stop_duration",
                "field": "stop_duration",
                "count": long_stop_durations,
                "affected_ids": affected_trips,
                "message": f"Durées d'arrêt excessives (>1h, max: {max_duration_hours:.1f}h)"
            })
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "conversion_error"]])
    temporal_violations = len([issue for issue in issues if issue["type"] in ["negative_duration", "temporal_inconsistency"]])
    quality_issues = len([issue for issue in issues if issue["type"] in ["extreme_time", "extreme_gap", "excessive_stop_duration"]])
    
    total_anomalies = sum(issue["count"] for issue in issues)
    anomaly_ratio = total_anomalies / total_stop_times if total_stop_times > 0 else 0
    
    if critical_issues > 0:
        status = "error"
    elif temporal_violations > 0 and anomaly_ratio > 0.05:  # >5% violations temporelles
        status = "error"
    elif total_anomalies > 0 and anomaly_ratio > 0.1:  # >10% anomalies totales
        status = "error"
    elif total_anomalies > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul des métriques de qualité
    temporal_integrity = max(0, 100 - (anomaly_ratio * 100))
    chronological_quality = 100
    if temporal_inconsistencies > 0:
        chronological_quality = max(0, 100 - (temporal_inconsistencies / total_stop_times * 100))
    
    # Construction du result
    result = {
        "temporal_analysis": {
            "total_records": total_stop_times,
            "valid_records": total_stop_times - total_anomalies,
            "temporal_integrity_percent": round(temporal_integrity, 1),
            "chronological_quality_percent": round(chronological_quality, 1)
        },
        "anomaly_breakdown": {
            "negative_durations": zero_negative_durations,
            "extreme_times": times_over_limit,
            "extreme_gaps": extreme_gaps_count,
            "temporal_inconsistencies": temporal_inconsistencies,
            "excessive_stop_durations": long_stop_durations
        },
        "gap_analysis": gap_statistics,
        "time_statistics": temporal_statistics,
        "impact_analysis": {
            "affected_trips_count": len(anomalous_trip_ids),
            "anomaly_ratio_percent": round(anomaly_ratio * 100, 2),
            "worst_anomalies": extreme_gap_details[:5]  # Top 5 pires gaps
        }
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations urgentes pour violations critiques
    if zero_negative_durations > 0:
        recommendations.append("URGENT: Corriger les durées négatives (arrival_time > departure_time)")
    
    if temporal_inconsistencies > 0:
        recommendations.append("URGENT: Résoudre les incohérences chronologiques entre arrêts")
    
    # Recommendations pour anomalies qualité
    if extreme_gaps_count > 0:
        worst_gap = max(extreme_gap_details, key=lambda x: x['gap_minutes']) if extreme_gap_details else None
        if worst_gap:
            recommendations.append(f"Priorité: Examiner le gap de {worst_gap['gap_minutes']}min dans le trip '{worst_gap['trip_id']}'")
        recommendations.append(f"Analyser {extreme_gaps_count} gaps extrêmes (seuil: {extreme_gap_threshold_minutes}min)")
    
    if times_over_limit > 0:
        recommendations.append(f"Réviser {times_over_limit} horaires dépassant {max_reasonable_hours}h (possibles erreurs de format)")
    
    if long_stop_durations > 0:
        recommendations.append(f"Vérifier {long_stop_durations} durées d'arrêt >1h (possibles terminus ou erreurs)")
    
    # Recommendations pour amélioration paramètres
    if gap_statistics.get('max_gap_minutes', 0) > extreme_gap_threshold_minutes * 2:
        recommendations.append(f"Considérer ajuster le seuil de détection (gap max détecté: {gap_statistics['max_gap_minutes']}min)")
    
    # Recommendations systémiques
    if anomaly_ratio > 0.02:  # >2%
        recommendations.append(f"Audit temporel recommandé: {total_anomalies} anomalies ({anomaly_ratio*100:.1f}%) détectées")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Excellente qualité temporelle - aucune anomalie significative détectée")
    elif status == "warning" and anomaly_ratio < 0.01:
        recommendations.append("Qualité temporelle globalement excellente, anomalies mineures présentes")
    
    # Recommendations préventives
    if len(anomalous_trip_ids) > 0:
        recommendations.append(f"Surveillance recommandée sur {len(anomalous_trip_ids)} trips identifiés avec anomalies")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Détecte les anomalies temporelles dans les horaires stop_times.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements sur {len(anomalous_trip_ids)} trips avec anomalies",
            "detection_criteria": f"Durées négatives, horaires >{max_reasonable_hours}h, gaps >{extreme_gap_threshold_minutes}min, durées arrêt >1h",
            "quality_impact": "Les anomalies temporelles affectent la fiabilité des calculs d'itinéraires"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="detect_unrealistic_patterns",
    genre="quality",
    description="Détecte les patterns irréalistes : vitesses impossibles, arrêts trop longs, patterns suspects",
    parameters={
        "max_speed_kmh": {"type": "number", "description": "Vitesse maximale réaliste km/h", "default": 200},
        "max_dwell_minutes": {"type": "number", "description": "Temps d'arrêt maximum en minutes", "default": 60}
    }
)
def detect_unrealistic_patterns(gtfs_data, max_speed_kmh=200, max_dwell_minutes=60, **params):
    """
    Détecte les patterns irréalistes dans stop_times.txt basés sur vitesses, temps d'arrêt et trajets.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        max_speed_kmh: Vitesse maximale raisonnable en km/h (défaut: 200)
        max_dwell_minutes: Temps d'arrêt maximal raisonnable en minutes (défaut: 60)
        **params: Paramètres additionnels
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    
    # Vérification colonnes temporelles
    required_time_columns = ['arrival_time', 'departure_time']
    missing_time_columns = [col for col in required_time_columns if col not in df.columns]
    
    if missing_time_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "time_columns",
                "count": len(missing_time_columns),
                "affected_ids": [],
                "message": f"Colonnes temporelles manquantes pour analyse patterns: {', '.join(missing_time_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt",
                "missing_requirement": f"Colonnes obligatoires pour analyse patterns: {', '.join(missing_time_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes temporelles: {', '.join(missing_time_columns)}"]
        }
    
    # Variables de tracking
    pattern_anomaly_trip_ids = set()
    dwell_time_analysis = {}
    speed_analysis = {}
    travel_time_analysis = {}
    
    # Conversion des temps avec gestion d'erreurs robuste
    try:
        arrival_times = pd.to_timedelta(df['arrival_time'], errors='coerce')
        departure_times = pd.to_timedelta(df['departure_time'], errors='coerce')
        
        # Vérifier taux de conversion
        conversion_success_rate = min(arrival_times.notna().sum(), departure_times.notna().sum()) / len(df) if len(df) > 0 else 0
        
        if conversion_success_rate < 0.9:  # <90% de succès
            issues.append({
                "type": "conversion_error",
                "field": "time_format",
                "count": int(len(df) * (1 - conversion_success_rate)),
                "affected_ids": [],
                "message": f"Échec conversion temporelle ({(1-conversion_success_rate)*100:.1f}% d'erreurs)"
            })
            
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "conversion_error",
                "field": "time_parsing",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur critique de conversion temporelle: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt"
            },
            "recommendations": ["Corriger les formats de temps (format GTFS: HH:MM:SS)"]
        }
    
    # 1. Analyse des temps d'arrêt (dwell times)
    dwell_times_seconds = (departure_times - arrival_times).dt.total_seconds()
    max_dwell_seconds = max_dwell_minutes * 60
    
    # Filtrer les durées valides
    valid_dwell_times = dwell_times_seconds[dwell_times_seconds.notna() & (dwell_times_seconds >= 0)]
    
    if len(valid_dwell_times) > 0:
        excessive_dwell_mask = (dwell_times_seconds > max_dwell_seconds) & dwell_times_seconds.notna()
        excessive_dwell_times = int(excessive_dwell_mask.sum())
        
        # Statistiques des temps d'arrêt
        dwell_time_analysis = {
            'total_analyzed': len(valid_dwell_times),
            'avg_dwell_seconds': round(valid_dwell_times.mean(), 1),
            'max_dwell_seconds': round(valid_dwell_times.max(), 1),
            'median_dwell_seconds': round(valid_dwell_times.median(), 1),
            'excessive_count': excessive_dwell_times
        }
        
        if excessive_dwell_times > 0:
            affected_trips = []
            dwell_details = []
            
            if 'trip_id' in df.columns:
                affected_trips = df[excessive_dwell_mask]['trip_id'].unique().tolist()[:100]
                pattern_anomaly_trip_ids.update(affected_trips)
                
                # Détails des pires cas
                excessive_data = df[excessive_dwell_mask].copy()
                excessive_data['dwell_minutes'] = dwell_times_seconds[excessive_dwell_mask] / 60
                excessive_data = excessive_data.nlargest(20, 'dwell_minutes')
                
                for _, row in excessive_data.iterrows():
                    dwell_details.append({
                        "trip_id": row['trip_id'],
                        "stop_id": row.get('stop_id', 'unknown'),
                        "stop_sequence": row.get('stop_sequence', 'unknown'),
                        "dwell_minutes": round(row['dwell_minutes'], 1)
                    })
            
            dwell_time_analysis['worst_cases'] = dwell_details
            
            issues.append({
                "type": "excessive_dwell_time",
                "field": "stop_duration",
                "count": excessive_dwell_times,
                "affected_ids": affected_trips,
                "message": f"Temps d'arrêt excessifs (>{max_dwell_minutes}min, max: {dwell_time_analysis['max_dwell_seconds']/60:.1f}min)"
            })
    
    # 2. Analyse des trajets et vitesses (si données géographiques disponibles)
    stops_data = gtfs_data.get('stops.txt')
    can_analyze_speeds = stops_data is not None and all(col in stops_data.columns for col in ['stop_id', 'stop_lat', 'stop_lon'])
    
    zero_travel_times = 0
    instantaneous_travels = 0
    unrealistic_speeds = []
    travel_patterns = {'total_segments': 0, 'analyzable_segments': 0}
    
    if 'trip_id' in df.columns:
        for trip_id, group in df.groupby('trip_id'):
            if len(group) < 2:
                continue
                
            # Trier par stop_sequence si disponible
            if 'stop_sequence' in df.columns:
                try:
                    group = group.sort_values('stop_sequence')
                except:
                    pass
            
            group_arrivals = pd.to_timedelta(group['arrival_time'], errors='coerce')
            group_departures = pd.to_timedelta(group['departure_time'], errors='coerce')
            
            # Analyser chaque segment de trajet
            for i in range(len(group) - 1):
                travel_patterns['total_segments'] += 1
                
                try:
                    current_dep = group_departures.iloc[i]
                    next_arr = group_arrivals.iloc[i + 1]
                    
                    if pd.notna(current_dep) and pd.notna(next_arr):
                        travel_time_seconds = (next_arr - current_dep).total_seconds()
                        travel_patterns['analyzable_segments'] += 1
                        
                        # Trajet instantané (temps de trajet = 0)
                        if travel_time_seconds == 0:
                            zero_travel_times += 1
                            pattern_anomaly_trip_ids.add(trip_id)
                        
                        # Trajet quasi-instantané (<30 secondes)
                        elif 0 < travel_time_seconds < 30:
                            instantaneous_travels += 1
                            pattern_anomaly_trip_ids.add(trip_id)
                        
                        # Analyse vitesse si données géographiques disponibles
                        elif travel_time_seconds > 0 and can_analyze_speeds:
                            current_stop_id = group.iloc[i]['stop_id']
                            next_stop_id = group.iloc[i + 1]['stop_id']
                            
                            current_stop = stops_data[stops_data['stop_id'] == current_stop_id]
                            next_stop = stops_data[stops_data['stop_id'] == next_stop_id]
                            
                            if not current_stop.empty and not next_stop.empty:
                                try:
                                    lat1 = float(current_stop.iloc[0]['stop_lat'])
                                    lon1 = float(current_stop.iloc[0]['stop_lon'])
                                    lat2 = float(next_stop.iloc[0]['stop_lat'])
                                    lon2 = float(next_stop.iloc[0]['stop_lon'])
                                    
                                    # Calcul distance haversine approximatif
                                    from math import radians, cos, sin, asin, sqrt
                                    
                                    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
                                    dlat = lat2 - lat1
                                    dlon = lon2 - lon1
                                    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                                    distance_km = 2 * asin(sqrt(a)) * 6371  # Rayon terre en km
                                    
                                    if distance_km > 0:
                                        speed_kmh = (distance_km / travel_time_seconds) * 3600
                                        
                                        if speed_kmh > max_speed_kmh:
                                            pattern_anomaly_trip_ids.add(trip_id)
                                            unrealistic_speeds.append({
                                                "trip_id": trip_id,
                                                "from_stop": current_stop_id,
                                                "to_stop": next_stop_id,
                                                "speed_kmh": round(speed_kmh, 1),
                                                "distance_km": round(distance_km, 2),
                                                "travel_time_minutes": round(travel_time_seconds / 60, 1)
                                            })
                                            
                                except (ValueError, TypeError, ZeroDivisionError):
                                    continue
                                
                except Exception:
                    continue
    
    # Construction des issues pour trajets
    if zero_travel_times > 0:
        issues.append({
            "type": "zero_travel_time",
            "field": "travel_duration",
            "count": zero_travel_times,
            "affected_ids": list(pattern_anomaly_trip_ids)[:100],
            "message": "Trajets instantanés détectés (temps de trajet = 0)"
        })
    
    if instantaneous_travels > 0:
        issues.append({
            "type": "instantaneous_travel",
            "field": "travel_duration",
            "count": instantaneous_travels,
            "affected_ids": list(pattern_anomaly_trip_ids)[:100],
            "message": "Trajets quasi-instantanés détectés (<30 secondes)"
        })
    
    if unrealistic_speeds:
        # Trier par vitesse décroissante
        unrealistic_speeds.sort(key=lambda x: x['speed_kmh'], reverse=True)
        worst_speed_trips = [speed['trip_id'] for speed in unrealistic_speeds[:100]]
        
        issues.append({
            "type": "unrealistic_speed",
            "field": "travel_speed",
            "count": len(unrealistic_speeds),
            "affected_ids": worst_speed_trips,
            "message": f"Vitesses irréalistes détectées (>{max_speed_kmh}km/h, max: {unrealistic_speeds[0]['speed_kmh']}km/h)"
        })
    
    # Statistiques des vitesses si analyse possible
    if can_analyze_speeds and unrealistic_speeds:
        speed_analysis = {
            'can_analyze_speeds': True,
            'unrealistic_speeds_count': len(unrealistic_speeds),
            'max_speed_detected': max(speed['speed_kmh'] for speed in unrealistic_speeds),
            'avg_unrealistic_speed': round(sum(speed['speed_kmh'] for speed in unrealistic_speeds) / len(unrealistic_speeds), 1),
            'worst_cases': unrealistic_speeds[:5]
        }
    else:
        speed_analysis = {
            'can_analyze_speeds': can_analyze_speeds,
            'reason': 'Données stops.txt manquantes ou incomplètes' if not can_analyze_speeds else 'Aucune vitesse irréaliste'
        }
    
    # Analyse des patterns de trajet
    travel_time_analysis = {
        **travel_patterns,
        'zero_travel_count': zero_travel_times,
        'instantaneous_count': instantaneous_travels,
        'coverage_percent': round((travel_patterns['analyzable_segments'] / max(travel_patterns['total_segments'], 1)) * 100, 1)
    }
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "conversion_error"]])
    pattern_violations = len([issue for issue in issues if issue["type"] in ["zero_travel_time", "unrealistic_speed"]])
    moderate_issues = len([issue for issue in issues if issue["type"] in ["excessive_dwell_time", "instantaneous_travel"]])
    
    total_pattern_anomalies = sum(issue["count"] for issue in issues if issue["type"] not in ["missing_file", "missing_field", "conversion_error"])
    anomaly_ratio = total_pattern_anomalies / max(travel_patterns['total_segments'], total_stop_times) if travel_patterns['total_segments'] > 0 else 0
    
    if critical_issues > 0:
        status = "error"
    elif pattern_violations > 0 and anomaly_ratio > 0.05:  # >5% violations critiques
        status = "error"
    elif total_pattern_anomalies > 0 and anomaly_ratio > 0.1:  # >10% anomalies totales
        status = "warning"
    elif total_pattern_anomalies > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul des métriques de qualité
    pattern_realism = max(0, 100 - (anomaly_ratio * 100))
    dwell_time_quality = 100
    if dwell_time_analysis.get('total_analyzed', 0) > 0:
        excessive_ratio = dwell_time_analysis['excessive_count'] / dwell_time_analysis['total_analyzed']
        dwell_time_quality = max(0, 100 - (excessive_ratio * 100))
    
    # Construction du result
    result = {
        "pattern_analysis": {
            "total_records": total_stop_times,
            "pattern_realism_percent": round(pattern_realism, 1),
            "affected_trips_count": len(pattern_anomaly_trip_ids),
            "analysis_coverage_percent": travel_time_analysis['coverage_percent']
        },
        "dwell_time_analysis": dwell_time_analysis,
        "travel_time_analysis": travel_time_analysis,
        "speed_analysis": speed_analysis,
        "quality_metrics": {
            "dwell_time_quality_percent": round(dwell_time_quality, 1),
            "realistic_patterns_percent": round(pattern_realism, 1),
            "total_anomalies": total_pattern_anomalies
        }
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations urgentes pour violations critiques
    if zero_travel_times > 0:
        recommendations.append(f"URGENT: Corriger {zero_travel_times} trajets instantanés (temps de trajet = 0)")
    
    if unrealistic_speeds:
        max_speed = max(speed['speed_kmh'] for speed in unrealistic_speeds) if unrealistic_speeds else 0
        worst_trip = unrealistic_speeds[0]['trip_id'] if unrealistic_speeds else None
        if worst_trip:
            recommendations.append(f"URGENT: Examiner le trip '{worst_trip}' avec vitesse de {max_speed:.1f}km/h")
        recommendations.append(f"Analyser {len(unrealistic_speeds)} vitesses >{max_speed_kmh}km/h (transport terrestre irréaliste)")
    
    # Recommendations pour patterns suspects
    if dwell_time_analysis.get('excessive_count', 0) > 0:
        worst_dwell = max(dwell_time_analysis.get('worst_cases', []), key=lambda x: x['dwell_minutes']) if dwell_time_analysis.get('worst_cases') else None
        if worst_dwell:
            recommendations.append(f"Priorité: Vérifier l'arrêt de {worst_dwell['dwell_minutes']}min dans le trip '{worst_dwell['trip_id']}'")
        recommendations.append(f"Réviser {dwell_time_analysis['excessive_count']} temps d'arrêt >{max_dwell_minutes}min")
    
    if instantaneous_travels > 0:
        recommendations.append(f"Examiner {instantaneous_travels} trajets quasi-instantanés (<30sec) - possibles erreurs de saisie")
    
    # Recommendations pour amélioration de l'analyse
    if not can_analyze_speeds:
        recommendations.append("Ajouter le fichier stops.txt avec coordonnées (stop_lat, stop_lon) pour analyse complète des vitesses")
    
    # Recommendations pour paramètres
    if unrealistic_speeds and max(speed['speed_kmh'] for speed in unrealistic_speeds) > max_speed_kmh * 2:
        recommendations.append(f"Considérer réviser le seuil de vitesse (détecté: {max(speed['speed_kmh'] for speed in unrealistic_speeds):.0f}km/h)")
    
    # Recommendations systémiques
    if anomaly_ratio > 0.02:  # >2%
        recommendations.append(f"Audit patterns recommandé: {total_pattern_anomalies} anomalies ({anomaly_ratio*100:.1f}%) nécessitent attention")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Excellents patterns de transport - comportements réalistes détectés")
    elif status == "warning" and anomaly_ratio < 0.01:
        recommendations.append("Patterns globalement réalistes, quelques optimisations mineures possibles")
    
    # Recommendations préventives
    if len(pattern_anomaly_trip_ids) > 0:
        recommendations.append(f"Surveillance recommandée sur {len(pattern_anomaly_trip_ids)} trips avec patterns suspects")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements, {travel_patterns['total_segments']} segments de trajet",
            "detection_criteria": f"Temps d'arrêt >{max_dwell_minutes}min, vitesses >{max_speed_kmh}km/h, trajets instantanés",
            "analysis_capabilities": "Vitesses analysées avec coordonnées stops" if can_analyze_speeds else "Analyse vitesses limitée (coordonnées manquantes)"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="analyze_data_completeness",
    genre="completeness",
    description="Analyse la complétude des champs optionnels : timepoint, stop_headsign, pickup_type, drop_off_type",
    parameters={}
)
def detect_unrealistic_patterns(gtfs_data, max_speed_kmh=200, max_dwell_minutes=60, **params):
    """
    Détecte les patterns irréalistes dans stop_times.txt basés sur vitesses, temps d'arrêt et trajets.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        max_speed_kmh: Vitesse maximale raisonnable en km/h (défaut: 200)
        max_dwell_minutes: Temps d'arrêt maximal raisonnable en minutes (défaut: 60)
        **params: Paramètres additionnels
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    
    # Vérification colonnes temporelles
    required_time_columns = ['arrival_time', 'departure_time']
    missing_time_columns = [col for col in required_time_columns if col not in df.columns]
    
    if missing_time_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "time_columns",
                "count": len(missing_time_columns),
                "affected_ids": [],
                "message": f"Colonnes temporelles manquantes pour analyse patterns: {', '.join(missing_time_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt",
                "missing_requirement": f"Colonnes obligatoires pour analyse patterns: {', '.join(missing_time_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes temporelles: {', '.join(missing_time_columns)}"]
        }
    
    # Variables de tracking
    pattern_anomaly_trip_ids = set()
    dwell_time_analysis = {}
    speed_analysis = {}
    travel_time_analysis = {}
    
    # Conversion des temps avec gestion d'erreurs robuste
    try:
        arrival_times = pd.to_timedelta(df['arrival_time'], errors='coerce')
        departure_times = pd.to_timedelta(df['departure_time'], errors='coerce')
        
        # Vérifier taux de conversion
        conversion_success_rate = min(arrival_times.notna().sum(), departure_times.notna().sum()) / len(df) if len(df) > 0 else 0
        
        if conversion_success_rate < 0.9:  # <90% de succès
            issues.append({
                "type": "conversion_error",
                "field": "time_format",
                "count": int(len(df) * (1 - conversion_success_rate)),
                "affected_ids": [],
                "message": f"Échec conversion temporelle ({(1-conversion_success_rate)*100:.1f}% d'erreurs)"
            })
            
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "conversion_error",
                "field": "time_parsing",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur critique de conversion temporelle: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt"
            },
            "recommendations": ["Corriger les formats de temps (format GTFS: HH:MM:SS)"]
        }
    
    # 1. Analyse des temps d'arrêt (dwell times)
    dwell_times_seconds = (departure_times - arrival_times).dt.total_seconds()
    max_dwell_seconds = max_dwell_minutes * 60
    
    # Filtrer les durées valides
    valid_dwell_times = dwell_times_seconds[dwell_times_seconds.notna() & (dwell_times_seconds >= 0)]
    
    if len(valid_dwell_times) > 0:
        excessive_dwell_mask = (dwell_times_seconds > max_dwell_seconds) & dwell_times_seconds.notna()
        excessive_dwell_times = int(excessive_dwell_mask.sum())
        
        # Statistiques des temps d'arrêt
        dwell_time_analysis = {
            'total_analyzed': len(valid_dwell_times),
            'avg_dwell_seconds': round(valid_dwell_times.mean(), 1),
            'max_dwell_seconds': round(valid_dwell_times.max(), 1),
            'median_dwell_seconds': round(valid_dwell_times.median(), 1),
            'excessive_count': excessive_dwell_times
        }
        
        if excessive_dwell_times > 0:
            affected_trips = []
            dwell_details = []
            
            if 'trip_id' in df.columns:
                affected_trips = df[excessive_dwell_mask]['trip_id'].unique().tolist()[:100]
                pattern_anomaly_trip_ids.update(affected_trips)
                
                # Détails des pires cas
                excessive_data = df[excessive_dwell_mask].copy()
                excessive_data['dwell_minutes'] = dwell_times_seconds[excessive_dwell_mask] / 60
                excessive_data = excessive_data.nlargest(20, 'dwell_minutes')
                
                for _, row in excessive_data.iterrows():
                    dwell_details.append({
                        "trip_id": row['trip_id'],
                        "stop_id": row.get('stop_id', 'unknown'),
                        "stop_sequence": row.get('stop_sequence', 'unknown'),
                        "dwell_minutes": round(row['dwell_minutes'], 1)
                    })
            
            dwell_time_analysis['worst_cases'] = dwell_details
            
            issues.append({
                "type": "excessive_dwell_time",
                "field": "stop_duration",
                "count": excessive_dwell_times,
                "affected_ids": affected_trips,
                "message": f"Temps d'arrêt excessifs (>{max_dwell_minutes}min, max: {dwell_time_analysis['max_dwell_seconds']/60:.1f}min)"
            })
    
    # 2. Analyse des trajets et vitesses (si données géographiques disponibles)
    stops_data = gtfs_data.get('stops.txt')
    can_analyze_speeds = stops_data is not None and all(col in stops_data.columns for col in ['stop_id', 'stop_lat', 'stop_lon'])
    
    zero_travel_times = 0
    instantaneous_travels = 0
    unrealistic_speeds = []
    travel_patterns = {'total_segments': 0, 'analyzable_segments': 0}
    
    if 'trip_id' in df.columns:
        for trip_id, group in df.groupby('trip_id'):
            if len(group) < 2:
                continue
                
            # Trier par stop_sequence si disponible
            if 'stop_sequence' in df.columns:
                try:
                    group = group.sort_values('stop_sequence')
                except:
                    pass
            
            group_arrivals = pd.to_timedelta(group['arrival_time'], errors='coerce')
            group_departures = pd.to_timedelta(group['departure_time'], errors='coerce')
            
            # Analyser chaque segment de trajet
            for i in range(len(group) - 1):
                travel_patterns['total_segments'] += 1
                
                try:
                    current_dep = group_departures.iloc[i]
                    next_arr = group_arrivals.iloc[i + 1]
                    
                    if pd.notna(current_dep) and pd.notna(next_arr):
                        travel_time_seconds = (next_arr - current_dep).total_seconds()
                        travel_patterns['analyzable_segments'] += 1
                        
                        # Trajet instantané (temps de trajet = 0)
                        if travel_time_seconds == 0:
                            zero_travel_times += 1
                            pattern_anomaly_trip_ids.add(trip_id)
                        
                        # Trajet quasi-instantané (<30 secondes)
                        elif 0 < travel_time_seconds < 30:
                            instantaneous_travels += 1
                            pattern_anomaly_trip_ids.add(trip_id)
                        
                        # Analyse vitesse si données géographiques disponibles
                        elif travel_time_seconds > 0 and can_analyze_speeds:
                            current_stop_id = group.iloc[i]['stop_id']
                            next_stop_id = group.iloc[i + 1]['stop_id']
                            
                            current_stop = stops_data[stops_data['stop_id'] == current_stop_id]
                            next_stop = stops_data[stops_data['stop_id'] == next_stop_id]
                            
                            if not current_stop.empty and not next_stop.empty:
                                try:
                                    lat1 = float(current_stop.iloc[0]['stop_lat'])
                                    lon1 = float(current_stop.iloc[0]['stop_lon'])
                                    lat2 = float(next_stop.iloc[0]['stop_lat'])
                                    lon2 = float(next_stop.iloc[0]['stop_lon'])
                                    
                                    # Calcul distance haversine approximatif
                                    from math import radians, cos, sin, asin, sqrt
                                    
                                    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
                                    dlat = lat2 - lat1
                                    dlon = lon2 - lon1
                                    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                                    distance_km = 2 * asin(sqrt(a)) * 6371  # Rayon terre en km
                                    
                                    if distance_km > 0:
                                        speed_kmh = (distance_km / travel_time_seconds) * 3600
                                        
                                        if speed_kmh > max_speed_kmh:
                                            pattern_anomaly_trip_ids.add(trip_id)
                                            unrealistic_speeds.append({
                                                "trip_id": trip_id,
                                                "from_stop": current_stop_id,
                                                "to_stop": next_stop_id,
                                                "speed_kmh": round(speed_kmh, 1),
                                                "distance_km": round(distance_km, 2),
                                                "travel_time_minutes": round(travel_time_seconds / 60, 1)
                                            })
                                            
                                except (ValueError, TypeError, ZeroDivisionError):
                                    continue
                                
                except Exception:
                    continue
    
    # Construction des issues pour trajets
    if zero_travel_times > 0:
        issues.append({
            "type": "zero_travel_time",
            "field": "travel_duration",
            "count": zero_travel_times,
            "affected_ids": list(pattern_anomaly_trip_ids)[:100],
            "message": "Trajets instantanés détectés (temps de trajet = 0)"
        })
    
    if instantaneous_travels > 0:
        issues.append({
            "type": "instantaneous_travel",
            "field": "travel_duration",
            "count": instantaneous_travels,
            "affected_ids": list(pattern_anomaly_trip_ids)[:100],
            "message": "Trajets quasi-instantanés détectés (<30 secondes)"
        })
    
    if unrealistic_speeds:
        # Trier par vitesse décroissante
        unrealistic_speeds.sort(key=lambda x: x['speed_kmh'], reverse=True)
        worst_speed_trips = [speed['trip_id'] for speed in unrealistic_speeds[:100]]
        
        issues.append({
            "type": "unrealistic_speed",
            "field": "travel_speed",
            "count": len(unrealistic_speeds),
            "affected_ids": worst_speed_trips,
            "message": f"Vitesses irréalistes détectées (>{max_speed_kmh}km/h, max: {unrealistic_speeds[0]['speed_kmh']}km/h)"
        })
    
    # Statistiques des vitesses si analyse possible
    if can_analyze_speeds and unrealistic_speeds:
        speed_analysis = {
            'can_analyze_speeds': True,
            'unrealistic_speeds_count': len(unrealistic_speeds),
            'max_speed_detected': max(speed['speed_kmh'] for speed in unrealistic_speeds),
            'avg_unrealistic_speed': round(sum(speed['speed_kmh'] for speed in unrealistic_speeds) / len(unrealistic_speeds), 1),
            'worst_cases': unrealistic_speeds[:5]
        }
    else:
        speed_analysis = {
            'can_analyze_speeds': can_analyze_speeds,
            'reason': 'Données stops.txt manquantes ou incomplètes' if not can_analyze_speeds else 'Aucune vitesse irréaliste'
        }
    
    # Analyse des patterns de trajet
    travel_time_analysis = {
        **travel_patterns,
        'zero_travel_count': zero_travel_times,
        'instantaneous_count': instantaneous_travels,
        'coverage_percent': round((travel_patterns['analyzable_segments'] / max(travel_patterns['total_segments'], 1)) * 100, 1)
    }
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "conversion_error"]])
    pattern_violations = len([issue for issue in issues if issue["type"] in ["zero_travel_time", "unrealistic_speed"]])
    moderate_issues = len([issue for issue in issues if issue["type"] in ["excessive_dwell_time", "instantaneous_travel"]])
    
    total_pattern_anomalies = sum(issue["count"] for issue in issues if issue["type"] not in ["missing_file", "missing_field", "conversion_error"])
    anomaly_ratio = total_pattern_anomalies / max(travel_patterns['total_segments'], total_stop_times) if travel_patterns['total_segments'] > 0 else 0
    
    if critical_issues > 0:
        status = "error"
    elif pattern_violations > 0 and anomaly_ratio > 0.05:  # >5% violations critiques
        status = "error"
    elif total_pattern_anomalies > 0 and anomaly_ratio > 0.1:  # >10% anomalies totales
        status = "warning"
    elif total_pattern_anomalies > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul des métriques de qualité
    pattern_realism = max(0, 100 - (anomaly_ratio * 100))
    dwell_time_quality = 100
    if dwell_time_analysis.get('total_analyzed', 0) > 0:
        excessive_ratio = dwell_time_analysis['excessive_count'] / dwell_time_analysis['total_analyzed']
        dwell_time_quality = max(0, 100 - (excessive_ratio * 100))
    
    # Construction du result
    result = {
        "pattern_analysis": {
            "total_records": total_stop_times,
            "pattern_realism_percent": round(pattern_realism, 1),
            "affected_trips_count": len(pattern_anomaly_trip_ids),
            "analysis_coverage_percent": travel_time_analysis['coverage_percent']
        },
        "dwell_time_analysis": dwell_time_analysis,
        "travel_time_analysis": travel_time_analysis,
        "speed_analysis": speed_analysis,
        "quality_metrics": {
            "dwell_time_quality_percent": round(dwell_time_quality, 1),
            "realistic_patterns_percent": round(pattern_realism, 1),
            "total_anomalies": total_pattern_anomalies
        }
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations urgentes pour violations critiques
    if zero_travel_times > 0:
        recommendations.append(f"URGENT: Corriger {zero_travel_times} trajets instantanés (temps de trajet = 0)")
    
    if unrealistic_speeds:
        max_speed = max(speed['speed_kmh'] for speed in unrealistic_speeds) if unrealistic_speeds else 0
        worst_trip = unrealistic_speeds[0]['trip_id'] if unrealistic_speeds else None
        if worst_trip:
            recommendations.append(f"URGENT: Examiner le trip '{worst_trip}' avec vitesse de {max_speed:.1f}km/h")
        recommendations.append(f"Analyser {len(unrealistic_speeds)} vitesses >{max_speed_kmh}km/h (transport terrestre irréaliste)")
    
    # Recommendations pour patterns suspects
    if dwell_time_analysis.get('excessive_count', 0) > 0:
        worst_dwell = max(dwell_time_analysis.get('worst_cases', []), key=lambda x: x['dwell_minutes']) if dwell_time_analysis.get('worst_cases') else None
        if worst_dwell:
            recommendations.append(f"Priorité: Vérifier l'arrêt de {worst_dwell['dwell_minutes']}min dans le trip '{worst_dwell['trip_id']}'")
        recommendations.append(f"Réviser {dwell_time_analysis['excessive_count']} temps d'arrêt >{max_dwell_minutes}min")
    
    if instantaneous_travels > 0:
        recommendations.append(f"Examiner {instantaneous_travels} trajets quasi-instantanés (<30sec) - possibles erreurs de saisie")
    
    # Recommendations pour amélioration de l'analyse
    if not can_analyze_speeds:
        recommendations.append("Ajouter le fichier stops.txt avec coordonnées (stop_lat, stop_lon) pour analyse complète des vitesses")
    
    # Recommendations pour paramètres
    if unrealistic_speeds and max(speed['speed_kmh'] for speed in unrealistic_speeds) > max_speed_kmh * 2:
        recommendations.append(f"Considérer réviser le seuil de vitesse (détecté: {max(speed['speed_kmh'] for speed in unrealistic_speeds):.0f}km/h)")
    
    # Recommendations systémiques
    if anomaly_ratio > 0.02:  # >2%
        recommendations.append(f"Audit patterns recommandé: {total_pattern_anomalies} anomalies ({anomaly_ratio*100:.1f}%) nécessitent attention")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Excellents patterns de transport - comportements réalistes détectés")
    elif status == "warning" and anomaly_ratio < 0.01:
        recommendations.append("Patterns globalement réalistes, quelques optimisations mineures possibles")
    
    # Recommendations préventives
    if len(pattern_anomaly_trip_ids) > 0:
        recommendations.append(f"Surveillance recommandée sur {len(pattern_anomaly_trip_ids)} trips avec patterns suspects")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Détecte les patterns de transport irréalistes dans stop_times.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements, {travel_patterns['total_segments']} segments de trajet",
            "detection_criteria": f"Temps d'arrêt >{max_dwell_minutes}min, vitesses >{max_speed_kmh}km/h, trajets instantanés",
            "analysis_capabilities": "Vitesses analysées avec coordonnées stops" if can_analyze_speeds else "Analyse vitesses limitée (coordonnées manquantes)"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="analyze_temporal_coverage",
    genre="completeness",
    description="Analyse la couverture temporelle : plages horaires, service de nuit, répartition des services",
    parameters={}
)
def analyze_temporal_coverage(gtfs_data, **params):
    """
    Analyse la couverture temporelle des services dans stop_times.txt pour évaluer l'étendue horaire.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyse la couverture temporelle des services de transport"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    
    # Vérification colonnes temporelles
    required_time_columns = ['arrival_time', 'departure_time']
    missing_time_columns = [col for col in required_time_columns if col not in df.columns]
    
    if missing_time_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "time_columns",
                "count": len(missing_time_columns),
                "affected_ids": [],
                "message": f"Colonnes temporelles manquantes: {', '.join(missing_time_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyse la couverture temporelle des services de transport",
                "missing_requirement": f"Colonnes obligatoires pour analyse temporelle: {', '.join(missing_time_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes temporelles: {', '.join(missing_time_columns)}"]
        }
    
    # Variables de tracking
    low_service_trip_ids = set()
    temporal_statistics = {}
    service_quality_metrics = {}
    
    # Conversion des temps avec gestion d'erreurs robuste
    try:
        arrival_times = pd.to_timedelta(df['arrival_time'], errors='coerce')
        departure_times = pd.to_timedelta(df['departure_time'], errors='coerce')
        
        # Vérifier taux de conversion
        valid_arrivals = arrival_times.notna().sum()
        valid_departures = departure_times.notna().sum()
        conversion_rate = min(valid_arrivals, valid_departures) / total_stop_times if total_stop_times > 0 else 0
        
        if conversion_rate < 0.9:  # <90% de conversion réussie
            issues.append({
                "type": "conversion_error",
                "field": "time_format",
                "count": int(total_stop_times * (1 - conversion_rate)),
                "affected_ids": [],
                "message": f"Échec conversion temporelle ({(1-conversion_rate)*100:.1f}% d'erreurs)"
            })
        
        # Combiner tous les temps valides pour analyse
        all_times = pd.concat([arrival_times, departure_times]).dropna()
        
        if len(all_times) == 0:
            return {
                "status": "error",
                "issues": [{
                    "type": "no_valid_times",
                    "field": "temporal_data",
                    "count": total_stop_times,
                    "affected_ids": [],
                    "message": "Aucun temps valide détecté pour analyse"
                }],
                "result": {},
                "explanation": {
                    "purpose": "Analyse la couverture temporelle des services de transport"
                },
                "recommendations": ["Corriger les formats de temps (format GTFS: HH:MM:SS)"]
            }
            
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "conversion_error",
                "field": "time_parsing",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur critique de conversion temporelle: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyse la couverture temporelle des services de transport"
            },
            "recommendations": ["Vérifier le format des colonnes temporelles (format GTFS: HH:MM:SS)"]
        }
    
    # 1. Analyse de la plage de service globale
    hours = (all_times.dt.total_seconds() // 3600).astype(int)
    earliest_hour = int(hours.min()) if len(hours) > 0 else 0
    latest_hour = int(hours.max()) if len(hours) > 0 else 0
    
    # Gérer les services qui traversent minuit
    crosses_midnight = latest_hour >= 24 or earliest_hour < 0
    if crosses_midnight:
        # Normaliser les heures >24 et <0
        normalized_hours = hours % 24
        service_span_hours = 24  # Service 24h si traverse minuit
    else:
        normalized_hours = hours
        service_span_hours = max(0, latest_hour - earliest_hour)
    
    service_time_span = {
        "earliest_hour": earliest_hour,
        "latest_hour": latest_hour,
        "earliest_service_formatted": f"{earliest_hour:02d}:00",
        "latest_service_formatted": f"{latest_hour:02d}:00",
        "total_service_hours": service_span_hours,
        "crosses_midnight": crosses_midnight,
        "service_type": "24h" if service_span_hours >= 20 else "étendu" if service_span_hours >= 16 else "standard" if service_span_hours >= 12 else "limité"
    }
    
    # Issue pour service temporellement limité
    if service_span_hours < 12:
        issues.append({
            "type": "limited_service_hours",
            "field": "temporal_coverage",
            "count": 1,
            "affected_ids": [],
            "message": f"Service limité à {service_span_hours}h par jour (recommandé: >12h)"
        })
    
    # 2. Distribution horaire détaillée
    hourly_counts = normalized_hours.value_counts().sort_index()
    
    # Classification par tranches horaires standard
    time_periods = {
        "early_morning": (5, 7),    # 5h-7h
        "morning_peak": (7, 10),    # 7h-10h
        "midday": (10, 16),         # 10h-16h
        "evening_peak": (16, 19),   # 16h-19h
        "evening": (19, 22),        # 19h-22h
        "night": [(22, 24), (0, 5)] # 22h-24h + 0h-5h
    }
    
    hourly_distribution = {}
    total_valid_times = len(all_times)
    
    for period, time_range in time_periods.items():
        if period == "night":
            # Période nocturne spans minuit
            count = sum(normalized_hours[(normalized_hours >= r[0]) & (normalized_hours < r[1])].count() 
                       for r in time_range)
        else:
            start, end = time_range
            count = normalized_hours[(normalized_hours >= start) & (normalized_hours < end)].count()
        
        hourly_distribution[period] = {
            "count": int(count),
            "percentage": round((count / total_valid_times) * 100, 1) if total_valid_times > 0 else 0
        }
    
    # Calcul ratios importants
    peak_total = hourly_distribution["morning_peak"]["count"] + hourly_distribution["evening_peak"]["count"]
    peak_ratio = round((peak_total / total_valid_times) * 100, 1) if total_valid_times > 0 else 0
    
    # 3. Détection des gaps de service significatifs
    service_gaps = []
    if len(hourly_counts) > 1:
        # Créer la plage complète d'heures de service
        full_range = list(range(earliest_hour, latest_hour + 1))
        if crosses_midnight:
            # Pour service 24h, analyser sur 24h
            full_range = list(range(24))
        
        missing_hours = [h % 24 for h in full_range if (h % 24) not in hourly_counts.index]
        
        # Grouper les heures consécutives manquantes en gaps
        if missing_hours:
            missing_hours.sort()
            current_gap = [missing_hours[0]]
            
            for i in range(1, len(missing_hours)):
                # Gérer la continuité à travers minuit
                prev_hour = missing_hours[i-1]
                curr_hour = missing_hours[i]
                
                if curr_hour == (prev_hour + 1) % 24:
                    current_gap.append(curr_hour)
                else:
                    # Fin du gap actuel si significatif (≥2h)
                    if len(current_gap) >= 2:
                        service_gaps.append({
                            "start_hour": current_gap[0],
                            "end_hour": current_gap[-1],
                            "duration_hours": len(current_gap),
                            "gap_type": "nocturne" if all(h >= 22 or h <= 5 for h in current_gap) else "diurne"
                        })
                    current_gap = [curr_hour]
            
            # Traiter le dernier gap
            if len(current_gap) >= 2:
                service_gaps.append({
                    "start_hour": current_gap[0],
                    "end_hour": current_gap[-1],
                    "duration_hours": len(current_gap),
                    "gap_type": "nocturne" if all(h >= 22 or h <= 5 for h in current_gap) else "diurne"
                })
    
    # Issue pour gaps significatifs
    if service_gaps:
        total_gap_hours = sum(gap['duration_hours'] for gap in service_gaps)
        diurnal_gaps = [gap for gap in service_gaps if gap['gap_type'] == 'diurne']
        
        if diurnal_gaps:
            affected_trips = []
            issues.append({
                "type": "service_gaps",
                "field": "temporal_continuity",
                "count": len(diurnal_gaps),
                "affected_ids": affected_trips,
                "message": f"Gaps de service diurne détectés ({total_gap_hours}h total)"
            })
    
    # 4. Analyse spécialisée service nocturne
    night_service_analysis = {
        "has_night_service": hourly_distribution["night"]["count"] > 0,
        "night_service_count": hourly_distribution["night"]["count"],
        "night_service_percentage": hourly_distribution["night"]["percentage"],
        "night_service_level": "complet" if hourly_distribution["night"]["percentage"] > 10 else "partiel" if hourly_distribution["night"]["percentage"] > 5 else "minimal" if hourly_distribution["night"]["count"] > 0 else "absent"
    }
    
    # 5. Analyse équilibre heures de pointe
    morning_peak_count = hourly_distribution["morning_peak"]["count"]
    evening_peak_count = hourly_distribution["evening_peak"]["count"]
    peak_balance_threshold = max(total_valid_times * 0.05, 10)  # 5% ou 10 services minimum
    
    peak_hours_analysis = {
        "morning_peak_count": morning_peak_count,
        "evening_peak_count": evening_peak_count,
        "morning_peak_percentage": hourly_distribution["morning_peak"]["percentage"],
        "evening_peak_percentage": hourly_distribution["evening_peak"]["percentage"],
        "has_balanced_peaks": abs(morning_peak_count - evening_peak_count) < peak_balance_threshold,
        "peak_service_ratio": peak_ratio,
        "peak_dominance": "équilibré" if abs(morning_peak_count - evening_peak_count) < peak_balance_threshold else "matin" if morning_peak_count > evening_peak_count else "soir"
    }
    
    # 6. Identification des trips en heures creuses
    if 'trip_id' in df.columns and len(hourly_counts) > 0:
        try:
            # Définir seuil heures creuses (< 5% du service total)
            min_service_threshold = max(1, total_valid_times * 0.05)
            low_service_hours = [h for h, count in hourly_counts.items() if count < min_service_threshold]
            
            if low_service_hours:
                for trip_id, group in df.groupby('trip_id'):
                    trip_arrival_times = pd.to_timedelta(group['arrival_time'], errors='coerce')
                    trip_hours = (trip_arrival_times.dt.total_seconds() // 3600).astype(int) % 24
                    
                    if any(h in low_service_hours for h in trip_hours if pd.notna(h)):
                        low_service_trip_ids.add(trip_id)
        except Exception:
            pass
    
    # Calcul métriques de qualité de couverture
    coverage_quality_score = 0
    
    # Points pour étendue temporelle
    if service_span_hours >= 20:
        coverage_quality_score += 30
    elif service_span_hours >= 16:
        coverage_quality_score += 25
    elif service_span_hours >= 12:
        coverage_quality_score += 20
    else:
        coverage_quality_score += 10
    
    # Points pour service nocturne
    if night_service_analysis["night_service_percentage"] > 10:
        coverage_quality_score += 20
    elif night_service_analysis["night_service_percentage"] > 5:
        coverage_quality_score += 15
    elif night_service_analysis["has_night_service"]:
        coverage_quality_score += 10
    
    # Points pour équilibre pics
    if peak_hours_analysis["has_balanced_peaks"]:
        coverage_quality_score += 20
    elif peak_ratio > 30:
        coverage_quality_score += 15
    elif peak_ratio > 20:
        coverage_quality_score += 10
    
    # Points pour continuité (sans gaps diurnes)
    diurnal_gaps_count = len([gap for gap in service_gaps if gap['gap_type'] == 'diurne'])
    if diurnal_gaps_count == 0:
        coverage_quality_score += 30
    elif diurnal_gaps_count <= 1:
        coverage_quality_score += 20
    elif diurnal_gaps_count <= 2:
        coverage_quality_score += 10
    
    service_quality_metrics = {
        "coverage_quality_score": min(100, coverage_quality_score),
        "temporal_classification": service_time_span["service_type"],
        "night_service_level": night_service_analysis["night_service_level"],
        "peak_dominance": peak_hours_analysis["peak_dominance"],
        "service_continuity": "excellent" if diurnal_gaps_count == 0 else "bon" if diurnal_gaps_count <= 1 else "moyen" if diurnal_gaps_count <= 2 else "faible"
    }
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "conversion_error", "no_valid_times"]])
    coverage_issues = len([issue for issue in issues if issue["type"] in ["limited_service_hours", "service_gaps"]])
    
    if critical_issues > 0:
        status = "error"
    elif coverage_quality_score < 40:
        status = "warning"
    elif coverage_issues > 0:
        status = "warning"
    else:
        status = "success"
    
    # Construction du result
    result = {
        "temporal_overview": {
            "total_records": total_stop_times,
            "valid_times_analyzed": len(all_times),
            "service_span_hours": service_span_hours,
            "coverage_quality_score": coverage_quality_score
        },
        "service_time_span": service_time_span,
        "hourly_distribution": hourly_distribution,
        "service_analysis": {
            "night_service": night_service_analysis,
            "peak_hours": peak_hours_analysis,
            "service_gaps": service_gaps,
            "low_service_periods": len(low_service_trip_ids)
        },
        "quality_metrics": service_quality_metrics
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations pour couverture temporelle limitée
    if service_span_hours < 12:
        recommendations.append(f"URGENT: Étendre la plage horaire de service (actuel: {service_span_hours}h, recommandé: >12h)")
    elif service_span_hours < 16:
        recommendations.append(f"Considérer l'extension des heures de service (actuel: {service_span_hours}h pour service étendu)")
    
    # Recommendations pour service nocturne
    if not night_service_analysis["has_night_service"]:
        recommendations.append("Évaluer l'opportunité d'un service nocturne pour répondre à la demande 24h")
    elif night_service_analysis["night_service_level"] == "minimal":
        recommendations.append("Renforcer le service nocturne existant pour améliorer la couverture")
    
    # Recommendations pour gaps de service
    if service_gaps:
        diurnal_gaps = [gap for gap in service_gaps if gap['gap_type'] == 'diurne']
        if diurnal_gaps:
            worst_gap = max(diurnal_gaps, key=lambda x: x['duration_hours'])
            recommendations.append(f"Priorité: Combler le gap de service {worst_gap['start_hour']}h-{worst_gap['end_hour']}h ({worst_gap['duration_hours']}h)")
        
        if len(service_gaps) > 1:
            recommendations.append(f"Améliorer la continuité: {len(service_gaps)} gaps de service à traiter")
    
    # Recommendations pour équilibre des pics
    if not peak_hours_analysis["has_balanced_peaks"]:
        dominant_peak = "matin" if peak_hours_analysis["morning_peak_count"] > peak_hours_analysis["evening_peak_count"] else "soir"
        recommendations.append(f"Équilibrer le service entre pics (dominance actuelle: {dominant_peak})")
    
    # Recommendations pour optimisation
    if coverage_quality_score < 60:
        recommendations.append(f"Amélioration globale recommandée: score de couverture {coverage_quality_score}/100")
    
    # Recommendations pour trips heures creuses
    if len(low_service_trip_ids) > 0:
        recommendations.append(f"Analyser {len(low_service_trip_ids)} trips en heures creuses pour optimisation")
    
    # Recommendations positives
    if status == "success" and coverage_quality_score > 80:
        recommendations.append("Excellente couverture temporelle - service de qualité professionnelle")
    elif status == "success":
        recommendations.append("Bonne couverture temporelle globale - quelques optimisations possibles")
    
    # Recommendations stratégiques
    if service_time_span["service_type"] == "24h":
        recommendations.append("Maintenir et optimiser le service 24h/24 existant")
    elif peak_ratio > 50:
        recommendations.append("Considérer redistribution: service très concentré sur les heures de pointe")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyse la couverture temporelle des services de transport",
            "scope": f"Analyse de {len(all_times)} horaires valides sur {total_stop_times} enregistrements",
            "coverage_assessment": f"Service {service_time_span['service_type']} ({service_span_hours}h), niveau nocturne {night_service_analysis['night_service_level']}",
            "quality_evaluation": f"Score de couverture: {coverage_quality_score}/100 - {service_quality_metrics['service_continuity']}"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="compute_service_metrics",
    genre="statistics",
    description="Calcule les métriques de service : durées moyennes, nombre d'arrêts par trip, fréquences",
    parameters={}
)
def compute_service_metrics(gtfs_data, **params):
    """
    Calcule les métriques de service détaillées à partir des données stop_times.txt.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Calcule les métriques de service de transport à partir des horaires"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    service_analytics = {}
    
    # Vérification colonnes essentielles
    required_columns = ['trip_id', 'stop_id']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "essential_columns",
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes essentielles manquantes: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Calcule les métriques de service de transport à partir des horaires",
                "missing_requirement": f"Colonnes obligatoires pour calcul métriques: {', '.join(missing_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }
    
    # Métriques de base
    basic_metrics = {
        "total_records": total_stop_times,
        "unique_trips": int(df['trip_id'].nunique()),
        "unique_stops": int(df['stop_id'].nunique()),
        "avg_records_per_trip": round(total_stop_times / max(df['trip_id'].nunique(), 1), 1)
    }
    
    # 1. Calcul des métriques de durée de parcours
    duration_analysis = {}
    if all(col in df.columns for col in ['arrival_time', 'departure_time']):
        try:
            # Calculer durées par trip
            trip_durations = []
            duration_details = []
            problematic_trips = []
            
            for trip_id, group in df.groupby('trip_id'):
                if len(group) < 2:
                    continue
                    
                # Trier par stop_sequence si disponible
                if 'stop_sequence' in df.columns:
                    try:
                        group = group.sort_values('stop_sequence')
                    except:
                        pass
                
                # Calculer durée totale du trip
                try:
                    first_departure = pd.to_timedelta(group.iloc[0]['departure_time'], errors='coerce')
                    last_arrival = pd.to_timedelta(group.iloc[-1]['arrival_time'], errors='coerce')
                    
                    if pd.notna(first_departure) and pd.notna(last_arrival):
                        duration_minutes = (last_arrival - first_departure).total_seconds() / 60
                        
                        if duration_minutes > 0:
                            trip_durations.append(duration_minutes)
                            duration_details.append({
                                'trip_id': trip_id,
                                'duration_minutes': round(duration_minutes, 1),
                                'stops_count': len(group)
                            })
                            
                            # Identifier trips avec durées anormales
                            if duration_minutes < 5:  # <5min suspect
                                problematic_trips.append({'trip_id': trip_id, 'issue': 'very_short', 'duration': duration_minutes})
                            elif duration_minutes > 480:  # >8h suspect
                                problematic_trips.append({'trip_id': trip_id, 'issue': 'very_long', 'duration': duration_minutes})
                        else:
                            problematic_trips.append({'trip_id': trip_id, 'issue': 'negative_duration', 'duration': duration_minutes})
                            
                except Exception:
                    problematic_trips.append({'trip_id': trip_id, 'issue': 'conversion_error', 'duration': None})
            
            if trip_durations:
                duration_analysis = {
                    "trips_with_duration": len(trip_durations),
                    "avg_duration_minutes": round(sum(trip_durations) / len(trip_durations), 1),
                    "min_duration_minutes": round(min(trip_durations), 1),
                    "max_duration_minutes": round(max(trip_durations), 1),
                    "median_duration_minutes": round(sorted(trip_durations)[len(trip_durations)//2], 1),
                    "duration_distribution": {
                        "very_short_trips": len([d for d in trip_durations if d < 15]),  # <15min
                        "short_trips": len([d for d in trip_durations if 15 <= d < 60]),  # 15-60min
                        "medium_trips": len([d for d in trip_durations if 60 <= d < 120]),  # 1-2h
                        "long_trips": len([d for d in trip_durations if d >= 120])  # >2h
                    },
                    "problematic_durations": len(problematic_trips)
                }
                
                # Issues pour durées problématiques
                if problematic_trips:
                    very_short = [t for t in problematic_trips if t['issue'] == 'very_short']
                    very_long = [t for t in problematic_trips if t['issue'] == 'very_long']
                    negative = [t for t in problematic_trips if t['issue'] == 'negative_duration']
                    
                    if very_short:
                        issues.append({
                            "type": "suspicious_short_duration",
                            "field": "trip_duration",
                            "count": len(very_short),
                            "affected_ids": [t['trip_id'] for t in very_short[:100]],
                            "message": f"Trips avec durées très courtes (<5min)"
                        })
                    
                    if very_long:
                        issues.append({
                            "type": "suspicious_long_duration",
                            "field": "trip_duration",
                            "count": len(very_long),
                            "affected_ids": [t['trip_id'] for t in very_long[:100]],
                            "message": f"Trips avec durées très longues (>8h)"
                        })
                    
                    if negative:
                        issues.append({
                            "type": "negative_duration",
                            "field": "trip_duration",
                            "count": len(negative),
                            "affected_ids": [t['trip_id'] for t in negative[:100]],
                            "message": "Trips avec durées négatives"
                        })
            else:
                issues.append({
                    "type": "no_duration_calculated",
                    "field": "trip_duration",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Aucune durée de trip calculable"
                })
                
        except Exception as e:
            issues.append({
                "type": "duration_calculation_error",
                "field": "trip_duration",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur calcul durées: {str(e)}"
            })
    else:
        duration_analysis = {"error": "Colonnes temporelles manquantes"}
        issues.append({
            "type": "missing_temporal_data",
            "field": "time_columns",
            "count": 1,
            "affected_ids": [],
            "message": "Impossible de calculer les durées sans arrival_time/departure_time"
        })
    
    # 2. Analyse des arrêts par trip
    stops_per_trip_analysis = {}
    try:
        stops_per_trip = df.groupby('trip_id')['stop_id'].count()
        
        stops_per_trip_analysis = {
            "avg_stops_per_trip": round(stops_per_trip.mean(), 1),
            "min_stops_per_trip": int(stops_per_trip.min()),
            "max_stops_per_trip": int(stops_per_trip.max()),
            "median_stops_per_trip": round(stops_per_trip.median(), 1),
            "stops_distribution": {
                "very_short_routes": int((stops_per_trip < 5).sum()),  # <5 arrêts
                "short_routes": int(((stops_per_trip >= 5) & (stops_per_trip < 15)).sum()),  # 5-15 arrêts
                "medium_routes": int(((stops_per_trip >= 15) & (stops_per_trip < 30)).sum()),  # 15-30 arrêts
                "long_routes": int((stops_per_trip >= 30).sum())  # >30 arrêts
            }
        }
        
        # Identifier trips avec très peu d'arrêts (potentiellement problématiques)
        very_short_trips = stops_per_trip[stops_per_trip < 3].index.tolist()
        if very_short_trips:
            issues.append({
                "type": "insufficient_stops",
                "field": "stops_per_trip",
                "count": len(very_short_trips),
                "affected_ids": very_short_trips[:100],
                "message": "Trips avec moins de 3 arrêts (potentiellement incomplets)"
            })
            
    except Exception as e:
        issues.append({
            "type": "stops_calculation_error",
            "field": "stops_per_trip",
            "count": 1,
            "affected_ids": [],
            "message": f"Erreur calcul arrêts par trip: {str(e)}"
        })
    
    # 3. Analyse de la fréquence de service par arrêt
    frequency_analysis = {}
    try:
        # Calculer fréquence par arrêt
        stop_frequencies = df['stop_id'].value_counts()
        
        frequency_analysis = {
            "total_unique_stops": len(stop_frequencies),
            "avg_frequency_per_stop": round(stop_frequencies.mean(), 1),
            "min_frequency_per_stop": int(stop_frequencies.min()),
            "max_frequency_per_stop": int(stop_frequencies.max()),
            "median_frequency_per_stop": round(stop_frequencies.median(), 1),
            "frequency_distribution": {
                "very_low_frequency": int((stop_frequencies < 5).sum()),    # <5 passages
                "low_frequency": int(((stop_frequencies >= 5) & (stop_frequencies < 20)).sum()),  # 5-20 passages
                "medium_frequency": int(((stop_frequencies >= 20) & (stop_frequencies < 50)).sum()),  # 20-50 passages
                "high_frequency": int((stop_frequencies >= 50).sum())  # >50 passages
            }
        }
        
        # Identifier arrêts très peu desservis
        underserved_stops = stop_frequencies[stop_frequencies < 3].index.tolist()
        if underserved_stops:
            issues.append({
                "type": "underserved_stops",
                "field": "stop_frequency",
                "count": len(underserved_stops),
                "affected_ids": underserved_stops[:100],
                "message": f"Arrêts très peu desservis (<3 passages)"
            })
        
        # Identifier arrêts sur-desservis (potentiels doublons)
        overserved_threshold = stop_frequencies.quantile(0.95)  # Top 5%
        overserved_stops = stop_frequencies[stop_frequencies > overserved_threshold].index.tolist()
        
        if len(overserved_stops) > 0 and overserved_threshold > 100:
            issues.append({
                "type": "potentially_overserved_stops",
                "field": "stop_frequency",
                "count": len(overserved_stops),
                "affected_ids": overserved_stops[:100],
                "message": f"Arrêts potentiellement sur-desservis (>{overserved_threshold:.0f} passages)"
            })
            
    except Exception as e:
        issues.append({
            "type": "frequency_calculation_error",
            "field": "stop_frequency",
            "count": 1,
            "affected_ids": [],
            "message": f"Erreur calcul fréquences: {str(e)}"
        })
    
    # 4. Métriques de performance du réseau
    network_performance = {}
    try:
        # Densité du réseau
        if basic_metrics["unique_trips"] > 0 and basic_metrics["unique_stops"] > 0:
            network_performance = {
                "network_density": round(total_stop_times / (basic_metrics["unique_trips"] * basic_metrics["unique_stops"]), 4),
                "stop_to_trip_ratio": round(basic_metrics["unique_stops"] / basic_metrics["unique_trips"], 2),
                "service_coverage_index": round(basic_metrics["avg_records_per_trip"] * basic_metrics["unique_trips"] / max(basic_metrics["unique_stops"], 1), 2)
            }
        
        # Classification du type de réseau
        if basic_metrics["unique_trips"] > 500 and basic_metrics["unique_stops"] > 1000:
            network_type = "réseau_urbain_dense"
        elif basic_metrics["unique_trips"] > 100 and basic_metrics["unique_stops"] > 200:
            network_type = "réseau_urbain_moyen"
        elif basic_metrics["unique_trips"] > 20:
            network_type = "réseau_local"
        else:
            network_type = "réseau_minimal"
            
        network_performance["network_classification"] = network_type
        
    except Exception:
        network_performance = {"error": "Impossible de calculer les métriques réseau"}
    
    # 5. Indicateurs de qualité de service
    service_quality_indicators = {}
    
    # Complétude des données
    completeness_score = 0
    if 'arrival_time' in df.columns and 'departure_time' in df.columns:
        completeness_score += 40
    if 'stop_sequence' in df.columns:
        completeness_score += 20
    if duration_analysis and "trips_with_duration" in duration_analysis:
        if duration_analysis["trips_with_duration"] > basic_metrics["unique_trips"] * 0.9:
            completeness_score += 20
    if stops_per_trip_analysis and stops_per_trip_analysis.get("avg_stops_per_trip", 0) > 5:
        completeness_score += 20
    
    service_quality_indicators = {
        "data_completeness_score": completeness_score,
        "service_regularity": "régulier" if len(issues) == 0 else "irrégulier",
        "network_coverage_level": network_performance.get("network_classification", "unknown")
    }
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "no_duration_calculated"]])
    data_quality_issues = len([issue for issue in issues if issue["type"] in ["negative_duration", "insufficient_stops"]])
    service_issues = len([issue for issue in issues if issue["type"] in ["underserved_stops", "suspicious_short_duration", "suspicious_long_duration"]])
    
    if critical_issues > 0:
        status = "error"
    elif data_quality_issues > 0:
        status = "warning"
    elif service_issues > 2:  # Plus de 2 problèmes de service
        status = "warning"
    else:
        status = "success"
    
    # Construction du result
    result = {
        "basic_metrics": basic_metrics,
        "duration_analysis": duration_analysis,
        "stops_per_trip_analysis": stops_per_trip_analysis,
        "frequency_analysis": frequency_analysis,
        "network_performance": network_performance,
        "service_quality_indicators": service_quality_indicators
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations pour durées problématiques
    if duration_analysis.get("problematic_durations", 0) > 0:
        recommendations.append("Vérifier et corriger les trips avec durées anormales (très courtes <5min ou très longues >8h)")
    
    # Recommendations pour arrêts insuffisants
    if any(issue["type"] == "insufficient_stops" for issue in issues):
        recommendations.append("Compléter les trips avec moins de 3 arrêts ou les supprimer s'ils sont incomplets")
    
    # Recommendations pour fréquence
    if any(issue["type"] == "underserved_stops" for issue in issues):
        underserved_count = next(issue["count"] for issue in issues if issue["type"] == "underserved_stops")
        recommendations.append(f"Améliorer la desserte de {underserved_count} arrêts peu fréquentés (<3 passages)")
    
    # Recommendations réseau
    if network_performance.get("network_classification") == "réseau_minimal":
        recommendations.append("Considérer l'extension du réseau pour améliorer la couverture de service")
    
    # Recommendations qualité
    if service_quality_indicators.get("data_completeness_score", 0) < 60:
        recommendations.append("Enrichir les données avec stop_sequence et horaires complets pour améliorer la qualité")
    
    # Recommendations pour durées
    if duration_analysis.get("avg_duration_minutes", 0) > 0:
        avg_duration = duration_analysis["avg_duration_minutes"]
        if avg_duration < 20:
            recommendations.append("Durée moyenne faible - vérifier la complétude des parcours")
        elif avg_duration > 180:
            recommendations.append("Durée moyenne élevée - optimiser les itinéraires si possible")
    
    # Recommendations positives
    if status == "success":
        if service_quality_indicators.get("data_completeness_score", 0) > 80:
            recommendations.append("Excellentes métriques de service - données de qualité professionnelle")
        else:
            recommendations.append("Bonnes métriques de service - réseau fonctionnel et cohérent")
    
    # Recommendations d'analyse
    if basic_metrics["unique_trips"] > 100:
        recommendations.append("Dataset significatif - analyses avancées recommandées (patterns temporels, optimisation)")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Calcule les métriques de service de transport à partir des horaires",
            "scope": f"Analyse de {total_stop_times} enregistrements, {basic_metrics['unique_trips']} trips, {basic_metrics['unique_stops']} arrêts",
            "metrics_computed": "Durées parcours, fréquences, performance réseau, indicateurs qualité",
            "network_assessment": f"Réseau classifié: {network_performance.get('network_classification', 'unknown')}"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="compute_temporal_distribution",
    genre="statistics",
    description="Analyse la répartition temporelle : pics horaires, patterns de service",
    parameters={}
)
def compute_service_metrics(gtfs_data, **params):
    """
    Calcule les métriques de service détaillées à partir des données stop_times.txt.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Calcule les métriques de service de transport à partir des horaires"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    service_analytics = {}
    
    # Vérification colonnes essentielles
    required_columns = ['trip_id', 'stop_id']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_field",
                "field": "essential_columns",
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes essentielles manquantes: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Calcule les métriques de service de transport à partir des horaires",
                "missing_requirement": f"Colonnes obligatoires pour calcul métriques: {', '.join(missing_columns)}"
            },
            "recommendations": [f"URGENT: Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }
    
    # Métriques de base
    basic_metrics = {
        "total_records": total_stop_times,
        "unique_trips": int(df['trip_id'].nunique()),
        "unique_stops": int(df['stop_id'].nunique()),
        "avg_records_per_trip": round(total_stop_times / max(df['trip_id'].nunique(), 1), 1)
    }
    
    # 1. Calcul des métriques de durée de parcours
    duration_analysis = {}
    if all(col in df.columns for col in ['arrival_time', 'departure_time']):
        try:
            # Calculer durées par trip
            trip_durations = []
            duration_details = []
            problematic_trips = []
            
            for trip_id, group in df.groupby('trip_id'):
                if len(group) < 2:
                    continue
                    
                # Trier par stop_sequence si disponible
                if 'stop_sequence' in df.columns:
                    try:
                        group = group.sort_values('stop_sequence')
                    except:
                        pass
                
                # Calculer durée totale du trip
                try:
                    first_departure = pd.to_timedelta(group.iloc[0]['departure_time'], errors='coerce')
                    last_arrival = pd.to_timedelta(group.iloc[-1]['arrival_time'], errors='coerce')
                    
                    if pd.notna(first_departure) and pd.notna(last_arrival):
                        duration_minutes = (last_arrival - first_departure).total_seconds() / 60
                        
                        if duration_minutes > 0:
                            trip_durations.append(duration_minutes)
                            duration_details.append({
                                'trip_id': trip_id,
                                'duration_minutes': round(duration_minutes, 1),
                                'stops_count': len(group)
                            })
                            
                            # Identifier trips avec durées anormales
                            if duration_minutes < 5:  # <5min suspect
                                problematic_trips.append({'trip_id': trip_id, 'issue': 'very_short', 'duration': duration_minutes})
                            elif duration_minutes > 480:  # >8h suspect
                                problematic_trips.append({'trip_id': trip_id, 'issue': 'very_long', 'duration': duration_minutes})
                        else:
                            problematic_trips.append({'trip_id': trip_id, 'issue': 'negative_duration', 'duration': duration_minutes})
                            
                except Exception:
                    problematic_trips.append({'trip_id': trip_id, 'issue': 'conversion_error', 'duration': None})
            
            if trip_durations:
                duration_analysis = {
                    "trips_with_duration": len(trip_durations),
                    "avg_duration_minutes": round(sum(trip_durations) / len(trip_durations), 1),
                    "min_duration_minutes": round(min(trip_durations), 1),
                    "max_duration_minutes": round(max(trip_durations), 1),
                    "median_duration_minutes": round(sorted(trip_durations)[len(trip_durations)//2], 1),
                    "duration_distribution": {
                        "very_short_trips": len([d for d in trip_durations if d < 15]),  # <15min
                        "short_trips": len([d for d in trip_durations if 15 <= d < 60]),  # 15-60min
                        "medium_trips": len([d for d in trip_durations if 60 <= d < 120]),  # 1-2h
                        "long_trips": len([d for d in trip_durations if d >= 120])  # >2h
                    },
                    "problematic_durations": len(problematic_trips)
                }
                
                # Issues pour durées problématiques
                if problematic_trips:
                    very_short = [t for t in problematic_trips if t['issue'] == 'very_short']
                    very_long = [t for t in problematic_trips if t['issue'] == 'very_long']
                    negative = [t for t in problematic_trips if t['issue'] == 'negative_duration']
                    
                    if very_short:
                        issues.append({
                            "type": "suspicious_short_duration",
                            "field": "trip_duration",
                            "count": len(very_short),
                            "affected_ids": [t['trip_id'] for t in very_short[:100]],
                            "message": f"Trips avec durées très courtes (<5min)"
                        })
                    
                    if very_long:
                        issues.append({
                            "type": "suspicious_long_duration",
                            "field": "trip_duration",
                            "count": len(very_long),
                            "affected_ids": [t['trip_id'] for t in very_long[:100]],
                            "message": f"Trips avec durées très longues (>8h)"
                        })
                    
                    if negative:
                        issues.append({
                            "type": "negative_duration",
                            "field": "trip_duration",
                            "count": len(negative),
                            "affected_ids": [t['trip_id'] for t in negative[:100]],
                            "message": "Trips avec durées négatives"
                        })
            else:
                issues.append({
                    "type": "no_duration_calculated",
                    "field": "trip_duration",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Aucune durée de trip calculable"
                })
                
        except Exception as e:
            issues.append({
                "type": "duration_calculation_error",
                "field": "trip_duration",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur calcul durées: {str(e)}"
            })
    else:
        duration_analysis = {"error": "Colonnes temporelles manquantes"}
        issues.append({
            "type": "missing_temporal_data",
            "field": "time_columns",
            "count": 1,
            "affected_ids": [],
            "message": "Impossible de calculer les durées sans arrival_time/departure_time"
        })
    
    # 2. Analyse des arrêts par trip
    stops_per_trip_analysis = {}
    try:
        stops_per_trip = df.groupby('trip_id')['stop_id'].count()
        
        stops_per_trip_analysis = {
            "avg_stops_per_trip": round(stops_per_trip.mean(), 1),
            "min_stops_per_trip": int(stops_per_trip.min()),
            "max_stops_per_trip": int(stops_per_trip.max()),
            "median_stops_per_trip": round(stops_per_trip.median(), 1),
            "stops_distribution": {
                "very_short_routes": int((stops_per_trip < 5).sum()),  # <5 arrêts
                "short_routes": int(((stops_per_trip >= 5) & (stops_per_trip < 15)).sum()),  # 5-15 arrêts
                "medium_routes": int(((stops_per_trip >= 15) & (stops_per_trip < 30)).sum()),  # 15-30 arrêts
                "long_routes": int((stops_per_trip >= 30).sum())  # >30 arrêts
            }
        }
        
        # Identifier trips avec très peu d'arrêts (potentiellement problématiques)
        very_short_trips = stops_per_trip[stops_per_trip < 3].index.tolist()
        if very_short_trips:
            issues.append({
                "type": "insufficient_stops",
                "field": "stops_per_trip",
                "count": len(very_short_trips),
                "affected_ids": very_short_trips[:100],
                "message": "Trips avec moins de 3 arrêts (potentiellement incomplets)"
            })
            
    except Exception as e:
        issues.append({
            "type": "stops_calculation_error",
            "field": "stops_per_trip",
            "count": 1,
            "affected_ids": [],
            "message": f"Erreur calcul arrêts par trip: {str(e)}"
        })
    
    # 3. Analyse de la fréquence de service par arrêt
    frequency_analysis = {}
    try:
        # Calculer fréquence par arrêt
        stop_frequencies = df['stop_id'].value_counts()
        
        frequency_analysis = {
            "total_unique_stops": len(stop_frequencies),
            "avg_frequency_per_stop": round(stop_frequencies.mean(), 1),
            "min_frequency_per_stop": int(stop_frequencies.min()),
            "max_frequency_per_stop": int(stop_frequencies.max()),
            "median_frequency_per_stop": round(stop_frequencies.median(), 1),
            "frequency_distribution": {
                "very_low_frequency": int((stop_frequencies < 5).sum()),    # <5 passages
                "low_frequency": int(((stop_frequencies >= 5) & (stop_frequencies < 20)).sum()),  # 5-20 passages
                "medium_frequency": int(((stop_frequencies >= 20) & (stop_frequencies < 50)).sum()),  # 20-50 passages
                "high_frequency": int((stop_frequencies >= 50).sum())  # >50 passages
            }
        }
        
        # Identifier arrêts très peu desservis
        underserved_stops = stop_frequencies[stop_frequencies < 3].index.tolist()
        if underserved_stops:
            issues.append({
                "type": "underserved_stops",
                "field": "stop_frequency",
                "count": len(underserved_stops),
                "affected_ids": underserved_stops[:100],
                "message": f"Arrêts très peu desservis (<3 passages)"
            })
        
        # Identifier arrêts sur-desservis (potentiels doublons)
        overserved_threshold = stop_frequencies.quantile(0.95)  # Top 5%
        overserved_stops = stop_frequencies[stop_frequencies > overserved_threshold].index.tolist()
        
        if len(overserved_stops) > 0 and overserved_threshold > 100:
            issues.append({
                "type": "potentially_overserved_stops",
                "field": "stop_frequency",
                "count": len(overserved_stops),
                "affected_ids": overserved_stops[:100],
                "message": f"Arrêts potentiellement sur-desservis (>{overserved_threshold:.0f} passages)"
            })
            
    except Exception as e:
        issues.append({
            "type": "frequency_calculation_error",
            "field": "stop_frequency",
            "count": 1,
            "affected_ids": [],
            "message": f"Erreur calcul fréquences: {str(e)}"
        })
    
    # 4. Métriques de performance du réseau
    network_performance = {}
    try:
        # Densité du réseau
        if basic_metrics["unique_trips"] > 0 and basic_metrics["unique_stops"] > 0:
            network_performance = {
                "network_density": round(total_stop_times / (basic_metrics["unique_trips"] * basic_metrics["unique_stops"]), 4),
                "stop_to_trip_ratio": round(basic_metrics["unique_stops"] / basic_metrics["unique_trips"], 2),
                "service_coverage_index": round(basic_metrics["avg_records_per_trip"] * basic_metrics["unique_trips"] / max(basic_metrics["unique_stops"], 1), 2)
            }
        
        # Classification du type de réseau
        if basic_metrics["unique_trips"] > 500 and basic_metrics["unique_stops"] > 1000:
            network_type = "réseau_urbain_dense"
        elif basic_metrics["unique_trips"] > 100 and basic_metrics["unique_stops"] > 200:
            network_type = "réseau_urbain_moyen"
        elif basic_metrics["unique_trips"] > 20:
            network_type = "réseau_local"
        else:
            network_type = "réseau_minimal"
            
        network_performance["network_classification"] = network_type
        
    except Exception:
        network_performance = {"error": "Impossible de calculer les métriques réseau"}
    
    # 5. Indicateurs de qualité de service
    service_quality_indicators = {}
    
    # Complétude des données
    completeness_score = 0
    if 'arrival_time' in df.columns and 'departure_time' in df.columns:
        completeness_score += 40
    if 'stop_sequence' in df.columns:
        completeness_score += 20
    if duration_analysis and "trips_with_duration" in duration_analysis:
        if duration_analysis["trips_with_duration"] > basic_metrics["unique_trips"] * 0.9:
            completeness_score += 20
    if stops_per_trip_analysis and stops_per_trip_analysis.get("avg_stops_per_trip", 0) > 5:
        completeness_score += 20
    
    service_quality_indicators = {
        "data_completeness_score": completeness_score,
        "service_regularity": "régulier" if len(issues) == 0 else "irrégulier",
        "network_coverage_level": network_performance.get("network_classification", "unknown")
    }
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "missing_field", "no_duration_calculated"]])
    data_quality_issues = len([issue for issue in issues if issue["type"] in ["negative_duration", "insufficient_stops"]])
    service_issues = len([issue for issue in issues if issue["type"] in ["underserved_stops", "suspicious_short_duration", "suspicious_long_duration"]])
    
    if critical_issues > 0:
        status = "error"
    elif data_quality_issues > 0:
        status = "warning"
    elif service_issues > 2:  # Plus de 2 problèmes de service
        status = "warning"
    else:
        status = "success"
    
    # Construction du result
    result = {
        "basic_metrics": basic_metrics,
        "duration_analysis": duration_analysis,
        "stops_per_trip_analysis": stops_per_trip_analysis,
        "frequency_analysis": frequency_analysis,
        "network_performance": network_performance,
        "service_quality_indicators": service_quality_indicators
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations pour durées problématiques
    if duration_analysis.get("problematic_durations", 0) > 0:
        recommendations.append("Vérifier et corriger les trips avec durées anormales (très courtes <5min ou très longues >8h)")
    
    # Recommendations pour arrêts insuffisants
    if any(issue["type"] == "insufficient_stops" for issue in issues):
        recommendations.append("Compléter les trips avec moins de 3 arrêts ou les supprimer s'ils sont incomplets")
    
    # Recommendations pour fréquence
    if any(issue["type"] == "underserved_stops" for issue in issues):
        underserved_count = next(issue["count"] for issue in issues if issue["type"] == "underserved_stops")
        recommendations.append(f"Améliorer la desserte de {underserved_count} arrêts peu fréquentés (<3 passages)")
    
    # Recommendations réseau
    if network_performance.get("network_classification") == "réseau_minimal":
        recommendations.append("Considérer l'extension du réseau pour améliorer la couverture de service")
    
    # Recommendations qualité
    if service_quality_indicators.get("data_completeness_score", 0) < 60:
        recommendations.append("Enrichir les données avec stop_sequence et horaires complets pour améliorer la qualité")
    
    # Recommendations pour durées
    if duration_analysis.get("avg_duration_minutes", 0) > 0:
        avg_duration = duration_analysis["avg_duration_minutes"]
        if avg_duration < 20:
            recommendations.append("Durée moyenne faible - vérifier la complétude des parcours")
        elif avg_duration > 180:
            recommendations.append("Durée moyenne élevée - optimiser les itinéraires si possible")
    
    # Recommendations positives
    if status == "success":
        if service_quality_indicators.get("data_completeness_score", 0) > 80:
            recommendations.append("Excellentes métriques de service - données de qualité professionnelle")
        else:
            recommendations.append("Bonnes métriques de service - réseau fonctionnel et cohérent")
    
    # Recommendations d'analyse
    if basic_metrics["unique_trips"] > 100:
        recommendations.append("Dataset significatif - analyses avancées recommandées (patterns temporels, optimisation)")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Calcule les métriques de service de transport à partir des horaires",
            "scope": f"Analyse de {total_stop_times} enregistrements, {basic_metrics['unique_trips']} trips, {basic_metrics['unique_stops']} arrêts",
            "metrics_computed": "Durées parcours, fréquences, performance réseau, indicateurs qualité",
            "network_assessment": f"Réseau classifié: {network_performance.get('network_classification', 'unknown')}"
        },
        "recommendations": recommendations
    }


@audit_function(
    file_type="stop_times",
    name="analyze_accessibility_features",
    genre="accessibility",
    description="Analyse les caractéristiques d'accessibilité : wheelchair_accessible, pickup/drop_off types",
    parameters={}
)
def analyze_accessibility_features(gtfs_data, **params):
    """
    Analyse les caractéristiques d'accessibilité dans stop_times.txt (wheelchair, pickup/drop_off types).
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Analyse les caractéristiques d'accessibilité dans stop_times.txt"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    accessibility_analytics = {}
    
    # 1. Analyse détaillée wheelchair_accessible
    wheelchair_analysis = {}
    if 'wheelchair_accessible' in df.columns:
        try:
            # Nettoyer et convertir les données
            wheelchair_data = pd.to_numeric(df['wheelchair_accessible'], errors='coerce')
            valid_wheelchair_data = wheelchair_data.dropna()
            
            if len(valid_wheelchair_data) > 0:
                # Compter les valeurs selon spécification GTFS
                value_counts = valid_wheelchair_data.value_counts()
                
                # Mapping GTFS standard
                accessibility_mapping = {
                    0: {"label": "no_information", "description": "Pas d'information d'accessibilité"},
                    1: {"label": "accessible", "description": "Accessible en fauteuil roulant"}, 
                    2: {"label": "not_accessible", "description": "Non accessible en fauteuil roulant"}
                }
                
                wheelchair_analysis = {
                    "total_with_data": len(valid_wheelchair_data),
                    "data_coverage_percent": round((len(valid_wheelchair_data) / total_stop_times) * 100, 1),
                    "values_breakdown": {}
                }
                
                for value, info in accessibility_mapping.items():
                    count = int(value_counts.get(value, 0))
                    percentage = round((count / len(valid_wheelchair_data)) * 100, 1) if len(valid_wheelchair_data) > 0 else 0
                    
                    wheelchair_analysis["values_breakdown"][info["label"]] = {
                        "count": count,
                        "percentage": percentage,
                        "description": info["description"]
                    }
                
                # Détecter valeurs invalides
                valid_values = set(accessibility_mapping.keys())
                invalid_values = set(valid_wheelchair_data.unique()) - valid_values
                
                if invalid_values:
                    invalid_count = valid_wheelchair_data.isin(invalid_values).sum()
                    issues.append({
                        "type": "invalid_accessibility_values",
                        "field": "wheelchair_accessible",
                        "count": int(invalid_count),
                        "affected_ids": [],
                        "message": f"Valeurs wheelchair_accessible invalides: {sorted(invalid_values)} (attendu: 0,1,2)"
                    })
                
                # Issue si beaucoup de données manquantes
                missing_data_ratio = (total_stop_times - len(valid_wheelchair_data)) / total_stop_times
                if missing_data_ratio > 0.5:  # >50% manquant
                    issues.append({
                        "type": "insufficient_accessibility_data",
                        "field": "wheelchair_accessible",
                        "count": int(total_stop_times - len(valid_wheelchair_data)),
                        "affected_ids": [],
                        "message": f"Données d'accessibilité manquantes ({missing_data_ratio*100:.1f}%)"
                    })
                
                # Issue si trop de "pas d'information"
                no_info_percentage = wheelchair_analysis["values_breakdown"]["no_information"]["percentage"]
                if no_info_percentage > 70:
                    issues.append({
                        "type": "excessive_no_information",
                        "field": "wheelchair_accessible",
                        "count": wheelchair_analysis["values_breakdown"]["no_information"]["count"],
                        "affected_ids": [],
                        "message": f"Trop d'arrêts sans information d'accessibilité ({no_info_percentage}%)"
                    })
                    
            else:
                wheelchair_analysis = {"no_valid_data": True}
                issues.append({
                    "type": "no_wheelchair_data",
                    "field": "wheelchair_accessible",
                    "count": 1,
                    "affected_ids": [],
                    "message": "Aucune donnée wheelchair_accessible valide"
                })
                
        except Exception as e:
            wheelchair_analysis = {"error": str(e)}
            issues.append({
                "type": "wheelchair_analysis_error",
                "field": "wheelchair_accessible",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur analyse wheelchair_accessible: {str(e)}"
            })
    else:
        wheelchair_analysis = {"field_missing": True}
        issues.append({
            "type": "missing_wheelchair_field",
            "field": "wheelchair_accessible",
            "count": 1,
            "affected_ids": [],
            "message": "Champ wheelchair_accessible absent (optionnel mais recommandé)"
        })
    
    # 2. Analyse pickup_type et drop_off_type
    pickup_dropoff_analysis = {}
    
    for field_name in ['pickup_type', 'drop_off_type']:
        field_analysis = {}
        
        if field_name in df.columns:
            try:
                # Nettoyer et convertir les données
                field_data = pd.to_numeric(df[field_name], errors='coerce')
                valid_field_data = field_data.dropna()
                
                if len(valid_field_data) > 0:
                    # Mapping GTFS pour pickup/drop_off types
                    type_mapping = {
                        0: {"label": "regular", "description": "Service régulier"},
                        1: {"label": "no_service", "description": "Pas de service"}, 
                        2: {"label": "phone_agency", "description": "Téléphoner à l'agence"},
                        3: {"label": "coordinate_driver", "description": "Coordonner avec le conducteur"}
                    }
                    
                    value_counts = valid_field_data.value_counts()
                    
                    field_analysis = {
                        "total_with_data": len(valid_field_data),
                        "data_coverage_percent": round((len(valid_field_data) / total_stop_times) * 100, 1),
                        "values_breakdown": {}
                    }
                    
                    for value, info in type_mapping.items():
                        count = int(value_counts.get(value, 0))
                        percentage = round((count / len(valid_field_data)) * 100, 1) if len(valid_field_data) > 0 else 0
                        
                        field_analysis["values_breakdown"][info["label"]] = {
                            "count": count,
                            "percentage": percentage,
                            "description": info["description"]
                        }
                    
                    # Analyser les restrictions d'accessibilité
                    restricted_service = sum(value_counts.get(i, 0) for i in [1, 2, 3])  # Non-regular service
                    if restricted_service > 0:
                        field_analysis["has_service_restrictions"] = True
                        field_analysis["restricted_service_count"] = int(restricted_service)
                        field_analysis["restricted_service_percent"] = round((restricted_service / len(valid_field_data)) * 100, 1)
                    else:
                        field_analysis["has_service_restrictions"] = False
                    
                    # Détecter valeurs invalides
                    valid_values = set(type_mapping.keys())
                    invalid_values = set(valid_field_data.unique()) - valid_values
                    
                    if invalid_values:
                        invalid_count = valid_field_data.isin(invalid_values).sum()
                        issues.append({
                            "type": "invalid_pickup_dropoff_values",
                            "field": field_name,
                            "count": int(invalid_count),
                            "affected_ids": [],
                            "message": f"Valeurs {field_name} invalides: {sorted(invalid_values)} (attendu: 0,1,2,3)"
                        })
                else:
                    field_analysis = {"no_valid_data": True}
            except Exception as e:
                field_analysis = {"error": str(e)}
                issues.append({
                    "type": "pickup_dropoff_analysis_error",
                    "field": field_name,
                    "count": 1,
                    "affected_ids": [],
                    "message": f"Erreur analyse {field_name}: {str(e)}"
                })
        else:
            field_analysis = {"field_missing": True}
        
        pickup_dropoff_analysis[field_name] = field_analysis
    
    # 3. Analyse croisée accessibilité
    cross_accessibility_analysis = {}
    
    # Vérifier cohérence entre wheelchair et pickup/drop_off
    if ('wheelchair_accessible' in df.columns and 'pickup_type' in df.columns and 'drop_off_type' in df.columns):
        try:
            # Analyser la cohérence logique
            wheelchair_not_accessible = (pd.to_numeric(df['wheelchair_accessible'], errors='coerce') == 2)
            pickup_no_service = (pd.to_numeric(df['pickup_type'], errors='coerce') == 1)
            dropoff_no_service = (pd.to_numeric(df['drop_off_type'], errors='coerce') == 1)
            
            # Incohérences potentielles
            inconsistent_accessibility = wheelchair_not_accessible & ~(pickup_no_service | dropoff_no_service)
            inconsistency_count = inconsistent_accessibility.sum()
            
            cross_accessibility_analysis = {
                "cross_analysis_possible": True,
                "potential_inconsistencies": int(inconsistency_count),
                "analysis_coverage": "Cohérence wheelchair vs pickup/drop_off analysée"
            }
            
            if inconsistency_count > 0:
                issues.append({
                    "type": "accessibility_inconsistency",
                    "field": "cross_accessibility",
                    "count": int(inconsistency_count),
                    "affected_ids": [],
                    "message": "Incohérences potentielles entre wheelchair_accessible=2 et pickup/drop_off types"
                })
        except Exception:
            cross_accessibility_analysis = {"cross_analysis_possible": False, "error": "Impossible d'analyser la cohérence"}
    else:
        cross_accessibility_analysis = {"cross_analysis_possible": False, "reason": "Champs requis manquants"}
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "no_wheelchair_data"]])
    quality_issues = len([issue for issue in issues if issue["type"] in ["invalid_accessibility_values", "invalid_pickup_dropoff_values"]])
    completeness_issues = len([issue for issue in issues if issue["type"] in ["insufficient_accessibility_data", "excessive_no_information"]])
    
    if critical_issues > 0:
        status = "warning"  # Pas error car champs optionnels
    elif quality_issues > 0:
        status = "warning"
    elif completeness_issues > 1:
        status = "warning"
    else:
        status = "success"
    
    # Calcul métriques de qualité accessibilité
    accessibility_quality_score = 0
    
    # Points pour présence des champs
    if wheelchair_analysis.get("total_with_data", 0) > 0:
        accessibility_quality_score += 40
    if pickup_dropoff_analysis.get("pickup_type", {}).get("total_with_data", 0) > 0:
        accessibility_quality_score += 20
    if pickup_dropoff_analysis.get("drop_off_type", {}).get("total_with_data", 0) > 0:
        accessibility_quality_score += 20
    
    # Points pour qualité des données
    if wheelchair_analysis.get("values_breakdown", {}).get("accessible", {}).get("percentage", 0) > 30:
        accessibility_quality_score += 10
    if wheelchair_analysis.get("values_breakdown", {}).get("no_information", {}).get("percentage", 100) < 30:
        accessibility_quality_score += 10
    
    # Construction du result
    result = {
        "accessibility_overview": {
            "total_records": total_stop_times,
            "accessibility_quality_score": accessibility_quality_score,
            "has_wheelchair_info": 'wheelchair_accessible' in df.columns,
            "has_pickup_dropoff_info": any(field in df.columns for field in ['pickup_type', 'drop_off_type'])
        },
        "wheelchair_analysis": wheelchair_analysis,
        "pickup_dropoff_analysis": pickup_dropoff_analysis,
        "cross_accessibility_analysis": cross_accessibility_analysis
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations pour wheelchair_accessible
    if wheelchair_analysis.get("field_missing"):
        recommendations.append("Ajouter le champ wheelchair_accessible pour améliorer l'accessibilité des données")
    elif wheelchair_analysis.get("values_breakdown", {}).get("accessible", {}).get("percentage", 0) < 30:
        accessible_percent = wheelchair_analysis.get("values_breakdown", {}).get("accessible", {}).get("percentage", 0)
        recommendations.append(f"Améliorer l'accessibilité: seulement {accessible_percent}% des arrêts sont wheelchair_accessible")
    
    if wheelchair_analysis.get("values_breakdown", {}).get("no_information", {}).get("percentage", 0) > 50:
        no_info_percent = wheelchair_analysis.get("values_breakdown", {}).get("no_information", {}).get("percentage", 0)
        recommendations.append(f"Compléter les informations d'accessibilité: {no_info_percent}% sans information")
    
    # Recommendations pour pickup/drop_off
    missing_pickup_dropoff = [field for field in ['pickup_type', 'drop_off_type'] 
                             if pickup_dropoff_analysis.get(field, {}).get("field_missing")]
    if missing_pickup_dropoff:
        recommendations.append(f"Considérer l'ajout des champs: {', '.join(missing_pickup_dropoff)} pour documenter les restrictions")
    
    # Recommendations pour restrictions de service
    for field_name in ['pickup_type', 'drop_off_type']:
        field_data = pickup_dropoff_analysis.get(field_name, {})
        if field_data.get("has_service_restrictions") and field_data.get("restricted_service_percent", 0) > 20:
            recommendations.append(f"Attention: {field_data['restricted_service_percent']}% des arrêts ont des restrictions {field_name}")
    
    # Recommendations pour incohérences
    if cross_accessibility_analysis.get("potential_inconsistencies", 0) > 0:
        inconsistencies = cross_accessibility_analysis["potential_inconsistencies"]
        recommendations.append(f"Vérifier {inconsistencies} incohérences potentielles entre wheelchair et pickup/drop_off")
    
    # Recommendations globales
    if accessibility_quality_score < 50:
        recommendations.append(f"Amélioration accessibilité recommandée: score {accessibility_quality_score}/100")
    
    # Recommendations positives
    if status == "success":
        if accessibility_quality_score > 80:
            recommendations.append("Excellente documentation de l'accessibilité - conforme aux standards")
        else:
            recommendations.append("Bonne prise en compte de l'accessibilité dans les données")
    
    # Recommendations stratégiques
    if wheelchair_analysis.get("values_breakdown", {}).get("accessible", {}).get("count", 0) > 0:
        recommendations.append("Promouvoir les arrêts accessibles identifiés dans la communication voyageur")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Analyse les caractéristiques d'accessibilité dans stop_times.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements pour wheelchair_accessible et pickup/drop_off types",
            "gtfs_standards": "wheelchair_accessible: 0=pas d'info, 1=accessible, 2=non accessible; pickup/drop_off: 0=régulier, 1=pas de service, 2=téléphone, 3=coordination",
            "accessibility_importance": "Les données d'accessibilité sont cruciales pour l'inclusion et la planification de trajets"
        },
        "recommendations": recommendations
    }

@audit_function(
    file_type="stop_times",
    name="duplicate_stop_times_detection",
    genre='redondances',
    description="Détecte stop_times édités plusieurs fois avec même trip_id, stop_sequence, arrival & departure identiques.",
    parameters={}
)
def duplicate_stop_times_detection(gtfs_data, **params):
    """
    Détecte les doublons stricts dans stop_times.txt basés sur trip_id, stop_sequence, arrival_time, departure_time.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Accès au fichier avec extension .txt  
    df = gtfs_data.get('stop_times.txt')
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": "stop_times.txt",
                "count": 1,
                "affected_ids": [],
                "message": "Fichier stop_times.txt manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les doublons stricts dans stop_times.txt"
            },
            "recommendations": ["URGENT: Créer le fichier stop_times.txt"]
        }
    
    total_stop_times = len(df)
    issues = []
    
    # Vérification colonnes nécessaires pour détection doublons
    required_duplicate_columns = ['trip_id', 'stop_sequence', 'arrival_time', 'departure_time']
    available_columns = [col for col in required_duplicate_columns if col in df.columns]
    missing_columns = [col for col in required_duplicate_columns if col not in df.columns]
    
    if len(available_columns) < 2:  # Minimum trip_id + au moins un autre
        return {
            "status": "error",
            "issues": [{
                "type": "insufficient_columns",
                "field": "duplicate_detection",
                "count": len(missing_columns),
                "affected_ids": [],
                "message": f"Colonnes insuffisantes pour détection doublons. Manquant: {', '.join(missing_columns)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Détecte les doublons stricts dans stop_times.txt",
                "missing_requirement": f"Colonnes minimum requises: {', '.join(required_duplicate_columns)}"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
        }
    
    # Détection des doublons avec colonnes disponibles
    duplicate_analysis = {}
    
    try:
        # Détecter doublons stricts sur toutes colonnes disponibles
        duplicates_mask = df.duplicated(subset=available_columns, keep=False)
        duplicate_records = df[duplicates_mask]
        duplicate_count = len(duplicate_records)
        
        if duplicate_count > 0:
            # Analyser les groupes de doublons
            duplicate_groups = duplicate_records.groupby(available_columns).size()
            unique_duplicate_groups = len(duplicate_groups)
            largest_duplicate_group = duplicate_groups.max() if len(duplicate_groups) > 0 else 0
            
            # Identifier les trips concernés
            affected_trips = duplicate_records['trip_id'].unique().tolist() if 'trip_id' in duplicate_records.columns else []
            
            duplicate_analysis = {
                "total_duplicate_records": duplicate_count,
                "unique_duplicate_groups": unique_duplicate_groups,
                "largest_duplicate_group_size": int(largest_duplicate_group),
                "affected_trips_count": len(affected_trips),
                "duplicate_ratio_percent": round((duplicate_count / total_stop_times) * 100, 2),
                "columns_used_for_detection": available_columns
            }
            
            # Analyser les patterns de doublons
            if 'trip_id' in available_columns:
                trips_with_duplicates = duplicate_records['trip_id'].value_counts()
                worst_trip = trips_with_duplicates.index[0] if len(trips_with_duplicates) > 0 else None
                worst_trip_duplicates = trips_with_duplicates.iloc[0] if len(trips_with_duplicates) > 0 else 0
                
                duplicate_analysis.update({
                    "worst_affected_trip": worst_trip,
                    "worst_trip_duplicate_count": int(worst_trip_duplicates)
                })
            
            # Issue principale pour doublons détectés
            issues.append({
                "type": "strict_duplicates",
                "field": "stop_times_records",
                "count": duplicate_count,
                "affected_ids": affected_trips[:100],
                "message": f"Doublons stricts détectés ({unique_duplicate_groups} groupes, {duplicate_count} enregistrements)"
            })
            
            # Issue spécifique si beaucoup de doublons
            if duplicate_count > total_stop_times * 0.05:  # >5% de doublons
                issues.append({
                    "type": "excessive_duplicates",
                    "field": "data_quality",
                    "count": 1,
                    "affected_ids": [],
                    "message": f"Taux de doublons élevé ({duplicate_analysis['duplicate_ratio_percent']}%)"
                })
        else:
            duplicate_analysis = {
                "total_duplicate_records": 0,
                "unique_duplicate_groups": 0,
                "largest_duplicate_group_size": 0,
                "affected_trips_count": 0,
                "duplicate_ratio_percent": 0.0,
                "columns_used_for_detection": available_columns,
                "data_quality": "no_duplicates_found"
            }
            
    except Exception as e:
        duplicate_analysis = {"error": str(e)}
        issues.append({
            "type": "duplicate_detection_error",
            "field": "analysis",
            "count": 1,
            "affected_ids": [],
            "message": f"Erreur lors de la détection de doublons: {str(e)}"
        })
    
    # Analyse complémentaire: doublons partiels
    partial_duplicate_analysis = {}
    
    if 'trip_id' in df.columns and 'stop_sequence' in df.columns:
        try:
            # Détecter doublons trip_id + stop_sequence (même arrêt visité plusieurs fois)
            partial_duplicates_mask = df.duplicated(subset=['trip_id', 'stop_sequence'], keep=False)
            partial_duplicate_count = partial_duplicates_mask.sum()
            
            partial_duplicate_analysis = {
                "trip_sequence_duplicates": int(partial_duplicate_count),
                "analysis_available": True
            }
            
            if partial_duplicate_count > duplicate_analysis.get("total_duplicate_records", 0):
                # Il y a des doublons partiels en plus des stricts
                additional_partials = partial_duplicate_count - duplicate_analysis.get("total_duplicate_records", 0)
                if additional_partials > 0:
                    issues.append({
                        "type": "partial_duplicates",
                        "field": "trip_sequence_combination",
                        "count": int(additional_partials),
                        "affected_ids": [],
                        "message": f"Doublons partiels trip_id+stop_sequence (horaires différents)"
                    })
        except Exception:
            partial_duplicate_analysis = {"analysis_available": False, "error": "Impossible d'analyser doublons partiels"}
    else:
        partial_duplicate_analysis = {"analysis_available": False, "reason": "Colonnes trip_id ou stop_sequence manquantes"}
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_file", "insufficient_columns", "duplicate_detection_error"]])
    duplicate_issues = len([issue for issue in issues if issue["type"] in ["strict_duplicates", "excessive_duplicates"]])
    
    if critical_issues > 0:
        status = "error"
    elif duplicate_analysis.get("duplicate_ratio_percent", 0) > 5:  # >5% de doublons
        status = "error"
    elif duplicate_issues > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul métrique qualité données
    data_uniqueness = 100 - duplicate_analysis.get("duplicate_ratio_percent", 0)
    
    # Construction du result
    result = {
        "duplicate_overview": {
            "total_records": total_stop_times,
            "unique_records": total_stop_times - duplicate_analysis.get("total_duplicate_records", 0),
            "data_uniqueness_percent": round(data_uniqueness, 2),
            "detection_completeness": f"{len(available_columns)}/{len(required_duplicate_columns)} colonnes utilisées"
        },
        "strict_duplicate_analysis": duplicate_analysis,
        "partial_duplicate_analysis": partial_duplicate_analysis,
        "data_quality_metrics": {
            "uniqueness_score": round(data_uniqueness, 1),
            "integrity_level": "excellent" if data_uniqueness > 99 else "good" if data_uniqueness > 95 else "needs_improvement"
        }
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations pour doublons stricts
    if duplicate_analysis.get("total_duplicate_records", 0) > 0:
        duplicate_count = duplicate_analysis["total_duplicate_records"]
        groups_count = duplicate_analysis.get("unique_duplicate_groups", 0)
        
        recommendations.append(f"URGENT: Supprimer {duplicate_count} doublons stricts ({groups_count} groupes distincts)")
        
        if duplicate_analysis.get("worst_affected_trip"):
            worst_trip = duplicate_analysis["worst_affected_trip"]
            worst_count = duplicate_analysis.get("worst_trip_duplicate_count", 0)
            recommendations.append(f"Priorité: Traiter le trip '{worst_trip}' avec {worst_count} doublons")
    
    # Recommendations pour doublons partiels
    if partial_duplicate_analysis.get("trip_sequence_duplicates", 0) > duplicate_analysis.get("total_duplicate_records", 0):
        additional_partials = partial_duplicate_analysis["trip_sequence_duplicates"] - duplicate_analysis.get("total_duplicate_records", 0)
        recommendations.append(f"Examiner {additional_partials} doublons partiels (même trip+sequence, horaires différents)")
    
    # Recommendations pour amélioration détection
    if missing_columns:
        recommendations.append(f"Améliorer la détection en ajoutant: {', '.join(missing_columns)}")
    
    # Recommendations pour qualité données
    duplicate_ratio = duplicate_analysis.get("duplicate_ratio_percent", 0)
    if duplicate_ratio > 1:
        recommendations.append(f"Audit qualité recommandé: {duplicate_ratio}% de doublons affecte la fiabilité")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Excellente unicité des données - aucun doublon strict détecté")
    
    # Recommendations préventives
    if duplicate_analysis.get("total_duplicate_records", 0) > 0:
        recommendations.append("Mettre en place des contrôles pour éviter les futurs doublons lors des imports")
    
    # Recommendations pour optimisation
    if duplicate_analysis.get("largest_duplicate_group_size", 0) > 3:
        largest_group = duplicate_analysis["largest_duplicate_group_size"]
        recommendations.append(f"Investiguer les {largest_group} copies du même enregistrement (possibles erreurs système)")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Détecte les doublons stricts dans stop_times.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements avec {len(available_columns)} colonnes de détection",
            "detection_method": f"Doublons stricts sur: {', '.join(available_columns)}",
            "data_impact": "Les doublons dégradent la qualité des données et peuvent causer des incohérences"
        },
        "recommendations": recommendations
    } 

@audit_function(
    file_type="stop_times",
    name="stop_times_within_frequencies_intervals",
    genre='cross-validation',
    description="Vérifie que les horaires dans stop_times.txt sont inclus dans les intervalles de frequencies.txt pour chaque trip_id.",
    parameters={}
)
def stop_times_within_frequencies_intervals(gtfs_data, **params):
    """
    Vérifie la concordance entre les horaires stop_times.txt et les intervalles frequencies.txt.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    # Vérification présence des fichiers requis
    required_files = ['stop_times.txt', 'frequencies.txt']
    missing_files = [file for file in required_files if gtfs_data.get(file) is None or gtfs_data.get(file).empty]
    
    if missing_files:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_required_files",
                "field": "file_dependency",
                "count": len(missing_files),
                "affected_ids": [],
                "message": f"Fichiers requis manquants: {', '.join(missing_files)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifie la concordance entre horaires stop_times.txt et intervalles frequencies.txt",
                "missing_requirement": f"Fichiers obligatoires: {', '.join(missing_files)}"
            },
            "recommendations": [f"URGENT: Fournir les fichiers manquants: {', '.join(missing_files)}"]
        }
    
    stop_times_df = gtfs_data['stop_times.txt']
    frequencies_df = gtfs_data['frequencies.txt']
    
    total_stop_times = len(stop_times_df)
    total_frequency_trips = frequencies_df['trip_id'].nunique() if 'trip_id' in frequencies_df.columns else 0
    issues = []
    
    # Vérification colonnes nécessaires
    required_stop_times_columns = ['trip_id', 'arrival_time', 'departure_time']
    required_frequencies_columns = ['trip_id', 'start_time', 'end_time']
    
    missing_st_columns = [col for col in required_stop_times_columns if col not in stop_times_df.columns]
    missing_freq_columns = [col for col in required_frequencies_columns if col not in frequencies_df.columns]
    
    if missing_st_columns or missing_freq_columns:
        all_missing = missing_st_columns + [f"frequencies.{col}" for col in missing_freq_columns]
        return {
            "status": "error",
            "issues": [{
                "type": "missing_required_columns",
                "field": "validation_columns",
                "count": len(all_missing),
                "affected_ids": [],
                "message": f"Colonnes requises manquantes: {', '.join(all_missing)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifie la concordance entre horaires stop_times.txt et intervalles frequencies.txt",
                "missing_requirement": f"Colonnes obligatoires manquantes: {', '.join(all_missing)}"
            },
            "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(all_missing)}"]
        }
    
    # Construction des intervalles de fréquence par trip
    frequency_intervals = {}
    frequency_parsing_errors = []
    
    try:
        for trip_id, group in frequencies_df.groupby('trip_id'):
            intervals = []
            
            for _, row in group.iterrows():
                try:
                    # Conversion temps en secondes (fonction helper supposée disponible)
                    start_sec = time_to_seconds(row['start_time']) if hasattr(row, 'start_time') else None
                    end_sec = time_to_seconds(row['end_time']) if hasattr(row, 'end_time') else None
                    
                    # Fallback si time_to_seconds n'est pas disponible
                    if start_sec is None:
                        start_time = pd.to_timedelta(row['start_time'], errors='coerce')
                        start_sec = start_time.total_seconds() if pd.notna(start_time) else None
                    
                    if end_sec is None:
                        end_time = pd.to_timedelta(row['end_time'], errors='coerce')
                        end_sec = end_time.total_seconds() if pd.notna(end_time) else None
                    
                    if start_sec is not None and end_sec is not None:
                        if start_sec <= end_sec:  # Intervalle valide
                            intervals.append({
                                'start_seconds': start_sec,
                                'end_seconds': end_sec,
                                'duration_seconds': end_sec - start_sec
                            })
                        else:
                            frequency_parsing_errors.append({
                                'trip_id': trip_id,
                                'error': 'invalid_interval',
                                'start_time': row['start_time'],
                                'end_time': row['end_time']
                            })
                    else:
                        frequency_parsing_errors.append({
                            'trip_id': trip_id,
                            'error': 'time_conversion',
                            'start_time': row.get('start_time', 'N/A'),
                            'end_time': row.get('end_time', 'N/A')
                        })
                        
                except Exception as e:
                    frequency_parsing_errors.append({
                        'trip_id': trip_id,
                        'error': f'parsing_exception: {str(e)}',
                        'start_time': row.get('start_time', 'N/A'),
                        'end_time': row.get('end_time', 'N/A')
                    })
            
            if intervals:  # Ne garder que les trips avec intervalles valides
                frequency_intervals[trip_id] = intervals
                
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "frequency_parsing_error",
                "field": "frequencies_analysis",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur critique lors de l'analyse frequencies.txt: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": "Vérifie la concordance entre horaires stop_times.txt et intervalles frequencies.txt"
            },
            "recommendations": ["Vérifier le format des données dans frequencies.txt"]
        }
    
    # Issues pour erreurs de parsing des fréquences
    if frequency_parsing_errors:
        error_trips = list(set(error['trip_id'] for error in frequency_parsing_errors))
        issues.append({
            "type": "frequency_time_parsing_errors",
            "field": "frequencies_time_format",
            "count": len(frequency_parsing_errors),
            "affected_ids": error_trips[:100],
            "message": f"Erreurs de parsing des temps dans frequencies.txt ({len(error_trips)} trips affectés)"
        })
    
    # Analyse de concordance pour les trips avec fréquences valides
    concordance_analysis = {
        "total_frequency_trips": len(frequency_intervals),
        "trips_analyzed": 0,
        "concordant_trips": 0,
        "discordant_trips": 0,
        "discordant_records": 0
    }
    
    discordant_trip_details = []
    discordant_trip_ids = set()
    
    # Vérifier chaque trip avec fréquences
    for trip_id, intervals in frequency_intervals.items():
        trip_stop_times = stop_times_df[stop_times_df['trip_id'] == trip_id]
        
        if len(trip_stop_times) == 0:
            continue  # Pas de stop_times pour ce trip
            
        concordance_analysis["trips_analyzed"] += 1
        trip_is_concordant = True
        discordant_records_in_trip = 0
        
        # Vérifier chaque enregistrement stop_times de ce trip
        for _, stop_time_row in trip_stop_times.iterrows():
            try:
                # Convertir les temps stop_times en secondes
                arrival_time = pd.to_timedelta(stop_time_row['arrival_time'], errors='coerce')
                departure_time = pd.to_timedelta(stop_time_row['departure_time'], errors='coerce')
                
                arrival_sec = arrival_time.total_seconds() if pd.notna(arrival_time) else None
                departure_sec = departure_time.total_seconds() if pd.notna(departure_time) else None
                
                # Vérifier si au moins un des temps est dans un intervalle de fréquence
                times_to_check = [t for t in [arrival_sec, departure_sec] if t is not None]
                
                if times_to_check:
                    time_in_interval = False
                    
                    for time_sec in times_to_check:
                        for interval in intervals:
                            if interval['start_seconds'] <= time_sec <= interval['end_seconds']:
                                time_in_interval = True
                                break
                        if time_in_interval:
                            break
                    
                    if not time_in_interval:
                        trip_is_concordant = False
                        discordant_records_in_trip += 1
                        concordance_analysis["discordant_records"] += 1
                else:
                    # Pas de temps valides pour cet enregistrement
                    trip_is_concordant = False
                    discordant_records_in_trip += 1
                    concordance_analysis["discordant_records"] += 1
                    
            except Exception:
                # Erreur de traitement pour cet enregistrement
                trip_is_concordant = False
                discordant_records_in_trip += 1
                concordance_analysis["discordant_records"] += 1
        
        # Enregistrer le résultat pour ce trip
        if trip_is_concordant:
            concordance_analysis["concordant_trips"] += 1
        else:
            concordance_analysis["discordant_trips"] += 1
            discordant_trip_ids.add(trip_id)
            
            # Détails du trip discordant
            discordant_trip_details.append({
                'trip_id': trip_id,
                'discordant_records': discordant_records_in_trip,
                'total_stop_times': len(trip_stop_times),
                'frequency_intervals_count': len(intervals)
            })
    
    # Issues pour trips discordants
    if concordance_analysis["discordant_trips"] > 0:
        issues.append({
            "type": "stop_times_frequency_mismatch",
            "field": "temporal_concordance",
            "count": concordance_analysis["discordant_trips"],
            "affected_ids": list(discordant_trip_ids)[:100],
            "message": f"Trips avec horaires stop_times hors intervalles frequencies ({concordance_analysis['discordant_records']} enregistrements)"
        })
    
    # Issue si beaucoup d'enregistrements discordants
    if concordance_analysis["discordant_records"] > total_stop_times * 0.1:  # >10%
        issues.append({
            "type": "high_discordance_rate",
            "field": "data_consistency",
            "count": 1,
            "affected_ids": [],
            "message": f"Taux élevé de discordance ({concordance_analysis['discordant_records']/total_stop_times*100:.1f}%)"
        })
    
    # Analyse des trips stop_times sans fréquences correspondantes
    stop_times_trip_ids = set(stop_times_df['trip_id'].unique())
    frequency_trip_ids = set(frequency_intervals.keys())
    
    orphaned_stop_times_trips = stop_times_trip_ids - frequency_trip_ids
    missing_stop_times_trips = frequency_trip_ids - stop_times_trip_ids
    
    coverage_analysis = {
        "total_stop_times_trips": len(stop_times_trip_ids),
        "total_frequency_trips": len(frequency_trip_ids),
        "common_trips": len(stop_times_trip_ids.intersection(frequency_trip_ids)),
        "orphaned_stop_times_trips": len(orphaned_stop_times_trips),
        "missing_stop_times_trips": len(missing_stop_times_trips)
    }
    
    # Issues pour couverture incomplète
    if orphaned_stop_times_trips:
        issues.append({
            "type": "orphaned_stop_times",
            "field": "trip_coverage",
            "count": len(orphaned_stop_times_trips),
            "affected_ids": list(orphaned_stop_times_trips)[:100],
            "message": f"Trips stop_times sans définition dans frequencies.txt"
        })
    
    if missing_stop_times_trips:
        issues.append({
            "type": "missing_stop_times",
            "field": "trip_coverage",
            "count": len(missing_stop_times_trips),
            "affected_ids": list(missing_stop_times_trips)[:100],
            "message": f"Trips frequencies.txt sans horaires stop_times correspondants"
        })
    
    # Détermination du status
    critical_issues = len([issue for issue in issues if issue["type"] in ["missing_required_files", "missing_required_columns", "frequency_parsing_error"]])
    concordance_issues = len([issue for issue in issues if issue["type"] in ["stop_times_frequency_mismatch", "high_discordance_rate"]])
    coverage_issues = len([issue for issue in issues if issue["type"] in ["orphaned_stop_times", "missing_stop_times"]])
    
    if critical_issues > 0:
        status = "error"
    elif concordance_analysis["discordant_records"] > total_stop_times * 0.2:  # >20% discordant
        status = "error"
    elif concordance_issues > 0 or coverage_issues > 0:
        status = "warning"
    else:
        status = "success"
    
    # Calcul métriques de qualité
    concordance_rate = 0
    if concordance_analysis["trips_analyzed"] > 0:
        concordance_rate = (concordance_analysis["concordant_trips"] / concordance_analysis["trips_analyzed"]) * 100
    
    coverage_rate = 0
    if len(stop_times_trip_ids) > 0:
        coverage_rate = (coverage_analysis["common_trips"] / len(stop_times_trip_ids)) * 100
    
    # Construction du result
    result = {
        "concordance_overview": {
            "total_stop_times_records": total_stop_times,
            "trips_with_frequencies": len(frequency_intervals),
            "concordance_rate_percent": round(concordance_rate, 1),
            "coverage_rate_percent": round(coverage_rate, 1)
        },
        "concordance_analysis": concordance_analysis,
        "coverage_analysis": coverage_analysis,
        "parsing_issues": {
            "frequency_parsing_errors": len(frequency_parsing_errors),
            "error_details": frequency_parsing_errors[:10]  # Limiter pour performance
        },
        "discordant_details": discordant_trip_details[:20]  # Top 20 trips problématiques
    }
    
    # Construction des recommendations
    recommendations = []
    
    # Recommendations pour erreurs de parsing
    if frequency_parsing_errors:
        error_count = len(frequency_parsing_errors)
        recommendations.append(f"URGENT: Corriger {error_count} erreurs de format dans frequencies.txt")
    
    # Recommendations pour discordances
    if concordance_analysis["discordant_trips"] > 0:
        discordant_count = concordance_analysis["discordant_trips"]
        discordant_records = concordance_analysis["discordant_records"]
        
        recommendations.append(f"Priorité: Réconcilier {discordant_count} trips avec {discordant_records} horaires hors intervalles")
        
        # Trip le plus problématique
        if discordant_trip_details:
            worst_trip = max(discordant_trip_details, key=lambda x: x['discordant_records'])
            recommendations.append(f"Examiner le trip '{worst_trip['trip_id']}' avec {worst_trip['discordant_records']} discordances")
    
    # Recommendations pour couverture
    if orphaned_stop_times_trips:
        recommendations.append(f"Définir les fréquences pour {len(orphaned_stop_times_trips)} trips stop_times orphelins")
        
    if missing_stop_times_trips:
        recommendations.append(f"Ajouter les horaires stop_times pour {len(missing_stop_times_trips)} trips frequencies orphelins")
    
    # Recommendations pour taux de concordance
    if concordance_rate < 80:
        recommendations.append(f"Améliorer la concordance: taux actuel {concordance_rate:.1f}% (objectif: >80%)")
    
    # Recommendations pour optimisation
    if concordance_analysis["trips_analyzed"] > 100 and concordance_rate > 95:
        recommendations.append("Excellent niveau de concordance - considérer automatisation validation continue")
    
    # Recommendations positives
    if status == "success":
        recommendations.append("Parfaite concordance stop_times/frequencies - cohérence temporelle assurée")
    elif status == "warning" and concordance_rate > 90:
        recommendations.append("Bonne concordance globale - quelques ajustements mineurs nécessaires")
    
    # Recommendations méthodologiques
    if len(frequency_intervals) > 0:
        avg_intervals_per_trip = sum(len(intervals) for intervals in frequency_intervals.values()) / len(frequency_intervals)
        if avg_intervals_per_trip > 3:
            recommendations.append(f"Complexité élevée: {avg_intervals_per_trip:.1f} intervalles/trip en moyenne - simplifier si possible")
    
    # Filtrer les recommendations None
    recommendations = [rec for rec in recommendations if rec is not None]
    
    return {
        "status": status,
        "issues": issues,
        "result": result,
        "explanation": {
            "purpose": "Vérifie la concordance entre horaires stop_times.txt et intervalles frequencies.txt",
            "scope": f"Analyse de {total_stop_times} enregistrements stop_times vs {len(frequency_intervals)} trips avec fréquences",
            "validation_method": "Vérification inclusion des horaires stop_times dans les intervalles start_time/end_time de frequencies",
            "data_consistency": f"Taux de concordance: {concordance_rate:.1f}%, couverture: {coverage_rate:.1f}%"
        },
        "recommendations": recommendations
    }