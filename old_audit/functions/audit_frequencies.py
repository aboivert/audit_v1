"""
Fonctions d'audit pour le file_type: frequencies
"""

#AJOUTER GARDE FOU DE BLOCAGE A L'INIT DE LA PAGE, SI PAS DE FICHIER, IMPOSSIBLE DE LANCER LES AUDITS

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="frequencies",
    name="required_fields_check",
    genre="validity",
    description="Vérifie que tous les champs requis sont présents dans frequencies.txt.",
    parameters={}
)
def required_fields_check(gtfs_data, **params):
    required = {'trip_id', 'start_time', 'end_time', 'headway_secs'}
    gtfs_data.get('frequencies.txt')
    df = gtfs_data.get('frequencies.txt')  # ← enlever .txt
    
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier frequencies.txt manquant."],
            "result": {},  # ← summary → result
            "explanation": {
                "purpose": "Vérifie la présence des champs obligatoires dans le fichier frequencies.txt selon la norme GTFS."
            },
            "recommendations": ["Fournir un fichier frequencies.txt valide."]
        }
    
    missing = required - set(df.columns)
    present = required & set(df.columns)
    total_required = len(required)
    present_count = len(present)
    missing_count = len(missing)
    
    # Score basé sur le pourcentage de champs présents
    completeness_score = (present_count / total_required) * 100
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if missing_count > 0:
        issues.append({
            "type": "missing_column",
            "field": "required_fields",
            "count": missing_count,
            "affected_ids": [],
            "details": list(missing),
            "message": f"{missing_count} champs obligatoires manquants: {', '.join(missing)}"
        })
    
    # Status basé sur la complétude
    if completeness_score == 100:
        status = "success"
    elif completeness_score >= 75:  # Si au moins 3/4 des champs requis
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_required_fields": total_required,
            "present_fields": present_count,
            "missing_fields": missing_count,
            "completeness_score": round(completeness_score, 1),
            "field_analysis": {
                "present": list(present),
                "missing": list(missing)
            }
        },
        "explanation": {
            "purpose": "Vérifie la présence des champs obligatoires dans frequencies.txt selon la spécification GTFS.",
            "required_fields": "Les champs trip_id, start_time, end_time et headway_secs sont obligatoires pour définir les fréquences de service.",
            "compliance_status": f"Structure conforme à {completeness_score:.1f}% - {present_count}/{total_required} champs requis présents",
            "field_roles": {
                "trip_id": "Identifiant du voyage auquel s'applique la fréquence",
                "start_time": "Heure de début de la période de fréquence",
                "end_time": "Heure de fin de la période de fréquence", 
                "headway_secs": "Intervalle en secondes entre les passages"
            }
        },
        "recommendations": [
            rec for rec in [
                f"Ajouter les colonnes manquantes: {', '.join(missing)} dans frequencies.txt." if missing_count > 0 else None,
                "Consulter la spécification GTFS officielle pour le format exact des champs requis." if missing_count > 0 else None,
                "Vérifier que les champs présents contiennent des données valides." if present_count > 0 and missing_count == 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="invalid_time_format",
    genre="validity",
    description="Détecte les start_time ou end_time mal formatées (HH:MM:SS).",
    parameters={}
)
def invalid_time_format(gtfs_data, **params):
    df = gtfs_data.get('frequencies.txt')  # ← enlever .txt
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier frequencies.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Vérifie que les heures de début et fin respectent le format HH:MM:SS dans frequencies.txt."
            },
            "recommendations": ["Fournir un fichier frequencies.txt valide."]
        }
    
    time_pattern = re.compile(r'^\d{1,2}:\d{2}:\d{2}$')
    
    # Vérification des formats start_time et end_time
    invalid_start = ~df['start_time'].astype(str).str.match(time_pattern)
    invalid_end = ~df['end_time'].astype(str).str.match(time_pattern)
    invalid_rows = df[invalid_start | invalid_end]
    
    count_invalid = len(invalid_rows)
    total = len(df)
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = compute_score(count_invalid, total)
    
    # Récupérer les trip_ids problématiques
    invalid_trip_ids = invalid_rows['trip_id'].tolist() if 'trip_id' in invalid_rows.columns else []
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if count_invalid > 0:
        issues.append({
            "type": "invalid_format",
            "field": "start_time/end_time",
            "count": count_invalid,
            "affected_ids": invalid_trip_ids,
            "message": f"{count_invalid} entrées avec format de temps invalide"
        })
    
    # Status basé sur le score ← UTILISER TON SYSTÈME
    if score == 100:
        status = "success"
    elif score >= 80:
        status = "warning" 
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_entries": total,
            "invalid_entries": count_invalid,
            "valid_entries": total - count_invalid,
            "validity_score": score,
            "format_compliance": f"{total - count_invalid}/{total} entrées conformes"
        },
        "explanation": {
            "purpose": "Vérifie que tous les champs temporels (start_time, end_time) respectent le format HH:MM:SS requis par GTFS.",
            "format_requirement": "Format attendu: HH:MM:SS (ex: 06:30:00, 14:45:30)",
            "validation_scope": f"Analyse de {total} entrées de fréquence",
            "quality_assessment": f"Score de validité: {score}/100",
            "compliance_rate": f"{((total - count_invalid) / total * 100):.1f}% des entrées ont un format valide" if total > 0 else "Aucune entrée à valider"
        },
        "recommendations": [
            rec for rec in [
                "Corriger les formats des champs start_time et end_time au format HH:MM:SS standard." if count_invalid > 0 else None,
                "Vérifier que les heures utilisent bien 2 chiffres pour les minutes et secondes (ex: 06:05:00 et non 6:5:0)." if count_invalid > 0 else None,
                "Considérer l'utilisation d'outils de validation GTFS pour détecter automatiquement ces erreurs." if count_invalid > 0 else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="frequencies",
    name="start_after_end_check",
    genre="validity",
    description="Vérifie que start_time est strictement antérieur à end_time.",
    parameters={}
)
def start_after_end_check(gtfs_data, **params):
    df = gtfs_data.get('frequencies.txt')  # ← enlever .txt
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier frequencies.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Vérifie que start_time est antérieur à end_time dans chaque plage de fréquence."
            },
            "recommendations": ["Fournir un fichier frequencies.txt valide."]
        }
    df = df.copy()
    # Utiliser ta fonction parse_time depuis __init__.py
    df['start'] = df['start_time'].apply(parse_time)
    df['end'] = df['end_time'].apply(parse_time)
    
    # Identifier les plages horaires invalides
    invalid = df[(df['start'].notnull()) & (df['end'].notnull()) & (df['start'] >= df['end'])]
    
    count_invalid = len(invalid)
    total = len(df)
    valid_count = total - count_invalid
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = compute_score(count_invalid, total)
    
    # Récupérer les trip_ids problématiques
    invalid_trip_ids = invalid['trip_id'].tolist() if 'trip_id' in invalid.columns else []
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if count_invalid > 0:
        issues.append({
            "type": "logical_error",
            "field": "start_time/end_time",
            "count": count_invalid,
            "affected_ids": invalid_trip_ids,
            "message": f"{count_invalid} plages horaires avec start_time >= end_time"
        })
    
    # Status basé sur le score ← UTILISER TON SYSTÈME
    if score == 100:
        status = "success"
    elif score >= 90:  # Plus strict car erreur logique grave
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_frequency_periods": total,
            "invalid_periods": count_invalid,
            "valid_periods": valid_count,
            "logical_consistency_score": score,
            "consistency_rate": f"{valid_count}/{total} plages cohérentes"
        },
        "explanation": {
            "purpose": "Vérifie la cohérence logique des plages horaires : start_time doit être antérieur à end_time.",
            "validation_logic": "Une plage de fréquence est valide si start_time < end_time (durée positive).",
            "validation_scope": f"Analyse de {total} plages de fréquence",
            "quality_assessment": f"Score de cohérence logique: {score}/100",
            "consistency_rate": f"{(valid_count / total * 100):.1f}% des plages ont une durée positive" if total > 0 else "Aucune plage à valider"
        },
        "recommendations": [
            rec for rec in [
                "Corriger les plages horaires où start_time est supérieur ou égal à end_time." if count_invalid > 0 else None,
                "Vérifier que les heures ne traversent pas minuit sans gestion appropriée (ex: 23:30 à 01:15)." if count_invalid > 0 else None,
                "S'assurer que les plages de fréquence ont une durée minimale cohérente avec headway_secs." if count_invalid > 0 else None,
                "Considérer l'ajout de validations automatiques lors de la saisie des données." if count_invalid > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="headway_outliers",
    genre="quality",
    description="Détecte les headway_secs en dehors des bornes minimales (par défaut 60s) et maximales (par défaut 3600s).",
    parameters={"min_secs": {"type": "number", "default": 60}, "max_secs": {"type": "number", "default": 3600}}
)
def headway_outliers(gtfs_data, min_secs=60, max_secs=3600, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier frequencies.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Détecte les valeurs aberrantes dans headway_secs (intervalles entre passages)."
            },
            "recommendations": ["Fournir un fichier frequencies.txt valide."]
        }
    
    # Détection des valeurs aberrantes
    low = df[df['headway_secs'] < min_secs]
    high = df[df['headway_secs'] > max_secs]
    valid = df[(df['headway_secs'] >= min_secs) & (df['headway_secs'] <= max_secs)]
    
    total = len(df)
    low_count = len(low)
    high_count = len(high)
    valid_count = len(valid)
    issues_count = low_count + high_count
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = compute_score(issues_count, total)
    
    # Statistiques des valeurs extrêmes
    extremes = {
        "min_headway": int(df['headway_secs'].min()) if total > 0 else None,
        "max_headway": int(df['headway_secs'].max()) if total > 0 else None,
        "mean_headway": round(df['headway_secs'].mean(), 1) if total > 0 else None
    }
    
    # Récupérer les trip_ids problématiques
    low_trip_ids = low['trip_id'].tolist() if 'trip_id' in low.columns else []
    high_trip_ids = high['trip_id'].tolist() if 'trip_id' in high.columns else []
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if low_count > 0:
        issues.append({
            "type": "suspicious_data",
            "field": "headway_secs",
            "category": "too_frequent",
            "count": low_count,
            "affected_ids": low_trip_ids,
            "threshold": min_secs,
            "message": f"{low_count} intervalles trop fréquents (< {min_secs}s)"
        })
    
    if high_count > 0:
        issues.append({
            "type": "suspicious_data", 
            "field": "headway_secs",
            "category": "too_rare",
            "count": high_count,
            "affected_ids": high_trip_ids,
            "threshold": max_secs,
            "message": f"{high_count} intervalles trop espacés (> {max_secs}s)"
        })
    
    # Status basé sur le score ← UTILISER TON SYSTÈME
    if score == 100:
        status = "success"
    elif score >= 80:  # Seuil pour warning car ce sont des "outliers" 
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_frequencies": total,
            "valid_headways": valid_count,
            "outliers_summary": {
                "too_frequent": low_count,
                "too_rare": high_count,
                "total_outliers": issues_count
            },
            "headway_statistics": extremes,
            "outlier_score": score,
            "compliance_rate": f"{valid_count}/{total} intervalles dans les limites"
        },
        "explanation": {
            "purpose": "Détecte les intervalles entre passages (headway_secs) qui sortent des plages opérationnelles habituelles.",
            "validation_thresholds": f"Plage acceptable: {min_secs}s - {max_secs}s ({min_secs//60}min - {max_secs//60}min)",
            "detection_logic": "Les valeurs trop faibles peuvent indiquer des erreurs de saisie, les valeurs trop élevées des services peu fréquents.",
            "data_overview": f"Analyse de {total} fréquences avec intervalles de {extremes['min_headway']}s à {extremes['max_headway']}s" if total > 0 else "Aucune donnée à analyser",
            "outlier_rate": f"{(issues_count / total * 100):.1f}% des intervalles sont hors limites" if total > 0 else "N/A"
        },
        "recommendations": [
            rec for rec in [
                f"Vérifier les {low_count} intervalles très courts (< {min_secs}s) - possibles erreurs de saisie." if low_count > 0 else None,
                f"Examiner les {high_count} intervalles très longs (> {max_secs}s) - services peu fréquents." if high_count > 0 else None,
                "Ajuster les seuils min_secs/max_secs selon le contexte opérationnel si nécessaire." if issues_count > 0 else None,
                "Considérer une validation des données à la source pour éviter les valeurs aberrantes." if issues_count > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="overlapping_intervals",
    genre="quality",
    description="Détecte des plages horaires qui se chevauchent pour un même trip_id.",
    parameters={}
)
def overlapping_intervals(gtfs_data, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier frequencies.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Détecte les chevauchements entre plages de fréquence d'un même voyage."
            },
            "recommendations": ["Fournir un fichier frequencies.txt valide."]
        }
    
    df = df.copy()
    # Utiliser ta fonction to_seconds depuis __init__.py
    df['start_sec'] = df['start_time'].apply(to_seconds)
    df['end_sec'] = df['end_time'].apply(to_seconds)

    trips_with_overlaps = []
    overlap_details = []
    
    for trip_id, group in df.groupby('trip_id'):
        sorted_group = group.sort_values('start_sec')
        trip_has_overlap = False
        
        for i in range(1, len(sorted_group)):
            current = sorted_group.iloc[i]
            previous = sorted_group.iloc[i-1]
            
            if current['start_sec'] < previous['end_sec']:
                if not trip_has_overlap:
                    trips_with_overlaps.append(trip_id)
                    trip_has_overlap = True
                
                # Détails du chevauchement pour debugging
                overlap_details.append({
                    'trip_id': trip_id,
                    'period_1': f"{previous['start_time']}-{previous['end_time']}",
                    'period_2': f"{current['start_time']}-{current['end_time']}",
                    'overlap_duration': previous['end_sec'] - current['start_sec']
                })

    count = len(trips_with_overlaps)
    total_trips = len(df['trip_id'].unique()) if 'trip_id' in df.columns else 0
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = compute_score(count, total_trips)
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if count > 0:
        issues.append({
            "type": "logical_error",
            "field": "frequency_periods",
            "count": count,
            "affected_ids": trips_with_overlaps,
            "details": overlap_details,
            "message": f"{count} voyages avec plages de fréquence qui se chevauchent"
        })
    
    # Status basé sur le score
    if score == 100:
        status = "success"
    elif score >= 90:  # Strict car chevauchement = erreur logique grave
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_trips": total_trips,
            "trips_with_overlaps": count,
            "clean_trips": total_trips - count,
            "overlap_score": score,
            "overlap_rate": f"{count}/{total_trips} voyages avec chevauchements",
            "total_overlapping_periods": len(overlap_details)
        },
        "explanation": {
            "purpose": "Détecte les plages de fréquence qui se chevauchent temporellement pour un même voyage (trip_id).",
            "validation_logic": "Pour chaque voyage, les plages de fréquence ne doivent pas se superposer dans le temps.",
            "detection_method": "Tri chronologique des plages par start_time et vérification que start_time[i] >= end_time[i-1]",
            "data_scope": f"Analyse de {total_trips} voyages distincts avec leurs plages de fréquence",
            "quality_assessment": f"Score de cohérence temporelle: {score}/100",
            "overlap_impact": "Les chevauchements peuvent créer des ambiguïtés dans les horaires de passage"
        },
        "recommendations": [
            rec for rec in [
                "Réviser les plages de fréquence pour éliminer tous les chevauchements temporels." if count > 0 else None,
                "Vérifier que les heures de fin d'une plage correspondent bien au début de la suivante." if count > 0 else None,
                "Considérer l'utilisation d'outils de validation GTFS pour detecter automatiquement ces conflits." if count > 0 else None,
                "Revoir le processus de saisie des données pour éviter les erreurs de plages temporelles." if count > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="invalid_exact_times",
    genre="validity",
    description="Vérifie que la colonne exact_times contient uniquement 0 ou 1.",
    parameters={}
)
def invalid_exact_times(gtfs_data, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier frequencies.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Vérifie que le champ exact_times contient uniquement les valeurs 0 ou 1."
            },
            "recommendations": ["Fournir un fichier frequencies.txt valide."]
        }
    
    # Vérification de la présence de la colonne
    if 'exact_times' not in df.columns:
        return {
            "status": "warning",
            "issues": [{
                "type": "missing_column",
                "field": "exact_times",
                "count": 0,
                "affected_ids": [],
                "message": "Colonne exact_times absente"
            }],
            "result": {
                "has_exact_times_column": False,
                "total_entries": len(df),
                "validation_score": 50  # Score partiel car colonne optionnelle
            },
            "explanation": {
                "purpose": "Vérifie que le champ exact_times contient uniquement les valeurs 0 ou 1.",
                "column_status": "La colonne exact_times est optionnelle mais recommandée pour préciser le type de fréquence.",
                "impact": "Sans exact_times, le comportement par défaut est exact_times=0 (horaires approximatifs)."
            },
            "recommendations": ["Ajouter la colonne exact_times (0=approximatif, 1=exact) si la précision des horaires est importante."]
        }

    # Validation des valeurs
    invalids = df[~df['exact_times'].isin([0, 1])]
    count_invalid = len(invalids)
    total = len(df)
    valid_count = total - count_invalid
    
    # Utiliser ta fonction _compute_score depuis __init__.py
    score = 100 if count_invalid == 0 else compute_score(count_invalid, total)
    
    # Récupérer les identifiants problématiques (index ou trip_id si disponible)
    invalid_ids = invalids['trip_id'].tolist() if 'trip_id' in invalids.columns else invalids.index.tolist()
    
    # Distribution des valeurs pour information
    value_distribution = df['exact_times'].value_counts().to_dict()
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if count_invalid > 0:
        issues.append({
            "type": "invalid_value",
            "field": "exact_times",
            "count": count_invalid,
            "affected_ids": invalid_ids,
            "expected_values": [0, 1],
            "message": f"{count_invalid} valeurs invalides dans exact_times (doivent être 0 ou 1)"
        })
    
    # Status basé sur le score
    if score == 100:
        status = "success"
    elif score >= 80:
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "has_exact_times_column": True,
            "total_entries": total,
            "valid_entries": valid_count,
            "invalid_entries": count_invalid,
            "validation_score": score,
            "value_distribution": value_distribution,
            "compliance_rate": f"{valid_count}/{total} valeurs conformes"
        },
        "explanation": {
            "purpose": "Vérifie que le champ exact_times respecte la spécification GTFS (valeurs 0 ou 1 uniquement).",
            "field_meaning": "exact_times=0: horaires approximatifs basés sur headway_secs | exact_times=1: horaires exacts selon stop_times.txt",
            "validation_rule": "Seules les valeurs 0 et 1 sont autorisées selon la spécification GTFS.",
            "data_overview": f"Analyse de {total} entrées de fréquence",
            "quality_assessment": f"Score de conformité: {score}/100",
            "value_breakdown": f"Distribution: {value_distribution}" if value_distribution else "Aucune valeur valide détectée"
        },
        "recommendations": [
            rec for rec in [
                "Corriger les valeurs de exact_times pour qu'elles soient 0 ou 1 uniquement." if count_invalid > 0 else None,
                "Vérifier que exact_times=1 est cohérent avec la présence de données détaillées dans stop_times.txt." if 1 in value_distribution else None,
                "Considérer l'utilisation d'exact_times=0 pour des services à fréquence régulière sans horaires fixes." if 0 not in value_distribution and count_invalid == 0 else None,
                "Valider que les valeurs de exact_times correspondent bien au type de service opéré." if count_invalid == 0 and len(value_distribution) > 1 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="frequencies_gap_analysis",
    genre="quality",
    description="Détecte les trous horaires non couverts entre intervalles pour un même trip_id.",
    parameters={}
)
def frequencies_gap_analysis(gtfs_data, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None:
        return {
            "status": "error",
            "issues": ["Fichier frequencies.txt manquant."],
            "result": {},  # ← summary → result, enlever score/problem_ids
            "explanation": {
                "purpose": "Analyse les interruptions de service entre les plages de fréquence d'un même voyage."
            },
            "recommendations": ["Fournir un fichier frequencies.txt valide."]
        }
    
    df = df.copy()
    # Utiliser tes fonctions depuis __init__.py
    df['start_sec'] = df['start_time'].apply(to_seconds)
    df['end_sec'] = df['end_time'].apply(to_seconds)

    gaps = []
    gap_threshold = params.get('gap_threshold_secs', 60)  # Paramétrable
    
    for trip_id, group in df.groupby('trip_id'):
        sorted_group = group.sort_values('start_sec')
        for i in range(1, len(sorted_group)):
            prev_end = sorted_group.iloc[i-1]['end_sec']
            curr_start = sorted_group.iloc[i]['start_sec']
            
            if prev_end is not None and curr_start is not None and curr_start > prev_end + gap_threshold:
                gap_duration = curr_start - prev_end
                gaps.append({
                    "trip_id": trip_id,
                    "gap_start": sorted_group.iloc[i-1]['end_time'],
                    "gap_end": sorted_group.iloc[i]['start_time'],
                    "gap_duration_secs": gap_duration,
                    "gap_duration_mins": round(gap_duration / 60, 1)
                })

    count = len(gaps)
    trips_with_gaps = len(set(g['trip_id'] for g in gaps))
    total_trips = len(df['trip_id'].unique()) if 'trip_id' in df.columns else 0
    
    # Calcul du score (ta logique existante)
    score = 100 if count == 0 else max(0, 100 - (count / total_trips * 100))
    
    # Statistiques des gaps
    gap_stats = {}
    if gaps:
        durations = [g['gap_duration_secs'] for g in gaps]
        gap_stats = {
            "min_gap_secs": min(durations),
            "max_gap_secs": max(durations),
            "avg_gap_secs": round(sum(durations) / len(durations), 1),
            "total_gap_time": sum(durations)
        }
    
    # Trip IDs avec gaps
    affected_trip_ids = list(set(g['trip_id'] for g in gaps))
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if count > 0:
        issues.append({
            "type": "service_gap",
            "field": "frequency_periods",
            "count": count,
            "affected_ids": affected_trip_ids,
            "details": gaps,
            "threshold": gap_threshold,
            "message": f"{count} interruptions de service détectées dans {trips_with_gaps} voyages"
        })
    
    # Status basé sur l'impact
    if count == 0:
        status = "success"
    elif trips_with_gaps / total_trips <= 0.1:  # ≤ 10% des trips affectés
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_trips": total_trips,
            "trips_with_gaps": trips_with_gaps,
            "trips_without_gaps": total_trips - trips_with_gaps,
            "gap_analysis": {
                "total_gaps": count,
                "gap_threshold_secs": gap_threshold,
                "gap_statistics": gap_stats
            },
            "continuity_score": score,
            "service_continuity_rate": f"{total_trips - trips_with_gaps}/{total_trips} voyages sans interruption"
        },
        "explanation": {
            "purpose": "Détecte les interruptions de service (gaps) entre plages de fréquence consécutives d'un même voyage.",
            "detection_method": f"Un gap est détecté si l'écart entre la fin d'une plage et le début de la suivante dépasse {gap_threshold}s ({gap_threshold//60}min)",
            "impact_assessment": "Les gaps peuvent créer des périodes sans service, affectant la continuité de l'offre de transport",
            "data_scope": f"Analyse de {total_trips} voyages avec leurs plages de fréquence",
            "quality_metrics": f"Score de continuité: {score}/100",
            "gap_overview": f"Durée totale des interruptions: {gap_stats.get('total_gap_time', 0)//60}min" if gap_stats else "Aucune interruption détectée"
        },
        "recommendations": [
            rec for rec in [
                f"Combler les {count} interruptions de service pour améliorer la continuité." if count > 0 else None,
                f"Prioriser les voyages avec les plus longues interruptions (max: {gap_stats.get('max_gap_secs', 0)//60}min)." if gap_stats and gap_stats.get('max_gap_secs', 0) > 600 else None,
                "Vérifier si les gaps correspondent à des pauses opérationnelles intentionnelles." if count > 0 else None,
                "Ajuster le seuil de détection si certains gaps sont acceptables opérationnellement." if count > 0 else None,
                f"Considérer l'ajout de plages de fréquence pour couvrir les {gap_stats.get('total_gap_time', 0)//3600}h d'interruption totale." if gap_stats and gap_stats.get('total_gap_time', 0) > 3600 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="duplicate_frequencies_intervals",
    genre="quality",
    description="Détecte doublons exacts dans frequencies.txt (trip_id, start_time, end_time, headway_secs).",
    parameters={}
)
def duplicate_frequencies_intervals(gtfs_data, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None or df.empty:
        return {
            "status": "success",  # ← ok → success
            "issues": [],
            "result": {  # ← summary → result, enlever score/problem_ids
                "total_entries": 0,
                "duplicate_count": 0,
                "uniqueness_score": 100
            },
            "explanation": {
                "purpose": "Détecte les doublons exacts dans les définitions de fréquence.",
                "data_status": "Aucune donnée à analyser (fichier vide ou absent)."
            },
            "recommendations": []
        }
    
    # Détection des doublons exacts
    duplicated = df.duplicated(subset=['trip_id', 'start_time', 'end_time', 'headway_secs'], keep=False)
    dup_count = int(duplicated.sum())
    total_entries = len(df)
    unique_entries = total_entries - dup_count
    
    # Calcul du score (ta logique existante)
    score = 100 if dup_count == 0 else max(0, 100 - (dup_count / total_entries * 100))
    
    # Analyse détaillée des doublons
    duplicate_groups = []
    if dup_count > 0:
        dup_df = df[duplicated]
        # Grouper les doublons identiques
        for group_key, group in dup_df.groupby(['trip_id', 'start_time', 'end_time', 'headway_secs']):
            if len(group) > 1:
                duplicate_groups.append({
                    'trip_id': group_key[0],
                    'start_time': group_key[1],
                    'end_time': group_key[2], 
                    'headway_secs': group_key[3],
                    'occurrence_count': len(group),
                    'row_indices': group.index.tolist()
                })
    
    # Trip IDs affectés
    affected_trip_ids = df[duplicated]['trip_id'].unique().tolist() if 'trip_id' in df.columns and dup_count > 0 else []
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if dup_count > 0:
        issues.append({
            "type": "duplicate_data",
            "field": "frequency_definition",
            "count": dup_count,
            "affected_ids": affected_trip_ids,
            "details": duplicate_groups,
            "duplicate_criteria": ['trip_id', 'start_time', 'end_time', 'headway_secs'],
            "message": f"{dup_count} doublons exacts détectés dans {len(duplicate_groups)} groupes"
        })
    
    # Status basé sur l'impact
    if dup_count == 0:
        status = "success"
    elif dup_count / total_entries <= 0.05:  # ≤ 5% de doublons
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_entries": total_entries,
            "unique_entries": unique_entries,
            "duplicate_entries": dup_count,
            "uniqueness_score": score,
            "duplicate_analysis": {
                "duplicate_groups": len(duplicate_groups),
                "affected_trips": len(affected_trip_ids),
                "duplication_rate": f"{dup_count}/{total_entries} entrées dupliquées"
            }
        },
        "explanation": {
            "purpose": "Détecte les définitions de fréquence identiques qui peuvent créer des redondances ou des conflits.",
            "detection_criteria": "Doublons basés sur: trip_id, start_time, end_time, headway_secs (combinaison exacte)",
            "data_quality_impact": "Les doublons peuvent créer des ambiguïtés dans l'interprétation des fréquences de service",
            "analysis_scope": f"Analyse de {total_entries} définitions de fréquence",
            "uniqueness_assessment": f"Score d'unicité: {score}/100",
            "duplication_overview": f"Taux de duplication: {(dup_count/total_entries*100):.1f}%" if total_entries > 0 else "N/A"
        },
        "recommendations": [
            rec for rec in [
                f"Supprimer les {dup_count} doublons pour éviter les redondances de définition." if dup_count > 0 else None,
                f"Examiner les {len(duplicate_groups)} groupes de doublons pour identifier les causes de duplication." if len(duplicate_groups) > 1 else None,
                "Mettre en place des contraintes d'unicité lors de la création des données de fréquence." if dup_count > 0 else None,
                "Vérifier que les doublons ne résultent pas d'erreurs d'import ou de fusion de données." if dup_count > 0 else None,
                f"Prioriser le nettoyage des voyages les plus affectés: {', '.join(map(str, affected_trip_ids[:5]))}" if len(affected_trip_ids) > 5 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="frequencies_count_by_trip",
    genre='statistics',
    description="Nombre d’intervalles de fréquence par trip_id."
)
def frequencies_count_by_trip(gtfs_data, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None or df.empty:
        return {
            "status": "warning",  # ← Ajouter status
            "issues": [{
                "type": "no_data",
                "field": "frequencies",
                "count": 0,
                "affected_ids": [],
                "message": "Aucune donnée de fréquence disponible"
            }],
            "result": {  # ← Nouvelle structure
                "total_trips": 0,
                "average_intervals_per_trip": 0,
                "max_intervals_per_trip": 0,
                "min_intervals_per_trip": 0,
                "distribution_analysis": {}
            },
            "explanation": {
                "purpose": "Analyse la distribution des plages de fréquence par voyage.",
                "data_status": "Aucune donnée dans frequencies.txt pour calculer les statistiques."
            },
            "recommendations": ["Ajouter des définitions de fréquence si nécessaire pour le service."]
        }
    
    # Calcul des statistiques par trip
    counts = df.groupby('trip_id').size()
    total_trips = counts.shape[0]
    avg_intervals = counts.mean()
    max_intervals = counts.max()
    min_intervals = counts.min()
    
    # Distribution détaillée
    distribution = counts.value_counts().sort_index().to_dict()
    
    # Identifica des trips avec beaucoup/peu de plages
    trips_many_intervals = counts[counts >= 5].index.tolist()  # ≥ 5 plages
    trips_single_interval = counts[counts == 1].index.tolist()  # 1 seule plage
    
    # Issues pour signaler les cas particuliers
    issues = []
    if len(trips_many_intervals) > 0:
        issues.append({
            "type": "data_complexity",
            "field": "frequency_intervals",
            "count": len(trips_many_intervals),
            "affected_ids": trips_many_intervals,
            "threshold": 5,
            "message": f"{len(trips_many_intervals)} voyages avec nombreuses plages de fréquence (≥5)"
        })
    
    # Status basé sur la complexité
    if max_intervals <= 3 and total_trips > 0:
        status = "success"  # Distribution simple
    elif max_intervals <= 6:
        status = "warning"  # Complexité modérée
    else:
        status = "error"     # Très complexe, possibles erreurs
    
    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_trips": total_trips,
            "average_intervals_per_trip": round(avg_intervals, 2),
            "max_intervals_per_trip": int(max_intervals),
            "min_intervals_per_trip": int(min_intervals),
            "distribution_analysis": {
                "intervals_distribution": distribution,
                "trips_with_single_interval": len(trips_single_interval),
                "trips_with_many_intervals": len(trips_many_intervals),
                "complexity_indicators": {
                    "std_deviation": round(counts.std(), 2),
                    "median_intervals": int(counts.median())
                }
            }
        },
        "explanation": {
            "purpose": "Analyse la distribution des plages de fréquence par voyage pour identifier les patterns et anomalies.",
            "statistical_summary": f"{total_trips} voyages avec fréquences définies. Moyenne: {avg_intervals:.2f} plages/voyage",
            "distribution_insight": f"Répartition: min={min_intervals}, médiane={int(counts.median())}, max={max_intervals} plages par voyage",
            "complexity_assessment": "Distribution simple" if max_intervals <= 3 else "Distribution complexe" if max_intervals > 6 else "Distribution modérée",
            "operational_context": "Les voyages avec nombreuses plages peuvent indiquer des Services à fréquence variable ou des erreurs de modélisation"
        },
        "recommendations": [
            rec for rec in [
                f"Examiner les {len(trips_many_intervals)} voyages avec ≥5 plages de fréquence pour vérifier la cohérence." if len(trips_many_intervals) > 0 else None,
                "Considérer la simplification des définitions de fréquence si la complexité est excessive." if max_intervals > 8 else None,
                f"Vérifier que les {len(trips_single_interval)} voyages à plage unique ne nécessitent pas de fréquences variables." if len(trips_single_interval) > total_trips * 0.8 else None,
                "Documenter les raisons operationnelles des voyages à fréquences multiples." if len(trips_many_intervals) > 0 else None,
                "Utiliser des outils de visualisation pour analyser les patterns temporels complexes." if max_intervals > 6 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="headway_seconds_stats",
    genre='statistics',
    description="Statistiques sur la distribution de headway_secs."
)
def headway_seconds_stats(gtfs_data, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None or df.empty or 'headway_secs' not in df.columns:
        return {
            "status": "error",  # ← Ajouter status
            "issues": [{
                "type": "missing_data",
                "field": "headway_secs",
                "count": 0,
                "affected_ids": [],
                "message": "Colonne headway_secs manquante ou aucune donnée disponible"
            }],
            "result": {  # ← Nouvelle structure
                "count": 0,
                "statistics": {},
                "distribution_analysis": {},
                "quality_indicators": {}
            },
            "explanation": {
                "purpose": "Analyse statistique des intervalles entre passages (headway_secs).",
                "data_status": "Aucune donnée headway_secs disponible dans frequencies.txt."
            },
            "recommendations": ["Ajouter des valeurs headway_secs pour définir les fréquences de service."]
        }
    
    headways = df['headway_secs'].dropna()  # Enlever les valeurs manquantes
    count = len(headways)
    
    if count == 0:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_data",
                "field": "headway_secs",
                "count": 0,
                "affected_ids": [],
                "message": "Toutes les valeurs headway_secs sont manquantes"
            }],
            "result": {"count": 0, "statistics": {}, "distribution_analysis": {}, "quality_indicators": {}},
            "explanation": {"purpose": "Analyse statistique des intervalles entre passages.", "data_status": "Toutes les valeurs sont manquantes."},
            "recommendations": ["Corriger les valeurs manquantes dans headway_secs."]
        }
    
    # Calculs statistiques
    mean_val = headways.mean()
    median_val = headways.median()
    min_val = headways.min()
    max_val = headways.max()
    std_dev_val = headways.std()
    
    # Quartiles et percentiles
    q1 = headways.quantile(0.25)
    q3 = headways.quantile(0.75)
    p90 = headways.quantile(0.90)
    p10 = headways.quantile(0.10)
    
    # Distribution par plages
    ranges = {
        "very_frequent": (headways < 300).sum(),      # < 5min
        "frequent": ((headways >= 300) & (headways < 900)).sum(),  # 5-15min
        "moderate": ((headways >= 900) & (headways < 1800)).sum(), # 15-30min
        "infrequent": (headways >= 1800).sum()       # > 30min
    }
    
    # Détection d'anomalies (outliers)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    outliers = headways[(headways < lower_bound) | (headways > upper_bound)]
    outlier_count = len(outliers)
    
    # Issues pour les anomalies
    issues = []
    if outlier_count > 0:
        issues.append({
            "type": "statistical_outlier",
            "field": "headway_secs",
            "count": outlier_count,
            "affected_ids": [],  # Pourrait être enrichi avec les indices
            "bounds": {"lower": round(lower_bound, 1), "upper": round(upper_bound, 1)},
            "message": f"{outlier_count} valeurs aberrantes détectées (méthode IQR)"
        })
    
    # Coefficient de variation pour évaluer la dispersion
    cv = (std_dev_val / mean_val) * 100 if mean_val > 0 else 0
    
    # Status basé sur la cohérence des données
    if cv <= 30 and outlier_count == 0:
        status = "success"  # Distribution homogène
    elif cv <= 60 or outlier_count <= count * 0.05:
        status = "warning"   # Dispersion modérée
    else:
        status = "error"     # Très dispersé, possibles erreurs
    
    return {
        "status": status,
        "issues": issues,
        "result": {
            "count": count,
            "statistics": {
                "mean_headway": round(mean_val, 1),
                "median_headway": round(median_val, 1),
                "min_headway": int(min_val),
                "max_headway": int(max_val),
                "std_dev_headway": round(std_dev_val, 1),
                "quartiles": {
                    "q1": round(q1, 1),
                    "q3": round(q3, 1)
                },
                "percentiles": {
                    "p10": round(p10, 1),
                    "p90": round(p90, 1)
                }
            },
            "distribution_analysis": {
                "frequency_ranges": ranges,
                "coefficient_variation": round(cv, 1),
                "outlier_analysis": {
                    "outlier_count": outlier_count,
                    "outlier_percentage": round((outlier_count/count)*100, 1) if count > 0 else 0,
                    "detection_bounds": {"lower": round(lower_bound, 1), "upper": round(upper_bound, 1)}
                }
            },
            "quality_indicators": {
                "data_consistency": "Élevée" if cv <= 30 else "Modérée" if cv <= 60 else "Faible",
                "outlier_impact": "Négligeable" if outlier_count <= count * 0.01 else "Modéré" if outlier_count <= count * 0.05 else "Élevé"
            }
        },
        "explanation": {
            "purpose": "Analyse statistique complète des intervalles entre passages pour évaluer la cohérence des fréquences de service.",
            "data_overview": f"Analyse de {count} valeurs headway_secs avec une moyenne de {mean_val:.1f}s ({mean_val/60:.1f}min)",
            "distribution_summary": f"Médiane: {median_val:.0f}s, plage: {min_val}s-{max_val}s, écart-type: {std_dev_val:.1f}s",
            "variability_assessment": f"Coefficient de variation: {cv:.1f}% ({'faible' if cv <= 30 else 'modérée' if cv <= 60 else 'élevée'} dispersion)",
            "service_patterns": f"Services: {ranges['very_frequent']} très fréquents, {ranges['frequent']} fréquents, {ranges['moderate']} modérés, {ranges['infrequent']} peu fréquents"
        },
        "recommendations": [
            rec for rec in [
                f"Examiner les {outlier_count} valeurs aberrantes pour détecter d'éventuelles erreurs de saisie." if outlier_count > 0 else None,
                "Considérer une standardisation des intervalles pour réduire la dispersion." if cv > 60 else None,
                f"Vérifier la cohérence opérationnelle des intervalles extrêmes ({min_val}s - {max_val}s)." if max_val > min_val * 10 else None,
                "Documenter les raisons des variations importantes d'intervalles entre services." if cv > 30 else None,
                f"Optimiser les {ranges['infrequent']} services peu fréquents (>30min) si possible." if ranges['infrequent'] > 0 else None
            ] if rec is not None
        ]
    }


@audit_function(
    file_type="frequencies",
    name="time_coverage_stats",
    genre='statistics',
    description="Synthèse de la couverture temporelle (min et max start/end times) dans frequencies.txt."
)
def time_coverage_stats(gtfs_data, **params):
    df = gtfs_data.get('frequencies')  # ← enlever .txt
    if df is None or df.empty:
        return {
            "status": "warning",  # ← Ajouter status
            "issues": [{
                "type": "no_data",
                "field": "frequencies",
                "count": 0,
                "affected_ids": [],
                "message": "Aucune donnée de fréquence pour analyser la couverture temporelle"
            }],
            "result": {  # ← Nouvelle structure
                "earliest_start_time": None,
                "latest_end_time": None,
                "duration_seconds": 0,
                "coverage_analysis": {},
                "service_periods": []
            },
            "explanation": {
                "purpose": "Analyse la couverture temporelle des services à fréquence définie.",
                "data_status": "Pas de données frequencies.txt pour analyser la couverture temporelle."
            },
            "recommendations": ["Ajouter des définitions de fréquence pour analyser la couverture de service."]
        }
    
    # Utiliser ta fonction to_seconds depuis __init__.py
    start_secs = df['start_time'].map(to_seconds).dropna()
    end_secs = df['end_time'].map(to_seconds).dropna()
    
    if start_secs.empty or end_secs.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "invalid_data",
                "field": "start_time/end_time",
                "count": len(df) - len(start_secs) - len(end_secs),
                "affected_ids": [],
                "message": "Valeurs temporelles non exploitables"
            }],
            "result": {
                "earliest_start_time": None,
                "latest_end_time": None,
                "duration_seconds": 0,
                "coverage_analysis": {},
                "service_periods": []
            },
            "explanation": {
                "purpose": "Analyse la couverture temporelle des services à fréquence définie.",
                "data_status": "Valeurs non exploitables dans start_time ou end_time."
            },
            "recommendations": ["Corriger les formats des champs temporels start_time et end_time."]
        }

    earliest = min(start_secs)
    latest = max(end_secs)
    duration = latest - earliest if latest >= earliest else 0

    # Analyse des périodes de service
    service_periods = []
    for _, row in df.iterrows():
        start_sec = to_seconds(row['start_time'])
        end_sec = to_seconds(row['end_time'])
        if start_sec is not None and end_sec is not None:
            service_periods.append({
                'trip_id': row.get('trip_id', 'unknown'),
                'start_time': row['start_time'],
                'end_time': row['end_time'],
                'start_seconds': start_sec,
                'end_seconds': end_sec,
                'duration_seconds': end_sec - start_sec if end_sec >= start_sec else 0
            })
    
    # Analyse des créneaux horaires
    time_ranges = {
        "early_morning": sum(1 for p in service_periods if p['start_seconds'] < 6*3600),  # < 6h
        "morning": sum(1 for p in service_periods if 6*3600 <= p['start_seconds'] < 12*3600),  # 6h-12h
        "afternoon": sum(1 for p in service_periods if 12*3600 <= p['start_seconds'] < 18*3600),  # 12h-18h
        "evening": sum(1 for p in service_periods if 18*3600 <= p['start_seconds'] < 22*3600),  # 18h-22h
        "late_night": sum(1 for p in service_periods if p['start_seconds'] >= 22*3600)  # >= 22h
    }
    
    # Détection de gaps dans la couverture
    gaps = []
    sorted_periods = sorted(service_periods, key=lambda x: x['start_seconds'])
    for i in range(1, len(sorted_periods)):
        prev_end = sorted_periods[i-1]['end_seconds']
        curr_start = sorted_periods[i]['start_seconds']
        if curr_start > prev_end + 300:  # Gap > 5min
            gap_duration = curr_start - prev_end
            gaps.append({
                'gap_start': seconds_to_hhmmss(prev_end),
                'gap_end': seconds_to_hhmmss(curr_start),
                'gap_duration_seconds': gap_duration,
                'gap_duration_formatted': f"{gap_duration//3600}h{(gap_duration%3600)//60}min"
            })
    
    # Issues pour les problèmes de couverture
    issues = []
    if len(gaps) > 0:
        issues.append({
            "type": "service_gap",
            "field": "time_coverage",
            "count": len(gaps),
            "affected_ids": [],
            "details": gaps,
            "message": f"{len(gaps)} interruptions dans la couverture temporelle"
        })
    
    if duration < 12*3600:  # Moins de 12h de service
        issues.append({
            "type": "limited_coverage",
            "field": "service_duration",
            "count": 1,
            "affected_ids": [],
            "threshold": 12*3600,
            "message": f"Couverture limitée: {duration//3600}h{(duration%3600)//60}min"
        })
    
    # Status basé sur la qualité de la couverture
    if duration >= 16*3600 and len(gaps) == 0:
        status = "success"  # Excellente couverture
    elif duration >= 12*3600 and len(gaps) <= 2:
        status = "warning"  # Couverture correcte
    else:
        status = "error"    # Couverture insuffisante
    
    return {
        "status": status,
        "issues": issues,
        "result": {
            "earliest_start_time": seconds_to_hhmmss(earliest),
            "latest_end_time": seconds_to_hhmmss(latest),
            "duration_seconds": duration,
            "duration_formatted": f"{duration//3600}h{(duration%3600)//60}min",
            "coverage_analysis": {
                "total_service_periods": len(service_periods),
                "time_range_distribution": time_ranges,
                "coverage_gaps": len(gaps),
                "coverage_percentage": round((duration / (24*3600)) * 100, 1) if duration > 0 else 0
            },
            "service_periods": service_periods[:10]  # Limiter pour l'affichage
        },
        "explanation": {
            "purpose": "Analyse la couverture temporelle globale des services définis par fréquence.",
            "coverage_summary": f"Service de {seconds_to_hhmmss(earliest)} à {seconds_to_hhmmss(latest)} ({duration//3600}h{(duration%3600)//60}min)",
            "distribution_analysis": f"Répartition: {time_ranges['morning']} créneaux matinées, {time_ranges['afternoon']} après-midi, {time_ranges['evening']} soirées",
            "continuity_assessment": f"Continuité: {len(gaps)} interruptions détectées" if len(gaps) > 0 else "Service continu sans interruptions",
            "coverage_quality": f"Couverture {round((duration/(24*3600))*100, 1)}% de la journée" if duration > 0 else "Aucune couverture"
        },
        "recommendations": [
            rec for rec in [
                f"Combler les {len(gaps)} interruptions de service pour améliorer la continuité." if len(gaps) > 0 else None,
                "Étendre les heures de service pour améliorer la couverture journalière." if duration < 16*3600 else None,
                f"Ajouter des services en début de matinée (avant 6h)." if time_ranges['early_morning'] == 0 and duration > 8*3600 else None,
                f"Considérer des services tardifs (après 22h)." if time_ranges['late_night'] == 0 and duration > 12*3600 else None,
                "Optimiser la répartition des créneaux horaires selon la demande." if max(time_ranges.values()) > min(time_ranges.values()) * 3 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="frequencies",
    name="frequencies_no_time_overlap",
    genre='quality',
    description="Vérifie qu'il n'y a pas de chevauchement des intervalles horaires dans frequencies.txt par trip_id.",
    parameters={}
)
def frequencies_no_time_overlap(gtfs_data, **params):
   """
   Valide l'absence de chevauchement temporel dans les fréquences par trip_id
   """
   df = gtfs_data.get('frequencies.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "frequencies.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier frequencies.txt est requis pour valider les chevauchements temporels"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Valide l'absence de chevauchement temporel dans les fréquences pour assurer la cohérence opérationnelle."
           },
           "recommendations": ["Fournir le fichier frequencies.txt pour analyser les intervalles de fréquence."]
       }

   total_frequencies = len(df)
   total_trips = df['trip_id'].nunique()
   
   # Vérification des colonnes requises
   required_columns = ['trip_id', 'start_time', 'end_time', 'headway_secs']
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
                   "message": f"Colonnes obligatoires manquantes dans frequencies.txt: {', '.join(missing_columns)}"
               }
           ],
           "result": {
               "total_frequencies": total_frequencies,
               "total_trips": total_trips,
               "missing_columns": missing_columns
           },
           "explanation": {
               "purpose": "Valide l'absence de chevauchement temporel dans les fréquences pour assurer la cohérence opérationnelle",
               "context": "Colonnes obligatoires manquantes pour l'analyse des intervalles",
               "impact": "Impossible de valider les chevauchements temporels"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
       }

   # Analyse des chevauchements par trip_id
   overlapping_trips = set()
   invalid_time_trips = set()
   processing_errors = []
   overlap_details = []

   for trip_id, group in df.groupby('trip_id'):
       try:
           intervals = []
           has_invalid_times = False
           
           for _, row in group.iterrows():
               start_sec = time_to_seconds(row['start_time'])
               end_sec = time_to_seconds(row['end_time'])
               
               # Validation des horaires
               if start_sec is None or end_sec is None:
                   invalid_time_trips.add(trip_id)
                   has_invalid_times = True
                   break
               elif start_sec >= end_sec:
                   invalid_time_trips.add(trip_id)
                   has_invalid_times = True
                   break
               
               intervals.append((start_sec, end_sec, row.name))  # Ajouter l'index pour traçabilité
           
           if has_invalid_times:
               continue
           
           # Tri des intervalles par heure de début
           intervals.sort(key=lambda x: x[0])
           
           # Détection des chevauchements
           for i in range(len(intervals) - 1):
               current_end = intervals[i][1]
               next_start = intervals[i+1][0]
               
               if current_end > next_start:
                   overlapping_trips.add(trip_id)
                   overlap_details.append({
                       "trip_id": trip_id,
                       "interval_1": f"{seconds_to_time(intervals[i][0])}-{seconds_to_time(intervals[i][1])}",
                       "interval_2": f"{seconds_to_time(intervals[i+1][0])}-{seconds_to_time(intervals[i+1][1])}",
                       "overlap_duration": current_end - next_start
                   })
                   break
                   
       except Exception as e:
           processing_errors.append(trip_id)

   # Calcul des métriques
   total_problematic_trips = len(overlapping_trips) + len(invalid_time_trips)
   overlap_rate = round(len(overlapping_trips) / total_trips * 100, 2) if total_trips > 0 else 0
   error_rate = round(total_problematic_trips / total_trips * 100, 2) if total_trips > 0 else 0

   # Détermination du statut
   if total_problematic_trips == 0 and not processing_errors:
       status = "success"
   elif overlap_rate <= 5:  # ≤5% de trips avec chevauchement
       status = "warning"
   else:
       status = "error"

   # Construction des issues
   issues = []
   
   if overlapping_trips:
       issues.append({
           "type": "time_overlap",
           "field": "frequency_intervals",
           "count": len(overlapping_trips),
           "affected_ids": list(overlapping_trips)[:100],
           "message": f"{len(overlapping_trips)} trips ont des intervalles de fréquence qui se chevauchent"
       })
   
   if invalid_time_trips:
       issues.append({
           "type": "invalid_time_range",
           "field": "start_time/end_time",
           "count": len(invalid_time_trips),
           "affected_ids": list(invalid_time_trips)[:100],
           "message": f"{len(invalid_time_trips)} trips ont des intervalles temporels invalides (start_time ≥ end_time ou format incorrect)"
       })
   
   if processing_errors:
       issues.append({
           "type": "processing_error",
           "field": "frequency_analysis",
           "count": len(processing_errors),
           "affected_ids": processing_errors[:50],
           "message": f"{len(processing_errors)} trips n'ont pas pu être analysés (erreurs de traitement)"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_frequencies": total_frequencies,
           "total_trips": total_trips,
           "overlapping_trips": len(overlapping_trips),
           "invalid_time_trips": len(invalid_time_trips),
           "valid_trips": total_trips - total_problematic_trips,
           "overlap_rate": overlap_rate,
           "validation_metrics": {
               "trips_analyzed": total_trips - len(processing_errors),
               "processing_errors": len(processing_errors),
               "analysis_coverage": round((total_trips - len(processing_errors)) / total_trips * 100, 2) if total_trips > 0 else 0
           },
           "overlap_analysis": {
               "overlap_details": overlap_details[:10],  # Top 10 exemples
               "avg_overlap_duration": round(sum(d["overlap_duration"] for d in overlap_details) / len(overlap_details), 2) if overlap_details else 0,
               "max_overlap_duration": max(d["overlap_duration"] for d in overlap_details) if overlap_details else 0
           },
           "operational_impact": {
               "service_conflicts": len(overlapping_trips),
               "data_reliability": (
                   "high" if error_rate < 5
                   else "medium" if error_rate < 15
                   else "low"
               ),
               "scheduling_integrity": "valid" if len(overlapping_trips) == 0 else "compromised"
           }
       },
       "explanation": {
           "purpose": "Valide l'absence de chevauchement temporel dans les fréquences pour assurer la cohérence des services",
           "validation_method": "Analyse des intervalles start_time/end_time par trip_id pour détecter les conflits temporels",
           "context": f"Analyse de {total_frequencies} fréquences sur {total_trips} trips distincts",
           "overlap_summary": f"Taux de chevauchement: {overlap_rate}% ({len(overlapping_trips)} trips concernés)",
           "impact": (
               f"Intervalles de fréquence cohérents pour tous les {total_trips} trips" if status == "success"
               else f"Conflits temporels détectés : {len(overlapping_trips)} chevauchements, {len(invalid_time_trips)} intervalles invalides"
           )
       },
       "recommendations": [
           rec for rec in [
               f"URGENT: Corriger {len(overlapping_trips)} trips avec chevauchements temporels" if overlapping_trips else None,
               f"Valider {len(invalid_time_trips)} trips avec intervalles temporels invalides (start_time ≥ end_time)" if invalid_time_trips else None,
               f"Traiter {len(processing_errors)} trips avec erreurs d'analyse" if processing_errors else None,
               "Implémenter une validation des intervalles dans votre processus de génération frequencies.txt" if overlap_rate > 5 else None,
               "Espacer les intervalles de fréquence pour éviter les conflits opérationnels" if len(overlap_details) > 0 else None,
               "Maintenir cette qualité de données temporelles pour assurer la fiabilité des services" if status == "success" else None
           ] if rec is not None
       ]
   }

# Fonctions utilitaires nécessaires
def time_to_seconds(time_str):
   """Convertit un horaire HH:MM:SS en secondes"""
   try:
       if pd.isna(time_str):
           return None
       parts = str(time_str).split(':')
       if len(parts) != 3:
           return None
       hours, minutes, seconds = map(int, parts)
       return hours * 3600 + minutes * 60 + seconds
   except:
       return None

def seconds_to_time(seconds):
   """Convertit des secondes en format HH:MM:SS"""
   try:
       hours = seconds // 3600
       minutes = (seconds % 3600) // 60
       secs = seconds % 60
       return f"{hours:02d}:{minutes:02d}:{secs:02d}"
   except:
       return "00:00:00"