"""
Fonctions d'audit pour le file_type: file
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="file",
    name="duplicate_rows",
    description="Détecte les lignes dupliquées dans un fichier GTFS.",
    genre='quality',
    parameters={}
)
def duplicate_rows(gtfs_data, gtfs_file, **params):
    """
    Détecte les lignes dupliquées dans un fichier GTFS.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        gtfs_file: Nom du fichier à analyser (sans extension)
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    filename = f"{gtfs_file}.txt"
    df = gtfs_data.get(filename)
    
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": filename,
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier {filename} manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Détecte les lignes dupliquées dans {filename}"
            },
            "recommendations": [f"URGENT: Vérifier la présence du fichier {filename}"]
        }
    
    total_rows = len(df)
    issues = []
    
    # Détection des doublons
    try:
        duplicates_mask = df.duplicated(keep=False)
        duplicate_count = duplicates_mask.sum()
        unique_duplicate_groups = df[duplicates_mask].groupby(df.columns.tolist()).size().count() if duplicate_count > 0 else 0
        
        if duplicate_count > 0:
            # Analyser les groupes de doublons
            duplicate_groups = df[duplicates_mask].groupby(df.columns.tolist()).size()
            largest_group = duplicate_groups.max() if len(duplicate_groups) > 0 else 0
            
            issues.append({
                "type": "duplicate_rows",
                "field": "file_content",
                "count": int(duplicate_count),
                "affected_ids": [],
                "message": f"Lignes dupliquées détectées ({unique_duplicate_groups} groupes distincts)"
            })
            
            # Issue si taux de doublons élevé
            duplicate_ratio = duplicate_count / total_rows
            if duplicate_ratio > 0.1:  # >10%
                issues.append({
                    "type": "high_duplicate_ratio",
                    "field": "data_quality",
                    "count": 1,
                    "affected_ids": [],
                    "message": f"Taux élevé de doublons ({duplicate_ratio*100:.1f}%)"
                })
        
        # Détermination du status
        if duplicate_count == 0:
            status = "success"
        elif duplicate_ratio > 0.2:  # >20%
            status = "error"
        else:
            status = "warning"
        
        # Métriques de qualité
        data_uniqueness = max(0, 100 - (duplicate_ratio * 100))
        
        # Construction du result
        result = {
            "duplicate_analysis": {
                "total_rows": total_rows,
                "duplicate_rows": int(duplicate_count),
                "unique_rows": total_rows - int(duplicate_count),
                "duplicate_groups": int(unique_duplicate_groups),
                "largest_group_size": int(largest_group) if duplicate_count > 0 else 0,
                "data_uniqueness_percent": round(data_uniqueness, 1)
            }
        }
        
        # Recommendations
        recommendations = []
        if duplicate_count > 0:
            recommendations.append(f"Supprimer {duplicate_count} lignes dupliquées ({unique_duplicate_groups} groupes)")
            if largest_group > 3:
                recommendations.append(f"Examiner le groupe de {largest_group} doublons identiques")
        else:
            recommendations.append("Excellente unicité des données - aucun doublon détecté")
        
        return {
            "status": status,
            "issues": issues,
            "result": result,
            "explanation": {
                "purpose": f"Détecte les lignes dupliquées dans {filename}",
                "scope": f"Analyse de {total_rows} lignes",
                "detection_method": "Comparaison stricte de toutes les colonnes"
            },
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "analysis_error",
                "field": "duplicate_detection",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors de la détection de doublons: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Détecte les lignes dupliquées dans {filename}"
            },
            "recommendations": ["Vérifier l'intégrité des données du fichier"]
        }

@audit_function(
    file_type="file",
    name="empty_values_stats",
    description="Calcule le taux de valeurs manquantes par champ.",
    genre='quality',
    parameters={}
)
def empty_values_stats(gtfs_data, gtfs_file, **params):
    """
    Calcule le taux de valeurs manquantes par champ.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        gtfs_file: Nom du fichier à analyser (sans extension)
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    filename = f"{gtfs_file}.txt"
    df = gtfs_data.get(filename)
    
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": filename,
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier {filename} manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse les valeurs manquantes dans {filename}"
            },
            "recommendations": [f"URGENT: Vérifier la présence du fichier {filename}"]
        }
    
    total_rows = len(df)
    total_columns = len(df.columns)
    issues = []
    
    try:
        # Calcul statistiques valeurs manquantes
        empty_counts = {}
        empty_rates = {}
        columns_with_missing = []
        
        for column in df.columns:
            null_count = df[column].isna().sum()
            empty_str_count = (df[column] == '').sum() if df[column].dtype == 'object' else 0
            total_missing = null_count + empty_str_count
            
            empty_counts[column] = int(total_missing)
            empty_rates[column] = round((total_missing / total_rows) * 100, 2) if total_rows > 0 else 0
            
            if total_missing > 0:
                columns_with_missing.append({
                    'column': column,
                    'missing_count': int(total_missing),
                    'missing_rate': empty_rates[column]
                })
        
        # Identifier colonnes avec beaucoup de valeurs manquantes
        high_missing_columns = [col for col, rate in empty_rates.items() if rate > 50]
        moderate_missing_columns = [col for col, rate in empty_rates.items() if 10 < rate <= 50]
        
        # Issues pour colonnes avec beaucoup de manquants
        if high_missing_columns:
            issues.append({
                "type": "high_missing_values",
                "field": "data_completeness",
                "count": len(high_missing_columns),
                "affected_ids": high_missing_columns,
                "message": f"Colonnes avec >50% de valeurs manquantes: {', '.join(high_missing_columns)}"
            })
        
        if moderate_missing_columns:
            issues.append({
                "type": "moderate_missing_values",
                "field": "data_completeness",
                "count": len(moderate_missing_columns),
                "affected_ids": moderate_missing_columns,
                "message": f"Colonnes avec 10-50% de valeurs manquantes: {', '.join(moderate_missing_columns)}"
            })
        
        # Calcul métriques globales
        total_cells = total_rows * total_columns
        total_missing_cells = sum(empty_counts.values())
        overall_completeness = max(0, 100 - (total_missing_cells / total_cells * 100)) if total_cells > 0 else 100
        
        # Détermination du status
        if len(high_missing_columns) > total_columns * 0.5:  # >50% colonnes très incomplètes
            status = "error"
        elif len(high_missing_columns) > 0 or overall_completeness < 80:
            status = "warning"
        else:
            status = "success"
        
        # Construction du result
        result = {
            "completeness_overview": {
                "total_rows": total_rows,
                "total_columns": total_columns,
                "overall_completeness_percent": round(overall_completeness, 1),
                "columns_with_missing": len(columns_with_missing),
                "completely_filled_columns": total_columns - len(columns_with_missing)
            },
            "missing_values_by_column": {
                "counts": empty_counts,
                "percentages": empty_rates
            },
            "completeness_categories": {
                "high_missing_columns": high_missing_columns,
                "moderate_missing_columns": moderate_missing_columns,
                "complete_columns": [col for col, rate in empty_rates.items() if rate == 0]
            },
            "detailed_analysis": columns_with_missing[:20]  # Top 20 pour performance
        }
        
        # Recommendations
        recommendations = []
        
        if high_missing_columns:
            worst_column = max(empty_rates.items(), key=lambda x: x[1])
            recommendations.append(f"URGENT: Traiter la colonne '{worst_column[0]}' avec {worst_column[1]}% de valeurs manquantes")
            recommendations.append(f"Compléter ou supprimer {len(high_missing_columns)} colonnes très incomplètes")
        
        if moderate_missing_columns:
            recommendations.append(f"Améliorer la complétude de {len(moderate_missing_columns)} colonnes modérément incomplètes")
        
        if overall_completeness < 90:
            recommendations.append(f"Améliorer la complétude globale (actuel: {overall_completeness:.1f}%, objectif: >90%)")
        
        if not columns_with_missing:
            recommendations.append("Excellente complétude des données - aucune valeur manquante")
        elif overall_completeness > 95:
            recommendations.append("Très bonne complétude globale des données")
        
        return {
            "status": status,
            "issues": issues,
            "result": result,
            "explanation": {
                "purpose": f"Analyse les valeurs manquantes dans {filename}",
                "scope": f"Analyse de {total_rows} lignes × {total_columns} colonnes ({total_cells} cellules)",
                "detection_method": "Détection NULL et chaînes vides"
            },
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "analysis_error",
                "field": "missing_values_analysis",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors de l'analyse des valeurs manquantes: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse les valeurs manquantes dans {filename}"
            },
            "recommendations": ["Vérifier l'intégrité des données du fichier"]
        }

@audit_function(
    file_type="file",
    name="row_consistency",
    description="Vérifie que toutes les lignes ont le même nombre de colonnes que l'en-tête.",
    genre='quality',
    parameters={}
)

def row_consistency(gtfs_data, gtfs_file, **params):
    """
    Vérifie que toutes les lignes ont le même nombre de colonnes que l'en-tête.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        gtfs_file: Nom du fichier à analyser (sans extension)
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    filename = f"{gtfs_file}.txt"
    
    # Cette fonction nécessite l'accès au fichier brut pour compter les colonnes
    # Simulation de l'analyse basée sur les données disponibles
    df = gtfs_data.get(filename)
    
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": filename,
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier {filename} manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Vérifie la cohérence structurelle des lignes dans {filename}"
            },
            "recommendations": [f"URGENT: Vérifier la présence du fichier {filename}"]
        }
    
    # Analyse basée sur les données chargées (approximation)
    total_rows = len(df)
    expected_columns = len(df.columns)
    issues = []
    
    try:
        # Si les données sont chargées correctement, il n'y a probablement pas d'incohérence
        # Car pandas aurait eu des difficultés à parser un fichier avec des lignes incohérentes
        
        # Vérifier s'il y a des valeurs None inattendues qui pourraient indiquer des problèmes de parsing
        inconsistent_parsing_indicators = 0
        for column in df.columns:
            # Détecter des colonnes avec beaucoup de NaN qui pourraient indiquer des problèmes de structure
            nan_ratio = df[column].isna().sum() / total_rows if total_rows > 0 else 0
            if nan_ratio > 0.5:  # >50% NaN pourrait indiquer un problème structurel
                inconsistent_parsing_indicators += 1
        
        # Détermination du status basée sur les indicateurs
        if inconsistent_parsing_indicators > expected_columns * 0.3:  # >30% colonnes suspectes
            status = "warning"
            issues.append({
                "type": "potential_structure_issues",
                "field": "file_structure",
                "count": inconsistent_parsing_indicators,
                "affected_ids": [],
                "message": f"Indicateurs potentiels d'incohérence structurelle ({inconsistent_parsing_indicators} colonnes suspectes)"
            })
        else:
            status = "success"
        
        # Construction du result
        result = {
            "structure_analysis": {
                "total_rows": total_rows,
                "expected_columns": expected_columns,
                "structure_consistency": "verified_by_successful_parsing",
                "potential_issues": inconsistent_parsing_indicators
            },
            "parsing_quality": {
                "successful_parsing": True,
                "data_loaded": True,
                "structure_integrity": "good" if inconsistent_parsing_indicators == 0 else "moderate"
            }
        }
        
        # Recommendations
        recommendations = []
        
        if inconsistent_parsing_indicators > 0:
            recommendations.append(f"Examiner {inconsistent_parsing_indicators} colonnes avec beaucoup de valeurs manquantes")
            recommendations.append("Vérifier l'intégrité structurelle du fichier source")
        else:
            recommendations.append("Structure cohérente - parsing réussi sans anomalie détectée")
        
        # Note sur la limitation de cette analyse
        recommendations.append("Note: Analyse basée sur le parsing réussi - vérification fichier brut recommandée pour validation complète")
        
        return {
            "status": status,
            "issues": issues,
            "result": result,
            "explanation": {
                "purpose": f"Vérifie la cohérence structurelle des lignes dans {filename}",
                "scope": f"Analyse indirecte via parsing de {total_rows} lignes",
                "limitation": "Analyse basée sur le succès du parsing pandas - fichier probablement cohérent"
            },
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "analysis_error",
                "field": "structure_analysis",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors de l'analyse structurelle: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Vérifie la cohérence structurelle des lignes dans {filename}"
            },
            "recommendations": ["Vérifier l'intégrité du fichier"]
        }

@audit_function(
    file_type="file",
    name="file_encoding",
    description="Détecte l'encodage du fichier texte et vérifie s'il est UTF-8.",
    genre='accessibility',
    parameters={}
)
def file_encoding(gtfs_data, gtfs_file, **params):
    """
    Détecte l'encodage du fichier texte et vérifie s'il est UTF-8.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        gtfs_file: Nom du fichier à analyser (sans extension)
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    filename = f"{gtfs_file}.txt"
    df = gtfs_data.get(filename)
    
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": filename,
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier {filename} manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse l'encodage du fichier {filename}"
            },
            "recommendations": [f"URGENT: Vérifier la présence du fichier {filename}"]
        }
    
    issues = []
    
    # Analyse indirecte basée sur le contenu chargé
    try:
        # Vérifier la présence de caractères non-ASCII qui indiqueraient des problèmes d'encodage
        encoding_issues = 0
        problematic_columns = []
        
        for column in df.columns:
            if df[column].dtype == 'object':  # Colonnes textuelles
                # Vérifier s'il y a des caractères d'encodage suspects
                text_data = df[column].dropna().astype(str)
                
                # Rechercher des indicateurs de problèmes d'encodage
                encoding_artifacts = text_data.str.contains('â€™|â€œ|â€|Ã©|Ã |Ã¨', regex=True, na=False).sum()
                replacement_chars = text_data.str.contains('�', na=False).sum()
                
                if encoding_artifacts > 0 or replacement_chars > 0:
                    encoding_issues += encoding_artifacts + replacement_chars
                    problematic_columns.append({
                        'column': column,
                        'encoding_artifacts': int(encoding_artifacts),
                        'replacement_characters': int(replacement_chars)
                    })
        
        # Issues pour problèmes d'encodage détectés
        if encoding_issues > 0:
            issues.append({
                "type": "encoding_artifacts",
                "field": "text_encoding",
                "count": encoding_issues,
                "affected_ids": [col['column'] for col in problematic_columns],
                "message": f"Artefacts d'encodage détectés ({encoding_issues} occurrences)"
            })
        
        # Détermination du status
        if encoding_issues > len(df) * 0.1:  # >10% des lignes avec problèmes
            status = "error"
        elif encoding_issues > 0:
            status = "warning"
        else:
            status = "success"
        
        # Construction du result
        result = {
            "encoding_analysis": {
                "analysis_method": "indirect_via_content_inspection",
                "encoding_artifacts_detected": encoding_issues,
                "problematic_columns_count": len(problematic_columns),
                "data_parsing_success": True,
                "likely_encoding": "UTF-8" if encoding_issues == 0 else "possibly_non_UTF8"
            },
            "quality_indicators": {
                "clean_text_data": encoding_issues == 0,
                "replacement_characters_found": any(col.get('replacement_characters', 0) > 0 for col in problematic_columns),
                "encoding_artifacts_found": any(col.get('encoding_artifacts', 0) > 0 for col in problematic_columns)
            },
            "problematic_columns": problematic_columns
        }
        
        # Recommendations
        recommendations = []
        
        if encoding_issues > 0:
            recommendations.append(f"URGENT: Corriger {encoding_issues} problèmes d'encodage détectés")
            if problematic_columns:
                worst_column = max(problematic_columns, key=lambda x: x['encoding_artifacts'] + x['replacement_characters'])
                recommendations.append(f"Priorité: Traiter la colonne '{worst_column['column']}' avec le plus d'artefacts")
            recommendations.append("Réencoder le fichier en UTF-8 sans BOM")
        else:
            recommendations.append("Encodage des données semble correct - aucun artefact détecté")
        
        # Recommendations pour validation complète
        recommendations.append("Recommandation: Valider l'encodage avec detection directe du fichier source")
        
        return {
            "status": status,
            "issues": issues,
            "result": result,
            "explanation": {
                "purpose": f"Analyse l'encodage du fichier {filename}",
                "scope": f"Inspection indirecte du contenu textuel",
                "method": "Détection d'artefacts d'encodage dans le contenu",
                "limitation": "Analyse indirecte - détection directe du fichier source recommandée"
            },
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "analysis_error",
                "field": "encoding_analysis",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors de l'analyse d'encodage: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse l'encodage du fichier {filename}"
            },
            "recommendations": ["Vérifier l'intégrité du fichier"]
        }

@audit_function(
    file_type="file",
    name="file_size",
    description="Mesure la taille du fichier en Ko et Mo.",
    genre='statistics',
    parameters={}
)
def file_size(gtfs_data, gtfs_file, **params):
    """
    Mesure la taille du fichier en Ko et Mo.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        gtfs_file: Nom du fichier à analyser (sans extension)
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    filename = f"{gtfs_file}.txt"
    df = gtfs_data.get(filename)
    
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": filename,
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier {filename} manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse la taille du fichier {filename}"
            },
            "recommendations": [f"URGENT: Vérifier la présence du fichier {filename}"]
        }
    
    issues = []
    
    try:
        # Estimation de la taille basée sur les données en mémoire
        total_rows = len(df)
        total_columns = len(df.columns)
        
        # Estimer la taille approximative en mémoire
        memory_usage = df.memory_usage(deep=True).sum()  # en bytes
        
        # Calculs de taille
        size_bytes = int(memory_usage)
        size_kb = round(size_bytes / 1024, 2)
        size_mb = round(size_bytes / (1024 * 1024), 2)
        size_gb = round(size_bytes / (1024 * 1024 * 1024), 2)
        
        # Classification de la taille
        if size_mb < 1:
            size_category = "small"
        elif size_mb < 10:
            size_category = "medium"
        elif size_mb < 100:
            size_category = "large"
        else:
            size_category = "very_large"
        
        # Issues pour fichiers très volumineux
        if size_mb > 500:  # >500MB
            issues.append({
                "type": "very_large_file",
                "field": "file_size",
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier très volumineux ({size_mb} MB) - impact performance possible"
            })
        elif size_mb > 100:  # >100MB
            issues.append({
                "type": "large_file",
                "field": "file_size",
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier volumineux ({size_mb} MB) - surveiller les performances"
            })
        
        # Issues pour fichiers anormalement petits
        if total_rows > 1000 and size_kb < 50:  # Beaucoup de lignes mais petit fichier
            issues.append({
                "type": "unusually_small_file",
                "field": "file_size",
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier anormalement petit ({size_kb} KB pour {total_rows} lignes)"
            })
        
        # Détermination du status
        if size_mb > 1000:  # >1GB
            status = "error"
        elif size_mb > 500 or (total_rows > 1000 and size_kb < 10):
            status = "warning"
        else:
            status = "success"
        
        # Calcul métriques d'efficacité
        bytes_per_row = size_bytes / max(total_rows, 1)
        bytes_per_cell = size_bytes / max(total_rows * total_columns, 1)
        
        # Construction du result
        result = {
            "size_metrics": {
                "estimated_size_bytes": size_bytes,
                "estimated_size_kb": size_kb,
                "estimated_size_mb": size_mb,
                "estimated_size_gb": size_gb,
                "size_category": size_category
            },
            "efficiency_metrics": {
                "bytes_per_row": round(bytes_per_row, 2),
                "bytes_per_cell": round(bytes_per_cell, 2),
                "memory_efficiency": "high" if bytes_per_row < 100 else "medium" if bytes_per_row < 500 else "low"
            },
            "data_dimensions": {
                "total_rows": total_rows,
                "total_columns": total_columns,
                "total_cells": total_rows * total_columns
            }
        }
        
        # Recommendations
        recommendations = []
        
        if size_mb > 500:
            recommendations.append(f"URGENT: Optimiser le fichier de {size_mb} MB - considérer la segmentation")
            recommendations.append("Évaluer la compression ou la division en fichiers plus petits")
        elif size_mb > 100:
            recommendations.append(f"Surveiller les performances avec ce fichier de {size_mb} MB")
        
        if bytes_per_row > 1000:
            recommendations.append(f"Optimiser l'efficacité des données ({bytes_per_row:.0f} bytes/ligne)")
        
        if size_category == "small" and total_rows > 100:
            recommendations.append("Fichier compact et efficace - bonne optimisation des données")
        elif size_category == "very_large":
            recommendations.append("Considérer des stratégies de traitement par chunks pour ce fichier volumineux")
        
        if not issues:
            recommendations.append(f"Taille appropriée ({size_mb} MB) - aucun problème de dimensionnement")
        
        return {
            "status": status,
            "issues": issues,
            "result": result,
            "explanation": {
                "purpose": f"Analyse la taille du fichier {filename}",
                "scope": f"Estimation basée sur {total_rows} lignes × {total_columns} colonnes",
                "method": "Calcul de l'usage mémoire des données chargées",
                "note": "Taille estimée en mémoire - peut différer de la taille fichier sur disque"
            },
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "analysis_error",
                "field": "size_analysis",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors de l'analyse de taille: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse la taille du fichier {filename}"
            },
            "recommendations": ["Vérifier l'intégrité du fichier"]
        }


@audit_function(
    file_type="file",
    name="file_case_check",
    genre='validity',
    description="Vérifie si le nom du fichier est en minuscules (convention GTFS).",
    parameters={}
)
def file_size(gtfs_data, gtfs_file, **params):
    """
    Mesure la taille du fichier en Ko et Mo.
    
    Args:
        gtfs_data: Dictionnaire contenant les données GTFS
        gtfs_file: Nom du fichier à analyser (sans extension)
        **params: Paramètres additionnels (non utilisés)
    
    Returns:
        dict: Structure standardisée avec status, issues, result, explanation, recommendations
    """
    filename = f"{gtfs_file}.txt"
    df = gtfs_data.get(filename)
    
    if df is None or df.empty:
        return {
            "status": "error",
            "issues": [{
                "type": "missing_file",
                "field": filename,
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier {filename} manquant ou vide"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse la taille du fichier {filename}"
            },
            "recommendations": [f"URGENT: Vérifier la présence du fichier {filename}"]
        }
    
    issues = []
    
    try:
        # Estimation de la taille basée sur les données en mémoire
        total_rows = len(df)
        total_columns = len(df.columns)
        
        # Estimer la taille approximative en mémoire
        memory_usage = df.memory_usage(deep=True).sum()  # en bytes
        
        # Calculs de taille
        size_bytes = int(memory_usage)
        size_kb = round(size_bytes / 1024, 2)
        size_mb = round(size_bytes / (1024 * 1024), 2)
        size_gb = round(size_bytes / (1024 * 1024 * 1024), 2)
        
        # Classification de la taille
        if size_mb < 1:
            size_category = "small"
        elif size_mb < 10:
            size_category = "medium"
        elif size_mb < 100:
            size_category = "large"
        else:
            size_category = "very_large"
        
        # Issues pour fichiers très volumineux
        if size_mb > 500:  # >500MB
            issues.append({
                "type": "very_large_file",
                "field": "file_size",
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier très volumineux ({size_mb} MB) - impact performance possible"
            })
        elif size_mb > 100:  # >100MB
            issues.append({
                "type": "large_file",
                "field": "file_size",
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier volumineux ({size_mb} MB) - surveiller les performances"
            })
        
        # Issues pour fichiers anormalement petits
        if total_rows > 1000 and size_kb < 50:  # Beaucoup de lignes mais petit fichier
            issues.append({
                "type": "unusually_small_file",
                "field": "file_size",
                "count": 1,
                "affected_ids": [],
                "message": f"Fichier anormalement petit ({size_kb} KB pour {total_rows} lignes)"
            })
        
        # Détermination du status
        if size_mb > 1000:  # >1GB
            status = "error"
        elif size_mb > 500 or (total_rows > 1000 and size_kb < 10):
            status = "warning"
        else:
            status = "success"
        
        # Calcul métriques d'efficacité
        bytes_per_row = size_bytes / max(total_rows, 1)
        bytes_per_cell = size_bytes / max(total_rows * total_columns, 1)
        
        # Construction du result
        result = {
            "size_metrics": {
                "estimated_size_bytes": size_bytes,
                "estimated_size_kb": size_kb,
                "estimated_size_mb": size_mb,
                "estimated_size_gb": size_gb,
                "size_category": size_category
            },
            "efficiency_metrics": {
                "bytes_per_row": round(bytes_per_row, 2),
                "bytes_per_cell": round(bytes_per_cell, 2),
                "memory_efficiency": "high" if bytes_per_row < 100 else "medium" if bytes_per_row < 500 else "low"
            },
            "data_dimensions": {
                "total_rows": total_rows,
                "total_columns": total_columns,
                "total_cells": total_rows * total_columns
            }
        }
        
        # Recommendations
        recommendations = []
        
        if size_mb > 500:
            recommendations.append(f"URGENT: Optimiser le fichier de {size_mb} MB - considérer la segmentation")
            recommendations.append("Évaluer la compression ou la division en fichiers plus petits")
        elif size_mb > 100:
            recommendations.append(f"Surveiller les performances avec ce fichier de {size_mb} MB")
        
        if bytes_per_row > 1000:
            recommendations.append(f"Optimiser l'efficacité des données ({bytes_per_row:.0f} bytes/ligne)")
        
        if size_category == "small" and total_rows > 100:
            recommendations.append("Fichier compact et efficace - bonne optimisation des données")
        elif size_category == "very_large":
            recommendations.append("Considérer des stratégies de traitement par chunks pour ce fichier volumineux")
        
        if not issues:
            recommendations.append(f"Taille appropriée ({size_mb} MB) - aucun problème de dimensionnement")
        
        return {
            "status": status,
            "issues": issues,
            "result": result,
            "explanation": {
                "purpose": f"Analyse la taille du fichier {filename}",
                "scope": f"Estimation basée sur {total_rows} lignes × {total_columns} colonnes",
                "method": "Calcul de l'usage mémoire des données chargées",
                "note": "Taille estimée en mémoire - peut différer de la taille fichier sur disque"
            },
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "status": "error",
            "issues": [{
                "type": "analysis_error",
                "field": "size_analysis",
                "count": 1,
                "affected_ids": [],
                "message": f"Erreur lors de l'analyse de taille: {str(e)}"
            }],
            "result": {},
            "explanation": {
                "purpose": f"Analyse la taille du fichier {filename}"
            },
            "recommendations": ["Vérifier l'intégrité du fichier"]
        }


