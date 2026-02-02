"""
Fonctions d'audit pour le file_type: agency
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="tarifs",
    name="duplicate_fare_attributes",
    genre="validity",
    description="Détecte des fare_attributes dupliqués sur tarif, devise, transfert, etc.",
    parameters={}
)
def duplicate_fare_attributes(gtfs_data, **params):
    df = gtfs_data.get('fare_attributes')  # ← Déjà sans .txt, OK
    total = len(df) if df is not None else 0

    if df is None or df.empty:
        return {
            "status": "success",  # ← ok → success
            "issues": [],
            "result": {  # ← Nouvelle structure, enlever score
                "total_fare_attributes": 0,
                "duplicate_count": 0,
                "uniqueness_score": 100,
                "duplicate_analysis": {}
            },
            "explanation": {
                "purpose": "Détecte les doublons dans les attributs tarifaires (fare_attributes).",
                "data_status": "Aucun fare_attributes dans les données."
            },
            "recommendations": []
        }

    # Colonnes à considérer pour la détection de doublons
    subset_cols = ['price', 'currency_type', 'payment_method', 'transfers', 'transfer_duration']
    available_cols = [col for col in subset_cols if col in df.columns]
    
    if not available_cols:
        return {
            "status": "warning",
            "issues": [{
                "type": "missing_column",
                "field": "fare_comparison_fields",
                "count": len(subset_cols),
                "affected_ids": [],
                "message": "Colonnes nécessaires pour la comparaison manquantes"
            }],
            "result": {
                "total_fare_attributes": total,
                "duplicate_count": 0,
                "uniqueness_score": 0,
                "duplicate_analysis": {"available_columns": available_cols}
            },
            "explanation": {
                "purpose": "Détecte les doublons dans les attributs tarifaires.",
                "validation_status": "Impossible de détecter les doublons sans colonnes de comparaison."
            },
            "recommendations": ["Ajouter les colonnes standard fare_attributes selon la spécification GTFS."]
        }

    # Détection des doublons
    duplicated_mask = df.duplicated(subset=available_cols, keep=False)
    duplicates_df = df[duplicated_mask]
    count = len(duplicates_df)
    unique_count = total - count
    
    # Utiliser ta fonction _compute_score depuis __init__.py (correction du nom)
    score = 100 if count == 0 else compute_score(count, total)
    
    # Analyse détaillée des groupes de doublons
    duplicate_groups = []
    if count > 0:
        # Grouper les doublons identiques
        for group_values, group_df in duplicates_df.groupby(available_cols):
            if len(group_df) > 1:
                fare_ids = group_df.index.tolist()  # ou group_df['fare_id'] si la colonne existe
                duplicate_groups.append({
                    'duplicate_values': dict(zip(available_cols, group_values)),
                    'occurrence_count': len(group_df),
                    'fare_indices': fare_ids,
                    'records': group_df.to_dict(orient='records')
                })
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if count > 0:
        issues.append({
            "type": "duplicate_data",
            "field": "fare_attributes",
            "count": count,
            "affected_ids": duplicates_df.index.tolist(),
            "details": duplicate_groups,
            "comparison_fields": available_cols,
            "message": f"{count} attributs tarifaires dupliqués dans {len(duplicate_groups)} groupes"
        })
    
    # Status basé sur l'impact
    if count == 0:
        status = "success"
    elif count / total <= 0.1:  # ≤ 10% de doublons
        status = "warning"
    else:
        status = "error"

    return {
        "status": status,
        "issues": issues,
        "result": {  # ← Nouvelle structure
            "total_fare_attributes": total,
            "unique_fare_attributes": unique_count,
            "duplicate_count": count,
            "uniqueness_score": score,
            "duplicate_analysis": {
                "duplicate_groups": len(duplicate_groups),
                "comparison_fields": available_cols,
                "duplication_rate": f"{count}/{total} attributs dupliqués" if total > 0 else "N/A",
                "most_duplicated": duplicate_groups[0] if duplicate_groups else None
            }
        },
        "explanation": {
            "purpose": "Identifie les attributs tarifaires dupliqués qui peuvent créer des redondances dans la structure tarifaire.",
            "detection_method": f"Comparaison basée sur: {', '.join(available_cols)}",
            "duplication_impact": "Les doublons peuvent compliquer la gestion tarifaire et créer des incohérences",
            "data_overview": f"Analyse de {total} attributs tarifaires",
            "quality_assessment": f"Score d'unicité: {score}/100"
        },
        "recommendations": [
            rec for rec in [
                f"Supprimer ou fusionner les {count} attributs tarifaires dupliqués pour simplifier la structure." if count > 0 else None,
                f"Examiner les {len(duplicate_groups)} groupes de doublons pour identifier les redondances." if len(duplicate_groups) > 1 else None,
                "Mettre en place des contraintes d'unicité lors de la création des données tarifaires." if count > 0 else None,
                "Vérifier que les doublons ne résultent pas d'erreurs d'import ou de versions multiples." if count > 0 else None,
                f"Prioriser le nettoyage du groupe le plus dupliqué: {duplicate_groups[0]['occurrence_count']} occurrences" if duplicate_groups and duplicate_groups[0]['occurrence_count'] > 2 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="tarifs",
    name="fare_attributes_unused",
    genre='cross-validation',
    description="Détecte les fare_attributes non utilisés dans fare_rules.",
    parameters={}
)
def fare_attributes_unused(gtfs_data, **params):
   """
   Identifie les fare_attributes non référencés dans fare_rules pour optimiser les données tarifaires
   """
   fare_attributes_df = gtfs_data.get('fare_attributes.txt')
   fare_rules_df = gtfs_data.get('fare_rules.txt')

   # Cas 1: Pas de fare_attributes
   if fare_attributes_df is None or fare_attributes_df.empty:
       return {
           "status": "success",
           "issues": [],
           "result": {
               "total_fare_attributes": 0,
               "unused_fare_attributes": 0,
               "used_fare_attributes": 0,
               "usage_rate": 0.0,
               "unused_fare_ids": []
           },
           "explanation": {
               "purpose": "Identifie les fare_attributes non référencés dans fare_rules pour optimiser les données tarifaires",
               "context": "Aucun fare_attributes défini dans le système",
               "impact": "Pas d'analyse nécessaire - système sans tarification"
           },
           "recommendations": []
       }

   total_fare_attributes = len(fare_attributes_df)
   
   # Vérification de la colonne fare_id
   if 'fare_id' not in fare_attributes_df.columns:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "fare_id",
                   "count": total_fare_attributes,
                   "affected_ids": [],
                   "message": "La colonne fare_id est obligatoire dans fare_attributes.txt"
               }
           ],
           "result": {
               "total_fare_attributes": total_fare_attributes,
               "unused_fare_attributes": 0,
               "used_fare_attributes": 0,
               "usage_rate": 0.0
           },
           "explanation": {
               "purpose": "Identifie les fare_attributes non référencés dans fare_rules pour optimiser les données tarifaires",
               "context": "Colonne fare_id obligatoire manquante",
               "impact": "Impossible d'analyser l'utilisation des fare_attributes"
           },
           "recommendations": ["Ajouter la colonne fare_id obligatoire dans fare_attributes.txt"]
       }

   # Cas 2: Pas de fare_rules - tous les fare_attributes sont inutilisés
   if fare_rules_df is None or fare_rules_df.empty:
       unused_fare_ids = fare_attributes_df['fare_id'].tolist()
       return {
           "status": "warning",
           "issues": [
               {
                   "type": "unused_data",
                   "field": "fare_attributes",
                   "count": total_fare_attributes,
                   "affected_ids": unused_fare_ids[:100],
                   "message": f"Tous les {total_fare_attributes} fare_attributes sont inutilisés (fare_rules.txt absent)"
               }
           ],
           "result": {
               "total_fare_attributes": total_fare_attributes,
               "unused_fare_attributes": total_fare_attributes,
               "used_fare_attributes": 0,
               "usage_rate": 0.0,
               "unused_fare_ids": unused_fare_ids,
               "orphaned_analysis": {
                   "all_orphaned": True,
                   "reason": "fare_rules.txt absent"
               }
           },
           "explanation": {
               "purpose": "Identifie les fare_attributes non référencés dans fare_rules pour optimiser les données tarifaires",
               "context": f"{total_fare_attributes} fare_attributes définis mais aucun fare_rules pour les référencer",
               "impact": "Données tarifaires définies mais inapplicables sans règles d'association"
           },
           "recommendations": [
               "Créer le fichier fare_rules.txt pour activer le système tarifaire",
               f"Associer les {total_fare_attributes} fare_attributes aux routes/zones appropriées"
           ]
       }

   # Vérification de la colonne fare_id dans fare_rules
   if 'fare_id' not in fare_rules_df.columns:
       return {
           "status": "warning", 
           "issues": [
               {
                   "type": "missing_field",
                   "field": "fare_id",
                   "count": len(fare_rules_df),
                   "affected_ids": [],
                   "message": "La colonne fare_id est manquante dans fare_rules.txt"
               }
           ],
           "result": {
               "total_fare_attributes": total_fare_attributes,
               "unused_fare_attributes": total_fare_attributes,
               "used_fare_attributes": 0,
               "usage_rate": 0.0,
               "unused_fare_ids": fare_attributes_df['fare_id'].tolist()
           },
           "explanation": {
               "purpose": "Identifie les fare_attributes non référencés dans fare_rules pour optimiser les données tarifaires",
               "context": "Colonne fare_id manquante dans fare_rules.txt",
               "impact": "Impossible de lier les fare_attributes aux règles tarifaires"
           },
           "recommendations": ["Ajouter la colonne fare_id dans fare_rules.txt pour référencer les tarifs"]
       }

   # Analyse des références
   used_fare_ids = set(fare_rules_df['fare_id'].dropna().unique())
   all_fare_ids = set(fare_attributes_df['fare_id'].dropna().unique())
   
   unused_fare_ids = all_fare_ids - used_fare_ids
   used_count = len(used_fare_ids)
   unused_count = len(unused_fare_ids)
   usage_rate = round(used_count / total_fare_attributes * 100, 2) if total_fare_attributes > 0 else 0

   # Analyse des tarifs non référencés dans fare_attributes
   orphaned_fare_rules = used_fare_ids - all_fare_ids
   
   # Détermination du statut
   if unused_count == 0 and len(orphaned_fare_rules) == 0:
       status = "success"
   elif unused_count <= total_fare_attributes * 0.1:  # ≤10% inutilisés
       status = "warning"
   else:
       status = "error"

   # Construction des issues
   issues = []
   
   if unused_count > 0:
       issues.append({
           "type": "unused_data",
           "field": "fare_attributes",
           "count": unused_count,
           "affected_ids": list(unused_fare_ids)[:100],
           "message": f"{unused_count} fare_attributes ne sont référencés dans aucune règle tarifaire"
       })
   
   if orphaned_fare_rules:
       issues.append({
           "type": "missing_reference",
           "field": "fare_rules",
           "count": len(orphaned_fare_rules),
           "affected_ids": list(orphaned_fare_rules)[:50],
           "message": f"{len(orphaned_fare_rules)} fare_id dans fare_rules n'existent pas dans fare_attributes"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_fare_attributes": total_fare_attributes,
           "unused_fare_attributes": unused_count,
           "used_fare_attributes": used_count,
           "usage_rate": usage_rate,
           "unused_fare_ids": list(unused_fare_ids),
           "reference_analysis": {
               "valid_references": len(used_fare_ids & all_fare_ids),
               "orphaned_rules": len(orphaned_fare_rules),
               "orphaned_fare_ids": list(orphaned_fare_rules)[:20]
           },
           "optimization_potential": {
               "removable_fare_attributes": unused_count,
               "data_efficiency": round((used_count / total_fare_attributes) * 100, 2) if total_fare_attributes > 0 else 0,
               "cleanup_impact": f"{unused_count} fare_attributes supprimables"
           }
       },
       "explanation": {
           "purpose": "Identifie les fare_attributes non référencés dans fare_rules pour optimiser les données tarifaires et détecter les incohérences",
           "context": f"Analyse de {total_fare_attributes} fare_attributes avec {len(fare_rules_df)} règles tarifaires",
           "usage_analysis": f"Taux d'utilisation: {usage_rate}% ({used_count}/{total_fare_attributes} fare_attributes utilisés)",
           "reference_integrity": f"Intégrité référentielle: {len(orphaned_fare_rules)} règles orphelines détectées" if orphaned_fare_rules else "Références cohérentes",
           "impact": (
               f"Système tarifaire optimisé avec tous les fare_attributes utilisés" if status == "success"
               else f"Optimisation possible : {unused_count} fare_attributes inutilisés, {len(orphaned_fare_rules)} références invalides"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Supprimer {unused_count} fare_attributes inutilisés pour optimiser les données" if unused_count > 0 else None,
               f"Corriger {len(orphaned_fare_rules)} fare_id invalides dans fare_rules.txt" if orphaned_fare_rules else None,
               "Créer des règles tarifaires pour utiliser les fare_attributes définis" if unused_count > used_count else None,
               "Vérifier que les fare_attributes inutilisés ne correspondent pas à de futurs tarifs" if unused_count > 0 else None,
               "Auditer régulièrement la cohérence entre fare_attributes et fare_rules" if len(orphaned_fare_rules) > 0 else None,
               "Maintenir cette efficacité du système tarifaire" if status == "success" else None
           ] if rec is not None
       ]
   }