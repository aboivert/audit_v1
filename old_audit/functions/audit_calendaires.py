"""
Fonctions d'audit pour le file_type: agency
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="calendaires",
    name="validate_feed_info_dates",
    genre='validity',
    description="Vérifie la présence et validité des dates start_date et end_date dans feed_info.txt."
)
def validate_feed_info_dates(gtfs_data, **params):
   """
   Valide les dates de validité du feed GTFS dans feed_info.txt
   """
   if gtfs_data.get('feed_info.txt') is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "feed_info.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier feed_info.txt est obligatoire pour définir la période de validité du feed"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Valide les dates de validité du feed GTFS pour assurer la cohérence temporelle des données."
           },
           "recommendations": ["Créer le fichier feed_info.txt avec les dates de validité du feed GTFS."]
       }
   
   if df.empty:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "empty_file",
                   "field": "feed_info.txt",
                   "count": 0,
                   "affected_ids": [],
                   "message": "Le fichier feed_info.txt est vide"
               }
           ],
           "result": {
               "feed_entries": 0,
               "date_validation": "impossible"
           },
           "explanation": {
               "purpose": "Valide les dates de validité du feed GTFS pour assurer la cohérence temporelle des données",
               "context": "Fichier feed_info.txt vide",
               "impact": "Aucune information de validité temporelle disponible"
           },
           "recommendations": ["Ajouter une entrée dans feed_info.txt avec start_date et end_date."]
       }

   # Vérification des colonnes requises
   required_columns = ['feed_start_date', 'feed_end_date']
   missing_columns = [col for col in required_columns if col not in df.columns]
   
   # Vérification alternative avec start_date/end_date (format parfois utilisé)
   if missing_columns and ('start_date' in df.columns and 'end_date' in df.columns):
       required_columns = ['start_date', 'end_date']
       missing_columns = []
   
   if missing_columns:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "date_columns",
                   "count": len(missing_columns),
                   "affected_ids": [],
                   "message": f"Colonnes de dates manquantes dans feed_info.txt: {', '.join(missing_columns)}"
               }
           ],
           "result": {
               "feed_entries": len(df),
               "available_columns": list(df.columns),
               "missing_columns": missing_columns
           },
           "explanation": {
               "purpose": "Valide les dates de validité du feed GTFS pour assurer la cohérence temporelle des données",
               "context": "Colonnes de dates obligatoires manquantes",
               "impact": "Impossible de déterminer la période de validité du feed"
           },
           "recommendations": [
               f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}",
               "Utiliser le format YYYYMMDD pour les dates"
           ]
       }

   # Analyse des dates (première ligne du feed_info)
   feed_entry = df.iloc[0]
   start_col, end_col = required_columns
   start_str = feed_entry[start_col]
   end_str = feed_entry[end_col]
   
   # Validation du format des dates
   date_errors = []
   start_date = None
   end_date = None
   
   try:
       start_date = parse_date(start_str)
       if start_date is None:
           date_errors.append(f"Format start_date invalide: {start_str}")
   except Exception as e:
       date_errors.append(f"Erreur parsing start_date: {start_str}")
   
   try:
       end_date = parse_date(end_str)
       if end_date is None:
           date_errors.append(f"Format end_date invalide: {end_str}")
   except Exception as e:
       date_errors.append(f"Erreur parsing end_date: {end_str}")

   # Construction des issues pour erreurs de format
   issues = []
   if date_errors:
       issues.append({
           "type": "invalid_format",
           "field": "date_fields",
           "count": len(date_errors),
           "affected_ids": [],
           "message": f"Erreurs de format de date: {'; '.join(date_errors)}"
       })

   # Si erreurs de format, retourner directement
   if date_errors:
       return {
           "status": "error",
           "issues": issues,
           "result": {
               "feed_entries": len(df),
               "start_date_raw": str(start_str),
               "end_date_raw": str(end_str),
               "date_validation": "failed_parsing"
           },
           "explanation": {
               "purpose": "Valide les dates de validité du feed GTFS pour assurer la cohérence temporelle des données",
               "context": "Erreurs de format dans les dates de validité",
               "impact": "Format de dates invalide empêche la validation temporelle"
           },
           "recommendations": [
               "Corriger le format des dates (utiliser YYYYMMDD)",
               "Vérifier que les dates sont complètes et valides"
           ]
       }

   # Validation de la cohérence temporelle
   from datetime import datetime, date
   today = date.today()
   
   # Calcul des métriques temporelles
   validity_period_days = (end_date - start_date).days
   days_until_start = (start_date - today).days
   days_until_end = (end_date - today).days
   
   # Validation logique des dates
   if start_date > end_date:
       issues.append({
           "type": "invalid_date_range",
           "field": "date_logic",
           "count": 1,
           "affected_ids": [],
           "message": f"start_date ({start_date}) est postérieure à end_date ({end_date})"
       })
   
   # Alertes pour période de validité
   if validity_period_days < 30:
       issues.append({
           "type": "short_validity_period",
           "field": "validity_duration",
           "count": validity_period_days,
           "affected_ids": [],
           "message": f"Période de validité très courte: {validity_period_days} jours"
       })
   
   if days_until_end < 0:
       issues.append({
           "type": "expired_feed",
           "field": "end_date",
           "count": abs(days_until_end),
           "affected_ids": [],
           "message": f"Feed expiré depuis {abs(days_until_end)} jours"
       })
   elif days_until_end < 7:
       issues.append({
           "type": "expiring_soon",
           "field": "end_date", 
           "count": days_until_end,
           "affected_ids": [],
           "message": f"Feed expire dans {days_until_end} jours"
       })

   # Détermination du statut
   critical_issues = [i for i in issues if i["type"] in ["invalid_date_range", "expired_feed"]]
   warning_issues = [i for i in issues if i["type"] in ["short_validity_period", "expiring_soon"]]
   
   if critical_issues:
       status = "error"
   elif warning_issues:
       status = "warning"
   else:
       status = "success"

   return {
       "status": status,
       "issues": issues,
       "result": {
           "feed_entries": len(df),
           "start_date": str(start_date),
           "end_date": str(end_date),
           "validity_analysis": {
               "validity_period_days": validity_period_days,
               "days_until_start": days_until_start,
               "days_until_end": days_until_end,
               "feed_status": (
                   "active" if days_until_start <= 0 and days_until_end > 0
                   else "future" if days_until_start > 0
                   else "expired"
               )
           },
           "temporal_quality": {
               "date_format_valid": len(date_errors) == 0,
               "date_logic_valid": start_date <= end_date if start_date and end_date else False,
               "adequate_duration": validity_period_days >= 30 if validity_period_days else False
           }
       },
       "explanation": {
           "purpose": "Valide les dates de validité du feed GTFS pour assurer la cohérence temporelle et l'actualité des données",
           "context": f"Période de validité: {start_date} à {end_date} ({validity_period_days} jours)",
           "validity_status": f"Feed {'actif' if days_until_start <= 0 and days_until_end > 0 else 'futur' if days_until_start > 0 else 'expiré'}",
           "temporal_analysis": f"Expire dans {days_until_end} jours" if days_until_end > 0 else f"Expiré depuis {abs(days_until_end)} jours",
           "impact": (
               f"Période de validité cohérente et actuelle" if status == "success"
               else f"Problèmes temporels détectés : {len(critical_issues)} critiques, {len(warning_issues)} avertissements"
           )
       },
       "recommendations": [
           rec for rec in [
               "URGENT: Corriger l'incohérence start_date > end_date" if any(i["type"] == "invalid_date_range" for i in issues) else None,
               f"URGENT: Mettre à jour le feed expiré (expiré depuis {abs(days_until_end)} jours)" if days_until_end < 0 else None,
               f"Planifier la mise à jour du feed (expire dans {days_until_end} jours)" if 0 <= days_until_end < 14 else None,
               f"Étendre la période de validité ({validity_period_days} jours seulement)" if validity_period_days < 30 else None,
               "Synchroniser la validité du feed avec les calendriers de service" if status != "error" else None,
               "Maintenir la validité du feed avec des mises à jour régulières" if status == "success" else None
           ] if rec is not None
       ]
   }


@audit_function(
    file_type="calendaires",
    name="invalid_or_inverted_dates",
    genre='validity',
    description="Vérifie validité et ordre de start_date et end_date."
)
def invalid_or_inverted_dates(gtfs_data, **params):
   """
   Vérifie la validité et la cohérence des dates start_date et end_date dans calendar.txt
   """
   df = gtfs_data.get('calendar.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "calendar.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier calendar.txt est requis pour valider les dates de service"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Vérifie la validité et la cohérence des dates start_date et end_date dans calendar.txt."
           },
           "recommendations": ["Fournir le fichier calendar.txt obligatoire."]
       }
   
   total_services = len(df)
   
   # Vérification des colonnes requises
   required_columns = ['service_id', 'start_date', 'end_date']
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
                   "message": f"Colonnes obligatoires manquantes dans calendar.txt: {', '.join(missing_columns)}"
               }
           ],
           "result": {
               "total_services": total_services,
               "missing_columns": missing_columns
           },
           "explanation": {
               "purpose": "Vérifie la validité et la cohérence des dates start_date et end_date dans calendar.txt",
               "context": "Colonnes obligatoires manquantes",
               "impact": "Impossible de valider les dates de service"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
       }

   # Analyse des dates par service
   invalid_format_services = []
   inverted_date_services = []
   null_date_services = []
   valid_services = []
   date_analysis = {}

   for _, row in df.iterrows():
       service_id = row['service_id']
       start_date_str = row['start_date']
       end_date_str = row['end_date']
       
       # Vérification des valeurs nulles
       if pd.isna(start_date_str) or pd.isna(end_date_str):
           null_date_services.append(service_id)
           continue
       
       try:
           # Parsing des dates au format GTFS (YYYYMMDD)
           start_date = pd.to_datetime(str(start_date_str), format='%Y%m%d')
           end_date = pd.to_datetime(str(end_date_str), format='%Y%m%d')
           
           # Vérification de la cohérence temporelle
           if start_date > end_date:
               inverted_date_services.append(service_id)
               date_analysis[service_id] = {
                   "start_date": start_date.strftime('%Y-%m-%d'),
                   "end_date": end_date.strftime('%Y-%m-%d'),
                   "issue": "inverted",
                   "gap_days": (start_date - end_date).days
               }
           else:
               valid_services.append(service_id)
               date_analysis[service_id] = {
                   "start_date": start_date.strftime('%Y-%m-%d'),
                   "end_date": end_date.strftime('%Y-%m-%d'),
                   "issue": None,
                   "duration_days": (end_date - start_date).days
               }
               
       except (ValueError, TypeError) as e:
           invalid_format_services.append(service_id)
           date_analysis[service_id] = {
               "start_date": str(start_date_str),
               "end_date": str(end_date_str),
               "issue": "invalid_format",
               "error": str(e)
           }

   # Calcul des métriques
   total_invalid = len(invalid_format_services) + len(inverted_date_services) + len(null_date_services)
   valid_count = len(valid_services)
   validation_rate = round(valid_count / total_services * 100, 2) if total_services > 0 else 0

   # Analyse des durées pour les services valides
   if valid_services:
       durations = [date_analysis[sid]["duration_days"] for sid in valid_services]
       avg_duration = round(sum(durations) / len(durations), 1)
       min_duration = min(durations)
       max_duration = max(durations)
   else:
       avg_duration = min_duration = max_duration = 0

   # Détermination du statut
   if total_invalid == 0:
       status = "success"
   elif validation_rate >= 90:
       status = "warning"
   else:
       status = "error"

   # Construction des issues
   issues = []
   
   if invalid_format_services:
       issues.append({
           "type": "invalid_format",
           "field": "date_format",
           "count": len(invalid_format_services),
           "affected_ids": invalid_format_services[:100],
           "message": f"{len(invalid_format_services)} services ont des dates au format invalide (attendu: YYYYMMDD)"
       })
   
   if inverted_date_services:
       issues.append({
           "type": "invalid_date_range",
           "field": "date_logic",
           "count": len(inverted_date_services),
           "affected_ids": inverted_date_services[:100],
           "message": f"{len(inverted_date_services)} services ont start_date postérieure à end_date"
       })
   
   if null_date_services:
       issues.append({
           "type": "missing_data",
           "field": "date_values",
           "count": len(null_date_services),
           "affected_ids": null_date_services[:100],
           "message": f"{len(null_date_services)} services ont des dates manquantes (null/vide)"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_services": total_services,
           "valid_services": valid_count,
           "invalid_format_services": len(invalid_format_services),
           "inverted_date_services": len(inverted_date_services),
           "null_date_services": len(null_date_services),
           "validation_rate": validation_rate,
           "date_analysis": {
               "avg_service_duration_days": avg_duration,
               "min_service_duration_days": min_duration,
               "max_service_duration_days": max_duration,
               "service_details": {k: v for k, v in list(date_analysis.items())[:10]}  # Top 10 exemples
           },
           "data_quality": {
               "format_compliance": round((total_services - len(invalid_format_services)) / total_services * 100, 2) if total_services > 0 else 0,
               "logic_compliance": round((total_services - len(inverted_date_services)) / total_services * 100, 2) if total_services > 0 else 0,
               "completeness": round((total_services - len(null_date_services)) / total_services * 100, 2) if total_services > 0 else 0
           }
       },
       "explanation": {
           "purpose": "Vérifie la validité du format et la cohérence logique des dates start_date et end_date dans calendar.txt",
           "context": f"Analyse de {total_services} services de calendrier",
           "validation_summary": f"Taux de validation: {validation_rate}% ({valid_count} services valides)",
           "date_format_spec": "Format GTFS attendu: YYYYMMDD avec start_date ≤ end_date",
           "impact": (
               f"Toutes les dates de service sont valides et cohérentes" if status == "success"
               else f"Problèmes de dates détectés : {len(invalid_format_services)} formats invalides, {len(inverted_date_services)} inversées, {len(null_date_services)} manquantes"
           )
       },
       "recommendations": [
           rec for rec in [
               f"URGENT: Corriger {len(invalid_format_services)} dates au format invalide (utiliser YYYYMMDD)" if invalid_format_services else None,
               f"URGENT: Corriger {len(inverted_date_services)} services avec start_date > end_date" if inverted_date_services else None,
               f"Renseigner {len(null_date_services)} dates manquantes" if null_date_services else None,
               "Implémenter une validation de format dans votre processus de génération calendar.txt" if total_invalid > 0 else None,
               f"Vérifier la cohérence avec la période de validité du feed" if valid_count > 0 else None,
               f"Examiner les services de courte durée (<7 jours)" if min_duration < 7 and valid_count > 0 else None,
               "Maintenir cette qualité de données temporelles pour assurer la fiabilité des calendriers" if status == "success" else None
           ] if rec is not None
       ]
   }

@audit_function(
    file_type="calendaires",
    name="inactive_services",
    genre='statistics',
    description="Détecte services sans jours actifs."
)
def inactive_services(gtfs_data, **params):
   """
   Identifie les services de calendrier sans aucun jour actif dans calendar.txt
   """
   df = gtfs_data.get('calendar.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "calendar.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier calendar.txt est requis pour analyser l'activité des services"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Identifie les services de calendrier sans aucun jour actif pour optimiser les données de service."
           },
           "recommendations": ["Fournir le fichier calendar.txt obligatoire."]
       }
   
   total_services = len(df)
   weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
   
   # Vérification des colonnes de jours de la semaine
   missing_weekday_columns = [day for day in weekdays if day not in df.columns]
   
   if missing_weekday_columns:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "weekday_columns",
                   "count": len(missing_weekday_columns),
                   "affected_ids": [],
                   "message": f"Colonnes de jours manquantes dans calendar.txt: {', '.join(missing_weekday_columns)}"
               }
           ],
           "result": {
               "total_services": total_services,
               "missing_columns": missing_weekday_columns,
               "available_columns": [col for col in weekdays if col in df.columns]
           },
           "explanation": {
               "purpose": "Identifie les services de calendrier sans aucun jour actif pour optimiser les données de service",
               "context": "Colonnes de jours de la semaine manquantes",
               "impact": "Impossible d'analyser l'activité des services"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_weekday_columns)}"]
       }

   # Vérification de la colonne service_id
   if 'service_id' not in df.columns:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "service_id",
                   "count": total_services,
                   "affected_ids": [],
                   "message": "La colonne service_id est obligatoire dans calendar.txt"
               }
           ],
           "result": {
               "total_services": total_services
           },
           "explanation": {
               "purpose": "Identifie les services de calendrier sans aucun jour actif pour optimiser les données de service",
               "context": "Colonne service_id obligatoire manquante",
               "impact": "Impossible d'identifier les services inactifs"
           },
           "recommendations": ["Ajouter la colonne service_id obligatoire dans calendar.txt"]
       }

   # Calcul de l'activité des services
   try:
       # Validation des valeurs dans les colonnes de jours (doivent être 0 ou 1)
       invalid_values_services = []
       valid_services_data = {}
       
       for _, row in df.iterrows():
           service_id = row['service_id']
           day_values = []
           has_invalid = False
           
           for day in weekdays:
               val = row[day]
               if pd.isna(val) or val not in [0, 1]:
                   has_invalid = True
                   break
               day_values.append(int(val))
           
           if has_invalid:
               invalid_values_services.append(service_id)
           else:
               valid_services_data[service_id] = {
                   'days': day_values,
                   'sum_days': sum(day_values),
                   'active_days': [weekdays[i] for i, v in enumerate(day_values) if v == 1]
               }

       # Identification des services inactifs
       inactive_services = [sid for sid, data in valid_services_data.items() if data['sum_days'] == 0]
       active_services = [sid for sid, data in valid_services_data.items() if data['sum_days'] > 0]
       
       # Analyse de la distribution d'activité
       activity_distribution = {}
       for data in valid_services_data.values():
           count = data['sum_days']
           activity_distribution[count] = activity_distribution.get(count, 0) + 1
       
       # Analyse des patterns d'activité
       weekday_only = [sid for sid, data in valid_services_data.items() 
                      if data['sum_days'] == 5 and all(data['days'][i] == 1 for i in range(5)) and data['days'][5] == 0 and data['days'][6] == 0]
       weekend_only = [sid for sid, data in valid_services_data.items() 
                      if data['sum_days'] == 2 and data['days'][5] == 1 and data['days'][6] == 1 and all(data['days'][i] == 0 for i in range(5))]
       daily_services = [sid for sid, data in valid_services_data.items() if data['sum_days'] == 7]

       # Calcul des métriques
       inactive_count = len(inactive_services)
       inactive_rate = round(inactive_count / total_services * 100, 2) if total_services > 0 else 0
       
       # Détermination du statut
       if inactive_count == 0 and len(invalid_values_services) == 0:
           status = "success"
       elif inactive_count <= total_services * 0.05:  # ≤5% inactifs
           status = "warning"
       else:
           status = "error"

       # Construction des issues
       issues = []
       
       if invalid_values_services:
           issues.append({
               "type": "invalid_format",
               "field": "weekday_values",
               "count": len(invalid_values_services),
               "affected_ids": invalid_values_services[:100],
               "message": f"{len(invalid_values_services)} services ont des valeurs invalides dans les colonnes de jours (attendu: 0 ou 1)"
           })
       
       if inactive_services:
           issues.append({
               "type": "inactive_service",
               "field": "service_activity",
               "count": inactive_count,
               "affected_ids": inactive_services[:100],
               "message": f"{inactive_count} services n'ont aucun jour actif (tous les jours à 0)"
           })

       return {
           "status": status,
           "issues": issues,
           "result": {
               "total_services": total_services,
               "active_services": len(active_services),
               "inactive_services": inactive_count,
               "invalid_format_services": len(invalid_values_services),
               "inactive_rate": inactive_rate,
               "activity_analysis": {
                   "activity_distribution": activity_distribution,
                   "service_patterns": {
                       "weekday_only": len(weekday_only),
                       "weekend_only": len(weekend_only),
                       "daily_services": len(daily_services),
                       "partial_week": len(active_services) - len(weekday_only) - len(weekend_only) - len(daily_services)
                   },
                   "avg_active_days": round(sum(data['sum_days'] for data in valid_services_data.values()) / len(valid_services_data), 2) if valid_services_data else 0
               },
               "service_optimization": {
                   "removable_services": inactive_count,
                   "efficiency_rate": round((len(active_services) / total_services) * 100, 2) if total_services > 0 else 0,
                   "cleanup_potential": f"{inactive_count} services supprimables"
               }
           },
           "explanation": {
               "purpose": "Identifie les services de calendrier sans aucun jour actif pour optimiser les données et détecter les erreurs de configuration",
               "context": f"Analyse de {total_services} services avec {len(active_services)} services actifs",
               "activity_summary": f"Taux d'inactivité: {inactive_rate}% ({inactive_count} services complètement inactifs)",
               "pattern_analysis": f"Répartition: {len(weekday_only)} semaine, {len(weekend_only)} weekend, {len(daily_services)} quotidiens",
               "impact": (
                   f"Tous les services sont correctement configurés avec au moins un jour actif" if status == "success"
                   else f"Services inactifs détectés : {inactive_count} services sans aucun jour de fonctionnement"
               )
           },
           "recommendations": [
               rec for rec in [
                   f"Corriger {len(invalid_values_services)} services avec valeurs invalides (utiliser 0 ou 1 uniquement)" if invalid_values_services else None,
                   f"Supprimer ou corriger {inactive_count} services complètement inactifs" if inactive_services else None,
                   "Vérifier si les services inactifs correspondent à des périodes spéciales (vacances, travaux)" if inactive_count > 0 else None,
                   "Activer au moins un jour pour les services destinés à être utilisés" if inactive_count > 0 else None,
                   f"Considérer l'utilisation de calendar_dates.txt pour les {inactive_count} services inactifs si nécessaire" if inactive_count > 0 else None,
                   "Optimiser la configuration des services pour améliorer l'efficacité opérationnelle" if inactive_rate > 10 else None,
                   "Maintenir cette configuration active des services pour assurer la continuité du service" if status == "success" else None
               ] if rec is not None
           ]
       }

   except Exception as e:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "processing_error",
                   "field": "service_analysis",
                   "count": 1,
                   "affected_ids": [],
                   "message": f"Erreur lors de l'analyse de l'activité des services: {str(e)}"
               }
           ],
           "result": {
               "total_services": total_services,
               "processing_error": str(e)
           },
           "explanation": {
               "purpose": "Identifie les services de calendrier sans aucun jour actif pour optimiser les données de service",
               "context": "Erreur de traitement lors de l'analyse",
               "impact": "Analyse d'activité impossible"
           },
           "recommendations": [
               "Vérifier le format des données dans calendar.txt",
               "Contrôler que les colonnes de jours contiennent des valeurs 0 ou 1"
           ]
       }

@audit_function(
    file_type="calendaires",
    name="excessive_duration_services",
    genre='statistics',
    description="Services actifs > 2 ans."
)
def excessive_duration_services(gtfs_data, **params):
   """
   Identifie les services de calendrier avec une durée excessive (>2 ans par défaut)
   """
   df = gtfs_data.get('calendar.txt')
   if df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "calendar.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier calendar.txt est requis pour analyser la durée des services"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Identifie les services de calendrier avec une durée excessive pour détecter les anomalies de configuration."
           },
           "recommendations": ["Fournir le fichier calendar.txt obligatoire."]
       }
   
   total_services = len(df)
   
   # Paramètres configurables
   max_duration_days = params.get('max_duration_days', 730)  # 2 ans par défaut
   warning_duration_days = params.get('warning_duration_days', 365)  # 1 an pour warning
   
   # Vérification des colonnes requises
   required_columns = ['service_id', 'start_date', 'end_date']
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
                   "message": f"Colonnes obligatoires manquantes dans calendar.txt: {', '.join(missing_columns)}"
               }
           ],
           "result": {
               "total_services": total_services,
               "missing_columns": missing_columns
           },
           "explanation": {
               "purpose": "Identifie les services de calendrier avec une durée excessive pour détecter les anomalies de configuration",
               "context": "Colonnes obligatoires manquantes",
               "impact": "Impossible d'analyser la durée des services"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_columns)}"]
       }

   # Analyse des durées des services
   excessive_services = []  # > max_duration_days
   long_services = []       # > warning_duration_days mais <= max_duration_days
   processing_errors = []
   duration_analysis = {}
   valid_durations = []

   for _, row in df.iterrows():
       service_id = row['service_id']
       start_date_str = row['start_date']
       end_date_str = row['end_date']
       
       try:
           # Parsing des dates GTFS (YYYYMMDD)
           start_date = pd.to_datetime(str(start_date_str), format='%Y%m%d')
           end_date = pd.to_datetime(str(end_date_str), format='%Y%m%d')
           
           # Calcul de la durée en jours
           duration_days = (end_date - start_date).days
           valid_durations.append(duration_days)
           
           duration_analysis[service_id] = {
               "start_date": start_date.strftime('%Y-%m-%d'),
               "end_date": end_date.strftime('%Y-%m-%d'),
               "duration_days": duration_days,
               "duration_years": round(duration_days / 365.25, 2)
           }
           
           # Classification selon les seuils
           if duration_days > max_duration_days:
               excessive_services.append(service_id)
               duration_analysis[service_id]["issue"] = "excessive"
           elif duration_days > warning_duration_days:
               long_services.append(service_id)
               duration_analysis[service_id]["issue"] = "long"
           else:
               duration_analysis[service_id]["issue"] = None
               
       except (ValueError, TypeError) as e:
           processing_errors.append(service_id)
           duration_analysis[service_id] = {
               "start_date": str(start_date_str),
               "end_date": str(end_date_str),
               "duration_days": None,
               "error": str(e),
               "issue": "parsing_error"
           }

   # Calcul des statistiques
   total_problematic = len(excessive_services) + len(long_services)
   excessive_rate = round(len(excessive_services) / total_services * 100, 2) if total_services > 0 else 0
   
   if valid_durations:
       avg_duration = round(sum(valid_durations) / len(valid_durations), 1)
       max_duration = max(valid_durations)
       min_duration = min(valid_durations)
       
       # Services par catégorie de durée
       short_services = len([d for d in valid_durations if d <= 30])      # ≤ 1 mois
       medium_services = len([d for d in valid_durations if 30 < d <= 365])  # 1 mois - 1 an
       long_valid_services = len([d for d in valid_durations if 365 < d <= max_duration_days])  # 1-2 ans
   else:
       avg_duration = max_duration = min_duration = 0
       short_services = medium_services = long_valid_services = 0

   # Détermination du statut
   if len(excessive_services) > 0:
       status = "error"
   elif len(long_services) > 0:
       status = "warning"
   else:
       status = "success"

   # Construction des issues
   issues = []
   
   if excessive_services:
       issues.append({
           "type": "excessive_duration",
           "field": "service_duration",
           "count": len(excessive_services),
           "affected_ids": excessive_services[:100],
           "message": f"{len(excessive_services)} services ont une durée excessive (>{max_duration_days} jours / {max_duration_days//365} ans)"
       })
   
   if long_services:
       issues.append({
           "type": "long_duration",
           "field": "service_duration",
           "count": len(long_services),
           "affected_ids": long_services[:100],
           "message": f"{len(long_services)} services ont une durée longue ({warning_duration_days}-{max_duration_days} jours)"
       })
   
   if processing_errors:
       issues.append({
           "type": "processing_error",
           "field": "date_parsing",
           "count": len(processing_errors),
           "affected_ids": processing_errors[:50],
           "message": f"{len(processing_errors)} services ont des erreurs de parsing de dates"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_services": total_services,
           "excessive_services": len(excessive_services),
           "long_services": len(long_services),
           "normal_services": total_services - total_problematic - len(processing_errors),
           "processing_errors": len(processing_errors),
           "excessive_rate": excessive_rate,
           "duration_thresholds": {
               "warning_threshold_days": warning_duration_days,
               "error_threshold_days": max_duration_days,
               "warning_threshold_years": round(warning_duration_days / 365.25, 1),
               "error_threshold_years": round(max_duration_days / 365.25, 1)
           },
           "duration_statistics": {
               "avg_duration_days": avg_duration,
               "max_duration_days": max_duration,
               "min_duration_days": min_duration,
               "avg_duration_years": round(avg_duration / 365.25, 2) if avg_duration > 0 else 0,
               "max_duration_years": round(max_duration / 365.25, 2) if max_duration > 0 else 0
           },
           "duration_distribution": {
               "short_services": short_services,      # ≤ 1 mois
               "medium_services": medium_services,    # 1 mois - 1 an
               "long_valid_services": long_valid_services,  # 1-2 ans
               "excessive_services": len(excessive_services)  # > 2 ans
           },
           "service_details": {k: v for k, v in list(duration_analysis.items())[:10]}  # Top 10 exemples
       },
       "explanation": {
           "purpose": "Identifie les services de calendrier avec une durée excessive pour détecter les anomalies de configuration et optimiser la gestion",
           "context": f"Analyse de {total_services} services avec seuils: {warning_duration_days} jours (warning), {max_duration_days} jours (error)",
           "duration_analysis": f"Durée moyenne: {avg_duration} jours ({round(avg_duration/365.25, 1)} ans) - Maximum: {max_duration} jours ({round(max_duration/365.25, 1)} ans)",
           "threshold_rationale": f"Services >2 ans souvent suspects (erreurs de saisie, services oubliés, maintenance insuffisante)",
           "impact": (
               f"Toutes les durées de service sont raisonnables (≤{max_duration_days} jours)" if status == "success"
               else f"Durées suspectes détectées : {len(excessive_services)} excessives, {len(long_services)} longues"
           )
       },
       "recommendations": [
           rec for rec in [
               f"URGENT: Examiner {len(excessive_services)} services avec durée >{max_duration_days} jours (possibles erreurs)" if excessive_services else None,
               f"Vérifier {len(long_services)} services de longue durée ({warning_duration_days}-{max_duration_days} jours)" if long_services else None,
               f"Corriger {len(processing_errors)} services avec erreurs de dates" if processing_errors else None,
               "Segmenter les services de très longue durée en périodes plus courtes pour faciliter la maintenance" if len(excessive_services) > 0 else None,
               "Vérifier que les services longs correspondent à des besoins opérationnels réels" if len(long_services) > 0 else None,
               "Implémenter une révision périodique des services pour éviter les durées excessives" if total_problematic > 0 else None,
               f"Considérer des seuils personnalisés si votre contexte justifie des services >{max_duration_days} jours" if len(excessive_services) > total_services * 0.1 else None,
               "Maintenir cette gestion raisonnable des durées de service" if status == "success" else None
           ] if rec is not None
       ]
   }

@audit_function(
    file_type="calendaires",
    name="exceptions_outside_date_range",
    genre='quality',
    description="Dates dans calendar_dates.txt hors plages calendar.txt."
)
def exceptions_outside_date_range(gtfs_data, **params):
   """
   Détecte les exceptions de calendrier (calendar_dates) situées en dehors des plages de dates définies dans calendar
   """
   calendar_df = gtfs_data.get('calendar.txt')
   calendar_dates_df = gtfs_data.get('calendar_dates.txt')
   
   # Vérification de la présence des fichiers
   missing_files = []
   if calendar_df is None:
       missing_files.append('calendar.txt')
   if calendar_dates_df is None:
       missing_files.append('calendar_dates.txt')
   
   if missing_files:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "required_files",
                   "count": len(missing_files),
                   "affected_ids": [],
                   "message": f"Fichiers manquants requis pour l'analyse des exceptions: {', '.join(missing_files)}"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Détecte les exceptions de calendrier situées en dehors des plages de dates définies pour chaque service."
           },
           "recommendations": [f"Fournir les fichiers manquants: {', '.join(missing_files)}"]
       }

   total_calendar_services = len(calendar_df)
   total_exceptions = len(calendar_dates_df)
   
   # Vérification des colonnes requises
   calendar_required = ['service_id', 'start_date', 'end_date']
   calendar_dates_required = ['service_id', 'date', 'exception_type']
   
   missing_calendar_cols = [col for col in calendar_required if col not in calendar_df.columns]
   missing_dates_cols = [col for col in calendar_dates_required if col not in calendar_dates_df.columns]
   
   if missing_calendar_cols or missing_dates_cols:
       missing_all = missing_calendar_cols + [f"calendar_dates.{col}" for col in missing_dates_cols]
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "required_columns",
                   "count": len(missing_all),
                   "affected_ids": [],
                   "message": f"Colonnes obligatoires manquantes: {', '.join(missing_all)}"
               }
           ],
           "result": {
               "total_calendar_services": total_calendar_services,
               "total_exceptions": total_exceptions,
               "missing_columns": missing_all
           },
           "explanation": {
               "purpose": "Détecte les exceptions de calendrier situées en dehors des plages de dates définies pour chaque service",
               "context": "Colonnes obligatoires manquantes",
               "impact": "Impossible d'analyser les exceptions hors plage"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_all)}"]
       }

   # Parsing des dates avec gestion d'erreurs
   parsing_errors = []
   try:
       calendar_df['start_date_parsed'] = pd.to_datetime(calendar_df['start_date'], format='%Y%m%d', errors='coerce')
       calendar_df['end_date_parsed'] = pd.to_datetime(calendar_df['end_date'], format='%Y%m%d', errors='coerce')
       calendar_dates_df['date_parsed'] = pd.to_datetime(calendar_dates_df['date'], format='%Y%m%d', errors='coerce')
       
       # Identification des erreurs de parsing
       calendar_parsing_errors = calendar_df[calendar_df['start_date_parsed'].isna() | calendar_df['end_date_parsed'].isna()]
       dates_parsing_errors = calendar_dates_df[calendar_dates_df['date_parsed'].isna()]
       
       if not calendar_parsing_errors.empty:
           parsing_errors.extend(calendar_parsing_errors['service_id'].tolist())
       if not dates_parsing_errors.empty:
           parsing_errors.extend([f"exception_{idx}" for idx in dates_parsing_errors.index])
           
   except Exception as e:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "invalid_format",
                   "field": "date_parsing",
                   "count": 1,
                   "affected_ids": [],
                   "message": f"Erreur critique de parsing des dates: {str(e)}"
               }
           ],
           "result": {
               "total_calendar_services": total_calendar_services,
               "total_exceptions": total_exceptions,
               "parsing_error": str(e)
           },
           "explanation": {
               "purpose": "Détecte les exceptions de calendrier situées en dehors des plages de dates définies pour chaque service",
               "context": "Erreur de parsing des dates GTFS",
               "impact": "Format de dates invalide empêche l'analyse"
           },
           "recommendations": [
               "Corriger le format des dates (utiliser YYYYMMDD)",
               "Vérifier l'intégrité des données de dates"
           ]
       }

   # Création du dictionnaire des plages de dates par service
   calendar_ranges = {}
   for _, row in calendar_df.iterrows():
       if pd.notnull(row['start_date_parsed']) and pd.notnull(row['end_date_parsed']):
           calendar_ranges[row['service_id']] = {
               'start_date': row['start_date_parsed'],
               'end_date': row['end_date_parsed']
           }

   # Analyse des exceptions hors plage
   outside_range_exceptions = []
   orphaned_exceptions = []  # Exceptions pour services non définis dans calendar
   conflicting_exceptions = []  # Exceptions multiples sur même date/service
   
   # Groupement des exceptions par service_id et date pour détecter les conflits
   exception_groups = calendar_dates_df.groupby(['service_id', 'date_parsed'])
   
   for (service_id, exception_date), group in exception_groups:
       if pd.isna(exception_date):
           continue
           
       # Détection des conflits (multiples exception_type pour même service/date)
       if len(group) > 1:
           conflicting_exceptions.append({
               'service_id': service_id,
               'date': exception_date.strftime('%Y-%m-%d'),
               'exception_types': group['exception_type'].tolist(),
               'count': len(group)
           })
       
       # Vérification si le service existe dans calendar
       if service_id not in calendar_ranges:
           orphaned_exceptions.append({
               'service_id': service_id,
               'date': exception_date.strftime('%Y-%m-%d'),
               'exception_type': group.iloc[0]['exception_type']
           })
           continue
       
       # Vérification si l'exception est dans la plage de dates
       service_range = calendar_ranges[service_id]
       if not (service_range['start_date'] <= exception_date <= service_range['end_date']):
           days_outside = min(
               (service_range['start_date'] - exception_date).days,
               (exception_date - service_range['end_date']).days
           )
           outside_range_exceptions.append({
               'service_id': service_id,
               'date': exception_date.strftime('%Y-%m-%d'),
               'exception_type': group.iloc[0]['exception_type'],
               'service_start': service_range['start_date'].strftime('%Y-%m-%d'),
               'service_end': service_range['end_date'].strftime('%Y-%m-%d'),
               'days_outside': max(days_outside, 0)
           })

   # Calcul des métriques
   total_issues = len(outside_range_exceptions) + len(orphaned_exceptions) + len(conflicting_exceptions)
   outside_rate = round(len(outside_range_exceptions) / total_exceptions * 100, 2) if total_exceptions > 0 else 0
   
   # Détermination du statut
   if total_issues == 0 and not parsing_errors:
       status = "success"
   elif len(outside_range_exceptions) == 0 and len(orphaned_exceptions) <= total_exceptions * 0.05:
       status = "warning"
   else:
       status = "error"

   # Construction des issues
   issues = []
   
   if parsing_errors:
       issues.append({
           "type": "invalid_format",
           "field": "date_parsing",
           "count": len(parsing_errors),
           "affected_ids": parsing_errors[:50],
           "message": f"{len(parsing_errors)} services/exceptions ont des erreurs de format de date"
       })
   
   if outside_range_exceptions:
       outside_service_ids = list(set(exc['service_id'] for exc in outside_range_exceptions))
       issues.append({
           "type": "date_range_violation",
           "field": "exception_dates",
           "count": len(outside_range_exceptions),
           "affected_ids": outside_service_ids[:100],
           "message": f"{len(outside_range_exceptions)} exceptions sont en dehors des plages de dates de leurs services"
       })
   
   if orphaned_exceptions:
       orphaned_service_ids = list(set(exc['service_id'] for exc in orphaned_exceptions))
       issues.append({
           "type": "orphaned_exception",
           "field": "service_reference",
           "count": len(orphaned_exceptions),
           "affected_ids": orphaned_service_ids[:100],
           "message": f"{len(orphaned_exceptions)} exceptions référencent des services inexistants dans calendar.txt"
       })
   
   if conflicting_exceptions:
       conflicting_service_ids = list(set(exc['service_id'] for exc in conflicting_exceptions))
       issues.append({
           "type": "conflicting_exceptions",
           "field": "exception_consistency",
           "count": len(conflicting_exceptions),
           "affected_ids": conflicting_service_ids[:50],
           "message": f"{len(conflicting_exceptions)} dates ont des exception_type multiples pour le même service"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "total_calendar_services": total_calendar_services,
           "total_exceptions": total_exceptions,
           "outside_range_exceptions": len(outside_range_exceptions),
           "orphaned_exceptions": len(orphaned_exceptions),
           "conflicting_exceptions": len(conflicting_exceptions),
           "valid_exceptions": total_exceptions - total_issues,
           "outside_range_rate": outside_rate,
           "exception_analysis": {
               "outside_range_details": outside_range_exceptions[:10],  # Top 10 exemples
               "orphaned_details": orphaned_exceptions[:10],
               "conflicts_details": conflicting_exceptions[:10],
               "avg_days_outside": round(sum(exc['days_outside'] for exc in outside_range_exceptions) / len(outside_range_exceptions), 1) if outside_range_exceptions else 0,
               "max_days_outside": max(exc['days_outside'] for exc in outside_range_exceptions) if outside_range_exceptions else 0
           },
           "data_integrity": {
               "calendar_services_with_ranges": len(calendar_ranges),
               "exceptions_with_valid_dates": total_exceptions - len([e for e in parsing_errors if 'exception_' in str(e)]),
               "parsing_error_count": len(parsing_errors)
           }
       },
       "explanation": {
           "purpose": "Détecte les exceptions de calendrier situées en dehors des plages de dates définies pour assurer la cohérence temporelle",
           "context": f"Analyse de {total_exceptions} exceptions sur {total_calendar_services} services de calendrier",
           "range_analysis": f"Taux d'exceptions hors plage: {outside_rate}% ({len(outside_range_exceptions)} exceptions concernées)",
           "integrity_check": f"Intégrité: {len(orphaned_exceptions)} exceptions orphelines, {len(conflicting_exceptions)} conflits détectés",
           "impact": (
               f"Toutes les exceptions respectent les plages de dates de leurs services" if status == "success"
               else f"Incohérences temporelles détectées : {len(outside_range_exceptions)} hors plage, {len(orphaned_exceptions)} orphelines"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Corriger {len(parsing_errors)} erreurs de format de date (utiliser YYYYMMDD)" if parsing_errors else None,
               f"URGENT: Réviser {len(outside_range_exceptions)} exceptions hors plage de dates" if outside_range_exceptions else None,
               f"Corriger {len(orphaned_exceptions)} exceptions référençant des services inexistants" if orphaned_exceptions else None,
               f"Résoudre {len(conflicting_exceptions)} conflits d'exception_type sur même date/service" if conflicting_exceptions else None,
               "Étendre les plages de dates des services si les exceptions hors plage sont légitimes" if len(outside_range_exceptions) > 0 else None,
               "Vérifier la synchronisation entre calendar.txt et calendar_dates.txt" if len(orphaned_exceptions) > 0 else None,
               "Implémenter une validation de cohérence dans votre processus de génération GTFS" if total_issues > 0 else None,
               "Maintenir cette cohérence temporelle entre calendriers et exceptions" if status == "success" else None
           ] if rec is not None
       ]
   }

@audit_function(
    file_type="calendaires",
    name="calendar_dates_conflicts_with_calendar",
    genre='quality',
    description="Vérifie cohérence calendar_dates.txt avec calendar.txt (ajouts/suppressions valides)"
)
def calendar_dates_conflicts_with_calendar(gtfs_data, **params):
   """
   Détecte les conflits entre calendar.txt et calendar_dates.txt (services inexistants, logique incohérente)
   """
   cal_df = gtfs_data.get('calendar.txt')
   cal_dates_df = gtfs_data.get('calendar_dates.txt')
   
   # Gestion des fichiers manquants ou vides
   if cal_df is None and cal_dates_df is None:
       return {
           "status": "warning",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "calendar_files",
                   "count": 2,
                   "affected_ids": [],
                   "message": "Aucun fichier de calendrier disponible (calendar.txt et calendar_dates.txt manquants)"
               }
           ],
           "result": {
               "calendar_services": 0,
               "exception_entries": 0,
               "conflicts": 0
           },
           "explanation": {
               "purpose": "Détecte les conflits entre calendar.txt et calendar_dates.txt pour assurer la cohérence du système de calendrier."
           },
           "recommendations": ["Fournir au moins un fichier de calendrier (calendar.txt ou calendar_dates.txt)."]
       }
   
   if cal_df is None or cal_df.empty:
       # Cas où seul calendar_dates.txt existe
       if cal_dates_df is not None and not cal_dates_df.empty:
           return {
               "status": "success",
               "issues": [],
               "result": {
                   "calendar_services": 0,
                   "exception_entries": len(cal_dates_df),
                   "conflicts": 0,
                   "calendar_mode": "exceptions_only"
               },
               "explanation": {
                   "purpose": "Détecte les conflits entre calendar.txt et calendar_dates.txt pour assurer la cohérence du système de calendrier",
                   "context": "Système basé uniquement sur calendar_dates.txt (mode exceptions)",
                   "impact": "Aucun conflit possible - fonctionnement normal en mode exceptions"
               },
               "recommendations": []
           }
       else:
           return {
               "status": "error",
               "issues": [
                   {
                       "type": "empty_calendar_system",
                       "field": "calendar_data",
                       "count": 0,
                       "affected_ids": [],
                       "message": "Aucune donnée de calendrier disponible"
                   }
               ],
               "result": {
                   "calendar_services": 0,
                   "exception_entries": 0,
                   "conflicts": 0
               },
               "explanation": {
                   "purpose": "Détecte les conflits entre calendar.txt et calendar_dates.txt pour assurer la cohérence du système de calendrier",
                   "context": "Système de calendrier complètement vide",
                   "impact": "Aucun service de transport défini"
               },
               "recommendations": ["Créer des services dans calendar.txt ou calendar_dates.txt."]
           }
   
   if cal_dates_df is None or cal_dates_df.empty:
       # Cas où seul calendar.txt existe
       return {
           "status": "success",
           "issues": [],
           "result": {
               "calendar_services": len(cal_df),
               "exception_entries": 0,
               "conflicts": 0,
               "calendar_mode": "regular_only"
           },
           "explanation": {
               "purpose": "Détecte les conflits entre calendar.txt et calendar_dates.txt pour assurer la cohérence du système de calendrier",
               "context": "Système basé uniquement sur calendar.txt (mode régulier)",
               "impact": "Aucun conflit possible - fonctionnement normal en mode régulier"
           },
           "recommendations": []
       }

   # Vérification des colonnes requises
   cal_required = ['service_id']
   cal_dates_required = ['service_id', 'date', 'exception_type']
   
   missing_cal_cols = [col for col in cal_required if col not in cal_df.columns]
   missing_dates_cols = [col for col in cal_dates_required if col not in cal_dates_df.columns]
   
   if missing_cal_cols or missing_dates_cols:
       missing_all = [f"calendar.{col}" for col in missing_cal_cols] + [f"calendar_dates.{col}" for col in missing_dates_cols]
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "required_columns",
                   "count": len(missing_all),
                   "affected_ids": [],
                   "message": f"Colonnes obligatoires manquantes: {', '.join(missing_all)}"
               }
           ],
           "result": {
               "calendar_services": len(cal_df),
               "exception_entries": len(cal_dates_df),
               "missing_columns": missing_all
           },
           "explanation": {
               "purpose": "Détecte les conflits entre calendar.txt et calendar_dates.txt pour assurer la cohérence du système de calendrier",
               "context": "Colonnes obligatoires manquantes",
               "impact": "Impossible d'analyser les conflits"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_all)}"]
       }

   # Analyse des conflits
   active_services = set(cal_df['service_id'].unique())
   exception_services = set(cal_dates_df['service_id'].unique())
   
   # Classification des conflits
   reference_conflicts = []  # Services inexistants dans calendar
   logic_conflicts = []      # Exception_type incohérent
   orphaned_exceptions = []  # Services dans calendar_dates uniquement
   
   # Analyse par exception
   for _, row in cal_dates_df.iterrows():
       service_id = row['service_id']
       exception_type = row['exception_type']
       date = row['date']
       
       # Conflit de référence : service inexistant dans calendar
       if service_id not in active_services:
           conflict_detail = {
               "service_id": service_id,
               "date": str(date),
               "exception_type": exception_type,
               "issue_type": "missing_service_reference"
           }
           
           if exception_type == 1:
               conflict_detail["description"] = "Tentative d'ajout (type 1) d'un service inexistant dans calendar.txt"
           elif exception_type == 2:
               conflict_detail["description"] = "Tentative de suppression (type 2) d'un service inexistant dans calendar.txt"
           else:
               conflict_detail["description"] = f"Exception avec type invalide ({exception_type}) pour service inexistant"
           
           reference_conflicts.append(conflict_detail)
       
       # Validation du type d'exception
       elif exception_type not in [1, 2]:
           logic_conflicts.append({
               "service_id": service_id,
               "date": str(date),
               "exception_type": exception_type,
               "issue_type": "invalid_exception_type",
               "description": f"Type d'exception invalide: {exception_type} (attendu: 1=ajout, 2=suppression)"
           })

   # Services orphelins (dans calendar_dates mais pas dans calendar)
   orphaned_services = exception_services - active_services
   for service_id in orphaned_services:
       service_exceptions = cal_dates_df[cal_dates_df['service_id'] == service_id]
       orphaned_exceptions.append({
           "service_id": service_id,
           "exception_count": len(service_exceptions),
           "exception_types": service_exceptions['exception_type'].unique().tolist()
       })

   # Calcul des métriques
   total_conflicts = len(reference_conflicts) + len(logic_conflicts)
   total_exceptions = len(cal_dates_df)
   conflict_rate = round(total_conflicts / total_exceptions * 100, 2) if total_exceptions > 0 else 0
   
   # Détermination du statut
   if total_conflicts == 0:
       status = "success"
   elif len(reference_conflicts) == 0 and len(logic_conflicts) <= total_exceptions * 0.05:
       status = "warning"
   else:
       status = "error"

   # Construction des issues
   issues = []
   
   if reference_conflicts:
       conflicting_services = list(set(conf['service_id'] for conf in reference_conflicts))
       issues.append({
           "type": "missing_service_reference",
           "field": "service_id",
           "count": len(reference_conflicts),
           "affected_ids": conflicting_services[:100],
           "message": f"{len(reference_conflicts)} exceptions référencent des services inexistants dans calendar.txt"
       })
   
   if logic_conflicts:
       invalid_services = list(set(conf['service_id'] for conf in logic_conflicts))
       issues.append({
           "type": "invalid_exception_type",
           "field": "exception_type",
           "count": len(logic_conflicts),
           "affected_ids": invalid_services[:50],
           "message": f"{len(logic_conflicts)} exceptions ont des types invalides (attendu: 1 ou 2)"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "calendar_services": len(cal_df),
           "exception_entries": len(cal_dates_df),
           "active_services": len(active_services),
           "exception_services": len(exception_services),
           "reference_conflicts": len(reference_conflicts),
           "logic_conflicts": len(logic_conflicts),
           "orphaned_services": len(orphaned_services),
           "conflict_rate": conflict_rate,
           "conflict_analysis": {
               "reference_conflict_details": reference_conflicts[:10],  # Top 10 exemples
               "logic_conflict_details": logic_conflicts[:10],
               "orphaned_service_details": orphaned_exceptions[:10],
               "services_overlap": len(active_services & exception_services),
               "services_only_in_calendar": len(active_services - exception_services),
               "services_only_in_exceptions": len(orphaned_services)
           },
           "system_coherence": {
               "calendar_mode": "hybrid",  # Les deux fichiers existent
               "reference_integrity": len(reference_conflicts) == 0,
               "type_validity": len(logic_conflicts) == 0,
               "cross_file_consistency": total_conflicts == 0
           }
       },
       "explanation": {
           "purpose": "Détecte les conflits entre calendar.txt et calendar_dates.txt pour assurer la cohérence du système de calendrier hybride",
           "context": f"Analyse de {len(active_services)} services calendar vs {len(exception_services)} services exceptions",
           "conflict_summary": f"Taux de conflits: {conflict_rate}% ({total_conflicts} conflits sur {total_exceptions} exceptions)",
           "reference_integrity": f"Intégrité référentielle: {len(reference_conflicts)} références invalides détectées",
           "impact": (
               f"Système de calendrier cohérent sans conflits entre les fichiers" if status == "success"
               else f"Conflits détectés : {len(reference_conflicts)} références invalides, {len(logic_conflicts)} types incorrects"
           )
       },
       "recommendations": [
           rec for rec in [
               f"URGENT: Corriger {len(reference_conflicts)} exceptions référençant des services inexistants" if reference_conflicts else None,
               f"Valider {len(logic_conflicts)} exception_type invalides (utiliser 1=ajout, 2=suppression)" if logic_conflicts else None,
               f"Créer {len(orphaned_services)} services manquants dans calendar.txt ou les supprimer de calendar_dates.txt" if orphaned_services else None,
               "Implémenter une validation croisée dans votre processus de génération GTFS" if total_conflicts > 0 else None,
               "Synchroniser les identifiants de service entre calendar.txt et calendar_dates.txt" if len(reference_conflicts) > 0 else None,
               "Vérifier la logique métier des exceptions par rapport aux services réguliers" if total_conflicts > 0 else None,
               "Maintenir cette cohérence entre les fichiers de calendrier" if status == "success" else None
           ] if rec is not None
       ]
   }

@audit_function(
    file_type="calendaires",
    name="feed_info_date_coverage",
    genre='cross-validation',
    description="Vérifie que l'intervalle start_date/end_date de feed_info.txt couvre toutes les dates de calendar.txt et calendar_dates.txt.",
    parameters={}
)
def feed_info_date_coverage(gtfs_data, **params):
   """
   Vérifie que toutes les dates de service (calendar + calendar_dates) sont couvertes par la plage feed_info
   """
   feed_df = gtfs_data.get('feed_info.txt')
   if feed_df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "feed_info.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier feed_info.txt est requis pour définir la couverture temporelle du feed"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Vérifie que toutes les dates de service sont couvertes par la plage temporelle définie dans feed_info.txt."
           },
           "recommendations": ["Créer le fichier feed_info.txt avec les dates de couverture du feed."]
       }

   calendar_df = gtfs_data.get('calendar.txt')
   calendar_dates_df = gtfs_data.get('calendar_dates.txt')
   
   # Vérification de la présence d'au moins un fichier de calendrier
   if (calendar_df is None or calendar_df.empty) and (calendar_dates_df is None or calendar_dates_df.empty):
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "calendar_files",
                   "count": 2,
                   "affected_ids": [],
                   "message": "Aucun fichier de calendrier disponible (calendar.txt et calendar_dates.txt manquants ou vides)"
               }
           ],
           "result": {
               "feed_info_available": True,
               "calendar_data_available": False
           },
           "explanation": {
               "purpose": "Vérifie que toutes les dates de service sont couvertes par la plage temporelle définie dans feed_info.txt",
               "context": "Aucune donnée de calendrier pour valider la couverture",
               "impact": "Impossible de vérifier la cohérence temporelle"
           },
           "recommendations": ["Fournir au moins calendar.txt ou calendar_dates.txt pour définir les services."]
       }

   # Vérification des colonnes feed_info
   required_feed_cols = ['start_date', 'end_date']
   missing_feed_cols = [col for col in required_feed_cols if col not in feed_df.columns]
   
   if missing_feed_cols:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "feed_info_dates",
                   "count": len(missing_feed_cols),
                   "affected_ids": [],
                   "message": f"Colonnes de dates manquantes dans feed_info.txt: {', '.join(missing_feed_cols)}"
               }
           ],
           "result": {
               "feed_info_available": True,
               "missing_columns": missing_feed_cols
           },
           "explanation": {
               "purpose": "Vérifie que toutes les dates de service sont couvertes par la plage temporelle définie dans feed_info.txt",
               "context": "Colonnes de dates manquantes dans feed_info.txt",
               "impact": "Impossible de déterminer la plage de couverture du feed"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_feed_cols)}"]
       }

   # Parsing des dates feed_info
   try:
       feed_start = datetime.strptime(str(feed_df.iloc[0]['start_date']), "%Y%m%d").date()
       feed_end = datetime.strptime(str(feed_df.iloc[0]['end_date']), "%Y%m%d").date()
   except Exception as e:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "invalid_format",
                   "field": "feed_info_dates",
                   "count": 1,
                   "affected_ids": [],
                   "message": f"Format de dates invalide dans feed_info.txt: {str(e)}"
               }
           ],
           "result": {
               "feed_info_available": True,
               "feed_start_raw": str(feed_df.iloc[0]['start_date']),
               "feed_end_raw": str(feed_df.iloc[0]['end_date'])
           },
           "explanation": {
               "purpose": "Vérifie que toutes les dates de service sont couvertes par la plage temporelle définie dans feed_info.txt",
               "context": "Format de dates invalide dans feed_info.txt",
               "impact": "Impossible de parser les dates de couverture du feed"
           },
           "recommendations": ["Corriger le format des dates dans feed_info.txt (utiliser YYYYMMDD)."]
       }

   # Collecte de toutes les dates de service
   service_dates = set()
   out_of_range_calendar = []
   out_of_range_exceptions = []
   parsing_errors = []

   # Analyse de calendar.txt
   if calendar_df is not None and not calendar_df.empty:
       for idx, row in calendar_df.iterrows():
           try:
               start_date = datetime.strptime(str(row['start_date']), "%Y%m%d").date()
               end_date = datetime.strptime(str(row['end_date']), "%Y%m%d").date()
               
               # Vérification que la plage du service est dans la plage feed
               if start_date < feed_start or end_date > feed_end:
                   out_of_range_calendar.append({
                       "service_id": row.get('service_id', f'ligne_{idx}'),
                       "start_date": start_date.strftime('%Y-%m-%d'),
                       "end_date": end_date.strftime('%Y-%m-%d'),
                       "days_before_feed": max(0, (feed_start - start_date).days),
                       "days_after_feed": max(0, (end_date - feed_end).days)
                   })
               
               # Ajout des dates dans la plage feed (pour statistiques)
               current_date = max(start_date, feed_start)
               end_date_clipped = min(end_date, feed_end)
               while current_date <= end_date_clipped:
                   service_dates.add(current_date)
                   current_date += pd.Timedelta(days=1)
                   
           except Exception as e:
               parsing_errors.append(f"calendar.txt ligne {idx}: {str(e)}")

   # Analyse de calendar_dates.txt
   if calendar_dates_df is not None and not calendar_dates_df.empty and 'date' in calendar_dates_df.columns:
       for idx, row in calendar_dates_df.iterrows():
           try:
               service_date = datetime.strptime(str(row['date']), "%Y%m%d").date()
               
               if service_date < feed_start or service_date > feed_end:
                   out_of_range_exceptions.append({
                       "service_id": row.get('service_id', f'exception_{idx}'),
                       "date": service_date.strftime('%Y-%m-%d'),
                       "exception_type": row.get('exception_type', 'unknown'),
                       "days_outside": min((feed_start - service_date).days, (service_date - feed_end).days)
                   })
               else:
                   service_dates.add(service_date)
                   
           except Exception as e:
               parsing_errors.append(f"calendar_dates.txt ligne {idx}: {str(e)}")

   # Calcul des métriques
   total_out_of_range = len(out_of_range_calendar) + len(out_of_range_exceptions)
   feed_duration_days = (feed_end - feed_start).days + 1
   coverage_days = len(service_dates)
   coverage_rate = round(coverage_days / feed_duration_days * 100, 2) if feed_duration_days > 0 else 0

   # Détermination du statut
   if parsing_errors:
       status = "error"
   elif total_out_of_range > 0:
       status = "error"
   elif coverage_rate < 50:  # Moins de 50% de couverture
       status = "warning"
   else:
       status = "success"

   # Construction des issues
   issues = []
   
   if parsing_errors:
       issues.append({
           "type": "invalid_format",
           "field": "calendar_dates",
           "count": len(parsing_errors),
           "affected_ids": [],
           "message": f"{len(parsing_errors)} erreurs de format de date dans les fichiers de calendrier"
       })
   
   if out_of_range_calendar:
       calendar_service_ids = [item['service_id'] for item in out_of_range_calendar]
       issues.append({
           "type": "date_range_violation",
           "field": "calendar_services",
           "count": len(out_of_range_calendar),
           "affected_ids": calendar_service_ids[:100],
           "message": f"{len(out_of_range_calendar)} services dans calendar.txt dépassent la plage feed_info"
       })
   
   if out_of_range_exceptions:
       exception_service_ids = [item['service_id'] for item in out_of_range_exceptions]
       issues.append({
           "type": "date_range_violation",
           "field": "calendar_exceptions",
           "count": len(out_of_range_exceptions),
           "affected_ids": exception_service_ids[:100],
           "message": f"{len(out_of_range_exceptions)} exceptions dans calendar_dates.txt sont hors plage feed_info"
       })
   
   if coverage_rate < 50 and total_out_of_range == 0:
       issues.append({
           "type": "low_coverage",
           "field": "service_coverage",
           "count": feed_duration_days - coverage_days,
           "affected_ids": [],
           "message": f"Couverture de service faible: {coverage_rate}% de la plage feed_info couverte"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "feed_start_date": feed_start.strftime('%Y-%m-%d'),
           "feed_end_date": feed_end.strftime('%Y-%m-%d'),
           "feed_duration_days": feed_duration_days,
           "service_coverage_days": coverage_days,
           "coverage_rate": coverage_rate,
           "out_of_range_analysis": {
               "calendar_violations": len(out_of_range_calendar),
               "exception_violations": len(out_of_range_exceptions),
               "total_violations": total_out_of_range,
               "calendar_details": out_of_range_calendar[:10],
               "exception_details": out_of_range_exceptions[:10]
           },
           "temporal_analysis": {
               "first_service_date": min(service_dates).strftime('%Y-%m-%d') if service_dates else None,
               "last_service_date": max(service_dates).strftime('%Y-%m-%d') if service_dates else None,
               "service_span_days": (max(service_dates) - min(service_dates)).days + 1 if service_dates else 0,
               "gaps_in_coverage": feed_duration_days - coverage_days
           },
           "data_quality": {
               "parsing_errors": len(parsing_errors),
               "calendar_entries_analyzed": len(calendar_df) if calendar_df is not None else 0,
               "exception_entries_analyzed": len(calendar_dates_df) if calendar_dates_df is not None else 0
           }
       },
       "explanation": {
           "purpose": "Vérifie que toutes les dates de service sont couvertes par la plage temporelle définie dans feed_info.txt pour assurer la cohérence",
           "context": f"Plage feed_info: {feed_start} à {feed_end} ({feed_duration_days} jours)",
           "coverage_analysis": f"Couverture service: {coverage_rate}% ({coverage_days}/{feed_duration_days} jours couverts)",
           "violation_summary": f"Violations temporelles: {total_out_of_range} services/exceptions hors plage",
           "impact": (
               f"Couverture temporelle cohérente : tous les services respectent la plage feed_info" if status == "success"
               else f"Incohérences temporelles : {total_out_of_range} violations, {len(parsing_errors)} erreurs de format"
           )
       },
       "recommendations": [
           rec for rec in [
               f"Corriger {len(parsing_errors)} erreurs de format de date (utiliser YYYYMMDD)" if parsing_errors else None,
               f"URGENT: Étendre la plage feed_info pour couvrir {len(out_of_range_calendar)} services calendar dépassants" if out_of_range_calendar else None,
               f"URGENT: Étendre la plage feed_info pour couvrir {len(out_of_range_exceptions)} exceptions dépassantes" if out_of_range_exceptions else None,
               f"Optimiser la couverture service (seulement {coverage_rate}% de la plage feed utilisée)" if coverage_rate < 70 and total_out_of_range == 0 else None,
               "Ajuster la plage feed_info pour correspondre exactement aux services actifs" if coverage_rate < 80 and total_out_of_range == 0 else None,
               "Vérifier que la plage feed_info reflète bien la période opérationnelle prévue" if total_out_of_range > 0 else None,
               "Synchroniser les dates entre feed_info.txt et les fichiers de calendrier" if total_out_of_range > 0 else None,
               "Maintenir cette cohérence temporelle entre feed_info et les services" if status == "success" else None
           ] if rec is not None
       ]
   }


@audit_function(
    file_type="calendaires",
    name="date_coverage_feed_info",
    genre='validity',
    description="Vérifie que feed_info.txt couvre toutes les dates de calendar.txt et calendar_dates.txt."
)
def date_coverage_feed_info(gtfs_data, **params):
   """
   Vérifie que la période feed_info couvre toutes les dates de service définies dans calendar et calendar_dates
   """
   feed_df = gtfs_data.get('feed_info.txt')
   if feed_df is None:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "feed_info.txt",
                   "count": 1,
                   "affected_ids": [],
                   "message": "Le fichier feed_info.txt est requis pour définir la période de couverture"
               }
           ],
           "result": {None},
           "explanation": {
               "purpose": "Vérifie que la période feed_info couvre toutes les dates de service pour assurer la cohérence temporelle."
           },
           "recommendations": ["Créer le fichier feed_info.txt avec start_date et end_date."]
       }

   calendar_df = gtfs_data.get('calendar.txt', pd.DataFrame())
   calendar_dates_df = gtfs_data.get('calendar_dates.txt', pd.DataFrame())
   
   # Vérification de la présence de données de calendrier
   if calendar_df.empty and calendar_dates_df.empty:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_file",
                   "field": "calendar_data",
                   "count": 2,
                   "affected_ids": [],
                   "message": "Aucune donnée de calendrier disponible (calendar.txt et calendar_dates.txt manquants ou vides)"
               }
           ],
           "result": {
               "feed_info_available": True,
               "calendar_data_available": False
           },
           "explanation": {
               "purpose": "Vérifie que la période feed_info couvre toutes les dates de service pour assurer la cohérence temporelle",
               "context": "Aucune donnée de service à vérifier",
               "impact": "Impossible de valider la couverture temporelle"
           },
           "recommendations": ["Fournir au moins calendar.txt ou calendar_dates.txt pour définir les dates de service."]
       }

   # Vérification des colonnes feed_info
   required_feed_cols = ['start_date', 'end_date']
   missing_feed_cols = [col for col in required_feed_cols if col not in feed_df.columns]
   
   if missing_feed_cols:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "missing_field",
                   "field": "feed_info_dates",
                   "count": len(missing_feed_cols),
                   "affected_ids": [],
                   "message": f"Colonnes de dates manquantes dans feed_info.txt: {', '.join(missing_feed_cols)}"
               }
           ],
           "result": {
               "feed_info_available": True,
               "missing_columns": missing_feed_cols
           },
           "explanation": {
               "purpose": "Vérifie que la période feed_info couvre toutes les dates de service pour assurer la cohérence temporelle",
               "context": "Colonnes de dates manquantes dans feed_info.txt",
               "impact": "Impossible de déterminer la période de couverture"
           },
           "recommendations": [f"Ajouter les colonnes manquantes: {', '.join(missing_feed_cols)}"]
       }

   # Parsing des dates feed_info
   try:
       feed_start = parse_date(feed_df.iloc[0]['start_date'])
       feed_end = parse_date(feed_df.iloc[0]['end_date'])
       
       if feed_start is None or feed_end is None:
           raise ValueError("Dates feed_info non parsables")
           
   except Exception as e:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "invalid_format",
                   "field": "feed_info_dates",
                   "count": 1,
                   "affected_ids": [],
                   "message": f"Format de dates invalide dans feed_info.txt: {str(e)}"
               }
           ],
           "result": {
               "feed_start_raw": str(feed_df.iloc[0].get('start_date', 'N/A')),
               "feed_end_raw": str(feed_df.iloc[0].get('end_date', 'N/A'))
           },
           "explanation": {
               "purpose": "Vérifie que la période feed_info couvre toutes les dates de service pour assurer la cohérence temporelle",
               "context": "Format de dates invalide dans feed_info.txt",
               "impact": "Impossible de parser les dates de couverture"
           },
           "recommendations": ["Corriger le format des dates dans feed_info.txt (utiliser YYYYMMDD)."]
       }

   # Collecte de toutes les dates de service
   service_dates = set()
   parsing_errors = []
   calendar_date_ranges = []

   # Analyse de calendar.txt
   if not calendar_df.empty:
       required_calendar_cols = ['start_date', 'end_date', 'service_id']
       missing_calendar_cols = [col for col in required_calendar_cols if col not in calendar_df.columns]
       
       if not missing_calendar_cols:
           for idx, row in calendar_df.iterrows():
               try:
                   service_start = parse_date(row['start_date'])
                   service_end = parse_date(row['end_date'])
                   
                   if service_start and service_end:
                       calendar_date_ranges.append({
                           'service_id': row['service_id'],
                           'start_date': service_start,
                           'end_date': service_end
                       })
                       
                       # Génération de toutes les dates du service
                       current_date = service_start
                       while current_date <= service_end:
                           service_dates.add(current_date)
                           current_date += pd.Timedelta(days=1)
                   else:
                       parsing_errors.append(f"calendar.txt ligne {idx}: format de date invalide")
                       
               except Exception as e:
                   parsing_errors.append(f"calendar.txt ligne {idx}: {str(e)}")

   # Analyse de calendar_dates.txt
   exception_dates = []
   if not calendar_dates_df.empty and 'date' in calendar_dates_df.columns:
       for idx, row in calendar_dates_df.iterrows():
           try:
               service_date = parse_date(row['date'])
               if service_date:
                   service_dates.add(service_date)
                   exception_dates.append({
                       'service_id': row.get('service_id', f'exception_{idx}'),
                       'date': service_date,
                       'exception_type': row.get('exception_type', 'unknown')
                   })
               else:
                   parsing_errors.append(f"calendar_dates.txt ligne {idx}: format de date invalide")
                   
           except Exception as e:
               parsing_errors.append(f"calendar_dates.txt ligne {idx}: {str(e)}")

   # Gestion des erreurs de parsing
   if parsing_errors:
       return {
           "status": "error",
           "issues": [
               {
                   "type": "invalid_format",
                   "field": "calendar_dates",
                   "count": len(parsing_errors),
                   "affected_ids": [],
                   "message": f"{len(parsing_errors)} erreurs de format de date dans les fichiers de calendrier"
               }
           ],
           "result": {
               "feed_start": feed_start.strftime('%Y-%m-%d'),
               "feed_end": feed_end.strftime('%Y-%m-%d'),
               "parsing_errors": parsing_errors[:10]  # Top 10 erreurs
           },
           "explanation": {
               "purpose": "Vérifie que la période feed_info couvre toutes les dates de service pour assurer la cohérence temporelle",
               "context": "Erreurs de format dans les fichiers de calendrier",
               "impact": "Impossible d'analyser toutes les dates de service"
           },
           "recommendations": ["Corriger les formats de dates dans calendar.txt et calendar_dates.txt (utiliser YYYYMMDD)."]
       }

   # Identification des dates hors couverture
   out_of_range_dates = [d for d in service_dates if d < feed_start or d > feed_end]
   before_feed = [d for d in out_of_range_dates if d < feed_start]
   after_feed = [d for d in out_of_range_dates if d > feed_end]
   
   # Calcul des métriques
   total_service_dates = len(service_dates)
   out_of_range_count = len(out_of_range_dates)
   coverage_rate = round((total_service_dates - out_of_range_count) / total_service_dates * 100, 2) if total_service_dates > 0 else 100
   
   # Analyse temporelle
   if service_dates:
       actual_start = min(service_dates)
       actual_end = max(service_dates)
       service_span_days = (actual_end - actual_start).days + 1
       feed_span_days = (feed_end - feed_start).days + 1
   else:
       actual_start = actual_end = None
       service_span_days = feed_span_days = 0

   # Détermination du statut
   if out_of_range_count == 0:
       status = "success"
   elif coverage_rate >= 95:
       status = "warning"
   else:
       status = "error"

   # Construction des issues
   issues = []
   
   if out_of_range_count > 0:
       issues.append({
           "type": "date_range_violation",
           "field": "service_dates",
           "count": out_of_range_count,
           "affected_ids": [d.strftime('%Y-%m-%d') for d in out_of_range_dates[:100]],
           "message": f"{out_of_range_count} dates de service sont en dehors de la période feed_info"
       })

   return {
       "status": status,
       "issues": issues,
       "result": {
           "feed_period": {
               "start_date": feed_start.strftime('%Y-%m-%d'),
               "end_date": feed_end.strftime('%Y-%m-%d'),
               "duration_days": feed_span_days
           },
           "service_period": {
               "start_date": actual_start.strftime('%Y-%m-%d') if actual_start else None,
               "end_date": actual_end.strftime('%Y-%m-%d') if actual_end else None,
               "duration_days": service_span_days
           },
           "coverage_analysis": {
               "total_service_dates": total_service_dates,
               "dates_in_range": total_service_dates - out_of_range_count,
               "dates_out_of_range": out_of_range_count,
               "coverage_rate": coverage_rate,
               "dates_before_feed": len(before_feed),
               "dates_after_feed": len(after_feed)
           },
           "temporal_details": {
               "calendar_services": len(calendar_date_ranges),
               "exception_dates": len(exception_dates),
               "earliest_out_of_range": min(out_of_range_dates).strftime('%Y-%m-%d') if out_of_range_dates else None,
               "latest_out_of_range": max(out_of_range_dates).strftime('%Y-%m-%d') if out_of_range_dates else None,
               "suggested_feed_start": min(service_dates).strftime('%Y-%m-%d') if service_dates else feed_start.strftime('%Y-%m-%d'),
               "suggested_feed_end": max(service_dates).strftime('%Y-%m-%d') if service_dates else feed_end.strftime('%Y-%m-%d')
           }
       },
       "explanation": {
           "purpose": "Vérifie que la période feed_info couvre toutes les dates de service pour assurer la cohérence temporelle du feed GTFS",
           "context": f"Période feed_info: {feed_start.strftime('%Y-%m-%d')} à {feed_end.strftime('%Y-%m-%d')} ({feed_span_days} jours)",
           "service_analysis": f"Services actifs: {actual_start.strftime('%Y-%m-%d') if actual_start else 'N/A'} à {actual_end.strftime('%Y-%m-%d') if actual_end else 'N/A'} ({total_service_dates} dates)",
           "coverage_summary": f"Couverture: {coverage_rate}% ({out_of_range_count} dates hors période)",
           "impact": (
               f"Couverture temporelle complète : toutes les dates de service respectent la période feed_info" if status == "success"
               else f"Couverture incomplète : {out_of_range_count} dates de service dépassent la période définie"
           )
       },
       "recommendations": [
           rec for rec in [
               f"URGENT: Étendre start_date feed_info à {min(service_dates).strftime('%Y-%m-%d')} pour couvrir {len(before_feed)} dates antérieures" if before_feed else None,
               f"URGENT: Étendre end_date feed_info à {max(service_dates).strftime('%Y-%m-%d')} pour couvrir {len(after_feed)} dates postérieures" if after_feed else None,
               f"Ajuster la période feed_info de {feed_start.strftime('%Y-%m-%d')} à {feed_end.strftime('%Y-%m-%d')} vers {min(service_dates).strftime('%Y-%m-%d')} à {max(service_dates).strftime('%Y-%m-%d')}" if out_of_range_count > total_service_dates * 0.1 else None,
               "Vérifier que les dates de service hors période correspondent à des besoins opérationnels réels" if out_of_range_count > 0 else None,
               "Synchroniser les périodes entre feed_info.txt et les fichiers de calendrier" if out_of_range_count > 0 else None,
               "Optimiser la période feed_info pour correspondre exactement aux services actifs" if coverage_rate < 100 and coverage_rate > 90 else None,
               "Maintenir cette cohérence temporelle parfaite entre feed_info et les services" if status == "success" else None
           ] if rec is not None
       ]
   }
