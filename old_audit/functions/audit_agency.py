"""
Fonctions d'audit pour le file_type: agency
"""

from ..decorators import audit_function
from . import *  # Imports centralisés

@audit_function(
    file_type="agency",
    name="validate_agency_ids",
    genre="validity",
    description="Vérifie que agency_id est présent et complété lorsqu’il y a plusieurs agences, et que les fuseaux horaires sont cohérents.",
    parameters={}
)
def validate_agency_ids(gtfs_data, **params):
    agency_df = gtfs_data.get("agency.txt")  # ← enlever .txt, cohérence
    if agency_df is None:
        return {
            "status": "error",
            "issues": ["Fichier agency.txt manquant"],
            "result": {},  # ← summary → result
            "explanation": {
                "purpose": "Valide la cohérence des identifiants d'agence et des fuseaux horaires."
            },
            "recommendations": ["Fournir le fichier agency.txt."]
        }

    total_agencies = len(agency_df)
    agency_ids = agency_df.get("agency_id", [])
    timezones = agency_df.get("agency_timezone", [])
    
    has_agency_id_column = len(agency_ids) > 0
    has_timezone_column = len(timezones) > 0
    
    # Calcul des problèmes
    missing_ids_count = 0
    missing_ids_positions = []
    
    if has_agency_id_column:
        for i, aid in enumerate(agency_ids):
            if aid is None or str(aid).strip() == "":
                missing_ids_count += 1
                missing_ids_positions.append(f"row_{i}")
    
    # Fuseaux horaires uniques (en excluant les valeurs vides)
    unique_timezones = []
    if has_timezone_column:
        unique_timezones = list(set([tz for tz in timezones if tz and str(tz).strip()]))
    
    timezone_conflict = len(unique_timezones) > 1
    
    # Score calculation (ton système existant)
    score = 100
    issues = []
    
    # Vérif 1 : agency_id présent si plusieurs agences
    if total_agencies > 1:
        if not has_agency_id_column:
            score -= 40
            issues.append({
                "type": "missing_column",
                "field": "agency_id", 
                "count": total_agencies,
                "affected_ids": [f"agency_{i}" for i in range(total_agencies)],
                "message": "Colonne agency_id requise pour plusieurs agences"
            })
        elif missing_ids_count > 0:
            score -= 30
            issues.append({
                "type": "missing_field",
                "field": "agency_id",
                "count": missing_ids_count,
                "affected_ids": missing_ids_positions,
                "message": f"{missing_ids_count} agences sans agency_id"
            })

    # Vérif 2 : fuseaux horaires cohérents
    if has_timezone_column and total_agencies > 1 and timezone_conflict:
        score -= 30
        issues.append({
            "type": "data_inconsistency",
            "field": "agency_timezone",
            "count": len(unique_timezones),
            "affected_ids": [],  # Pourrait être calculé si nécessaire
            "details": unique_timezones,
            "message": f"Fuseaux horaires incohérents: {', '.join(unique_timezones)}"
        })

    # Status basé sur la criticité
    if not has_agency_id_column and total_agencies > 1:
        status = "error"  # Critique : pas d'IDs avec plusieurs agences
    elif missing_ids_count > 0 or timezone_conflict:
        status = "warning" if score >= 70 else "error"
    else:
        status = "success"

    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_agencies": total_agencies,
            "validity_score": score,
            "agency_id_analysis": {
                "has_column": has_agency_id_column,
                "missing_count": missing_ids_count,
                "coverage": f"{total_agencies - missing_ids_count}/{total_agencies}" if has_agency_id_column else "0/0"
            },
            "timezone_analysis": {
                "has_column": has_timezone_column,
                "unique_timezones": len(unique_timezones),
                "has_conflict": timezone_conflict,
                "timezones_list": unique_timezones
            }
        },
        "explanation": {
            "purpose": "Valide la cohérence structurelle des identifiants d'agence et l'uniformité des fuseaux horaires.",
            "validation_rules": {
                "agency_id": "Requis si plusieurs agences sont présentes dans le GTFS",
                "timezone_consistency": "Toutes les agences doivent utiliser le même fuseau horaire"
            },
            "current_status": {
                "id_compliance": "Tous les agency_id sont renseignés" if missing_ids_count == 0 and has_agency_id_column else f"{missing_ids_count} agency_id manquants" if has_agency_id_column else "Colonne agency_id manquante",
                "timezone_consistency": "Fuseau horaire uniforme" if not timezone_conflict else f"{len(unique_timezones)} fuseaux différents détectés"
            },
            "score_breakdown": f"Score: {score}/100 (agency_id: {40 if not has_agency_id_column and total_agencies > 1 else 30 if missing_ids_count > 0 else 0}pts, timezone: {30 if timezone_conflict else 0}pts)"
        },
        "recommendations": [
            rec for rec in [
                "Ajouter une colonne agency_id avec des identifiants uniques pour chaque agence." if not has_agency_id_column and total_agencies > 1 else None,
                "Renseigner les agency_id manquants avec des identifiants uniques." if has_agency_id_column and missing_ids_count > 0 else None,
                "Harmoniser les fuseaux horaires entre toutes les agences du GTFS." if timezone_conflict else None,
                "Vérifier que les fuseaux horaires utilisent la nomenclature IANA (ex: Europe/Paris)." if has_timezone_column and len(unique_timezones) > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="agency",
    name="essential_info_completeness",
    genre='completeness',
    description=(
        "Vérifie la complétude des informations essentielles dans agency.txt : "
        "agency_name, agency_url, agency_timezone."
    ),
    parameters={}
)
def essential_info_completeness(gtfs_data, **params):
    agency_df = gtfs_data.get("agency.txt")
    if agency_df is None:
        return {
            "status": "error",
            "issues": ["Fichier agency.txt manquant."],
            "result": {},  
            "explanation": {
                "purpose": "Vérifie la présence des informations essentielles (nom, site internet, fuseau horaire) pour chaque agence."
            },
            "recommendations": ["Fournir un fichier agency.txt valide."]
        }

    total_agencies = len(agency_df)

    missing_name_agency_ids = missing_field(agency_df, "agency_name", "agency_id")
    missing_url_agency_ids = missing_field(agency_df, "agency_url", "agency_id")
    missing_timezone_agency_ids = missing_field(agency_df, "agency_timezone", "agency_id")

    missing_name_count = len(missing_name_agency_ids)
    missing_url_count = len(missing_url_agency_ids)
    missing_timezone_count = len(missing_timezone_agency_ids)

    # Complétude : agences avec aucun champ manquant sur les 3 essentiels
    complete_agencies = total_agencies - len(
        set(missing_name_agency_ids) | set(missing_url_agency_ids) | set(missing_timezone_agency_ids)
    )
    completeness_score = (complete_agencies / total_agencies) * 100 if total_agencies > 0 else 0

    # Issues plus détaillées avec les IDs
    issues = []
    if missing_name_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "agency_name",
            "count": missing_name_count,
            "affected_ids": missing_name_agency_ids,
            "message": f"{missing_name_count} agence(s) sans nom"
        })
    if missing_url_count > 0:
        issues.append({
            "type": "missing_field", 
            "field": "agency_url",
            "count": missing_url_count,
            "affected_ids": missing_url_agency_ids,
            "message": f"{missing_url_count} agence(s) sans site internet"
        })
    if missing_timezone_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "agency_timezone", 
            "count": missing_timezone_count,
            "affected_ids": missing_timezone_agency_ids,
            "message": f"{missing_timezone_count} agence(s) sans fuseau horaire"
        })

    # Status plus nuancé
    if completeness_score == 100:
        status = "success"
    elif completeness_score >= 80:
        status = "warning"
    else:
        status = "error"

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_agencies": total_agencies,
            "complete_agencies": complete_agencies,
            "score": round(completeness_score, 1),
            "missing_counts": {
                "name": missing_name_count,
                "url": missing_url_count, 
                "timezone": missing_timezone_count
            }
        },
        "explanation": {
            "purpose": "Vérifie la complétude des informations essentielles pour chaque agence de transport selon la norme GTFS.",
            "completeness_definition": "Une agence est considérée comme complète si elle a un nom, un URL vers un site web ET un fuseau horaire.",
            "score_interpretation": f"{completeness_score:.1f}% des agences ont toutes les informations essentielles renseignées."
        },
        "recommendations": [
            rec for rec in [
                "Renseigner le champ agency_name pour toutes les agences." if missing_name_count > 0 else None,
                "Ajouter un numéro de téléphone valide (agency_url) aux agences concernées." if missing_url_count > 0 else None,
                "Ajouter une adresse email valide (agency_timezone) aux agences concernées." if missing_timezone_count > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="agency",
    name="url_validity",
    genre="quality",
    description=(
        "Vérifie la validité des URLs renseignées dans agency.txt. "
        "Identifie les agences avec des URLs mal formées."
    ),
    parameters={}
)
def url_validity(gtfs_data, **params):
    agency_df = gtfs_data.get("agency.txt")
    #print(agency_df["agency_fare_url"])
    if agency_df is None:
        return {
            "status": "error",
            "issues": ["Fichier agency.txt manquant."],
            "result": {},
            "explanation": {
                "purpose": "Vérifie la validité des URLs des agences de transport."
            },
            "recommendations": ["Fournir un fichier agency.txt valide."]
        }

    total_agencies = len(agency_df)
    agency_ids = agency_df.get("agency_id", [f"agency_{i}" for i in range(total_agencies)])
    urls = agency_df.get("agency_url", [""] * total_agencies)
    fare_urls = agency_df.get("agency_fare_url", [""] * total_agencies)
    print(fare_urls)
    print(pd.na)

    # On vérifie uniquement les URLs non vides
    checked_urls = [(aid, url) for aid, url in zip(agency_ids, urls) if url and str(url).strip() != np.nan]
    checked_fare_urls = [(aid, url) for aid, url in zip(agency_ids, fare_urls) if url and str(url).strip() != np.nan]

    invalid_urls_agency_ids = [aid for aid, url in checked_urls if not is_valid_url(str(url))]
    empty_urls_agency_ids = [aid for aid, url in zip(agency_ids, urls) if not url or str(url).strip() == np.nan]
    invalid_fare_urls_agency_ids = [aid for aid, url in checked_fare_urls if not is_valid_url(str(url))]
    empty_fare_urls_agency_ids = [aid for aid, url in zip(agency_ids, fare_urls) if not url or str(url).strip() == np.nan]

    invalid_urls_count = len(invalid_urls_agency_ids)
    checked_urls_count = len(checked_urls)
    empty_urls_count = len(empty_urls_agency_ids)
    valid_urls_count = checked_urls_count - invalid_urls_count
    invalid_fare_urls_count = len(invalid_fare_urls_agency_ids)
    checked_fare_urls_count = len(checked_fare_urls)
    empty_fare_urls_count = len(empty_fare_urls_agency_ids)
    valid_fare_urls_count = checked_fare_urls_count - invalid_fare_urls_count

    issues = []
    if invalid_urls_count > 0:
        issues.append({
            "type": "invalid_format",
            "field": "agency_url", 
            "count": invalid_urls_count,
            "affected_ids": invalid_urls_agency_ids,
            "message": f"{invalid_urls_count} URL(s) mal formée(s)"
        })
    if empty_urls_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "agency_url",
            "count": empty_urls_count, 
            "affected_ids": empty_urls_agency_ids,
            "message": f"{empty_urls_count} agence(s) sans URL(s)"
        })
    if invalid_fare_urls_count > 0:
        issues.append({
            "type": "invalid_format",
            "field": "agency_fare_url", 
            "count": invalid_fare_urls_count,
            "affected_ids": invalid_fare_urls_agency_ids,
            "message": f"{invalid_fare_urls_count} URL(s) tarifaire(s) mal formée(s)"
        })
    if empty_fare_urls_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "agency_fare_url",
            "count": empty_fare_urls_count, 
            "affected_ids": empty_fare_urls_agency_ids,
            "message": f"{empty_fare_urls_count} agence(s) sans URL(s) tarifaire(s)"
        })
    
    # Option 1 : Status basé sur le pire cas
    if total_agencies == 0:
        status = "error"
    elif (invalid_urls_count > 0) or (invalid_fare_urls_count > 0):
        status = "error"  # Dès qu'il y a une URL invalide
    elif (empty_urls_count > 0) or (empty_fare_urls_count > 0):
        status = "warning"  # Des URLs manquantes mais celles renseignées sont valides
    else:
        status = "success"  # Tout est parfait

    # Paramétrer en arguments !
    # Option 3 : Status différencié par importance, url > fare_url
    # if invalid_urls_count > 0:  # agency_url plus critique
    #     status = "error"
    # elif invalid_fare_urls_count > 0:
    #     status = "warning"  # fare_url moins critique
    # elif empty__urls_count > 0:
    #     status = "warning"
    # else:
    #     status = "success"

    explanation = {
        "checked_urls": f"{checked_urls_count} URL(s) vérifiées.",
        "invalid_urls": f"{invalid_urls_count} URL(s) mal formées.",
        "checked_fare_urls": f"{checked_fare_urls_count} URL(s) tarifaire(s) vérifiées.",
        "invalid_fare_urls": f"{invalid_fare_urls_count} URL(s) tarifaire(s) mal formées."
    }

    recommendations = []
    if invalid_urls_count > 0:
        recommendations.append("Corriger les URLs mal formées pour les agences concernées.")
    if invalid_fare_urls_count > 0:
        recommendations.append("Corriger les URLs tarifaires mal formées pour les agences concernées.")
    validity_score_url = (valid_urls_count / checked_urls_count * 100) if checked_urls_count > 0 else 0
    validity_score_fare_url = (valid_fare_urls_count / checked_fare_urls_count * 100) if checked_fare_urls_count > 0 else 0
    validity_score = (validity_score_url+validity_score_fare_url)/2

    return {
        "status": status,
        "issues": issues,
        "result": {
            "total_agencies": total_agencies,
            "checked_urls": checked_urls_count,
            "valid_urls": valid_urls_count,
            "invalid_urls": invalid_urls_count,
            "empty_urls": empty_urls_count,
            "validity_score_url": round(validity_score_url, 1),
            "checked_fare_urls": checked_fare_urls_count,
            "valid_fare_urls": valid_fare_urls_count,
            "invalid_fare_urls": invalid_fare_urls_count,
            "empty_fare_urls": empty_fare_urls_count,
            "validity_score_fare_url": round(validity_score_fare_url, 1),
            "score": round(validity_score,1)
        },
        "explanation": {
            "purpose": "Vérifie la validité des URLs des sites web et tarifaires des agences de transport.",
            "validation_rules": "Une URL est considérée comme valide si elle respecte le format standard (http/https).",
            "score_interpretation": f"{validity_score:.1f}% des URLs renseignées sont valides.",
            "coverage_url": f"{checked_urls_count}/{total_agencies} agences ont une URL renseignée.",
            "coverage_fare_url": f"{checked_fare_urls_count}/{total_agencies} agences ont une URL tarifaire renseignée.",
        },
        "recommendations": [
            rec for rec in [
                "Corriger les URLs mal formées pour les agences concernées." if invalid_urls_count > 0 else None,
                "Ajouter des URLs de sites web pour les agences qui n'en ont pas." if empty_urls_count > 0 else None,
                "Vérifier que les URLs pointent vers des sites web actifs." if valid_urls_count > 0 else None,
                "Corriger les URLs tarifaires mal formées pour les agences concernées." if invalid_fare_urls_count > 0 else None,
                "Ajouter des URLs tarifaires de sites web pour les agences qui n'en ont pas." if empty_fare_urls_count > 0 else None,
                "Vérifier que les URLs tarifaires pointent vers des sites web actifs." if valid_fare_urls_count > 0 else None,
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="agency",
    name="check_text_quality",
    genre="qualiity",
    description=(
        "Analyse la qualité des textes dans agency.txt, notamment : "
        "usage cohérent des majuscules, présence d'abréviations courantes, "
        "caractères spéciaux non conformes, et lisibilité générale."
    ),
    parameters={}
)
def check_text_quality(gtfs_data, **params):
    ##default_abbr = ["St.", "Ste.", "Av.", "Bd.", "SNCF", "RATP", ...]
    ##common_abbr = params.get('abbreviations', default_abbr)
    agency_df = gtfs_data.get("agency.txt")
    if agency_df is None:
        return {
            "status": "error",
            "issues": ["Fichier agency.txt manquant."],
            "result": {},  # ← summary → result
            "explanation": {
                "purpose": "Analyse la qualité textuelle des noms d'agences de transport."
            },
            "recommendations": ["Fournir un fichier agency.txt valide."]
        }
    
    total_rows = len(agency_df)
    names = agency_df.get("agency_name", [""] * total_rows)
    agency_ids = agency_df.get("agency_id", [f"agency_{i}" for i in range(total_rows)])
    
    uppercase_name_count = 0
    uppercase_name_agency_ids = []
    abbreviations_found = set()
    abbreviations_agency_ids = set()
    special_chars_count = 0
    special_chars_agency_ids = []
    readability_issues_count = 0
    readability_issues_agency_ids = []
    
    common_abbr = [
        # Voies françaises
        "St.", "Ste.", "Av.", "Bd.", "Pl.", "R.", "Rte.", "Ch.", "All.", "Imp.",
        # Transport
        "SNCF", "RATP", "TER", "RER", "Tram", "Bus", "Métro", "Cie", "Transp.",
        # Lieux publics
        "Gare", "Aérop.", "Univ.", "Hôp.", "Ctr.", "ZI.", "ZA.", "Préf.",
        # Géographie
        "N.", "S.", "E.", "O.", "dept.", "Rég.", "Com.",
        # Directions supplémentaires
        "NE.", "NO.", "SE.", "SO.",
        # Régions
        "IdF", "PACA"
    ]
        
    special_chars_pattern = re.compile(r"[^a-zA-Z0-9\s.,'-]")
    uppercase_pattern = re.compile(r"^[A-Z\s'-]+$")
    
    for aid, name in zip(agency_ids, names):
        if not isinstance(name, str) or name.strip() == "":
            continue
        
        # Majuscules uniquement
        if uppercase_pattern.match(name.strip()):
            uppercase_name_count += 1
            uppercase_name_agency_ids.append(aid)
        
        # Chercher abréviations
        for abbr in common_abbr:
            if abbr in name:
                abbreviations_found.add(abbr)
                abbreviations_agency_ids.add(aid)
        
        # Caractères spéciaux
        if special_chars_pattern.search(name):
            special_chars_count += 1
            special_chars_agency_ids.append(aid)
        
        # Lisibilité simple : ponctuation répétée
        if re.search(r"([!?.])\1{2,}", name):
            readability_issues_count += 1
            readability_issues_agency_ids.append(aid)
    
    # Calcul score qualité
    score = 100
    if uppercase_name_count > 0:
        score -= 10
    if len(abbreviations_found) > 0:
        score -= 10
    if special_chars_count > 0:
        score -= 10
    if readability_issues_count > 0:
        score -= 10
    score = max(score, 0)
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if uppercase_name_count > 0:
        issues.append({
            "type": "text_quality",
            "category": "uppercase",
            "count": uppercase_name_count,
            "affected_ids": uppercase_name_agency_ids,
            "message": f"{uppercase_name_count} noms d'agences en majuscules intégrales"
        })
    
    if abbreviations_found:
        issues.append({
            "type": "text_quality", 
            "category": "abbreviations",
            "count": len(abbreviations_agency_ids),
            "affected_ids": sorted(abbreviations_agency_ids),
            "details": sorted(abbreviations_found),
            "message": f"Abréviations détectées: {', '.join(sorted(abbreviations_found))}"
        })
    
    if special_chars_count > 0:
        issues.append({
            "type": "text_quality",
            "category": "special_chars", 
            "count": special_chars_count,
            "affected_ids": special_chars_agency_ids,
            "message": f"{special_chars_count} noms avec caractères spéciaux non standards"
        })
    
    if readability_issues_count > 0:
        issues.append({
            "type": "text_quality",
            "category": "readability",
            "count": readability_issues_count,
            "affected_ids": readability_issues_agency_ids,
            "message": f"{readability_issues_count} noms avec ponctuation excessive"
        })
    
    # Status plus nuancé ← NOUVEAU
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
            "total_agencies": total_rows,
            "quality_score": score,
            "issues_summary": {
                "uppercase_names": uppercase_name_count,
                "abbreviations_count": len(abbreviations_found),
                "special_chars": special_chars_count, 
                "readability_issues": readability_issues_count
            }
        },
        "explanation": {
            "purpose": "Analyse la qualité textuelle des noms d'agences pour identifier les problèmes de lisibilité et de standardisation.",
            "scoring": "Score calculé en retirant 10 points par type d'anomalie détectée (plancher à 0).",
            "quality_assessment": f"Score de qualité textuelle: {score}/100",
            "detected_issues": {
                "uppercase": f"{uppercase_name_count} noms entièrement en majuscules",
                "abbreviations": f"{len(abbreviations_found)} types d'abréviations détectées", 
                "special_chars": f"{special_chars_count} noms avec caractères spéciaux",
                "readability": f"{readability_issues_count} noms avec ponctuation excessive"
            }
        },
        "recommendations": [
            rec for rec in [
                "Uniformiser la casse des noms d'agences (privilégier la casse normale)." if uppercase_name_count > 0 else None,
                "Clarifier ou développer les abréviations pour éviter les ambiguïtés." if abbreviations_found else None,
                "Éviter les caractères spéciaux non standards dans les noms d'agences." if special_chars_count > 0 else None,
                "Limiter l'usage excessif de ponctuation pour améliorer la lisibilité." if readability_issues_count > 0 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="agency",
    name="validate_phone_formats",
    genre="quality",
    description=(
        "Vérifie la structure et la logique des numéros de téléphone dans agency.txt. "
        "Détecte les numéros manquants, mal formés ou peu plausibles."
    ),
    parameters={}
)
def validate_phone_formats(gtfs_data, **params):
    agency_df = gtfs_data.get("agency.txt")  # ← enlever .txt
    if agency_df is None:
        return {
            "status": "error",
            "issues": ["Fichier agency.txt manquant."],
            "result": {},  # ← summary → result
            "explanation": {
                "purpose": "Valide le format des numéros de téléphone des agences de transport."
            },
            "recommendations": ["Fournir un fichier agency.txt valide."]
        }
    
    total_rows = len(agency_df)
    phones = agency_df.get("agency_phone", [""] * total_rows)
    agency_ids = agency_df.get("agency_id", [f"agency_{i}" for i in range(total_rows)])
    
    missing_phone_agency_ids = []
    invalid_phone_agency_ids = []
    suspicious_phone_agency_ids = []
    valid_phone_agency_ids = []  # ← AJOUTER pour tracking
    
    # Modèle simple international + local
    phone_pattern = re.compile(r"^\+?[0-9\s\-\(\)]{6,20}$")
    
    for aid, phone in zip(agency_ids, phones):
        if phone is None or str(phone).strip() == "":
            missing_phone_agency_ids.append(aid)
            continue
        phone_str = str(phone).strip()
        
        # Vérification format global
        if not phone_pattern.match(phone_str):
            invalid_phone_agency_ids.append(aid)
            continue
        
        # Nettoyage pour analyse longitudinale
        digits_only = re.sub(r"\D", "", phone_str)
        
        # Longueur plausible (au moins 6 chiffres, max 15 chiffres)
        if len(digits_only) < 6 or len(digits_only) > 15:
            suspicious_phone_agency_ids.append(aid)
            continue
        
        # Si on arrive ici, le téléphone est valide
        valid_phone_agency_ids.append(aid)  # ← AJOUTER
    
    missing_phone_count = len(missing_phone_agency_ids)
    invalid_phone_count = len(invalid_phone_agency_ids)
    suspicious_phone_count = len(suspicious_phone_agency_ids)
    valid_phone_count = len(valid_phone_agency_ids)  # ← AJOUTER
    
    # Calcul du score qualité
    score = 100
    if missing_phone_count > 0:
        score -= 15
    if invalid_phone_count > 0:
        score -= 15
    if suspicious_phone_count > 0:
        score -= 15
    score = max(score, 0)
    
    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if missing_phone_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "agency_phone",
            "count": missing_phone_count,
            "affected_ids": missing_phone_agency_ids,
            "message": f"{missing_phone_count} agences sans numéro de téléphone"
        })
    
    if invalid_phone_count > 0:
        issues.append({
            "type": "invalid_format",
            "field": "agency_phone", 
            "count": invalid_phone_count,
            "affected_ids": invalid_phone_agency_ids,
            "message": f"{invalid_phone_count} numéros mal formés"
        })
    
    if suspicious_phone_count > 0:
        issues.append({
            "type": "suspicious_data",
            "field": "agency_phone",
            "count": suspicious_phone_count,
            "affected_ids": suspicious_phone_agency_ids,
            "message": f"{suspicious_phone_count} numéros suspects (longueur improbable)"
        })
    
    # Status plus nuancé ← NOUVEAU
    if score == 100:
        status = "success"
    elif score >= 70:  # Plus strict car téléphone important
        status = "warning"
    else:
        status = "error"
    
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_agencies": total_rows,
            "valid_phones": valid_phone_count,
            "phone_quality_score": score,
            "issues_summary": {
                "missing": missing_phone_count,
                "invalid_format": invalid_phone_count,
                "suspicious": suspicious_phone_count
            }
        },
        "explanation": {
            "purpose": "Valide le format et la plausibilité des numéros de téléphone des agences de transport.",
            "validation_rules": "Format accepté: +33123456789, 01.23.45.67.89, (123) 456-7890, etc. Longueur: 6-15 chiffres.",
            "scoring": "Score calculé en retirant 15 points par type de problème détecté (plancher à 0).",
            "quality_assessment": f"Score de qualité téléphonique: {score}/100",
            "coverage": f"{total_rows - missing_phone_count}/{total_rows} agences ont un numéro renseigné",
            "validity_rate": f"{valid_phone_count}/{total_rows - missing_phone_count} numéros renseignés sont valides" if total_rows - missing_phone_count > 0 else "Aucun numéro renseigné"
        },
        "recommendations": [
            rec for rec in [
                "Ajouter un numéro de téléphone valide pour chaque agence." if missing_phone_count > 0 else None,
                "Corriger les numéros mal formés pour respecter un format standard international." if invalid_phone_count > 0 else None,
                "Vérifier la plausibilité des numéros suspects (longueur, indicatif pays, etc.)." if suspicious_phone_count > 0 else None,
                "Considérer l'ajout d'indicatifs pays pour améliorer la compatibilité internationale." if valid_phone_count > 0 and score < 100 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="agency",
    name="check_accessibility",
    genre="accessibility",
    description=(
        "Vérifie l’accessibilité des données dans agency.txt : "
        "présence et lisibilité des champs textes obligatoires, "
        "gestion des encodages, et détection de champs vides ou illisibles."
    ),
    parameters={}
)
def check_accessibility(gtfs_data, **params):
    agency_df = gtfs_data.get("agency.txt")  # ← enlever .txt
    if agency_df is None:
        return {
            "status": "error",
            "issues": ["Fichier agency.txt manquant."],
            "result": {},  # ← summary → result
            "explanation": {
                "purpose": "Vérifie l'accessibilité linguistique et la lisibilité des informations des agences."
            },
            "recommendations": ["Fournir un fichier agency.txt valide."]
        }

    total_agencies = len(agency_df)
    agency_ids = agency_df.get("agency_id", [f"agency_{i}" for i in range(total_agencies)])
    names = agency_df.get("agency_name", [""] * total_agencies)
    langs = agency_df.get("agency_lang", [None] * total_agencies)

    empty_name_agency_ids = []
    unreadable_name_agency_ids = []
    empty_lang_agency_ids = []
    valid_accessibility_agency_ids = []  # ← AJOUTER

    for aid, name, lang in zip(agency_ids, names, langs):
        has_issues = False
        
        if not name or str(name).strip() == "":
            empty_name_agency_ids.append(aid)
            has_issues = True
        elif not is_readable(str(name)):
            unreadable_name_agency_ids.append(aid)
            has_issues = True
            
        if lang is not None:
            lang_str = str(lang).strip()
            if lang_str == "" or len(lang_str) != 2:
                empty_lang_agency_ids.append(aid)
                has_issues = True
        else:
            empty_lang_agency_ids.append(aid)
            has_issues = True
        
        # Si aucun problème, c'est accessible
        if not has_issues:
            valid_accessibility_agency_ids.append(aid)

    empty_name_count = len(empty_name_agency_ids)
    unreadable_name_count = len(unreadable_name_agency_ids)
    empty_lang_count = len(empty_lang_agency_ids)
    valid_accessibility_count = len(valid_accessibility_agency_ids)

    # Calcul du score (ton système existant)
    score = 100
    score -= empty_name_count * 20
    score -= unreadable_name_count * 20
    score -= empty_lang_count * 10
    score = max(score, 0)

    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    if empty_name_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "agency_name",
            "count": empty_name_count,
            "affected_ids": empty_name_agency_ids,
            "message": f"{empty_name_count} agences sans nom valide"
        })
    
    if unreadable_name_count > 0:
        issues.append({
            "type": "data_quality",
            "field": "agency_name",
            "count": unreadable_name_count,
            "affected_ids": unreadable_name_agency_ids,
            "message": f"{unreadable_name_count} noms avec caractères non lisibles"
        })
    
    if empty_lang_count > 0:
        issues.append({
            "type": "missing_field",
            "field": "agency_lang",
            "count": empty_lang_count,
            "affected_ids": empty_lang_agency_ids,
            "message": f"{empty_lang_count} codes langue manquants ou invalides"
        })

    # Status basé sur ton système existant ← GARDER TA LOGIQUE
    if score >= 90:
        status = "success"  # ← ok → success
    elif score >= 60:
        status = "warning"
    else:
        status = "error"
    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_agencies": total_agencies,
            "accessible_agencies": valid_accessibility_count,
            "accessibility_score": score,
            "issues_summary": {
                "empty_names": empty_name_count,
                "unreadable_names": unreadable_name_count,
                "missing_languages": empty_lang_count
            }
        },
        "explanation": {
            "purpose": "Évalue l'accessibilité linguistique et la lisibilité des informations des agences de transport.",
            "accessibility_criteria": "Une agence est accessible si elle a un nom lisible ET un code langue ISO 639-1 valide.",
            "scoring": "Score: 100 - (noms vides × 20) - (noms illisibles × 20) - (langues manquantes × 10)",
            "quality_assessment": f"Score d'accessibilité: {score}/100",
            "coverage": f"{valid_accessibility_count}/{total_agencies} agences sont pleinement accessibles",
            "language_standard": "Les codes langue doivent respecter la norme ISO 639-1 (2 lettres: fr, en, es, etc.)"
        },
        "recommendations": [
            rec for rec in [
                "Compléter le champ agency_name avec un nom valide pour toutes les agences." if empty_name_count > 0 else None,
                "Corriger les caractères non lisibles dans les noms d'agences." if unreadable_name_count > 0 else None,
                "Fournir un code langue ISO 639-1 valide (2 lettres) dans agency_lang." if empty_lang_count > 0 else None,
                "Considérer l'ajout de traductions pour améliorer l'accessibilité multilingue." if valid_accessibility_count > 0 and score < 100 else None
            ] if rec is not None
        ]
    }

@audit_function(
    file_type="agency",
    name="agency_statistical_summary",
    genre="statistics",
    description=(
        "Génère un résumé statistique des données présentes dans agency.txt : "
        "compte des agences, analyse des valeurs uniques, distribution des langues, "
        "et longueur moyenne des champs textes."
    ),
    parameters={}
)
def agency_statistical_summary(gtfs_data, **params):
    agency_df = gtfs_data.get("agency.txt")  # ← enlever .txt
    if agency_df is None or len(agency_df) == 0:
        return {
            "status": "error",
            "issues": ["Fichier agency.txt manquant ou vide."],
            "result": {},  # ← summary → result
            "explanation": {
                "purpose": "Fournit un résumé statistique des agences de transport présentes dans le GTFS."
            },
            "recommendations": ["Fournir un fichier agency.txt valide et non vide."]
        }

    total_agencies = len(agency_df)
    agency_ids = agency_df.get("agency_id", [f"agency_{i}" for i in range(total_agencies)])
    agency_names = agency_df.get("agency_name", [""] * total_agencies)
    agency_langs = agency_df.get("agency_lang", [None] * total_agencies)
    agency_urls = agency_df.get("agency_url", [""] * total_agencies)

    unique_agency_ids = len(set(agency_ids))
    unique_agency_names = len(set(agency_names))

    # Distribution des langues (agency_lang), sans compter None ou vide
    lang_counts = {}
    for lang in agency_langs:
        if lang and str(lang).strip():
            key = str(lang).strip()
            lang_counts[key] = lang_counts.get(key, 0) + 1

    avg_name_length = sum(len(str(name)) for name in agency_names) / total_agencies
    avg_url_length = sum(len(str(url)) for url in agency_urls) / total_agencies

    # Issues structurées ← NOUVEAU FORMAT
    issues = []
    duplicate_ids_count = total_agencies - unique_agency_ids
    duplicate_names_count = total_agencies - unique_agency_names
    
    if duplicate_ids_count > 0:
        issues.append({
            "type": "data_integrity",
            "field": "agency_id",
            "count": duplicate_ids_count,
            "affected_ids": [],  # Pourrait être calculé si nécessaire
            "message": f"{duplicate_ids_count} doublons dans agency_id"
        })
    
    if duplicate_names_count > 0:
        issues.append({
            "type": "data_quality",
            "field": "agency_name", 
            "count": duplicate_names_count,
            "affected_ids": [],
            "message": f"{duplicate_names_count} noms d'agences dupliqués"
        })
    
    if not lang_counts:
        issues.append({
            "type": "missing_field",
            "field": "agency_lang",
            "count": total_agencies,
            "affected_ids": agency_ids,
            "message": "Aucun code langue renseigné"
        })

    # Status basé sur la qualité des données ← NOUVEAU
    if duplicate_ids_count > 0:
        status = "error"  # IDs dupliqués = critique
    elif duplicate_names_count > 0 or not lang_counts:
        status = "warning"  # Noms dupliqués ou langues manquantes
    else:
        status = "success"

    return {
        "status": status,
        "issues": issues,
        "result": {  # ← summary → result
            "total_agencies": total_agencies,
            "data_quality": {
                "unique_ids": unique_agency_ids,
                "unique_names": unique_agency_names,
                "duplicate_ids": duplicate_ids_count,
                "duplicate_names": duplicate_names_count
            },
            "language_distribution": lang_counts,
            "content_stats": {
                "avg_name_length": round(avg_name_length, 1),
                "avg_url_length": round(avg_url_length, 1),
                "languages_count": len(lang_counts)
            }
        },
        "explanation": {
            "purpose": "Fournit un résumé statistique complet des agences de transport présentes dans le GTFS.",
            "data_overview": f"Analyse de {total_agencies} agences avec {unique_agency_ids} identifiants uniques",
            "quality_indicators": {
                "id_uniqueness": f"{unique_agency_ids}/{total_agencies} identifiants uniques ({(unique_agency_ids/total_agencies*100):.1f}%)",
                "name_diversity": f"{unique_agency_names} noms distincts sur {total_agencies} agences",
                "language_coverage": f"{len(lang_counts)} langues différentes renseignées" if lang_counts else "Aucune langue renseignée"
            },
            "content_analysis": {
                "name_length": f"Longueur moyenne des noms: {avg_name_length:.1f} caractères",
                "url_length": f"Longueur moyenne des URLs: {avg_url_length:.1f} caractères",
                "most_common_language": max(lang_counts.items(), key=lambda x: x[1])[0] if lang_counts else None
            }
        },
        "recommendations": [
            rec for rec in [
                "Corriger les identifiants d'agence dupliqués pour éviter les conflits dans le GTFS." if duplicate_ids_count > 0 else None,
                "Examiner les noms d'agences dupliqués pour éviter les confusions utilisateur." if duplicate_names_count > 0 else None,
                "Ajouter des codes langue ISO 639-1 (agency_lang) pour améliorer l'accessibilité." if not lang_counts else None,
                "Considérer l'ajout de plus de langues pour une meilleure couverture multilingue." if len(lang_counts) == 1 else None,
                "Vérifier la cohérence des longueurs de noms si certains semblent trop courts ou trop longs." if avg_name_length < 5 or avg_name_length > 50 else None
            ] if rec is not None
        ]
    }