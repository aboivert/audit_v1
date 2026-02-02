"""
Fonctions d'audit pour le file_type: agency
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="agency",
    name="completion_url",
    description=(
        "Vérifie la complétude des informations essentielles dans agency.txt : "
        "agency_name, agency_phone, agency_email."
    ),
    parameters={
        "min_completion_rate": {
            "type": "slider", 
            "min": 0, 
            "max": 100, 
            "default": 80, 
            "description": "Seuil minimal de complétude (%)"
        }
    }
)
def completion_url(gtfs_data, gtfs_file, **params):
    """Audit de complétude des URLs des agences"""
    min_rate = params.get('min_completion_rate', 80)
    
    if 'agency.txt' not in gtfs_data:
        return 0, []
    
    agency_df = gtfs_data['agency.txt']
    
    if 'agency_url' not in agency_df.columns:
        return 0, []
    
    total_agencies = len(agency_df)
    if total_agencies == 0:
        return 0, []
    
    agencies_with_url = len(agency_df[agency_df['agency_url'].notna()])
    completion_rate = (agencies_with_url / total_agencies) * 100
    
    score = completion_rate
    
    # IDs des agences problématiques
    problem_agencies = agency_df[agency_df['agency_url'].isna()]
    if 'agency_id' in problem_agencies.columns:
        problem_ids = problem_agencies['agency_id'].tolist()
    else:
        problem_ids = problem_agencies.index.tolist()
    
    return {'completion_rate' :score}, problem_ids

@audit_function(
    file_type="agency",
    name="completion_phone",
    description="Vérifie la présence de numéros de téléphone dans les agences"
)
def completion_phone(gtfs_data,  gtfs_file, **params):
    """Audit de complétude des téléphones des agences"""
    if 'agency.txt' not in gtfs_data:
        return 0, []
    
    agency_df = gtfs_data['agency.txt']
    
    if 'agency_phone' not in agency_df.columns:
        return 0, []
    
    total_agencies = len(agency_df)
    if total_agencies == 0:
        return 0, []
    
    agencies_with_phone = len(agency_df[agency_df['agency_phone'].notna()])
    completion_rate = (agencies_with_phone / total_agencies) * 100
    
    score = completion_rate
    
    problem_agencies = agency_df[agency_df['agency_phone'].isna()]
    if 'agency_id' in problem_agencies.columns:
        problem_ids = problem_agencies['agency_id'].tolist()
    else:
        problem_ids = problem_agencies.index.tolist()
    
    return {'completion_rate' :score}, problem_ids

@audit_function(
    file_type="agency",
    name="check_required_columns",
    description="Vérifie la présence des colonnes obligatoires dans agency.txt",
    parameters={}
)
def check_required_columns(gtfs_data,  gtfs_file, **params):
    required_columns = ['agency_name', 'agency_url', 'agency_timezone']
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    missing_columns = [col for col in required_columns if col not in df.columns]
    return {"missing_columns": missing_columns, "all_present": len(missing_columns) == 0}, []

@audit_function(
    file_type="agency",
    name="validate_agency_url",
    description="Vérifie que les URLs dans agency_url sont valides",
    parameters={}
)
def validate_agency_url(gtfs_data,  gtfs_file, **params):
    import re
    url_pattern = re.compile(r'^https?://', re.IGNORECASE)
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    if 'agency_url' not in df.columns:
        return {"missing_column": "agency_url"}
    invalid_urls = df.loc[~df['agency_url'].fillna('').str.match(url_pattern), 'agency_url'].tolist()
    return {
        "invalid_urls_count": len(invalid_urls),
        "invalid_urls": invalid_urls
    },[]

@audit_function(
    file_type="agency",
    name="validate_agency_timezone",
    description="Vérifie que les fuseaux horaires dans agency_timezone sont valides selon IANA",
    parameters={}
)
def validate_agency_timezone(gtfs_data,  gtfs_file, **params):
    import pytz
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    if 'agency_timezone' not in df.columns:
        return {"missing_column": "agency_timezone"}
    valid_timezones = set(pytz.all_timezones)
    invalid_timezones = df.loc[~df['agency_timezone'].isin(valid_timezones), 'agency_timezone'].dropna().unique().tolist()
    return {
        "invalid_timezones_count": len(invalid_timezones),
        "invalid_timezones": invalid_timezones
    },[]

@audit_function(
    file_type="agency",
    name="check_duplicate_agency_id",
    description="Vérifie la présence de doublons dans agency_id",
    parameters={}
)
def check_duplicate_agency_id(gtfs_data,  gtfs_file, **params):
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    if 'agency_id' not in df.columns:
        # Pas d'agence_id, donc pas de doublons possibles
        return {"agency_id_exists": False, "duplicate_count": 0}
    duplicates = df['agency_id'][df['agency_id'].duplicated(keep=False)]
    return {
        "agency_id_exists": True,
        "duplicate_count": duplicates.nunique(),
        "duplicate_values": duplicates.unique().tolist()
    }, []

@audit_function(
    file_type="agency",
    name="missing_values_stats",
    description="Calcule le nombre et le taux de valeurs manquantes par colonne",
    parameters={}
)
def missing_values_stats(gtfs_data,  gtfs_file, **params):
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    empty_counts = df.isna().sum().to_dict()
    empty_rate = {col: round((count / len(df)) * 100, 2) for col, count in empty_counts.items()}
    return {
        "empty_counts": empty_counts,
        "empty_rate": empty_rate,
        "has_missing_values": any(count > 0 for count in empty_counts.values())
    }, []

@audit_function(
    file_type="agency",
    name="check_field_length",
    description="Vérifie que les champs texte ne dépassent pas 255 caractères",
    parameters={}
)
def check_field_length(gtfs_data, max_length=255, **params):
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    too_long = {}
    for col in df.select_dtypes(include=['object']).columns:
        long_vals = df.loc[df[col].notna() & (df[col].str.len() > max_length), col]
        if not long_vals.empty:
            too_long[col] = long_vals.tolist()
    return {
        "fields_with_too_long_values": too_long,
        "max_length": max_length
    }, []

@audit_function(
    file_type="agency",
    name="validate_agency_lang",
    description="Vérifie que agency_lang correspond à un code langue ISO 639-1 valide",
    parameters={}
)
def validate_agency_lang(gtfs_data,  gtfs_file, **params):
    valid_langs = {
        'aa', 'ab', 'ae', 'af', 'ak', 'am', 'an', 'ar', 'as', 'av', 'ay', 'az',
        'ba', 'be', 'bg', 'bh', 'bi', 'bm', 'bn', 'bo', 'br', 'bs', 'ca', 'ce',
        'ch', 'co', 'cr', 'cs', 'cu', 'cv', 'cy', 'da', 'de', 'dv', 'dz', 'ee',
        'el', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'ff', 'fi', 'fj', 'fo', 'fr',
        'fy', 'ga', 'gd', 'gl', 'gn', 'gu', 'gv', 'ha', 'he', 'hi', 'ho', 'hr',
        'ht', 'hu', 'hy', 'hz', 'ia', 'id', 'ie', 'ig', 'ii', 'ik', 'io', 'is',
        'it', 'iu', 'ja', 'jv', 'ka', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'kn',
        'ko', 'kr', 'ks', 'ku', 'kv', 'kw', 'ky', 'la', 'lb', 'lg', 'li', 'ln',
        'lo', 'lt', 'lu', 'lv', 'mg', 'mh', 'mi', 'mk', 'ml', 'mn', 'mr', 'ms',
        'mt', 'my', 'na', 'nb', 'nd', 'ne', 'ng', 'nl', 'nn', 'no', 'nr', 'nv',
        'ny', 'oc', 'oj', 'om', 'or', 'os', 'pa', 'pi', 'pl', 'ps', 'pt', 'qu',
        'rm', 'rn', 'ro', 'ru', 'rw', 'sa', 'sc', 'sd', 'se', 'sg', 'si', 'sk',
        'sl', 'sm', 'sn', 'so', 'sq', 'sr', 'ss', 'st', 'su', 'sv', 'sw', 'ta',
        'te', 'tg', 'th', 'ti', 'tk', 'tl', 'tn', 'to', 'tr', 'ts', 'tt', 'tw',
        'ty', 'ug', 'uk', 'ur', 'uz', 've', 'vi', 'vo', 'wa', 'wo', 'xh', 'yi',
        'yo', 'za', 'zh', 'zu'
    }
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    if 'agency_lang' not in df.columns:
        return {"missing_column": "agency_lang"}
    invalid_langs = df.loc[~df['agency_lang'].isin(valid_langs), 'agency_lang'].dropna().unique().tolist()
    return {
        "invalid_lang_codes_count": len(invalid_langs),
        "invalid_lang_codes": invalid_langs
    }, []

@audit_function(
    file_type="agency",
    name="validate_agency_phone",
    description="Vérifie que agency_phone contient un numéro de téléphone valide",
    parameters={}
)
def validate_agency_phone(gtfs_data,  gtfs_file, **params):
    import re
    phone_pattern = re.compile(r'^[\d\s\+\-\(\)]+$')
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    if 'agency_phone' not in df.columns:
        return {"missing_column": "agency_phone"}
    invalid_phones = df.loc[~df['agency_phone'].fillna('').str.match(phone_pattern), 'agency_phone'].tolist()
    return {
        "invalid_phones_count": len(invalid_phones),
        "invalid_phones": invalid_phones
    }, []

@audit_function(
    file_type="agency",
    name="validate_agency_fare_url",
    description="Vérifie que agency_fare_url, si présent, est une URL valide",
    parameters={}
)
def validate_agency_fare_url(gtfs_data,  gtfs_file, **params):
    import re
    url_pattern = re.compile(r'^https?://', re.IGNORECASE)
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    if 'agency_fare_url' not in df.columns:
        return {"missing_column": "agency_fare_url"}
    invalid_urls = df.loc[~df['agency_fare_url'].fillna('').str.match(url_pattern), 'agency_fare_url'].tolist()
    return {
        "invalid_fare_urls_count": len(invalid_urls),
        "invalid_fare_urls": invalid_urls
    }, []

@audit_function(
    file_type="agency",
    name="check_agency_consistency",
    description="Vérifie la cohérence des infos par agency_id ou agency_name",
    parameters={}
)
def check_agency_consistency(gtfs_data,  gtfs_file, **params):
    if 'agency.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['agency.txt']
    # On utilise agency_id si présent, sinon agency_name
    key = 'agency_id' if 'agency_id' in df.columns else 'agency_name'
    if key not in df.columns:
        return {"missing_key": True}
    grouped = df.groupby(key)
    inconsistencies = {}
    for k, group in grouped:
        # Par exemple, vérifier si agency_url varie pour même agence
        url_vals = group['agency_url'].dropna().unique() if 'agency_url' in group.columns else []
        if len(url_vals) > 1:
            inconsistencies[k] = {
                "agency_url": url_vals.tolist()
            }
    return {
        "inconsistencies_count": len(inconsistencies),
        "inconsistencies": inconsistencies
    }, []

@audit_function(
    file_type="agency",
    name="duplicate_agencies_by_name_contact",
    description="Détecte agences avec noms, adresses et contacts très similaires ou identiques.",
    parameters={}
)
def duplicate_agencies_by_name_contact(gtfs_data,  gtfs_file, **params):
    from difflib import SequenceMatcher
    df = gtfs_data.get('agency')
    if df is None or df.empty:
        return {"duplicate_pairs": []}

    def similar(a, b):
        return SequenceMatcher(None, str(a), str(b)).ratio()

    threshold = 0.9  # 90% similarité

    duplicates = []
    n = len(df)
    for i in range(n):
        for j in range(i + 1, n):
            name_sim = similar(df.iloc[i]['agency_name'], df.iloc[j]['agency_name'])
            url_sim = similar(df.iloc[i].get('agency_url', ''), df.iloc[j].get('agency_url', ''))
            email_sim = similar(df.iloc[i].get('agency_email', ''), df.iloc[j].get('agency_email', ''))
            # On peut aussi comparer phone ou address si présents

            if name_sim > threshold and url_sim > threshold and email_sim > threshold:
                duplicates.append({
                    "agency_1": df.iloc[i].to_dict(),
                    "agency_2": df.iloc[j].to_dict(),
                    "similarity": {
                        "name": name_sim,
                        "url": url_sim,
                        "email": email_sim
                    }
                })

    return {"duplicate_pairs": duplicates}, []

