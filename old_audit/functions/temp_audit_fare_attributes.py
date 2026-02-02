"""
Fonctions d'audit pour le file_type: fare_attributes
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="fare_attributes",
    name="duplicate_fare_attributes",
    description="Détecte des fare_attributes dupliqués sur tarif, devise, transfert, etc.",
    parameters={}
)
def duplicate_fare_attributes(gtfs_data, **params):
    df = gtfs_data.get('fare_attributes')
    if df is None or df.empty:
        return {"duplicate_count": 0, "duplicates": []}
    subset_cols = ['price', 'currency_type', 'payment_method', 'transfers', 'transfer_duration']
    duplicated = df.duplicated(subset=subset_cols, keep=False)
    duplicates_df = df[duplicated]
    count = len(duplicates_df)
    duplicates_list = duplicates_df.to_dict(orient='records')
    return {"duplicate_count": count, "duplicates": duplicates_list}

@audit_function(
    file_type="fare_attributes",
    name="fare_attributes_unused",
    description="Fare_attributes non utilisés dans fare_rules.",
    parameters={}
)
def fare_attributes_unused(gtfs_data, **params):
    fare_attributes = gtfs_data.get('fare_attributes.txt')
    fare_rules = gtfs_data.get('fare_rules.txt')
    if fare_attributes is None:
        return {}
    if fare_rules is None:
        # Si pas de fare_rules, tous sont inutilisés
        return {
            "unused_fare_attributes_count": len(fare_attributes),
            "unused_fare_ids": fare_attributes['fare_id'].tolist()
        }
    used_fare_ids = set(fare_rules['fare_id'])
    unused = fare_attributes[~fare_attributes['fare_id'].isin(used_fare_ids)]
    return {
        "unused_fare_attributes_count": len(unused),
        "unused_fare_ids": unused['fare_id'].tolist()
    }



@audit_function(
    file_type="fare_attributes",
    name="validate_fare_prices_and_currency",
    description="Vérifie la validité du champ price (numérique positif) et currency_type (ISO 4217).",
    parameters={}
)
def validate_fare_prices_and_currency(gtfs_data, **params):
    import pycountry
    df = gtfs_data.get('fare_attributes')
    if df is None or df.empty:
        return {
            "status": "ok",
            "issues": [],
            "invalid_prices_count": 0,
            "invalid_currency_count": 0,
            "explanation": "Pas de données fare_attributes pour analyse.",
            "recommendations": []
        }
    invalid_prices = df[(pd.to_numeric(df['price'], errors='coerce').isna()) | (df['price'].astype(float) < 0)]
    valid_currencies = {c.alpha_3 for c in pycountry.currencies}
    invalid_currency = df[~df['currency_type'].isin(valid_currencies)]

    issues = []
    if not invalid_prices.empty:
        issues.append(f"{len(invalid_prices)} prix invalides détectés (non numériques ou négatifs).")
    if not invalid_currency.empty:
        issues.append(f"{len(invalid_currency)} currency_type invalides détectées (ISO 4217 non respectées).")

    status = "ok" if len(issues) == 0 else "error"

    return {
        "status": status,
        "issues": issues,
        "invalid_prices_count": len(invalid_prices),
        "invalid_currency_count": len(invalid_currency),
        "problem_ids": {
            "invalid_prices": invalid_prices['fare_id'].tolist(),
            "invalid_currency": invalid_currency['fare_id'].tolist()
        },
        "explanation": "Validation des prix et devises dans fare_attributes.",
        "recommendations": ["Corriger les prix et codes devises invalides."]
    }
@audit_function(
    file_type="fare_rules",
    name="validate_fare_rules_reference",
    description="Vérifie que les IDs fare_id référencés dans fare_rules existent bien dans fare_attributes.",
    parameters={}
)
def validate_fare_rules_reference(gtfs_data, **params):
    fare_attributes = gtfs_data.get('fare_attributes')
    fare_rules = gtfs_data.get('fare_rules')
    if fare_attributes is None or fare_attributes.empty:
        return {"status": "error", "issues": ["fare_attributes.txt manquant ou vide."], "problem_ids": []}
    if fare_rules is None or fare_rules.empty:
        return {"status": "ok", "issues": [], "problem_ids": [], "explanation": "Pas de règles tarifaires à valider."}
    
    fare_ids = set(fare_attributes['fare_id'])
    referenced_ids = set(fare_rules['fare_id'].dropna())

    invalid_ids = referenced_ids - fare_ids

    status = "ok" if len(invalid_ids) == 0 else "error"
    issues = [] if status == "ok" else [f"{len(invalid_ids)} fare_id dans fare_rules non définis dans fare_attributes."]
    
    return {
        "status": status,
        "issues": issues,
        "invalid_fare_ids_in_rules": list(invalid_ids),
        "explanation": "Vérification de la correspondance entre fare_rules et fare_attributes.",
        "recommendations": ["Corriger les fare_id inexistants dans fare_attributes."]
    }

@statistics_function(
    category="fare_attributes",
    name="fare_price_distribution_by_currency",
    description="Distribution des prix des fares par devise."
)
def fare_price_distribution_by_currency(gtfs_data, **params):
    df = gtfs_data.get('fare_attributes')
    if df is None or df.empty:
        return {"explanation": "Pas de données fare_attributes à analyser.", "counts_by_currency": {}}, []

    try:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    except Exception:
        return {"explanation": "Erreur lors de la conversion des prix.", "counts_by_currency": {}}, []

    grouped = df.groupby('currency_type')['price']
    summary = {}
    for currency, prices in grouped:
        summary[currency] = {
            "count": len(prices),
            "min_price": prices.min(),
            "max_price": prices.max(),
            "mean_price": round(prices.mean(), 2),
            "median_price": prices.median()
        }
    explanation = f"Analyse des prix par devise pour {len(df)} fares."
    return {
        "counts_by_currency": summary,
        "explanation": explanation
    }, []

@audit_function(
    category="fare_rules",
    name="fare_rules_usage_by_entity",
    genre='statistics',
    description="Nombre de règles tarifaires par type d’entité ciblée (zone, route, etc.)."
)
def fare_rules_usage_by_entity(gtfs_data, **params):
    df = gtfs_data.get('fare_rules')
    if df is None or df.empty:
        return {"explanation": "Pas de données fare_rules à analyser.", "count_by_entity": {}}, []

    columns = ['route_id', 'origin_id', 'destination_id', 'contains_id']
    present_cols = [c for c in columns if c in df.columns]
    if not present_cols:
        return {"explanation": "fare_rules.txt ne contient pas de colonnes d’entité reconnues.", "count_by_entity": {}}, []

    counts = {}
    for col in present_cols:
        counts[col] = df[col].notna().sum()

    explanation = "Répartition des règles tarifaires par type d’entité ciblée."
    return {
        "count_by_entity": counts,
        "explanation": explanation
    }, []
