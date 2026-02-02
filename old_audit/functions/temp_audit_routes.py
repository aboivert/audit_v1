"""
Fonctions d'audit pour le file_type: routes
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="routes",
    name="Validation couleurs",
    description="Vérifie le format hexadécimal des couleurs des routes",
    parameters={
        "check_text_color": {
            "type": "checkbox",
            "default": True,
            "description": "Vérifier aussi les couleurs de texte"
        }
    }
)
def validate_route_colors(gtfs_data, **params):
    """Validation des couleurs des routes"""
    check_text = params.get('check_text_color', True)
    
    if 'routes.txt' not in gtfs_data:
        return 0, []
    
    routes_df = gtfs_data['routes.txt']
    problem_ids = []
    total_routes = len(routes_df)
    
    if total_routes == 0:
        return 100, []
    
    issues = 0
    
    # Vérifier route_color
    if 'route_color' in routes_df.columns:
        invalid_colors = routes_df[
            (routes_df['route_color'].notna()) & 
            (~routes_df['route_color'].str.match(r'^[0-9A-Fa-f]{6}$', na=False))
        ]
        issues += len(invalid_colors)
        if 'route_id' in invalid_colors.columns:
            problem_ids.extend(invalid_colors['route_id'].tolist())
    
    # Vérifier route_text_color si demandé
    if check_text and 'route_text_color' in routes_df.columns:
        invalid_text_colors = routes_df[
            (routes_df['route_text_color'].notna()) & 
            (~routes_df['route_text_color'].str.match(r'^[0-9A-Fa-f]{6}$', na=False))
        ]
        issues += len(invalid_text_colors)
        if 'route_id' in invalid_text_colors.columns:
            problem_ids.extend(invalid_text_colors['route_id'].tolist())
    
    score = max(0, 100 - (issues / total_routes * 100))
    return score, list(set(problem_ids))  # Supprimer les doublons

@audit_function(
    file_type="routes",
    name="check_route_id_uniqueness",
    description="Vérifie que les route_id sont uniques.",
    parameters={}
)
def check_route_id_uniqueness(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['routes.txt']
    duplicated_ids = df['route_id'][df['route_id'].duplicated()].tolist()
    return {
        "duplicate_route_ids": duplicated_ids,
        "count_duplicates": len(duplicated_ids)
    }

@audit_function(
    file_type="routes",
    name="check_route_name_presence",
    description="Vérifie la présence de route_short_name ou route_long_name (au moins un requis).",
    parameters={}
)
def check_route_name_presence(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['routes.txt']
    missing_names = df[df['route_short_name'].isna() & df['route_long_name'].isna()]
    return {
        "missing_route_names_count": len(missing_names),
        "missing_routes": missing_names['route_id'].tolist()
    }

@audit_function(
    file_type="routes",
    name="check_duplicate_route_names",
    description="Détecte les doublons dans route_short_name ou route_long_name.",
    parameters={}
)
def check_duplicate_route_names(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['routes.txt']
    short_dups = df['route_short_name'][df['route_short_name'].duplicated(keep=False)]
    long_dups = df['route_long_name'][df['route_long_name'].duplicated(keep=False)]
    return {
        "duplicate_short_names": short_dups.dropna().unique().tolist(),
        "duplicate_long_names": long_dups.dropna().unique().tolist()
    }

@audit_function(
    file_type="routes",
    name="check_route_type_validity",
    description="Vérifie que route_type est dans les valeurs valides du GTFS.",
    parameters={}
)
def check_route_type_validity(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['routes.txt']
    valid_types = set(range(0, 12))  # GTFS de base
    invalid = df[~df['route_type'].isin(valid_types)]
    return {
        "invalid_route_type_count": len(invalid),
        "invalid_route_ids": invalid['route_id'].tolist()
    }

@audit_function(
    file_type="routes",
    name="check_route_color_format",
    description="Vérifie le format de route_color et route_text_color (6 caractères hex).",
    parameters={}
)
def check_route_color_format(gtfs_data, **params):
    import re
    if 'routes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['routes.txt']
    hex_pattern = re.compile(r'^[0-9A-Fa-f]{6}$')
    invalid_colors = []
    for color_field in ['route_color', 'route_text_color']:
        if color_field in df.columns:
            for idx, val in df[color_field].dropna().iteritems():
                if not hex_pattern.match(str(val)):
                    invalid_colors.append({"field": color_field, "index": idx, "value": val})
    return {
        "invalid_colors": invalid_colors,
        "invalid_count": len(invalid_colors)
    }

@audit_function(
    file_type="routes",
    name="check_identical_route_names",
    description="Vérifie si route_short_name et route_long_name sont identiques (souvent un problème de saisie).",
    parameters={}
)
def check_identical_route_names(gtfs_data, **params):
    if 'routes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['routes.txt']
    same_name = df[df['route_short_name'] == df['route_long_name']]
    return {
        "identical_name_count": len(same_name),
        "route_ids": same_name['route_id'].tolist()
    }

@audit_function(
    file_type="routes",
    name="check_route_url_validity",
    description="Vérifie la validité des URLs dans route_url.",
    parameters={}
)
def check_route_url_validity(gtfs_data, **params):
    import validators
    if 'routes.txt' not in gtfs_data:
        return {"missing_file": True}
    df = gtfs_data['routes.txt']
    if 'route_url' not in df.columns:
        return {"missing_column": True}
    invalid_urls = []
    for idx, url in df['route_url'].dropna().iteritems():
        if not validators.url(url):
            invalid_urls.append({"index": idx, "route_url": url})
    return {
        "invalid_url_count": len(invalid_urls),
        "invalid_urls": invalid_urls
    }

@audit_function(
    file_type="routes",
    name="check_required_columns",
    description="Vérifie la présence des colonnes obligatoires dans routes.txt",
    parameters={}
)
def check_required_columns(gtfs_data, **params):
    required = {"route_id", "route_type", "route_short_name", "route_long_name"}
    if 'routes.txt' not in gtfs_data:
        return 0, []
    df = gtfs_data['routes.txt']
    missing = list(required - set(df.columns))
    return {"missing_columns": missing, "has_missing_columns": len(missing) > 0}

@audit_function(
    file_type="routes",
    name="route_id_uniqueness",
    description="Vérifie que les route_id sont uniques",
    parameters={}
)
def route_id_uniqueness(gtfs_data, **params):
    df = gtfs_data['routes.txt']
    duplicate_ids = df[df['route_id'].duplicated()]['route_id'].tolist()
    return {"duplicate_ids": duplicate_ids, "has_duplicates": len(duplicate_ids) > 0}

@audit_function(
    file_type="routes",
    name="route_name_conflicts",
    description="Détecte les conflits entre route_short_name et route_long_name",
    parameters={}
)
def route_name_conflicts(gtfs_data, **params):
    df = gtfs_data['routes.txt']
    conflicts = df[df['route_short_name'] == df['route_long_name']]['route_id'].tolist()
    return {"conflicting_ids": conflicts, "conflict_count": len(conflicts)}

@audit_function(
    file_type="routes",
    name="missing_names",
    description="Identifie les lignes sans route_short_name ou route_long_name",
    parameters={}
)
def missing_names(gtfs_data, **params):
    df = gtfs_data['routes.txt']
    missing_short = df[df['route_short_name'].isna()]['route_id'].tolist()
    missing_long = df[df['route_long_name'].isna()]['route_id'].tolist()
    return {
        "missing_short_name_ids": missing_short,
        "missing_long_name_ids": missing_long,
        "missing_total": len(missing_short) + len(missing_long)
    }

@audit_function(
    file_type="routes",
    name="route_type_validity",
    description="Vérifie que les route_type sont valides (0 à 11, ou GTFS-extended)",
    parameters={}
)
def route_type_validity(gtfs_data, **params):
    df = gtfs_data['routes.txt']
    valid_values = list(range(12))  # GTFS officiel
    invalid = df[~df['route_type'].isin(valid_values)]
    return {
        "invalid_route_type_ids": invalid['route_id'].tolist(),
        "invalid_count": len(invalid)
    }

@audit_function(
    file_type="routes",
    name="route_color_format",
    description="Vérifie que les champs route_color et route_text_color sont des hex RGB valides",
    parameters={}
)
def route_color_format(gtfs_data, **params):
    import re
    df = gtfs_data['routes.txt']
    hex_pattern = re.compile(r"^[0-9A-Fa-f]{6}$")
    bad_color = df['route_color'].dropna().apply(lambda x: not bool(hex_pattern.match(str(x))))
    bad_text_color = df['route_text_color'].dropna().apply(lambda x: not bool(hex_pattern.match(str(x))))
    return {
        "invalid_route_color_ids": df[bad_color]['route_id'].tolist(),
        "invalid_text_color_ids": df[bad_text_color]['route_id'].tolist(),
        "invalid_color_count": bad_color.sum() + bad_text_color.sum()
    }

@audit_function(
    file_type="routes",
    name="route_names_whitespace",
    description="Détecte les noms avec espaces en début/fin ou multiples espaces",
    parameters={}
)
def route_names_whitespace(gtfs_data, **params):
    import re
    df = gtfs_data['routes.txt']
    suspicious = df[
        df['route_short_name'].str.contains(r"^\s|\s$|\s{2,}", na=False) |
        df['route_long_name'].str.contains(r"^\s|\s$|\s{2,}", na=False)
    ]
    return {
        "suspicious_route_ids": suspicious['route_id'].tolist(),
        "suspicious_count": len(suspicious)
    }

@audit_function(
    file_type="routes",
    name="long_name_contains_short_name",
    description="Vérifie si route_long_name contient route_short_name",
    parameters={}
)
def long_name_contains_short_name(gtfs_data, **params):
    df = gtfs_data['routes.txt']
    mismatches = df[
        ~df.apply(lambda row: str(row['route_short_name']).strip().lower() in str(row['route_long_name']).strip().lower(), axis=1)
    ]
    return {
        "mismatch_ids": mismatches['route_id'].tolist(),
        "mismatch_count": len(mismatches)
    }

@audit_function(
    file_type="routes",
    name="duplicate_routes_by_name_type",
    description="Détecte routes dupliquées avec mêmes route_short_name, route_long_name et route_type.",
    parameters={}
)
def duplicate_routes_by_name_type(gtfs_data, **params):
    df = gtfs_data.get('routes')
    if df is None or df.empty:
        return {"duplicate_count": 0, "duplicates": []}
    subset_cols = ['route_short_name', 'route_long_name', 'route_type']
    duplicated = df.duplicated(subset=subset_cols, keep=False)
    duplicates_df = df[duplicated]
    count = len(duplicates_df)
    duplicates_list = duplicates_df.to_dict(orient='records')
    return {"duplicate_count": count, "duplicates": duplicates_list}

@audit_function(
    file_type="routes",
    name="routes_without_trips",
    description="Routes définies dans routes.txt mais sans trips associés.",
    parameters={}
)
def routes_without_trips(gtfs_data, **params):
    routes = gtfs_data.get('routes.txt')
    trips = gtfs_data.get('trips.txt')
    if routes is None:
        return {}
    if trips is None:
        return {
            "routes_without_trips_count": len(routes),
            "routes_without_trips": routes['route_id'].tolist()
        }
    used_route_ids = set(trips['route_id'])
    unused_routes = routes[~routes['route_id'].isin(used_route_ids)]
    return {
        "routes_without_trips_count": len(unused_routes),
        "routes_without_trips": unused_routes['route_id'].tolist()
    }

