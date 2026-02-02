"""
Fonctions d'audit pour le file_type: transfers
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="transfers",
    name="invalid_stop_ids",
    description="Détecte les stop_ids (from/to) qui ne sont pas présents dans stops.txt.",
    parameters={}
)
def invalid_stop_ids(gtfs_data, **params):
    transfers = gtfs_data['transfers.txt']
    stops = gtfs_data.get('stops.txt')

    invalid_from = set(transfers['from_stop_id']) - set(stops['stop_id']) if stops is not None else set()
    invalid_to = set(transfers['to_stop_id']) - set(stops['stop_id']) if stops is not None else set()
    
    return {
        "invalid_from_stop_ids": list(invalid_from),
        "invalid_to_stop_ids": list(invalid_to),
        "total_invalid": len(invalid_from) + len(invalid_to)
    }

@audit_function(
    file_type="transfers",
    name="invalid_transfer_type_values",
    description="Détecte les valeurs inattendues dans le champ transfer_type.",
    parameters={}
)
def invalid_transfer_type_values(gtfs_data, **params):
    df = gtfs_data['transfers.txt']
    allowed = {0, 1, 2, 3, 4}
    invalid_values = df[~df['transfer_type'].isin(allowed)]['transfer_type'].unique().tolist()
    return {
        "invalid_transfer_types": invalid_values,
        "count_invalid": len(invalid_values)
    }

@audit_function(
    file_type="transfers",
    name="duplicate_transfers",
    description="Détecte les lignes dupliquées (mêmes from_stop_id/to_stop_id).",
    parameters={}
)
def duplicate_transfers(gtfs_data, **params):
    df = gtfs_data['transfers.txt']
    duplicates = df.duplicated(subset=['from_stop_id', 'to_stop_id'], keep=False)
    dup_df = df[duplicates]
    return {
        "duplicate_count": len(dup_df),
        "duplicate_pairs": dup_df[['from_stop_id', 'to_stop_id']].drop_duplicates().to_dict(orient="records")
    }

@audit_function(
    file_type="transfers",
    name="missing_min_transfer_time",
    description="Détecte les lignes où min_transfer_time est requis mais manquant.",
    parameters={}
)
def missing_min_transfer_time(gtfs_data, **params):
    df = gtfs_data['transfers.txt']
    missing = df[(df['transfer_type'] == 2) & (df['min_transfer_time'].isna())]
    return {
        "missing_count": len(missing),
        "rows_with_missing_min_transfer_time": missing.to_dict(orient="records")
    }

@audit_function(
    file_type="transfers",
    name="missing_symmetric_transfers",
    description="Détecte les transferts où le transfert inverse n'existe pas.",
    parameters={}
)
def missing_symmetric_transfers(gtfs_data, **params):
    df = gtfs_data['transfers.txt']
    pairs = set(tuple(x) for x in df[['from_stop_id', 'to_stop_id']].values)
    missing = []
    for from_id, to_id in pairs:
        if (to_id, from_id) not in pairs:
            missing.append({"from": to_id, "to": from_id})
    return {
        "missing_symmetric_count": len(missing),
        "missing_symmetric_transfers": missing
    }

@audit_function(
    file_type="transfers",
    name="duplicate_transfers_pairs",
    description="Détecte doublons dans transfers.txt sur from_stop_id, to_stop_id et transfer_type.",
    parameters={}
)
def duplicate_transfers_pairs(gtfs_data, **params):
    df = gtfs_data.get('transfers')
    if df is None or df.empty:
        return {"duplicate_count": 0}
    duplicated = df.duplicated(subset=['from_stop_id', 'to_stop_id', 'transfer_type'])
    count = duplicated.sum()
    return {"duplicate_count": int(count), "has_duplicates": count > 0}

