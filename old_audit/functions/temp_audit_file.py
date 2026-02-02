"""
Fonctions d'audit pour le file_type: file
"""

from ..decorators import audit_function
from . import *  # Imports centralisés


@audit_function(
    file_type="file",
    name="duplicate_rows",
    description="Détecte les lignes dupliquées dans un fichier GTFS.",
    parameters={}
)
def duplicate_rows(gtfs_data, gtfs_file, **params):
    # Exemple avec routes.txt en dur (tu peux changer le nom si besoin)
    if gtfs_file+'.txt' not in gtfs_data:
        return 0, []
    df = gtfs_data[gtfs_file+'.txt']
    count = df.duplicated().sum()
    return {'number_duplicate_rows':count, 
            'existence_duplicate_rows':count > 0},[]

@audit_function(
    file_type="file",
    name="empty_values_stats",
    description="Calcule le taux de valeurs manquantes par champ.",
    parameters={}
)
def empty_values_stats(gtfs_data, gtfs_file,  **params):
    if gtfs_file+'.txt' not in gtfs_data:
        return {}, {}, False
    df = gtfs_data[gtfs_file+'.txt']
    empty_counts = df.isna().sum().to_dict()
    empty_rate = {col: round((df[col].isna().sum() / len(df)) * 100, 2) for col in df.columns}
    has_missing = any(v > 0 for v in empty_counts.values())
    return {'empty_counts':empty_counts, 
            'empty_rate':empty_rate,
            'existence_missing_stats':has_missing},[]

@audit_function(
    file_type="file",
    name="row_consistency",
    description="Vérifie que toutes les lignes ont le même nombre de colonnes que l'en-tête.",
    parameters={}
)
def row_consistency(gtfs_data, gtfs_file, **params):
    from flask import session

    zip_path = os.path.join('uploads', session.get('gtfs_filename'))
    txt_filename = f"{gtfs_file}.txt"

    if not os.path.exists(zip_path):
        return {'error': 'Fichier GTFS introuvable'}, []

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            if txt_filename not in z.namelist():
                return {'error': f'Fichier {txt_filename} introuvable dans l\'archive'}, []

            with z.open(txt_filename) as file:
                lines = [line.decode('utf-8').strip() for line in file.readlines()]
    except Exception as e:
        return {'error': f'Erreur lors de la lecture du fichier : {str(e)}'}, []

    if not lines:
        return {'error': 'Fichier vide'}, []

    header_col_count = len(lines[0].split(','))
    inconsistent_lines = []

    for idx, line in enumerate(lines[1:], start=2):  # Lignes 2 à N
        col_count = len(line.split(','))
        if col_count != header_col_count:
            inconsistent_lines.append(idx)

    return {
        'row_consistent': len(inconsistent_lines) == 0,
        'inconsistent_row_count': len(inconsistent_lines),
        'inconsistent_rows': inconsistent_lines[:10]  # max 10 pour ne pas surcharger
    }, []

@audit_function(
    file_type="file",
    name="file_encoding",
    description="Détecte l'encodage du fichier texte et vérifie s'il est UTF-8.",
    parameters={}
)
def file_encoding(gtfs_data, gtfs_file, **params):
    from flask import session

    zip_path = os.path.join('uploads', session.get('gtfs_filename'))
    txt_filename = f"{gtfs_file}.txt"

    if not os.path.exists(zip_path):
        return {'error': 'Fichier GTFS introuvable'}, []

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            if txt_filename not in z.namelist():
                return {'error': f'Fichier {txt_filename} introuvable dans l\'archive'}, []

            with z.open(txt_filename) as file:
                raw_data = file.read(10000)  # Lire les premiers 10k octets (suffisant)
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                confidence = round(result['confidence'] * 100, 2)

                is_utf8 = encoding.lower() == 'utf-8'

                return {
                    'detected_encoding': encoding,
                    'confidence': confidence,
                    'is_utf8': is_utf8
                }, []

    except Exception as e:
        return {'error': f'Erreur lors de la détection de l\'encodage : {str(e)}'}, []

@audit_function(
    file_type="file",
    name="file_size",
    description="Mesure la taille du fichier en Ko et Mo.",
    parameters={}
)
def file_size(gtfs_data, gtfs_file, **params):
    from flask import session

    zip_path = os.path.join('uploads', session.get('gtfs_filename'))
    txt_filename = f"{gtfs_file}.txt"

    if not os.path.exists(zip_path):
        return {'error': 'Archive GTFS introuvable'}, []

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            if txt_filename not in z.namelist():
                return {'error': f'{txt_filename} introuvable dans l\'archive'}, []

            info = z.getinfo(txt_filename)
            size_bytes = info.file_size
            size_kb = round(size_bytes / 1024, 2)
            size_mb = round(size_bytes / (1024 * 1024), 2)

            return {
                'size_bytes': size_bytes,
                'size_kilobytes': size_kb,
                'size_megabytes': size_mb
            }, []

    except Exception as e:
        return {'error': f'Erreur lors de la lecture de l\'archive : {str(e)}'}, []

@audit_function(
    file_type="file",
    name="file_case_check",
    description="Vérifie si le nom du fichier est en minuscules (convention GTFS).",
    parameters={}
)
def file_case_check(gtfs_data, gtfs_file, **params):
    filename = gtfs_file + '.txt'
    
    is_lowercase = gtfs_file == gtfs_file.lower()
    df = gtfs_data.get(filename)

    condition_ok = is_lowercase and df is not None and not df.empty

    return {'is_lowercase': condition_ok}, []

