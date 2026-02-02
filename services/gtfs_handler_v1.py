"""
Service pour la gestion des fichiers GTFS
"""

import os
import zipfile
import pandas as pd
import tempfile
import shutil

class GTFSHandler:
    
    GTFS_FILES = [
        'agency.txt', 'routes.txt', 'trips.txt', 'stops.txt',
        'stop_times.txt', 'calendar.txt', 'calendar_dates.txt',
        'fare_attributes.txt', 'fare_rules.txt', 'shapes.txt',
        'frequencies.txt', 'transfers.txt', 'feed_info.txt'
    ]
    
    @staticmethod
    def extract_and_load_gtfs(zip_path):
        """
        Extrait un fichier ZIP GTFS et charge les données
        
        Args:
            zip_path (str): Chemin vers le fichier ZIP
            
        Returns:
            dict: Dictionnaire avec les DataFrames des fichiers GTFS
        """
        gtfs_data = {}
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Extraire le ZIP
            print(f"Chemin ZIP: {zip_path}")
            print(f"Fichier existe: {os.path.exists(zip_path)}")
            print(f"Taille fichier: {os.path.getsize(zip_path) if os.path.exists(zip_path) else 'N/A'}")

            # Avant l'extraction
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                print(f"Contenu du ZIP: {zip_ref.namelist()}")
                zip_ref.extractall(temp_dir)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                print(f"Extraction vers: {temp_dir}")
                print(f"Contenu après extraction: {os.listdir(temp_dir)}")
            # Charger chaque fichier CSV en DataFrame
            for filename in GTFSHandler.GTFS_FILES:
                file_path = os.path.join(temp_dir, filename)
                if os.path.exists(file_path):
                    try:
                        gtfs_data[filename] = pd.read_csv(file_path)
                        print(f"✅ Chargé: {filename} ({len(gtfs_data[filename])} lignes)")
                    except Exception as e:
                        print(f"❌ Erreur lors du chargement de {filename}: {e}")
                else:
                    print(f"⚠️  Fichier manquant: {filename}")
            
            return gtfs_data
            
        except Exception as e:
            print(f"Erreur lors de l'extraction du GTFS: {e}")
            return None
        
        finally:
            # Nettoyer le dossier temporaire
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    @staticmethod
    def get_gtfs_info(gtfs_data):
        """
        Génère un résumé des informations GTFS
        
        Args:
            gtfs_data (dict): Données GTFS
            
        Returns:
            dict: Informations sur chaque fichier
        """
        gtfs_info = {}
        for file_type, df in gtfs_data.items():
            gtfs_info[file_type] = {
                'rows': len(df),
                'columns': list(df.columns)
            }
        return gtfs_info