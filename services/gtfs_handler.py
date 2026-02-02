"""
Service pour la gestion des fichiers GTFS avec cache mémoire
"""
import os
import zipfile
import pandas as pd
import tempfile
import shutil
import pickle
from flask import session
from config import Config

class GTFSHandler:
    
    GTFS_FILES = [
        'agency.txt', 'routes.txt', 'trips.txt', 'stops.txt',
        'stop_times.txt', 'calendar.txt', 'calendar_dates.txt',
        'fare_attributes.txt', 'fare_rules.txt', 'shapes.txt',
        'frequencies.txt', 'transfers.txt', 'feed_info.txt'
    ]
    
    # Cache mémoire global par projet
    _memory_cache = {}
    
    @staticmethod
    def extract_and_cache_gtfs(zip_path, project_id):
        """
        Extrait un fichier ZIP GTFS et le met en cache mémoire
        
        Args:
            zip_path (str): Chemin vers le fichier ZIP
            project_id (str): ID du projet
            
        Returns:
            dict: Les données GTFS ou None si erreur
        """
        temp_dir = tempfile.mkdtemp()
        
        try:
            # 1. Nettoyer l'ancien cache pour ce projet
            GTFSHandler.clear_gtfs_cache(project_id)
            
            # 2. Extraire le ZIP
            print(f"Extraction GTFS pour projet {project_id}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                print(f"Contenu extrait: {zip_ref.namelist()}")
            
            # 3. Charger chaque fichier en DataFrame
            gtfs_data = {}
            loaded_files = []
            
            for filename in GTFSHandler.GTFS_FILES:
                file_path = os.path.join(temp_dir, filename)
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        gtfs_data[filename] = df
                        loaded_files.append(filename)
                        print(f"✅ Chargé: {filename} ({len(df)} lignes)")
                        
                    except Exception as e:
                        print(f"❌ Erreur lors du chargement de {filename}: {e}")
                else:
                    print(f"⚠️  Fichier manquant: {filename}")
            
            # 4. Stocker en cache mémoire
            GTFSHandler._memory_cache[project_id] = gtfs_data
            
            # 5. Optionnel: Sauvegarder sur disque pour persistance
            GTFSHandler._save_to_disk(project_id, gtfs_data)
            
            print(f"GTFS mis en cache pour projet {project_id}: {loaded_files}")
            return gtfs_data
                    
        except Exception as e:
            print(f"Erreur lors de l'extraction/cache du GTFS: {e}")
            return None
                
        finally:
            # Nettoyer le dossier temporaire
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    @staticmethod
    def get_gtfs_data(project_id, file_type=None):
        """
        Récupère les données GTFS depuis le cache mémoire
        
        Args:
            project_id (str): ID du projet
            file_type (str, optional): Type de fichier spécifique
            
        Returns:
            dict ou DataFrame: Données GTFS
        """
        # 1. Vérifier le cache mémoire
        if project_id in GTFSHandler._memory_cache:
            gtfs_data = GTFSHandler._memory_cache[project_id]
            
            if file_type:
                return gtfs_data.get(file_type)
            else:
                return gtfs_data
        
        # 2. Essayer de charger depuis le disque
        gtfs_data = GTFSHandler._load_from_disk(project_id)
        if gtfs_data:
            # Remettre en cache mémoire
            GTFSHandler._memory_cache[project_id] = gtfs_data
            
            if file_type:
                return gtfs_data.get(file_type)
            else:
                return gtfs_data
        
        # 3. Aucune donnée trouvée
        return None if file_type else {}
    
    @staticmethod
    def has_gtfs_data(project_id):
        """
        Vérifie si des données GTFS existent pour un projet
        
        Args:
            project_id (str): ID du projet
            
        Returns:
            bool: True si des données existent
        """
        # Vérifier le cache mémoire
        if project_id in GTFSHandler._memory_cache:
            return len(GTFSHandler._memory_cache[project_id]) > 0
        
        # Vérifier le disque
        return GTFSHandler._disk_cache_exists(project_id)
    
    @staticmethod
    def clear_gtfs_cache(project_id):
        """
        Supprime les données GTFS du cache (mémoire + disque)
        
        Args:
            project_id (str): ID du projet
        """
        # Supprimer du cache mémoire
        if project_id in GTFSHandler._memory_cache:
            del GTFSHandler._memory_cache[project_id]
            print(f"Cache mémoire supprimé pour projet {project_id}")
        
        # Supprimer du disque
        GTFSHandler._remove_from_disk(project_id)
    
    @staticmethod
    def get_gtfs_info(project_id):
        """
        Génère un résumé des informations GTFS
        
        Args:
            project_id (str): ID du projet
            
        Returns:
            dict: Informations sur chaque fichier
        """
        gtfs_data = GTFSHandler.get_gtfs_data(project_id)
        if not gtfs_data:
            return {}
        
        gtfs_info = {}
        for file_type, df in gtfs_data.items():
            if df is not None and not df.empty:
                gtfs_info[file_type] = {
                    'rows': len(df),
                    'columns': list(df.columns)
                }
        return gtfs_info
    
    # ===== MÉTHODES PRIVÉES POUR PERSISTANCE DISQUE =====
    
    @staticmethod
    def _get_cache_path(project_id):
        """Chemin du fichier cache sur disque"""
        cache_dir = os.path.join(Config.UPLOAD_FOLDER, 'gtfs_cache')
        os.makedirs(cache_dir, exist_ok=True)
        return os.path.join(cache_dir, f"{project_id}.pkl")
    
    @staticmethod
    def _save_to_disk(project_id, gtfs_data):
        """Sauvegarde les données GTFS sur disque"""
        try:
            cache_path = GTFSHandler._get_cache_path(project_id)
            with open(cache_path, 'wb') as f:
                pickle.dump(gtfs_data, f)
            print(f"GTFS sauvegardé sur disque: {cache_path}")
        except Exception as e:
            print(f"Erreur sauvegarde disque: {e}")
    
    @staticmethod
    def _load_from_disk(project_id):
        """Charge les données GTFS depuis le disque"""
        try:
            cache_path = GTFSHandler._get_cache_path(project_id)
            if os.path.exists(cache_path):
                with open(cache_path, 'rb') as f:
                    gtfs_data = pickle.load(f)
                print(f"GTFS chargé depuis le disque: {cache_path}")
                return gtfs_data
        except Exception as e:
            print(f"Erreur chargement disque: {e}")
        return None
    
    @staticmethod
    def _disk_cache_exists(project_id):
        """Vérifie si un cache disque existe"""
        cache_path = GTFSHandler._get_cache_path(project_id)
        return os.path.exists(cache_path)
    
    @staticmethod
    def _remove_from_disk(project_id):
        """Supprime le cache disque"""
        try:
            cache_path = GTFSHandler._get_cache_path(project_id)
            if os.path.exists(cache_path):
                os.remove(cache_path)
                print(f"Cache disque supprimé: {cache_path}")
        except Exception as e:
            print(f"Erreur suppression cache disque: {e}")
    
    # ===== MÉTHODES DE DIAGNOSTIC =====
    
    @staticmethod
    def get_cache_status():
        """Retourne le statut du cache pour debug"""
        return {
            'memory_cache_projects': list(GTFSHandler._memory_cache.keys()),
            'memory_cache_size': len(GTFSHandler._memory_cache),
            'disk_caches': GTFSHandler._list_disk_caches()
        }
    
    @staticmethod
    def _list_disk_caches():
        """Liste les caches disque existants"""
        try:
            cache_dir = os.path.join(Config.UPLOAD_FOLDER, 'gtfs_cache')
            if os.path.exists(cache_dir):
                return [f.replace('.pkl', '') for f in os.listdir(cache_dir) if f.endswith('.pkl')]
        except:
            pass
        return []