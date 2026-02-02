"""
Service principal pour l'orchestration des statistiques GTFS
Suit le même pattern que AuditService
"""

from gtfs_statistics.decorators import STATISTICS_REGISTRY
from services.gtfs_handler import GTFSHandler

class StatisticsService:
    
    @staticmethod
    def get_available_statistics():
        """Retourne toutes les statistiques organisées par catégorie"""
        return STATISTICS_REGISTRY
    
    @staticmethod
    def get_categories():
        """Retourne la liste des catégories disponibles"""
        return list(STATISTICS_REGISTRY.keys())
    
    def get_statistics_for_category(self, category):
        """Retourne toutes les fonctions de statistiques pour une catégorie"""
        return STATISTICS_REGISTRY.get(category, [])
    
    def run_statistic(self, category, function_name, gtfs_data, parameters=None):
        """
        Exécute une statistique spécifique
        
        Args:
            category (str): Catégorie de statistique
            function_name (str): Nom de la fonction de statistique
            gtfs_data (dict): Données GTFS
            parameters (dict): Paramètres de la fonction
        
        Returns:
            tuple: (result, problem_ids) ou (None, None) si erreur
        """
        try: 
            if category in STATISTICS_REGISTRY:
                for stat in STATISTICS_REGISTRY[category]:
                    if stat['function_name'] == function_name:
                        result, problem_ids = stat['function'](gtfs_data, **(parameters or {}))
                        return result, problem_ids
            
            raise ValueError(f"Statistique {function_name} not found in {category}")
        
        except Exception as e:
            print(f"Erreur lors de l'exécution de la statistique {function_name}: {e}")
            return None, None
    
    def run_all_statistics(self, gtfs_data):
        """
        Exécute toutes les statistiques disponibles
        
        Args:
            gtfs_data (dict): Données GTFS
            
        Returns:
            dict: Résultats organisés par catégorie
        """
        results = {}
        
        for category, statistics in STATISTICS_REGISTRY.items():
            results[category] = {}
            
            for stat in statistics:
                try:
                    result, problem_ids = stat['function'](gtfs_data)
                    results[category][stat['function_name']] = {
                        'name': stat['name'],
                        'result': result,
                        'description': stat['description'],
                        'problem_ids': problem_ids
                    }
                except Exception as e:
                    print(f"Erreur {stat['function_name']}: {e}")
                    results[category][stat['function_name']] = {
                        'name': stat['name'],
                        'result': None,
                        'error': str(e)
                    }
        
        return results
    
    def run_essential_statistics(self, gtfs_data):
        """
        Exécute uniquement les statistiques essentielles pour l'affichage principal
        
        Args:
            gtfs_data (dict): Données GTFS
            
        Returns:
            dict: Résultats des statistiques essentielles
        """
        # Statistiques essentielles à afficher en permanence
        essential_stats = [
            ('files', 'present_files'),
            ('agency', 'agency_count'),
            ('routes', 'routes_count_by_type'),
            ('stops', 'stops_count_by_type'),
            ('trips', 'trips_count_and_info'),
            ('calendar', 'calendar_validity_period'),
            ('quality', 'data_completeness'),
            ('summary', 'dataset_overview')
        ]
        
        results = {}
        
        for category, function_name in essential_stats:
            try:
                result, problem_ids = self.run_statistic(category, function_name, gtfs_data)
                if result is not None:
                    if category not in results:
                        results[category] = {}
                    
                    # Récupérer les métadonnées de la fonction
                    stat_info = None
                    if category in STATISTICS_REGISTRY:
                        for stat in STATISTICS_REGISTRY[category]:
                            if stat['function_name'] == function_name:
                                stat_info = stat
                                break
                    
                    results[category][function_name] = {
                        'name': stat_info['name'] if stat_info else function_name,
                        'result': result,
                        'description': stat_info['description'] if stat_info else '',
                        'problem_ids': problem_ids
                    }
            except Exception as e:
                print(f"Erreur statistique essentielle {category}.{function_name}: {e}")
        
        return results
    
    def run_category_statistics(self, category, gtfs_data):
        """
        Exécute toutes les statistiques d'une catégorie spécifique
        
        Args:
            category (str): Catégorie de statistiques
            gtfs_data (dict): Données GTFS
            
        Returns:
            dict: Résultats de la catégorie
        """
        if category not in STATISTICS_REGISTRY:
            return {}
        
        results = {}
        
        for stat in STATISTICS_REGISTRY[category]:
            try:
                result, problem_ids = stat['function'](gtfs_data)
                results[stat['function_name']] = {
                    'name': stat['name'],
                    'result': result,
                    'description': stat['description'],
                    'problem_ids': problem_ids
                }
            except Exception as e:
                print(f"Erreur {stat['function_name']}: {e}")
                results[stat['function_name']] = {
                    'name': stat['name'],
                    'result': None,
                    'error': str(e)
                }
        
        return results
    
    def get_statistics_summary(self, gtfs_data):
        """
        Génère un résumé compact des statistiques principales
        
        Args:
            gtfs_data (dict): Données GTFS
            
        Returns:
            dict: Résumé compact
        """
        summary = {
            'files_count': len(gtfs_data),
            'total_rows': sum(len(df) for df in gtfs_data.values()),
            'agencies': 0,
            'routes': 0,
            'stops': 0,
            'trips': 0,
            'has_shapes': False,
            'has_fares': False,
            'quality_score': 0
        }
        
        try:
            # Comptes de base
            if 'agency.txt' in gtfs_data:
                summary['agencies'] = len(gtfs_data['agency.txt'])
            
            if 'routes.txt' in gtfs_data:
                summary['routes'] = len(gtfs_data['routes.txt'])
            
            if 'stops.txt' in gtfs_data:
                summary['stops'] = len(gtfs_data['stops.txt'])
            
            if 'trips.txt' in gtfs_data:
                summary['trips'] = len(gtfs_data['trips.txt'])
            
            # Présence de fichiers optionnels
            summary['has_shapes'] = 'shapes.txt' in gtfs_data and len(gtfs_data['shapes.txt']) > 0
            summary['has_fares'] = 'fare_attributes.txt' in gtfs_data and len(gtfs_data['fare_attributes.txt']) > 0
            
            # Score de qualité rapide
            quality_result, _ = self.run_statistic('quality', 'data_completeness', gtfs_data)
            if quality_result:
                summary['quality_score'] = quality_result.get('overall_score', 0)
        
        except Exception as e:
            print(f"Erreur dans get_statistics_summary: {e}")
        
        return summary
    
    def format_statistic_for_display(self, result, stat_name):
        """
        Formate une statistique pour l'affichage dans l'interface
        
        Args:
            result: Résultat de la statistique
            stat_name (str): Nom de la statistique
            
        Returns:
            dict: Données formatées pour l'affichage
        """
        if result is None:
            return {'error': 'Données non disponibles'}
        
        # Formatage spécifique selon le type de résultat
        if isinstance(result, dict):
            formatted = {}
            
            for key, value in result.items():
                print(key)
                print(value)
                if isinstance(value, float):
                    # Arrondir les flottants
                    formatted[key] = round(value, 2)
                elif isinstance(value, dict) and 'by_type' in key:
                    # Formater les répartitions par type
                    formatted[key] = value
                else:
                    formatted[key] = value
            
            return formatted
        
        return result