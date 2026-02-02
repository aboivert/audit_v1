"""
Système de décorateurs pour l'auto-découverte des fonctions de statistiques GTFS
"""

STATISTICS_REGISTRY = {}

def statistics_function(category, name, description="", parameters=None):
    """
    Décorateur pour enregistrer automatiquement les fonctions de statistiques
    
    Args:
        category (str): Catégorie de statistique (files, agency, routes, stops, etc.)
        name (str): Nom affiché dans l'interface
        description (str): Description de la statistique
        parameters (dict): Configuration des paramètres de la fonction (optionnel)
    
    Example:
        @statistics_function(
            category="agency",
            name="Nombre d'agences",
            description="Compte le nombre total d'agences dans le GTFS"
        )
        def agency_count(gtfs_data, **params):
            return {'count': len(gtfs_data.get('agency.txt', []))}, []
    """
    def decorator(func):
        if category not in STATISTICS_REGISTRY:
            STATISTICS_REGISTRY[category] = []
        
        STATISTICS_REGISTRY[category].append({
            'function': func,
            'name': name,
            'description': description,
            'parameters': parameters or {},
            'function_name': func.__name__
        })
        return func
    return decorator

def get_statistics_registry():
    """Retourne le registre complet des fonctions de statistiques"""
    return STATISTICS_REGISTRY

def get_categories():
    """Retourne la liste des catégories disponibles"""
    return list(STATISTICS_REGISTRY.keys())

def get_statistics_for_category(category):
    """Retourne toutes les fonctions de statistiques pour une catégorie donnée"""
    return STATISTICS_REGISTRY.get(category, [])