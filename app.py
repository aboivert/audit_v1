"""
Point d'entrée principal de l'application Flask GTFS Audit
"""

from flask import Flask
from config import Config
import os
from models.project import db, Project

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialiser SQLAlchemy
    db.init_app(app)
    
    # Créer les tables et initialiser les données
    with app.app_context():
        # Import des modèles pour que SQLAlchemy les connaisse
        from models.audit_config import AuditConfig
        from models.audit_result import AuditResult
        
        db.create_all()
        
        # Créer le projet sandbox s'il n'existe pas
        Project.get_sandbox_project()

    import visualization.functions
    import gtfs_statistics.functions



    # Enregistrer les blueprints
    from routes.main import main_bp
    from routes.audit import audit_bp
    from routes.visualization import visualization_bp
    from routes.admin import admin_bp
    from routes.statistics import statistics_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(audit_bp)
    app.register_blueprint(visualization_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(statistics_bp)

    return app

def init_database(app):
    """Initialise la base de données avec des données de test (optionnel)"""
    with app.app_context():
        # Vérifier si on a déjà des projets (autres que sandbox)
        existing_projects = Project.query.filter_by(is_sandbox=False).count()
        
        if existing_projects == 0:
            # Créer quelques projets d'exemple
            sample_projects = [
                {
                    'nom_projet': 'Réseau Transport Métropole',
                    'trigramme': 'RTM',
                    'coverage': 'Métropole de Lyon'
                },
                {
                    'nom_projet': 'Transport Urbain Ville',
                    'trigramme': 'TUV',
                    'coverage': 'Centre-ville et périphérie'
                }
            ]
            
            for project_data in sample_projects:
                try:
                    Project.create_project(**project_data)
                    print(f"Projet créé: {project_data['trigramme']}")
                except ValueError as e:
                    print(f"Erreur création projet: {e}")

if __name__ == '__main__':
    app = create_app()
    # Initialiser avec des données de test en développement
    if app.config.get('DEBUG'):
        init_database(app)
    app.run(debug=True, port=5000)