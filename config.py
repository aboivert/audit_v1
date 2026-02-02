import os
from pathlib import Path

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-change-this'
    UPLOAD_FOLDER = 'uploads'
    
    # Nouveaux paramètres pour téléchargement URL
    DOWNLOAD_TIMEOUT = 60  # 60 secondes
    MAX_DOWNLOAD_SIZE = 50 * 1024 * 1024  # 50MB max pour téléchargement
    
    # Configuration base de données
    basedir = Path(__file__).parent
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or f'sqlite:///{basedir}/gtfs_audit.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Configuration upload
    MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB max pour les fichiers GTFS
    
    # Extensions autorisées
    ALLOWED_EXTENSIONS = {'zip'}
    
    # Configuration audit
    AUDIT_TIMEOUT = 300  # 5 minutes max par audit
    
    # Configuration visualisation
    VIZ_CACHE_TIMEOUT = 3600  # 1 heure
    
    # Configuration PDF
    PDF_OUTPUT_DIR = basedir / 'static' / 'reports'
    
    def __init__(self):
        # Créer les dossiers nécessaires
        self.UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
        self.PDF_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class DevelopmentConfig(Config):
    """Configuration pour le développement"""
    DEBUG = True
    SQLALCHEMY_ECHO = True  # Log des requêtes SQL

class ProductionConfig(Config):
    """Configuration pour la production"""
    DEBUG = False
    SQLALCHEMY_ECHO = False

class TestingConfig(Config):
    """Configuration pour les tests"""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    WTF_CSRF_ENABLED = False

# Dictionnaire des configurations
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}