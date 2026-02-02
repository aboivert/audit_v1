#!/usr/bin/env python3
"""
Script de migration pour créer les tables GTFS
À exécuter depuis la racine du projet
"""

from sqlalchemy import create_engine
from config import Config
from models.gtfs_models import create_gtfs_tables

def main():
    print("🚀 Création des tables GTFS...")
    
    try:
        engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
        create_gtfs_tables(engine)
        print("✅ Tables GTFS créées avec succès!")
        
        # Vérifier que les tables ont été créées
        with engine.connect() as conn:
            result = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name LIKE 'gtfs_%'
            """)
            tables = [row[0] for row in result.fetchall()]
            
        print(f"📋 Tables créées: {', '.join(tables)}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la création des tables: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()