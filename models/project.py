from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import uuid

# Dans ta classe Project
db = SQLAlchemy()

class Project(db.Model):
    __tablename__ = 'projects'
    
    # Colonnes principales
    project_id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    nom_projet = db.Column(db.String(255), nullable=False)
    trigramme = db.Column(db.String(3), nullable=False, unique=True)
    coverage = db.Column(db.String(255), nullable=True)
    
    # Métadonnées
    date_creation = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    date_modification = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    is_sandbox = db.Column(db.Boolean, default=False, nullable=False)
    
    audit_configs = db.relationship('AuditConfig', backref='project', lazy=True, cascade='all, delete-orphan')
    audit_results = db.relationship('AuditResult', backref='project', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Project {self.trigramme}: {self.nom_projet}>'
    
    def to_dict(self):
        """Convertit l'objet en dictionnaire pour JSON/API"""
        return {
            'project_id': self.project_id,
            'nom_projet': self.nom_projet,
            'trigramme': self.trigramme,
            'coverage': self.coverage,
            'date_creation': self.date_creation.isoformat() if self.date_creation else None,
            'date_modification': self.date_modification.isoformat() if self.date_modification else None,
            'is_sandbox': self.is_sandbox
        }
    
    @staticmethod
    def get_sandbox_project():
        """Récupère ou crée le projet sandbox"""
        sandbox = Project.query.filter_by(is_sandbox=True).first()
        if not sandbox:
            sandbox = Project(
                nom_projet="Projet Sandbox",
                trigramme="SBX",
                coverage="Test rapide",
                is_sandbox=True
            )
            db.session.add(sandbox)
            db.session.commit()
        return sandbox
    
    @staticmethod
    def get_all_projects(include_sandbox=True):
        """Récupère tous les projets"""
        query = Project.query
        if not include_sandbox:
            query = query.filter_by(is_sandbox=False)
        return query.order_by(Project.nom_projet).all()
    
    @staticmethod
    def get_by_trigramme(trigramme):
        """Récupère un projet par son trigramme"""
        return Project.query.filter_by(trigramme=trigramme.upper()).first()
    
    @staticmethod
    def create_project(nom_projet, trigramme, coverage=None):
        """Crée un nouveau projet avec validation"""
        # Vérification unicité trigramme
        if Project.get_by_trigramme(trigramme):
            raise ValueError(f"Le trigramme '{trigramme}' existe déjà")
        
        # Validation trigramme (3 caractères exactement)
        if len(trigramme) != 3:
            raise ValueError("Le trigramme doit faire exactement 3 caractères")
        
        project = Project()
        project.nom_projet = nom_projet
        project.trigramme = trigramme.upper()
        project.coverage = coverage
        project.is_sandbox = False
        
        db.session.add(project)
        db.session.commit()
        return project
    
    def update_project(self, nom_projet=None, trigramme=None, coverage=None):
        """Met à jour un projet existant"""
        try:
            if nom_projet:
                self.nom_projet = nom_projet
            
            if trigramme and trigramme.upper() != self.trigramme:
                # Vérification unicité du nouveau trigramme
                if Project.get_by_trigramme(trigramme):
                    raise ValueError(f"Le trigramme '{trigramme}' existe déjà")
                if len(trigramme) != 3:
                    raise ValueError("Le trigramme doit faire exactement 3 caractères")
                self.trigramme = trigramme.upper()
            
            if coverage is not None:  # Permet de mettre coverage à None
                self.coverage = coverage
            
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            raise e
        
    def delete_project(self):
        """Supprime un projet (avec vérifications)"""
        if self.is_sandbox:
            raise ValueError("Impossible de supprimer le projet sandbox")
        
        # TODO: Ajouter vérifications sur les audits/configs liés
        # if self.audit_configs or self.audit_results:
        #     raise ValueError("Impossible de supprimer un projet avec des données d'audit")
        
        db.session.delete(self)
        db.session.commit()
