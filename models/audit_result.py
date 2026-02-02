from datetime import datetime
import uuid

from .project import db

from sqlalchemy.dialects.postgresql import JSONB

class AuditResult(db.Model):
    __tablename__ = 'audit_results'
    
    # Colonnes principales
    result_id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id = db.Column(db.String(36), db.ForeignKey('projects.project_id'), nullable=False)
    date_audit = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    agency_audit = db.Column(db.JSON, nullable=True)
    routes_audit = db.Column(db.JSON, nullable=True)
    trips_audit = db.Column(db.JSON, nullable=True)
    stops_audit = db.Column(db.JSON, nullable=True)
    stop_times_audit = db.Column(db.JSON, nullable=True)
    calendar_audit = db.Column(db.JSON, nullable=True)
    calendar_dates_audit = db.Column(db.JSON, nullable=True)

    statistiques = db.Column(db.JSON, nullable=True)
    
    # Relation
    
    def to_dict(self):
        """Convertit l'objet en dictionnaire"""
        return {
            'result_id': self.result_id,
            'project_id': self.project_id,
            'date_audit': self.date_audit.isoformat() if self.date_audit else None,

            'agency_audit': self.agency_audit,
            'routes_audit': self.routes_audit,
            'trips_audit': self.trips_audit,
            'stops_audit': self.stops_audit,
            'stop_times_audit': self.stop_times_audit,
            'calendar_audit': self.calendar_audit,
            'calendar_dates_audit': self.calendar_dates_audit,
            'statistiques': self.statistiques,
        }