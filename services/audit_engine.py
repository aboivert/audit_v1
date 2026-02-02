"""
Moteur principal d'audit GTFS
"""
from datetime import datetime
from services.gtfs_handler import GTFSHandler
from services.audit_functions.agency_audit import audit_agency_file
from services.audit_functions.routes_audit import audit_routes_file
from services.audit_functions.stops_audit import audit_stops_file
from services.audit_functions.trips_audit import audit_trips_file
from services.audit_functions.stop_times_audit import audit_stop_times_file
from services.audit_functions.calendar_audit import audit_calendar_file
from services.audit_functions.calendar_dates_audit import audit_calendar_dates_file
from services.audit_functions.generic_functions import calculate_gtfs_statistics

AUDIT_COLUMN_MAPPING = {
    'agency.txt': 'agency_audit',
    'routes.txt': 'routes_audit',
    'trips.txt': 'trips_audit',
    'stops.txt': 'stops_audit',
    'stop_times.txt': 'stop_times_audit',
    'calendar.txt': 'calendar_audit',
    'calendar_dates.txt': 'calendar_dates_audit',
    'shapes.txt': None,  # Pas encore de colonne pour ça
    'fare_attributes.txt': None,
    'fare_rules.txt': None,
    'frequencies.txt': None,
    'transfers.txt': None,
    'feed_info.txt': None,
}

# Import pour accéder à la variable globale de progression
audit_progress = {}

def update_progress(project_id, file_type, progress, message, step):
    """Met à jour la progression d'un audit"""
    progress_key = f"{project_id}_{file_type}"
    audit_progress[progress_key] = {
        'progress': progress,
        'message': message,
        'step': step
    }
    print(f"📈 Progression {file_type}: {progress}% - {message}")

class AuditEngine:
    """
    Moteur d'audit pour les fichiers GTFS
    """
    
    # Mapping des fichiers GTFS vers leurs fonctions d'audit
    AUDIT_FUNCTIONS = {
        'agency.txt': audit_agency_file,
        'routes.txt': audit_routes_file,
        'stops.txt': audit_stops_file,
        'trips.txt': audit_trips_file,
        'stop_times.txt': audit_stop_times_file,
        'calendar.txt': audit_calendar_file,
        'calendar_dates.txt': audit_calendar_dates_file,
        # TODO: Ajouter les autres fichiers au fur et à mesure
    }
    
    def __init__(self):
        # Cache temporaire pour les résultats (en attendant la BDD)
        self._results_cache = {}
    
    def run_file_audit(self, project_id, file_type):
        """
        Lance l'audit d'un fichier spécifique avec suivi de progression
        """
        print(f"🔍 Lancement audit {file_type} pour projet {project_id}")
        
        # Initialiser la progression
        update_progress(project_id, file_type, 0, "Initialisation...", "init")
        
        # Vérifier si une fonction d'audit existe pour ce fichier
        if file_type not in self.AUDIT_FUNCTIONS:
            update_progress(project_id, file_type, 0, "Audit non implémenté", "error")
            return {
                "file": file_type,
                "status": "not_implemented",
                "message": f"Audit non implémenté pour {file_type}",
                "timestamp": datetime.now().isoformat()
            }
        
        try:
            update_progress(project_id, file_type, 10, "Chargement des données...", "loading")
            
            # Lancer la fonction d'audit avec progression
            audit_function = self.AUDIT_FUNCTIONS[file_type]
            results = audit_function(project_id, progress_callback=lambda p, m, s: update_progress(project_id, file_type, p, m, s))
            
            update_progress(project_id, file_type, 90, "Finalisation...", "finalizing")
            
            # Mettre en cache (temporaire)
            cache_key = f"{project_id}_{file_type}"
            self._results_cache[cache_key] = results
            
            update_progress(project_id, file_type, 100, "Audit terminé", "complete")
            
            print(f"✅ Audit {file_type} terminé - Statut: {results.get('summary', {}).get('overall_status', 'unknown')}")
            
            return results
            
        except Exception as e:
            update_progress(project_id, file_type, 0, f"Erreur: {str(e)}", "error")
            
            error_result = {
                "file": file_type,
                "status": "error",
                "message": f"Erreur lors de l'audit: {str(e)}",
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "overall_status": "critical",
                    "total_checks": 0,
                    "passed_checks": 0,
                    "warning_checks": 0,
                    "error_checks": 0,
                    "critical_checks": 1
                }
            }
            
            print(f"❌ Erreur audit {file_type}: {str(e)}")
            return error_result
    
    def run_all_audits(self, project_id, save_to_db=False):
        print(f"🔍 Lancement de tous les audits pour projet {project_id}")
        
        print("📋 Tentative récupération GTFS info...")
        try:
            # Récupérer les fichiers GTFS disponibles
            gtfs_info = GTFSHandler.get_gtfs_info(project_id)
            print(f"✅ GTFS info récupéré: {gtfs_info}")
            available_files = list(gtfs_info.keys())
            print(f"📁 Fichiers disponibles: {available_files}")
        except Exception as e:
            print(f"❌ ERREUR get_gtfs_info: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": f"Erreur GTFS: {str(e)}"}
        
        print("📊 Création structure all_results...")
        all_results = {
            "project_id": project_id,
            "timestamp": datetime.now().isoformat(),
            "files_audited": [],
            "results": {},
            "global_summary": {
                "total_files": 0,
                "files_passed": 0,
                "files_with_warnings": 0,
                "files_with_errors": 0,
                "files_critical": 0
            }
        }
        
        print(f"🔄 Début boucle sur {len(available_files)} fichiers...")

        # ... votre boucle d'audit existante (inchangée) ...
        for i, file_type in enumerate(available_files):
            print(f"🔄 Fichier {i+1}/{len(available_files)}: {file_type}")
            if file_type in self.AUDIT_FUNCTIONS:
                print(f"  📄 Audit de {file_type}...")
                try:
                    results = self.run_file_audit(project_id, file_type)
                    all_results["results"][file_type] = results
                    all_results["files_audited"].append(file_type)
                    
                    # Mettre à jour le résumé global
                    overall_status = results.get('summary', {}).get('overall_status', 'unknown')
                    all_results["global_summary"]["total_files"] += 1
                    
                    if overall_status == "pass":
                        all_results["global_summary"]["files_passed"] += 1
                    elif overall_status == "warning":
                        all_results["global_summary"]["files_with_warnings"] += 1
                    elif overall_status == "error":
                        all_results["global_summary"]["files_with_errors"] += 1
                    elif overall_status == "critical":
                        all_results["global_summary"]["files_critical"] += 1
                        
                except Exception as e:
                    print(f"❌ Erreur audit {file_type}: {str(e)}")
                    error_result = self._create_error_result(file_type, f"Erreur lors de l'audit: {str(e)}")
                    all_results["results"][file_type] = error_result
                    all_results["files_audited"].append(file_type)
                    all_results["global_summary"]["total_files"] += 1
                    all_results["global_summary"]["files_with_errors"] += 1
            else:
                print(f"⚠️ Audit non implémenté pour {file_type}")
                not_implemented_result = {
                    "file": file_type,
                    "status": "not_implemented",
                    "message": f"Audit non implémenté pour {file_type}",
                    "timestamp": datetime.now().isoformat(),
                    "summary": {
                        "overall_status": "not_implemented",
                        "total_checks": 0,
                        "passed_checks": 0,
                        "warning_checks": 0,
                        "error_checks": 0,
                        "critical_checks": 0
                    }
                }
                all_results["results"][file_type] = not_implemented_result
                all_results["files_audited"].append(file_type)
                all_results["global_summary"]["total_files"] += 1
        
        print(f"✅ Tous les audits terminés: {len(all_results['files_audited'])} fichiers audités")

        # NOUVEAU : Calcul des statistiques globales
        print("📊 Calcul des statistiques globales du GTFS...")
        try:
            statistiques_globales = calculate_gtfs_statistics(project_id)
            all_results["statistiques_globales"] = statistiques_globales
            print("✅ Statistiques globales calculées")
            print(statistiques_globales)
        except Exception as e:
            print(f"⚠️ Erreur calcul statistiques: {str(e)}")
            all_results["statistiques_globales"] = None

        if save_to_db:
            try:
                print(f"🔍 DEBUG: Tentative sauvegarde en BDD...")
                print(f"🔍 DEBUG: project_id = {project_id}")
                print(f"🔍 DEBUG: Statistiques calculées = {statistiques_globales is not None}")
                
                # MODIFIÉ : passer les statistiques à la sauvegarde
                result_id = self._save_results_to_db(project_id, all_results["results"], statistiques_globales)
                print(f"✅ Résultats sauvegardés en base de données avec ID: {result_id}")
            except Exception as e:
                print(f"❌ Erreur lors de la sauvegarde en base: {str(e)}")
                import traceback
                traceback.print_exc()  # Afficher la stack trace complète
                all_results["save_error"] = str(e)
        
        return all_results

    # ET modifiez votre méthode _save_results_to_db() pour accepter les statistiques :
    def _save_results_to_db(self, project_id, results, statistiques_globales=None):
        """Sauvegarde les résultats d'audit en base avec statistiques"""
        from models.audit_result import AuditResult
        from models.project import db
        
        try:
            audit_result = AuditResult(
                project_id=project_id,
                agency_audit=results.get('agency.txt'),
                routes_audit=results.get('routes.txt'),
                trips_audit=results.get('trips.txt'),
                stops_audit=results.get('stops.txt'),
                stop_times_audit=results.get('stop_times.txt'),
                calendar_audit=results.get('calendar.txt'),
                calendar_dates_audit=results.get('calendar_dates.txt'),
                statistiques=statistiques_globales
            )
            
            
            db.session.add(audit_result)
            
            db.session.commit()
            
            print(f"✅ Résultats d'audit sauvegardés : {audit_result.result_id}")
            return audit_result.result_id
            
        except Exception as e:
            print(f"❌ ERREUR dans _save_results_to_db: {e}")
            db.session.rollback()
            print(f"🔄 Rollback effectué")
            raise
            
    def get_cached_results(self, project_id, file_type):
        """
        Récupère les résultats en cache pour un fichier
        
        Args:
            project_id (str): ID du projet
            file_type (str): Type de fichier
            
        Returns:
            dict ou None: Résultats en cache
        """
        cache_key = f"{project_id}_{file_type}"
        return self._results_cache.get(cache_key)
    
    def clear_cache(self, project_id=None):
        """
        Vide le cache des résultats
        
        Args:
            project_id (str, optional): Si spécifié, vide seulement ce projet
        """
        if project_id:
            # Supprimer seulement les résultats de ce projet
            keys_to_remove = [key for key in self._results_cache.keys() 
                            if key.startswith(f"{project_id}_")]
            for key in keys_to_remove:
                del self._results_cache[key]
            print(f"Cache vidé pour projet {project_id}")
        else:
            # Vider tout le cache
            self._results_cache.clear()
            print("Tous les caches d'audit vidés")
    
    def get_available_audits(self):
        """
        Retourne la liste des audits disponibles
        
        Returns:
            list: Liste des fichiers pour lesquels un audit est disponible
        """
        return list(self.AUDIT_FUNCTIONS.keys())
    

    def _create_error_result(self, file_type, error_message):
        """
        Crée un objet d'erreur standardisé
        
        Args:
            file_type (str): Type de fichier
            error_message (str): Message d'erreur
            
        Returns:
            dict: Objet d'erreur standardisé
        """
        return {
            "file": file_type,
            "status": "error",
            "message": error_message,
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "overall_status": "error",
                "total_checks": 0,
                "passed_checks": 0,
                "warning_checks": 0,
                "error_checks": 1,
                "critical_checks": 0
            }
        }