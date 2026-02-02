"""
Service de génération de rapports PDF pour les audits GTFS
"""
import io
import json
from datetime import datetime
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.units import inch, mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.platypus.flowables import HRFlowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
from reportlab.graphics.shapes import Drawing, Rect
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics import renderPDF


class PDFReportGenerator:
    def __init__(self):
        self.buffer = None
        self.doc = None
        self.styles = self._create_styles()
        self.colors = self._define_colors()
        
    def _create_styles(self):
        """Définit les styles de base similaires à Bootstrap"""
        styles = getSampleStyleSheet()
        
        # Style pour les titres principaux
        styles.add(ParagraphStyle(
            name='MainTitle',
            parent=styles['Title'],
            fontSize=24,
            spaceAfter=30,
            textColor=HexColor('#0d6efd'),  # Bleu Bootstrap primary
            alignment=TA_CENTER
        ))
        
        # Style pour les titres de section
        styles.add(ParagraphStyle(
            name='SectionTitle',
            parent=styles['Heading1'],
            fontSize=16,
            spaceAfter=12,
            spaceBefore=20,
            textColor=HexColor('#212529'),  # Couleur texte Bootstrap
            borderWidth=0,
            borderColor=HexColor('#dee2e6'),
            borderPadding=5
        ))
        
        # Style pour les sous-titres
        styles.add(ParagraphStyle(
            name='SubTitle',
            parent=styles['Heading2'],
            fontSize=14,
            spaceAfter=8,
            spaceBefore=12,
            textColor=HexColor('#495057')
        ))
        
        # Style pour le texte normal
        styles.add(ParagraphStyle(
            name='CustomBodyText',
            parent=styles['Normal'],
            fontSize=10,
            spaceAfter=6,
            textColor=HexColor('#212529')
        ))
        
        # Style pour les métriques importantes
        styles.add(ParagraphStyle(
            name='MetricValue',
            parent=styles['Normal'],
            fontSize=18,
            alignment=TA_CENTER,
            textColor=HexColor('#0d6efd'),
            fontName='Helvetica-Bold'
        ))
        
        return styles
    
    def _define_colors(self):
        """Définit les couleurs utilisées (similaires à Bootstrap)"""
        return {
            'primary': HexColor('#0d6efd'),
            'success': HexColor('#198754'),
            'warning': HexColor('#ffc107'),
            'danger': HexColor('#dc3545'),
            'info': HexColor('#0dcaf0'),
            'secondary': HexColor('#6c757d'),
            'light': HexColor('#f8f9fa'),
            'dark': HexColor('#212529'),
            'orange': HexColor('#fd7e14')
        }
    
    def generate_audit_report(self, audit_result, project):
        """Point d'entrée principal pour générer le rapport"""
        self.buffer = io.BytesIO()
        self.doc = SimpleDocTemplate(
            self.buffer,
            pagesize=A4,
            rightMargin=20*mm,
            leftMargin=20*mm,
            topMargin=20*mm,
            bottomMargin=20*mm
        )
        
        self.annexes_data = []
        self.annexe_counter = 0

        # Construire le contenu
        story = []
        
        # Page de garde COMPLÈTE (plus de sommaire séparé)
        story.extend(self._create_cover_page(audit_result, project))
        story.append(PageBreak())
        
        # Pages par fichier audité (SANS CHANGEMENT)
        gtfs_files = [
            ('agency_audit', 'Agency'),
            ('routes_audit', 'Routes'),
            ('trips_audit', 'Trips'),
            ('stops_audit', 'Stops'),
            ('stop_times_audit', 'Stop Times'),
            ('calendar_audit', 'Calendar'),
            ('calendar_dates_audit', 'Calendar Dates')
        ]
        
        for field_name, display_name in gtfs_files:
            audit_data = getattr(audit_result, field_name)
            if audit_data:
                story.extend(self._create_file_audit_page(audit_data, display_name))
                story.append(PageBreak())

        # Annexes
        if self.annexes_data:
            story.extend(self._create_all_annexes())
        
        # Générer le PDF
        self.doc.build(story)
        self.buffer.seek(0)
        return self.buffer
    
# Dans votre PDFReportGenerator, remplacez la méthode _create_cover_page :

    def _create_cover_page(self, audit_result, project):
        """Crée la page de couverture complète avec toutes les statistiques"""
        elements = []
        
        # === EN-TÊTE SIMPLIFIÉ ===
        elements.append(Paragraph("Rapport d'Audit GTFS", self.styles['MainTitle']))
        elements.append(Spacer(1,0))
        
        # Projet et date sur 2 lignes simples
        elements.append(Paragraph(f"Projet : {project.nom_projet}", self.styles['SectionTitle']))
        
        date_audit = audit_result.date_audit.strftime("%d/%m/%Y à %H:%M") if audit_result.date_audit else "N/A"
        date_para = Paragraph(f"Date d'audit : {date_audit}", self.styles['CustomBodyText'])
        date_container = Table([[date_para]], colWidths=[A4[0] - 40*mm])
        date_container.setStyle(TableStyle([
            ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['secondary']),
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('VALIGN', (0, 0), (0, 0), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
            ('RIGHTPADDING', (0, 0), (-1, -1), 15),
        ]))
        elements.append(date_container)
        elements.append(Spacer(1, 5))
        
        # === COMPOSITION DU GTFS ===
        if audit_result.statistiques:
            stats = audit_result.statistiques
            elements.append(Paragraph("Composition du GTFS", self.styles['SectionTitle']))
            elements.append(Spacer(1, 2))
            
            # Récupérer les détails des arrêts
            stops_detail = stats.get('stops_par_type', {})
            points_arret = stops_detail.get('points_arret', 0)
            zones_arret = stops_detail.get('zones_arret', 0)
            
            # Récupérer les infos de période pour la timeline
            periode = stats.get('periode_validite', {})
            date_debut = periode.get('date_debut', 'N/A')
            try:
                date_debut = datetime.strptime(date_debut, '%Y-%m-%d').strftime('%d/%m/%Y')
            except:
                date_debut = date_debut
            date_fin = periode.get('date_fin', 'N/A')
            try:
                date_fin = datetime.strptime(date_fin, '%Y-%m-%d').strftime('%d/%m/%Y')
            except:
                date_fin = date_fin
            duree_jours = periode.get('duree_jours', 0)
            
            # Formater la durée joliment
            if duree_jours > 365:
                duree_text = f"{duree_jours//365} an{('s' if duree_jours//365 > 1 else '')}"
            elif duree_jours > 30:
                duree_text = f"{duree_jours//30} mois"
            else:
                duree_text = f"{duree_jours} jours"
            
            # Données pour les cartes (3 lignes)
            cards_data = [
                [
                    self._create_stats_card(str(stats.get('nombre_fichiers', 0)), "Fichiers GTFS", HexColor('#0d6efd')),  # Bleu
                    self._create_stats_card(str(stats.get('nombre_agences', 0)), "Agences", HexColor('#6610f2')),  # Violet
                    self._create_stats_card(f"{stats.get('nombre_routes', 0):,}", "Lignes", HexColor('#198754')),  # Vert
                    self._create_stats_card(f"{stats.get('nombre_trips', 0):,}", "Voyages", HexColor('#0dcaf0')),  # Cyan
                    self._create_stats_card(f"{stats.get('nombre_shapes', 0):,}", "Shapes", HexColor('#fd7e14')),  # Orange
                ],
                [
                    self._create_empty_card(),
                    self._create_stats_card(f"{stats.get('nombre_stops_total', 0):,}", "Arrêts totaux", HexColor('#ffc107')),  # Jaune
                    self._create_stats_card(f"{points_arret:,}", "Points d'arrêt", HexColor('#20c997')),  # Vert turquoise
                    self._create_stats_card(f"{zones_arret:,}", "Zones d'arrêt", HexColor('#6f42c1')),  # Violet foncé
                    self._create_empty_card(),
                ],
                # LIGNE 3 : Timeline étendue (centrée)
                [
                    self._create_empty_card(),
                    self._create_timeline_card(date_debut, date_fin, duree_text),
                    self._create_empty_card(),
                    self._create_empty_card(),
                    self._create_empty_card(),
                ]
            ]
            
            # Créer le tableau des cartes
            #cards_table = Table(cards_data, colWidths=[1.1*inch] * 6)
            page_width = A4[0] - 40*mm  # Largeur page moins marges
            cards_table = Table(cards_data, colWidths=[page_width/6] * 5)
            cards_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 2),
                ('RIGHTPADDING', (0, 0), (-1, -1), 2),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('SPAN', (1, 2), (3, 2)), 
            ]))
            centered_cards = Table([[cards_table]], colWidths=[A4[0] - 40*mm])
            centered_cards.setStyle(TableStyle([
                ('ALIGN', (0, 0), (0, 0), 'CENTER'),
                ('VALIGN', (0, 0), (0, 0), 'MIDDLE'),
            ]))

            # Puis ajouter centered_cards au lieu de cards_table :
            #elements.append(centered_cards)
            cards_container = Table([[centered_cards]], colWidths=[A4[0] - 40*mm])
            cards_container.setStyle(TableStyle([
                ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['secondary']),  # Bordure gauche grise
                ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')), 
                ('ALIGN', (0, 0), (0, 0), 'CENTER'),        # Ajouter cette ligne
                ('VALIGN', (0, 0), (0, 0), 'MIDDLE'),           # Fond gris clair
                ('TOPPADDING', (0, 0), (-1, -1), 15),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
                ('LEFTPADDING', (0, 0), (-1, -1), 15),
                ('RIGHTPADDING', (0, 0), (-1, -1), 15),
            ]))

            elements.append(cards_container)
            #elements.append(cards_table)
            elements.append(Spacer(1, 5))
            
            # === TIMELINE DES FICHIERS GTFS ===
            elements.extend(self._create_files_timeline_section(stats))
            elements.append(Spacer(1, 5))
        
        # === VUE D'ENSEMBLE DE L'AUDIT ===
        elements.extend(self._create_audit_overview_section(audit_result))
        elements.append(Spacer(1, 5))
        
        # Note finale
        note = """Ce rapport présente les résultats de l'audit de conformité des données GTFS.
        Chaque fichier est analysé selon les spécifications GTFS officielles avec attribution 
        de scores et recommandations d'amélioration."""
        
        #elements.append(Paragraph(note, self.styles['CustomBodyText']))
        
        return elements
    
    def _create_summary_page(self, audit_result):
        """Crée la page de sommaire global"""
        elements = []
        
        elements.append(Paragraph("Sommaire Exécutif", self.styles['MainTitle']))
        elements.append(Spacer(1, 20))
        
        # Analyser tous les fichiers audités pour créer un résumé
        total_files = 0
        files_with_scores = []
        
        gtfs_fields = [
            ('agency_audit', 'Agency'),
            ('routes_audit', 'Routes'), 
            ('trips_audit', 'Trips'),
            ('stops_audit', 'Stops'),
            ('stop_times_audit', 'Stop Times'),
            ('calendar_audit', 'Calendar'),
            ('calendar_dates_audit', 'Calendar Dates')
        ]
        
        for field_name, display_name in gtfs_fields:
            audit_data = getattr(audit_result, field_name)
            if audit_data:
                total_files += 1
                # Extraire le score principal si disponible
                score = self._extract_main_score(audit_data)
                if score is not None:
                    files_with_scores.append((display_name, score))
        
        # Statistiques globales
        elements.append(Paragraph("Vue d'ensemble", self.styles['SectionTitle']))
        
        stats_data = [
            ['Fichiers audités', str(total_files)],
            ['Fichiers avec score', str(len(files_with_scores))],
        ]
        
        if files_with_scores:
            avg_score = sum(score for _, score in files_with_scores) / len(files_with_scores)
            stats_data.append(['Score moyen', f"{avg_score:.1f}/100"])
        
        stats_table = Table(stats_data, colWidths=[2*inch, 1.5*inch])
        stats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), self.colors['light']),
            ('TEXTCOLOR', (0, 0), (-1, -1), self.colors['dark']),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, self.colors['secondary'])
        ]))
        
        elements.append(stats_table)
        elements.append(Spacer(1, 30))
        
        # Tableau récapitulatif des scores par fichier
        if files_with_scores:
            elements.append(Paragraph("Scores par fichier", self.styles['SectionTitle']))
            
            score_data = [['Fichier GTFS', 'Score', 'Grade']]
            for file_name, score in files_with_scores:
                grade = self._calculate_grade(score)
                score_data.append([file_name, f"{score:.1f}/100", grade])
            
            score_table = Table(score_data, colWidths=[2*inch, 1*inch, 1*inch])
            score_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
                ('TEXTCOLOR', (0, 0), (-1, 0), white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
                ('GRID', (0, 0), (-1, -1), 1, self.colors['secondary'])
            ]))
            
            elements.append(score_table)
        
        return elements
    
    def _create_file_audit_page(self, audit_data, file_name):
        """Crée une page pour un fichier audité spécifique"""
        elements = []
        
        # Titre du fichier
        # Titre du fichier
        elements.append(Paragraph(f"Audit du fichier \"{file_name}.txt\"", self.styles['MainTitle']))
        elements.append(Spacer(1, 5))
                
        # Traiter chaque catégorie d'audit
        categories = [
            ('required_fields', 'Validation de la donnée', 'danger'),
            ('data_format', 'Formattage de la donnée', 'info'),
            ('data_consistency', 'Cohérence des données', 'warning'),
            ('accessiblity', 'Accessibilité', 'success'),  # Note: typo dans votre code original
            ('ufr_analysis', 'Accessibilité UFR', 'success'),
            ('hierarchy_analysis', 'Hiérarchie Parent_Station', 'info'),
            ('temporal_analysis', 'Cohérence Temporelle', 'warning'),
            ('statistics', 'Statistiques', 'secondary')
        ]
        
        for category_key, category_title, color_type in categories:
            if category_key in audit_data:
                category_data = audit_data[category_key]
                if category_key == 'statistics':
                    elements.extend(self._create_statistics_with_charts(category_data))
                else:
                    elements.extend(self._create_category_section(category_data, category_title, color_type, file_name))
                elements.append(Spacer(1, 20))
        
        return elements
    
    def _create_category_section(self, category_data, title, color_type, file_name=None):
        """Crée une section élégante pour une catégorie d'audit"""
        elements = []
        
        # === EN-TÊTE DE SECTION ===
        # Données pour l'en-tête : [Titre, Score, Grade, Statut]
        header_data = []
        
        # === EN-TÊTE DE SECTION ===
        if 'score' in category_data and 'grade' in category_data:
            # Cas avec score complet : 4 colonnes
            score = category_data['score']
            grade = category_data['grade']
            status = category_data.get('status', 'pass')
            header_data = [[title, f"{score:.0f}/100", grade, status.upper()]]
            header_table = Table(header_data, colWidths=[4*inch, 1.0*inch, 0.7*inch, 1.0*inch])
        else:
            # Cas avec seulement le statut : 2 colonnes
            status = category_data.get('status', 'unknown')
            header_data = [[title, status.upper()]]
            header_table = Table(header_data, colWidths=[5.7*inch, 1.0*inch])
            
        # Style de l'en-tête
        color = self.colors.get(color_type, self.colors['primary'])
        status_color = {
            'pass': self.colors['success'],
            'warning': self.colors['warning'],
            'error': self.colors['danger'],
            'critical': self.colors['dark']
        }.get(category_data.get('status', 'unknown'), color)

        if 'score' in category_data:
            score_value = category_data['score']
            if score_value >= 90:
                score_color = self.colors['success']  # Vert
            elif score_value >= 70:
                score_color = self.colors['warning']  # Orange  
            else:
                score_color = self.colors['danger']   # Rouge
        else:
            score_color = color

        if 'score' in category_data and 'grade' in category_data:
            # Style pour 4 colonnes
            header_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, 0), HexColor('#f8f9fa')),
                ('BACKGROUND', (1, 0), (1, 0), score_color),
                ('BACKGROUND', (2, 0), (2, 0), score_color),
                ('BACKGROUND', (3, 0), (3, 0), status_color),
                ('TEXTCOLOR', (0, 0), (0, 0), HexColor('#212529')),
                ('TEXTCOLOR', (1, 0), (-1, -1), white),
                ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
                ('FONTNAME', (1, 0), (-1, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 12),
                ('ALIGN', (0, 0), (0, 0), 'LEFT'),
                ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('LEFTPADDING', (0, 0), (0, 0), 15),
                ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
            ]))
        else:
            # Style pour 2 colonnes seulement
            header_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, 0), HexColor('#f8f9fa')),
                ('BACKGROUND', (1, 0), (1, 0), status_color),
                ('TEXTCOLOR', (0, 0), (0, 0), HexColor('#212529')),
                ('TEXTCOLOR', (1, 0), (1, 0), white),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 12),
                ('ALIGN', (0, 0), (0, 0), 'LEFT'),
                ('ALIGN', (1, 0), (1, 0), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('LEFTPADDING', (0, 0), (0, 0), 15),
                ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
            ]))
                
        elements.append(header_table)
        
        # === BARRE DE PROGRESSION ===
        #if 'percentage' in category_data:
        #    elements.append(Spacer(1, 5))
        #    elements.append(self._create_progress_bar(category_data['percentage'], color_type))
            
            # Texte du pourcentage sous la barre
        #    pct_text = f"{category_data['percentage']}% de conformité"
        #    elements.append(Paragraph(pct_text, self.styles['CustomBodyText']))
        
        elements.append(Spacer(1, 5))

        # === NOUVEAU : AFFICHAGE MESSAGE D'ERREUR ===
        if 'message' in category_data and category_data.get('status') == 'error':
            # Créer une alerte d'erreur
            error_message = category_data['message']
            error_para = Paragraph(f"<b>Erreur :</b> {error_message}", self.styles['CustomBodyText'])
            
            error_table = Table([[error_para]], colWidths=[6.7*inch])
            error_table.setStyle(TableStyle([
                ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['danger']),
                ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8d7da')),  # Fond rouge clair
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
                ('LEFTPADDING', (0, 0), (-1, -1), 15),
                ('RIGHTPADDING', (0, 0), (-1, -1), 15),
            ]))
            
            elements.append(error_table)
            #return elements
        else:
            # === CHECKS DIRECTEMENT ===
            if 'checks' in category_data and category_data['checks']:
                for check in category_data['checks']:
                    elements.extend(self._create_elegant_check(check))

            if title == "Accessibilité UFR":
                # Vérifier s'il y a une erreur de champ manquant
                has_missing_field = False
                if 'checks' in category_data:
                    for check in category_data['checks']:
                        if ('manquant' in check.get('message', '').lower() or 
                            'missing' in check.get('message', '').lower()):
                            has_missing_field = True
                            break
                
                # Si pas de champ manquant, afficher les sections enrichies
                if not has_missing_field:
                    elements.extend(self._create_ufr_analysis_section(category_data))

            if title == "Validation de la donnée" and 'checks' in category_data:
                has_details = any(check.get('details') and len(check.get('details', {})) > 0 
                                for check in category_data['checks'])
                
                if has_details:
                    annexe_letter = chr(65 + self.annexe_counter)  # A, B, C...
                    self.annexes_data.append({
                        'letter': annexe_letter,
                        'title': f"Annexe {annexe_letter} - Validation {file_name}",
                        'category_data': category_data,
                        'file_name': file_name
                    })
                    self.annexe_counter += 1
                    
                    # Ajouter le renvoi
                    renvoi = Paragraph(f"<i>→ Voir détails en Annexe {annexe_letter}</i>", self.styles['CustomBodyText'])
                    elements.append(renvoi)

        return elements
    
    def _create_elegant_check(self, check):
        """Crée un check élégant avec bordure colorée"""

        if check.get('description') == 'Analyse métier UFR':
            return []
         
        elements = []
        
        # Couleur selon le statut
        status = check.get('status', 'unknown')
        border_color = {
            'pass': self.colors['success'],
            'warning': self.colors['warning'], 
            'error': self.colors['danger'],
            'critical': self.colors['dark']
        }.get(status, self.colors['secondary'])
        
        # Titre du check en gras
        title = check.get('description', 'Check sans nom')
        title_para = Paragraph(f"<b>{title}</b>", self.styles['CustomBodyText'])
        
        # Message du check
        message = check.get('message', '')
        message_para = Paragraph(message, self.styles['CustomBodyText'])
        
        # Statistiques si disponibles
        stats_text = ""
        if 'statistics' in check:
            stats = check['statistics']
            if 'number' in stats:
                total = stats['number']
                invalid = stats.get('invalid', 0)
                empty = stats.get('empty', 0)
                valid = total - invalid - empty
                stats_text = f"{valid}/{total} valides ({((valid/total)*100):.1f}%)"
        
        # Badge de statut
        #status_badge = Paragraph(f"<b>{status.upper()}</b>", self.styles['CustomBodyText'])
                
        # Créer le contenu selon qu'on a des stats ou pas
# Créer le contenu selon qu'on a des stats ou pas
        if stats_text and message:
            # Titre sur une ligne, puis message + stats sur la ligne suivante
            title_para = Paragraph(f"<b>{title}</b>", self.styles['CustomBodyText'])
            message_stats_data = [[Paragraph(message, self.styles['CustomBodyText']), 
                                Paragraph(f"<i>{stats_text}</i>", self.styles['CustomBodyText'])]]
            message_stats_table = Table(message_stats_data, colWidths=[4.5*inch, 2.2*inch])
            message_stats_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (0, 0), 'LEFT'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
                ('VALIGN', (0, 0), (-1, 0), 'TOP'),
            ]))
            
            check_data = [
                [title_para],
                [message_stats_table]
            ]
            check_table = Table(check_data, colWidths=[6.7*inch])
            
        elif stats_text:
            # Pas de message, juste titre + stats
            check_data = [[Paragraph(f"<b>{title}</b>", self.styles['CustomBodyText']), 
                        Paragraph(f"<i>{stats_text}</i>", self.styles['CustomBodyText'])]]
            check_table = Table(check_data, colWidths=[5.0*inch, 1.7*inch])
        else:
            # Cas sans stats : 1 colonne
            content_html = f"<b>{title}</b>"
            if message:
                content_html += f"<br/>{message}"
            
            check_data = [[Paragraph(content_html, self.styles['CustomBodyText'])]]
            check_table = Table(check_data, colWidths=[6.7*inch])

        # Style commun pour tous les cas
        check_table.setStyle(TableStyle([
            ('LINEBEFORE', (0, 0), (0, -1), 4, border_color),
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
        ]))

        elements.append(check_table)
        elements.append(Spacer(1, 5))
        return elements

    def _create_progress_bar(self, percentage, color_type):
        drawing = Drawing(500, 25)  # Plus large
        # Barre de fond grise
        drawing.add(Rect(0, 8, 500, 12, fillColor=HexColor('#e9ecef'), strokeColor=None))
        # Barre colorée
        width = (percentage / 100) * 500
        color = self.colors.get(color_type, self.colors['success'])
        drawing.add(Rect(0, 8, width, 12, fillColor=color, strokeColor=None))
        # Texte du pourcentage à droite
        from reportlab.graphics.shapes import String
        drawing.add(String(510, 12, f"{percentage}%", fontSize=10))
        return drawing
    
    def _create_check_section(self, check):
        """Crée une section pour un check individuel avec bordure colorée"""
        elements = []
        
        # Déterminer la couleur selon le statut
        status = check.get('status', 'unknown')
        border_color = {
            'pass': self.colors['success'],
            'warning': self.colors['warning'], 
            'error': self.colors['danger'],
            'critical': self.colors['dark']
        }.get(status, self.colors['secondary'])
        
        # Créer le contenu du check
        description = check.get('description', 'Check sans nom')
        message = check.get('message', '')
        
        # Données pour le tableau avec bordure
        check_content = f"{description}"
        if message:
            check_content += f"<br/>{message}"
        
        # Statistiques si disponibles
        if 'statistics' in check:
            stats = check['statistics']
            if 'number' in stats:
                total = stats['number']
                invalid = stats.get('invalid', 0)
                empty = stats.get('empty', 0)
                valid = total - invalid - empty
                
                stats_text = f"→ {valid}/{total} valides ({((valid/total)*100):.1f}%)"
                if invalid > 0:
                    stats_text += f", {invalid} invalides"
                if empty > 0:
                    stats_text += f", {empty} vides"
                
                check_content += f"<br/><font size='9'>{stats_text}</font>"
        
        # Créer un tableau avec bordure colorée à gauche
        check_data = [[check_content, status.upper()]]
        check_table = Table(check_data, colWidths=[4.5*inch, 0.8*inch])
        check_table.setStyle(TableStyle([
            # Bordure gauche colorée de 3px (comme border-start Bootstrap)
            ('LINEBEFORE', (0, 0), (0, 0), 3, border_color),
            # Fond léger pour la cellule de contenu
            ('BACKGROUND', (0, 0), (0, 0), HexColor('#f8f9fa')),
            # Styling du contenu
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (0, 0), 10),
            ('RIGHTPADDING', (0, 0), (0, 0), 5),
            # Badge de statut à droite
            ('BACKGROUND', (1, 0), (1, 0), border_color),
            ('TEXTCOLOR', (1, 0), (1, 0), white),
            ('FONTNAME', (1, 0), (1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (1, 0), (1, 0), 9),
        ]))
        
        elements.append(check_table)
        elements.append(Spacer(1, 8))
        return elements
    
    def _create_business_metrics_section(self, metrics):
        """Crée une section pour les métriques business"""
        elements = []
        
        elements.append(Paragraph("Métriques :", self.styles['SubTitle']))
        
        # Convertir les métriques en tableau
        metrics_data = []
        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                if key.endswith('_rate') or key.endswith('_percentage'):
                    metrics_data.append([key.replace('_', ' ').title(), f"{value}%"])
                else:
                    metrics_data.append([key.replace('_', ' ').title(), str(value)])
        
        if metrics_data:
            metrics_table = Table(metrics_data, colWidths=[2.5*inch, 1.5*inch])
            metrics_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), self.colors['light']),
                ('TEXTCOLOR', (0, 0), (-1, -1), self.colors['dark']),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 0.5, self.colors['secondary'])
            ]))
            
            elements.append(metrics_table)
            elements.append(Spacer(1, 10))
        
        return elements
    
    def _extract_main_score(self, audit_data):
        """Extrait le score principal d'un audit"""
        # Priorité : required_fields > data_format > premier disponible
        for key in ['required_fields', 'data_format', 'data_consistency', 'accessiblity']:
            if key in audit_data and 'score' in audit_data[key]:
                return audit_data[key]['score']
        return None
    
    def _calculate_grade(self, score):
        """Calcule la note littérale à partir du score"""
        if score >= 95: return "A+"
        elif score >= 90: return "A"
        elif score >= 85: return "B+"
        elif score >= 80: return "B"
        elif score >= 75: return "C+"
        elif score >= 70: return "C"
        elif score >= 60: return "D"
        else: return "F"

    def _create_statistics_with_charts(self, statistics):
        """Crée la section statistiques avec graphiques circulaires"""
        elements = []
        
        # Vérifier d'abord s'il y a des données à afficher
        has_data = False
        charts_to_display = []
        
        if 'repartition' in statistics:
            repartition_data = statistics['repartition']
            
            # Mapping des titres
            chart_titles = {
                'routes_by_type': 'Répartition par type de transport',
                'routes_by_agency': 'Répartition par agence',
                'stops_by_direction_id': 'Répartition par sens de direction',
                'stats_exception_type': 'Répartition par type d\'exception',
                'stats_pickup': 'Répartition par type de montée',
                'stats_dropoff': 'Répartition par type de descente',
                'stop_repartition': 'Répartition par type d\'arrêt'
            }
            
            # Collecter seulement les charts qui ont des données
            for stat_key, data in repartition_data.items():
                if stat_key in chart_titles and data:  # Vérifier que data n'est pas vide
                    charts_to_display.append((stat_key, data, chart_titles[stat_key]))
                    has_data = True
        
        # Si aucune donnée à afficher, retourner une liste vide
        if not has_data:
            return elements
        
        # Créer l'en-tête seulement s'il y a des données
        header_data = [["Statistiques", "INFO"]]
        header_table = Table(header_data, colWidths=[5.7*inch, 1.0*inch])
        header_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, 0), HexColor('#f8f9fa')),
            ('BACKGROUND', (1, 0), (1, 0), self.colors['secondary']),
            ('TEXTCOLOR', (0, 0), (0, 0), HexColor('#212529')),
            ('TEXTCOLOR', (1, 0), (1, 0), white),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 12),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (0, 0), 15),
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
        ]))
        
        elements.append(header_table)
        elements.append(Spacer(1, 5))
        
        # Afficher les charts qui ont des données
        for stat_key, data, title in charts_to_display:
            elements.extend(self._create_chart_section(data, title))
            elements.append(Spacer(1, 10))
        
        return elements
    
    def _create_chart_section(self, data, title):
        """Crée une section avec graphique + tableau, avec pagination automatique"""
        elements = []
        
        # Créer le graphique
        chart = self._create_pie_chart(data, title)
        
        # Couleurs pour la légende
        colors = [
            HexColor('#FF6384'), HexColor('#36A2EB'), HexColor('#FFCE56'), 
            HexColor('#4BC0C0'), HexColor('#9966FF'), HexColor('#FF9F40'),
            HexColor('#FF6384'), HexColor('#C9CBCF'), HexColor('#4BC0C0'),
            HexColor('#36A2EB'), HexColor('#FFCE56'), HexColor('#9966FF')
        ]

        # Préparer toutes les données
        all_data = []
        for i, (key, item_data) in enumerate(data.items()):
            display_name = item_data.get('type_name', item_data.get('agency_name', key))
            
            # Créer un petit carré coloré pour la légende
            from reportlab.graphics.shapes import Drawing, Rect
            color_square = Drawing(12, 12)
            color_square.add(Rect(0, 0, 12, 12, 
                                fillColor=colors[i % len(colors)], 
                                strokeColor=HexColor('#ffffff'), 
                                strokeWidth=1))
            
            all_data.append([
                color_square,
                display_name,
                str(item_data['count']),
                f"{item_data['percentage']}%"
            ])

        # Diviser en chunks de 15 lignes max par tableau
        chunks = [all_data[i:i+15] for i in range(0, len(all_data), 15)]
        
        # Première page : titre + graphique + premier chunk
        title_para = Paragraph(f"<b>{title}</b>", self.styles['CustomBodyText'])
        
        if chunks:
            # Premier tableau avec en-tête
            first_table_data = [['', 'Élément', 'Nombre', '%']] + chunks[0]
            first_table = Table(first_table_data, colWidths=[0.3*inch, 1.7*inch, 0.8*inch, 0.8*inch])
            first_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
                ('TEXTCOLOR', (0, 0), (-1, 0), white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BACKGROUND', (0, 1), (-1, -1), HexColor('#ffffff')),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
            ]))
            
            # Layout graphique + premier tableau
            chart_table_data = [[chart, first_table]]
            chart_table_layout = Table(chart_table_data, colWidths=[2*inch, 3*inch])
            chart_table_layout.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (0, 0), 0),
                ('LEFTPADDING', (1, 0), (1, 0), 20),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ]))
            
            # Tableau principal avec bordure
            main_data = [
                [title_para],
                [chart_table_layout]
            ]
            
            main_table = Table(main_data, colWidths=[6.7*inch])
            main_table.setStyle(TableStyle([
                ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['secondary']),
                ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('LEFTPADDING', (0, 0), (-1, -1), 15),
            ]))
            
            elements.append(main_table)
            
            # Tableaux suivants (sans graphique, juste les données)
            for chunk in chunks[1:]:
                elements.append(Spacer(1, 15))
                
                # Tableau de continuation (sans en-tête)
                continuation_table = Table(chunk, colWidths=[0.2*inch, 1.2*inch, 0.6*inch, 0.6*inch])
                continuation_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, -1), HexColor('#ffffff')),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
                    ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('TOPPADDING', (0, 0), (-1, -1), 4),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                    ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
                ]))
                
                # Encadrer aussi les tableaux de continuation
                continuation_main = Table([[continuation_table]], colWidths=[3*inch])
                continuation_main.setStyle(TableStyle([
                    ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['secondary']),
                    ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
                    ('TOPPADDING', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('LEFTPADDING', (0, 0), (-1, -1), 15),
                ]))
                
                elements.append(continuation_main)

        return elements

    def _create_pie_chart(self, data, title):
        """Crée un graphique circulaire avec ReportLab"""
        from reportlab.graphics.charts.piecharts import Pie
        from reportlab.graphics.shapes import Drawing
        
        # Préparer les données
        labels = []
        values = []
        
        for key, item_data in data.items():
            display_name = item_data.get('type_name', item_data.get('agency_name', key))
            labels.append(display_name)
            values.append(item_data['count'])
        
        # Couleurs similaires à votre HTML
        colors = [
            HexColor('#FF6384'), HexColor('#36A2EB'), HexColor('#FFCE56'), 
            HexColor('#4BC0C0'), HexColor('#9966FF'), HexColor('#FF9F40'),
            HexColor('#FF6384'), HexColor('#C9CBCF'), HexColor('#4BC0C0'),
            HexColor('#36A2EB'), HexColor('#FFCE56'), HexColor('#9966FF')
        ]
        
        # Créer le graphique
        drawing = Drawing(160, 160)
        pie = Pie()
        pie.x = 10
        pie.y = 10
        pie.width = 140
        pie.height = 140
        pie.data = values
        #pie.labels = labels
        pie.slices.strokeColor = HexColor('#ffffff')
        pie.slices.strokeWidth = 1
        
        # Appliquer les couleurs
        for i, color in enumerate(colors[:len(values)]):
            pie.slices[i].fillColor = color
        
        drawing.add(pie)
        return drawing
    

    def _create_all_annexes(self):
        """Crée toutes les annexes à la suite"""
        elements = []
        
        for annexe in self.annexes_data:
            elements.extend(self._create_single_annexe(annexe))
            elements.append(Spacer(1, 30))  # Espace entre annexes, pas de PageBreak
        
        return elements

    def _create_single_annexe(self, annexe_data):
        """Crée une annexe individuelle"""
        elements = []
        
        # Titre de l'annexe
        elements.append(Paragraph(annexe_data['title'], self.styles['MainTitle']))
        elements.append(Spacer(1, 15))
        
        # Détails des checks avec style similaire aux autres
        category_data = annexe_data['category_data']
        if 'checks' in category_data:
            for check in category_data['checks']:
                if check.get('details'):
                    elements.extend(self._create_styled_details_section(check))
        
        return elements

    def _create_styled_details_section(self, check):
        """Affiche les détails avec le style des autres checks"""
        elements = []
        
        # Titre du check en plus gros
        title = check.get('description', 'Check sans nom')
        title_para = Paragraph(f"<b><font size='12'>{title}</font></b>", self.styles['CustomBodyText'])
        
        # Détails formatés
        details_html = self._format_structured_details(check['details'])
        details_para = Paragraph(details_html, self.styles['CustomBodyText'])
        
        # Tableau avec titre et détails séparés pour moins d'espacement
        check_data = [
            [title_para],
            [details_para]
        ]
        
        check_table = Table(check_data, colWidths=[6.7*inch])
        check_table.setStyle(TableStyle([
            # Bordure gauche colorée
            ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['danger']),
            # Fond gris
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            # Alignement
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            # Espacement réduit entre titre et détails
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (0, 0), 2),  # Moins d'espace après le titre
            ('BOTTOMPADDING', (0, 1), (0, 1), 8),  # Espace normal après les détails
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
        ]))
        
        elements.append(check_table)
        elements.append(Spacer(1, 8))
        return elements

    def _format_structured_details(self, details):
        """Formate les détails avec structure hiérarchique"""
        html = ""
        
        for key, value in details.items():
            html += f"<b><i>{key}:</i></b> "
            
            if isinstance(value, dict):
                # Dictionnaire : affichage structuré
                html += "<br/>"
                for sub_key, sub_value in value.items():
                    html += f"&nbsp;&nbsp;&nbsp;&nbsp;<i>{sub_key}:</i> "
                    if isinstance(sub_value, list):
                        if len(sub_value) <= 10:
                            html += f"{', '.join(map(str, sub_value))}<br/>"
                        else:
                            html += f"{', '.join(map(str, sub_value[:10]))}... (et {len(sub_value)-10} autres)<br/>"
                    else:
                        html += f"{sub_value}<br/>"
            elif isinstance(value, list):
                # Liste : affichage compact
                if len(value) <= 15:
                    html += f"{', '.join(map(str, value))}<br/>"
                else:
                    html += f"{', '.join(map(str, value[:15]))}... (et {len(value)-15} autres)<br/>"
            else:
                # Valeur simple
                html += f"{value}<br/>"
            
            html += "<br/>"  # Espace entre les sections
        
        return html
    
    def _create_empty_card(self):
        """Crée une carte vide pour l'espacement"""
        from reportlab.graphics.shapes import Drawing
        return Drawing(70, 50)
    
    def _create_stats_card(self, number, label, accent_color):
        """Crée une carte statistique style dashboard professionnel"""
        from reportlab.graphics.shapes import Drawing, Rect, String
        from reportlab.lib.colors import white
        
        card_width = 70
        card_height = 50
        
        drawing = Drawing(card_width, card_height)
        
        # 1. OMBRE EN PREMIER (décalée vers le bas-droite)
        drawing.add(Rect(3, 1, card_width-4, card_height-4,
                        fillColor=HexColor('#00000015'),  # Ombre légère
                        strokeColor=None,
                        rx=4, ry=4))
        
        # 2. FOND GRIS CLAIR PAR-DESSUS L'OMBRE
        drawing.add(Rect(2, 2, card_width-4, card_height-4, 
                        fillColor=HexColor('#f8f9fa'),  # Gris très clair
                        strokeColor=HexColor('#dee2e6'),  # Bordure gris clair
                        strokeWidth=1,
                        rx=4, ry=4))
        
        # 3. BARRE D'ACCENT EN HAUT
        drawing.add(Rect(2, card_height-8, card_width-4, 6,
                        fillColor=accent_color,
                        strokeColor=None,
                        rx=4, ry=4))
        
        # 4. TEXTES PAR-DESSUS TOUT
        # Texte du nombre (noir, plus lisible)
        drawing.add(String(card_width/2, card_height-25, number,
                        fontSize=16,
                        fontName='Helvetica-Bold',
                        fillColor=HexColor('#212529'),  # Noir professionnel
                        textAnchor='middle'))
        
        # Texte du libellé (gris foncé)
        drawing.add(String(card_width/2, 10, label,
                        fontSize=8,
                        fontName='Helvetica',
                        fillColor=HexColor('#6c757d'),  # Gris foncé
                        textAnchor='middle'))
        
        return drawing


    def _create_timeline_card(self, date_debut, date_fin, duree_text):
        """Crée une carte timeline professionnelle"""
        from reportlab.graphics.shapes import Drawing, Rect, String, Line
        from reportlab.lib.colors import white
        
        card_width = int((A4[0] - 40*mm) * 0.8)
        card_height = 50
        
        drawing = Drawing(card_width, card_height)
        
        # Fond gris clair professionnel
        drawing.add(Rect(2, 2, card_width-4, card_height-4,
                        fillColor=HexColor('#f8f9fa'),
                        strokeColor=HexColor('#dee2e6'),
                        strokeWidth=1,
                        rx=6, ry=6))
        
        # Barre d'accent en haut (bleu professionnel)
        drawing.add(Rect(2, card_height-8, card_width-4, 6,
                        fillColor=HexColor('#0d6efd'),
                        strokeColor=None,
                        rx=6, ry=6))
        
        # Timeline horizontale (gris foncé au lieu de blanc)
        y_center = card_height // 2 - 5
        timeline_start = 40
        timeline_end = card_width - 40
        drawing.add(Line(timeline_start, y_center, timeline_end, y_center,
                        strokeColor=HexColor('#6c757d'), strokeWidth=3))
        
        # Points début et fin (accent bleu)
        drawing.add(Rect(timeline_start-4, y_center-4, 8, 8,
                        fillColor=HexColor('#0d6efd'), strokeColor=white,
                        strokeWidth=1, rx=4, ry=4))
        drawing.add(Rect(timeline_end-4, y_center-4, 8, 8,
                        fillColor=HexColor('#0d6efd'), strokeColor=white,
                        strokeWidth=1, rx=4, ry=4))
        
        # Textes (noir professionnel)
        drawing.add(String(timeline_start, card_height-20, f"{date_debut}",
                        fontSize=9, fontName='Helvetica-Bold',
                        fillColor=HexColor('#212529'), textAnchor='middle'))
        
        drawing.add(String(card_width//2, card_height-21, f"{duree_text}",
                        fontSize=14, fontName='Helvetica-Bold',
                        fillColor=HexColor('#212529'), textAnchor='middle'))
        
        drawing.add(String(card_width//2, 8, "Période de validité",
                        fontSize=8, fontName='Helvetica',
                        fillColor=HexColor('#6c757d'), textAnchor='middle'))
        
        drawing.add(String(timeline_end, card_height-20, f"{date_fin}",
                        fontSize=9, fontName='Helvetica-Bold',
                        fillColor=HexColor('#212529'), textAnchor='middle'))
        
        return drawing

    def _create_files_timeline_section(self, stats):
        """Crée la section timeline des fichiers GTFS"""
        elements = []
        
        elements.append(Paragraph("Fichiers GTFS", self.styles['SectionTitle']))
        elements.append(Spacer(1, 2))
        
        # Timeline des fichiers
        timeline_drawing = self._create_files_timeline(stats)
        
        # Légende
        #fichiers_obligatoires = stats.get('fichiers_obligatoires', {})
        #nb_obligatoires_presents = len(fichiers_obligatoires.get('presents', []))
        #nb_obligatoires_total = 5  # agency, routes, trips, stops, stop_times
        
        #legende = f"Obligatoires: {nb_obligatoires_presents}/{nb_obligatoires_total}"
        #if nb_obligatoires_presents == nb_obligatoires_total:
        #    legende += " ✓"
        
       # legende_para = Paragraph(legende, self.styles['CustomBodyText'])
        
        # Conteneur pour timeline + légende
       # timeline_content = Table([
        #    [timeline_drawing],
          #  [legende_para]
        #], colWidths=[A4[0] - 70*mm])
        
        timeline_container = Table([[timeline_drawing]], colWidths=[A4[0] - 40*mm])
        timeline_container.setStyle(TableStyle([
            ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['secondary']),
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('VALIGN', (0, 0), (0, 0), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 15),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
            ('RIGHTPADDING', (0, 0), (-1, -1), 15),
        ]))
        
        elements.append(timeline_container)
        
        return elements

    def _create_files_timeline(self, stats):
        """Crée la timeline horizontale des fichiers"""
        from reportlab.graphics.shapes import Drawing, Rect, String, Line, Circle
        from reportlab.lib.colors import white
        
        timeline_width = 450
        timeline_height = 60
        
        drawing = Drawing(timeline_width, timeline_height)
        
        # Fond général
        drawing.add(Rect(0, 0, timeline_width, timeline_height,
                        fillColor=HexColor('#f8f9fa'),
                        strokeColor=HexColor('#dee2e6'),
                        strokeWidth=1,
                        rx=5, ry=5))
        
        # Sections : 40% - 20% - 40%
        obligatoires_width = timeline_width * 0.4
        calendaires_width = timeline_width * 0.25
        optionnels_width = timeline_width * 0.35
        
        # Section obligatoires (fond vert clair)
        drawing.add(Rect(5, 5, obligatoires_width-10, timeline_height-10,
                        fillColor=HexColor('#d4edda'),
                        strokeColor=None,
                        rx=3, ry=3))
        
        # Section calendaires (fond jaune clair)
        drawing.add(Rect(obligatoires_width+5, 5, calendaires_width-10, timeline_height-10,
                        fillColor=HexColor('#fff3cd'),
                        strokeColor=None,
                        rx=3, ry=3))
        
        # Section optionnels (fond gris)
        drawing.add(Rect(obligatoires_width+calendaires_width+5, 5, optionnels_width-15, timeline_height-10,
                        fillColor=HexColor('#e9ecef'),
                        strokeColor=None,
                        rx=3, ry=3))
        
        # Lignes séparatrices
        drawing.add(Line(obligatoires_width, 10, obligatoires_width, timeline_height-10,
                        strokeColor=HexColor('#6c757d'), strokeWidth=2))
        drawing.add(Line(obligatoires_width+calendaires_width, 10, obligatoires_width+calendaires_width, timeline_height-10,
                        strokeColor=HexColor('#6c757d'), strokeWidth=2))
        
        # Titres des sections
        drawing.add(String(obligatoires_width/2, timeline_height-15, "OBLIGATOIRES",
                        fontSize=9, fontName='Helvetica-Bold',
                        fillColor=HexColor('#155724'), textAnchor='middle'))
        
        drawing.add(String(obligatoires_width + calendaires_width/2, timeline_height-15, "CALENDAIRES",
                        fontSize=9, fontName='Helvetica-Bold',
                        fillColor=HexColor('#856404'), textAnchor='middle'))
        
        drawing.add(String(obligatoires_width + calendaires_width + optionnels_width/2, timeline_height-15, "OPTIONNELS",
                        fontSize=9, fontName='Helvetica-Bold',
                        fillColor=HexColor('#495057'), textAnchor='middle'))
        
        # === FICHIERS OBLIGATOIRES (toujours affichés) ===
        fichiers_obligatoires = ['agency.txt', 'routes.txt', 'trips.txt', 'stops.txt', 'stop_times.txt']
        fichiers_obligatoires_presents = stats.get('fichiers_obligatoires', {}).get('presents', [])
        
        x_start = 25  # Plus éloigné du bord
        x_end = obligatoires_width - 25  # Plus éloigné du bord
        x_step = (x_end - x_start) / max(1, len(fichiers_obligatoires) - 1) if len(fichiers_obligatoires) > 1 else 0
        y_center = timeline_height // 2
        
        for i, fichier in enumerate(fichiers_obligatoires):
            x_pos = x_start + i * x_step
            
            if fichier in fichiers_obligatoires_presents:
                # Point plein vert
                drawing.add(Circle(x_pos, y_center, 4,
                                fillColor=HexColor('#28a745'),
                                strokeColor=white,
                                strokeWidth=2))
            else:
                # Point plein rouge
                drawing.add(Circle(x_pos, y_center, 4,
                                fillColor=HexColor('#dc3545'),
                                strokeColor=white,
                                strokeWidth=2))
            
            # Nom du fichier (sans .txt)
            nom_court = fichier.replace('.txt', '')
            drawing.add(String(x_pos, y_center-15, nom_court,
                            fontSize=8, fontName='Helvetica',
                            fillColor=HexColor('#495057'),
                            textAnchor='middle'))
        
        # === FICHIERS CALENDAIRES (toujours affichés) ===
        fichiers_calendaires = ['calendar.txt', 'calendar_dates.txt']
        # Récupérer tous les fichiers présents pour vérifier
        tous_fichiers_presents = fichiers_obligatoires_presents + stats.get('fichiers_optionnels_presents', [])
        
        cal_x_start = obligatoires_width + 30
        cal_x_end = obligatoires_width + calendaires_width - 35
        cal_x_step = (cal_x_end - cal_x_start) / max(1, len(fichiers_calendaires) - 1) if len(fichiers_calendaires) > 1 else 0
        
        for i, fichier in enumerate(fichiers_calendaires):
            x_pos = cal_x_start + i * cal_x_step
            
            if fichier in tous_fichiers_presents:
                # Point plein vert
                drawing.add(Circle(x_pos, y_center, 4,
                                fillColor=HexColor('#28a745'),
                                strokeColor=white,
                                strokeWidth=2))
            else:
                # Point plein gris
                drawing.add(Circle(x_pos, y_center, 4,
                                fillColor=HexColor("#495057"),
                                strokeColor=white,
                                strokeWidth=2))
            
            # Nom du fichier
            nom_court = fichier.replace('.txt', '').replace('_', '_')
            #if nom_court == 'calendar_dates':
            #    nom_court = 'cal_dates'  # Raccourcir pour l'affichage
            drawing.add(String(x_pos, y_center-15, nom_court,
                            fontSize=8, fontName='Helvetica',
                            fillColor=HexColor("#495057"),
                            textAnchor='middle'))
        
        fichiers_optionnels_presents = [f for f in stats.get('fichiers_optionnels_presents', []) 
                                    if f not in fichiers_calendaires]

        if fichiers_optionnels_presents:
            opt_x_start = obligatoires_width + calendaires_width + 25
            opt_x_end = timeline_width - 25
            
            if len(fichiers_optionnels_presents) == 1:
                # Centrer si un seul fichier
                x_pos = (opt_x_start + opt_x_end) / 2
                positions = [x_pos]
            else:
                opt_x_step = (opt_x_end - opt_x_start) / max(1, len(fichiers_optionnels_presents) - 1)
                positions = [opt_x_start + i * opt_x_step for i in range(len(fichiers_optionnels_presents))]
            
            for i, fichier in enumerate(fichiers_optionnels_presents):
                x_pos = positions[i]
                
                # Point plein vert (toujours présents puisqu'on les affiche)
                drawing.add(Circle(x_pos, y_center, 4,
                                fillColor=HexColor("#495057"),
                                strokeColor=white,
                                strokeWidth=2))
                
                # Nom du fichier
                nom_court = fichier.replace('.txt', '').replace('_', '_')
                drawing.add(String(x_pos, y_center-15, nom_court,
                                fontSize=8, fontName='Helvetica',
                                fillColor=HexColor('#495057'),
                                textAnchor='middle'))
        
        return drawing
    def _create_audit_overview_section(self, audit_result):
        """Crée la section vue d'ensemble de l'audit"""
        elements = []
        
        elements.append(Paragraph("Vue d'ensemble de l'audit", self.styles['SectionTitle']))
        elements.append(Spacer(1, 2))
        
        # Calculer les métriques d'audit
        total_files, avg_score = self._calculate_audit_metrics(audit_result)
        
        # Tableau 2 lignes
        overview_data = [
            ['Fichiers audités', str(total_files)],
            ['Score moyen', f"{avg_score:.0f}/100" if avg_score is not None else "N/A"]
        ]
        
        overview_table = Table(overview_data, colWidths=[2.5*inch, 1.5*inch])
        overview_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), self.colors['light']),
            ('TEXTCOLOR', (0, 0), (-1, -1), self.colors['dark']),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, self.colors['secondary'])
        ]))
        overview_container = Table([[overview_table]], colWidths=[A4[0] - 40*mm])
        overview_container.setStyle(TableStyle([
            ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['secondary']),
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('VALIGN', (0, 0), (0, 0), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 15),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
            ('RIGHTPADDING', (0, 0), (-1, -1), 15),
        ]))
        elements.append(overview_container)
        
        return elements

    def _calculate_audit_metrics(self, audit_result):
        """Calcule les métriques globales de l'audit"""
        files_audited = 0
        scores = []
        
        # Liste des audits possibles
        audit_fields = [
            'agency_audit', 'routes_audit', 'trips_audit', 'stops_audit',
            'stop_times_audit', 'calendar_audit', 'calendar_dates_audit'
        ]
        
        for field in audit_fields:
            audit_data = getattr(audit_result, field, None)
            if audit_data:
                files_audited += 1
                
                # Extraire le score principal
                score = self._extract_main_score(audit_data)
                if score is not None:
                    scores.append(score)
        
        # Calculer la moyenne des scores
        avg_score = sum(scores) / len(scores) if scores else None
        
        return files_audited, avg_score
    
    def _create_ufr_analysis_section(self, ufr_data):
        """Crée l'affichage enrichi pour l'analyse UFR"""
        elements = []
        
        # 1. MÉTRIQUES PRINCIPALES (toujours afficher si business_metrics existe)
        if 'business_metrics' in ufr_data:
            elements.extend(self._create_ufr_metrics_section(ufr_data['business_metrics']))
        
        # 2. RÉPARTITION (seulement si données disponibles ET champ présent)
        if ('repartition' in ufr_data and 
            ufr_data['repartition'] and 
            ufr_data.get('field_info', {}).get('field_present', True)):  # Vérifier que le champ existe
            elements.extend(self._create_ufr_repartition_section(ufr_data['repartition']))
        
        # 3. RECOMMANDATIONS (seulement si présentes)
        if 'recommendations' in ufr_data and ufr_data['recommendations']:
            elements.extend(self._create_ufr_recommendations_section(ufr_data['recommendations']))
        
        return elements

    def _create_ufr_metrics_section(self, metrics):
        """Section métriques UFR avec cartes et barres"""
        elements = []
        
        # Créer les cartes de métriques
        metrics_content = []
        
        # Cas spécial : tous à 0
        if metrics.get('no_info_count', 0) == metrics.get('total_records', 0) and metrics.get('total_records', 0) > 0:
            warning_para = Paragraph(
                "<b>Attention :</b> L'accessibilité UFR n'est pas renseignée sur ce réseau (tous les champs sont à la valeur 0)",
                self.styles['CustomBodyText']
            )
            metrics_content.append([warning_para])
        
        # Métriques principales en tableau 2 colonnes
        metrics_data = [
            [
                f"Taux de renseignement\n{metrics.get('completion_rate', 0)}%\n{metrics.get('accessible_count', 0) + metrics.get('not_accessible_count', 0)}/{metrics.get('total_records', 0)} avec info",
                f"Taux d'accessibilité\n{metrics.get('accessibility_rate', 0)}%\n{metrics.get('accessible_count', 0)}/{metrics.get('total_records', 0)} accessibles"
            ]
        ]
        
        metrics_table = Table(metrics_data, colWidths=[3*inch, 3*inch])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#e9ecef')),
            ('TEXTCOLOR', (0, 0), (-1, -1), HexColor('#212529')),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('TOPPADDING', (0, 0), (-1, -1), 15),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
        ]))
        
        metrics_content.append([metrics_table])
        
        # Conteneur principal avec bordure bleue
        main_content = Table(metrics_content, colWidths=[6.7*inch])
        main_content.setStyle(TableStyle([
            ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['primary']),  # Bordure bleue
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
            ('RIGHTPADDING', (0, 0), (-1, -1), 15),
        ]))
        
        elements.append(main_content)
        elements.append(Spacer(1, 10))
        return elements

    def _create_ufr_repartition_section(self, repartition):
        """Section répartition UFR avec graphique et tableau"""
        elements = []
        
        # Titre
        title_para = Paragraph("<b>Répartition des valeurs</b>", self.styles['CustomBodyText'])
        
        # Créer le graphique circulaire
        chart = self._create_ufr_pie_chart(repartition)
        
        # Créer le tableau de répartition avec couleurs - LARGEURS ENCORE AUGMENTÉES
        table_data = [['Valeur', 'Libellé', 'Nombre', '%']]
        
        # Trier les valeurs
        sorted_entries = sorted(repartition.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 999)
        
        for value, data in sorted_entries:
            table_data.append([value, data.get('type_name', ''), str(data['count']), f"{data['percentage']}%"])
        
        # LARGEURS MAXIMISÉES pour libellés très longs
        repartition_table = Table(table_data, colWidths=[0.4*inch, 2.8*inch, 0.7*inch, 0.4*inch])
        
        # Style avec couleurs UFR
        table_style = [
            ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
            ('TEXTCOLOR', (0, 0), (-1, 0), white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (0, 1), (1, -1), 'LEFT'),  # Libellés alignés à gauche
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
        ]
        
        # Ajouter les couleurs de fond par ligne
        row = 1
        for value, data in sorted_entries:
            int_value = int(value) if value.isdigit() else 999
            if int_value == 0:
                bg_color = HexColor('#fff3cd')  # Jaune clair
            elif int_value == 1:
                bg_color = HexColor('#d1eddf')  # Vert clair
            elif int_value == 2:
                bg_color = HexColor('#f8d7da')  # Rouge clair
            else:
                bg_color = HexColor('#f8f9fa')  # Gris clair
            
            table_style.append(('BACKGROUND', (0, row), (-1, row), bg_color))
            row += 1
        
        repartition_table.setStyle(TableStyle(table_style))
        
        # Layout graphique + tableau - TABLEAU ENCORE PLUS LARGE
        chart_table_layout = Table([[chart, repartition_table]], colWidths=[1.8*inch, 4.7*inch])
        chart_table_layout.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # CENTRAGE VERTICAL
            ('LEFTPADDING', (0, 0), (0, 0), 0),
            ('LEFTPADDING', (1, 0), (1, 0), 10),  # Réduit l'espace entre graphique et tableau
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        
        content_data = [
            [title_para],
            [chart_table_layout]
        ]
        
        # Conteneur principal avec bordure bleue
        main_content = Table(content_data, colWidths=[6.7*inch])
        main_content.setStyle(TableStyle([
            ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['primary']),  # Bordure bleue
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
            ('RIGHTPADDING', (0, 0), (-1, -1), 15),
        ]))
        
        elements.append(main_content)
        elements.append(Spacer(1, 10))
        return elements

    def _create_ufr_pie_chart(self, repartition):
        """Crée le graphique circulaire UFR avec couleurs spécifiques"""
        from reportlab.graphics.charts.piecharts import Pie
        from reportlab.graphics.shapes import Drawing
        
        # Couleurs UFR
        ufr_colors = {
            '0': HexColor('#ffc107'),  # Orange pour "pas d'info"
            '1': HexColor('#198754'),  # Vert pour "accessible"  
            '2': HexColor('#dc3545'),  # Rouge pour "non accessible"
        }
        
        # Préparer données
        labels = []
        values = []
        colors = []
        
        sorted_entries = sorted(repartition.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 999)
        
        for key, data in sorted_entries:
            labels.append(data.get('type_name', key))
            values.append(data['count'])
            colors.append(ufr_colors.get(key, HexColor('#6c757d')))
        
        # Créer le graphique
        drawing = Drawing(120, 120)
        pie = Pie()
        pie.x = 10
        pie.y = 10
        pie.width = 100
        pie.height = 100
        pie.data = values
        pie.slices.strokeColor = HexColor('#ffffff')
        pie.slices.strokeWidth = 1
        
        # Appliquer couleurs
        for i, color in enumerate(colors):
            pie.slices[i].fillColor = color
        
        drawing.add(pie)
        return drawing

    def _create_ufr_recommendations_section(self, recommendations):
        """Section recommandations UFR avec alertes colorées"""
        elements = []
        
        # Titre
        title_para = Paragraph("<b>Recommandations</b>", self.styles['CustomBodyText'])
        
        # Contenu des recommandations
        reco_content = [title_para]
        
        for rec in recommendations:
            # Couleur selon le type
            if rec.get('type') == 'critical':
                alert_color = HexColor('#f8d7da')  # Rouge clair
                icon = '⚠️'
            elif rec.get('type') == 'warning':
                alert_color = HexColor('#fff3cd')  # Jaune clair
                icon = '⚠️'
            elif rec.get('type') == 'success':
                alert_color = HexColor('#d1eddf')  # Vert clair
                icon = '✅'
            else:
                alert_color = HexColor('#d1ecf1')  # Bleu clair
                icon = 'ℹ️'
            
            # Contenu de la recommandation
            reco_text = f"{icon} <b>{rec.get('message', '')}</b><br/>{rec.get('description', '')}"
            reco_para = Paragraph(reco_text, self.styles['CustomBodyText'])
            
            # Encadrer chaque recommandation
            reco_table = Table([[reco_para]], colWidths=[6*inch])
            reco_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), alert_color),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('LEFTPADDING', (0, 0), (-1, -1), 10),
                ('RIGHTPADDING', (0, 0), (-1, -1), 10),
            ]))
            
            reco_content.append(reco_table)
            reco_content.append(Spacer(1, 5))
        
        # Conteneur principal avec bordure jaune
        main_content = Table([[Table([[item] for item in reco_content], colWidths=[6.7*inch])]], colWidths=[6.7*inch])
        main_content.setStyle(TableStyle([
            ('LINEBEFORE', (0, 0), (0, -1), 4, self.colors['warning']),  # Bordure jaune
            ('BACKGROUND', (0, 0), (-1, -1), HexColor('#f8f9fa')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 15),
            ('RIGHTPADDING', (0, 0), (-1, -1), 15),
        ]))
        
        elements.append(main_content)
        elements.append(Spacer(1, 10))
        return elements