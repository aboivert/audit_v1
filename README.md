# GTFS Audit Tool - Version Sandbox

Application Flask simple pour auditer les fichiers GTFS en mode sandbox.

## 🚀 Installation et démarrage

### 1. Créer la structure des dossiers

```bash
mkdir gtfs_audit_app
cd gtfs_audit_app

# Créer les dossiers nécessaires
mkdir templates uploads
```

### 2. Créer les fichiers

Créez les fichiers suivants dans votre projet :

```
gtfs_audit_app/
├── app.py                 # Application Flask principale
├── requirements.txt       # Dépendances Python
├── README.md             # Ce fichier
├── templates/
│   ├── base.html         # Template de base
│   ├── index.html        # Page d'accueil
│   └── audit.html        # Page d'audit
└── uploads/              # Dossier pour les fichiers uploadés (créé automatiquement)
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Lancer l'application

```bash
python app.py
```

L'application sera accessible sur http://localhost:5000

## 🎯 Utilisation

### 1. Page d'accueil
- Uploadez un fichier GTFS au format ZIP
- Le système vérifie et charge automatiquement les fichiers

### 2. Page d'audit
- Naviguez dans les différents types de fichiers GTFS (agency, routes, etc.)
- Cliquez sur une fonction d'audit pour la développer
- Configurez les paramètres si nécessaire
- Cliquez sur "Exécuter l'audit" pour voir les résultats

## 🔧 Ajouter de nouvelles fonctions d'audit

Pour ajouter une nouvelle fonction d'audit, modifiez le fichier `app.py` et ajoutez votre fonction avec le décorateur :

```python
@audit_function(
    file_type="agency",  # Type de fichier GTFS
    name="Ma nouvelle fonction",  # Nom affiché
    description="Description de ce que fait ma fonction",
    parameters={
        "mon_parametre": {
            "type": "slider",  # slider, checkbox, text, select
            "min": 0,
            "max": 100,
            "default": 50,
            "description": "Description du paramètre"
        }
    }
)
def ma_nouvelle_fonction(gtfs_data, **params):
    """Ma fonction d'audit"""
    
    # Récupérer les paramètres
    seuil = params.get('mon_parametre', 50)
    
    # Votre logique d'audit ici
    if 'agency.txt' not in gtfs_data:
        return 0, []
    
    # Exemple de calcul
    score = 85  # Score entre 0 et 100
    problem_ids = ['agency_1', 'agency_2']  # Liste des IDs problématiques
    
    return score, problem_ids
```

## 📋 Types de paramètres supportés

### Slider (curseur)
```python
"mon_slider": {
    "type": "slider",
    "min": 0,
    "max": 100,
    "default": 50,
    "description": "Seuil en pourcentage"
}
```

### Checkbox (case à cocher)
```python
"mon_checkbox": {
    "type": "checkbox", 
    "default": True,
    "description": "Activer cette option"
}
```

### Texte
```python
"mon_texte": {
    "type": "text",
    "default": "valeur par défaut",
    "description": "Entrez une valeur"
}
```

## 🗂️ Structure des données GTFS

Les données GTFS sont chargées comme un dictionnaire de DataFrames pandas :

```python
gtfs_data = {
    'agency.txt': DataFrame,
    'routes.txt': DataFrame,
    'trips.txt': DataFrame,
    # etc.
}
```

## 🎨 Fonctionnalités de l'interface

- **Interface responsive** avec Bootstrap
- **Accordéons** pour organiser les fonctions par type de fichier
- **Paramètres dynamiques** générés automatiquement
- **Affichage des scores** avec codes couleurs :
  - 🟢 Excellent (90-100%)
  - 🟡 Bon (70-89%)
  - 🟠 Attention (50-69%)
  - 🔴 Problème (0-49%)
- **Liste des IDs problématiques** affichable/masquable

## 🔒 Sécurité

- Validation des extensions de fichiers (ZIP uniquement)
- Sécurisation des noms de fichiers avec `secure_filename()`
- Nettoyage automatique des fichiers temporaires

## 🐛 Debug

Pour activer le mode debug, l'application est configurée avec `debug=True`. 
En production, changez cette valeur à `False`.

## 📝 Notes

- **Mode Sandbox** : Les fichiers GTFS ne sont pas sauvegardés de façon permanente
- **Session temporaire** : Les données sont stockées en session et perdues à la fermeture
- **Extensibilité** : Architecture prête pour l'ajout de nouvelles fonctionnalités

## 🎯 Prochaines étapes possibles

1. Ajout de la gestion de projets avec base de données
2. Sauvegarde des historiques d'audit
3. Génération de rapports PDF
4. Fonctions de visualisation des données
5. API REST complète