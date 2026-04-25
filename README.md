# 🚀 Mexora Analytics — Pipeline ETL & Data Warehouse

Bienvenue dans le dépôt du Miniprojet 1 pour **Mexora Analytics**. 
Ce projet consiste à concevoir et implémenter un pipeline ETL complet (Extract, Transform, Load) en Python, afin d'alimenter un Data Warehouse modélisé en étoile pour une entreprise de e-commerce marocaine.

---

## 🛠️ Prérequis

Pour exécuter ce pipeline, vous aurez besoin des éléments suivants :
- **Python 3.8+**
- **PostgreSQL 15+** (si vous souhaitez charger les données en base)
- Les librairies Python listées dans le fichier `requirements.txt` (principalement `pandas`, `sqlalchemy`, et `psycopg2`).

---

## ⚙️ Installation pas à pas

1. **Cloner le dépôt**
   ```bash
   git clone <url-du-depot>
   cd mexora_etl
   ```

2. **Créer un environnement virtuel (recommandé)**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Sur Windows : venv\Scripts\activate
   ```

3. **Installer les dépendances**
   ```bash
   pip install -r requirements.txt
   ```

---

## 🎲 Génération des données de test

Les données brutes ne sont pas fournies dans le dépôt. Vous devez les générer à l'aide du script prévu à cet effet, qui injectera automatiquement les anomalies et problèmes intentionnels requis par le cahier des charges.

```bash
python data/generate_all_data.py
```
Cela va créer 4 fichiers dans le dossier `data/` : `commandes_mexora.csv`, `clients_mexora.csv`, `produits_mexora.json` et `regions_maroc.csv`.

---

## 🚀 Exécution du pipeline ETL

Le pipeline peut être lancé dans deux modes différents :

- **Mode CSV (par défaut)** : Extrait, nettoie, transforme les données et génère les tables du Data Warehouse sous forme de fichiers CSV dans le dossier `output/`.
  ```bash
  python main.py --mode csv
  ```

- **Mode PostgreSQL** : Identique au mode CSV, mais charge les données directement dans votre base de données PostgreSQL locale (nécessite de configurer la connexion dans `config/settings.py`).
  ```bash
  python main.py --mode postgres
  ```

---

## 📂 Structure complète du projet

```text
mexora_etl/
├── config/
│   └── settings.py          # Paramètres de connexion BDD, chemins et constantes métier
├── data/                    # Dossier contenant les données brutes générées
│   ├── generate_all_data.py # Script de génération des données
│   └── ... (fichiers CSV/JSON générés)
├── extract/
│   └── extractor.py         # Fonctions d'extraction par source (sans modification)
├── transform/
│   ├── clean_commandes.py   # Règles de nettoyage des commandes
│   ├── clean_clients.py     # Règles de nettoyage des clients
│   ├── clean_produits.py    # Règles de nettoyage des produits
│   └── build_dimensions.py  # Construction des dimensions et de la table de faits
├── load/
│   └── loader.py            # Logique de chargement PostgreSQL (et fallback CSV)
├── utils/
│   └── logger.py            # Configuration du logging (console + fichier)
├── logs/                    # Fichiers de log horodatés générés lors de l'exécution
├── output/                  # Fichiers finaux générés en mode CSV
├── sql/                     # Scripts SQL pour PostgreSQL (Data Warehouse, vues)
├── main.py                  # Point d'entrée et orchestration du pipeline ETL
├── requirements.txt         # Liste des dépendances Python
├── README.md                # Description et mode d'emploi du projet
└── rapport_transformations.md # Documentation détaillée des règles métier
```

---

## 📊 Bilan de l'exécution et Performances

Notre pipeline a été optimisé pour être extrêmement performant grâce à l'utilisation native de `pandas`.

**Chiffres clés du dernier run :**
- **Données en entrée** : 50 000 commandes
- **Nettoyage initial** : 2 697 lignes rejetées (doublons, prix nuls, quantités invalides)
- **Validation dimensionnelle** : 1 716 lignes rejetées (intégrité référentielle non respectée)
- **Sortie finale** : **45 587 lignes saines** chargées dans `FAIT_VENTES`
- **Temps d'exécution** : **~5.5 secondes**

### 📝 Exemple de sortie des logs (Console)

```log
2026-04-22 22:54:09,680 — INFO — ============================================================
2026-04-22 22:54:09,680 — INFO — DÉMARRAGE PIPELINE ETL MEXORA
2026-04-22 22:54:09,680 — INFO — Mode de chargement : CSV
2026-04-22 22:54:09,680 — INFO — ============================================================
...
2026-04-22 22:54:12,473 — INFO — [TRANSFORM] Commandes : 50000 → 47303 lignes (2697 supprimées au total, 5.4%)
...
2026-04-22 22:54:13,829 — WARNING — [TRANSFORM] FAIT_VENTES : 1716 lignes sans correspondance dimension supprimées
2026-04-22 22:54:13,857 — INFO — [TRANSFORM] FAIT_VENTES : 45587 lignes construites
2026-04-22 22:54:13,860 — INFO — [TRANSFORM] CA total TTC : 2,256,549,112.80 MAD
...
2026-04-22 22:54:15,177 — INFO — ============================================================
2026-04-22 22:54:15,178 — INFO — PIPELINE ETL MEXORA — TERMINÉ AVEC SUCCÈS
2026-04-22 22:54:15,178 — INFO — Durée totale : 5.5 secondes
2026-04-22 22:54:15,180 — INFO — ============================================================
```
