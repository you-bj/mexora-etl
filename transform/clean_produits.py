# =============================================================================
# Mexora ETL — Nettoyage des Produits
# =============================================================================
# Règles de transformation :
#   R1 - Harmonisation de la casse des catégories (Title Case)
#   R2 - Gestion des produits inactifs (marquage pour SCD)
#   R3 - Traitement des prix_catalogue null (remplacement par médiane)
# =============================================================================

import pandas as pd
import logging


def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les produits Mexora.
    
    Args:
        df: DataFrame brut des produits
    
    Returns:
        DataFrame nettoyé
    """
    initial = len(df)
    logging.info(f"[TRANSFORM] Produits — Début du nettoyage ({initial} lignes)")

    # R1 — Harmonisation de la casse des catégories
    # Standardisation en Title Case après nettoyage des espaces
    df['categorie'] = df['categorie'].str.strip().str.lower().str.title()
    df['sous_categorie'] = df['sous_categorie'].str.strip()
    nb_categories = df['categorie'].nunique()
    logging.info(f"[TRANSFORM] R1 catégories : harmonisées en Title Case ({nb_categories} catégories distinctes)")

    # Correction spécifique : "Électronique" → "Electronique" pour uniformité
    df['categorie'] = df['categorie'].str.replace('Électronique', 'Electronique', regex=False)

    # R2 — Gestion des produits inactifs
    # Les produits inactifs sont conservés car ils peuvent avoir des commandes historiques.
    # Ils seront gérés via SCD Type 2 dans la dimension produit.
    nb_inactifs = (~df['actif']).sum() if df['actif'].dtype == bool else (df['actif'] == False).sum()
    logging.info(f"[TRANSFORM] R2 produits inactifs : {nb_inactifs} produits marqués inactifs (conservés pour SCD)")

    # R3 — Traitement des prix_catalogue null
    # Stratégie : remplacement par la médiane de la catégorie correspondante
    prix_null = df['prix_catalogue'].isna().sum()
    if prix_null > 0:
        mediane_par_cat = df.groupby('categorie')['prix_catalogue'].transform('median')
        df['prix_catalogue'] = df['prix_catalogue'].fillna(mediane_par_cat)
        # Si encore null (catégorie entière sans prix), utiliser la médiane globale
        mediane_globale = df['prix_catalogue'].median()
        df['prix_catalogue'] = df['prix_catalogue'].fillna(mediane_globale)
    logging.info(f"[TRANSFORM] R3 prix catalogue : {prix_null} valeurs nulles remplacées par la médiane catégorie")

    # Standardisation des autres colonnes texte
    df['marque'] = df['marque'].str.strip()
    df['fournisseur'] = df['fournisseur'].str.strip()
    df['nom'] = df['nom'].str.strip()

    logging.info(f"[TRANSFORM] Produits : {initial} → {len(df)} lignes (aucune suppression)")
    return df
