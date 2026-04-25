# =============================================================================
# Mexora ETL — Nettoyage des Commandes
# =============================================================================
# Règles de transformation :
#   R1 - Suppression des doublons sur id_commande (conserver la dernière occurrence)
#   R2 - Standardisation des dates (format cible : YYYY-MM-DD)
#   R3 - Harmonisation des noms de villes via le référentiel regions_maroc
#   R4 - Standardisation des statuts de commande
#   R5 - Suppression des lignes avec quantite <= 0
#   R6 - Suppression des lignes avec prix_unitaire = 0 (commandes test)
#   R7 - Remplacement des id_livreur manquants par la valeur -1
# =============================================================================

import pandas as pd
import logging


def charger_referentiel_villes(filepath: str) -> dict:
    """
    Charge le référentiel géographique et construit un dictionnaire
    de correspondance : variante_ville (lowercase) → nom_ville_standard.
    
    Le mapping inclut :
      - Le nom standard lui-même (en minuscule)
      - Le code_ville (en minuscule)
      - Des variantes connues courantes
    """
    df = pd.read_csv(filepath, encoding='utf-8')
    mapping = {}

    # Variantes connues spécifiques à Mexora
    variantes_specifiques = {
        'tnja': 'Tanger',
        'tange': 'Tanger',
        'tangier': 'Tanger',
        'casa': 'Casablanca',
        'dar el beida': 'Casablanca',
        'fez': 'Fès',
        'marrakesh': 'Marrakech',
    }

    for _, row in df.iterrows():
        ville_std = row['nom_ville_standard']
        code = row['code_ville']

        # Le nom standard en minuscule
        mapping[ville_std.lower()] = ville_std
        # Le code ville en minuscule
        mapping[code.lower()] = ville_std
        # Version sans accents courante
        ville_sans_accent = (ville_std.lower()
                             .replace('é', 'e').replace('è', 'e')
                             .replace('ê', 'e').replace('â', 'a')
                             .replace('î', 'i').replace('ô', 'o')
                             .replace('û', 'u').replace('ï', 'i'))
        if ville_sans_accent != ville_std.lower():
            mapping[ville_sans_accent] = ville_std

    # Ajout des variantes spécifiques
    mapping.update(variantes_specifiques)

    logging.info(f"[TRANSFORM] Référentiel villes chargé : {len(mapping)} entrées de mapping")
    return mapping


def transform_commandes(df: pd.DataFrame, referentiel_path: str) -> pd.DataFrame:
    """
    Applique l'ensemble des règles de nettoyage sur les commandes Mexora.
    
    Args:
        df: DataFrame brut des commandes
        referentiel_path: Chemin vers le fichier regions_maroc.csv
    
    Returns:
        DataFrame nettoyé
    """
    initial = len(df)
    logging.info(f"[TRANSFORM] Commandes — Début du nettoyage ({initial} lignes)")

    # R1 — Suppression des doublons sur id_commande (conserver la dernière occurrence)
    df = df.drop_duplicates(subset=['id_commande'], keep='last')
    r1_supprimees = initial - len(df)
    logging.info(f"[TRANSFORM] R1 doublons : {r1_supprimees} lignes supprimées ({r1_supprimees/initial*100:.1f}%)")

    # R2 — Standardisation des dates (format cible : YYYY-MM-DD)
    df['date_commande'] = pd.to_datetime(
        df['date_commande'], format='mixed', dayfirst=True, errors='coerce'
    )
    dates_invalides = df['date_commande'].isna().sum()
    df = df.dropna(subset=['date_commande'])
    logging.info(f"[TRANSFORM] R2 dates : {dates_invalides} dates invalides supprimées")

    df['date_livraison'] = pd.to_datetime(
        df['date_livraison'], format='mixed', dayfirst=True, errors='coerce'
    )

    # R3 — Harmonisation des villes via le référentiel
    mapping_villes = charger_referentiel_villes(referentiel_path)
    df['ville_livraison'] = df['ville_livraison'].str.strip().str.lower()
    ville_avant = df['ville_livraison'].nunique()
    df['ville_livraison'] = df['ville_livraison'].map(mapping_villes).fillna('Non renseignée')
    ville_apres = df['ville_livraison'].nunique()
    non_mappees = (df['ville_livraison'] == 'Non renseignée').sum()
    logging.info(f"[TRANSFORM] R3 villes : {ville_avant} variantes → {ville_apres} villes standardisées ({non_mappees} non mappées)")

    # R4 — Standardisation des statuts de commande
    mapping_statuts = {
        'livré': 'livré', 'livre': 'livré', 'LIVRE': 'livré', 'DONE': 'livré',
        'annulé': 'annulé', 'annule': 'annulé', 'KO': 'annulé',
        'en_cours': 'en_cours', 'OK': 'en_cours',
        'retourné': 'retourné', 'retourne': 'retourné',
    }
    df['statut'] = df['statut'].replace(mapping_statuts)
    invalides = ~df['statut'].isin(['livré', 'annulé', 'en_cours', 'retourné'])
    nb_invalides = invalides.sum()
    df.loc[invalides, 'statut'] = 'inconnu'
    logging.info(f"[TRANSFORM] R4 statuts : {nb_invalides} valeurs non reconnues → 'inconnu'")

    # R5 — Suppression des quantités invalides (quantite <= 0)
    avant = len(df)
    df['quantite'] = df['quantite'].astype(float).astype(int)
    df = df[df['quantite'] > 0]
    r5_supprimees = avant - len(df)
    logging.info(f"[TRANSFORM] R5 quantités : {r5_supprimees} lignes supprimées (quantité <= 0)")

    # R6 — Suppression des lignes avec prix_unitaire = 0 (commandes test)
    avant = len(df)
    df['prix_unitaire'] = df['prix_unitaire'].astype(float)
    df = df[df['prix_unitaire'] > 0]
    r6_supprimees = avant - len(df)
    logging.info(f"[TRANSFORM] R6 prix : {r6_supprimees} commandes test supprimées (prix = 0)")

    # R7 — Remplacement des id_livreur manquants par -1 (livreur inconnu)
    nb_manquants = df['id_livreur'].isna().sum() + (df['id_livreur'] == '').sum()
    df['id_livreur'] = df['id_livreur'].replace('', pd.NA)
    df['id_livreur'] = df['id_livreur'].fillna('-1')
    logging.info(f"[TRANSFORM] R7 livreurs : {nb_manquants} valeurs manquantes remplacées par -1")

    total_supprimees = initial - len(df)
    logging.info(f"[TRANSFORM] Commandes : {initial} → {len(df)} lignes ({total_supprimees} supprimées au total, {total_supprimees/initial*100:.1f}%)")
    return df
