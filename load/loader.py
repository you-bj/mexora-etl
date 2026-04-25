# =============================================================================
# Mexora ETL — Module de Chargement (Load)
# =============================================================================
# Stratégies de chargement :
#   - Dimensions : TRUNCATE + RELOAD (replace)
#   - Table de faits : UPSERT via ON CONFLICT (insert/update)
# =============================================================================

import pandas as pd
import logging
from config.settings import SCHEMA_DWH, CHUNK_SIZE


def charger_dimension(df: pd.DataFrame, table_name: str, engine, if_exists='replace'):
    """
    Charge une table de dimension dans PostgreSQL.
    Stratégie : replace (truncate + reload) pour les dimensions.
    
    Args:
        df: DataFrame à charger
        table_name: Nom de la table dans le schéma DWH
        engine: Engine SQLAlchemy
        if_exists: Stratégie de chargement ('replace' ou 'append')
    """
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA_DWH,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=CHUNK_SIZE
        )
        logging.info(f"[LOAD] {table_name} : {len(df)} lignes chargées dans {SCHEMA_DWH}.{table_name}")
    except Exception as e:
        logging.error(f"[LOAD] Erreur lors du chargement de {table_name} : {e}")
        raise


def charger_faits(df: pd.DataFrame, table_name: str, engine):
    """
    Charge la table de faits dans PostgreSQL.
    Stratégie : insertion par chunks.
    
    Pour un vrai upsert ON CONFLICT, il faudrait utiliser
    SQLAlchemy Core avec les dialects PostgreSQL.
    Ici on utilise une stratégie append simplifiée.
    
    Args:
        df: DataFrame de la table de faits
        table_name: Nom de la table
        engine: Engine SQLAlchemy
    """
    try:
        total = len(df)
        loaded = 0

        for i in range(0, total, CHUNK_SIZE):
            chunk = df.iloc[i:i + CHUNK_SIZE]
            chunk.to_sql(
                name=table_name,
                con=engine,
                schema=SCHEMA_DWH,
                if_exists='append' if i > 0 else 'replace',
                index=False,
                method='multi',
                chunksize=CHUNK_SIZE
            )
            loaded += len(chunk)
            logging.info(f"[LOAD] {table_name} : {loaded}/{total} lignes chargées")

        logging.info(f"[LOAD] {table_name} : chargement terminé ({total} lignes)")
    except Exception as e:
        logging.error(f"[LOAD] Erreur lors du chargement de {table_name} : {e}")
        raise


def sauvegarder_csv(df: pd.DataFrame, filepath: str, description: str = ''):
    """
    Sauvegarde un DataFrame en CSV (mode fallback si PostgreSQL non disponible).
    
    Args:
        df: DataFrame à sauvegarder
        filepath: Chemin de sortie
        description: Description pour le log
    """
    df.to_csv(filepath, index=False, encoding='utf-8')
    logging.info(f"[LOAD] {description} : {len(df)} lignes sauvegardées dans {filepath}")
