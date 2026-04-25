import pandas as pd
import json

# Commandes
df = pd.read_csv('data/commandes_mexora.csv', dtype=str)
print('=== COMMANDES ===')
print(f'Total lignes: {len(df)}')
print(f'Doublons id_commande: {df.duplicated(subset=["id_commande"]).sum()}')
missing_liv = df['id_livreur'].isna().sum() + (df['id_livreur'] == '').sum()
print(f'id_livreur manquants: {missing_liv} ({missing_liv/len(df)*100:.1f}%)')
print(f'prix_unitaire=0: {(df["prix_unitaire"].astype(float)==0).sum()}')
print(f'quantite negative: {(df["quantite"].astype(int)<0).sum()}')
print(f'Statuts uniques: {sorted(df["statut"].unique().tolist())}')
print(f'Villes sample: {sorted(df["ville_livraison"].unique().tolist())[:15]}')

# Clients
dc = pd.read_csv('data/clients_mexora.csv', dtype=str)
print(f'\n=== CLIENTS ===')
print(f'Total lignes: {len(dc)}')
dc['email_low'] = dc['email'].str.lower().str.strip()
dups = dc.duplicated(subset=['email_low'], keep=False).sum()
print(f'Doublons email: {dups}')
print(f'Sexe uniques: {sorted(dc["sexe"].unique().tolist())}')

# Produits
with open('data/produits_mexora.json', 'r', encoding='utf-8') as f:
    prods = json.load(f)['produits']
cats = sorted(set(p['categorie'] for p in prods))
nullprix = sum(1 for p in prods if p['prix_catalogue'] is None)
inactifs = sum(1 for p in prods if not p['actif'])
print(f'\n=== PRODUITS ===')
print(f'Total: {len(prods)}')
print(f'Categories: {cats}')
print(f'Prix null: {nullprix}, Inactifs: {inactifs}')

print('\n=== VALIDATION OK ===')
