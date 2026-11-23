# 📘 Guide pour comprendre (et défendre) le projet — en français

> Ce guide est pour toi, Ousmane. Le README est pour les recruteurs.
> Objectif : que tu saches **expliquer chaque choix** à l'oral, pas juste lancer les commandes.
> Méthode : une idée à la fois → une analogie d'entreprise → le « pourquoi » → à toi de reformuler.

---

## 🧭 Idée 1 — Le problème, en une phrase

Une entreprise veut des **prix de 50 cryptos, minute par minute**, fiables, pour tester des stratégies de trading (« backtesting »).

🏢 **Analogie** : c'est la chaîne logistique d'un grand distributeur. Les fournisseurs (Binance, CoinGecko) livrent de la marchandise (des données). Notre rôle : **réceptionner, contrôler, ranger, et servir** aux équipes qui en ont besoin.

✍️ *À toi : explique avec tes mots pourquoi un simple script Python lancé à la main ne suffit pas.*

---

## 🧭 Idée 2 — Les 5 étages du pipeline

| Étage | Rôle | 🏢 Analogie |
|---|---|---|
| **Sources** (Binance, CoinGecko) | Fournissent les données | Les fournisseurs |
| **Airflow** | Déclenche et surveille chaque tâche | Le planning des livraisons + le chef d'équipe |
| **PostgreSQL** | Stocke pour servir vite | Les rayons du magasin (accès rapide) |
| **Parquet** | Archive l'historique compressé | L'entrepôt d'archives au sous-sol (peu cher, volumineux) |
| **FastAPI** | Distribue aux clients | Le guichet de retrait |

---

## 🧭 Idée 3 — Idempotence (LE concept à maîtriser)

**Définition** : exécuter une opération 1 fois ou 10 fois donne **le même résultat**.

🏢 **Analogie** : un bon de commande **numéroté**. Si la secrétaire saisit deux fois le bon n°4521, le système dit « déjà enregistré » : pas de commande en double.

**Dans le code** : la clé primaire `(symbol, open_time)` + `ON CONFLICT DO NOTHING`.
Une bougie « BTC à 14h32 » ne peut exister qu'une fois, quoi qu'il arrive.

**Pourquoi c'est vital** : Airflow *relance* les tâches qui échouent. Sans idempotence, chaque retry créerait des doublons et fausserait les calculs.

👀 **Preuve dans nos tests** : on a vu `rows_fetched=200 rows_written=197` → les 3 bougies déjà connues ont été ignorées automatiquement.

---

## 🧭 Idée 4 — Pourquoi récupérer 5 bougies chaque minute (et pas 1) ?

Si une exécution rate (réseau coupé 2 minutes), la suivante **récupère aussi les minutes manquantes** grâce au chevauchement. Et grâce à l'idempotence, aucun doublon.

🏢 **Analogie** : le livreur repasse chaque jour avec les colis des 5 derniers jours non signés. Rien ne se perd, rien n'est livré deux fois.

C'est de l'**auto-guérison** : le pipeline répare ses petits trous tout seul.

---

## 🧭 Idée 5 — Pourquoi on ignore la bougie « en cours » ?

À 14h32m20s, la bougie de 14h32 n'est pas finie : son prix de clôture va encore changer. L'enregistrer maintenant = enregistrer une **valeur fausse** qu'on ne corrigerait jamais (la ligne existe déjà → ignorée).

🏢 **Analogie** : ne pas facturer un client avant la fin de la livraison.

Code : `parse_klines` écarte toute bougie dont `close_time >= maintenant`.

---

## 🧭 Idée 6 — Retry, backoff, et erreurs qu'on ne réessaie PAS

- **Retry** : si l'API répond 503 (« surchargé »), on **réessaie** après 0,5 s, puis 1 s, puis 2 s (*backoff exponentiel*). On laisse respirer le serveur au lieu de le marteler.
- **429** (« trop de requêtes ») : on respecte l'en-tête `Retry-After` — c'est le fournisseur qui dit « reviens dans 30 s ».
- **404** : on **ne réessaie pas** ; l'adresse n'existera pas plus à la 3ᵉ tentative.

🏢 **Analogie** : rappeler un fournisseur dont la ligne est occupée → oui. Rappeler un numéro qui n'existe pas → non.

👀 **Preuve** : `tests/test_http_client.py` lance un vrai mini-serveur qui échoue exprès. On a aussi vu les retries en direct quand mon faux serveur était éteint.

---

## 🧭 Idée 7 — L'isolation des pannes (`partial`)

Si la crypto « TON » échoue mais que les 49 autres marchent : on enregistre les 49, et le run est marqué **`partial`**. Si **toutes** échouent, on lève une erreur → Airflow réessaie.

🏢 **Analogie** : si 1 camion sur 50 est en retard, on décharge quand même les 49 autres.

---

## 🧭 Idée 8 — La « porte qualité » (quality gate)

Après chaque collecte, la tâche `quality_check` vérifie :
1. **Fraîcheur** : dernière bougie < 5 minutes ?
2. **Complétude** : moins de 5 % de minutes manquantes ?

Sinon → la tâche passe **rouge**. Mieux vaut une alerte qu'un trou silencieux dans un backtest.

🏢 **Analogie** : le contrôle à la réception des marchandises.

👀 **Vécu pendant les tests** : elle a échoué quand mes données avaient 7 min de retard (« données périmées »), puis est repassée verte après collecte. Elle fait son travail.

---

## 🧭 Idée 9 — PostgreSQL **et** Parquet : pourquoi les deux ?

| | PostgreSQL | Parquet |
|---|---|---|
| Usage | Servir l'API vite | Archive d'analyse |
| Format | Lignes | **Colonnes**, compressé |
| Coût | Plus cher (disque + base active) | Très économique |
| Lecture | Requêtes ciblées | Gros calculs (pandas, Spark, DuckDB) |

Mesuré : 57 800 lignes → **~3 Mo** en Parquet.
Le partitionnement `date=…/symbol=…` permet de ne lire que les dossiers utiles.

---

## 🧭 Idée 10 — Pourquoi pas Spark ? (question de jury probable)

Le sujet dit « Spark *si volume important* ». Notre volume : 50 × 1 440 = **72 000 lignes/jour**, ~26 M/an. PostgreSQL + pandas le font sans effort. Ajouter Spark = complexité sans bénéfice.

🏢 **Analogie** : louer un cargo pour livrer un colis.

**La bonne réponse en entretien** : « Je l'ai écarté volontairement, et voici le seuil où je l'ajouterais : centaines de symboles, données tick-par-tick, ou retraitement sur plusieurs années. Mon format Parquet partitionné est déjà prêt pour Spark. »
*Savoir justifier l'absence d'un outil vaut mieux que l'empiler.*

---

## 🧭 Idée 11 — Pourquoi les DAGs sont si courts ?

Les DAGs ne font que **planifier** ; la logique est dans `src/crypto_pipeline/`.
Raison : un DAG Airflow est pénible à tester ; une fonction Python normale se teste en 1 seconde.

C'est pour ça qu'on a 33 tests rapides.

---

## 🧭 Idée 12 — Une vraie leçon de ce projet : les versions

Pendant les tests, j'ai découvert que **la dernière version de pyarrow exigeait NumPy 2**, alors qu'Airflow 2.10 impose NumPy 1.26. Résultat : l'export Parquet plantait.

Solution : fixer `pyarrow<21` et installer via le **fichier de contraintes officiel d'Airflow**.

🏢 **Analogie** : une pièce neuve qui ne s'adapte pas à une vieille machine.

**Leçon** : on ne fait pas confiance à « ça marche chez moi » ; on exécute vraiment. Raconte cette histoire en entretien : elle montre une vraie expérience de terrain.

---

## 🎤 Questions de soutenance — entraîne-toi à répondre

1. Qu'est-ce que l'idempotence et où la vois-tu dans ton projet ?
2. Que se passe-t-il si Binance est en panne 3 minutes ?
3. Pourquoi ne stockes-tu pas la bougie en cours ?
4. Pourquoi Parquet *et* PostgreSQL ?
5. Pourquoi n'as-tu pas utilisé Spark ?
6. Comment sais-tu que ton pipeline est en bonne santé ? *(→ `/pipeline/status`, `ops.pipeline_runs`, quality gate)*
7. Comment ton API gère-t-elle 1 million de bougies ? *(→ pagination par curseur `next_start`)*
8. Quelle est la limite de ton architecture ? *(→ pas du vrai streaming ; LocalExecutor ; voir README)*

---

## 🧪 Pour apprendre en pratiquant (dans cet ordre)

1. `python scripts/mock_binance.py &` puis `python -m crypto_pipeline.cli seed` et `backfill --days 1`
2. Ouvre `http://localhost:8000/docs` et teste `/klines/BTC?interval=5m&limit=5`
3. **Casse quelque chose exprès** : arrête le faux serveur, lance `ingest-klines`, observe les retries dans les logs
4. Lis `src/crypto_pipeline/transform.py` (le plus court, le plus pur) puis `ingest.py`
5. Lis `tests/test_ingest.py` : chaque test raconte une règle métier

💡 **Règle d'or** : si tu ne peux pas expliquer une ligne, ne la mets pas dans ton portfolio. Demande-moi, on la décortique ensemble.
