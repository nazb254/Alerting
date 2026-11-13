# Alertes portefeuilles ZB et Europa One

Job Python 3.11+ autonome, lance chaque minute par cron sur Linux. Aucun serveur
web ni processus permanent n'est necessaire.

## Regles

| Controle | Frequence | Deduplication persistante |
|---|---|---|
| Toutes les news, sans filtre editorial | Chaque execution | Une alerte par identifiant news, tous titres et portefeuilles concernes regroupes |
| Variation depuis la cloture precedente | Chaque execution | Une alerte par titre, jour Europe/Paris, sens et seuil : 5, 10, 15 % |
| Tous les evenements du calendrier des titres detenus | Une fois par jour a partir de 01:00 Europe/Paris | Un rappel par evenement/date a J-7, J-3, J-1 et J |

Les seuils sont inclusifs. Un passage direct a +16 % produit un seul mail pour
+5 %, +10 % et +15 %, qui sont tous reserves dans la meme transaction. Les
seuils baissiers sont independants. Un titre commun a plusieurs portefeuilles
n'est controle qu'une fois ; le mail nomme les portefeuilles concernes.

La journee de surveillance commence a **01:00 heure de Paris**, ete comme hiver.
Entre minuit et 00:59, la date de surveillance reste celle de la veille.
Cette date sert aux seuils journaliers, a la comparaison de la date du cours
et aux rappels du calendrier. Le controle quotidien est configure a 01:00.

Les news anterieures au premier demarrage effectif ne sont pas envoyees.
L'instant de demarrage est conserve dans DuckDB, y compris apres un redemarrage.
Chaque titre a son propre curseur. Apres une panne, la collecte reprend au dernier
curseur reussi ; un chevauchement de 24 h couvre aussi l'indexation tardive.
Une news indexee avec plus de 24 h de retard apres un controle reussi peut donc
echapper a la collecte. Une relecture ne renvoie pas les news deja reservees.

Les rappels utilisent les jours **calendaires**, week-ends compris. Si le cron est
indisponible a 01:00, le premier passage ulterieur fait le controle du jour.
Il n'envoie pas retroactivement les rappels des jours integralement manques.
Si une date de publication change, les rappels de la nouvelle date sont distincts.

## Sources API

Le contrat et les conventions viennent de `Page_fonds-repo` (`app.py` et `swagger`).
Ce projet n'importe pas le dashboard et ne depend pas de son serveur.

- `portfolios/{europe,usa,asie}/summary` : positions `open_trades`.
- `fonds/composition` : positions `holdings` d'Europa One.
- `instrument/quotes` : cours `close` par date et codezb, utilises pour calculer la variation.
- `instrument/{codezb}` : resume, `instrument.quotes.last` et `last_date` pour le dernier cours.
- `instrument/news-screener` : filtre `fIC`, bornes `fDf`/`fDt`, identifiant `news_id`.
- `instrument/{codezb}/events` : calendrier de tous les types d'evenements par defaut.
- `instrument/{codezb}/events/types` : decouverte des identifiants de types.

Le calendrier est pagine. Une fenetre news tronquee est subdivisee dans le temps ;
si meme une seconde est saturee, le controle echoue explicitement sans avancer
le curseur. Deux workers par defaut interrogent news/calendriers ; DuckDB reste
utilise exclusivement par le thread principal.

### Cache quotidien et limitation de la charge API

Les compositions des quatre portefeuilles sont chargees au premier passage de
la journee de surveillance (bascule a 01:00 Paris), puis reutilisees depuis DuckDB.
Les clotures de reference sont egalement stockees pour cette journee et par titre.
Les caches survivent aux redemarrages. Une position ajoutee/retiree en cours de
journee ne sera prise en compte qu'au prochain chargement quotidien. Modifier
la liste des portefeuilles dans la configuration invalide le cache de composition.

Une composition partiellement chargee n'est jamais validee : la precedente reste
stockee, mais le passage echoue explicitement plutot que de surveiller une
composition ancienne sans le signaler. Le prochain passage retente le chargement.
Pour les clotures, seuls les titres manquants ou en echec sont redemandes.
Une fois validee, la cloture de reference est figee pour la journee, meme si
l'API publie ensuite une correction. Le cache des references ne garde que
la journee courante.

Les resumes des instruments (`last`/`last_date`) et les news restent interroges a
chaque passage ; l'agenda reste quotidien. Deux parametres limitent la charge :

```json
"api_workers": 2,
"api_min_interval_seconds": 0.5
```

Il y a au plus deux requetes simultanees et au moins 500 ms entre les debuts
d'appels, tous workers et routes confondus dans un passage, y compris les reprises
apres erreur. Pour un mode entierement sequentiel, utiliser `api_workers: 1`.
Les valeurs autorisees sont 1 a 8 workers et un intervalle strictement positif.
Le ralentissement est volontaire : si le passage depasse une minute, le verrou
empeche le suivant de se superposer. Cela reduit la charge, sans garantir
la disparition des erreurs 502 cote serveur.

### Essai des alertes cours et diagnostic de la date de cotation

Les alertes cours sont **activees pour l'essai** (`"price_alerts_enabled": true`,
egalement si l'option est absente), avec verification de la date du dernier cours.
Pour les suspendre, utiliser `false` : les news et l'agenda restent actifs,
et les alertes cours encore `pending` ne sont ni affichees ni envoyees.

La route `instrument/quotes` fournit les cours OHLCV classes par date et codezb.
Le job demande 30 jours de cours sans ajustement des dividendes, puis lit `last`
et `last_date` via le resume `instrument/{codezb}`. Il calcule :
`100 * (last - cloture precedente) / cloture precedente`.
La cloture precedente est la derniere date cotee strictement anterieure a aujourd'hui, et non
forcement la veille calendaire (week-ends et jours feries).
L'intervalle `3600` reprend celui de `Page_fonds-repo` ; `86400`, indique par
le Swagger, est refuse par l'API reelle. Si plusieurs bougies sont renvoyees
pour un jour, la derniere de ce jour est retenue.
La requete historique se termine a aujourd'hui (borne API observee exclusive) ;
une eventuelle bougie datee d'aujourd'hui est explicitement exclue de la reference.
Les resumes respectent les memes limites de parallelisme et d'espacement.
Un echec HTTP d'un resume apres les trois tentatives est signale sur une seule
ligne, sans traceback. Seul le titre concerne est ecarte pour ce passage ;
les autres continuent, et le titre sera retente au prochain lancement.
Cet echec reste une erreur de collecte (code de sortie 1), pas un succes silencieux.

Apres connexion, examiner une reponse reelle :

```powershell
.\.venv\Scripts\python.exe alerting.py --config config.json --quote-info 4941
```

Cette commande affiche l'heure du diagnostic, le codezb, le dernier cours et
sa date, le cours et la date de reference, la variation calculee et `is_today`.
Elle ne modifie aucun etat, ne declenche
aucune alerte et ne requiert pas de SMTP. Comparer les reponses en seance et hors
seance, sur les places europeennes, americaines et asiatiques.

Avant de creer une alerte pour un seuil non encore signale, le job lit
`last_date` dans le resume de l'instrument. Sa date calendaire est comparee a la
**date de surveillance Europe/Paris (bascule a 01:00)**, sans conversion du fuseau de la cotation. Si les dates
different (veille ou date future), aucune alerte cours n'est creee et les seuils
restent disponibles. Si la date est absente/invalide ou la requete echoue,
l'alerte est egalement supprimee pour ce passage, avec une erreur explicite.
Les news et l'agenda continuent. Une cotation devenue datee du jour pourra
declencher l'alerte au passage suivant.
Une simple difference de date est ignoree silencieusement : aucun message
`skipped`, notamment lorsque les marches sont fermes. Aucun calendrier de
seances n'est suppose ; les erreurs de donnees et d'API restent journalisees.

Une variation absente ou non finie est signalee comme une erreur, sans remplacement
par un cours plus ancien. Les cours absents, invalides, non finis, nuls ou negatifs
et l'absence de seance de reference sont signales explicitement, sans alerte
pour le titre concerne. Le screener F207 n'est plus utilise.
La date peut etre une chaine ISO ou un objet Symfony
`{"date": "...", "timezone_type": 3, "timezone": "Europe/Paris"}` :
dans ce second cas, c'est le champ `date` qui est lu sans conversion de fuseau.

**Limite de la source** : la fraicheur de `last` depend du resume fourni par ZB.
Un resume reste date de la veille ne declenche aucune alerte, meme si le seuil
est depasse. Les deux routes sont interrogees separement.
La regle porte sur la date, pas sur l'age intrajournalier du cours ou l'ouverture
de la place.

Les dates de news sans fuseau explicite sont interpretees en Europe/Paris.
Ce comportement doit etre confirme sur des reponses reelles avant mise en production.
Les mails news contiennent le titre, la date et l'identifiant API, pas le texte
integral de l'article.

## Configuration et installation

1. Creer un environnement Python et installer `requirements.txt`.
2. Copier `config.example.json` vers `config.json`, ignore par Git.
3. Conserver `"delivery": "console"` pour afficher les alertes sans SMTP.
4. Lancer la connexion interactive ci-dessous pour creer le jeton temporaire.
5. Conserver `"earnings_event_types": []` pour suivre tous les types d'evenements.

Exemple Linux, pour une installation sous `/opt/zb-alerting` :

```sh
python3 -m venv /opt/zb-alerting/.venv
/opt/zb-alerting/.venv/bin/python -m pip install -r /opt/zb-alerting/requirements.txt
cp /opt/zb-alerting/config.example.json /opt/zb-alerting/config.json
```

### Affichage sans envoi de mails

Le modele utilise `"delivery": "console"`. Lancer normalement `alerting.py`
affiche les nouvelles alertes dans le terminal (ou dans le journal du cron).
Aucun SMTP ni destinataire n'est requis. L'etat DuckDB et les exports Parquet
sont conserves : une alerte affichee passe a `displayed` et ne reapparait pas
au passage suivant, meme apres redemarrage.

Ce mode est different de `--dry-run`, qui ne conserve pas ses changements.
Une interruption pendant l'affichage laisse l'alerte `uncertain` plutot que de
la reafficher automatiquement. `--resolve ID sent` confirme alors l'affichage,
et `--resolve ID retry` autorise une nouvelle tentative au prochain passage.

Pour activer les mails plus tard, passer `"delivery"` a `"smtp"` et renseigner
le serveur, l'expediteur et les destinataires. Les alertes deja affichees ne
seront **pas envoyees retroactivement**. Les alertes `pending` encore non traitees
utiliseront le mode selectionne au prochain passage. Sans cle `delivery`, le
mode SMTP est conserve pour les anciennes configurations.

### Connexion interactive au lancement

Depuis un terminal, sous le compte qui executera le cron :

```sh
python alerting.py --config config.json --login
```

Le programme demande l'identifiant ZB, le mot de passe et le code OTP. Le mot de
passe et l'OTP sont masques ; ils ne sont ni journalises ni enregistres.
La connexion appelle `/login` et enregistre uniquement le jeton temporaire
dans `token_file`, par remplacement atomique et avec les permissions `600`
sous Linux. Le dossier parent doit etre prive et accessible au compte d'execution.
Sous Windows, proteger ce dossier avec les ACL du compte utilisateur.
Un echec de connexion conserve le jeton precedent.

La commande `--login` se termine apres la connexion, sans lancer la collecte
ni envoyer de mail. Elle peut etre utilisee avant de configurer le SMTP et
un filtre d'evenements. Ensuite, lancer normalement la collecte ou le cron.
Ne pas ajouter `--login` a la crontab : une tache planifiee ne peut pas repondre
a une demande interactive.

Le fichier du jeton doit appartenir au compte d'execution, etre un fichier regulier
(pas un lien symbolique) et ne pas etre accessible au groupe/aux autres utilisateurs
(`chmod 600`). Il contient seulement le JWT, sans prefixe `Bearer`.
Le cron le relit a chaque lancement ; remplacer le fichier **atomiquement** lors
du renouvellement. Ne jamais le mettre dans Git ou en argument de commande.

**Le JWT documente expire apres 24 h. Pour l'instant, relancer manuellement
`--login` pour le renouveler.** Son renouvellement automatique reste a organiser.
Un HTTP 401 provoque un echec explicite ; les curseurs
news ne sont pas avances et les alertes en attente restent conservees.

Optionnel : interroger les types pour un codezb reel :

```sh
/opt/zb-alerting/.venv/bin/python /opt/zb-alerting/alerting.py \
  --config /opt/zb-alerting/config.json --event-types 4941
```

Par defaut, la liste `earnings_event_types` vide (ou absente) ne filtre rien :
resultats, conferences, presentations, dividendes et tous les autres evenements
retournes par l'API sont controles, avec les rappels J-7, J-3, J-1 et J.
Les alertes portent le libelle **Agenda** et le titre de l'evenement.
Pour restreindre ulterieurement le calendrier, renseigner les identifiants
souhaites dans cette liste. Le nom historique de l'option et les cles internes
`earnings` sont conserves pour compatibilite avec les donnees existantes.

SMTP impose TLS (`starttls` ou `ssl`). Si `username` est renseigne, le mot de
passe vient de la variable d'environnement indiquee par `password_env`, jamais
du fichier JSON. Cron n'herite pas automatiquement de l'environnement du shell :
prevoir l'injection securisee de cette variable pour le compte d'execution.
L'envoi du dashboard n'a pas pu etre reutilise : aucun dispositif d'envoi
n'a ete trouve dans `Page_fonds-repo`.

Les chemins relatifs sont resolus depuis le dossier du fichier de configuration,
independamment du repertoire courant. Les donnees et exports doivent rester sur
un disque local persistant, prive, sous le meme compte utilisateur.

## Activation du cron

Verifier d'abord avec :

```sh
/opt/zb-alerting/.venv/bin/python /opt/zb-alerting/alerting.py \
  --config /opt/zb-alerting/config.json --dry-run
```

Ce mode utilise une copie temporaire de l'etat, affiche les mails en attente,
n'envoie rien et ne change ni les curseurs ni la date de mise en service.
Sans base existante, il demarre a l'instant du test : il n'affichera donc pas
les news precedentes. Un checkpoint peut etre effectue avant la copie d'une
base existante. Les sorties contiennent des positions : les conserver privees.

Apres validation de la configuration et des reponses API, installer cette entree
dans la crontab du compte dedie (adapter les chemins et preparer le journal) :

```cron
* * * * * /opt/zb-alerting/.venv/bin/python /opt/zb-alerting/alerting.py --config /opt/zb-alerting/config.json >> /var/log/zb-alerting/cron.log 2>&1
```

Un verrou local empeche tout chevauchement, en collecte, export et resolution
manuelle. Un lancement est ignore si le precedent tourne encore. La frequence
effective depend donc de la latence et des quotas API/SMTP : mesurer la duree
en production, en particulier lors du controle de 01:00, avant de considerer
la surveillance a la minute comme acquise.

Le code de sortie vaut 0 en cas de succes ou de verrou deja pris, 1 en cas
d'erreur ou d'envoi incertain. Superviser ce code, les erreurs, les lancements
ignores et le renouvellement du JWT **independamment de ce SMTP**. Prevoir la
rotation des journaux. Le job ne modifie pas la crontab lui-meme.

## DuckDB, Parquet et envois

`data/state.duckdb` est la source de verite transactionnelle :

| Table | Contenu |
|---|---|
| `metadata` | Mise en service, controles quotidiens et validite du cache de composition |
| `holdings` | Derniere composition complete, nom et portefeuilles par codezb |
| `quote_references` | Cloture de reference et sa date, par journee surveillee et codezb |
| `news_cursors` | Derniere collecte news reussie par titre |
| `alert_keys` | Cles permanentes de deduplication liees a la file d'envoi |
| `outbox` | Objet, texte, identifiant stable et etat de chaque mail |

Les cles et le mail sont reserves dans la meme transaction, avant l'envoi.
Une erreur de connexion/authentification SMTP avant soumission laisse le mail
`pending`, pour une tentative au prochain passage. Une acceptation confirme
`sent`. Un refus partiel, une erreur pendant la soumission ou une interruption
entre soumission et confirmation locale laisse le mail `uncertain`.
Un ancien etat `sending` est converti en `uncertain` au passage suivant.

**SMTP ne permet pas de garantir simultanement zero perte et zero doublon**
en cas de rupture apres acceptation distante. Ces cas ne sont donc pas
reexpedies automatiquement. Le `Message-ID` est stable, mais sa deduplication
par les serveurs destinataires n'est pas garantie.

Lister puis resoudre apres verification des journaux SMTP :

```sh
python alerting.py --config config.json --outbox
python alerting.py --config config.json --resolve IDENTIFIANT sent
python alerting.py --config config.json --resolve IDENTIFIANT retry
```

`sent` confirme manuellement la reception ; `retry` remet le meme mail en attente
pour le prochain cron, avec risque de doublon explicitement accepte.

Apres chaque execution normale, les six tables sont exportees avec compression
ZSTD sous `data/parquet/*.parquet`, par remplacement atomique de chaque fichier.
Ce sont des **instantanes courants pour analyse**, pas un historique versionne
ni un remplacement de la base transactionnelle. Les exports peuvent etre
partiels si le processus s'arrete entre deux fichiers. Aucun secret API/SMTP
n'est conserve dans DuckDB ou Parquet.

Exemple d'analyse depuis Python :

```python
import duckdb

print(duckdb.sql("""
    SELECT kind, status, count(*) AS mails
    FROM read_parquet('data/parquet/outbox.parquet')
    GROUP BY kind, status
""").fetchall())
```

Sauvegarder la base via une procedure DuckDB coherente, ou avec le cron arrete
et toutes les connexions fermees. Ne pas copier une base ouverte sans son WAL.
Ne pas supprimer `alert_keys`, la base ou les curseurs pour corriger un incident :
cela perdrait la deduplication. Restaurer une sauvegarde ancienne peut reenvoyer
des mails deja acceptes apres cette sauvegarde.

## Verification locale

```sh
python -m unittest -q
```

Les tests utilisent une API et un SMTP simules, de vraies bases DuckDB temporaires
et de vrais exports Parquet. Ils ne contactent pas les services de production.
