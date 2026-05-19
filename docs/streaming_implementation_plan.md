# Plan d'implementation de la partie streaming

## Objectif

Ajouter une extension streaming au projet PadelSpot sans remplacer le pipeline batch existant.

Le pipeline batch reste responsable du socle territorial lourd :

- DVF ;
- Filosofi ;
- SIRENE / RES ;
- OpenStreetMap ;
- Google Trends ;
- score final ;
- exports `dash_ready`.

La partie streaming doit se concentrer sur une donnee metier qui peut evoluer rapidement : les evenements lies aux clubs de padel.

Exemples d'evenements :

- creation d'un club ;
- modification d'un club ;
- suppression ou fermeture d'un club ;
- changement du nombre de terrains ;
- ajout d'une implantation candidate.

L'objectif pedagogique est de montrer que le projet peut traiter un flux d'evenements en continu, puis mettre a jour des tables exploitables par la carte ou par une couche d'analyse.

## Justification du choix

Les sources principales du projet sont majoritairement batch ou peu frequentes. Elles ne justifient pas naturellement une architecture streaming complete.

Le streaming est donc ajoute comme une extension controlee :

- les donnees territoriales lourdes restent traitees en batch ;
- les evenements de clubs sont traites en streaming ;
- un simulateur permet de generer du volume pour justifier le choix technologique ;
- la demonstration peut rester controlable et reproductible.

Positionnement a conserver dans le rapport ou la demo :

> Le streaming ne remplace pas le batch. Il le complete sur une donnee metier evolutive : les clubs de padel et la concurrence locale.

## Architecture cible

Architecture retenue :

```text
scripts/send_club_event.py
scripts/generate_club_events.py
        |
        v
Kafka topic: padel_club_events
        |
        v
Spark Structured Streaming
        |
        v
data/streaming/bronze_club_events/
        |
        v
data/streaming/silver_clubs_current/
        |
        v
data/streaming/gold_clubs_by_department/
```

Cette version utilise obligatoirement Kafka.

Raison :

- cela correspond mieux a une architecture streaming classique ;
- le producteur envoie de vrais messages dans un topic ;
- Spark Structured Streaming consomme ce topic en continu ;
- la demonstration montre une chaine streaming complete : producer -> Kafka -> Spark -> tables de sortie.

## Format des evenements

Les evenements seront envoyes en JSON dans un topic Kafka nomme `padel_club_events`.

Schema cible :

```json
{
  "event_id": "evt_000001",
  "event_type": "club_created",
  "club_id": "club_001",
  "name": "Padel Arena Lille",
  "city": "Lille",
  "department": "59",
  "latitude": 50.637,
  "longitude": 3.063,
  "courts": 6,
  "source": "manual_demo",
  "event_time": "2026-05-19T10:00:00"
}
```

Valeurs attendues pour `event_type` :

- `club_created`
- `club_updated`
- `club_deleted`

Regles :

- `event_id` doit etre unique ;
- `club_id` identifie le club concerne ;
- `event_time` sert a ordonner les changements ;
- pour `club_deleted`, seuls `event_id`, `event_type`, `club_id` et `event_time` sont strictement necessaires ;
- les evenements invalides doivent etre ignores ou envoyes dans une sortie d'erreur.

## Tables de sortie

### Bronze : evenements bruts

Chemin :

```text
data/streaming/bronze_club_events/
```

Contenu :

- tous les evenements recus ;
- donnees append-only ;
- conservation de l'historique.

But :

- prouver que le flux est bien ingere ;
- garder une trace brute des evenements.

### Silver : etat courant des clubs

Chemin :

```text
data/streaming/silver_clubs_current/
```

Contenu :

- un enregistrement par club actif ;
- derniere version connue du club ;
- exclusion des clubs supprimes.

But :

- representer l'etat courant apres application des creations, modifications et suppressions.

### Gold : indicateurs agregees

Chemin :

```text
data/streaming/gold_clubs_by_department/
```

Contenu minimal :

- `department`
- `nb_active_clubs`
- `nb_total_courts`
- `last_event_time`

But :

- produire une table simple a afficher ou a reintegrer dans une couche `dash_ready`.

## Services et fichiers a ajouter

### `docker-compose.yml`

Role :

- ajouter un service Kafka ;
- ajouter si necessaire un service Zookeeper ou utiliser une image Kafka en mode KRaft ;
- exposer un port Kafka utilisable depuis le conteneur Spark ;
- permettre aux scripts producteurs d'envoyer des evenements vers le topic `padel_club_events`.

Choix recommande :

- utiliser une image Kafka simple pour la demo ;
- garder le service `jupyter` existant ;
- eviter une architecture multi-broker.

### Topic Kafka

Topic cible :

```text
padel_club_events
```

Le topic recevra les evenements :

- `club_created`
- `club_updated`
- `club_deleted`

Le topic pourra etre cree automatiquement au demarrage du producteur si l'API Kafka le permet, ou documente comme une commande de preparation.

### `src/padelspot/streaming/club_events_stream.py`

Role :

- lancer Spark Structured Streaming ;
- lire les messages JSON depuis Kafka ;
- valider le schema ;
- ecrire les evenements bruts en bronze ;
- construire une vue courante des clubs ;
- produire les agregats gold par departement.

Contraintes :

- utiliser `/home/jovyan/work` comme racine dans le conteneur ;
- garder des chemins compatibles avec le projet actuel ;
- produire des logs lisibles pour la demo ;
- accepter un mode `--once` ou `trigger(availableNow=True)` si possible pour faciliter la validation.
- configurer Spark avec le connecteur Kafka compatible avec la version de Spark du conteneur.

### `scripts/send_club_event.py`

Role :

- creer un evenement unique depuis la ligne de commande ;
- envoyer un message JSON dans le topic Kafka `padel_club_events`.

Commandes cible :

```bash
/opt/conda/bin/python scripts/send_club_event.py create --club-id club_001 --name "Padel Arena Lille" --city Lille --department 59 --latitude 50.637 --longitude 3.063 --courts 6
/opt/conda/bin/python scripts/send_club_event.py update --club-id club_001 --courts 8
/opt/conda/bin/python scripts/send_club_event.py delete --club-id club_001
```

### `scripts/generate_club_events.py`

Role :

- generer un volume d'evenements pour justifier l'usage du streaming ;
- simuler des creations, updates et suppressions ;
- permettre une demo avec beaucoup d'evenements.
- envoyer les evenements vers Kafka avec un delai configurable.

Commande cible :

```bash
/opt/conda/bin/python scripts/generate_club_events.py --events 1000 --delay 0.1
```

### Documentation

Ajouter ou mettre a jour :

- `docs/streaming_extension.md`
- eventuellement `README.md`

La documentation devra expliquer :

- pourquoi le projet reste batch sur les sources lourdes ;
- pourquoi le streaming est ajoute sur les clubs ;
- comment lancer la demo ;
- ce que prouvent les tables bronze, silver et gold.

## Etapes d'implementation

### Etape 1 : creer l'arborescence streaming

Creer ou verifier les dossiers suivants :

```text
src/padelspot/streaming/
data/streaming/bronze_club_events/
data/streaming/silver_clubs_current/
data/streaming/gold_clubs_by_department/
data/streaming/checkpoints/
```

Ajouter un `__init__.py` dans `src/padelspot/streaming/`.

### Etape 2 : implementer le producteur manuel

Creer `scripts/send_club_event.py`.

Fonctions attendues :

- parser les commandes `create`, `update`, `delete` ;
- generer un `event_id` unique ;
- ajouter `event_time` automatiquement si absent ;
- serialiser l'evenement en JSON ;
- envoyer le message dans Kafka ;
- afficher le topic et le contenu envoye.

### Etape 3 : implementer le generateur de volume

Creer `scripts/generate_club_events.py`.

Fonctions attendues :

- generer N evenements ;
- varier les departements, villes, coordonnees et nombres de terrains ;
- melanger creations, updates et suppressions ;
- ajouter un delai optionnel entre les evenements ;
- envoyer les evenements vers Kafka.

### Etape 4 : implementer le job Spark Structured Streaming

Creer `src/padelspot/streaming/club_events_stream.py`.

Comportement attendu :

- lire le topic Kafka `padel_club_events` ;
- caster `value` en string ;
- parser le JSON avec un schema explicite ;
- ajouter une colonne d'ingestion si utile ;
- ecrire les evenements bruts en bronze ;
- calculer l'etat courant des clubs ;
- calculer les agregats par departement ;
- ecrire les sorties en Parquet ;
- utiliser des checkpoints separes.

Point technique :

- Spark Structured Streaming gere bien l'append en bronze ;
- pour silver/gold, privilegier une logique `foreachBatch` afin de recalculer proprement l'etat courant a chaque micro-batch.
- le connecteur Spark Kafka devra etre disponible dans le conteneur Spark.

### Etape 5 : ajouter une commande de demo

Ajouter un script simple, par exemple :

```text
demo_streaming_club_events.sh
```

Objectif :

- nettoyer les dossiers de demonstration si necessaire ;
- verifier que Kafka est demarre ;
- lancer une execution streaming en mode controle ;
- generer quelques evenements ;
- afficher les sorties bronze/silver/gold.

La demo en deux terminaux reste recommandee pour montrer le comportement en continu.

### Etape 6 : integrer avec le projet existant

Integration minimale :

- garder les sorties streaming dans `data/streaming/` ;
- ne pas modifier le pipeline batch principal au debut ;
- expliquer que ces sorties peuvent alimenter la carte ou enrichir `dash_ready`.

Integration plus forte, si temps disponible :

- ajouter une lecture optionnelle de `gold_clubs_by_department` dans le stage `dash_ready` ;
- afficher les indicateurs streaming dans `dash_metadata.json` ;
- ajouter un champ comme `nb_active_clubs_streaming` ou `last_streaming_event_time`.

### Etape 7 : tests et validation

Tests minimaux :

- creer un evenement `club_created` ;
- verifier qu'il apparait en bronze ;
- verifier qu'il apparait en silver ;
- verifier que le departement est mis a jour en gold ;
- envoyer un `club_updated` ;
- verifier que le nombre de terrains change ;
- envoyer un `club_deleted` ;
- verifier que le club n'est plus actif en silver ;
- verifier que les agregats gold changent.

Validation demo :

- montrer que le flux attend de nouveaux evenements ;
- ajouter un club manuellement ;
- generer un volume d'evenements ;
- afficher les tables de sortie ;
- expliquer que le streaming complete le batch.

## Demo cible

### Terminal 1

Lancer le streaming :

```bash
/opt/conda/bin/python src/padelspot/streaming/club_events_stream.py
```

### Terminal 2

Ajouter un evenement manuel :

```bash
/opt/conda/bin/python scripts/send_club_event.py create --club-id club_001 --name "Padel Arena Lille" --city Lille --department 59 --latitude 50.637 --longitude 3.063 --courts 6
```

Modifier le club :

```bash
/opt/conda/bin/python scripts/send_club_event.py update --club-id club_001 --courts 8
```

Supprimer le club :

```bash
/opt/conda/bin/python scripts/send_club_event.py delete --club-id club_001
```

Generer du volume :

```bash
/opt/conda/bin/python scripts/generate_club_events.py --events 1000 --delay 0.1
```

## Message a defendre

Le message principal a conserver :

> Les sources territoriales du projet sont majoritairement batch. Le streaming est donc ajoute sur la partie la plus evolutive du domaine : les clubs de padel. Les evenements sont envoyes dans Kafka, consommes par Spark Structured Streaming, puis transformes en indicateurs de concurrence locale.

Formulation courte :

> Batch pour le socle territorial, streaming pour les changements rapides de concurrence.

## Points d'attention

- Ne pas presenter le streaming comme necessaire pour toutes les sources du projet.
- Assumer que le volume reel est faible aujourd'hui.
- Justifier le streaming par la simulation controlee de volume.
- Garder le lien metier avec la concurrence locale.
- Kafka est obligatoire dans cette implementation.
- Prioriser une demo fiable avec un seul topic Kafka et un seul consumer Spark.
- Eviter de complexifier avec plusieurs brokers ou une architecture Kafka avancee.
