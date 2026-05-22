# Fonctionnement du projet PadelSpot

Ce document explique simplement comment fonctionne le projet PadelSpot et comment la partie streaming a été ajoutée au pipeline batch existant.

## Objectif du projet

PadelSpot sert à analyser les zones d'implantation potentielles pour des clubs de padel en France. Le projet combine plusieurs sources de données : clubs existants, données foncières, données socio-économiques, accessibilité, Google Trends et indicateurs géographiques.

Le résultat principal est un ensemble de fichiers prêts pour la visualisation, notamment dans `data/dash_ready/`. Ces fichiers alimentent la carte interactive.

## Organisation générale

Le projet est structuré comme un projet data engineering :

- `src/padelspot/jobs/` contient les scripts de traitement par étape.
- `src/padelspot/pipelines/` contient les pipelines Kedro.
- `src/padelspot/pipeline_registry.py` déclare les pipelines disponibles.
- `conf/base/` contient la configuration du projet.
- `dvc.yaml` décrit les stages, leurs dépendances et leurs sorties.
- `data/dash_ready/` contient les données finales utilisées par l'interface.
- `src/padelspot/apps/streaming_dash_app.py` contient l'application Dash de démonstration streaming.
- `src/padelspot/streaming/club_events_stream.py` contient le traitement Spark Structured Streaming.

## Pipeline batch

La partie batch correspond au pipeline principal du projet. Elle traite les données par étapes :

1. chargement et préparation des sources,
2. harmonisation des données,
3. calculs géographiques et socio-économiques,
4. calcul du score d'implantation,
5. production des exports utilisés par la carte.

Kedro sert à organiser ces traitements en pipelines nommés. DVC sert à décrire l'ordre des stages, à suivre les dépendances et à éviter de relancer les étapes qui n'ont pas changé.

## Rôle de DVC

DVC lit le fichier `dvc.yaml` et construit un DAG, c'est-à-dire un graphe d'exécution. Chaque stage déclare :

- une commande à lancer,
- des dépendances en entrée,
- des sorties produites.

Quand `dvc repro` est lancé, DVC compare les dépendances et les sorties. Si rien n'a changé, le stage est ignoré. Ce comportement est normal : il prouve que le pipeline est reproductible et que DVC sait reconnaître un état déjà à jour.

## Ajout du streaming

Le streaming a été ajouté pour simuler des événements qui peuvent arriver après la production batch : création, modification ou suppression de clubs de padel.

Le but n'est pas de remplacer le pipeline batch. Le streaming vient au-dessus du résultat batch pour montrer comment le système pourrait réagir à des changements fréquents.

## Fonctionnement exact du streaming

Le fonctionnement est le suivant :

1. L'utilisateur clique sur le bouton de génération dans l'interface Dash.
2. L'application crée des événements aléatoires de clubs.
3. Chaque événement est envoyé dans Kafka sur le topic `padel_club_events`.
4. Un job Spark Structured Streaming lit ce topic Kafka.
5. Spark écrit les événements dans `data/streaming/bronze_club_events`.
6. Spark reconstruit l'état courant des clubs dans `data/streaming/silver_clubs_current`.
7. Spark produit aussi une agrégation par département dans `data/streaming/gold_clubs_by_department`.
8. L'application Dash recharge régulièrement les données et met à jour la carte.

Les événements ont ce format logique :

- `club_created` : ajout d'un nouveau club,
- `club_updated` : modification d'un club existant,
- `club_deleted` : suppression d'un club.

Dans la démonstration, les suppressions sont rares. Les clubs générés ont entre 2 et 10 terrains, afin de rester réalistes.

## Recalcul du score dans l'interface

La carte affiche un score unique par club. Ce score part du score batch déjà calculé, puis applique une pression concurrentielle locale.

Quand un club est créé ou modifié par le flux Kafka :

- les clubs proches sont impactés,
- les clubs éloignés ne changent pas,
- plus un club est proche, plus l'impact est fort,
- plus le nouveau club a de terrains, plus son impact est fort,
- plusieurs clubs proches peuvent cumuler leur effet.

Cette logique permet de voir visuellement les scores changer sur la carte pendant la démonstration.

## Pourquoi une couche visuelle directe existe dans Dash

Kafka et Spark sont bien utilisés pour la preuve technique du streaming. En parallèle, l'application Dash garde aussi les événements récents en mémoire afin que la carte réagisse immédiatement pendant la démo.

Ce choix évite d'attendre uniquement l'écriture Parquet de Spark pour voir un changement. Il rend la démonstration plus lisible, tout en gardant Kafka et Spark comme chaîne de streaming réelle.

## Interface Dash

L'interface permet de :

- visualiser les clubs historiques,
- lancer un flux d'événements simulés,
- voir les clubs créés par le flux Kafka,
- filtrer par source, département, type, commune et score,
- observer les scores impactés autour des nouveaux clubs.

La carte distingue les clubs historiques et les clubs issus du flux. Les couleurs représentent le score d'implantation mis à jour.

## Commandes utiles

Depuis PowerShell, à la racine du projet :

```powershell
cd "C:\Users\tompe\OneDrive\Documenti\HELMo\BIG DATA\Projet-PadelSpot"
docker compose up -d
```

Pour lancer l'interface :

```powershell
docker compose exec jupyter bash -lc "cd /home/jovyan/work && PYTHONPATH=src /opt/conda/bin/python -m padelspot.apps.streaming_dash_app"
```

Puis ouvrir :

```text
http://localhost:8050
```

Pour lancer une démonstration en ligne de commande :

```powershell
docker compose exec jupyter bash -lc "cd /home/jovyan/work && bash demo_streaming_club_events.sh"
```

## Limites

Les événements sont simulés pour la démonstration. Dans un cas réel, ils pourraient venir d'une API, d'un outil de saisie interne ou d'un système métier. Le principe resterait le même : produire des événements dans Kafka, les traiter avec Spark, puis mettre à jour les sorties utilisées par l'application.

Le score affiché dans Dash est un score de démonstration dynamique. Il sert à montrer l'effet local d'un changement en streaming. Le pipeline batch reste la source principale pour les calculs complets.
