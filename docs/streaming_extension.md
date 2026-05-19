# Extension streaming Kafka

## Principe

Le projet PadelSpot reste batch pour les sources territoriales lourdes : DVF, Filosofi, SIRENE, OSM et Trends. Ces donnees changent peu souvent et ne justifient pas toutes seules une architecture streaming.

La partie streaming est ajoutee sur les evenements de clubs de padel, car ce sont les donnees metier les plus evolutives :

- creation d'un club ;
- modification du nombre de terrains ;
- suppression ou fermeture d'un club ;
- ajout d'une implantation candidate.

## Architecture

```text
Producteurs Python
        |
        v
Kafka topic padel_club_events
        |
        v
Spark Structured Streaming
        |
        v
Bronze: data/streaming/bronze_club_events
Silver: data/streaming/silver_clubs_current
Gold: data/streaming/gold_clubs_by_department
```

## Lancer l'environnement

```bash
docker compose up -d kafka jupyter
```

Dans le conteneur `jupyter` :

```bash
cd /home/jovyan/work
/opt/conda/bin/python -m pip install -e .
```

## Envoyer des evenements

```bash
/opt/conda/bin/python scripts/send_club_event.py create --club-id club_001 --name "Padel Arena Lille" --city Lille --department 59 --latitude 50.637 --longitude 3.063 --courts 6
/opt/conda/bin/python scripts/send_club_event.py update --club-id club_001 --courts 8
/opt/conda/bin/python scripts/send_club_event.py delete --club-id club_001
```

## Generer du volume

```bash
/opt/conda/bin/python scripts/generate_club_events.py --events 1000 --delay 0.1
```

## Consommer le flux

Execution controlee, utile pour une demo :

```bash
/opt/conda/bin/python src/padelspot/streaming/club_events_stream.py --once
```

Execution continue :

```bash
/opt/conda/bin/python src/padelspot/streaming/club_events_stream.py
```

## Demo complete

```bash
bash demo_streaming_club_events.sh
```

## Interface Dash temps reel

L'interface web affiche les clubs historiques et les clubs issus du flux Kafka sur une carte Plotly. Elle se rafraichit automatiquement et contient un bouton pour envoyer des evenements aleatoires dans Kafka.

Lancer l'application dans le conteneur `jupyter` :

```bash
cd /home/jovyan/work
/opt/conda/bin/python -m pip install -e .
/opt/conda/bin/python -m padelspot.apps.streaming_dash_app
```

Depuis la machine hote, ouvrir :

```text
http://localhost:8050
```

Dans l'interface, le bouton `Lancer des events aleatoires` demarre le consumer Spark Streaming si necessaire, envoie des evenements Kafka, puis la carte se met a jour via les tables `data/streaming/`.

Les evenements aleatoires sont distribues sur plusieurs villes afin de montrer un flux plus realiste et moins artificiel. Ils peuvent :

- creer un nouveau club ;
- modifier le nom d'un club ;
- modifier son nombre de terrains ;
- deplacer legerement un club ;
- relocaliser un club dans une autre ville ;
- supprimer un club.

La carte recalcule ensuite un score unique : un club Kafka actif applique une pression concurrentielle sur les clubs situes dans un rayon local. Les clubs proches changent donc de score et de couleur, tandis qu'un club trop eloigne, par exemple a Paris lorsqu'un evenement arrive a Marseille, n'est pas affecte.

## Message a defendre

Le streaming ne remplace pas le pipeline batch existant. Il le complete sur une couche evenementielle controlee :

> Batch pour construire le socle territorial, Kafka + Spark Structured Streaming pour traiter les changements rapides de concurrence locale.
