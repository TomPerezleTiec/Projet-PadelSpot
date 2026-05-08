# Rapport d'alignement Data Engineering

## Introduction

Le projet **PadelSpot** a d'abord ete construit autour d'un notebook unique, [padelspot.ipynb](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\padelspot.ipynb), contenant la logique de traitement, les transformations et les exports finaux. Cette approche etait adaptee a une phase exploratoire, mais elle restait insuffisante pour repondre aux attentes d'un projet de **data engineering** tel qu'il est presente dans le document de cours `BIGDATA_DATA_ENGINEERING.pdf`.

L'objectif du travail realise a donc ete de faire evoluer ce notebook vers un projet **structure, orchestrable, reproductible et reusable**, tout en conservant la logique metier deja developpee. La transformation ne consistait pas a reecrire completement le projet, mais a lui donner une architecture et des outils permettant une execution plus rigoureuse.

## Transformation du projet

La transformation principale a consiste a faire passer le projet d'une logique de **notebook monolithique** a une logique de **pipeline decoupe en etapes**.

Le notebook reste la source historique de la logique metier, mais il n'est plus l'unique mode d'execution. Le projet contient maintenant des **scripts Python de jobs** dans `src/padelspot/jobs/`, correspondant aux grandes etapes du traitement :

- donnees DVF ;
- donnees Filosofi ;
- concurrence ;
- accessibilite ;
- Google Trends ;
- score ;
- preparation des exports `dash_ready`.

Cette evolution s'accompagne d'une structure de projet plus lisible, avec notamment :

- [pyproject.toml](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\pyproject.toml) ;
- [conf/base/](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\conf\base) ;
- [src/padelspot/pipeline_registry.py](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\src\padelspot\pipeline_registry.py) ;
- `src/padelspot/pipelines/stage_*` ;
- `src/padelspot/jobs/` ;
- [dvc.yaml](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\dvc.yaml) ;
- `scaffolding/copier-template/`.

Le projet n'est donc plus seulement une analyse exploratoire dans un notebook. Il est devenu une **chaine de traitement de donnees explicite**, organisee autour d'etapes nommees, de sorties identifiees et d'un cadre d'execution stable.

## Choix des outils et justification

### Kedro

Le premier outil central retenu est **Kedro**. Son role dans le projet est de fournir une **structure de projet standardisee** et une **orchestration explicite des pipelines**.

Dans PadelSpot, Kedro apporte :

- une arborescence de projet plus claire ;
- un registre de pipelines via `pipeline_registry.py` ;
- des pipelines nommes par etape (`stage_01_dvf`, `stage_02_filosofi`, etc.) ;
- une gestion centralisee de la configuration sous `conf/base/` ;
- un point d'entree standard pour l'execution.

Ce choix repond directement aux attentes du PDF sur la structuration et l'orchestration. Kedro n'est pas ici decoratif : il est effectivement utilise pour lancer les pipelines et encadrer l'execution des jobs.

### DVC

Le second outil central est **DVC**. Son role est de formaliser les **stages** du pipeline, leurs dependances, leurs outputs et leur reproductibilite.

Dans PadelSpot, DVC apporte :

- une declaration explicite des etapes dans [dvc.yaml](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\dvc.yaml) ;
- un **DAG** de pipeline, c'est-a-dire un graphe des dependances entre les differentes etapes ;
- la possibilite de verifier ce DAG avec `dvc repro --dry` ;
- la possibilite de rejouer une etape ou la pipeline ;
- la capacite a detecter qu'aucune etape ne doit etre relancee lorsque rien n'a change.

Ce point est important : DVC ne sert pas seulement a lancer des commandes. Il sert a **raisonner sur l'etat du pipeline**. Lorsque la commande `dvc repro` indique que certaines etapes sont "skipped", cela signifie que DVC a verifie les dependances et les sorties, puis a conclu qu'aucun recalcul n'etait necessaire.

### Docker + Spark

Le projet s'appuie egalement sur **Docker** et **Spark**, qui assurent un environnement d'execution stable et adapte a un volume de donnees important.

Leur apport est double :

- **Docker** fige l'environnement d'execution et limite les ecarts entre postes ou sessions ;
- **Spark** permet de traiter les donnees du projet dans un cadre coherent avec la volumetrie et les transformations realisees.

Dans PadelSpot, les jobs executes par Kedro s'appuient sur cet environnement conteneurise. Cela renforce la reproductibilite du projet et correspond a l'esprit du data engineering tel qu'il est decrit dans le PDF.

### Copier

Le PDF insiste egalement sur les notions de **template**, de **scaffolding** et de standardisation des projets. Pour couvrir cette attente, le projet inclut un **template reutilisable** base sur **Copier**, dans `scaffolding/copier-template/`.

Ce template permet de generer un nouveau projet avec :

- une structure `pyproject.toml` ;
- une base `conf/base/` ;
- une structure Kedro ;
- un `dvc.yaml` ;
- un `docker-compose.yml` ;
- des jobs et pipelines initiaux.

Autrement dit, le projet ne se contente pas d'etre bien structure pour lui-meme : il propose aussi un **modele reutilisable** pour demarrer un autre projet de data engineering sur les memes bases.

## Ce qui a ete concretement mis en place

Plusieurs elements concrets ont ete ajoutes ou structures pour faire evoluer le projet :

1. **Un scaffold Kedro**
   - `pyproject.toml`
   - `conf/base/catalog.yml`
   - `conf/base/parameters.yml`
   - `src/padelspot/settings.py`
   - `src/padelspot/pipeline_registry.py`
   - `src/padelspot/pipelines/stage_*`
   - `src/padelspot/kedro_nodes.py`

2. **Une declaration DVC du pipeline**
   - un stage par grande etape dans `dvc.yaml`
   - une logique de dependances, outputs et reproduction

3. **Des scripts d'etape executables**
   - generation et validation des jobs dans `src/padelspot/jobs/`
   - separation des traitements par phase de pipeline

4. **Un template reutilisable**
   - `scaffolding/copier-template/`
   - documentation associee dans [docs/project_template.md](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\docs\project_template.md)

5. **Une documentation d'alignement**
   - [docs/data_engineering_alignment.md](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\docs\data_engineering_alignment.md)
   - [README.md](C:\Users\tompe\OneDrive\Documenti\HELMo\BIG%20DATA\Projet-PadelSpot\README.md)

6. **Une execution validee du pipeline**
   - lecture du DAG par DVC
   - execution reelle du stage final
   - verification de la reproductibilite
   - presence des artefacts finaux dans `data/dash_ready`

Ces ajouts montrent que le travail ne s'est pas limite a une reorganisation cosmetique. Il y a eu une **mise en place effective d'outils et de mecanismes de pipeline**.

## Validation par la demonstration

La demonstration realisee permet de valider plusieurs aspects essentiels du projet.

### `kedro info`

Cette commande prouve que Kedro est bien installe et reconnu dans le projet. Elle montre que le projet utilise un **vrai framework de pipeline**, et non une imitation de structure.

### `dvc version`

Cette commande prouve que DVC fait partie de l'environnement d'execution et du dispositif du projet. Elle confirme que le pipeline est gere avec un outil dedie au suivi des stages et des artefacts.

### `copier copy`

Cette commande prouve que le template n'est pas seulement present dans le repo, mais qu'il est **reellement capable de generer un nouveau projet**. C'est la validation directe de la partie **template / scaffolding** du PDF.

### `dvc repro --dry`

Cette commande permet a DVC de relire le **DAG complet** du pipeline sans relancer les traitements. Elle prouve que les dependances entre etapes sont bien declarees et interpretees. Meme si certaines etapes sont "skipped", cela signifie que DVC a verifie leur etat avant de conclure qu'aucune relance n'etait necessaire.

### Execution forcee du stage final

Dans la demonstration, le stage final `stage_07_dash_ready` est relance avec forçage. Ce choix est **pedagogique** : sans forçage, DVC aurait pu constater que tout etait deja a jour et ne rien recalculer.

Le forçage permet donc de montrer une **execution reelle** du stage final :

- relance du traitement ;
- regeneration des artefacts `dash_ready` ;
- ecriture des tables finales ;
- completion du pipeline du stage.

Il ne s'agit pas d'un contournement du fonctionnement de DVC, mais d'un moyen de rendre visible l'execution lors de la demonstration.

### Seconde execution avec skip

Apres cette relance forcee, une seconde execution sans forçage permet de montrer le comportement normal de DVC : les etapes sont **skippees** parce que rien n'a change.

Cette seconde execution constitue une preuve tres forte de **reproductibilite**. Le pipeline est capable de se relancer sans recalcul inutile, ce qui correspond exactement a l'un des objectifs du data engineering.

### Presence des artefacts finaux

Enfin, la demonstration montre la presence d'artefacts concrets dans `data/dash_ready`, notamment :

- `dash_carreaux_full`
- `dash_communes_agg`
- `dash_clubs.parquet`
- `dash_top_zones.parquet`
- `dash_departements_stats.parquet`
- `dash_metadata.json`

Ces sorties prouvent que le pipeline ne se limite pas a des commandes abstraites : il produit effectivement des artefacts exploitables pour la phase finale du projet.

## Limites et honnetete du dispositif

Il est important de presenter aussi les limites du travail realise.

Premiere limite : le notebook initial reste encore la source historique d'une partie importante de la logique metier. Le projet est donc plus structure qu'au depart, mais il n'est pas encore totalement emancipe du notebook.

Deuxieme limite : l'industrialisation reste **progressive**. Le projet dispose des bons outils et d'une architecture plus robuste, mais il pourrait encore evoluer vers :

- des modules Python davantage maintenus a la main ;
- une reduction de la dependance au notebook ;
- une utilisation plus poussee des abstractions Kedro ;
- une gestion plus large du partage d'artefacts si un besoin collaboratif apparait.

Troisieme limite : il s'agit d'un projet de **pipeline de donnees**, pas d'une plateforme data complete. Le projet repond donc aux attentes essentielles de structuration, d'orchestration et de reproductibilite, mais sans pretendre couvrir tous les cas d'usage industriels possibles.

Ces limites n'annulent pas la valeur du travail. Elles permettent au contraire d'en presenter une lecture honnete : le projet a bien franchi un cap data engineering, tout en restant perfectible.

## Conclusion

Le travail realise sur PadelSpot repond de maniere credible et argumentee aux attentes du PDF pour la partie data engineering pertinente au projet.

Le projet est passe :

- d'un notebook unique a une organisation en **etapes explicites** ;
- d'une logique exploratoire a une **structure de projet claire** ;
- d'une execution peu formalisee a une **orchestration par Kedro** ;
- d'une relance manuelle implicite a une **reproductibilite geree par DVC** ;
- d'un projet isole a un **template reutilisable** via Copier.

En consequence, PadelSpot peut desormais etre presente comme un projet :

- **structure** ;
- **orchestre** ;
- **reproductible** ;
- **templatisable**.

Il ne s'agit plus simplement d'un notebook de traitement de donnees, mais d'une base de projet qui s'inscrit clairement dans une demarche de **data engineering**.
