Installation du logiciel E-Maj
==============================

Source de téléchargement
************************

E-Maj est disponible en téléchargement sur le site Internet **PGXN** (https://pgxn.org/dist/e-maj/).

E-Maj et ses compléments sont également disponibles sur le site Internet **github.org** :

* Composants sources (https://github.com/dalibo/emaj)
* Documentation (https://github.com/beaud76/emaj_doc)
* Interface graphique Emaj_web (https://github.com/dalibo/emaj_web)

.. caution::
   Installer l’extension à partir du dépôt *github.org* crée l’extension en version de développement ("devel"). Il sera alors impossible de procéder à des mises à jour de l'extension dans le futur. Pour une utilisation dans la durée, il est fortement recommandé d’utiliser les paquets versionnés disponibles sur *PGXN*.

Installation standard sur Linux
*******************************

Téléchargez la dernière version d’E-Maj par un moyen à votre convenance. Si le *client pgxn* est installé, on peut simplement exécuter la commande ::

	pgxn download E-Maj

Puis décompressez l’archive avec les commandes suivantes ::

	unzip e-maj-<version>.zip

	cd e-maj-<version>/

Identifiez la localisation précise du répertoire *SHAREDIR*. Selon l’installation de PostgreSQL, la commande *pg_config --sharedir* peut retourner directement le nom du répertoire. Sinon, rechercher les localisations typiques, telles que :

* */usr/share/postgresql/<pg_version>* pour Debian ou Ubuntu
* */usr/pgsql-<pg_version>/share* pour RedHat ou CentOS

Copiez quelques fichiers vers le répertoire des extensions de la version de PostgreSQL souhaitée. En tant que super-utilisateur ou en préfixant les commandes avec sudo, taper ::

	cp emaj.control <répertoire_SHAREDIR>/extension/.

	cp sql/emaj--* <répertoire_SHAREDIR>/extension/.

La dernière version d’E-Maj est maintenant installée et référencée par PostgreSQL. Le répertoire e-maj-<version> contient l’arborescence :doc:`décrite ici <content>`.

.. _minimum_install:

Installation minimale sur Linux
*******************************

Sur certains environnements (cloud de type DBaaS par exemple), il n’est pas possible d’ajouter des extensions dans le répertoire *SHAREDIR*. Pour ces cas de figure, on peut procéder à une installation minimale.

Téléchargez la dernière version d’E-Maj par un moyen à votre convenance, et décompressez la.

Par exemple, si le client pgxn est installé, exécutez les commandes ::

	pgxn download E-Maj

	unzip e-maj-<version>.zip

Le répertoire e-maj-<version> généré contient l’arborescence :doc:`décrite ici <content>`.

La :doc:`création de l’extension <setup>` est alors un peu différente.

Installation sous Windows
*************************

Pour installer E-Maj sous Windows, il faut :

* Télécharger l’extension depuis le site pgxn.org,
* Extraire l’arborescence du fichier zip reçu,
* En copier les fichiers *emaj.control* et *sql/emaj--** dans le dossier *share\\extension* du dossier d’installation de la version de PostgreSQL (typiquement *c:\\Program_Files\\PostgreSQL\\<version_postgres>*).

Localisation alternative des scripts SQL d’installation
*******************************************************

Le fichier *emaj.control*, positionné dans le répertoire *SHAREDIR/extension* de la version de PostgreSQL, peut contenir une directive indiquant à PostgreSQL le répertoire dans lequel sont localisés les scripts SQL d’installation ou d’upgrade.

Il est donc possible de ne mettre dans ce répertoire *SHAREDIR/extension* que le seul fichier *emaj.control* en créant ce pointeur vers le répertoire de scripts. Pour ce faire, il faut :

* Copier le fichier *emaj.control* fourni dans le répertoire racine de la version décompressée vers le répertoire *SHAREDIR/extension*,
* Adapter la directive *directory* du fichier *emaj.control* pour spécifier le répertoire sql contenant les scripts d’installation d’E-Maj.

