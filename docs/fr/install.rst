Installation du logiciel E-Maj
==============================

Source de téléchargement
************************

E-Maj est disponible en téléchargement sur le site Internet **PGXN** (https://pgxn.org/dist/e-maj/).

E-Maj et ses compléments sont également disponibles sur le site Internet **github.org** :

* Composants sources (https://github.com/beaud76/emaj)
* Documentation (https://github.com/beaud76/emaj_doc)
* Plugin pour phpPgAdmin (https://github.com/beaud76/emaj_ppa_plugin)
* Interface graphique Emaj_web (https://github.com/beaud76/emaj_web)


Installation sur Linux
**********************

Télécharger la dernière version d’E-Maj par un moyen à votre convenance. Si le *client pgxn* est installé, on peut simplement exécuter la commande ::

	pgxn download E-Maj

Puis décompresser l’archive avec les commandes suivantes ::

	unzip e-maj-<version>.zip

	cd e-maj-<version>/

Identifier la localisation précise du répertoire *SHAREDIR*. Selon l’installation de PostgreSQL, la commande *pg_config --sharedir* peut retourner directement le nom du répertoire. Sinon, rechercher les localisations typiques, telles que :

* */usr/share/postgresql/<pg_version>* pour Debian ou Ubuntu
* */usr/pgsql-<pg_version>/share* pour RedHat ou CentOS

Copier quelques fichiers vers le répertoire des extensions de la version de PostgreSQL souhaitée. En tant que super-utilisateur ou en préfixant les commandes avec sudo, taper ::

	cp emaj.control <répertoire_SHAREDIR>/extension/.

	cp sql/emaj--* <répertoire_SHAREDIR>/extension/.

La dernière version d’E-Maj est maintenant installée et référencée par PostgreSQL. Le répertoire e-maj-<version> contient l’arborescence :doc:`décrite ici <content>`.


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

