Installation du logiciel E-Maj
==============================

Source de téléchargement
************************

E-Maj est disponible en téléchargement sur le site Internet **PGXN** (http://pgxn.org).

E-Maj et ses compléments sont également disponibles sur le site Internet **github.org** :

* Composants sources (https://github.com/beaud76/emaj)
* Documentation (https://github.com/beaud76/emaj_doc)
* Plugin pour phpPgAdmin (https://github.com/beaud76/emaj_ppa_plugin)
* Interface graphique Emaj_web (https://github.com/beaud76/emaj_web)


Installation sur Linux
**********************

Télécharger la dernière version d’E-Maj par un moyen à votre convenance. Si le *client pgxn* est installé, on peut simplement exécuter la commande ::

	pgxn download E-Maj

Puis décompresser l’archive et exécuter la procédure d’installation, en enchaînant les commandes suivantes ::

	unzip e-maj-<version>.zip
	cd e-maj-<version>/
	sudo make install

La dernière version d’E-Maj est maintenant installée et référencée par PostgreSQL. Le répertoire e-maj-<version> contient l’arborescence :doc:`décrite ici <content>`.

NB : La commande *sudo make uninstall* permet d’annuler la dernière commande.

NB : Il n’est pas recommandé de faire l’installation complète d’E-Maj en une seule commande *pgxn install E-Maj*. En effet, bien que cette commande installe correctement les fichiers nécessaires à la :doc:`création ultérieure de l’EXTENSION emaj<setup>` dans une base de données, elle ne permet pas d’obtenir tous les autres fichiers de la distribution (clients php, autres scripts sql, documentation,…).

Installation sous Windows
*************************

Pour installer E-Maj sous Windows, il faut :

* Télécharger l’extension depuis le site pgxn.org,
* Extraire l’arborescence du fichier zip reçu,
* En copier les fichiers *emaj.control* et *sql/emaj--** dans le dossier *share\\extension* du dossier d’installation de la version de PostgreSQL (typiquement *c:\\Program_Files\\PostgreSQL\\<version_postgres>*).

Localisation alternative des scripts SQL d’installation
*******************************************************

Le fichier *emaj.control*, positionné dans le répertoire *SHAREDIR* de la version de PostgreSQL, peut contenir une directive indiquant à PostgreSQL le répertoire dans lequel sont localisés les scripts SQL d’installation ou d’upgrade.

Il est donc possible de ne mettre dans ce répertoire *SHAREDIR* que le seul fichier *emaj.control* en créant ce pointeur vers le répertoire de scripts. Pour ce faire, il faut :

* Identifier l’emplacement précis du répertoire *SHAREDIR* de l'installation en utilisant la commande shell ::

   pg_config --sharedir

* Copier le fichier *emaj.control* fourni dans le répertoire racine de la version décompressée vers le répertoire *SHAREDIR*,
* Adapter la directive *directory* du fichier *emaj.control* pour spécifier le répertoire sql contenant les scripts d’installation d’E-Maj.

