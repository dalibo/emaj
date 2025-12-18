Installer le logiciel E-Maj
===========================

Source de téléchargement
************************

E-Maj est disponible en téléchargement sur le site Internet **PGXN** (https://pgxn.org/dist/e-maj/).

E-Maj et ses compléments sont également disponibles sur le site Internet **github.org** :

* Composants sources (https://github.org/dalibo/emaj)
* Documentation (https://github.com/beaud76/emaj_doc)
* Interface graphique Emaj_web (https://github.com/dalibo/emaj_web)

.. caution::
   Installer l’extension à partir du dépôt *github.org* crée l’extension en version de développement ("devel"), même en téléchargeant une version 'taguée'. Il sera alors impossible de procéder à des mises à jour de l'extension dans le futur. Pour une utilisation dans la durée, il est fortement recommandé d’utiliser les paquets versionnés disponibles sur *PGXN*.

Installation standard sur Linux
*******************************

Avec le client pgxn
^^^^^^^^^^^^^^^^^^^

Si le client *pgxn* est installé, une simple commande suffit ::

  pgxn install E-Maj --sudo

Sans le client pgxn
^^^^^^^^^^^^^^^^^^^

Téléchargez la dernière version d’E-Maj depuis le site pgxn.org, par un moyen à votre convenance, par exemple avec la commande *wget* ::
 
  wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

Puis décompressez l’archive et installer les composants avec les commandes ::

  unzip e-maj-<version>.zip

  cd e-maj-<version>/

  sudo make install

Localisation des composants
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dans les 2 cas de figure, les composants sont installés dans les répertoires usuels des composants PostgreSQL. En particulier :

* les scripts SQL sont dans *<répertoire_SHAREDIR>/emaj/*
* les clients en mode ligne de commande sont dans *<répertoire_BINDIR>*, avec les clients PostgreSQL
* la documentation est dans *<répertoire_DOCDIR>/emaj/*

La localisation physique des répertoires *SHAREDIR*, *BINDIR* et *DOCDIR* sur le système peut être retrouvée à l’aide de la commande *pg_config*.

Installation manuelle sur Linux
-------------------------------

Téléchargez la dernière version d’E-Maj depuis le site pgxn.org, par un moyen à votre convenance, par exemple avec la commande *wget* ::

   wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

Puis décompressez l’archive avec les commandes suivantes ::

   unzip e-maj-<version>.zip

   cd e-maj-<version>/

Éditez le fichier *emaj.control* pour valoriser la directive *directory* avec le répertoire contenant les scripts d’installation d’E-Maj (chemin absolu du répertoire *e-maj-<version>/sql*).

Identifiez la localisation précise du répertoire *SHAREDIR*. Selon l’installation de PostgreSQL, la commande *pg_config --sharedir* peut retourner directement le nom du répertoire. Sinon, rechercher les localisations typiques, telles que :

* */usr/share/postgresql/<pg_version>* pour Debian ou Ubuntu
* */usr/pgsql-<pg_version>/share* pour RedHat ou CentOS

Puis copiez le fichier *emaj.control* modifié dans le répertoire des extensions de la version de PostgreSQL souhaitée : en tant que super-utilisateur ou en préfixant les commandes avec sudo, tapez : ::

	cp emaj.control <répertoire_SHAREDIR>/extension/.

La dernière version d’E-Maj est maintenant installée et référencée par PostgreSQL. Le répertoire e-maj-<version> contient l’arborescence :doc:`décrite ici <content>`.

.. _minimum_install:

Installation minimale sur Linux
*******************************

Sur certains environnements (cloud de type DBaaS par exemple), il n’est pas possible d’ajouter des extensions dans le répertoire *SHAREDIR*. Pour ces cas de figure, on peut procéder à une installation minimale.

Téléchargez la dernière version d’E-Maj par un moyen à votre convenance, et décompressez la.

Le répertoire e-maj-<version> généré contient l’arborescence :doc:`décrite ici <content>`.

La :ref:`création de l’extension <create_emaj_extension_by_script>` est alors un peu différente.

Installation sous Windows
*************************

Pour installer E-Maj sous Windows, il faut :

* Télécharger l’extension depuis le site pgxn.org,
* Extraire l’arborescence du fichier zip reçu,
* Copier dans le dossier *share* du dossier d’installation de la version de PostgreSQL (typiquement *c:\\Program_Files\\PostgreSQL\\<version_postgres>\\share*) :

   * le fichier *emaj.control* dans *\\extension*,
   * les fichiers *sql\\emaj--** dans un nouveau dossier *\\emaj*.
