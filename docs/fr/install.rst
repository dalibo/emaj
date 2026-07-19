Installer le logiciel E-Maj
===========================

Sources de téléchargement
-------------------------

E-Maj est disponible en téléchargement depuis les sources suivantes : 

- **PGXN (PostgreSQL Extension Network)**: `https://pgxn.org/dist/e-maj/ <https://pgxn.org/dist/e-maj/>`_
- **Dépôts GitHub** :

  - Code source : `https://github.com/dalibo/emaj <https://github.com/dalibo/emaj>`_
  - Documentation : `https://github.com/beaud76/emaj_doc <https://github.com/beaud76/emaj_doc>`_
  - Interface graphique Emaj_web: `https://github.com/dalibo/emaj_web <https://github.com/dalibo/emaj_web>`_

.. caution::
   Installer l’extension à partir du dépôt **GitHub** crée l’extension en **version de développement** ("devel"), même en téléchargeant une version 'taguée'. Il sera alors impossible de procéder à des mises à jour de l'extension dans le futur. Pour une utilisation dans la durée, il est **fortement recommandé** d’utiliser les paquets versionnés disponibles sur **PGXN**.

----

Installation standard sur Linux
-------------------------------

Avec le client pgxn
^^^^^^^^^^^^^^^^^^^

Si le client *pgxn* est installé, une simple commande suffit : ::

  pgxn install E-Maj --sudo

Sans le client pgxn
^^^^^^^^^^^^^^^^^^^

1. Téléchargez la dernière version d’E-Maj depuis le site *pgxn.org*, par un moyen à votre convenance, par exemple avec la commande *wget* : ::
 
     wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

2. Décompressez l’archive et installer les composants : ::

     unzip e-maj-<version>.zip
     cd e-maj-<version>/
     sudo make install

Localisation des composants
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dans les deux cas de figure, les composants sont installés dans les répertoires usuels des composants PostgreSQL :

- les **scripts SQL** : ``<SHAREDIR>/emaj/``,
- les **clients** en mode ligne de commande : ``<BINDIR>`` (avec les clients PostgreSQL),
- la **documentation** : ``<DOCDIR>/emaj/``.

Pour connaitre la localisation des répertoires *SHAREDIR*, *BINDIR* et *DOCDIR* sur le système, utiliser la commande ``pg_config``.

----

Installation manuelle sur Linux
-------------------------------

1. Télécharger la dernière version d’E-Maj depuis le site *pgxn.org*, par exemple avec la commande *wget* ::

     wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

2. Décompresser l’archive avec les commandes suivantes : ::

     unzip e-maj-<version>.zip
     cd e-maj-<version>/

3. Éditer le fichier **emaj.control** pour valoriser la directive ``directory`` avec le répertoire contenant les scripts d’installation d’E-Maj (chemin absolu du répertoire contenant *e-maj-<version>/sql*).

4. Identifier la localisation du répertoire *SHAREDIR*. Vous pouvez utiliser la commande : ::

     pg_config --sharedir

   En cas d'échec, recherchez les localisations typiques, telles que */usr/share/postgresql/<pg_version>* pour Debian ou Ubuntu ou */usr/pgsql-<pg_version>/share* pour RedHat ou CentOS.

5. Copier le fichier *emaj.control* modifié dans le répertoire des extensions de la version de PostgreSQL souhaitée : en tant que super-utilisateur ou en préfixant les commandes avec *sudo*, taper : ::

	cp emaj.control <répertoire_SHAREDIR>/extension/.

La dernière version d’E-Maj est maintenant installée et référencée par PostgreSQL. Le répertoire ``e-maj-<version>`` contient l’arborescence :doc:`décrite ici <content>`.

----

.. _minimum_install:

Installation minimale sur Linux
-------------------------------

Sur certains environnements (**cloud de type DBaaS** par exemple), il n’est pas possible d’ajouter des extensions dans le répertoire *SHAREDIR*. Pour ces cas de figure, une **installation minimale** est disponible.

1. **Télécharger** l’extension depuis le `site PGXN <https://pgxn.org/dist/e-maj/>`_,
2. **Extraire** l’arborescence du fichier zip reçu.

Le répertoire ``e-maj-<version>`` généré contient l’arborescence :doc:`décrite ici <content>`.

La :ref:`création de l’extension <create_emaj_extension_by_script>` est alors un peu différente.

----

Installation sous Windows
-------------------------

Pour installer E-Maj sous Windows, il faut :

1. **Télécharger** l’extension depuis le `site PGXN <https://pgxn.org/dist/e-maj/>`_,
2. **Extraire** l’arborescence du fichier zip reçu,
3. **Copier** dans le dossier *share* du dossier d’installation de la version de PostgreSQL (typiquement *c:\\Program_Files\\PostgreSQL\\<version_postgres>\\share*) :

   * le fichier ``emaj.control`` dans ``\extension\``,
   * les fichiers ``sql\emaj--*.sql`` dans un nouveau dossier ``\emaj\``.
