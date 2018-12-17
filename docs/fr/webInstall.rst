Installation du client Emaj_web
===============================

Pré-requis
----------

*Emaj_web* nécessite un serveur web avec un interpréteur php.

Téléchargement du plug-in
-------------------------

L’application *Emaj_web* peut être téléchargée depuis le dépôt git suivant :

https://github.com/dalibo/emaj_web

Configuration de l’application
------------------------------

Deux fichiers de configuration sont à renseigner.

Paramétrage générale
^^^^^^^^^^^^^^^^^^^^

Le fichier *emaj_web/conf/config.inc.php* contient les paramètres généraux de l’application, incluant notamment la description des connexions aux instances PostgreSQL.

Le fichier *emaj_web/conf/config.inc.php-dist* peut servir de base pour la configuration.

Paramétrage du plug-in
^^^^^^^^^^^^^^^^^^^^^^

Comme *Emaj_web* utilise lui même le plugin pour *phpPgAdmin*, les paramètres spécifiques à ce plugin sont décrits dans un fichier de configuration séparé :  *emaj_web/plugins/Emaj/conf/config.inc.php*

Pour pouvoir soumettre des rollbacks en tâche de fonds (c'est à dire sans mobiliser le navigateur durant le déroulement des rollbacks), il est nécessaire de valoriser deux paramètres de configuration contenus dans le fichier *Emaj/conf/config.inc.php* :

* *$plugin_conf['psql_path']* définit le chemin de l'exécutable *psql*,
* *$plugin_conf['temp_dir']* définit un répertoire temporaire utilisable lors des rollbacks en tâche de fonds. 

Le fichier *config.inc.php-dist* fourni peut-être utilisé comme modèle de fichier de configuration.

