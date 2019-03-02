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

La configuration est centralisée dans un unique fichier : *emaj_web/conf/config.inc.php*. Il contient les paramètres généraux de l’application, ainsi que la description des connexions aux instances PostgreSQL.

Quand le nombre d’instances est important, il est possible de les répartir dans des *groupes d’instances*. Un groupe peut contenir des instances ou d’autres groupes d’instances.

Pour pouvoir soumettre des rollbacks en tâche de fonds (c'est à dire sans mobiliser le navigateur durant le déroulement des rollbacks), il est nécessaire de valoriser deux paramètres de configuration :

* *$conf['psql_path']* définit le chemin de l'exécutable *psql*,
* *$conf['temp_dir']* définit un répertoire temporaire utilisable lors des rollbacks en tâche de fonds. 

Le fichier *emaj_web/conf/config.inc.php-dist* peut servir de base pour la configuration.
