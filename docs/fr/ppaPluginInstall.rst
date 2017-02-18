Installation du plug-in phpPgAdmin
==================================

Pré-requis
----------

Une version de *phpPgAdmin*, de version au moins égale à 5.1, doit être installée et opérationnelle dans un serveur web.

Téléchargement du plug-in
-------------------------

Le plug-in E-Maj pour *phpPgAdmin* peut être téléchargé depuis le dépôt git suivant : 
https://github.com/beaud76/emaj_ppa_plugin

Le répertoire *Emaj* téléchargé doit être copié dans le sous-répertoire *plugins* de la version *phpPgAdmin* utilisée.


Activation du plug-in
---------------------

Pour activer le plug-in, il suffit d'ouvrir le fichier *conf/config.inc.php* de l'arborescence de *phpPgAdmin* et d'ajouter la chaîne de caractères *'Emaj'* à la variable *$conf['plugins']*.

On peut ainsi avoir par exemple ::

   $conf['plugins'] = array('Emaj');

ou encore, si un autre plug-in est déjà activé ::

   $conf['plugins'] = array('Report','Emaj');


Paramétrage du plug-in
----------------------

Pour pouvoir soumettre des rollbacks en tâche de fonds (c'est à dire sans mobiliser le navigateur durant le déroulement des rollbacks), il est nécessaire de valoriser deux paramètres de configuration contenus dans le fichier *Emaj/conf/config.inc.php* :

* *$plugin_conf['psql_path']* définit le chemin de l'exécutable *psql*,
* *$plugin_conf['temp_dir']* définit un répertoire temporaire utilisable lors des rollbacks en tâche de fonds. 

Le fichier *config.inc.php-dist* fourni peut-être utilisé comme modèle de fichier de configuration.

