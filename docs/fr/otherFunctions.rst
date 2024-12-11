Autres fonctions
================

.. _emaj_get_version:

Obtenir la version courante de l’extension emaj
-----------------------------------------------

La fonction *emaj_get_version()* retourne l’identifiant de la version courante de l’extension *emaj*. ::

   SELECT emaj.emaj_get_version();

.. _emaj_verify_all:

Vérifier la consistance de l'environnement E-Maj
------------------------------------------------

Une fonction permet de vérifier la consistance de l'environnement E-Maj. Cela consiste à  vérifier l'intégrité de chaque schéma d'E-Maj et de chaque groupe de tables créé. Cette fonction s'exécute par la requête SQL suivante ::

   SELECT * FROM emaj.emaj_verify_all();

Pour chaque schéma E-Maj (*emaj* et les schémas de log), la fonction vérifie :

* que toutes les tables, fonctions et séquences et tous les types soit sont des objets de l'extension elle-même, soit sont bien liés aux groupes de tables créés,
* qu'il ne contient ni vue, ni « *foreign table* », ni domaine, ni conversion, ni opérateur et ni classe d'opérateur.

Ensuite, pour chaque groupe de tables créé, la fonction procède aux mêmes contrôles que ceux effectués lors des opérations de démarrage de groupe, de pose de marque et de rollback (:ref:`plus de détails <internal_checks>`).

La fonction retourne un ensemble de lignes qui décrivent les éventuelles anomalies rencontrées. Si aucune anomalie n'est détectée, la fonction retourne une unique ligne contenant le message ::

   'No error detected'

La fonction retourne également des avertissements quand :

* une séquence associée à une colonne est assignée à un groupe de tables mais la table associée ne fait pas partie de ce groupe de tables,
* une table d’un groupe est liée à une autre table par une clé étrangère, mais la table associée ne font pas partie du même groupe de tables,
* la connexion dblink n’est pas opérationnelle,
* des event triggers de protection E-Maj sont manquants ou désactivés.

La fonction *emaj_verify_all()* peut être exécutée par les rôles membres de *emaj_adm* et *emaj_viewer* (le test de la connexion dblink n’étant pas effectué par ces derniers).

Si des anomalies sont détectées, par exemple suite à la suppression d'une table applicative référencée dans un groupe, les mesures appropriées doivent être prises. Typiquement, les éventuelles tables de log ou fonctions orphelines doivent être supprimées manuellement.

.. _export_import_param_conf:

Export et import des configurations de paramètres
-------------------------------------------------

Deux jeux de fonctions permettent de respectivement exporter et importer des jeux de paramètres. Elles peuvent être utiles pour déployer un jeu standardisé de paramètres sur plusieurs bases de données ou lors de :doc:`changements de version E-Maj<upgrade>` par désinstallation et réinstallation complète de l’extension.

.. _export_param_conf:

Exporter une configuration de paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_export_parameters_configuration()* exportent sous forme de structure JSON l’ensemble des paramètres de la configuration présents dans la table :ref:`emaj_param<emaj_param>`.

On peut écrire dans un fichier les données de paramétrage par ::

   SELECT emaj_export_parameters_configuration('<chemin.fichier>');

Le chemin du fichier doit être accessible en écriture par l’instance PostgreSQL.

La fonction retourne le nombre de paramètres exportés.

Si le chemin du fichier n’est pas renseigné, la fonction retourne directement la structure JSON contenant les valeurs de paramètres. Cette structure ressemble à ceci ::

   {
     "_comment": "E-Maj parameters, generated from the database <db> with E-Maj version <version> at <date_heure>",
     "parameters": [
       {
          "key": "...",
          "value": "..."
       },
       {
          ...
       }
     ]
   }

.. _import_param_conf:

Importer une configuration de paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_import_parameters_configuration()* importent des paramètres sous forme de structure JSON dans la table :ref:`emaj_param<emaj_param>`.

On peut lire dans un fichier des paramètres à charger par ::

   SELECT emaj_import_parameters_configuration('<chemin.fichier>', <suppression.configuration.courante>);

Le chemin du fichier doit être accessible par l’instance PostgreSQL.

Le fichier doit contenir une structure JSON ayant un attribut nommé *"parameters"* de type tableau, et contenant des sous-structures avec les attributs *"key"* et *"value"* ::

   {"parameters": [
     {
       "key": "...",
       "value": "..."
     },
     {
   	   ...
     }
   ]}

Si un paramètre n’a pas d’attribut *"value"* ou si cet attribut est valorisé à *NULL*, le paramètre n’est pas inséré dans la table *emaj_param*, et est supprimé s’il existait déjà dans la table. En conséquence, la valeur par défaut du paramètre sera utilisée par l’extension *emaj*.

La fonction peut directement charger un fichier généré par la fonction *emaj_export_parameters_configuration()*.

Le second paramètre, de type booléen, est optionnel. Il indique si l’ensemble de la configuration présente doit être supprimée avant le chargement. Par défaut, sa valeur *FALSE* indique que les clés présentes dans la table *emaj_param* mais absentes de la structure JSON sont conservées (chargement en mode différentiel). Si la valeur du second paramètre est positionnée à *TRUE*, la fonction effectue un remplacement complet de la configuration de paramétrage (chargement en mode complet).

La fonction retourne le nombre de paramètres importés.

Dans une variante de la fonction, le premier paramètre en entrée contient directement la structure JSON des valeurs à charger ::

   SELECT emaj_import_parameters_configuration('<structure.JSON>', <suppression.configuration.courante>);

.. _emaj_get_current_log_table:

Identifier la table de log courante associée à une table applicative
--------------------------------------------------------------------

La fonction *emaj_get_current_log_table()* permet d’obtenir le schéma et le nom de la table de log courante associée à une table applicative. ::

	SELECT log_schema, log_table FROM
		emaj_get_current_log_table(<schéma>, <table>);

La fonction retourne toujours 1 ligne. Si la table applicative n’appartient pas actuellement à un groupe de tables, les colonnes *log_schema* et *log_table* ont une valeur NULL.

La fonction *emaj_get_current_log_table()* peut être exécutée par les rôles membres de *emaj_adm* et *emaj_viewer*.

Il est ainsi possible de construire une requête accédant à une table de log. Par exemple ::

	SELECT 'select count(*) from '
		|| quote_ident(log_schema) || '.' || quote_ident(log_table)
		FROM emaj.emaj_get_current_log_table('monschema','matable');

.. _emaj_purge_histories:

Purger les historiques
----------------------

E-Maj historise certaines données : traces globales de fonctionnement, détail des rollbacks E-Maj, évolutions de structures de groupes de tables (:ref:`plus de détails...<emaj_hist>`), Les traces les plus anciennes sont automatiquement purgées par l’extension. Mais une fonction permet également de déclencher la purge de manière manuelle ::

   SELECT emaj.emaj_purge_histories('<délai.rétention>');

La paramètre <délai.rétention> est de type *INTERVAL*. Il surcharge le paramètre *'history_retention'* de la table *emaj_param*.

.. _emaj_disable_protection_by_event_triggers:
.. _emaj_enable_protection_by_event_triggers:

Désactiver/réactiver les triggers sur événements
------------------------------------------------

L'installation de l'extension E-Maj créé et active des :ref:`triggers sur événements <event_triggers>` pour la protéger. En principe, ces triggers doivent rester en l'état. Mais si l'administrateur E-Maj a besoin de les désactiver puis les réactiver, il dispose de deux fonctions.

Pour désactiver les triggers sur événement existants ::

   SELECT emaj.emaj_disable_protection_by_event_triggers();

La fonction retourne le nombre de triggers désactivés (cette valeur dépend de la version de PostgreSQL installée).

Pour réactiver les triggers sur événement existants ::

   SELECT emaj.emaj_enable_protection_by_event_triggers();

La fonction retourne le nombre de triggers réactivés.

.. _emaj_snap_group:

Vider les tables et séquences d'un groupe de tables
---------------------------------------------------

Il peut s'avérer utile de prendre des images de toutes les tables et séquences appartenant à un groupe, afin de pouvoir en observer le contenu ou les comparer. Une fonction permet d'obtenir le vidage sur fichiers des tables d'un groupe ::

   SELECT emaj.emaj_snap_group('<nom.du.groupe>', '<répertoire.de.stockage>', '<options.COPY>');

Le nom du répertoire fourni doit être un chemin absolu. Ce répertoire doit exister au préalable et avoir les permissions adéquates pour que l'instance PostgreSQL puisse y écrire. 

Le troisième paramètre précise le format souhaité pour les fichiers générés. Il prend la forme d'une chaîne de caractères reprenant la syntaxe précise des options disponibles pour la commande SQL *COPY TO*. Voir la documentation de PostgreSQL pour le détail des options disponibles (https://www.postgresql.org/docs/current/sql-copy.html).

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Cette fonction *emaj_snap_group()* génère un fichier par table et par séquence appartenant au groupe de tables cité. Ces fichiers sont stockés dans le répertoire ou dossier correspondant au second paramètre de la fonction. D'éventuels fichiers de même nom se trouveront écrasés.

Le nom des fichiers créés est du type : *<nom.du.schema>_<nom.de.table/séquence>.snap*

Pour faciliter la manipulation des fichiers générés, d’éventuels caractères espaces, "/", "\\", "$", ">", "<", "|", simples ou doubles guillemets et "\*" sont remplacés par des "_". Attention, cette adaptation des noms de fichier peut conduire à des doublons, le dernier fichier généré écrasant alors les précédents.

Les fichiers correspondant aux séquences ne comportent qu'une seule ligne, qui contient les caractéristiques de la séquence.

Les fichiers correspondant aux tables contiennent un enregistrement par ligne de la table, dans le format spécifié en paramètre. Ces enregistrements sont triés dans l'ordre croissant de la clé primaire.

En fin d'opération, un fichier *_INFO* est créé dans ce même répertoire. Il contient un message incluant le nom du groupe de tables et la date et l'heure de l'opération.

Il n'est pas nécessaire que le groupe de tables soit dans un état inactif, c'est-à-dire qu'il ait été arrêté au préalable. 

Comme la fonction peut générer de gros ou très gros fichiers (dépendant bien sûr de la taille des tables), il est de la responsabilité de l'utilisateur de prévoir un espace disque suffisant.

Avec cette fonction, un test simple de fonctionnement d'E-Maj peut enchaîner :

* :ref:`emaj_create_group() <emaj_create_group>`,
* :ref:`emaj_start_group() <emaj_start_group>`,
* emaj_snap_group(<répertoire_1>),
* mises à jour des tables applicatives,
* :ref:`emaj_rollback_group() <emaj_rollback_group>`,
* emaj_snap_group(<répertoire_2>),
* comparaison du contenu des deux répertoires par une commande *diff* par exemple.
