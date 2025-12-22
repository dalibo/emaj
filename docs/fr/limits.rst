Limites d'utilisation
=====================

Limites générales
-----------------

L'utilisation de l'extension E-Maj présente quelques limitations.

* La **version PostgreSQL** minimum requise est la version 12.
* Toutes les tables appartenant à un groupe de tables de type "*rollbackable*" doivent avoir une **clé primaire** explicite (*PRIMARY KEY*). Si une table n’a pas de clé primaire explicite mais a un index *UNIQUE* référençant des colonnes *NOT NULL*, alors il est préférable de transformer cet index en clé primaire explicite.
* Les tables *UNLOGGED* ne peuvent pas appartenir à un groupe de tables de type "*rollbackable*".
* Les tables temporaires (*TEMPORARY*) ne sont pas gérées par E-Maj
* Dans certaines configurations, les clés étrangères (*FOREIGN KEYs*) définies au niveau des tables partitionnées ne sont pas supportées par les opérations de rollback E-Maj (:ref:`plus de détails<fk_on_partitionned_tables>`).
* Si une **opération de DDL** est exécutée sur une table applicative appartenant à un groupe de tables, il n'est pas possible pour E-Maj de remettre la table dans un état antérieur (:doc:`plus de détails<alterGroups>`).

.. _non_superuser_install_limits:

Limites des installations réalisées sans droit SUPERUSER
--------------------------------------------------------

Un rôle qui n’a pas les droits SUPERUSER peut :ref:`installer l’extension emaj avec le script psql <create_emaj_extension_by_script>` emaj-<version>.sql. Mais dans ce cas, il existe un certain nombre de limitations dans l’utilisation et le fonctionnement de l’extension. Ces limitations dépendent des droits réels que détient ce rôle, soit au moment de l’installation, soit lors de l’utilisation.

Propriétaire des tables et séquences
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La principale contrainte porte sur le propriétaire des tables et séquences. **Seules des tables et des séquences appartenant au rôle ayant installé emaj peuvent être assignées à un groupe de tables**.

.. _roles_limits:

Rôles emaj_adm et emaj_viewer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Si, lors de l’installation, le rôle *emaj_adm* n’existe pas, ou si le rôle de l’installateur n’a pas le droit *ADMIN* sur *emaj_adm*, E-Maj fonctionnera sans ce rôle et l’installateur sera le seul administrateur E-Maj.

Si, lors de l’installation, le rôle *emaj_viewer* n’existe pas, et si le rôle de l’installateur ne dispose pas du droit *CREATEROLE*, *emaj_viewer* n’est pas créé et E-Maj fonctionnera sans ce rôle de consultation.

Avant l’installation d’*emaj*, un rôle disposant des droits nécessaires peut exécuter les requêtes ::

   CREATE ROLE emaj_adm;
   GRANT emaj_adm TO <rôle.installateur> WITH ADMIN TRUE;

et ::

   CREATE ROLE emaj_viewer;
   ou
   ALTER ROLE <rôle.installateur> CREATEROLE;

.. _event_triggers_limits:

Triggers sur événements
^^^^^^^^^^^^^^^^^^^^^^^

Des :ref:`triggers sur événements<event_triggers>` protègent l’environnement E-Maj. Si le rôle qui installe l’extension ne dispose pas des droits nécessaires, ces triggers sur événements ne sont pas créés, diminuant le niveau de protection de l’environnement.


Importation et exportation de fichiers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Les fonctions d’importation des configurations de groupes de tables ou de paramètres lisent les données depuis des fichiers externes. Si le rôle d’installation ne dispose pas du droit *pg_read_server_files*, l’opération lui est interdite.

De la même manière, les fonctions d’exportation des configurations de groupes de tables ou de paramètres, de vidage des tables ou des mises à jour enregistrées, et de génération de scripts SQL écrivent dans des fichiers externes. Si le rôle d’installation ne dispose pas des droits *pg_write_server_files*, l’opération lui est interdite. Certaines de ces fonctions requièrent aussi les droits *pg_execute_server_program*.

Pour autoriser le rôle d’installation à lire ou écrire dans un fichier externe, un rôle disposant des droits nécessaires peut exécuter la requête ::

   GRANT pg_read_server_files, pg_write_server_files, pg_execute_server_program
   	 TO  <rôle_installation>;

Les opérations de manipulation de fichiers deviennent possibles dès l’attribution de ces droits, sans qu’il soit nécessaire de ré-installer l’extension.

.. _rollbacks_limits:

Gestion des rollbacks E-Maj
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Lorsque le rôle qui installe l’extension ne dispose pas du droit *SUPERUSER*, le :ref:`suivi des rollbacks E-Maj<emaj_rollback_activity>` ou la soumission des :doc:`rollbacks E-Maj parallélisés<parallelRollbackClient>` nécessitent d’attribuer à ce rôle (et uniquement à lui) :

* le droit d’exécuter la fonction *dblink_connect_u()*, ce droit n’étant pas attribué par défaut pour cette fonction, pour des raisons de sécurité ;
* le droit de lire le paramètre PostgreSQL *unix_socket_directories* lorsque l’instance n’est accessible que par une socket (i.e. quand le paramètre *listen_addresses* est vide).

Un rôle disposant des droits nécessaires peut exécuter les requêtes ::

   GRANT EXECUTE ON FUNCTION dblink_connect_u(text,text) TO <rôle install>;
   
   GRANT pg_read_all_settings TO <rôle install>;

Le suivi des rollbacks E-Maj et la soumission de rollbacks parallélisés deviennent possibles dès l’attribution de ces droits, sans qu’il soit nécessaire de ré-installer l’extension.

En outre, dans ce mode d’installation sans droit *SUPERUSER*, toutes les optimisations des rollbacks E-Maj ne sont pas disponibles, conduisant à un niveau de performance dégradé sur ces opérations.
