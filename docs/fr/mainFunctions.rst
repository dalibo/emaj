Fonctions principales
=====================

Avant de décrire chacune des principales fonctions d'E-Maj, il est intéressant d'avoir un aperçu global de l'enchaînement typique des opérations. 

Enchaînement des opérations
---------------------------

L'enchaînement des opérations possibles pour un groupe de tables peut se matérialiser par ce synoptique.

.. image:: images/group_flow.png
   :align: center

.. _emaj_start_group:

Démarrage d'un groupe de tables
-------------------------------

Démarrer un groupe de table consiste à activer l'enregistrement des mises à jour des tables du groupe. Pour ce faire, il faut exécuter la commande ::

   SELECT emaj.emaj_start_group('<nom.du.groupe>'[, '<nom.de.marque>' [, <effacer.anciens.logs?>]]);

Le groupe de tables doit être au préalable à l'état inactif.

Le démarrage du groupe de tables créé une première marque.

S'il est spécifié, le nom de la marque initiale peut contenir un caractère générique '%'. Ce caractère est alors remplacé par l'heure courante, au format *hh.mn.ss.mmmm*,

Si le paramètre représentant la marque n'est pas spécifié, ou s'il est vide ou *NULL*, un nom est automatiquement généré : "*START_%*", où le caractère '%' représente l'heure courante, au format *hh.mn.ss.mmmm*.
 
Le paramètre *<anciens.logs.à.effacer>* est un booléen optionnel. Par défaut sa valeur est égal à vrai (true), ce qui signifie que les tables de log du groupe de tables sont purgées de toutes anciennes données avant l'activation des triggers de log. Si le paramètre est explicitement positionné à « faux » (false), les anciens enregistrements sont conservés dans les tables de log. De la même manière, les anciennes marques sont conservées, même si ces dernières ne sont alors plus utilisables pour un éventuel rollback (des mises à jour ont pu être effectuées sans être tracées alors que le groupe de tables était arrêté).

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Pour être certain qu'aucune transaction impliquant les tables du groupe n'est en cours, la fonction *emaj_start_group()* pose explicitement sur chacune des tables du groupe un verrou de type *SHARE ROW EXCLUSIVE*. Si des transactions accédant à ces tables sont en cours, ceci peut se traduire par la survenue d'une étreinte fatale (*deadlock*). Si la résolution de l'étreinte fatale impacte la fonction E-Maj, le deadlock est intercepté et la pose de verrou est automatiquement réitérée, avec un maximum de 5 tentatives.

La fonction procède également à la purge des événements les plus anciens de la table technique :ref:`emaj_hist <emaj_hist>`.

A l'issue du démarrage d'un groupe, celui-ci devient actif ("*LOGGING*").

Plusieurs groupes de tables peuvent être démarrés en même temps, en utilisant la fonction *emaj_start_groups()* ::

   SELECT emaj.emaj_start_groups('<tableau.des.groupes>'[, '<nom.de.marque>' [, <effacer.anciens.logs?>]]);

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.


.. _emaj_set_mark_group:

Pose d'une marque intermédiaire
-------------------------------

Lorsque toutes les tables et séquences d'un groupe sont jugées dans un état stable pouvant servir de référence pour un éventuel *rollback*, une marque peut être posée. Ceci s'effectue par la requête SQL suivante ::

   SELECT emaj.emaj_set_mark_group('<nom.du.groupe>'[, '<nom.de.marque>']);

Le groupe de tables doit être à l'état actif.

Une marque de même nom ne doit pas déjà exister pour le groupe de tables.

Le nom de la marque peut contenir un caractère générique '%'. Ce caractère est alors remplacé par l'heure courante, au format *hh.mn.ss.mmmm*,

Si le paramètre représentant la marque n'est pas spécifié ou s'il est vide ou *NULL*, un nom est automatiquement généré : « *MARK_%* », où le caractère '%' représente l'heure courante, au format *hh.mn.ss.mmmm*.
 
La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

La fonction *emaj_set_mark_group()* enregistre l'identité de la nouvelle marque, avec l'état des séquences applicatives appartenant au groupe, ainsi que l'état des séquences associées aux tables de log. Les séquences applicatives sont traitées en premier, pour enregistrer leur état au plus près du début de la transaction, ces séquences ne pouvant pas être protégées des mises à jour par des verrous.

Il est possible d'enregistrer deux marques consécutives sans que des mises à jour de tables aient été enregistrées entre ces deux marques.

La fonction *emaj_set_mark_group()* pose des verrous de type « *ROW EXCLUSIVE* » sur chaque table du groupe. Ceci permet de s'assurer qu'aucune transaction ayant déjà fait des mises à jour sur une table du groupe n'est en cours. Néanmoins, ceci ne garantit pas qu'une transaction ayant lu une ou plusieurs tables avant la pose de la marque, fasse des mises à jours après la pose de la marque. Dans ce cas, ces mises à jours effectuées après la pose de la marque seraient candidates à un éventuel rollback sur cette marque.

Une marque peut être posée sur plusieurs groupes de tables même temps, en utilisant la fonction *emaj_set_mark_groups()* ::

   SELECT emaj.emaj_set_mark_groups('<tableau.des.groupes>'[, '<nom.de.marque>']);

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.


.. _emaj_rollback_group:

Rollback simple d'un groupe de tables
-------------------------------------

S'il est nécessaire de remettre les tables et séquences d'un groupe dans l'état dans lequel elles se trouvaient lors de la prise d'une marque, il faut procéder à un rollback. Pour un rollback simple (« *unlogged* » ou « *non tracé* »), il suffit d'exécuter la requête SQL suivante ::

   SELECT * FROM emaj.emaj_rollback_group('<nom.du.groupe>', '<nom.de.marque>' [, <est_altération_groupe_permise>]);

Le groupe de tables doit être à l'état actif et la marque indiquée doit être toujours « active », c'est à dire qu'elle ne doit pas être marquée comme logiquement supprimée.

Le mot clé '*EMAJ_LAST_MARK*' peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

Le 3ème paramètre est un booléen qui indique si l’opération de rollback peut cibler une marque posée antérieurement à une opération de :doc:`modification du groupe de tables <alterGroups>`. Selon leur nature, les modifications de groupe de tables effectuées alors que ce dernier est en état *LOGGING* peuvent être ou non automatiquement annulées. Dans certains cas, cette annulation peut être partielle. Par défaut, ce paramètre prend la valeur *FAUX*.

La fonction retourne un ensemble de lignes comportant un niveau de sévérité pouvant prendre les valeurs « *Notice* » ou « *Warning* », et un texte de message. La fonction retourne une ligne de type « *Notice* » indiquant le nombre de tables et de séquences effectivement modifiées par l'opération de rollback. Des lignes de types « *Warning* » peuvent aussi être émises dans le cas où des opérations de modification du groupe de tables ont du être traitées par le rollback.

Pour être certain qu'aucune transaction concurrente ne mette à jour une table du groupe pendant toute la durée du rollback, la fonction *emaj_rollback_group()* pose explicitement un verrou de type *EXCLUSIVE* sur chacune des tables du groupe. Si des transactions accédant à ces tables en mise à jour sont en cours, ceci peut se traduire par la survenue d'une étreinte fatale (deadlock). Si la résolution de l'étreinte fatale impacte la fonction E-Maj, le deadlock est intercepté et la pose de verrou est automatiquement réitérée, avec un maximum de 5 tentatives. En revanche, les tables du groupe continuent à être accessibles en lecture pendant l'opération.

Le rollback E-Maj prend en compte la présence éventuelle de triggers et de clés étrangères sur la table concernée. Plus de détails :doc:`ici <rollbackDetails>`.

Lorsque le volume de mises à jour à annuler est important et que l'opération de rollback est longue, il est possible de suivre l'avancement de l'opération à l'aide de la fonction :ref:`emaj_rollback_activity() <emaj_rollback_activity>` ou du client :doc:`emajRollbackMonitor <rollbackMonitorClient>`.

A l'issue de l'opération de rollback, se trouvent effacées :

* les données des tables de log qui concernent les mises à jour annulées,
* toutes les marques postérieures à la marque référencée dans la commande de rollback.

Il est alors possible de poursuivre les traitements de mises à jour, de poser ensuite d'autres marques et éventuellement de procéder à un nouveau rollback sur une marque quelconque.

Plusieurs groupes de tables peuvent être « rollbackés » en même temps, en utilisant la fonction *emaj_rollback_groups()* ::

   SELECT * FROM emaj.emaj_rollback_groups('<tableau.des.groupes>', '<nom.de.marque>' [, <est_altération_groupe_permise>]);

La marque indiquée doit strictement correspondre à un même moment dans le temps pour chacun des groupes listés. En d'autres termes, cette marque doit avoir été posée par l'appel d'une même fonction :ref:`emaj_set_mark_groups() <emaj_set_mark_group>`.

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.

.. _emaj_logged_rollback_group:

Rollback annulable d'un groupe de tables
----------------------------------------

Une autre fonction permet d'exécuter un rollback de type « *logged* », Dans ce cas, les triggers de log sur les tables applicatives ne sont pas désactivés durant le rollback, de sorte que durant le rollback les mises à jours de tables appliquées sont elles-mêmes enregistrées dans les tables de log. Ainsi, il est ensuite possible d'annuler le rollback ou, en quelque sorte, de « rollbacker le rollback ». 

Pour exécuter un « *logged rollback* » sur un groupe de tables, il suffit d'exécuter la requête SQL suivante::

   SELECT * FROM emaj.emaj_logged_rollback_group('<nom.du.groupe>', '<nom.de.marque>' [, <est_altération_groupe_permise>]);

Les règles d'utilisation sont les mêmes que pour la fonction *emaj_rollback_group()*, 

Le groupe de tables doit être en état démarré (*LOGGING*) et la marque indiquée doit être toujours « active », c'est à dire qu'elle ne doit pas être marquée comme logiquement supprimée (*DELETED*).

Le mot clé 'EMAJ_LAST_MARK' peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

Le 3ème paramètre est un booléen qui indique si l’opération de rollback peut cibler une marque posée antérieurement à une opération de :doc:`modification du groupe de tables <alterGroups>`. Selon leur nature, les modifications de groupe de tables effectuées alors que ce dernier est en état *LOGGING* peuvent être ou non automatiquement annulées. Dans certains cas, cette annulation peut être partielle. Par défaut, ce paramètre prend la valeur *FAUX*.

La fonction retourne un ensemble de lignes comportant un niveau de sévérité pouvant prendre les valeurs « *Notice* » ou « *Warning* », et un texte de message. La fonction retourne une ligne de type « *Notice* » indiquant le nombre de tables et de séquences effectivement modifiées par l'opération de rollback. Des lignes de types « *Warning* » peuvent aussi être émises dans le cas où des opérations de modification du groupe de tables ont du être traitées par le rollback.

Pour être certain qu'aucune transaction concurrente ne mette à jour une table du groupe pendant toute la durée du rollback, la fonction *emaj_logged_rollback_group()* pose explicitement un verrou de type *EXCLUSIVE* sur chacune des tables du groupe. Si des transactions accédant à ces tables en mise à jour sont en cours, ceci peut se traduire par la survenue d'une étreinte fatale (*deadlock*). Si la résolution de l'étreinte fatale impacte la fonction E-Maj, le *deadlock* est intercepté et la pose de verrou est automatiquement réitérée, avec un maximum de 5 tentatives. En revanche, les tables du groupe continuent à être accessibles en lecture pendant l'opération.

Le rollback E-Maj prend en compte la présence éventuelle de triggers et de clés étrangères sur la table concernée. Plus de détails :doc:`ici <rollbackDetails>`.

Contrairement à la fonction *emaj_rollback_group()*, à l'issue de l'opération de rollback, les données des tables de log qui concernent les mises à jour annulées, ainsi que les éventuelles marques postérieures à la marque référencée dans la commande de rollback sont conservées.

De plus, en début et en fin d'opération, la fonction pose automatiquement sur le groupe deux marques, nommées :

* '*RLBK_<marque.du.rollback>_<heure_du_rollback>_START*'
* '*RLBK_<marque.du.rollback>_<heure_du_rollback>_DONE*'

où *<heure_du_rollback>* représente l'heure de début de la transaction effectuant le rollback, exprimée sous la forme « *heures.minutes.secondes.millisecondes* ».

Lorsque le volume de mises à jour à annuler est important et que l'opération de rollback est longue, il est possible de suivre l'avancement de l'opération à l'aide de la fonction :ref:`emaj_rollback_activity() <emaj_rollback_activity>` ou du client :doc:`emajRollbackMonitor <rollbackMonitorClient>`.

A l'issue du rollback, il est possible de poursuivre les traitements de mises à jour, de poser d'autres marques et éventuellement de procéder à un nouveau rollback sur une marque quelconque, y compris la marque automatiquement posée en début de rollback, pour annuler ce dernier, ou encore une ancienne marque postérieure à la marque utilisée pour le rollback.
oDes rollbacks de différents types (*logged* / *unlogged*) peuvent être exécutés en séquence. on peut ainsi procéder à l'enchaînement suivant::

* Pose de la marque M1
* …
* Pose de la marque M2
* …
* Logged rollback à M1 (générant les marques *RLBK_M1_<heure>_STRT*, puis *RLBK_M1_<heure>_DONE*)
* …
* Rollback à RLBK_M1_<heure>_DONE (pour annuler le traitement d'après rollback)
* …
* Rollback à RLBK_M1_<heure>_STRT (pour finalement annuler le premier rollback)

Une :ref:`fonction de « consolidation »<emaj_consolidate_rollback_group>` de « *rollback tracé* » permet de transformer un rollback annulable en rollback simple.

Plusieurs groupes de tables peuvent être « rollbackés » en même temps, en utilisant la fonction *emaj_logged_rollback_groups()* ::

   SELECT * FROM emaj.emaj_logged_rollback_groups ('<tableau.des.groupes>', '<nom.de.marque>' [, <est_altération_groupe_permise>]);

La marque indiquée doit strictement correspondre à un même moment dans le temps pour chacun des groupes listés. En d'autres termes, cette marque doit avoir été posée par l'appel d'une même fonction :ref:`emaj_set_mark_groups() <emaj_set_mark_group>`.

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`. 

.. _emaj_stop_group:

Arrêt d'un groupe de tables
---------------------------

Lorsqu'on souhaite arrêter l'enregistrement des mises à jour des tables d'un groupe, il est possible de désactiver le log par la commande SQL ::

   SELECT emaj.emaj_stop_group('<nom.du.groupe>'[, '<nom.de.marque'>]);

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

La fonction pose automatiquement une marque correspondant à la fin de l'enregistrement. 
Si le paramètre représentant cette marque n'est pas spécifié ou s'il est vide ou *NULL*, un nom est automatiquement généré : « *STOP_%* », où le caractère '%' représente l'heure courante, au format *hh.mn.ss.mmmm*.

L'arrêt d'un groupe de table désactive simplement les triggers de log des tables applicatives du groupe. La pose de verrous de type *SHARE ROW EXCLUSIVE* qu’entraîne cette opération peut se traduire par la survenue d'une étreinte fatale (*deadlock*).  Si la résolution de l'étreinte fatale impacte la fonction E-Maj, le deadlock est intercepté et la pose de verrou est automatiquement réitérée, avec un maximum de 5 tentatives.

En complément, la fonction *emaj_stop_group()* passe le statut des marques à l'état « supprimé ». Il n'est dès lors plus possible d'exécuter une commande de rollback, même si aucune mise à jour n'est intervenue sur les tables entre l'exécution des deux fonctions *emaj_stop_group()* et *emaj_rollback_group()*.

Pour autant, le contenu des tables de log et des tables internes d'E-Maj peut encore être visualisé.

A l'issue de l'arrêt d'un groupe, celui-ci redevient inactif.

Exécuter la fonction *emaj_stop_group()* sur un groupe de tables déjà arrêté ne génère pas d'erreur. Seul un message d'avertissement est retourné.

Plusieurs groupes de tables peuvent être arrêtés en même temps, en utilisant la fonction *emaj_stop_groups()* ::

   SELECT emaj.emaj_stop_groups('<tableau.des.groupes>'[, '<nom.de.marque'>]);

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`. 

