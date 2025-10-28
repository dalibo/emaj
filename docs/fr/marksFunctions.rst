Gérer les marques
=================

En complément des :ref:`fonctions de pose de marques<emaj_set_mark_group>`, il existe plusieurs autres fonctions de gestion des marques.

.. _emaj_comment_mark_group:

Commenter une marque
--------------------

Il est possible de positionner un commentaire sur une marque lors de :ref:`son enregistrement<emaj_set_mark_group>`. Mais on peut le faire également plus tard avec : ::

   SELECT emaj.emaj_comment_mark_group('<nom.du.groupe>', '<nom.de.marque>', '<commentaire>');

Le mot clé '*EMAJ_LAST_MARK*' peut être utilisé comme nom de marque à commenter pour indiquer la dernière marque posée.

La fonction ne retourne aucune donnée.

Pour modifier un commentaire, il suffit d'exécuter à nouveau la fonction pour le même groupe de tables et la même marque, avec le nouveau commentaire.

Pour supprimer un commentaire, il suffit d'exécuter la fonction avec une valeur *NULL* pour le paramètre commentaire.

Les commentaires sont surtout intéressants avec l'utilisation d’:doc:`Emaj_web<webUsage>`, qui les affiche systématiquement dans le tableau des marques d'un groupe. Mais ils sont visibles également dans la colonne *mark_comment* de la table *emaj.emaj_mark*.

.. _emaj_get_previous_mark_group:

Rechercher une marque
---------------------

La fonction *emaj_get_previous_mark_group()* permet de connaître, pour un groupe de tables, le nom de la dernière marque qui précède soit une date et une heure donnée, soit une autre marque. ::

   SELECT emaj.emaj_get_previous_mark_group('<nom.du.groupe>', '<date.et.heure>');

ou ::

   SELECT emaj.emaj_get_previous_mark_group('<nom.du.groupe>', '<marque>');

Dans la première forme, la date et l'heure doivent être exprimées sous la forme d'un *TIMESTAMPTZ*, par exemple le littéral *'2011/06/30 12:00:00 +02'*. Si l'heure fournie est strictement égale à l'heure d'une marque existante, la marque retournée sera la marque précédente.

Dans la seconde forme, le mot clé '*EMAJ_LAST_MARK*' peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

.. _emaj_rename_mark_group:

Renommer une marque
-------------------

Une marque précédemment posée par l'une des fonctions :ref:`emaj_create_group() <emaj_create_group>` ou :ref:`emaj_set_mark_group() <emaj_set_mark_group>` peut être renommée avec la commande SQL ::

   SELECT emaj.emaj_rename_mark_group('<nom.du.groupe>', '<nom.de.marque>',
               '<nouveau.nom.de.marque>');

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque à renommer pour indiquer la dernière marque posée.

La fonction ne retourne aucune donnée.

Une marque portant le nouveau nom souhaité ne doit pas déjà exister pour le groupe de tables.

.. _emaj_delete_mark_group:

Effacer une marque
------------------

Une marque peut également être effacée par l'intermédiaire de la commande SQL ::

   SELECT emaj.emaj_delete_mark_group('<nom.du.groupe>', '<nom.de.marque>');

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

La fonction retourne la valeur 1, c'est à dire le nombre de marques effectivement effacées.

Pour qu'il reste au moins une marque après l'exécution de la fonction, l'effacement d'une marque n'est possible que s'il y a au moins 2 marques pour le groupe de tables concerné. 

Si la marque effacée est la première marque pour le groupe, les lignes devenues inutiles dans les tables de log sont supprimées.

Si une table a été :ref:`détachée d’un groupe de tables<remove_table_sequence>` et que la marque effacée correspond à la dernière marque connue pour cette table, les logs couvrant l’intervalle de temps entre cette marque et la marque précédente sont supprimés.


.. _emaj_delete_before_mark_group:

Effacer les marques les plus anciennes
--------------------------------------

Pour facilement effacer en une seule opération toutes les marques d'un groupe de tables antérieures à une marque donnée, on peut exécuter la requête ::

   SELECT emaj.emaj_delete_before_mark_group('<nom.du.groupe>', '<nom.de.marque>');

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

La fonction efface les marques antérieures à la marque spécifiée, cette dernière devenant la nouvelle première marque. Elle supprime également des tables de log toutes les données concernant les mises à jour de tables applicative antérieures à cette marque.

La fonction retourne le nombre de marques effacées.

La fonction procède également à la purge des événements les plus anciens de la table technique :ref:`emaj_hist <emaj_hist>`.

Cette fonction permet ainsi d'utiliser E-Maj sur de longues périodes sans avoir à arrêter et redémarrer les groupes, tout en limitant l'espace disque utilisé pour le log. 

Néanmoins, comme cette suppression de lignes dans les tables de log ne peut utiliser de verbe SQL *TRUNCATE*, la durée d'exécution de la fonction *emaj_delete_before_mark_group()* peut être plus longue qu'un simple arrêt et relance de groupe. En contrepartie, elle ne nécessite pas de pose de verrou sur les tables du groupe concerné. Son exécution peut donc se poursuivre alors que d'autres traitements mettent à jour les tables applicatives. Seules d'autres actions E-Maj sur le même groupe de tables, comme la pose d'une nouvelle marque, devront attendre la fin de l'exécution d'une fonction *emaj_delete_before_mark_group()*.

Associées, les fonctions *emaj_delete_before_mark_group()*, et :ref:`emaj_get_previous_mark_group() <emaj_get_previous_mark_group>` permettent d'effacer les marques antérieures à un délai de rétention. Ainsi par exemple, pour effacer toutes les marques (et supprimer les logs associés) posées depuis plus de 24 heures, on peut exécuter la requête ::

   SELECT emaj.emaj_delete_before_mark_group('<groupe>', 
          emaj.emaj_get_previous_mark_group('<groupe>', current_timestamp - '1 DAY'::INTERVAL));

.. _emaj_protect_mark_group:
.. _emaj_unprotect_mark_group:

Protéger une marque contre les rollbacks
----------------------------------------

Pour compléter le mécanisme de :ref:`protection des groupes de tables <emaj_protect_group>` contre les rollbacks intempestifs, il est possible de positionner des protections au niveau des marques. Deux fonctions répondent à ce besoin.

La fonction *emaj_protect_mark_group()* pose une protection sur une marque d'un groupe de tables.::

   SELECT emaj.emaj_protect_mark_group('<nom.du.groupe>','<nom.de.marque>');

La fonction retourne l'entier 1 si la marque n'était pas déjà protégée, ou 0 si elle était déjà protégée.

Une fois une marque protégée, toute tentative de rollback, tracé ou non, sera refusée si elle repositionne le groupe de tables à un état antérieur à cette marque protégée.

Une marque d'un groupe de tables de type « *audit-seul* » ou en état « *inactif* » ne peut être protégée.

Lorsqu'une marque est posée, elle n'est pas protégée. Les marques protégées d'un groupe de tables perdent automatiquement leur protection lorsque ce groupe de tables est arrêté. Attention, la suppression d'une marque protégée supprime de facto la protection. Elle ne reporte pas la protection sur une marque adjacente.

La fonction *emaj_unprotect_mark_group()* ôte une protection existante sur une marque d'un groupe de tables.::

   SELECT emaj.emaj_unprotect_mark_group('<nom.du.groupe>','<nom.de.marque>');

La fonction retourne l'entier 1 si la marque était bien protégée au préalable, ou 0 si elle n'était déjà déjà protégée.

Une marque d'un groupe de tables de type « *audit-seul* » ne peut être déprotégée.

Une fois la protection d'une marque ôtée, il devient à nouveau possible d'effectuer tous types de rollback sur une marque antérieure.

