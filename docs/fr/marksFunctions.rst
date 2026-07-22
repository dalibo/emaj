Gérer les marques
=================

En complément des :ref:`fonctions de pose de marques<emaj_set_mark_group>`, il existe plusieurs autres fonctions de gestion des marques.

.. _emaj_comment_mark_group:

Commenter une marque
--------------------

Il est possible d'ennregistrer, modifier ou supprimer un commentaire sur une marque avec : ::

   SELECT emaj.emaj_comment_mark_group(p_group, p_mark, p_comment);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables.
- ``p_mark`` (*TEXT*) : **Marque** à commenter. Le mot clé '*EMAJ_LAST_MARK*' peut être utilisé comme nom de marque à commenter pour indiquer la dernière marque posée.
- ``p_comment`` (*TEXT*) : **Commentaire** décrivant la marque. Une valeur *NULL* supprime tout commentaire existant.

**Données retournées**

La fonction ne retourne aucune donnée.

**Notes**

Un commentaire peut aussi être directement ennregistré :ref:`lors de la pose de la marque<emaj_set_mark_group>`.

Les commentaires sont surtout intéressants avec l'utilisation d’:doc:`Emaj_web<webUsage>`, qui les affiche systématiquement dans le tableau des marques d'un groupe. Mais ils sont visibles également dans la colonne *mark_comment* de la table *emaj.emaj_mark*.

----

.. _emaj_get_previous_mark_group:

Rechercher une marque
---------------------

La fonction ``emaj_get_previous_mark_group()`` permet de connaître, pour un groupe de tables, le nom de la dernière marque qui précède soit une date et une heure donnée, soit une autre marque. ::

   SELECT emaj.emaj_get_previous_mark_group(p_group, p_datetime);

ou ::

   SELECT emaj.emaj_get_previous_mark_group(p_group, p_mark);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables.
- ``p_datetime`` (*TIMESTAMPTZ*) : **Date et heure** à rechercher.
- ``p_mark`` (*TEXT*) : **Marque** à rechercher. Le mot clé ``EMAJ_LAST_MARK`` peut être utilisé comme nom de marque à commenter pour indiquer la dernière marque posée.

**Données retournées**

La fonction retourne le nom de la marque, ou *NULL* si aucune marque n'a été trouvée.

** Notes**

Si l'heure fournie est strictement égale à l'heure d'une marque existante, la marque retournée sera la marque qui précède.

----

.. _emaj_rename_mark_group:

Renommer une marque
-------------------

Une marque existante peut être renommée avec la requête SQL : ::

   SELECT emaj.emaj_rename_mark_group(p_group, p_mark, p_newName);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables.
- ``p_mark`` (*TEXT*) : **Marque** à renommer. Le mot clé ``EMAJ_LAST_MARK`` peut être utilisé comme nom de marque pour indiquer la dernière marque posée.
- ``p_newMark`` (*TEXT*) : **Nouveau nom** de la marque. Il peut contenir un caractère ``%`` représentant l’heure courante au format ``hh.mm.ss.mmmm``. Si le paramètre n'est pas fourni ou a une valeur non *NULL* ou vide, un nom de marque est généré : ``MARK_%``.

**Données retournées**

La fonction ne retourne aucune donnée.

**Notes**

Une marque portant le nouveau nom souhaité ne doit pas déjà exister pour le groupe de tables.

----

.. _emaj_delete_mark_group:

Effacer une marque
------------------

Une marque peut également être effacée par l'intermédiaire de la requête SQL : ::

   SELECT emaj.emaj_delete_mark_group(p_group, p_mark);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables.
- ``p_mark`` (*TEXT*) : **Marque** à effacer. Le mot clé ``EMAJ_LAST_MARK`` peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

**Données retournées**

La fonction retourne la valeur 1, c'est à dire le nombre de marques effectivement effacées.

**Notes**

Le groupe de table peut être actif ou inactif.

Une marque ne peut pas être effacée si c'est la seule marque de son groupe de tables.

Si la marque effacée est la plus ancienne marque du groupe, les lignes des tables de log enregistrées avant cette marque deviennent inutiles et sont donc supprimées.

----

.. _emaj_delete_before_mark_group:

Effacer les marques les plus anciennes
--------------------------------------

Pour facilement effacer en une seule opération toutes les marques d'un groupe de tables antérieures à une marque donnée, on peut exécuter la requête : ::

   SELECT emaj.emaj_delete_before_mark_group(p_group, p_mark);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables.
- ``p_mark`` (*TEXT*) : Nom de la nouvelle **plus ancienne marque**. Le mot clé ``EMAJ_LAST_MARK`` peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

**Données retournées**

La fonction retourne le nombre de marques effacées.

**Notes**

La fonction efface les marques antérieures à la marque spécifiée, cette dernière devenant la nouvelle première marque. Elle supprime également des tables de log toutes les données concernant les mises à jour de tables applicative antérieures à cette marque.

La fonction purge également les événements les plus anciens des tables techniques historisées.

Cette fonction permet ainsi d'utiliser E-Maj sur de longues périodes sans avoir à arrêter et redémarrer les groupes, tout en limitant l'espace disque utilisé pour le log. 

Néanmoins, comme cette suppression de lignes dans les tables de log ne peut utiliser de verbe SQL *TRUNCATE* (contrairement aux fonctions :ref:`emaj_start_group() <emaj_start_group>` ou :ref:`emaj_reset_group() <emaj_reset_group>`), la durée d'exécution de la fonction *emaj_delete_before_mark_group()* peut être plus longue qu'un simple arrêt et relance de groupe. En contrepartie, elle ne nécessite pas de pose de verrou sur les tables du groupe concerné. Son exécution peut donc se poursuivre alors que d'autres traitements mettent à jour les tables applicatives. Seules d'autres actions E-Maj sur le même groupe de tables, comme la pose d'une nouvelle marque, devront attendre la fin de l'exécution d'une fonction *emaj_delete_before_mark_group()*.

Associées, les fonctions *emaj_delete_before_mark_group()*, et :ref:`emaj_get_previous_mark_group() <emaj_get_previous_mark_group>` permettent d'effacer les marques antérieures à un délai de rétention. Ainsi par exemple, pour effacer toutes les marques (et supprimer les logs associés) posées depuis plus de 24 heures, on peut exécuter la requête : ::

   SELECT emaj.emaj_delete_before_mark_group('mon_groupe', 
          emaj.emaj_get_previous_mark_group('mon_groupe', current_timestamp - '1 DAY'::INTERVAL));

----

.. _emaj_protect_mark_group:

Protéger une marque contre les rollbacks
----------------------------------------

Pour compléter le mécanisme de :ref:`protection des groupes de tables <emaj_protect_group>` contre les rollbacks intempestifs, il est possible de positionner des protections au niveau des marques. Deux fonctions répondent à ce besoin.

La fonction ``emaj_protect_mark_group()`` pose une protection sur une marque d'un groupe de tables : ::

   SELECT emaj.emaj_protect_mark_group(p_group, p_mark);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables.
- ``p_mark`` (*TEXT*) : **Marque** à protéger. Le mot clé ``EMAJ_LAST_MARK`` peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

**Données retournées**

La fonction retourne l'entier 1 si la marque n'était pas déjà protégée, ou 0 si elle était déjà protégée.

**Notes**

Une fois une marque protégée, toute tentative de **rollback**, tracé ou non, **est refusée** si elle repositionne le groupe de tables à un état **antérieur** à cette marque protégée.

Une marque d'un groupe de tables de type *AUDIT-ONLY* ou en état inactif (*IDLE*) ne peut être protégée.

Lorsqu'une marque est posée, elle n'est pas protégée. Les marques protégées d'un groupe de tables perdent automatiquement leur protection lorsque ce groupe de tables est arrêté. 

.. caution::

   La suppression d'une marque protégée supprime de facto la protection. Elle ne reporte pas la protection sur une marque adjacente.

----

.. _emaj_unprotect_mark_group:

La fonction ``emaj_unprotect_mark_group()`` ôte une protection existante sur une marque d'un groupe de tables : ::

   SELECT emaj.emaj_unprotect_mark_group(p_group, p_mark);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables.
- ``p_mark`` (*TEXT*) : **Marque** à déprotéger. Le mot clé ``EMAJ_LAST_MARK`` peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

**Données retournées**

La fonction retourne l'entier 1 si la marque était bien protégée au préalable, ou 0 si elle n'était déjà déjà protégée.

**Notes**

Une fois la protection d'une marque ôtée, il devient à nouveau possible d'effectuer tous types de rollback sur une marque antérieure.

