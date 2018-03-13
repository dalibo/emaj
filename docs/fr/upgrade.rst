Mise à jour d'une version E-Maj existante
=========================================

Démarche générale
-----------------

La première étape consiste à :doc:`installer la nouvelle version du logiciel E-Maj <install>`. Conserver l’ancien répertoire E-Maj au moins jusqu’à la fin de la mise à jour. Certains fichiers pourront être utiles.

Il faut également vérifier si des :ref:`opérations préliminaires <preliminary_operations>` doivent être exécutées (extensions pré-requises, *tablespace* par défaut).

Ensuite, la procédure de mise à jour de la version d'E-Maj installée dans une base de données dépend de cette version installée.

Pour les versions d'E-Maj antérieures à 0.11.0, il n'existe pas de procédure spécifique de mise à jour. On procédera donc à une simple désinstallation puis réinstallation de l'extension. Cette démarche peut d'ailleurs être utilisée quelle que soit la version d'E-Maj installée. Elle présente néanmoins l'inconvénient de devoir supprimer tous les logs enregistrés, perdant ainsi toute capacité ultérieure de rollback ou d'examen des mises à jour enregistrées.

Pour les versions d'E-Maj installées 0.11.0 et suivantes, il est possible de procéder à une mise à jour sans désinstallation. Suivant la situation, il faut procéder en une ou en plusieurs étapes.

.. caution::

   A partir de la version 2.2.0, E-Maj ne supporte plus les versions de PostgreSQL antérieures à 9.2. Si une version antérieure de PostgreSQL est utilisée, il faut la faire évoluer avant de migrer E-Maj dans une version supérieure.


Mise à jour par désinstallation puis réinstallation
---------------------------------------------------

Pour ce type de mise à jour, il n'est pas nécessaire d'utiliser la procédure de :doc:`désinstallation complète <uninstall>`. Les tablespaces et les rôles peuvent notamment rester en l'état. En revanche, il peut s'avérer judicieux de sauvegarder quelques données utiles. C'est pourquoi, la démarche suivante est proposée.

Arrêt des groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Si certains groupes de tables sont encore actifs, il faut au préalable les arrêter à l'aide de la fonction :ref:`emaj_stop_group() <emaj_stop_group>` (ou de la fonction:ref:`emaj_force_stop_group() <emaj_force_stop_group>` si :ref:`emaj_stop_group() <emaj_stop_group>` retourne une erreur).

Sauvegarde des données utilisateurs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Il peut en effet être utile de sauvegarder le contenu de la table *emaj_group_def* pour un rechargement facile après le changement de version, par exemple en la copiant sur un fichier par une commande *\copy*, ou en dupliquant la table en dehors du schéma *emaj* avec une requête SQL ::

   CREATE TABLE public.sav_group_def AS SELECT * FROM emaj.emaj_group_def;

De la même manière, si l'administrateur E-Maj a modifié des paramètres dans la table *emaj_param*, il peut être souhaitable d'en conserver les valeurs, avec par exemple ::

   CREATE TABLE public.sav_param AS SELECT * FROM emaj.emaj_param WHERE param_key <> 'emaj_version';


Suppression et réinstallation d'E-Maj
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Une fois connecté en tant que super-utilisateur, il suffit d'enchaîner le script de désinstallation *uninstall.sql* de la version en place puis la création de l’extension. ::

   \i <répertoire_ancien_emaj>/sql/uninstall.sql

   CREATE EXTENSION emaj;

NB : à partir de la version 2.0.0, le script de désinstallation se nomme *emaj_uninstall.sql*.


Restauration des données utilisateurs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Les données sauvegardées au préalable peuvent alors être restaurées dans les deux tables techniques d’E-Maj, par exemple avec des requêtes de type *INSERT SELECT*. ::

   INSERT INTO emaj.emaj_group_def SELECT * FROM public.sav_group_def;

   INSERT INTO emaj.emaj_param SELECT * FROM public.sav_param;

Une fois les données copiées, les tables ou fichiers temporaires peuvent être supprimés.


Mise à jour à partir d’une version E-Maj comprise entre 0.11.0 et 1.3.1
-----------------------------------------------------------------------
Pour les versions comprises entre 0.11.0 et 1.3.1, des **scripts psql de mise à jour** sont livrés. Ils permettent de passer d’une version à la suivante.

Chaque étape peut être réalisée sans toucher aux groupes de tables, ceux-ci pouvant même être actifs au moment du changement de version. Ceci signifie en particulier :

* que des mises à jour de tables peuvent être enregistrées avant puis après le changement de version, sans que les groupes de tables soient arrêtés,
* et donc qu'après le changement de version, un *rollback* à une marque posée avant ce changement de version est possible.

+---------------+----------------+---------------------------+-------------+-------------------------------+
|Version source | Version cible  | script psql               | Durée       | Mises à jour concurrentes (1) |
+===============+================+===========================+=============+===============================+
| 0.11.0        | 0.11.1         | emaj-0.11.0-to-0.11.1.sql | Très rapide | Oui                           |
+---------------+----------------+---------------------------+-------------+-------------------------------+
| 0.11.1        | 1.0.0          | emaj-0.11.1-to-1.0.0.sql  | Très rapide | Oui                           |
+---------------+----------------+---------------------------+-------------+-------------------------------+
| 1.0.0         | 1.0.1          | emaj-1.0.0-to-1.0.1.sql   | Très rapide | Oui                           |
+---------------+----------------+---------------------------+-------------+-------------------------------+
| 1.0.1         | 1.0.2          | emaj-1.0.1-to-1.0.2.sql   | Très rapide | Oui                           |
+---------------+----------------+---------------------------+-------------+-------------------------------+
| 1.0.2         | 1.1.0          | emaj-1.0.2-to-1.1.0.sql   | Variable    | Non (2)                       |
+---------------+----------------+---------------------------+-------------+-------------------------------+
| 1.1.0         | 1.2.0          | emaj-1.1.0-to-1.2.0.sql   | Très rapide | Oui                           |
+---------------+----------------+---------------------------+-------------+-------------------------------+
| 1.2.0         | 1.3.0          | emaj-1.2.0-to-1.3.0.sql   | Rapide      | Oui (3)                       |
+---------------+----------------+---------------------------+-------------+-------------------------------+
| 1.3.0         | 1.3.1          | emaj-1.3.0-to-1.3.1.sql   | Très rapide | Oui                           |
+---------------+----------------+---------------------------+-------------+-------------------------------+

(1) La dernière colonne indique si la mise à jour de la version E-Maj peut être effectuée alors que des tables couvertes par E-Maj sont accédées en mise à jour. Notons que durant la mise à jour, d’éventuelles autres actions E-Maj (pose de marque, rollback,…) sont mises en attentes.

(2) Le passage en 1.1.0 nécessite la transformation des tables de log (ajout d'une colonne). Cela a pour conséquence que :

* même si les groupes de tables peuvent rester actifs, ce changement de version ne peut s'exécuter qu'à un moment où les tables ne sont pas mises à jour par des traitements,
* la durée de l'opération est très variable et dépend essentiellement du volume de données contenu dans les tables de log.

Notez également que les statistiques qu'E-Maj a collectées lors des précédentes opérations de rollback ne sont pas reprises (le fonctionnement des rollbacks est trop différent pour que ces anciennes statistiques soient pertinentes).

(3) Il est recommandé de réaliser le passage en 1.3.0 dans une période de faible activité sur la base de données. En effet, le renommage des triggers E-Maj sur les tables applicatives entraîne la pose de verrous de type *Access Exclusive* qui peuvent entrer en conflit avec d'autres accès.

A la fin de chaque mise à jour le message suivant est affiché :

>>> E-Maj successfully upgraded to <nouvelle_version>


Passage d’E-Maj 1.3.1 à une version supérieure
----------------------------------------------

La mise à jour de la version 1.3.1 est spécifique car elle doit gérer le passage d’une installation par script *psql* à une installation par *extension*.

Pour ce faire, il suffit d’exécuter la requête SQL ::

   CREATE EXTENSION emaj FROM unpackaged;

C’est le gestionnaire d’extension de PostgreSQL qui détermine le ou les scripts à exécuter en fonction de la version indiquée comme courante dans le fichier *emaj.control*.

Cette mise à jour ne peut néanmoins pas traiter le cas où au moins un groupe de tables a été créé avec une version de PostgreSQL antérieure à 8.4. Dans ce cas le ou les groupes de tables concernés doivent être supprimés au préalable puis recréés par la suite.


Mise à jour d’une version déjà installée comme extension
--------------------------------------------------------

Une version existante installée comme une *extension* se met à jour par une simple requête ::
 
   ALTER EXTENSION emaj UPDATE;

C’est le gestionnaire d’extension de PostgreSQL qui détermine le ou les scripts à exécuter en fonction de la version installée et de la version indiquée comme courante dans le fichier *emaj.control*.

L’opération est très rapide et ne touche pas aux groupes de tables. Ceux-ci peuvent rester actifs au moment de la mise à jour. Ceci signifie en particulier :

* que des mises à jour de tables peuvent être enregistrées avant puis après le changement de version
* et donc qu'après le changement de version, un *rollback* à une marque posée avant ce changement de version est possible.

Spécificités liées aux versions :

* La procédure de mise à jour d’une version 2.0.1 en version 2.1.0 peut modifier la table :ref:`emaj_group_def <emaj_group_def>` pour refléter le fait que le tablespace tspemaj n’est plus automatiquement considéré comme un tablespace par défaut. Si *tspemaj* est effectivement utilisé comme tablespace par défaut pour des groupes de tables créés, le contenu des colonnes *grpdef_log_dat_tsp* et *grpdef_log_idx_tsp* de la table *emaj_group_def* est automatiquement ajusté afin qu’une future opération de suppression puis recréation d’un groupe de tables puisse stocker les tables et index de log dans les mêmes tablespaces. L’administrateur peut revoir ces changements pour être sûr qu’ils correspondent bien à ses souhaits.

* La procédure de mise à jour d’une version 2.2.2 en version 2.2.3 vérifie les valeurs des séquences de log enregistrées. Dans certains cas, elle peut demander une ré-initialisation préalable de certains groupes de tables.
