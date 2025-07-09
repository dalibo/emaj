Contribuer au développement d'E-Maj
===================================

Toute contribution au développement et à l’amélioration de l’extension E-Maj est la bienvenue. Cette page fournit quelques informations pour faciliter ces contributions.

Bâtir l’environnement E-Maj
---------------------------

Le référentiel de l’extension E-Maj est hébergé sur le site *github* : https://github.com/dalibo/emaj

Cloner le dépôt E-Maj
^^^^^^^^^^^^^^^^^^^^^
La première opération à réaliser consiste donc à cloner ce dépôt en local sur son serveur/poste. Pour ce faire, utiliser les fonctionnalités de l’interface web de *github* ou taper la commande *shell* ::

   git clone https://github.com/dalibo/emaj.git

Description de l’arborescence E-Maj
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On dispose alors d’une arborescence complète (hors clients web). Elle comprend tous les répertoires et fichiers décrits en :doc:`annexe <content>`, à l’exception du contenu du répertoire *doc* maintenu séparément (voir plus bas).

L’arborescence comprend les éléments complémentaires suivants :

* le fichier *tar.index* qui permet de créer le fichier contenant la version E-Maj distribuée sur *pgxn.org*
* un répertoire *docs* avec tous les sources de la :ref:`documentation <documenting>` en ligne
* dans le répertoire *sql* :

  * le fichier *emaj- -devel.sql*, source de l’extension dans sa version courante
  * le source de la version précédente *emaj- -<version_précédente>.sql*
  * un script *emaj_prepare_emaj_web_test.sql* qui prépare un environnement E-Maj pour les tests du client *Emaj_web*

* un répertoire *test* contenant tous les éléments permettant de :ref:`tester l’extension<testing>`
* un répertoire *tools* contenant un certain nombre d’outils.


Paramétrer les outils
^^^^^^^^^^^^^^^^^^^^^
Les outils présents dans le répertoire tools nécessitent d’être paramétrés en fonction de l’environnement de chacun. Un système de paramétrage couvre certains outils. Pour les autres, le fichier *tools/README* détaille les adaptations à réaliser.

Le fichier *tools/emaj_tools.profile* contient des définitions de variables et de fonctions utilisées par différents outils. En principe, ces éléments n’ont pas besoin d’être modifiés.

Création du fichier emaj_tools.env
''''''''''''''''''''''''''''''''''

Les paramétres susceptibles d’être modifiés sont regroupés dans le fichier *tools/emaj_tools.env*, lui même appelé par *tools/emaj_tools.profile*.

Le dépôt contient un fichier *tools/emaj_tools.env-dist* qui peut servir de squelette pour créer le fichier *emaj_tools.env*.

Le fichier *emaj_tools.env* doit contenir :

* la liste des versions de PostgreSQL supportées par la version courante d’E-Maj et qui disposent d’une instance PostgreSQL de test (variable *EMAJ_USER_PGVER*),
* pour chaque version d’instance PostgreSQL utilisée pour les tests, 6 variables décrivant la localisation des binaires et du répertoire principal de l’instance, les rôle et port ip à utiliser pour la connexion à l’instance.


Coder
-----

Versionning
^^^^^^^^^^^

La version en cours de développement est nommée *devel*.

Régulièrement, et lorsque cela se justifie, une nouvelle version est créée. Elle porte un nom au format X.Y.Z.

L’outil *create_version* aide à la création de cette version. Il est utilisé uniquement par les mainteneurs d’E-Maj. Son utilisation n’est donc pas décrite ici.


Règles de codage
^^^^^^^^^^^^^^^^

Le codage du script emaj- -devel.sql respecte les règles suivantes :

* structure du script : après quelques contrôles vérifiant que les conditions d’exécution du script sont respectées, les objets sont créés dans l’ordre suivant : rôles, types énumérations, séquences, tables (avec leurs index et leurs contraintes), types composites, paramètres E-Maj, fonctions de bas niveau, fonctions élémentaires de gestion des tables et séquences, fonctions de gestion des groupes de tables, fonctions d’ordre général, triggers sur événements, droits, compléments pour les extensions. Le script se termine par quelques opérations finales.
* tous les objets sont créés dans le schéma *emaj*, à l’exception de la fonction *_emaj_protection_event_trigger_fnct()*, créée dans le schéma *public*,
* les noms des tables, séquences sont préfixés par *emaj_*,
* les noms des fonctions sont préfixés par *emaj_* lorsqu’elles ont une visibilité utilisateur, ou par *_* pour les fonctions internes,
* les tables internes et les fonctions appelables par les utilisateurs doivent avoir un commentaire,
* les mots-clé du langage sont mis en majuscule, les noms d'objets sont en minuscule,
* l’indentation est de 2 caractères espace,
* les lignes ne doivent pas comporter de caractère de tabulation, ne doivent pas dépasser 140 caractères et ne doivent pas se terminer par des espaces,
* dans la structure des fonctions, les délimiteurs du code doivent reprendre le nom de la fonction entouré par un caractère $ (ou *$do$* pour les blocs de code)
* les noms de variables sont préfixés par *v_* pour les variables simples, *p_* pour les paramètres des fonctions ou *r_* pour les variables de type *RECORD*,
* le code doit être compatible avec toutes les versions de PostgreSQL supportées par la version E-Maj courante. Quand cela s’avère strictement nécessaire, le code peut être différencié en fonction de la version de PostgreSQL.

Un script perl, *tools/check_code.pl* permet d’effectuer quelques contrôles sur le formatage du script de création de l’extension. Il permet aussi de détecter les variables inutilisées. Ce script est appelé directement dans les scénarios de tests de non régression.

Script d’upgrade de version
^^^^^^^^^^^^^^^^^^^^^^^^^^^

E-Maj s’installe dans une database comme une extension. L’administrateur E-Maj doit pouvoir facilement :ref:`mettre à jour la version de l’extension<extension_upgrade>`. Un script d’upgrade de l’extension est donc fourni pour chaque version, permettant de passer de la version précédente installée à la version suivante. Le script d’upgrade se nomme *emaj- -<version_précédente>- -devel.sql*.

Quelques règles guident les développements de ce script :

* Développer/maintenir le script d’*upgrade* en même temps que le script principal *emaj- -devel.sql*., de sorte que les tests d’une évolution incluent les cas de changement de version,
* Appliquer les mêmes règles de codage que pour le script principal,
* Autant que faire ce peut, faire en sorte que l’*upgrade* puisse être réalisé sur des groupes de tables actifs (en cours d’enregistrement) sans entamer la capacité à exécuter un *rollback E-Maj* sur une marque antérieure au changement de version.

En début de version, le script d’*upgrade* est bâti à partir d’un squelette (le fichier *tools/emaj_upgrade.template*).

Au fur et à mesure des développements, un script perl permet de synchroniser la création, la modification ou la suppression des fonctions. Il compare le script *emaj- -devel.sql* et le script de création de la version précédente et met à jour le script *emaj- -<version_précédente>- -devel.sql*. Pour son bon fonctionnement, il est essentiel de conserver les 2 balises qui délimitent le début et la fin de la partie de script qui décrit les fonctions.

Après adaptation du paramétrage (voir le fichier *TOOLS/README*), il faut simplement exécuter ::

   perl tools/sync_fct_in_upgrade_script.pl

Les autres parties du script doivent être codées manuellement. Si la structure d’une table interne est modifiée, le contenu de la table doit être migré (les scripts pour les versions antérieures peuvent servir d’exemple).

.. _testing:

Tester
------

L’extension E-Maj, par les fonctions de *rollback*, modifie le contenu des bases de données. La fiabilité du code est donc une caractéristique essentielle. L’attention à porter aux tests est donc tout aussi essentielle.

Créer des instances PostgreSQL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

L’idéal est de pouvoir tester E-Maj avec toutes les versions PostgreSQL supportées par l’extension.

Le script *tools/create_cluster.sh* est une aide à la création des instances de test. On peut s’inspirer de son contenu pour voir les caractéristiques des instances à créer. On peut aussi l’exécuter (après paramétrage comme indiqué dans *tools/README*) ::

   sh tools/create_cluster.sh <version_majeure_PostgreSQL>

Installer les dépendances logicielles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Les tests des clients peut nécessiter l’installation de quelques composants logiciels supplémentaires :

* le logiciel **php** et son interface PostgreSQL,
* le logiciel **perl** avec les modules *DBI* et *DBD::Pg*.

Exécuter les tests de non régression
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Un solide environnement de test est fourni dans le dépôt. Il contient :

* un outil de test,
* des scénarios de tests,
* des résultats attendus.

Les scénarios de test
'''''''''''''''''''''

Le système de test comprend 5 scénarios de test :

* un scénario standard complet,
* le même scénario mais en installant l’extension à partir du script *emaj-devel.sql* fourni pour les cas où une requête "*CREATE EXTENSION emaj*" n’est pas possible,
* le même scénario mais en installant l’extension à partir de la version précédente puis en effectuant un *upgrade* dans la version courante,
* un scénario réduit comportant un *upgrade* de la version précédente vers la version courante de l’extension alors que des groupes de tables sont actifs,
* un scénario réduit similaire au précédent mais avec un *upgrade* depuis la plus ancienne version d’E-Maj disponible avec la plus ancienne version de PostgreSQL supportée.

Ces scénarios font appel à des scripts *psql*, tous localisés dans *test/sql*. Les scripts enchaînent dans différents contextes des séquences d’appels de fonctions E-Maj et de requêtes SQL de préparation et de contrôle des résultats obtenus.

Généralement, en fin de script, des séquences internes sont réinitialisées pour qu’un simple ajout d’un appel de fonction dans le script ne génère pas d’impact dans le résultat des scripts suivants.

Les scripts *psql* de test doivent être maintenus en même temps que le code de l’extension.

Les résultats attendus
''''''''''''''''''''''

Pour chaque script *psql*, l’outil de test génère un fichier résultat. Ces fichiers sont différenciés en fonction de la version de PostgreSQL. Ils sont localisés dans le répertoire *test/<version_postgres>/results*.

En fin d’exécution, l’outil de test compare ces fichiers avec une référence située dans *test/<version_postgres>/expected*. 

Contrairement aux fichiers du répertoire *test/<version_postgres>/results*, les fichiers du répertoire *test/<version_postgres>/expected* font partie du dépôt *git*. Ils doivent être maintenus en cohérence avec le source de l’extension et les scripts *psql*.

L’outil de test
'''''''''''''''

L’outil de test, *regress.sh*, regroupe l’ensemble des fonctions de test. 

Avant de pouvoir l’utiliser, il faut :

* que les instances PostgreSQL de test et le fichier *tools/emaj_tools.env* aient été créés,
* créer manuellement les répertoires *test/<version_postgres>/results*

L’outil de test se lance avec la commande ::

   tools/regress.sh

Comme il commence par copier le fichier *emaj.control* dans les répertoires *SHAREDIR/extension* des versions de PostgreSQL configurées, il peut demander le mot de passe du compte Linux pour exécuter des commandes *sudo*. Au lancement, il génère aussi automatiquement le fichier *emaj-devel.sql*, la version *psql* du script de création de l’extension.

Il affiche ensuite la liste des fonctions de test dans un menu. Il suffit d’indiquer la lettre correspondant au test souhaité.

On trouve :

* les tests standards pour chaque version de PostgreSQL configurée,
* les tests avec installation de la version précédente puis upgrade,
* les tests avec installation de la version par le script *emaj-devel.sql*,
* les tests avec *upgrade* de version E-Maj sur des groupes actifs,
* des tests de sauvegarde de la base par *pg_dump* et restauration, avec des versions de PostgreSQL différentes,
* un test d’*upgrade* de version de PostgreSQL par *pg_upgrade* avec une base contenant l’extension E-Maj.

Il est important d’exécuter ces quatre premières séries de test pour chaque évolution E-Maj.

Valider les résultats
'''''''''''''''''''''

Après avoir exécuté un script *psql*, *regress.sh* compare le résultat obtenu avec le résultat attendu et affiche le résultat de la comparaison sous la forme *'ok'* ou *'FAILED'*.

Voici un exemple d’affichage du déroulement d’un test (ici le scénario avec installation et upgrade de version et avec une différence détectée) ::

	Run regression test
	============== dropping database "regression"         ==============
	DROP DATABASE
	============== creating database "regression"         ==============
	CREATE DATABASE
	ALTER DATABASE
	============== running regression test queries        ==============
	test install_upgrade          ... ok
	test setup                    ... ok
	test create_drop              ... ok
	test start_stop               ... ok
	test mark                     ... ok
	test rollback                 ... ok
	test stat                     ... ok
	test misc                     ... ok
	test verify                   ... ok
	test alter                    ... ok
	test alter_logging            ... ok
	test viewer                   ... ok
	test adm1                     ... ok
	test adm2                     ... ok
	test adm3                     ... ok
	test client                   ... ok
	test check                    ... FAILED
	test cleanup                  ... ok
	
	=======================
	1 of 18 tests failed.
	=======================
	
	The differences that caused some tests to fail can be viewed in the
	file "/home/postgres/proj/emaj/test/18/regression.diffs".  A copy of the test summary that you see
	above is saved in the file "/home/postgres/proj/emaj/test/18/regression.out".

Dans le cas où au moins un script ressort en différence, il convient d’analyser scrupuleusement le contenu du fichier *test/<version_postgres>/regression.diffs* pour vérifier si les écarts sont bien liés aux modifications apportées dans le code source de l’extension ou dans les scripts de test.

Une fois que les écarts relevés sont tous jugés valides, il faut copier le contenu des répertoires *test/<version_postgres>/result* dans *test/<version_postgres>/expected*. Un script *shell* permet de traiter toutes les versions PostgreSQL en une seule commande ::

   sh tools/copy2Expected.sh

Il peut arriver que certains résultats soient en écart à cause d’une différence de fonctionnement de PostgreSQL d’une exécution à une autre. La répétition du test permet alors de détecter ces cas.

Couverture des tests
^^^^^^^^^^^^^^^^^^^^

Couverture de test des fonctions
''''''''''''''''''''''''''''''''

Les clusters PostgreSQL de test sont configurés pour compter les exécutions des fonctions. Le script de test *check.sql* affiche les compteurs d’exécution des fonctions. Il liste aussi les fonctions E-Maj qui n’ont été exécutées dans aucun script.

Couverture de test des messages d’erreur
''''''''''''''''''''''''''''''''''''''''

Un script *perl* extrait les messages d’erreur et de *warning* codés dans le fichier *sql/emaj- -devel.sql*. Il extrait ensuite les messages présents dans les fichiers du répertoire *test/10/expected*. Ceci lui permet d’afficher les cas d’erreur ou de *warning* non couverts par les tests.

Le script s’exécute avec la commande ::

   perl tools/check_error_messages.pl

Certains messages sont connus pour ne pas être couverts (cas d’erreurs difficilement reproductibles par exemple). Ces messages, codés dans le script *perl*, sont exclus de l’affichage final.

Évaluer les performances
^^^^^^^^^^^^^^^^^^^^^^^^

Le répertoire *tools/performance* contient quelques scripts shell permettant de réaliser des mesures de performances. Comme le résultat des mesures est totalement dépendant de la plateforme et de l’environnement utilisés, aucun résultat de référence n’est fourni.

Les scripts couvrent les domaines suivants :

* *dump_changes/dump_changes_perf.sh* mesure les performances des opérations de vidage des mises à jour, avec différents niveaux de consolidation ;
* *large_group/large_group.sh* évalue le fonctionnement de groupes contenant un grand nombre de tables ;
* *log_overhead/pgbench.sh* évalue le surcoût du mécanisme de log, à l’aide de pgbench ;
* *rollback/rollback_perf.sh* évalue les performances des rollbacks E-Maj avec différents profils de tables.

Pour chacun de ces fichiers, des variables sont à configurer en début de scripts,

.. _documenting:

Documenter
----------

Une documentation au format *LibreOffice* est encore gérée par les mainteneurs. Elle dispose de son propre dépôt *github* : *emaj_doc*. De ce fait, le dossier *doc* du dépôt principal reste vide.

La documentation en ligne est gérée avec *sphinx*. Elle est localiséeœ dans le répertoire *docs*.

Pour installer *sphinx*, se référer au fichier *docs/README.rst*.

La documentation existe en deux langues, l’anglais et le français. En fonction de la langue, les sources des documents sont localisés dans */docs/en* et */docs/fr*. Ces documents sont au format *ReStructured Text*.

Pour compiler la documentation dans une langue, se placer dans le répertoire *docs/<langue>* et lancer la commande ::

   make html

Quand il n’y a plus d’erreur de compilation, la documentation peut être visualisée en local sur un navigateur, en ouvrant le fichier *docs/<langue>/_build/html/index.html*.

La mise à jour de la documentation présente sur le site *readthedocs.org* est automatique dès que le dépôt présent sur *github* est mis à jour.

Soumettre un patch
------------------

Tout patch peut être proposé aux mainteneurs d’E-Maj au travers d’un *Pull Request* sur le site *github*.

Avant de soumettre un patch, il peut être utile d’ouvrir une « *issue* » sur *github*, afin d’engager un dialogue avec les mainteneurs et ainsi avancer au mieux dans la réalisation du patch.

Contribuer à Emaj_web
---------------------

Le développement du client web Emaj_web fait l’objet d’un projet à part, bien que fonctionnellement lié à l’extension *emaj*. Des évolutions de l’extension peuvent nécessiter des évolutions du client, notamment :

* lorsque l’API d’utilisation de l’extension change ;
* pour permettre aux utilisateurs du client web de bénéficier de fonctionnalités ajoutées à l’extension.

Dans le premier cas, les évolutions de l’extension et du client doivent être synchronisées.

Le référentiel du projet est maintenu dans le dépôt github : https://github.com/dalibo/emaj_web

Il est important de garder en tête que le client web interface des extensions *emaj* qui peuvent être dans des versions différentes. Le fichier *libraries/version.inc.php* définit les plages de versions utilisables.
