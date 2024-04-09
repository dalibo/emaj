Présentation générale d'Emaj_web
================================

Une application web, **Emaj_web**, facilite grandement l'utilisation d'E-Maj.

*Emaj_web* a emprunté à *phpPgAdmin* son infrastructure (browser, barre d’icones, connexion aux bases de données,...) et quelques fonctions utiles telles que la consultation du contenu de tables ou la saisie de requêtes SQL.

Pour les bases de données dans lesquelles l'extension *emaj* a été installée, et si l'utilisateur est connecté avec un rôle qui dispose des autorisations nécessaires, tous les objets E-Maj sont visibles et manipulables.

Il est ainsi possible de :

* créer et constituer les groupes de tables,
* visualiser les groupes de tables et les manipuler, en fonction de leur état (suppression, démarrage, arrêt, pose de marque, rollback, ajout ou modification de commentaire),
* lister les marques posées pour un groupe de tables et effectuer toutes les actions possibles les concernant (suppression, renommage, rollback, ajout ou modification de commentaire),
* obtenir toutes les statistiques sur les changements enregistrés (le contenu des tables de log) et en visualiser le contenu,
* suivre les opérations de rollbacks E-Maj en cours d'exécution et examiner les rollbacks passés,
* vérifier le bon état de l’extension.
