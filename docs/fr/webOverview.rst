Présentation générale des clients web
=====================================

Pour faciliter l'utilisation d'E-Maj, deux applications web sont disponibles :

* un « **plug-in** » pour l'outil d'administration **phpPgAdmin**, dans ses versions 5.1 et suivantes,
* une application web indépendante, **Emaj_web**.

Les deux clients web offrent les mêmes fonctionnalités autour d’E-Maj, avec une interface utilisateur similaire.

*Emaj_web* empreinte à *phpPgAdmin* son infrastructure (browser, barre d’icones, connexion aux bases de données,...) et quelques fonctions utiles telles que la consultation du contenu de tables ou la saisie de requêtes SQL.

Pour les bases de données dans lesquelles l'extension E-Maj a été installée, et si l'utilisateur est connecté avec un rôle qui dispose des autorisations nécessaires, tous les objets E-Maj sont visibles et manipulables.

Il est ainsi possible de :

* définir ou modifier la composition des groupes,
* voir la liste des groupes de tables et effectuer toutes les actions possibles, en fonction de l'état du groupe (création, suppression, démarrage, arrêt,  pose de marque, rollback, ajout ou modification de commentaire),
* voir la liste des marques posées pour un groupe de tables et effectuer toutes les actions possibles les concernant (suppression, renommage, rollback, ajout ou modification de commentaire),
* obtenir toutes les statistiques sur le contenu des tables de log et en visualiser le contenu,
* suivre les opérations de rollbacks en cours d'exécution.

