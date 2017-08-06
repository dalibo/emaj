Désinstallation d'E-Maj d'une base de données
=============================================

Pour désinstaller E-Maj d'une base de données, l'utilisateur doit se connecter à cette base avec *psql*, en tant que super-utilisateur.

Si on souhaite supprimer les **rôles** *emaj_adm* et *emaj_viewer*, il faut au préalable retirer les droits donnés sur ces rôles à d'éventuels autres rôles, à l'aide de requêtes SQL *REVOKE*. ::

   REVOKE emaj_adm FROM <role.ou.liste.de.rôles>;
   REVOKE emaj_viewer FROM <role.ou.liste.de.rôles>;

Si ces rôles *emaj_adm* et *emaj_viewer* possèdent des droits d'accès sur des tables ou autres objets relationnels applicatifs, il faut également supprimer ces droits au préalable à l'aide d'autres requêtes SQL *REVOKE*.

Bien qu'installé avec une requête *CREATE EXTENSION*, E-Maj ne peut se désinstaller par une simple requête *DROP EXTENSION*. Un trigger sur événement bloque d'ailleurs l'exécution d'une telle requête (à partir de PostgreSQL 9.3).

Pour désinstaller E-Maj, il faut simplement exécuter le **script fourni** *emaj_uninstall.sql*. ::

   \i <répertoire_emaj>/sql/emaj_uninstall.sql


Ce script effectue les actions suivantes :

* iI exécute les éventuelles fonctions de nettoyage créées par l'exécution des scripts de démonstration ou de test fournis,
* il arrête les groupes de tables encore actifs, s’il y en a,
* il supprime les groupes de tables existants, supprimant en particulier les triggers sur les tables applicatives,
* il supprime l'extension et le schéma principal *emaj*,
* il supprime les rôles *emaj_adm* et *emaj_viewer* s'ils ne sont pas associés à d'autres rôles ou à d'autres bases de données de l'instance et ne possèdent pas de droits sur d'autres tables. 

L'exécution du script de désinstallation affiche ceci ::

   $ psql ... -f sql/emaj_uninstall.sql 
   >>> Starting E-Maj uninstallation procedure...
   SET
   psql:sql/emaj_uninstall.sql:203: WARNING:  emaj_uninstall: emaj_adm and emaj_viewer roles have been dropped.
   DO
   SET
   >>> E-maj successfully uninstalled from this database

