Désinstallation
===============

Suppression d'E-Maj d'une base de données
*****************************************

Pour supprimer E-Maj d'une base de données, l'utilisateur doit se connecter à cette base avec *psql*, en tant que super-utilisateur.

Si on souhaite supprimer les **rôles** *emaj_adm* et *emaj_viewer*, il faut au préalable retirer les droits donnés sur ces rôles à d'éventuels autres rôles, à l'aide de requêtes SQL *REVOKE*. ::

   REVOKE emaj_adm FROM <role.ou.liste.de.rôles>;
   REVOKE emaj_viewer FROM <role.ou.liste.de.rôles>;

Si ces rôles *emaj_adm* et *emaj_viewer* possèdent des droits d'accès sur des tables ou autres objets relationnels applicatifs, il faut également supprimer ces droits au préalable à l'aide d'autres requêtes SQL *REVOKE*.

Bien qu'installée en standard avec une requête *CREATE EXTENSION*, l’extension *emaj* ne peut être supprimée par une simple requête *DROP EXTENSION*. Un trigger sur événement bloque d'ailleurs l'exécution d'une telle requête.

Quelle que soit la façon dont l’extension *emaj* a été installée (en standard, par une requête *CREATE EXTENSION*, ou par l’exécution d’un script *psql* lorsque l’ajout d’une nouvelle extension n’est pas possible), sa suppression s’effectue par la simple fonction *emaj_drop_extension()* ::

   SELECT emaj.emaj_drop_extension();

Cette fonction effectue les actions suivantes :

* elle exécute les éventuelles fonctions de nettoyage créées par l'exécution des scripts de démonstration ou de test fournis,
* elle arrête les groupes de tables encore actifs, s’il y en a,
* elle supprime les groupes de tables existants, supprimant en particulier les triggers sur les tables applicatives,
* elle supprime l'extension et le schéma principal *emaj*,
* elle supprime les rôles *emaj_adm* et *emaj_viewer* s'ils ne sont pas associés à d'autres rôles ou à d'autres bases de données de l'instance et ne possèdent pas de droits sur d'autres tables. 

Dans les versions d’E-Maj 4.4.0 et antérieures cette suppression de l’extension *emaj* s’effectuait par l’exécution du script *sql/emaj_uninstall.sql*. Bien que dépréciée, cette façon de procéder est toujours possible.

Désinstallation du logiciel E-Maj
*********************************

Le mode de désinstallation du logiciel E-Maj dépend de son mode d’installation.

Pour une installation standard avec le client *pgxn*, une seule commande est requise ::

  pgxn uninstall E-Maj --sudo

Pour une installation standard sans le client *pgxn*, se placer dans le répertoire initial de la distribution E-Maj et taper ::

  sudo make uninstall

Pour une installation manuelle, il convient de supprimer les composants installés en annulant les opérations exécutées lors de l’installation.
