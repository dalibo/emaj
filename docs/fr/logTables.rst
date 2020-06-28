Structure des tables de log
===========================

.. _logTableStructure:

Structure standard
------------------

Les tables de log ont une structure qui découle directement des tables applicatives dont elles enregistrent les mises à jour. Elles contiennent les mêmes colonnes avec les mêmes types. Mais elles possèdent aussi quelques colonnes techniques complémentaires :

* emaj_verb : type de verbe SQL ayant généré la mise à jour (*INS*, *UPD*, *DEL*, *TRU*)
* emaj_tuple : version des lignes (*OLD* pour les *DEL*, *UPD* et *TRU* ; *NEW* pour *INS* et *UPD* ; NULL pour les événements *TRUNCATE*)
* emaj_gid : identifiant de la ligne de log
* emaj_changed : date et heure de l'insertion de la ligne dans la table de log
* emaj_txid : identifiant de la transaction à l'origine de la mise à jour (*txid* PostgreSQL)
* emaj_user : rôle de connexion à l'origine de la mise à jour

Lorsqu’une requête SQL *TRUNCATE* est exécutée sur une table, chaque ligne présente dans la table est enregistrée (avec *emaj_verb = TRU* et *emaj_tuple = OLD*). Une ligne est ajoutée avec *emaj_verb = TRU*, les colonnes de la table source et *emaj_tuple* étant positionnées à NULL. Cette ligne est utilisée pour la génération de scripts SQL.

.. _addLogColumns:

Ajouter des colonnes techniques
-------------------------------
Il est possible d’ajouter une ou plusieurs colonnes techniques pour enrichir les traces. Ces colonnes doivent doivent être valorisées avec une valeur par défaut (clause *DEFAULT*) associée à une fonction (pour que les triggers de logs ne soient pas impactés).

Pour ajouter une ou plusieurs colonnes techniques, il faut ajouter le paramètre de clé « alter_log_table » dans la table :ref:`emaj_param<emaj_param>`. La valeur texte associée doit contenir une clause d’*ALTER TABLE*. Lors de la création d’une table de log, si le paramètre existe, une requête *ALTER TABLE* avec ce paramètre est exécutée.

Par exemple, on peut ajouter dans les tables de log une colonne pour enregistrer la valeur du champ de connexion *application_name* de la manière suivante ::

   INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('alter_log_table',
     'ADD COLUMN extra_col_appname TEXT DEFAULT current_setting(''application_name'')');

Plusieurs directives *ADD COLUMN* peuvent être concaténées, séparées par une virgule. Par exemple pour créer des colonnes enregistrant l’adresse ip et le port du client connecté ::

   INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('alter_log_table',
	'ADD COLUMN emaj_user_ip INET DEFAULT inet_client_addr(),
	 ADD COLUMN emaj_user_port INT DEFAULT inet_client_port()');

Pour changer la structure de tables de log existantes après valorisation ou modification du paramètre alter_log_table, les groupes de tables doivent être supprimés puis recréés.
