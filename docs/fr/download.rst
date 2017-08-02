Téléchargement et préparation de l'extension
============================================

Téléchargement
**************

E-Maj est disponible en téléchargement sur le site Internet **PGXN** (http://pgxn.org).

E-Maj et ses compléments sont également disponibles sur le site Internet **github.org** :

* Composants sources (https://github.com/beaud76/emaj)
* Documentation (https://github.com/beaud76/emaj_doc)
* Plugin pour phpPgAdmin (https://github.com/beaud76/emaj_ppa_plugin)
* Interface graphique Emaj_web (https://github.com/beaud76/emaj_web)

Décompression
*************

L'extension est fournie sous la forme d'un unique fichier compressé. Pour pouvoir être utilisé, ce fichier doit donc être décompressé.

Sous Windows, vous pouvez utiliser votre utilitaire de décompression préféré (Winzip, 7zip,...). Sous Unix/Linux, une commande du type ::

   tar -xvzf emaj-<version>.tar.gz

peut être utilisée pour un fichier .tar.gz ou ::

   unzip e-maj-<version>.zip

pour un fichier zip.

On dispose maintenant d'un répertoire emaj-<version> comprenant l'arborescence suivante :

+---------------------------------------------+---------------------------------------------------------------------+
| Fichiers                                    | Usage                                                               |
+=============================================+=====================================================================+
| sql/emaj--2.1.0.sql                         | script d’installation de l’extension (vers. 2.1.0)                  |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--2.0.1--2.1.0.sql                  | script d’upgrade de l’extension de 2.0.1 vers 2.1.0                 |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--2.0.0--2.0.1.sql                  | script d’upgrade de l’extension de 2.0.0 vers 2.0.1                 |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--1.3.1--2.0.0.sql                  | script d’upgrade de l’extension de 1.3.1 vers 2.0.0                 |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--unpackaged--1.3.1.sql             | script de transformation en extension d'une version 1.3.1 existante |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.3.0-to-1.3.1.sql                 | script de mise à jour d’une version E-Maj 1.3.0                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.2.0-to-1.3.0.sql                 | script de mise à jour d’une version E-Maj 1.2.0                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.1.0-to-1.2.0.sql                 | script de mise à jour d’une version E-Maj 1.1.0                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.0.2-to-1.1.0.sql                 | script de mise à jour d’une version E-Maj 1.0.2                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.0.1-to-1.0.2.sql                 | script de mise à jour d’une version E-Maj 1.0.1                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.0.0-to-1.0.1.sql                 | script de mise à jour d’une version E-Maj 1.0.0                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-0.11.1-to-1.0.0.sql                | script de mise à jour d’une version E-Maj 0.11.1                    |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-0.11.0-to-0.11.1.sql               | script de mise à jour d’une version E-Maj 0.11.0                    |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj_demo.sql                           | script psql de démonstration d' E-Maj                               |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj_prepare_parallel_rollback_test.sql | script psql de test pour les rollbacks parallélisés                 |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj_uninstall.sql                      | script psql de désinstallation                                      |
+---------------------------------------------+---------------------------------------------------------------------+
| README.md                                   | documentation réduite de l'extension                                |
+---------------------------------------------+---------------------------------------------------------------------+
| CHANGES.md                                  | notes de versions                                                   |
+---------------------------------------------+---------------------------------------------------------------------+
| LICENSE                                     | information sur la licence utilisée pour E-Maj                      |
+---------------------------------------------+---------------------------------------------------------------------+
| AUTHORS                                     | identification des auteurs                                          |
+---------------------------------------------+---------------------------------------------------------------------+
| META.json                                   | données techniques destinées à PGXN                                 |
+---------------------------------------------+---------------------------------------------------------------------+
| emaj.control                                | fichier de contrôle utilisé par la gestion intégrée des extensions  |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_doc_en.pdf               | documentation en anglais de l'extension E-Maj                       |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_doc_fr.pdf               | documentation en français de l'extension E-Maj                      |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_pres.en.pdf              | présentation en anglais de l'extension E-Maj                        |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_pres.fr.pdf              | présentation en français de l'extension E-Maj                       |
+---------------------------------------------+---------------------------------------------------------------------+
| php/emajParallelRollback.php                | client php pour les rollbacks parallélisés                          |
+---------------------------------------------+---------------------------------------------------------------------+
| php/emajRollbackMonitor.php                 | client php pour le suivi des rollbacks                              |
+---------------------------------------------+---------------------------------------------------------------------+

Préparation du fichier emaj.control
***********************************

A partir de la version 2.0.0, l’installation d’E-Maj dans les databases PostgreSQL s’effectue sous la forme d’une *EXTENSION*. 

Mais pour qu’E-Maj soit connu du gestionnaire intégré des extensions, un fichier **emaj.control** doit être positionné dans le répertoire *SHAREDIR* de la version de PostgreSQL.

Pour ce faire :

* Identifier l’emplacement précis du répertoire *SHAREDIR* de votre installation en utilisant la commande shell ::

   pg_config --sharedir

* Copier le fichier emaj.control fourni dans le répertoire racine de la version décompressée vers le répertoire *SHAREDIR*,
* Adapter la directive *directory* du fichier *emaj.control* pour spécifier le répertoire sql contenant les scripts d’installation d’E-Maj.

