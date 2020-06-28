Limites d'utilisation
=====================

L'utilisation de l'extension E-Maj présente quelques limitations.

* La **version PostgreSQL** minimum requise est la version 9.5.
* Toutes les tables appartenant à un groupe de tables de type "*rollbackable*" doivent avoir une **clé primaire** explicite (*PRIMARY KEY*).
* Les tables *UNLOGGED* ou *WITH OIDS* ne peuvent pas appartenir à un groupe de tables de type "*rollbackable*".
* Les tables temporaires (*TEMPORARY*) ne sont pas gérées par E-Maj
* L'utilisation d'une séquence globale pour une base de données induit une limite dans le nombre de mises à jour qu'E-Maj est capable de tracer tout au long de sa vie. Cette limite est égale à 2^63, soit environ 10^19 (mais seulement d'environ 10^10  sur de vieilles plate-formes). Cela permet tout de même d'enregistrer 10 millions de mises à jour par seconde (soit 100 fois les meilleurs performances des benchmarks en 2012) pendant … 30.000 ans (et dans le pire des cas, 100 mises à jour par seconde pendant 5 ans). S'il s'avérait nécessaire de réinitialiser cette séquence, il faudrait simplement désinstaller puis réinstaller l'extension E-Maj.
* Si une **opération de DDL** est exécutée sur une table applicative appartenant à un groupe de tables, il n'est pas possible pour E-Maj de remettre la table dans un état antérieur. (plus de détails :doc:`ici <alterGroups>`)
