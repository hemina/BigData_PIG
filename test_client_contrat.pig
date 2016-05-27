nouveau_contrat = LOAD '/hdfs/staging/out/e1225/contrats_entrants' USING PigStorage(';') AS ( mois:chararray, 
cdSi:chararray,
noCtrScr:chararray,
cdFam:chararray,
cdSFam:chararray,
noPse:chararray,
age:int,
cdSex:chararray,
cdInseeCsp:chararray,
noStrGtn:chararray,
cotMacDo:chararray,
age_Tranche:chararray);

nouveau_client = LOAD '/hdfs/staging/out/e1225/clients_entrants' USING PigStorage(';') AS ( mois:chararray, 
cdSi:chararray,
noCtrScr:chararray,
noPse:chararray,
age:int,
cdSex:chararray,
cdInseeCsp:chararray,
noStrGtn:chararray,
cotMacDo:chararray,
age_Tranche:chararray);

tous_nouveaux_contrats = JOIN nouveau_contrat BY (cdSi, noPse) LEFT OUTER, nouveau_client BY (cdSi, noPse);

SPLIT tous_nouveaux_contrats INTO 
anciens_clients IF nouveau_client::cdSi IS NULL,
nouveaux_clients IF nouveau_client::cdSi IS NOT NULL;

anciens_clients_grp = GROUP anciens_clients BY nouveau_contrat::mois;
nouveaux_clients_grp = GROUP nouveaux_clients BY nouveau_client::mois;

anciens_clients_nb = FOREACH anciens_clients_grp GENERATE group AS mois, COUNT(anciens_clients) AS nb;
nouveaux_clients_nb = FOREACH nouveaux_clients_grp GENERATE group AS mois, COUNT(nouveaux_clients) AS nb;

anciens_clients = ORDER anciens_clients BY nouveau_contrat::mois;
nouveaux_clients = ORDER nouveaux_clients BY nouveau_client::mois;

anciens_clients_nb = ORDER anciens_clients_nb BY mois;
nouveaux_clients_nb = ORDER nouveaux_clients_nb BY mois;

STORE anciens_clients INTO '/hdfs/staging/out/e1225/anciens_clients' using PigStorage(';');
STORE nouveaux_clients INTO '/hdfs/staging/out/e1225/nouveaux_clients' using PigStorage(';');

STORE anciens_clients_nb INTO '/hdfs/staging/out/e1225/anciens_clients_nb' using PigStorage(';');
STORE nouveaux_clients_nb INTO '/hdfs/staging/out/e1225/nouveaux_clients_nb' using PigStorage(';');