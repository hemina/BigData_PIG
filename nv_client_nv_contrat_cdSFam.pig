nouveau_contrat_ancien_client = LOAD '/hdfs/staging/out/e1225/anciens_clients' USING PigStorage(';') AS ( mois:chararray, 
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

nouveau_contrat_nouveau_client = LOAD '/hdfs/staging/out/e1225/nouveaux_clients' USING PigStorage(';') AS ( mois:chararray, 
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

nouveau_contrat_ancien_client_grpSFam = GROUP nouveau_contrat_ancien_client BY cdSFam;
nouveau_contrat_ancien_client_nb_grp_SFam = FOREACH nouveau_contrat_ancien_client_grpSFam GENERATE group AS cdSFam, COUNT_STAR( nouveau_contrat_ancien_client ) AS nb;

nouveau_contrat_ancien_client_grpFam = GROUP nouveau_contrat_ancien_client BY cdFam;
nouveau_contrat_ancien_client_nb_grp_Fam = FOREACH nouveau_contrat_ancien_client_grpFam GENERATE group AS cdFam, COUNT_STAR( nouveau_contrat_ancien_client ) AS nb;


nouveau_contrat_nouveau_client_grpSFam = GROUP nouveau_contrat_nouveau_client BY cdSFam;
nouveau_contrat_nouveau_client_nb_grp_SFam = FOREACH nouveau_contrat_nouveau_client_grpSFam GENERATE group AS cdSFam, COUNT_STAR( nouveau_contrat_nouveau_client ) AS nb;

nouveau_contrat_nouveau_client_grpFam = GROUP nouveau_contrat_nouveau_client BY cdFam;
nouveau_contrat_nouveau_client_nb_grp_Fam = FOREACH nouveau_contrat_nouveau_client_grpFam GENERATE group AS cdFam, COUNT_STAR( nouveau_contrat_nouveau_client ) AS nb;


nouveau_contrat_ancien_client_nb_grp_SFam = ORDER nouveau_contrat_ancien_client_nb_grp_SFam BY cdSFam;
nouveau_contrat_ancien_client_nb_grp_Fam = ORDER nouveau_contrat_ancien_client_nb_grp_Fam BY cdFam;

nouveau_contrat_nouveau_client_nb_grp_SFam = ORDER nouveau_contrat_nouveau_client_nb_grp_SFam BY cdSFam;
nouveau_contrat_nouveau_client_nb_grp_Fam = ORDER nouveau_contrat_nouveau_client_nb_grp_Fam BY cdFam;


STORE nouveau_contrat_ancien_client_nb_grp_SFam INTO '/hdfs/staging/out/e1225/nouveau_contrat_ancien_client_nb_grp_SFam' using PigStorage(';');
STORE nouveau_contrat_ancien_client_nb_grp_Fam INTO '/hdfs/staging/out/e1225/nouveau_contrat_ancien_client_nb_grp_Fam' using PigStorage(';');

STORE nouveau_contrat_nouveau_client_nb_grp_SFam INTO '/hdfs/staging/out/e1225/nouveau_contrat_nouveau_client_nb_grp_SFam' using PigStorage(';');
STORE nouveau_contrat_nouveau_client_nb_grp_Fam INTO '/hdfs/staging/out/e1225/nouveau_contrat_nouveau_client_nb_grp_Fam' using PigStorage(';');