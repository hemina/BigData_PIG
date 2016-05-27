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

nouveau_contrat_ancien_client_grp_cdsi = GROUP nouveau_contrat_ancien_client BY cdSi;
nb_nouveau_contrat_ancien_client_grp_cdsi = FOREACH nouveau_contrat_ancien_client_grp_cdsi GENERATE group AS cdSi, COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_age_Tranche = GROUP nouveau_contrat_ancien_client BY age_Tranche;
nb_nouveau_contrat_ancien_client_grp_age_Tranche = FOREACH nouveau_contrat_ancien_client_grp_age_Tranche GENERATE group AS age_Tranche, COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_sex = GROUP nouveau_contrat_ancien_client BY cdSex;
nb_nouveau_contrat_ancien_client_grp_sex = FOREACH nouveau_contrat_ancien_client_grp_sex GENERATE group AS cdSex, COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_cdInseeCsp = GROUP nouveau_contrat_ancien_client BY cdInseeCsp;
nb_nouveau_contrat_ancien_client_grp_cdInseeCsp = FOREACH nouveau_contrat_ancien_client_grp_cdInseeCsp GENERATE group AS cdInseeCsp, COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_noStrGtn = GROUP nouveau_contrat_ancien_client BY noStrGtn;
nb_nouveau_contrat_ancien_client_grp_noStrGtn = FOREACH nouveau_contrat_ancien_client_grp_noStrGtn GENERATE group AS noStrGtn, COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_cotMacDo = GROUP nouveau_contrat_ancien_client BY cotMacDo;
nb_nouveau_contrat_ancien_client_grp_cotMacDo = FOREACH nouveau_contrat_ancien_client_grp_cotMacDo GENERATE group AS cotMacDo, COUNT_STAR(nouveau_contrat_ancien_client) AS nb;



nouveau_contrat_nouveau_client_grp_cdsi = GROUP nouveau_contrat_nouveau_client BY cdSi;
nb_nouveau_contrat_nouveau_client_grp_cdsi = FOREACH nouveau_contrat_nouveau_client_grp_cdsi GENERATE group AS cdSi, COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_age_Tranche = GROUP nouveau_contrat_nouveau_client BY age_Tranche;
nb_nouveau_contrat_nouveau_client_grp_age_Tranche = FOREACH nouveau_contrat_nouveau_client_grp_age_Tranche GENERATE group AS age_Tranche, COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_sex = GROUP nouveau_contrat_nouveau_client BY cdSex;
nb_nouveau_contrat_nouveau_client_grp_sex = FOREACH nouveau_contrat_nouveau_client_grp_sex GENERATE group AS cdSex, COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_cdInseeCsp = GROUP nouveau_contrat_nouveau_client BY cdInseeCsp;
nb_nouveau_contrat_nouveau_client_grp_cdInseeCsp = FOREACH nouveau_contrat_nouveau_client_grp_cdInseeCsp GENERATE group AS cdInseeCsp, COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_noStrGtn = GROUP nouveau_contrat_nouveau_client BY noStrGtn;
nb_nouveau_contrat_nouveau_client_grp_noStrGtn = FOREACH nouveau_contrat_nouveau_client_grp_noStrGtn GENERATE group AS noStrGtn, COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_cotMacDo = GROUP nouveau_contrat_nouveau_client BY cotMacDo;
nb_nouveau_contrat_nouveau_client_grp_cotMacDo = FOREACH nouveau_contrat_nouveau_client_grp_cotMacDo GENERATE group AS cotMacDo, COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;


ancien_client_cdSi = ORDER nb_nouveau_contrat_ancien_client_grp_cdsi BY cdSi;
ancien_client_age_Tranche = ORDER nb_nouveau_contrat_ancien_client_grp_age_Tranche BY age_Tranche;
ancien_client_sex = ORDER nb_nouveau_contrat_ancien_client_grp_sex BY cdSex;
ancien_client_cdInseeCsp = ORDER nb_nouveau_contrat_ancien_client_grp_cdInseeCsp BY cdInseeCsp;
ancien_client_noStrGtn = ORDER nb_nouveau_contrat_ancien_client_grp_noStrGtn BY noStrGtn;
ancien_client_cotMacDo = ORDER nb_nouveau_contrat_ancien_client_grp_cotMacDo BY cotMacDo;

nouveau_client_cdSi = ORDER nb_nouveau_contrat_nouveau_client_grp_cdsi BY cdSi;
nouveau_client_age_Tranche = ORDER nb_nouveau_contrat_nouveau_client_grp_age_Tranche BY age_Tranche;
nouveau_client_sex = ORDER nb_nouveau_contrat_nouveau_client_grp_sex BY cdSex;
nouveau_client_cdInseeCsp = ORDER nb_nouveau_contrat_nouveau_client_grp_cdInseeCsp BY cdInseeCsp;
nouveau_client_noStrGtn = ORDER nb_nouveau_contrat_nouveau_client_grp_noStrGtn BY noStrGtn;
nouveau_client_cotMacDo = ORDER nb_nouveau_contrat_nouveau_client_grp_cotMacDo BY cotMacDo;

STORE ancien_client_cdSi INTO '/hdfs/staging/out/e1225/ancien_client_cdSi' using PigStorage(';');
STORE ancien_client_age_Tranche INTO '/hdfs/staging/out/e1225/ancien_client_age_Tranche' using PigStorage(';');
STORE ancien_client_sex INTO '/hdfs/staging/out/e1225/ancien_client_sex' using PigStorage(';');
STORE ancien_client_cdInseeCsp INTO '/hdfs/staging/out/e1225/ancien_client_cdInseeCsp' using PigStorage(';');
STORE ancien_client_noStrGtn INTO '/hdfs/staging/out/e1225/ancien_client_noStrGtn' using PigStorage(';');
STORE ancien_client_cotMacDo INTO '/hdfs/staging/out/e1225/ancien_client_cotMacDo' using PigStorage(';');

STORE nouveau_client_cdSi INTO '/hdfs/staging/out/e1225/nouveau_client_cdSi' using PigStorage(';');
STORE nouveau_client_age_Tranche INTO '/hdfs/staging/out/e1225/nouveau_client_age_Tranche' using PigStorage(';');
STORE nouveau_client_sex INTO '/hdfs/staging/out/e1225/nouveau_client_sex' using PigStorage(';');
STORE nouveau_client_cdInseeCsp INTO '/hdfs/staging/out/e1225/nouveau_client_cdInseeCsp' using PigStorage(';');
STORE nouveau_client_noStrGtn INTO '/hdfs/staging/out/e1225/nouveau_client_noStrGtn' using PigStorage(';');
STORE nouveau_client_cotMacDo INTO '/hdfs/staging/out/e1225/nouveau_client_cotMacDo' using PigStorage(';');