nouveau_contrat_ancien_client = LOAD '/hdfs/staging/out/e1225/anciens_clients' USING PigStorage(';') AS ( mois:chararray, 
cdSi:chararray,
noCtrScr:chararray,
cdFam:chararray,
cdSFam:chararray,
noPse:chararray,
age:int,
cdSex:chararray,
cdInseeCsp:chararray,
noStrGtn:long,
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
noStrGtn:long,
cotMacDo:chararray,
age_Tranche:chararray);

localisation_stru = LOAD '/hdfs/staging/out/e1225/nouveaux_contrats/noGtn_region.csv' USING PigStorage(';') AS
(
noDep : int,
departement : chararray,
region : chararray,
numstr : long,
ville : chararray
);

nouveau_contrat_ancien_client = JOIN nouveau_contrat_ancien_client BY noStrGtn LEFT, localisation_stru BY numstr;
nouveau_contrat_nouveau_client = JOIN nouveau_contrat_nouveau_client BY noStrGtn LEFT, localisation_stru BY numstr;

nouveau_contrat_ancien_client_grp_cdsi = GROUP nouveau_contrat_ancien_client BY (mois,cdSi);
nb_nouveau_contrat_ancien_client_grp_cdsi = FOREACH nouveau_contrat_ancien_client_grp_cdsi GENERATE FLATTEN(group) AS (mois,cdSi), COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_age_Tranche = GROUP nouveau_contrat_ancien_client BY (mois,age_Tranche);
nb_nouveau_contrat_ancien_client_grp_age_Tranche = FOREACH nouveau_contrat_ancien_client_grp_age_Tranche GENERATE FLATTEN(group) AS (mois,age_Tranche), COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_sex = GROUP nouveau_contrat_ancien_client BY (mois,cdSex);
nb_nouveau_contrat_ancien_client_grp_sex = FOREACH nouveau_contrat_ancien_client_grp_sex GENERATE FLATTEN(group) AS (mois,cdSex), COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_cdInseeCsp = GROUP nouveau_contrat_ancien_client BY (mois,cdInseeCsp);
nb_nouveau_contrat_ancien_client_grp_cdInseeCsp = FOREACH nouveau_contrat_ancien_client_grp_cdInseeCsp GENERATE FLATTEN(group) AS (mois,cdInseeCsp), COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_departement = GROUP nouveau_contrat_ancien_client BY (mois,departement);
nb_nouveau_contrat_ancien_client_grp_departement = FOREACH nouveau_contrat_ancien_client_grp_departement GENERATE FLATTEN(group) AS (mois,departement), COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_region = GROUP nouveau_contrat_ancien_client BY (mois,region);
nb_nouveau_contrat_ancien_client_grp_region = FOREACH nouveau_contrat_ancien_client_grp_region GENERATE FLATTEN(group) AS (mois,region), COUNT_STAR(nouveau_contrat_ancien_client) AS nb;

nouveau_contrat_ancien_client_grp_cotMacDo = GROUP nouveau_contrat_ancien_client BY (mois,cotMacDo);
nb_nouveau_contrat_ancien_client_grp_cotMacDo = FOREACH nouveau_contrat_ancien_client_grp_cotMacDo GENERATE FLATTEN(group) AS (mois,cotMacDo), COUNT_STAR(nouveau_contrat_ancien_client) AS nb;



nouveau_contrat_nouveau_client_grp_cdsi = GROUP nouveau_contrat_nouveau_client BY (mois,cdSi);
nb_nouveau_contrat_nouveau_client_grp_cdsi = FOREACH nouveau_contrat_nouveau_client_grp_cdsi GENERATE FLATTEN(group) AS (mois,cdSi), COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_age_Tranche = GROUP nouveau_contrat_nouveau_client BY (mois,age_Tranche);
nb_nouveau_contrat_nouveau_client_grp_age_Tranche = FOREACH nouveau_contrat_nouveau_client_grp_age_Tranche GENERATE FLATTEN(group) AS (mois,age_Tranche), COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_sex = GROUP nouveau_contrat_nouveau_client BY (mois,cdSex);
nb_nouveau_contrat_nouveau_client_grp_sex = FOREACH nouveau_contrat_nouveau_client_grp_sex GENERATE FLATTEN(group) AS (mois,cdSex), COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_cdInseeCsp = GROUP nouveau_contrat_nouveau_client BY (mois,cdInseeCsp);
nb_nouveau_contrat_nouveau_client_grp_cdInseeCsp = FOREACH nouveau_contrat_nouveau_client_grp_cdInseeCsp GENERATE FLATTEN(group) AS (mois,cdInseeCsp), COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_departement = GROUP nouveau_contrat_nouveau_client BY (mois,departement);
nb_nouveau_contrat_nouveau_client_grp_departement = FOREACH nouveau_contrat_nouveau_client_grp_departement GENERATE FLATTEN(group) AS (mois,departement), COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_region = GROUP nouveau_contrat_nouveau_client BY (mois,region);
nb_nouveau_contrat_nouveau_client_grp_region = FOREACH nouveau_contrat_nouveau_client_grp_region GENERATE FLATTEN(group) AS (mois,region), COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_nouveau_client_grp_cotMacDo = GROUP nouveau_contrat_nouveau_client BY (mois,cotMacDo);
nb_nouveau_contrat_nouveau_client_grp_cotMacDo = FOREACH nouveau_contrat_nouveau_client_grp_cotMacDo GENERATE FLATTEN(group) AS (mois,cotMacDo), COUNT_STAR(nouveau_contrat_nouveau_client) AS nb;

nouveau_contrat_ancien_client = ORDER nouveau_contrat_ancien_client BY mois;
nouveau_contrat_nouveau_client = ORDER nouveau_contrat_nouveau_client BY mois;

ancien_client_cdSi = ORDER nb_nouveau_contrat_ancien_client_grp_cdsi BY mois,cdSi;
ancien_client_age_Tranche = ORDER nb_nouveau_contrat_ancien_client_grp_age_Tranche BY mois,age_Tranche;
ancien_client_sex = ORDER nb_nouveau_contrat_ancien_client_grp_sex BY mois,cdSex;
ancien_client_cdInseeCsp = ORDER nb_nouveau_contrat_ancien_client_grp_cdInseeCsp BY mois,cdInseeCsp;
ancien_client_departement = ORDER nb_nouveau_contrat_ancien_client_grp_departement BY mois,departement;
ancien_client_region = ORDER nb_nouveau_contrat_ancien_client_grp_region BY mois,region;
ancien_client_cotMacDo = ORDER nb_nouveau_contrat_ancien_client_grp_cotMacDo BY mois,cotMacDo;

nouveau_client_cdSi = ORDER nb_nouveau_contrat_nouveau_client_grp_cdsi BY mois,cdSi;
nouveau_client_age_Tranche = ORDER nb_nouveau_contrat_nouveau_client_grp_age_Tranche BY mois,age_Tranche;
nouveau_client_sex = ORDER nb_nouveau_contrat_nouveau_client_grp_sex BY mois,cdSex;
nouveau_client_cdInseeCsp = ORDER nb_nouveau_contrat_nouveau_client_grp_cdInseeCsp BY mois,cdInseeCsp;
nouveau_client_departement = ORDER nb_nouveau_contrat_nouveau_client_grp_departement BY mois,departement;
nouveau_client_region = ORDER nb_nouveau_contrat_nouveau_client_grp_region BY mois,region;
nouveau_client_cotMacDo = ORDER nb_nouveau_contrat_nouveau_client_grp_cotMacDo BY mois,cotMacDo;

STORE nouveau_contrat_ancien_client INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_contrat_ancien_client' using PigStorage(';');
STORE nouveau_contrat_nouveau_client INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_contrat_nouveau_client' using PigStorage(';');
/*
STORE ancien_client_cdSi INTO '/hdfs/staging/out/e1225/nouveaux_contrats/ancien_client_cdSi' using PigStorage(';');
STORE ancien_client_age_Tranche INTO '/hdfs/staging/out/e1225/nouveaux_contrats/ancien_client_age_Tranche' using PigStorage(';');
STORE ancien_client_sex INTO '/hdfs/staging/out/e1225/nouveaux_contrats/ancien_client_sex' using PigStorage(';');
STORE ancien_client_cdInseeCsp INTO '/hdfs/staging/out/e1225/nouveaux_contrats/ancien_client_cdInseeCsp' using PigStorage(';');
STORE ancien_client_departement INTO '/hdfs/staging/out/e1225/nouveaux_contrats/ancien_client_departement' using PigStorage(';');
STORE ancien_client_region INTO '/hdfs/staging/out/e1225/nouveaux_contrats/ancien_client_region' using PigStorage(';');
STORE ancien_client_cotMacDo INTO '/hdfs/staging/out/e1225/nouveaux_contrats/ancien_client_cotMacDo' using PigStorage(';');

STORE nouveau_client_cdSi INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_client_cdSi' using PigStorage(';');
STORE nouveau_client_age_Tranche INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_client_age_Tranche' using PigStorage(';');
STORE nouveau_client_sex INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_client_sex' using PigStorage(';');
STORE nouveau_client_cdInseeCsp INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_client_cdInseeCsp' using PigStorage(';');
STORE nouveau_client_departement INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_client_departement' using PigStorage(';');
STORE nouveau_client_region INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_client_region' using PigStorage(';');
STORE nouveau_client_cotMacDo INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nouveau_client_cotMacDo' using PigStorage(';');
*/