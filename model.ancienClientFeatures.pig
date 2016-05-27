nouveau_contrat_ancien_client = LOAD '/hdfs/staging/out/e1225/model_anciens_clients' USING PigStorage(';') AS ( mois:chararray, 
cdSi:chararray,
noCtrScr:chararray,
noPse:chararray,
age:int,
cdSex:chararray,
cdInseeCsp:chararray,
noStrGtn:chararray,
cotMacDo:chararray,
PNB:double, 
PNB_TOUS_CONTRATS:double,
cdSFam:chararray);

--pour vérifier que la plus part de client(99.39%) n'ont qu'un seul nouveau contrat par sous famille 
/*
nouveau_contrat_ancien_client_grp = GROUP nouveau_contrat_ancien_client BY (cdSi,noPse,age,cdSex,cdInseeCsp,noStrGtn,cotMacDo,PNB_TOUS_CONTRATS,cdSFam);

nb_contrats_SFam = FOREACH nouveau_contrat_ancien_client_grp GENERATE FLATTEN(group) AS (cdSi,noPse,age,cdSex,cdInseeCsp,noStrGtn,cotMacDo,PNB_TOUS_CONTRATS,cdSFam),
(COUNT_STAR(nouveau_contrat_ancien_client)==1?0:COUNT_STAR(nouveau_contrat_ancien_client)) AS nb_contrats;

nb_contrats_SFam_grp = GROUP nb_contrats_SFam BY nb_contrats;
--resultat pour montrer si on peut ignorer les nouveaux contrats de la même sous famille qui viennent du même ancien client
result = FOREACH nb_contrats_SFam_grp GENERATE group AS nb_contrats, COUNT(nb_contrats_SFam) AS nb;
DUMP result;
*/

--pour obtenir les étiquettes de sous famille 
--------------------------------------------------------------------------------------------------------------------
--enlever les cas d'un client avec plusieurs contrats dans la même sous famille
nouveau_contrat_ancien_client_grp = GROUP nouveau_contrat_ancien_client BY (cdSi,noPse,age,cdSex,cdInseeCsp,noStrGtn,cotMacDo,PNB_TOUS_CONTRATS);

nouveau_contrat_ancien_client_SFam = FOREACH nouveau_contrat_ancien_client_grp GENERATE FLATTEN(group) AS (cdSi,noPse,age,cdSex,cdInseeCsp,noStrGtn,cotMacDo,PNB_TOUS_CONTRATS), 
nouveau_contrat_ancien_client.cdSFam AS cdSFam;

nouveau_contrat_ancien_client_SFam = DISTINCT nouveau_contrat_ancien_client_SFam;
STORE nouveau_contrat_ancien_client_SFam INTO '/hdfs/staging/out/e1225/model_nouveau_contrat_ancien_client_SFam' using PigStorage(';');
-------------------------TBC
nouveau_contrat_ancien_client_SFam_tous = FOREACH (GROUP nouveau_contrat_ancien_client_SFam BY (cdSi,noPse)) GENERATE FLATTEN(group) AS (cdSi,noPse), BagToString(nouveau_contrat_ancien_client_SFam.cdSFam) AS cdSFamTous;
--STORE nouveau_contrat_ancien_client_SFam_tous INTO '/hdfs/staging/out/e1225/model_nouveau_contrat_ancien_client_SFam_tous' using PigStorage(';');

cdSFamTous_grp = GROUP nouveau_contrat_ancien_client_SFam_tous BY cdSFamTous;
nb_cdSFamTous = FOREACH cdSFamTous_grp GENERATE group AS cdSFamTous, COUNT_STAR(nouveau_contrat_ancien_client_SFam_tous) AS nb;
STORE nb_cdSFamTous INTO '/hdfs/staging/out/e1225/model_nb_cdSFamTous' using PigStorage(';');
