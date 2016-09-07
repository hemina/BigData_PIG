-- DESCRIPTION : Fichier pour gérer la projection des contrats sur n mois futurs

-- DATE DE CREATION VERSION V1 : 16/06/2016
-- AUTEUR: Mina HE
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Current
-- Bibliotheques Java 
--
-- HADOOP V2
-- Chargement des librairies HADOOP V2. Récupération de la dernière version sur la cellule HADOOP
REGISTER /opt/arkea/lib/com.arkea.commons.cobol.jar;
REGISTER /opt/arkea/lib/com.arkea.commons.hadoop.jar;
REGISTER /opt/arkea/lib/com.arkea.commons.pig.jar;

-- Chargement des librairies propres au projet
REGISTER /home/e1225/ValeurClient/lib/copybook.jar; -- Contient les copy COBOL
REGISTER /home/e1225/ValeurClient/lib/db2-metadata.jar; -- Contient les DDL et les LOAD pour les tables DB2
REGISTER /home/e1225/ValeurClient/lib/cb2xml.jar; -- Utilisé pour la lecture des fichiers COBOL
REGISTER /home/e1225/ValeurClient/lib/groovy-all.jar; -- Permet l'utilisation des UDFS Groovy
REGISTER /home/e1225/ValeurClient/lib/com.arkea.valeurClient.bigdata.jar; -- Contient les UDFS Java du projet
REGISTER /home/e1225/ValeurClient/lib/com.arkea.commons.piggybank.jar; -- Package Arkéa contenant les UDFS fréquemment utilisées (Formattage, conversion ...)

DEFINE monthInc minaUDFS.month_inc();
DEFINE changeDateFormat minaUDFS.changeDateFormat();
DEFINE genNoCtrScr minaUDFS.genNoCtrScr();
DEFINE ifExpire minaUDFS.ifExpire();

--Calcul de la marge et du taux de marge
DEFINE CALCUL_MARGE_AMORTISSABLE com.arkea.valeurinstantanee.udfs.CalculerMargeAmortissable();

-- Load data
Contrat = LOAD '/hdfs/staging/out/e1225/credit0.csv' USING PigStorage(';') AS 
(	
	mois:chararray,
	noClient:chararray,
	daNai:chararray,
	cdSex:chararray,
	noCtrScr:chararray,
	nbClients:int,
	famCredit:chararray,
	sfamCredit:chararray, 
	daPreDeblocage:chararray, 
	daFinCredit:chararray, 
	typeTaux:chararray,
	tauxClient:double,
	tauxBonification:double,
	indicateurEligible:double, --à vérifier
	mtNominal:double, --montant nominal
	mtDebloque:double, --montant débloqué
	mtCommission:double, --montant Commission apporteur
	mtCoc:double, --montant CoC
	reseau:chararray,
	encours0:double,
	encours1:double,
	encours2:double,
	encours3:double,
	encours4:double,
	encours5:double,
	encours6:double,
	encours7:double,
	encours8:double,
	encours9:double,
	encours10:double,
	encours11:double,
	encours12:double,
	encours13:double,
	encours14:double,
	encours15:double,
	encours16:double,
	encours17:double,
	encours18:double,
	encours19:double,
	encours20:double,
	encours21:double,
	encours22:double,
	encours23:double,
	encours24:double
);

-- ******************************************************************************** --
-- Récupération de code sous famille
-- ******************************************************************************** --
cdsfam = LOAD '/hdfs/staging/out/e1225/credit_cdSF.csv' USING PigStorage(';') AS
(
CD_SSF_VI: chararray,
LIB_SSF_VI: chararray,
CD_FML_VI: chararray,
LIB_FML_VI: chararray,
CD_DMN_VI: chararray,
LIB_DMN_VI: chararray
);


-- ******************************************************************************** --
-- Récupération de taux
-- ******************************************************************************** --
taux_evol_prod = LOAD '/hdfs/staging/out/e1225/taux_prod.csv' USING PigStorage(';') AS
(
mois: chararray,
cdSFam: chararray,
taux: double
); 

taux_evol_client = LOAD '/hdfs/staging/out/e1225/taux_cli.csv' USING PigStorage(';') AS
(
mois: chararray,
cdSFam: chararray,
taux: double
); 

tci = LOAD '/hdfs/staging/out/e1225/tci.csv' USING PigStorage(';') AS
(
mois: chararray,
cdSFam: chararray,
taux: double
); 

--
--taux = LOAD '/hdfs/staging/out/e1225/taux_evol.csv' USING PigStorage(';') AS 
--(	
--	month_i:chararray,
--	date:chararray,
--	tauxEvolProd:double,
--	tauxClient_t:double
--);
--
--taux1 = FOREACH taux GENERATE 
--month_i AS month_i,
--changeDateFormat(date) AS date,
--tauxEvolProd AS tauxEvolProd,
--tauxClient_t AS tauxClient_t
--;

Contrat1 = FOREACH Contrat GENERATE 
changeDateFormat(mois) AS mois,
noClient AS noClient,
changeDateFormat(daNai) AS daNai,
cdSex AS cdSex,
noCtrScr AS noCtrScr, --'1'
nbClients AS nbClients,
famCredit AS famCredit,
sfamCredit AS sfamCredit, 
changeDateFormat(daPreDeblocage) AS daPreDeblocage,
changeDateFormat(daFinCredit) AS daFinCredit,
typeTaux AS typeTaux,
tauxClient AS tauxClient,
tauxBonification AS tauxBonification,
indicateurEligible AS indicateurEligible,
mtNominal AS mtNominal,
mtDebloque AS mtDebloque,
mtCommission AS mtCommission,
mtCoc AS mtCoc,
reseau AS reseau,
encours0 AS encours0,
encours1 AS encours1,
encours2 AS encours2,
encours3 AS encours3,
encours4 AS encours4,
encours5 AS encours5,
encours6 AS encours6,
encours7 AS encours7,
encours8 AS encours8,
encours9 AS encours9,
encours10 AS encours10,
encours11 AS encours11,
encours12 AS encours12,
encours13 AS encours13,
encours14 AS encours14,
encours15 AS encours15,
encours16 AS encours16,
encours17 AS encours17,
encours18 AS encours18,
encours19 AS encours19,
encours20 AS encours20,
encours21 AS encours21,
encours22 AS encours22,
encours23 AS encours23,
encours24 AS encours24,
(indicateurEligible==1 ? 0.0025 : 0.0) AS txMinoSFH
;


-- Jointure pour obtenir le code sous famille du contrat
Contrat_sf = JOIN cdsfam BY LIB_SSF_VI, Contrat1 BY sfamCredit;

contrat_taux_prod = JOIN taux_evol_prod BY (mois, cdSFam), Contrat_sf BY (mois, CD_SSF_VI);

contrat_taux_client = JOIN taux_evol_client BY (mois, cdSFam), contrat_taux_prod BY (Contrat1::mois, cdsfam::CD_SSF_VI);

contrat_tci = JOIN tci BY (mois, cdSFam), contrat_taux_client BY (Contrat1::mois, cdsfam::CD_SSF_VI);

Contrat2 = FOREACH contrat_tci GENERATE 
Contrat1::mois AS mois, 
noClient AS noClient,
daNai AS daNai,
cdSex AS cdSex,
noCtrScr AS noCtrScr, 
nbClients AS nbClients,
famCredit AS famCredit,
sfamCredit AS sfamCredit, 
daPreDeblocage AS daPreDeblocage,
daFinCredit AS daFinCredit,
typeTaux AS typeTaux,
tauxClient AS tauxClient,
tauxBonification AS tauxBonification,
indicateurEligible AS indicateurEligible,
mtNominal AS mtNominal,
mtDebloque AS mtDebloque,
mtCommission AS mtCommission,
mtCoc AS mtCoc,
reseau AS reseau,
encours0 AS encours0,
encours1 AS encours1,
encours2 AS encours2,
encours3 AS encours3,
encours4 AS encours4,
encours5 AS encours5,
encours6 AS encours6,
encours7 AS encours7,
encours8 AS encours8,
encours9 AS encours9,
encours10 AS encours10,
encours11 AS encours11,
encours12 AS encours12,
encours13 AS encours13,
encours14 AS encours14,
encours15 AS encours15,
encours16 AS encours16,
encours17 AS encours17,
encours18 AS encours18,
encours19 AS encours19,
encours20 AS encours20,
encours21 AS encours21,
encours22 AS encours22,
encours23 AS encours23,
encours24 AS encours24,
txMinoSFH AS txMinoSFH,
cdsfam::CD_SSF_VI AS cdSFam,
FLATTEN(CALCUL_MARGE_AMORTISSABLE(cdsfam::CD_SSF_VI,tci::taux,0.0,tauxBonification,txMinoSFH,tauxClient,0.0,0.0,0.0,typeTaux)) AS (txMarge:double,marge:double,topTxMargeDft:int)
----1.0 AS probaVivant,
--taux_evol_prod::taux AS taux_evol_prod, 
--taux_evol_client::taux AS taux_evol_client, 
--tci::taux AS tci
;


STORE Contrat2 INTO '/hdfs/staging/out/e1225/credit/M+0' using PigStorage(';');

--STORE taux1 INTO '/hdfs/staging/out/e1225/credit/taux' using PigStorage(';');
