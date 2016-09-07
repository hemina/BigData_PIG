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

-- UDF pour calculer la probabilité d'actif mensuelle de manière récursive
DEFINE PROBA_ACTIF com.arkea.valeurinstantanee.udfs.ProbaActifAmortissables(); 

--Calcul de la marge et du taux de marge
DEFINE CALCUL_MARGE_AMORTISSABLE com.arkea.valeurinstantanee.udfs.CalculerMargeAmortissable();

-- UDF qui met à plat le paramétrage pour éviter les correspondances multiples et filter par interval
DEFINE INTERVAL com.arkea.valeurinstantanee.udfs.Interval(); 

-- UDF pour lire csv sans header
--DEFINE CSVExcelStorage com.arkea.valeurinstantanee.udfs.CSVExcelStorage();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

-- UDF pour calculer le nombre de mois entre deux dates
DEFINE MONTHS_BETWEEN com.arkea.valeurinstantanee.udfs.MonthsBetween(); 


-- Load data
-- ******************************************************************************** --
-- Récupération des contrats stock du mois M+i
-- ******************************************************************************** --
Contrat = LOAD '/hdfs/staging/out/e1225/credit/M+2' USING PigStorage(';') AS --$i 
(	
	mois:chararray,
	noClient:long,
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
	reseau:chararray, --CMB..
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

-- ******************************************************************************** --
-- Récupération de 09.07.REF.ClientsProbaVivant
-- ******************************************************************************** --
Client = LOAD --'/hdfs/staging/out/e1225/Clients' USING PigStorage(';') AS
'/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151231/09.07.REF.ClientsProbaVivant.20160517-154019/' USING PigStorage(';') AS 
(	
periodeTraitee:chararray,
cdSi:chararray,
noPse:long,
daNai:chararray,
cdSex:chararray,
cdInseeCsp:chararray,
noStr:chararray,
cotMacDo:chararray,
codeReseau:chararray,
libReseau:chararray,--CMB...
noPseMaitre:chararray,
moisProjRelatif:int,
moisProjAbsolu:chararray,
age:int,
probaVivant:double
);

-- Jointure pour obtenir la probaVivant et le code EFS du client (age)
Contrat_Client = JOIN Contrat BY (reseau, noClient, mois), Client BY (libReseau, noPse, moisProjAbsolu);

-- Jointure pour obtenir le code sous famille du contrat
Contrat_sf = JOIN cdsfam BY LIB_SSF_VI, Contrat_Client BY sfamCredit;

Contrat1 = FOREACH Contrat_sf GENERATE 
mois AS mois,
noClient AS noClient,
Contrat::daNai AS daNai,
Contrat::cdSex AS cdSex,
noCtrScr AS noCtrScr, --'1'
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
probaVivant AS probaVivant,
0 AS moisR, --($moisRelatif - $i)
codeReseau AS cdEfs,
CD_SSF_VI AS cdSFam,
(indicateurEligible==1 ? 0.0025 : 0.0) AS txMinoSFH
;

contrat_taux_prod = JOIN taux_evol_prod BY (mois, cdSFam), Contrat1 BY (mois, cdSFam);

contrat_taux_client = JOIN taux_evol_client BY (mois, cdSFam), contrat_taux_prod BY (Contrat1::mois, Contrat1::cdSFam);

contrat_tci = JOIN tci BY (mois, cdSFam), contrat_taux_client BY (Contrat1::mois, Contrat1::cdSFam);

FOREACH contrat_tci GENERATE taux_evol_prod::taux AS taux_evol_prod, 
taux_evol_client::taux AS taux_evol_client, 
tci::taux AS tci, 
Contrat1::mois AS mois, 
Contrat1::cdSFam,
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
probaVivant AS probaVivant,
0 AS moisR, --($moisRelatif - $i)
txMinoSFH AS txMinoSFH
;

FLATTEN(PROBA_ACTIF(PARAM_ATTRITION_CREDITS_GRP::PARAM_ATTRITION_CREDITS,cdEfs,tyTx,cdSFam,daNai,(int)maxAge,'$PERIODE_TRAITEE',moisProjRelatif)) AS (txAttr:double,probaActif:double);

FLATTEN(CALCUL_MARGE_AMORTISSABLE(cdSFam,tci,txMargeDft,txBoni,txMinoSFH,txCli,spread,spreadLiquidite,encours,tyTx)) AS (txMarge:double,marge:double,topTxMargeDft:int),

;


-- Filtrer les contrats non expirés
ContratMoisProch = FILTER Contrat1 BY ifExpireFlag==TRUE;
STORE ContratMoisProch INTO '/hdfs/staging/out/e1225/credit/M+$output' using PigStorage(';');
STORE check_tab3 INTO '/hdfs/staging/out/e1225/credit/check_tab_M+$output' using PigStorage(';');
