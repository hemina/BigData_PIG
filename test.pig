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
Contrat = LOAD '/hdfs/staging/out/e1225/credit/M*' USING PigStorage(';') AS --$i 
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


-- ******************************************************************************** --
--   Récupération du paramétrage global
-- ******************************************************************************** --
--Code attribut;Libellé attribut;Valeur attribut;Catégorie
PARAM_GLOBAL = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PARAM/PARAM_GLOBAL.20160725_152621_7' USING PigStorage(';') AS(
CD_ATTR:chararray,LIB_ATTR:chararray,VAL_ATTR:chararray,CATEGORIE:chararray);

PARAM_GLOBAL = FOREACH PARAM_GLOBAL GENERATE CD_ATTR,VAL_ATTR;

AGE_MX_VI = FOREACH PARAM_GLOBAL GENERATE CD_ATTR,VAL_ATTR AS maxAge,1 AS key;
AGE_MX_VI = FILTER AGE_MX_VI BY CD_ATTR MATCHES 'AGE_MX_VI';


-- ******************************************************************************** --
-- Récupération du paramétrage des attritions crédits
-- ******************************************************************************** --
PARAM_ATTRITION_CREDITS = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PARAM/ATTRITION_CREDITS.20160725_152621_7' USING PigStorage(';') AS (
CD_EFS:chararray,
CD_SSF_VI:chararray,
LIB_SSF_VI:chararray,
CD_TY_TX:chararray,
AGE_MIN:int,
AGE_MX:int,
TX_ATT_VI:double
);

PARAM_ATTRITION_CREDITS_GRP = GROUP PARAM_ATTRITION_CREDITS BY  (CD_EFS,CD_SSF_VI,CD_TY_TX);

-- ******************************************************************************** --
-- Récupération du paramétrage des marges par défaut 
-- ******************************************************************************** --
PARAM_MARGE_DFT = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PARAM/MARGE_DFT.20160725_152621_7' USING PigStorage(';') AS (
CD_EFS:chararray,
CD_SSF_VI:chararray,
LIB_SSF_VI:chararray,
TY_TX:chararray,
MATU_MIN:int,
MATU_MX:int,
TX_MRG_DFT:double
);

-- Création d'une ligne par mois de maturité
PARAM_MARGE_DFT_PLAT = FOREACH PARAM_MARGE_DFT GENERATE
CD_SSF_VI,
LIB_SSF_VI,
CD_EFS,
TY_TX,
--INTERVAL(MATU_MIN,MATU_MX),
FLATTEN(INTERVAL(MATU_MIN,MATU_MX)) AS maturite,
TX_MRG_DFT;


-- ******************************************************************************** --
-- Récupération du paramétrage des minorations SFH
-- ******************************************************************************** --
PARAM_SFH = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PARAM/SFH.20160725_152621_7' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (
TX_MNR_VI:double
);

PARAM_SFH = FOREACH PARAM_SFH GENERATE
TX_MNR_VI AS TX_MNR_VI,
1 AS key;


-- Jointure pour obtenir la probaVivant et le code EFS du client
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
(noCtrScr IS NOT NULL ? 1 : 0 ) AS idcEliSfh,
1 AS key,
codeReseau AS cdEfs,
CD_SSF_VI AS cdSFam
;

-- pour obtenir TX_MNR_VI
Contrat_sfh = JOIN PARAM_SFH BY key, Contrat1 BY key;

Contrat2 = FOREACH Contrat_sfh GENERATE 
mois AS mois,
noClient AS noClient,
daNai AS daNai,
cdSex AS cdSex,
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
cdEfs AS cdEfs,
cdSFam AS cdSFam,
(idcEliSfh==1 ? TX_MNR_VI : 0.0) AS txMinoSFH
;

Contrat2 = ORDER Contrat2 BY mois;
STORE Contrat2 INTO '/hdfs/staging/out/e1225/credit/contrat_basic' using PigStorage(';');