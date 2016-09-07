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
Contrat1 = LOAD '/hdfs/staging/out/e1225/credit/M+$input' USING PigStorage(';') AS --$input 0
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
	encours24:double,
	txMinoSFH:double,
	cdSFam:chararray
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

-- Jointure pour obtenir le taux evolution production, taux evolution client, et tci
contrat_taux_prod = JOIN taux_evol_prod BY (mois, cdSFam), Contrat1 BY (mois, cdSFam);

contrat_taux_client = JOIN taux_evol_client BY (mois, cdSFam), contrat_taux_prod BY (Contrat1::mois, Contrat1::cdSFam);

contrat_tci = JOIN tci BY (mois, cdSFam), contrat_taux_client BY (Contrat1::mois, Contrat1::cdSFam);

-- Calculer taux client pondéré 
cal = FOREACH contrat_tci GENERATE Contrat1::mois AS mois, noClient AS noClient, (tauxClient*mtDebloque) AS mult, mtDebloque AS mtDebloque;
cal_grp = GROUP cal BY mois;
res = FOREACH cal_grp GENERATE group AS mois, SUM(cal.mult) AS nomi, SUM(cal.mtDebloque) AS denomi; 
result = FOREACH res GENERATE mois AS month, nomi/denomi AS val;

contrat_taux1 = JOIN result BY month RIGHT, contrat_tci BY Contrat1::mois; 

Contrat2 = FOREACH contrat_taux1 GENERATE 
monthInc(Contrat1::mois) AS mois,
noClient AS noClient,
daNai AS daNai,
cdSex AS cdSex,
genNoCtrScr(noCtrScr,$output) AS noCtrScr, --$output '2'
nbClients AS nbClients,
famCredit AS famCredit,
sfamCredit AS sfamCredit,
daPreDeblocage AS daPreDeblocage,
daFinCredit AS daFinCredit, 
typeTaux AS typeTaux,
(tauxClient+taux_evol_client::taux-val) AS tauxClient,
tauxBonification AS tauxBonification,
indicateurEligible AS indicateurEligible,
(mtNominal*(1+taux_evol_prod::taux)) AS mtNominal,
(mtDebloque*(1+taux_evol_prod::taux)) AS mtDebloque,
mtCommission AS mtCommission,
mtCoc AS mtCoc,
reseau AS reseau,
(encours0*(1+taux_evol_prod::taux)) AS encours0,
(encours1*(1+taux_evol_prod::taux)) AS encours1,
(encours2*(1+taux_evol_prod::taux)) AS encours2,
(encours3*(1+taux_evol_prod::taux)) AS encours3,
(encours4*(1+taux_evol_prod::taux)) AS encours4,
(encours5*(1+taux_evol_prod::taux)) AS encours5,
(encours6*(1+taux_evol_prod::taux)) AS encours6,
(encours7*(1+taux_evol_prod::taux)) AS encours7,
(encours8*(1+taux_evol_prod::taux)) AS encours8,
(encours9*(1+taux_evol_prod::taux)) AS encours9,
(encours10*(1+taux_evol_prod::taux)) AS encours10,
(encours11*(1+taux_evol_prod::taux)) AS encours11,
(encours12*(1+taux_evol_prod::taux)) AS encours12,
(encours13*(1+taux_evol_prod::taux)) AS encours13,
(encours14*(1+taux_evol_prod::taux)) AS encours14,
(encours15*(1+taux_evol_prod::taux)) AS encours15,
(encours16*(1+taux_evol_prod::taux)) AS encours16,
(encours17*(1+taux_evol_prod::taux)) AS encours17,
(encours18*(1+taux_evol_prod::taux)) AS encours18,
(encours19*(1+taux_evol_prod::taux)) AS encours19,
(encours20*(1+taux_evol_prod::taux)) AS encours20,
(encours21*(1+taux_evol_prod::taux)) AS encours21,
(encours22*(1+taux_evol_prod::taux)) AS encours22,
(encours23*(1+taux_evol_prod::taux)) AS encours23,
(encours24*(1+taux_evol_prod::taux)) AS encours24,
txMinoSFH AS txMinoSFH,
Contrat1::cdSFam AS cdSFam,
FLATTEN(CALCUL_MARGE_AMORTISSABLE(Contrat1::cdSFam,tci::taux,0.0,tauxBonification,txMinoSFH,tauxClient,0.0,0.0,0.0,typeTaux)) AS (txMarge:double,marge:double,topTxMargeDft:int),
ifExpire(daFinCredit, Contrat1::mois) AS ifExpireFlag,
taux_evol_prod::taux AS taux_evol_prod, 
taux_evol_client::taux AS taux_evol_client, 
tci::taux AS tci,
val AS taux_pond
;
--
---- Vérifier le calcule de taux client dans un fichier
--check_cal = FOREACH Contrat2 GENERATE mois AS mois, noClient AS noClient, (tauxClient*mtDebloque) AS mult, mtDebloque AS mtDebloque;
--
--check_res = FOREACH (GROUP check_cal BY mois) GENERATE group AS mois, SUM(check_cal.mult) AS nomi, SUM(check_cal.mtDebloque) AS denomi; 
--
--check_result = FOREACH check_res GENERATE mois AS check_month, nomi/denomi AS check_taux_pond;
--
--
--check_tab = JOIN result BY month LEFT, taux BY date;
--
--check_tab1 = FOREACH check_tab GENERATE monthInc(month) AS month, result::val AS taux_client_pondere, taux_evol_client::taux AS taux_client_hypothese;
--
--
--check_tab2 = JOIN check_tab1 BY month LEFT, check_result BY check_month; 
--
--check_tab3 = FOREACH check_tab2 GENERATE check_month AS mois, taux_client_pondere AS taux_client_pondere, taux_client_hypothese AS taux_client_hypothese, check_result::check_taux_pond AS check_taux_client_pond;

-- Filtrer les contrats non expirés
ContratMoisProch = FILTER Contrat2 BY ifExpireFlag==TRUE;

STORE ContratMoisProch INTO '/hdfs/staging/out/e1225/credit/M+$output' using PigStorage(';');--$output

--STORE result INTO '/hdfs/staging/out/e1225/credit/taux_pond+$output' using PigStorage(';');
