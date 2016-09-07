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
--DEFINE multiply minaUDFS.multiply();
--DEFINE AppliquerSigneEtDecimales 

-- Load data
Contrat0 = LOAD '/hdfs/staging/out/e1225/credit0.csv' USING PigStorage(';') AS 
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

taux0 = LOAD '/hdfs/staging/out/e1225/taux_evol.csv' USING PigStorage(';') AS 
(	
	month_i:chararray,
	date:chararray,
	tauxEvolProd:double,
	tauxClient_t:double
);

taux = FOREACH taux0 GENERATE 
month_i AS month_i,
changeDateFormat(date) AS date,
tauxEvolProd AS tauxEvolProd,
tauxClient_t AS tauxClient_t
;

Contrat = FOREACH Contrat0 GENERATE 
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
encours24 AS encours24
;
	
-- Jointure pour avoir le taux client pondéré, le taux client hypothese dans les contrats
contrat_taux = JOIN taux BY date RIGHT, Contrat BY mois;

-- Calculer taux client pondéré 
cal = FOREACH contrat_taux GENERATE mois AS mois, noClient AS noClient, (tauxClient*mtDebloque) AS mult, mtDebloque AS mtDebloque;
--cal_ordered = ORDER cal BY noClient;
--DUMP cal_ordered;
cal_grp = GROUP cal BY mois;
res = FOREACH cal_grp GENERATE group AS mois, SUM(cal.mult) AS nomi, SUM(cal.mtDebloque) AS denomi; 
result = FOREACH res GENERATE mois AS month, nomi/denomi AS val;
--DUMP result;

-- Ajouter le taux client pondéré au tableau
contrat_taux1 = JOIN result BY month RIGHT, contrat_taux BY mois; 
	
Contrat1 = FOREACH contrat_taux1 GENERATE 
monthInc(mois) AS mois,
noClient AS noClient,
changeDateFormat(daNai) AS daNai,
cdSex AS cdSex,
genNoCtrScr(noCtrScr,'1') AS noCtrScr, --'1'
nbClients AS nbClients,
famCredit AS famCredit,
sfamCredit AS sfamCredit, 
changeDateFormat(daPreDeblocage) AS daPreDeblocage,
changeDateFormat(daFinCredit) AS daFinCredit,
typeTaux AS typeTaux,
(tauxClient+tauxClient_t-val) AS tauxClient,
tauxBonification AS tauxBonification,
indicateurEligible AS indicateurEligible,
(mtNominal*(1+tauxEvolProd)) AS mtNominal,
(mtDebloque*(1+tauxEvolProd)) AS mtDebloque,
mtCommission AS mtCommission,
mtCoc AS mtCoc,
reseau AS reseau,
(encours0*(1+tauxEvolProd)) AS encours0,
(encours1*(1+tauxEvolProd)) AS encours1,
(encours2*(1+tauxEvolProd)) AS encours2,
(encours3*(1+tauxEvolProd)) AS encours3,
(encours4*(1+tauxEvolProd)) AS encours4,
(encours5*(1+tauxEvolProd)) AS encours5,
(encours6*(1+tauxEvolProd)) AS encours6,
(encours7*(1+tauxEvolProd)) AS encours7,
(encours8*(1+tauxEvolProd)) AS encours8,
(encours9*(1+tauxEvolProd)) AS encours9,
(encours10*(1+tauxEvolProd)) AS encours10,
(encours11*(1+tauxEvolProd)) AS encours11,
(encours12*(1+tauxEvolProd)) AS encours12,
(encours13*(1+tauxEvolProd)) AS encours13,
(encours14*(1+tauxEvolProd)) AS encours14,
(encours15*(1+tauxEvolProd)) AS encours15,
(encours16*(1+tauxEvolProd)) AS encours16,
(encours17*(1+tauxEvolProd)) AS encours17,
(encours18*(1+tauxEvolProd)) AS encours18,
(encours19*(1+tauxEvolProd)) AS encours19,
(encours20*(1+tauxEvolProd)) AS encours20,
(encours21*(1+tauxEvolProd)) AS encours21,
(encours22*(1+tauxEvolProd)) AS encours22,
(encours23*(1+tauxEvolProd)) AS encours23,
(encours24*(1+tauxEvolProd)) AS encours24,
ifExpire(daFinCredit, mois) AS ifExpireFlag,
tauxEvolProd AS tauxEvolProd,
tauxClient_t AS tauxClient_t, 
val AS taux_pond
;

-- Vérifier le calcule de taux client dans un fichier
check_cal = FOREACH Contrat1 GENERATE mois AS mois, noClient AS noClient, (tauxClient*mtDebloque) AS mult, mtDebloque AS mtDebloque;
--cal_ordered = ORDER cal BY noClient;
--DUMP cal_ordered;
check_res = FOREACH (GROUP check_cal BY mois) GENERATE group AS mois, SUM(check_cal.mult) AS nomi, SUM(check_cal.mtDebloque) AS denomi; 
check_result = FOREACH check_res GENERATE mois AS check_month, nomi/denomi AS check_taux_pond;

check_tab = JOIN result BY month LEFT, taux BY date;
check_tab1 = FOREACH check_tab GENERATE monthInc(month) AS month, result::val AS taux_client_pondere, taux::tauxClient_t AS taux_client_hypothese;

check_tab2 = JOIN check_tab1 BY month LEFT, check_result BY check_month; 
check_tab3 = FOREACH check_tab2 GENERATE check_month AS mois, taux_client_pondere AS taux_client_pondere, taux_client_hypothese AS taux_client_hypothese, check_result::check_taux_pond AS check_taux_client_pond;

-- Filtrer les contrats non expirés
ContratMoisProch = FILTER Contrat1 BY ifExpireFlag==TRUE;
STORE ContratMoisProch INTO '/hdfs/staging/out/e1225/credit/M+1' using PigStorage(';');
STORE check_tab3 INTO '/hdfs/staging/out/e1225/credit/check_tab_M+1' using PigStorage(';');
