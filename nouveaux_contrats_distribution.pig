-- DESCRIPTION : Fichier pour gérer la projection des contrats sur n mois futurs

-- DATE DE CREATION VERSION V1 : 06/06/2016
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



-- Création d'alias pour utiliser les UDFS de manière plus simple dans le code
DEFINE AgeTranche minaUDFS.AgeTranche();

PNB_Contrat = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150531/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150630/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150731/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150831/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150930/09.07.SRV.2.CalculerPNB.Option.1.20160517-145553,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151031/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151130/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160131/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160229/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160331/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160430/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151231/09.07.SRV.2.CalculerPNB.Option.1.20160517-164359'
 USING PigStorage(';') AS 
(	
	moisTraitement:chararray,
	cdSi:chararray,
	cdEfs:chararray,
	noCtrScr:chararray,
	cdSFam:chararray,
	cdFam:chararray,
	optModel:chararray,
	daOuv:chararray,
	cdEtaCtrScr:chararray,
	mtMoyen12MoisGlissant:double,
	noPse:chararray,
	nbClients:int,
	noStrGtn:long,
	daNai:chararray,
	cdSex:chararray,
	cdInseeCsp:chararray,
	noStr:chararray,
	cotMacDo:chararray,	
	codeReseau:chararray,
	libReseau:chararray,
	noPseMaitre:chararray,
	moisProjAbsolu:chararray,
	moisProjRelatif:int,	
	age:int,
	probaVivant:double,
	txAjustement:double,
	txAttrition:double,
	probaPresence:double,
	PUAssurance:double,
	PNB:double
);

localisation_stru = LOAD '/hdfs/staging/out/e1225/nouveaux_contrats/noGtn_region.csv' USING PigStorage(';') AS
(
noDep : int,
departement : chararray,
region : chararray,
numstr : long,
ville : chararray
);


--On ne garde que le mois de projection 1
PNB_Contrat = FILTER PNB_Contrat BY moisProjRelatif==1;

-- On ne garde que les champs utiles au comptage des contrats, code SI et numéro de contrat
PNB_Contrat_1 = FOREACH PNB_Contrat GENERATE
moisTraitement AS mois, 
cdSi AS cdSi, 
noCtrScr AS noCtrScr, 
cdFam AS cdFam,
cdSFam AS cdSFam,
noPse AS noPse, 
age AS age, 
cdSex AS cdSex, 
cdInseeCsp AS cdInseeCsp, 
noStrGtn AS noStrGtn, 
cotMacDo AS cotMacDo, 
AgeTranche(age,0,90,5) AS age_Tranche;

-- On fait un distinct pour supprimer les doublons
PNB_Contrat_1 = DISTINCT PNB_Contrat_1;

SPLIT PNB_Contrat_1 INTO 
mois201505 IF mois =='20150531', 
mois201506 IF mois =='20150630', 
mois201507 IF mois =='20150731', 
mois201508 IF mois =='20150831', 
mois201509 IF mois =='20150930', 
mois201510 IF mois =='20151031', 
mois201511 IF mois =='20151130', 
mois201512 IF mois =='20151231', 
mois201601 IF mois =='20160131', 
mois201602 IF mois =='20160229',
mois201603 IF mois =='20160331',
mois201604 IF mois =='20160430';

contrats_mois201505_06 = JOIN mois201505 BY (cdSi, noCtrScr) FULL, mois201506 BY (cdSi, noCtrScr);
contrats_mois201506_07 = JOIN mois201506 BY (cdSi, noCtrScr) FULL, mois201507 BY (cdSi, noCtrScr);
contrats_mois201507_08 = JOIN mois201507 BY (cdSi, noCtrScr) FULL, mois201508 BY (cdSi, noCtrScr);
contrats_mois201508_09 = JOIN mois201508 BY (cdSi, noCtrScr) FULL, mois201509 BY (cdSi, noCtrScr);
contrats_mois201509_10 = JOIN mois201509 BY (cdSi, noCtrScr) FULL, mois201510 BY (cdSi, noCtrScr);
contrats_mois201510_11 = JOIN mois201510 BY (cdSi, noCtrScr) FULL, mois201511 BY (cdSi, noCtrScr);
contrats_mois201511_12 = JOIN mois201511 BY (cdSi, noCtrScr) FULL, mois201512 BY (cdSi, noCtrScr);
contrats_mois201512_01 = JOIN mois201512 BY (cdSi, noCtrScr) FULL, mois201601 BY (cdSi, noCtrScr);
contrats_mois201601_02 = JOIN mois201601 BY (cdSi, noCtrScr) FULL, mois201602 BY (cdSi, noCtrScr);
contrats_mois201602_03 = JOIN mois201602 BY (cdSi, noCtrScr) FULL, mois201603 BY (cdSi, noCtrScr);
contrats_mois201603_04 = JOIN mois201603 BY (cdSi, noCtrScr) FULL, mois201604 BY (cdSi, noCtrScr);

SPLIT contrats_mois201505_06 INTO
contrats_sortants_mois201505_06 IF mois201506::cdSi IS NULL,
contrats_entrants_mois201505_06 IF mois201505::cdSi IS NULL;

SPLIT contrats_mois201506_07 INTO
contrats_sortants_mois201506_07 IF mois201507::cdSi IS NULL,
contrats_entrants_mois201506_07 IF mois201506::cdSi IS NULL;

SPLIT contrats_mois201507_08 INTO
contrats_sortants_mois201507_08 IF mois201508::cdSi IS NULL,
contrats_entrants_mois201507_08 IF mois201507::cdSi IS NULL;

SPLIT contrats_mois201508_09 INTO
contrats_sortants_mois201508_09 IF mois201509::cdSi IS NULL,
contrats_entrants_mois201508_09 IF mois201508::cdSi IS NULL;

SPLIT contrats_mois201509_10 INTO
contrats_sortants_mois201509_10 IF mois201510::cdSi IS NULL,
contrats_entrants_mois201509_10 IF mois201509::cdSi IS NULL;

SPLIT contrats_mois201510_11 INTO
contrats_sortants_mois201510_11 IF mois201511::cdSi IS NULL,
contrats_entrants_mois201510_11 IF mois201510::cdSi IS NULL;

SPLIT contrats_mois201511_12 INTO
contrats_sortants_mois201511_12 IF mois201512::cdSi IS NULL,
contrats_entrants_mois201511_12 IF mois201511::cdSi IS NULL;

SPLIT contrats_mois201512_01 INTO
contrats_sortants_mois201512_01 IF mois201601::cdSi IS NULL,
contrats_entrants_mois201512_01 IF mois201512::cdSi IS NULL;

SPLIT contrats_mois201601_02 INTO
contrats_sortants_mois201601_02 IF mois201602::cdSi IS NULL,
contrats_entrants_mois201601_02 IF mois201601::cdSi IS NULL;

SPLIT contrats_mois201602_03 INTO
contrats_sortants_mois201602_03 IF mois201603::cdSi IS NULL,
contrats_entrants_mois201602_03 IF mois201602::cdSi IS NULL;

SPLIT contrats_mois201603_04 INTO
contrats_sortants_mois201603_04 IF mois201604::cdSi IS NULL,
contrats_entrants_mois201603_04 IF mois201603::cdSi IS NULL;


contrat1 = FOREACH contrats_entrants_mois201505_06 GENERATE '2015-06-30' AS mois, 
mois201506::cdSi AS cdSi,
mois201506::noCtrScr AS noCtrScr,
mois201506::cdFam AS cdFam,
mois201506::cdSFam AS cdSFam,
mois201506::noPse AS noPse,
mois201506::age AS age,
mois201506::cdSex AS cdSex,
mois201506::cdInseeCsp AS cdInseeCsp,
mois201506::noStrGtn AS noStrGtn,
mois201506::cotMacDo AS cotMacDo,
AgeTranche(mois201506::age,0,90,5) AS age_Tranche;

contrat2 = FOREACH contrats_entrants_mois201506_07 GENERATE '2015-07-31' AS mois, 
mois201507::cdSi AS cdSi,
mois201507::noCtrScr AS noCtrScr,
mois201507::cdFam AS cdFam,
mois201507::cdSFam AS cdSFam,
mois201507::noPse AS noPse,
mois201507::age AS age,
mois201507::cdSex AS cdSex,
mois201507::cdInseeCsp AS cdInseeCsp,
mois201507::noStrGtn AS noStrGtn,
mois201507::cotMacDo AS cotMacDo,
AgeTranche(mois201507::age,0,90,5) AS age_Tranche;

contrat3 = FOREACH contrats_entrants_mois201507_08 GENERATE '2015-08-31' AS mois, 
mois201508::cdSi AS cdSi,
mois201508::noCtrScr AS noCtrScr,
mois201508::cdFam AS cdFam,
mois201508::cdSFam AS cdSFam,
mois201508::noPse AS noPse,
mois201508::age AS age,
mois201508::cdSex AS cdSex,
mois201508::cdInseeCsp AS cdInseeCsp,
mois201508::noStrGtn AS noStrGtn,
mois201508::cotMacDo AS cotMacDo,
AgeTranche(mois201508::age,0,90,5) AS age_Tranche;

contrat4 = FOREACH contrats_entrants_mois201508_09 GENERATE '2015-09-30' AS mois, 
mois201509::cdSi AS cdSi,
mois201509::noCtrScr AS noCtrScr,
mois201509::cdFam AS cdFam,
mois201509::cdSFam AS cdSFam,
mois201509::noPse AS noPse,
mois201509::age AS age,
mois201509::cdSex AS cdSex,
mois201509::cdInseeCsp AS cdInseeCsp,
mois201509::noStrGtn AS noStrGtn,
mois201509::cotMacDo AS cotMacDo,
AgeTranche(mois201509::age,0,90,5) AS age_Tranche;

contrat5 = FOREACH contrats_entrants_mois201509_10 GENERATE '2015-10-31' AS mois, 
mois201510::cdSi AS cdSi,
mois201510::noCtrScr AS noCtrScr,
mois201510::cdFam AS cdFam,
mois201510::cdSFam AS cdSFam,
mois201510::noPse AS noPse,
mois201510::age AS age,
mois201510::cdSex AS cdSex,
mois201510::cdInseeCsp AS cdInseeCsp,
mois201510::noStrGtn AS noStrGtn,
mois201510::cotMacDo AS cotMacDo,
AgeTranche(mois201510::age,0,90,5) AS age_Tranche;

contrat6 = FOREACH contrats_entrants_mois201510_11 GENERATE '2015-11-30' AS mois, 
mois201511::cdSi AS cdSi,
mois201511::noCtrScr AS noCtrScr,
mois201511::cdFam AS cdFam,
mois201511::cdSFam AS cdSFam,
mois201511::noPse AS noPse,
mois201511::age AS age,
mois201511::cdSex AS cdSex,
mois201511::cdInseeCsp AS cdInseeCsp,
mois201511::noStrGtn AS noStrGtn,
mois201511::cotMacDo AS cotMacDo,
AgeTranche(mois201511::age,0,90,5) AS age_Tranche;

contrat7 = FOREACH contrats_entrants_mois201511_12 GENERATE '2015-12-31' AS mois, 
mois201512::cdSi AS cdSi,
mois201512::noCtrScr AS noCtrScr,
mois201512::cdFam AS cdFam,
mois201512::cdSFam AS cdSFam,
mois201512::noPse AS noPse,
mois201512::age AS age,
mois201512::cdSex AS cdSex,
mois201512::cdInseeCsp AS cdInseeCsp,
mois201512::noStrGtn AS noStrGtn,
mois201512::cotMacDo AS cotMacDo,
AgeTranche(mois201512::age,0,90,5) AS age_Tranche;

contrat8 = FOREACH contrats_entrants_mois201512_01 GENERATE '2016-01-31' AS mois, 
mois201601::cdSi AS cdSi,
mois201601::noCtrScr AS noCtrScr,
mois201601::cdFam AS cdFam,
mois201601::cdSFam AS cdSFam,
mois201601::noPse AS noPse,
mois201601::age AS age,
mois201601::cdSex AS cdSex,
mois201601::cdInseeCsp AS cdInseeCsp,
mois201601::noStrGtn AS noStrGtn,
mois201601::cotMacDo AS cotMacDo,
AgeTranche(mois201601::age,0,90,5) AS age_Tranche;

contrat9 = FOREACH contrats_entrants_mois201601_02 GENERATE '2016-02-29' AS mois, 
mois201602::cdSi AS cdSi,
mois201602::noCtrScr AS noCtrScr,
mois201602::cdFam AS cdFam,
mois201602::cdSFam AS cdSFam,
mois201602::noPse AS noPse,
mois201602::age AS age,
mois201602::cdSex AS cdSex,
mois201602::cdInseeCsp AS cdInseeCsp,
mois201602::noStrGtn AS noStrGtn,
mois201602::cotMacDo AS cotMacDo,
AgeTranche(mois201602::age,0,90,5) AS age_Tranche;

contrat10 = FOREACH contrats_entrants_mois201602_03 GENERATE '2016-03-31' AS mois, 
mois201603::cdSi AS cdSi,
mois201603::noCtrScr AS noCtrScr,
mois201603::cdFam AS cdFam,
mois201603::cdSFam AS cdSFam,
mois201603::noPse AS noPse,
mois201603::age AS age,
mois201603::cdSex AS cdSex,
mois201603::cdInseeCsp AS cdInseeCsp,
mois201603::noStrGtn AS noStrGtn,
mois201603::cotMacDo AS cotMacDo,
AgeTranche(mois201603::age,0,90,5) AS age_Tranche;

contrat11 = FOREACH contrats_entrants_mois201603_04 GENERATE '2016-04-30' AS mois, 
mois201604::cdSi AS cdSi,
mois201604::noCtrScr AS noCtrScr,
mois201604::cdFam AS cdFam,
mois201604::cdSFam AS cdSFam,
mois201604::noPse AS noPse,
mois201604::age AS age,
mois201604::cdSex AS cdSex,
mois201604::cdInseeCsp AS cdInseeCsp,
mois201604::noStrGtn AS noStrGtn,
mois201604::cotMacDo AS cotMacDo,
AgeTranche(mois201604::age,0,90,5) AS age_Tranche;

contrats_entrants = UNION contrat1,contrat2,contrat3,contrat4,contrat5,contrat6,contrat7,contrat8,contrat9,contrat10,contrat11;



clients_mois201505_06 = JOIN mois201505 BY (cdSi, noPse) FULL, mois201506 BY (cdSi, noPse);
clients_mois201506_07 = JOIN mois201506 BY (cdSi, noPse) FULL, mois201507 BY (cdSi, noPse);
clients_mois201507_08 = JOIN mois201507 BY (cdSi, noPse) FULL, mois201508 BY (cdSi, noPse);
clients_mois201508_09 = JOIN mois201508 BY (cdSi, noPse) FULL, mois201509 BY (cdSi, noPse);
clients_mois201509_10 = JOIN mois201509 BY (cdSi, noPse) FULL, mois201510 BY (cdSi, noPse);
clients_mois201510_11 = JOIN mois201510 BY (cdSi, noPse) FULL, mois201511 BY (cdSi, noPse);
clients_mois201511_12 = JOIN mois201511 BY (cdSi, noPse) FULL, mois201512 BY (cdSi, noPse);
clients_mois201512_01 = JOIN mois201512 BY (cdSi, noPse) FULL, mois201601 BY (cdSi, noPse);
clients_mois201601_02 = JOIN mois201601 BY (cdSi, noPse) FULL, mois201602 BY (cdSi, noPse);
clients_mois201602_03 = JOIN mois201602 BY (cdSi, noPse) FULL, mois201603 BY (cdSi, noPse);
clients_mois201603_04 = JOIN mois201603 BY (cdSi, noPse) FULL, mois201604 BY (cdSi, noPse);


SPLIT clients_mois201505_06 INTO
clients_sortants_mois201505_06 IF mois201506::cdSi IS NULL,
clients_entrants_mois201505_06 IF mois201505::cdSi IS NULL;

SPLIT clients_mois201506_07 INTO
clients_sortants_mois201506_07 IF mois201507::cdSi IS NULL,
clients_entrants_mois201506_07 IF mois201506::cdSi IS NULL;

SPLIT clients_mois201507_08 INTO
clients_sortants_mois201507_08 IF mois201508::cdSi IS NULL,
clients_entrants_mois201507_08 IF mois201507::cdSi IS NULL;

SPLIT clients_mois201508_09 INTO
clients_sortants_mois201508_09 IF mois201509::cdSi IS NULL,
clients_entrants_mois201508_09 IF mois201508::cdSi IS NULL;

SPLIT clients_mois201509_10 INTO
clients_sortants_mois201509_10 IF mois201510::cdSi IS NULL,
clients_entrants_mois201509_10 IF mois201509::cdSi IS NULL;

SPLIT clients_mois201510_11 INTO
clients_sortants_mois201510_11 IF mois201511::cdSi IS NULL,
clients_entrants_mois201510_11 IF mois201510::cdSi IS NULL;

SPLIT clients_mois201511_12 INTO
clients_sortants_mois201511_12 IF mois201512::cdSi IS NULL,
clients_entrants_mois201511_12 IF mois201511::cdSi IS NULL;

SPLIT clients_mois201512_01 INTO
clients_sortants_mois201512_01 IF mois201601::cdSi IS NULL,
clients_entrants_mois201512_01 IF mois201512::cdSi IS NULL;

SPLIT clients_mois201601_02 INTO
clients_sortants_mois201601_02 IF mois201602::cdSi IS NULL,
clients_entrants_mois201601_02 IF mois201601::cdSi IS NULL;

SPLIT clients_mois201602_03 INTO
clients_sortants_mois201602_03 IF mois201603::cdSi IS NULL,
clients_entrants_mois201602_03 IF mois201602::cdSi IS NULL;

SPLIT clients_mois201603_04 INTO
clients_sortants_mois201603_04 IF mois201604::cdSi IS NULL,
clients_entrants_mois201603_04 IF mois201603::cdSi IS NULL;


client1 = FOREACH clients_entrants_mois201505_06 GENERATE '2015-06-30' AS mois, 
mois201506::cdSi AS cdSi,
mois201506::noCtrScr AS noCtrScr,
mois201506::noPse AS noPse,
mois201506::age AS age,
mois201506::cdSex AS cdSex,
mois201506::cdInseeCsp AS cdInseeCsp,
mois201506::noStrGtn AS noStrGtn,
mois201506::cotMacDo AS cotMacDo,
AgeTranche(mois201506::age,0,90,5) AS age_Tranche;

client2 = FOREACH clients_entrants_mois201506_07 GENERATE '2015-07-31' AS mois, 
mois201507::cdSi AS cdSi,
mois201507::noCtrScr AS noCtrScr,
mois201507::noPse AS noPse,
mois201507::age AS age,
mois201507::cdSex AS cdSex,
mois201507::cdInseeCsp AS cdInseeCsp,
mois201507::noStrGtn AS noStrGtn,
mois201507::cotMacDo AS cotMacDo,
AgeTranche(mois201507::age,0,90,5) AS age_Tranche;

client3 = FOREACH clients_entrants_mois201507_08 GENERATE '2015-08-31' AS mois, 
mois201508::cdSi AS cdSi,
mois201508::noCtrScr AS noCtrScr,
mois201508::noPse AS noPse,
mois201508::age AS age,
mois201508::cdSex AS cdSex,
mois201508::cdInseeCsp AS cdInseeCsp,
mois201508::noStrGtn AS noStrGtn,
mois201508::cotMacDo AS cotMacDo,
AgeTranche(mois201508::age,0,90,5) AS age_Tranche;

client4 = FOREACH clients_entrants_mois201508_09 GENERATE '2015-09-30' AS mois, 
mois201509::cdSi AS cdSi,
mois201509::noCtrScr AS noCtrScr,
mois201509::noPse AS noPse,
mois201509::age AS age,
mois201509::cdSex AS cdSex,
mois201509::cdInseeCsp AS cdInseeCsp,
mois201509::noStrGtn AS noStrGtn,
mois201509::cotMacDo AS cotMacDo,
AgeTranche(mois201509::age,0,90,5) AS age_Tranche;

client5 = FOREACH clients_entrants_mois201509_10 GENERATE '2015-10-31' AS mois, 
mois201510::cdSi AS cdSi,
mois201510::noCtrScr AS noCtrScr,
mois201510::noPse AS noPse,
mois201510::age AS age,
mois201510::cdSex AS cdSex,
mois201510::cdInseeCsp AS cdInseeCsp,
mois201510::noStrGtn AS noStrGtn,
mois201510::cotMacDo AS cotMacDo,
AgeTranche(mois201510::age,0,90,5) AS age_Tranche;

client6 = FOREACH clients_entrants_mois201510_11 GENERATE '2015-11-30' AS mois, 
mois201511::cdSi AS cdSi,
mois201511::noCtrScr AS noCtrScr,
mois201511::noPse AS noPse,
mois201511::age AS age,
mois201511::cdSex AS cdSex,
mois201511::cdInseeCsp AS cdInseeCsp,
mois201511::noStrGtn AS noStrGtn,
mois201511::cotMacDo AS cotMacDo,
AgeTranche(mois201511::age,0,90,5) AS age_Tranche;

client7 = FOREACH clients_entrants_mois201511_12 GENERATE '2015-12-31' AS mois, 
mois201512::cdSi AS cdSi,
mois201512::noCtrScr AS noCtrScr,
mois201512::noPse AS noPse,
mois201512::age AS age,
mois201512::cdSex AS cdSex,
mois201512::cdInseeCsp AS cdInseeCsp,
mois201512::noStrGtn AS noStrGtn,
mois201512::cotMacDo AS cotMacDo,
AgeTranche(mois201512::age,0,90,5) AS age_Tranche;

client8 = FOREACH clients_entrants_mois201512_01 GENERATE '2016-01-31' AS mois, 
mois201601::cdSi AS cdSi,
mois201601::noCtrScr AS noCtrScr,
mois201601::noPse AS noPse,
mois201601::age AS age,
mois201601::cdSex AS cdSex,
mois201601::cdInseeCsp AS cdInseeCsp,
mois201601::noStrGtn AS noStrGtn,
mois201601::cotMacDo AS cotMacDo,
AgeTranche(mois201601::age,0,90,5) AS age_Tranche;

client9 = FOREACH clients_entrants_mois201601_02 GENERATE '2016-02-29' AS mois, 
mois201602::cdSi AS cdSi,
mois201602::noCtrScr AS noCtrScr,
mois201602::noPse AS noPse,
mois201602::age AS age,
mois201602::cdSex AS cdSex,
mois201602::cdInseeCsp AS cdInseeCsp,
mois201602::noStrGtn AS noStrGtn,
mois201602::cotMacDo AS cotMacDo,
AgeTranche(mois201602::age,0,90,5) AS age_Tranche;

client10 = FOREACH clients_entrants_mois201602_03 GENERATE '2016-03-31' AS mois, 
mois201603::cdSi AS cdSi,
mois201603::noCtrScr AS noCtrScr,
mois201603::noPse AS noPse,
mois201603::age AS age,
mois201603::cdSex AS cdSex,
mois201603::cdInseeCsp AS cdInseeCsp,
mois201603::noStrGtn AS noStrGtn,
mois201603::cotMacDo AS cotMacDo,
AgeTranche(mois201603::age,0,90,5) AS age_Tranche;

client11 = FOREACH clients_entrants_mois201603_04 GENERATE '2016-04-30' AS mois, 
mois201604::cdSi AS cdSi,
mois201604::noCtrScr AS noCtrScr,
mois201604::noPse AS noPse,
mois201604::age AS age,
mois201604::cdSex AS cdSex,
mois201604::cdInseeCsp AS cdInseeCsp,
mois201604::noStrGtn AS noStrGtn,
mois201604::cotMacDo AS cotMacDo,
AgeTranche(mois201604::age,0,90,5) AS age_Tranche;



contrat_grp_fam = GROUP contrats_entrants BY (mois,cdFam);
nbContrat_Par_Famille = FOREACH contrat_grp_fam GENERATE FLATTEN(group) AS (mois,cdFam), COUNT_STAR(contrats_entrants) AS nb_contrats;

contrat_grp_Sousfam = GROUP contrats_entrants BY (mois,cdSFam);
nbContrat_Par_SousFamille = FOREACH contrat_grp_Sousfam GENERATE FLATTEN(group) AS (mois,cdSFam), COUNT_STAR(contrats_entrants) AS nb_contrats;


clients_entrants = UNION client1,client2,client3,client4,client5,client6,client7,client8,client9,client10,client11;

clients_entrants_loc = JOIN clients_entrants BY noStrGtn LEFT, localisation_stru BY numstr;


client_grp_age = GROUP clients_entrants BY (mois,age_Tranche);
nbClient_Par_Age = FOREACH client_grp_age GENERATE FLATTEN(group) AS (mois,age_Tranche), COUNT_STAR(clients_entrants) AS nb_clients;

client_grp_cdInseeCsp = GROUP clients_entrants BY (mois,cdInseeCsp);
nbClient_Par_Metier = FOREACH client_grp_cdInseeCsp GENERATE FLATTEN(group) AS (mois,cdInseeCsp), COUNT_STAR(clients_entrants) AS nb_clients;

client_grp_cdSex = GROUP clients_entrants BY (mois,cdSex);
nbClient_Par_Sex = FOREACH client_grp_cdSex GENERATE FLATTEN(group) AS (mois,cdSex), COUNT_STAR(clients_entrants) AS nb_clients;

client_grp_cdSi = GROUP clients_entrants BY (mois,cdSi);
nbClient_Par_cdSi = FOREACH client_grp_cdSi GENERATE FLATTEN(group) AS (mois,cdSi), COUNT_STAR(clients_entrants) AS nb_clients;

client_grp_departement = GROUP clients_entrants_loc BY (mois,departement);
nbClient_Par_departement = FOREACH client_grp_departement GENERATE FLATTEN(group) AS (mois,departement), COUNT_STAR(clients_entrants_loc) AS nb_clients;

client_grp_region = GROUP clients_entrants_loc BY (mois,region);
nbClient_Par_region = FOREACH client_grp_region GENERATE FLATTEN(group) AS (mois,region), COUNT_STAR(clients_entrants_loc) AS nb_clients;

client_grp_cotMacDo = GROUP clients_entrants BY (mois,cotMacDo);
nbClient_Par_cotMacDo = FOREACH client_grp_cotMacDo GENERATE FLATTEN(group) AS (mois,cotMacDo), COUNT_STAR(clients_entrants) AS nb_clients;


nbContrat_Par_Famille = ORDER nbContrat_Par_Famille BY mois,cdFam;
nbContrat_Par_SousFamille = ORDER nbContrat_Par_SousFamille BY mois,cdSFam;

nbClient_Par_Age = ORDER nbClient_Par_Age BY mois,age_Tranche;
nbClient_Par_Metier = ORDER nbClient_Par_Metier BY mois,cdInseeCsp;
nbClient_Par_Sex = ORDER nbClient_Par_Sex BY mois,cdSex;
nbClient_Par_cdSi = ORDER nbClient_Par_cdSi BY mois,cdSi;
nbClient_Par_departement = ORDER nbClient_Par_departement BY mois,departement;
nbClient_Par_region = ORDER nbClient_Par_region BY mois,region;
nbClient_Par_cotMacDo = ORDER nbClient_Par_cotMacDo BY mois,cotMacDo;

--clients_entrants_loc = ORDER clients_entrants_loc BY mois;

--STORE clients_entrants_loc INTO '/hdfs/staging/out/e1225/nouveaux_contrats/clients_entrants' using PigStorage(';');

STORE nbContrat_Par_Famille INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbContrat_Par_Famille' using PigStorage(';');
STORE nbContrat_Par_SousFamille INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbContrat_Par_SousFamille' using PigStorage(';');


STORE nbClient_Par_Age INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbClient_Par_Age' using PigStorage(';');
STORE nbClient_Par_Metier INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbClient_Par_Metier' using PigStorage(';');
STORE nbClient_Par_Sex INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbClient_Par_Sex' using PigStorage(';');
STORE nbClient_Par_cdSi INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbClient_Par_cdSi' using PigStorage(';');
STORE nbClient_Par_departement INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbClient_Par_departement' using PigStorage(';');
STORE nbClient_Par_region INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbClient_Par_region' using PigStorage(';');
STORE nbClient_Par_cotMacDo INTO '/hdfs/staging/out/e1225/nouveaux_contrats/nbClient_Par_cotMacDo' using PigStorage(';');