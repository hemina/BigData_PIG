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



-- Création d'alias pour utiliser les UDFS de manière plus simple dans le code
DEFINE AgeTranche minaUDFS.AgeTranche();

PNB_Contrat = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150531/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150630/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150731/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150831/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150930/09.07.SRV.2.CalculerPNB.Option.1.20160517-145553,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151031/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151130/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160131/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160229/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160331/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160430/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151231/09.07.SRV.2.CalculerPNB.Option.1.20160517-164359'
--PNB_Contrat = LOAD '/hdfs/staging/out/e1225/validation_dataset'
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
	noStrGtn:chararray,
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

-- On ne garde que le mois de projection 1
PNB_Contrat = FILTER PNB_Contrat BY moisProjRelatif==1;

-- On ne garde que les champs utiles au comptage des contrats, code SI et numéro de contrat
PNB_Contrat_1 = FOREACH PNB_Contrat GENERATE
moisTraitement AS mois, 
daOuv AS daOuv,
daNai AS daNai,
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

-- On split les contrats par mois
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

-- On fait la jointure sur les contrats pour tous les deux mois
contrats_mois201505_06 = JOIN mois201506 BY (cdSi, noCtrScr) FULL, mois201505 BY (cdSi, noCtrScr);
contrats_mois201506_07 = JOIN mois201507 BY (cdSi, noCtrScr) FULL, mois201506 BY (cdSi, noCtrScr);
contrats_mois201507_08 = JOIN mois201508 BY (cdSi, noCtrScr) FULL, mois201507 BY (cdSi, noCtrScr);
contrats_mois201508_09 = JOIN mois201509 BY (cdSi, noCtrScr) FULL, mois201508 BY (cdSi, noCtrScr);
contrats_mois201509_10 = JOIN mois201510 BY (cdSi, noCtrScr) FULL, mois201509 BY (cdSi, noCtrScr);
contrats_mois201510_11 = JOIN mois201511 BY (cdSi, noCtrScr) FULL, mois201510 BY (cdSi, noCtrScr);
contrats_mois201511_12 = JOIN mois201512 BY (cdSi, noCtrScr) FULL, mois201511 BY (cdSi, noCtrScr);
contrats_mois201512_01 = JOIN mois201601 BY (cdSi, noCtrScr) FULL, mois201512 BY (cdSi, noCtrScr);
contrats_mois201601_02 = JOIN mois201602 BY (cdSi, noCtrScr) FULL, mois201601 BY (cdSi, noCtrScr);
contrats_mois201602_03 = JOIN mois201603 BY (cdSi, noCtrScr) FULL, mois201602 BY (cdSi, noCtrScr);
contrats_mois201603_04 = JOIN mois201604 BY (cdSi, noCtrScr) FULL, mois201603 BY (cdSi, noCtrScr);

-- On extrait les contrats sortants et entrants
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

-- On combine les contrats entrants de tous les mois
contrats_entrants = UNION contrats_entrants_mois201505_06, contrats_entrants_mois201506_07, contrats_entrants_mois201507_08, contrats_entrants_mois201508_09,
contrats_entrants_mois201509_10, contrats_entrants_mois201510_11, contrats_entrants_mois201511_12, contrats_entrants_mois201512_01, 
contrats_entrants_mois201601_02, contrats_entrants_mois201602_03, contrats_entrants_mois201603_04;

-- On fait la jointure sur les clients pour tous les deux mois
clients_mois201505_06 = JOIN mois201506 BY (cdSi, noPse) FULL, mois201505 BY (cdSi, noPse);
clients_mois201506_07 = JOIN mois201507 BY (cdSi, noPse) FULL, mois201506 BY (cdSi, noPse);
clients_mois201507_08 = JOIN mois201508 BY (cdSi, noPse) FULL, mois201507 BY (cdSi, noPse);
clients_mois201508_09 = JOIN mois201509 BY (cdSi, noPse) FULL, mois201508 BY (cdSi, noPse);
clients_mois201509_10 = JOIN mois201510 BY (cdSi, noPse) FULL, mois201509 BY (cdSi, noPse);
clients_mois201510_11 = JOIN mois201511 BY (cdSi, noPse) FULL, mois201510 BY (cdSi, noPse);
clients_mois201511_12 = JOIN mois201512 BY (cdSi, noPse) FULL, mois201511 BY (cdSi, noPse);
clients_mois201512_01 = JOIN mois201601 BY (cdSi, noPse) FULL, mois201512 BY (cdSi, noPse);
clients_mois201601_02 = JOIN mois201602 BY (cdSi, noPse) FULL, mois201601 BY (cdSi, noPse);
clients_mois201602_03 = JOIN mois201603 BY (cdSi, noPse) FULL, mois201602 BY (cdSi, noPse);
clients_mois201603_04 = JOIN mois201604 BY (cdSi, noPse) FULL, mois201603 BY (cdSi, noPse);

-- On extrait les clients sortants et entrants
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

-- On combine les clients entrants de tous les mois
clients_entrants = UNION clients_entrants_mois201505_06, clients_entrants_mois201506_07, clients_entrants_mois201507_08, clients_entrants_mois201509_10, 
clients_entrants_mois201510_11, clients_entrants_mois201511_12, clients_entrants_mois201512_01, clients_entrants_mois201601_02, 
clients_entrants_mois201602_03, clients_entrants_mois201603_04;

-- On stocke les contrats entrants et les clients entrants, mais ce sont des données après la jointure, pas très propre
STORE contrats_entrants INTO '/hdfs/staging/out/e1225/dateOuv/tmp_model_contrats_entrants' using PigStorage(';');
STORE clients_entrants INTO '/hdfs/staging/out/e1225/dateOuv/tmp_model_clients_entrants' using PigStorage(';');

-- On recharge ces données, mais en faisant exprès d'enlever la deuxième partie de la jointure. On ne charge que la première partie.
nouveau_contrat = LOAD '/hdfs/staging/out/e1225/dateOuv/tmp_model_contrats_entrants' USING PigStorage(';') AS ( mois:chararray, 
daOuv:chararray,
daNai:chararray,
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

nouveau_client = LOAD '/hdfs/staging/out/e1225/dateOuv/tmp_model_clients_entrants' USING PigStorage(';') AS ( mois:chararray, 
daOuv:chararray,
daNai:chararray,
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

-- On fait la jointure entre les nouveaux contrats et les nouveaux clients
nouveau_contrat_jointure = JOIN nouveau_contrat BY (cdSi, noPse) LEFT, nouveau_client BY (cdSi, noPse);

-- On distingue les nouveaux contrats qui viennent des anciens et nouveaux clients
SPLIT nouveau_contrat_jointure INTO
nouveau_contrat_ancien_client IF nouveau_client::cdSi IS NULL,
nouveau_contrat_nouveau_client IF nouveau_client::cdSi IS NOT NULL;

nouveau_contrat = ORDER nouveau_contrat BY mois;
nouveau_client = ORDER nouveau_client BY mois;

STORE nouveau_contrat INTO '/hdfs/staging/out/e1225/dateOuv/model_contrats_entrants' using PigStorage(';');
STORE nouveau_client INTO '/hdfs/staging/out/e1225/dateOuv/model_clients_entrants' using PigStorage(';');

-- On stocke les données après la jointure, les données pas très propre
STORE nouveau_contrat_ancien_client INTO '/hdfs/staging/out/e1225/dateOuv/model_nouveau_contrat_ancien_client' using PigStorage(';');
STORE nouveau_contrat_nouveau_client INTO '/hdfs/staging/out/e1225/dateOuv/model_nouveau_contrat_nouveau_client' using PigStorage(';');

-- On recharge les données, en faisant exprès d'enlever la deuxième partie de la jointure
nouveau_contrat_ancien_client = LOAD '/hdfs/staging/out/e1225/dateOuv/model_nouveau_contrat_ancien_client' USING PigStorage(';') AS ( mois:chararray, 
daOuv:chararray,
daNai:chararray,
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

nouveau_contrat_nouveau_client = LOAD '/hdfs/staging/out/e1225/dateOuv/model_nouveau_contrat_nouveau_client' USING PigStorage(';') AS ( mois:chararray,
daOuv:chararray,
daNai:chararray, 
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

localisation_stru = LOAD '/hdfs/staging/out/e1225/noGtn_region.csv' USING PigStorage(';') AS
(
noDep : chararray,
departement : chararray,
region : chararray,
numstr : long,
ville : chararray
);

nouveau_contrat_ancien_client = JOIN nouveau_contrat_ancien_client BY noStrGtn LEFT, localisation_stru BY numstr;
nouveau_contrat_nouveau_client = JOIN nouveau_contrat_nouveau_client BY noStrGtn LEFT, localisation_stru BY numstr;

nouveau_contrat_ancien_client = ORDER nouveau_contrat_ancien_client BY mois;
nouveau_contrat_nouveau_client = ORDER nouveau_contrat_nouveau_client BY mois;

STORE nouveau_contrat_ancien_client INTO '/hdfs/staging/out/e1225/dateOuv/model_nouveau_contrat/ancien_client' using PigStorage(';');
STORE nouveau_contrat_nouveau_client INTO '/hdfs/staging/out/e1225/dateOuv/model_nouveau_contrat/nouveau_client' using PigStorage(';');