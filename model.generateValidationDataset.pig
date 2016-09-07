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
 USING PigStorage(';') AS 
(	
	mois:chararray,
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

-- On fait un distinct pour supprimer les doublons
PNB_Contrat = DISTINCT PNB_Contrat;

-- On split les contrats par mois
SPLIT PNB_Contrat INTO 
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

mois201505 = LIMIT mois201505 10000;
mois201506 = LIMIT mois201506 10000;
mois201507 = LIMIT mois201507 10000;
mois201508 = LIMIT mois201508 10000;
mois201509 = LIMIT mois201509 10000;
mois201510 = LIMIT mois201510 10000;
mois201511 = LIMIT mois201511 10000;
mois201512 = LIMIT mois201512 10000;
mois201601 = LIMIT mois201601 10000;
mois201602 = LIMIT mois201602 10000;
mois201603 = LIMIT mois201603 10000;
mois201604 = LIMIT mois201604 10000;

validation_dataset = UNION mois201505,mois201506,mois201507,mois201508,mois201509,mois201510,mois201511,mois201512,mois201601,mois201602,mois201603,mois201604;

validation_dataset = ORDER validation_dataset BY mois;

STORE validation_dataset INTO '/hdfs/staging/out/e1225/validation_dataset' using PigStorage(';');
