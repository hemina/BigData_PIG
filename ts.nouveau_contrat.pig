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

-- On ne garde que les champs utiles au comptage des contrats, code SI et numéro de contrat
PNB_Contrat_1 = FOREACH PNB_Contrat GENERATE
mois AS mois, 
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
noStrGtn AS noStrGtn;
cotMacDo AS cotMacDo, 
AgeTranche(age,0,90,5) AS age_Tranche;

-- On fait un distinct pour supprimer les doublons
PNB_Contrat_1 = DISTINCT PNB_Contrat_1;

PNB_Contrat_1 = ORDER PNB_Contrat_1 BY daOuv;
STORE PNB_Contrat_1 INTO '/hdfs/staging/out/e1225/ts.nouveau_contrat' using PigStorage(';');