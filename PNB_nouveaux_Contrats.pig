-- DESCRIPTION : Fichier pour gérer la projection des contrats sur n mois futurs

-- DATE DE CREATION VERSION V1 : 23/03/2015
-- AUTEUR: David COURTE
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
DEFINE UDF1 minaUDFS.UDF1();



--PNB_Contrat = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150831/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150930/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151031/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151130/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151231/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160131/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160229/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930' USING PigStorage(';') AS 
PNB_Contrat = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150831/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930' USING PigStorage(';') AS 
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

--On ne garde que le mois de projection 1
PNB_Contrat = FILTER PNB_Contrat BY moisProjRelatif==1;

-- On ne garde que les champs utiles au comptage des contrats, code SI et numéro de contrat
PNB_Contrat_1 = FOREACH PNB_Contrat GENERATE moisTraitement,cdSi,noCtrScr,PNB;

-- On fait un distinct pour supprimer les doublons
PNB_Contrat_1 = DISTINCT PNB_Contrat_1;

SPLIT PNB_Contrat_1 INTO 
mois1 IF moisTraitement=='20150831', 
mois2 IF moisTraitement=='20150930', 
mois3 IF moisTraitement=='20151031', 
mois4 IF moisTraitement=='20151130', 
mois5 IF moisTraitement=='20151231', 
mois6 IF moisTraitement=='20160131', 
mois7 IF moisTraitement=='20160229';

mois1_2 = JOIN mois1 BY (cdSi,noCtrScr) FULL, mois2 BY (cdSi,noCtrScr);
mois2_3 = JOIN mois2 BY (cdSi,noCtrScr) FULL, mois3 BY (cdSi,noCtrScr);
mois3_4 = JOIN mois3 BY (cdSi,noCtrScr) FULL, mois4 BY (cdSi,noCtrScr);
mois4_5 = JOIN mois4 BY (cdSi,noCtrScr) FULL, mois5 BY (cdSi,noCtrScr);
mois5_6 = JOIN mois5 BY (cdSi,noCtrScr) FULL, mois6 BY (cdSi,noCtrScr);
mois6_7 = JOIN mois6 BY (cdSi,noCtrScr) FULL, mois7 BY (cdSi,noCtrScr);

SPLIT mois1_2 INTO
contrats_sortants_mois1_2 IF mois2::cdSi IS NULL,
contrats_entrants_mois1_2 IF mois1::cdSi IS NULL;

SPLIT mois2_3 INTO
contrats_sortants_mois2_3 IF mois3::cdSi IS NULL,
contrats_entrants_mois2_3 IF mois2::cdSi IS NULL;

SPLIT mois3_4 INTO
contrats_sortants_mois3_4 IF mois4::cdSi IS NULL,
contrats_entrants_mois3_4 IF mois3::cdSi IS NULL;

SPLIT mois4_5 INTO
contrats_sortants_mois4_5 IF mois5::cdSi IS NULL,
contrats_entrants_mois4_5 IF mois4::cdSi IS NULL;

SPLIT mois5_6 INTO
contrats_sortants_mois5_6 IF mois6::cdSi IS NULL,
contrats_entrants_mois5_6 IF mois5::cdSi IS NULL;

SPLIT mois6_7 INTO
contrats_sortants_mois6_7 IF mois7::cdSi IS NULL,
contrats_entrants_mois6_7 IF mois6::cdSi IS NULL;

/*
contrats_sortants_mois1_2 = FOREACH (GROUP contrats_sortants_mois1_2 ALL) GENERATE '2015-08-31' AS mois, COUNT_STAR(contrats_sortants_mois1_2) AS nb;
contrats_sortants_mois2_3 = FOREACH (GROUP contrats_sortants_mois2_3 ALL) GENERATE '2015-09-30' AS mois, COUNT_STAR(contrats_sortants_mois2_3) AS nb;
contrats_sortants_mois3_4 = FOREACH (GROUP contrats_sortants_mois3_4 ALL) GENERATE '2015-10-31' AS mois, COUNT_STAR(contrats_sortants_mois3_4) AS nb;
contrats_sortants_mois4_5 = FOREACH (GROUP contrats_sortants_mois4_5 ALL) GENERATE '2015-11-30' AS mois, COUNT_STAR(contrats_sortants_mois4_5) AS nb;
contrats_sortants_mois5_6 = FOREACH (GROUP contrats_sortants_mois5_6 ALL) GENERATE '2015-12-31' AS mois, COUNT_STAR(contrats_sortants_mois5_6) AS nb;
contrats_sortants_mois6_7 = FOREACH (GROUP contrats_sortants_mois6_7 ALL) GENERATE '2016-01-31' AS mois, COUNT_STAR(contrats_sortants_mois6_7) AS nb;


contrats_entrants_mois1_2 = FOREACH (GROUP contrats_entrants_mois1_2 ALL) GENERATE '2015-08-31' AS mois, group, SUM(contrats_entrants_mois1_2.PNB) AS PNB;
contrats_entrants_mois2_3 = FOREACH (GROUP contrats_entrants_mois2_3 ALL) GENERATE '2015-09-30' AS mois, COUNT_STAR(contrats_entrants_mois2_3) AS nb;
contrats_entrants_mois3_4 = FOREACH (GROUP contrats_entrants_mois3_4 ALL) GENERATE '2015-10-31' AS mois, COUNT_STAR(contrats_entrants_mois3_4) AS nb;
contrats_entrants_mois4_5 = FOREACH (GROUP contrats_entrants_mois4_5 ALL) GENERATE '2015-11-30' AS mois, COUNT_STAR(contrats_entrants_mois4_5) AS nb;
contrats_entrants_mois5_6 = FOREACH (GROUP contrats_entrants_mois5_6 ALL) GENERATE '2015-12-31' AS mois, COUNT_STAR(contrats_entrants_mois5_6) AS nb;
contrats_entrants_mois6_7 = FOREACH (GROUP contrats_entrants_mois6_7 ALL) GENERATE '2016-01-31' AS mois, COUNT_STAR(contrats_entrants_mois6_7) AS nb;

resultat_contrat_sortants = UNION contrats_sortants_mois1_2,contrats_sortants_mois2_3,contrats_sortants_mois3_4,
contrats_sortants_mois4_5,contrats_sortants_mois5_6,contrats_sortants_mois6_7;

resultat_contrat_sortants = ORDER resultat_contrat_sortants BY mois;

resultat_contrat_entrants = UNION contrats_entrants_mois1_2,contrats_entrants_mois2_3,contrats_entrants_mois3_4,
contrats_entrants_mois4_5,contrats_entrants_mois5_6,contrats_entrants_mois6_7;

resultat_contrat_entrants = ORDER resultat_contrat_entrants BY mois;

STORE resultat_contrat_entrants INTO '/hdfs/staging/out/e1225/resultat_contrat_entrants$DATE_EXECUTION' using PigStorage(',');
STORE resultat_contrat_sortants INTO '/hdfs/staging/out/e1225/resultat_contrat_sortants$DATE_EXECUTION' using PigStorage(',');
*/