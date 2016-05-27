-- DESCRIPTION : Fichier pour gérer la projection des clients sur n mois futurs

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



PNB_Contrat = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150831/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150930/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151031/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151130/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151231/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160131/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160229/09.07.SRV.2.CalculerPNB.Option.1.20160509-114930' USING PigStorage(';') AS 
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

-- On ne garde que les champs utiles au comptage des personnes, code SI et numéro personne
PNB_Contrat_1 = FOREACH PNB_Contrat GENERATE moisTraitement,cdSi,noPse;

-- On fait un distinct pour supprimer les doublons
PNB_Contrat_1 = DISTINCT PNB_Contrat_1;

-- Group ALL pour le comptage
COMPTAGE_GRP = GROUP PNB_Contrat_1 BY moisTraitement;

-- Appel au COUNT_STAR pour compter le nombre de lignes PNB_Contrat_1 contenues dans le group COMPTAGE_GRP
COMPTAGE = FOREACH COMPTAGE_GRP GENERATE group,COUNT_STAR(PNB_Contrat_1);

SPLIT PNB_Contrat_1 INTO 
mois1 IF moisTraitement=='20150831', 
mois2 IF moisTraitement=='20150930', 
mois3 IF moisTraitement=='20151031', 
mois4 IF moisTraitement=='20151130', 
mois5 IF moisTraitement=='20151231', 
mois6 IF moisTraitement=='20160131', 
mois7 IF moisTraitement=='20160229';

mois1_2 = JOIN mois1 BY (cdSi,noPse) FULL, mois2 BY (cdSi,noPse);
mois2_3 = JOIN mois2 BY (cdSi,noPse) FULL, mois3 BY (cdSi,noPse);
mois3_4 = JOIN mois3 BY (cdSi,noPse) FULL, mois4 BY (cdSi,noPse);
mois4_5 = JOIN mois4 BY (cdSi,noPse) FULL, mois5 BY (cdSi,noPse);
mois5_6 = JOIN mois5 BY (cdSi,noPse) FULL, mois6 BY (cdSi,noPse);
mois6_7 = JOIN mois6 BY (cdSi,noPse) FULL, mois7 BY (cdSi,noPse);

mois1_2all = GROUP mois1_2 ALL;
mois2_3all = GROUP mois2_3 ALL;
mois3_4all = GROUP mois3_4 ALL;
mois4_5all = GROUP mois4_5 ALL;
mois5_6all = GROUP mois5_6 ALL;
mois6_7all = GROUP mois6_7 ALL;

nb_client_mois1_2all = FOREACH mois1_2all GENERATE '20150831' AS moisDebut, '20150930' AS moisFin, COUNT_STAR(mois1_2) AS nbclient;
nb_client_mois2_3all = FOREACH mois2_3all GENERATE '20150930' AS moisDebut, '20151031' AS moisFin, COUNT_STAR(mois2_3) AS nbclient;
nb_client_mois3_4all = FOREACH mois3_4all GENERATE '20151031' AS moisDebut, '20151130' AS moisFin, COUNT_STAR(mois3_4) AS nbclient;
nb_client_mois4_5all = FOREACH mois4_5all GENERATE '20151130' AS moisDebut, '20151231' AS moisFin, COUNT_STAR(mois4_5) AS nbclient;
nb_client_mois5_6all = FOREACH mois5_6all GENERATE '20151231' AS moisDebut, '20160131' AS moisFin, COUNT_STAR(mois5_6) AS nbclient;
nb_client_mois6_7all = FOREACH mois6_7all GENERATE '20160131' AS moisDebut, '20160229' AS moisFin, COUNT_STAR(mois6_7) AS nbclient;

/*
STORE nb_client_mois1_2all INTO '/hdfs/staging/out/e1225/nbclient20150831-20150930' using PigStorage(',');
STORE nb_client_mois2_3all INTO '/hdfs/staging/out/e1225/nbclient20150930-20151031' using PigStorage(',');
STORE nb_client_mois3_4all INTO '/hdfs/staging/out/e1225/nbclient20151031-20151130' using PigStorage(',');
STORE nb_client_mois4_5all INTO '/hdfs/staging/out/e1225/nbclient20151130-20151231' using PigStorage(',');
STORE nb_client_mois5_6all INTO '/hdfs/staging/out/e1225/nbclient20151231-20160131' using PigStorage(',');
STORE nb_client_mois6_7all INTO '/hdfs/staging/out/e1225/nbclient20160131-20160229' using PigStorage(',');
*/
--mois1_2lmt = LIMIT mois1_2 100;

-- Dump pour afficher le résultat
--DUMP mois1_2lmt;