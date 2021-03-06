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

-- On ne garde que les champs utiles au comptage des contrats, code SI et numéro de contrat
Contrat_1 = FOREACH PNB_Contrat GENERATE moisTraitement,cdSi,noCtrScr,cdSFam,cdFam;

PNB_Contrat_1 = FOREACH PNB_Contrat GENERATE moisTraitement,cdSi,noCtrScr,cdSFam,cdFam,PNB;

Contrat_1 = DISTINCT Contrat_1;

Contrat_group_sous_famille = GROUP PNB_Contrat_1 BY (moisTraitement,cdFam,cdSFam);

NB_contrat = FOREACH Contrat_group_sous_famille GENERATE group, COUNT(PNB_Contrat_1) AS NB;

--VI_famille_group = GROUP PNB_Contrat_1 BY (moisTraitement,cdFam);

--VI_famille = FOREACH VI_famille_group GENERATE FLATTEN(group) AS (moisTraitement:chararray, cdFam:chararray),SUM(PNB_Contrat_1.PNB);


VI_sous_famille_group = GROUP PNB_Contrat_1 BY (moisTraitement,cdFam,cdSFam);

VI_sous_famille = FOREACH VI_sous_famille_group GENERATE group,SUM(PNB_Contrat_1.PNB) AS VI;

VI_par_contrat_sous_famille = JOIN VI_sous_famille BY group FULL, NB_contrat BY group;

Valeur_VI_par_contrat = FOREACH VI_par_contrat_sous_famille GENERATE NB_contrat.group, VI_sous_famille.VI AS VI, NB_contrat.NB AS NB, VI/NB AS VI_par_contrat; 

--VI_famille = ORDER VI_famille BY moisTraitement;

VI_sous_famille = ORDER VI_sous_famille BY group;

NB_contrat = ORDER NB_contrat BY group;

Valeur_VI_par_contrat = ORDER Valeur_VI_par_contrat BY group;

STORE NB_contrat INTO '/hdfs/staging/out/e1225/NB_contrat' using PigStorage(',');

--STORE VI_famille INTO '/hdfs/staging/out/e1225/VI_famille' using PigStorage(',');

STORE VI_sous_famille INTO '/hdfs/staging/out/e1225/VI_sous_famille' using PigStorage(',');

STORE Valeur_VI_par_contrat INTO '/hdfs/staging/out/e1225/Valeur_VI_par_contrat' using PigStorage(',');