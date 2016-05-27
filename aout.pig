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
 
PNB_Contrat = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150831/09.07.SRV.6.CalculerPNB.Cartes.Commissions.CIP.CIR.20151029-100037' USING PigStorage(';') AS 
(	periodeTraitee:chararray,
	cdSi:chararray,
	cdEfs:chararray,
	noCtrScr:chararray,
	cdSFam:chararray,
	cdFam:chararray,
	optModel:chararray,
	daOuv:chararray,
	cdEtaCtrScr:chararray,
	TXCommission1:double,
	noPse:chararray,
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
	TXCommission:double,
	PNB:double);

clientTOP = FILTER PNB_Contrat BY (noPse MATCHES '17009236' AND cdSi MATCHES '001' AND noCtrScr MATCHES 'DD05120250' AND TXCommission==13.0 ); 
	
--PNB_Contrat = GROUP clientTOP BY noCtrScr;

CLIENT = FOREACH clientTOP GENERATE PNB, age, probaVivant, probaPresence, moisProjAbsolu, moisProjRelatif, txAjustement, txAttrition, TXCommission;

CLIENTINFO = ORDER CLIENT BY moisProjRelatif DESC;
--PNB_Contrat_Group = GROUP PNB_Contrat BY (cdSi, noPse, noPseMaitre, daNai, cdSex, cdInseeCsp, cotMacDo);

--PNB_Contrat_Group = GROUP PNB_Contrat BY (noCtrScr, cdSi, cdEfs, codeReseau, libReseau, noStr, noStrGtn, cdSFam, cdEtaCtrScr, 
--noPse, noPseMaitre, daOuv, daNai, age, cdSex, cdInseeCsp, cotMacDo, probaVivant, probaPresence,
--moisProjAbsolu, moisProjRelatif, txAjustement, txAttrition, TXCommission);

--VI_Alea = FOREACH PNB_Contrat_Group GENERATE FLATTEN(group), noPse,--PNB_Contrat.moisProjRelatif, PNB_Contrat.moisProjAbsolu,
--	SUM(PNB_Contrat.PNB) AS ValeurInstant;
	


--VI = ORDER VI_Alea BY ValeurInstant DESC;

--Sample_VI = LIMIT VI 20;
STORE CLIENTINFO INTO '/hdfs/staging/out/e1225/aout' using PigStorage(',');
--DUMP CLIENTINFO;