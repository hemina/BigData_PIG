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

-- Load les nombres des clients par deux mois 
PNB_NbClient = LOAD '/hdfs/staging/out/e1225/nbclient20150831-20150930,/hdfs/staging/out/e1225/nbclient20150930-20151031,/hdfs/staging/out/e1225/nbclient20151031-20151130,/hdfs/staging/out/e1225/nbclient20151130-20151231,/hdfs/staging/out/e1225/nbclient20151231-20160131,/hdfs/staging/out/e1225/nbclient20160131-20160229' 
USING PigStorage(',') AS 
(	
	moisDebut: chararray,
	moisFin: chararray,
	nbclient: int
);

-- Load les nombres des clients de chaque mois 
PNB_NbClient_ParMois = LOAD '/hdfs/staging/out/e1225/nbclient' USING PigStorage(',') AS 
(
	moisTraitement: chararray,
	nbclient: int
);

-- 
nouveau = JOIN PNB_NbClient BY moisDebut, PNB_NbClient_ParMois BY moisTraitement;
nb_nouveau_client = FOREACH nouveau GENERATE PNB_NbClient::moisFin AS mois, (PNB_NbClient::nbclient-PNB_NbClient_ParMois::nbclient) AS nb;

resiliation = JOIN PNB_NbClient BY moisFin, PNB_NbClient_ParMois BY moisTraitement;
nb_resiliation_client = FOREACH resiliation GENERATE PNB_NbClient::moisDebut AS mois, (PNB_NbClient::nbclient-PNB_NbClient_ParMois::nbclient) AS nb;

--STORE nouveau INTO '/hdfs/staging/out/e1225/nouveau' using PigStorage(',');
STORE nb_nouveau_client INTO '/hdfs/staging/out/e1225/nb_nouveau_client' using PigStorage(',');
STORE nb_resiliation_client INTO '/hdfs/staging/out/e1225/nb_resiliation_client' using PigStorage(',');
