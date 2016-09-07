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


-- Load data
check_taux = LOAD '/hdfs/staging/out/e1225/credit/check_tab_M*' USING PigStorage(';') AS 
(	
	mois:chararray,
	taux_client_pondere:double,
	taux_client_hypothese:double,
	check_taux_client_pond:double
);

check_taux = ORDER check_taux BY mois;

STORE check_taux INTO '/hdfs/staging/out/e1225/credit/check_taux' using PigStorage(';');
