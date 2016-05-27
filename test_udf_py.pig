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

DEFINE AgeTranche minaUDFS.AgeTranche();
REGISTER '/home/e1225/ValeurClient/src/mina/struGestExtr.py' USING streaming_python AS myudfs;

nouveaux_contrat_ancien_client = LOAD '/hdfs/staging/out/e1225/ancien_client_noStrGtn' USING PigStorage(';') AS (noStrGtn: chararray, nb:int);
nouveaux_contrat_nouveau_client = LOAD '/hdfs/staging/out/e1225/nouveau_client_noStrGtn' USING PigStorage(';') AS (noStrGtn: chararray, nb:int);


ancien_noStrGtn = FOREACH nouveaux_contrat_ancien_client GENERATE 
noStrGtn AS noStrGtn,
myudfs.struGestExtr_udf(noStrGtn,2) AS category,
nb AS nb;

nouveau_noStrGtn = FOREACH nouveaux_contrat_nouveau_client GENERATE 
noStrGtn AS noStrGtn,
myudfs.struGestExtr_udf(noStrGtn,2) AS category,
nb AS nb;

ancien_noStrGtn = ORDER ancien_noStrGtn BY noStrGtn;
nouveau_noStrGtn = ORDER nouveau_noStrGtn BY noStrGtn;

STORE ancien_noStrGtn INTO '/hdfs/staging/out/e1225/ancien_noStrGtn' using PigStorage(';');
STORE nouveau_noStrGtn INTO '/hdfs/staging/out/e1225/nouveau_noStrGtn' using PigStorage(';');
