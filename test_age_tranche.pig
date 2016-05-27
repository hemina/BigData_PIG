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

nouveaux_contrat = LOAD '/hdfs/staging/out/e1225/resultat_contrat_entrants_test' USING PigStorage(',') AS (mois: chararray, cdSi:chararray, noCtrScr:chararray, noPse:chararray, age: int, cdSex:chararray, cdInseeCsp:chararray, noStrGtn:chararray, cotMacDo:chararray);

nouveaux_contrat_ageTranche = FOREACH nouveaux_contrat GENERATE 
mois AS mois, 
cdSi AS cdSi, 
noCtrScr AS noCtrScr, 
noPse AS noPse, 
age AS age, 
cdSex AS cdSex, 
cdInseeCsp AS cdInseeCsp, 
noStrGtn AS noStrGtn, 
cotMacDo AS cotMacDo, 
AgeTranche(age,0,90,5) AS age_Tranche;

nouveaux_contrat_ageTranche = ORDER nouveaux_contrat_ageTranche BY mois;

STORE nouveaux_contrat_ageTranche INTO '/hdfs/staging/out/e1225/nouveaux_contrat_ageTranche' using PigStorage(',');