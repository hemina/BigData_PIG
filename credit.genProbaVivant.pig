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

-- UDF pour calculer la probabilité d'actif mensuelle de manière récursive
DEFINE PROBA_ACTIF com.arkea.valeurinstantanee.udfs.ProbaActifAmortissables(); 

--Calcul de la marge et du taux de marge
DEFINE CALCUL_MARGE_AMORTISSABLE com.arkea.valeurinstantanee.udfs.CalculerMargeAmortissable();

-- UDF qui met à plat le paramétrage pour éviter les correspondances multiples et filter par interval
DEFINE INTERVAL com.arkea.valeurinstantanee.udfs.Interval(); 

-- UDF pour lire csv sans header
--DEFINE CSVExcelStorage com.arkea.valeurinstantanee.udfs.CSVExcelStorage();
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

-- UDF pour calculer le nombre de mois entre deux dates
DEFINE MONTHS_BETWEEN com.arkea.valeurinstantanee.udfs.MonthsBetween(); 


-- Load data
-- ******************************************************************************** --
-- Récupération de 09.07.REF.ClientsProbaVivant
-- ******************************************************************************** --
Client = LOAD --'/hdfs/staging/out/e1225/Clients' USING PigStorage(';') AS
'/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151231/09.07.REF.ClientsProbaVivant.20160517-154019/' USING PigStorage(';') AS 
(	
periodeTraitee:chararray,
cdSi:int,
noPse:long,
daNai:chararray,
cdSex:chararray,
cdInseeCsp:chararray,
noStr:chararray,
cotMacDo:chararray,
codeReseau:chararray,
libReseau:chararray,--CMB...
noPseMaitre:chararray,
moisProjRelatif:int,
moisProjAbsolu:chararray,
age:int,
probaVivant:double
);


Contrat = LOAD '/hdfs/staging/out/e1225/credit/M*' USING PigStorage(';') AS --$i 
(	
	mois:chararray,
	noClient:long,
	daNai:chararray,
	cdSex:chararray,
	noCtrScr:chararray,
	nbClients:int,
	famCredit:chararray,
	sfamCredit:chararray, 
	daPreDeblocage:chararray, 
	daFinCredit:chararray, 
	typeTaux:chararray,
	tauxClient:double,
	tauxBonification:double,
	indicateurEligible:double, --à vérifier
	mtNominal:double, --montant nominal
	mtDebloque:double, --montant débloqué
	mtCommission:double, --montant Commission apporteur
	mtCoc:double, --montant CoC
	reseau:chararray
);

Contrat = FOREACH Contrat GENERATE mois AS mois, noClient AS noClient, reseau AS reseau, noCtrScr AS noCtrScr;
Contrat = DISTINCT Contrat;

client_contrat = JOIN Client BY (libReseau, noPse, moisProjAbsolu), Contrat BY (reseau,noClient,mois);

client_contrat = FOREACH client_contrat GENERATE libReseau AS libReseau, noPse AS noPse;

client_contrat = DISTINCT client_contrat;

client_contrat_grp = GROUP client_contrat BY (libReseau, noPse);

client_grp = GROUP Client BY (libReseau, noPse);

test = JOIN client_contrat_grp BY group, client_grp BY group;
--DESCRIBE test;

t =  FOREACH test GENERATE FLATTEN(client_grp::Client) AS 
(periodeTraitee, cdSi, noPse, daNai, cdSex, cdInseeCsp, noStr, cotMacDo, 
codeReseau, libReseau, noPseMaitre, moisProjRelatif, moisProjAbsolu, age, probaVivant); 

--new = FOREACH (GROUP t BY moisProjAbsolu) GENERATE group;

client_contrat_proba = FOREACH t GENERATE cdSi AS cdSi, libReseau AS libReseau, moisProjAbsolu AS mois, noPse AS noClient, probaVivant AS probaVivant, age AS age;

--nb = FOREACH (GROUP client_contrat ALL) GENERATE COUNT(client_contrat);
--
--client_contrat_right = JOIN Client BY (libReseau, noPse, moisProjAbsolu) RIGHT, Contrat BY (reseau,noClient,mois);
--
--nb_right = FOREACH (GROUP client_contrat_right ALL) GENERATE COUNT(client_contrat_right);
--
--nb_comp = UNION nb,nb_right;

client_contrat_proba = ORDER client_contrat_proba BY mois;
STORE client_contrat_proba INTO '/hdfs/staging/out/e1225/credit/contrat_proba_vivant' using PigStorage(';');