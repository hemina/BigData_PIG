PNB_Contrat = LOAD '/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150531/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150630/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150731/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150831/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20150930/09.07.SRV.2.CalculerPNB.Option.1.20160517-145553,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151031/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151130/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160131/09.07.SRV.2.CalculerPNB.Option.1.20160517-124324,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160229/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160331/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20160430/09.07.SRV.2.CalculerPNB.Option.1.20160517-091254,/hdfs/data/adhoc/RE/99/0907/ValeurClient/PNB/20151231/09.07.SRV.2.CalculerPNB.Option.1.20160517-164359'
 USING PigStorage(';') AS 
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

PNB_Contrat_grp = GROUP PNB_Contrat BY (cdSi,noPse,moisTraitement);

PNB_Tous_les_contrat = FOREACH PNB_Contrat_grp GENERATE FLATTEN(group) AS (cdSi,noPse,mois), SUM(PNB_Contrat.PNB) AS PNB_TOUS_CONTRATS;

PNB_Tous_les_contrat_grp = GROUP PNB_Tous_les_contrat BY mois;
--nombre de client par mois sur tous les contrats
nb_client_par_mois = FOREACH PNB_Tous_les_contrat_grp GENERATE group AS mois, COUNT_STAR(PNB_Tous_les_contrat) AS nb_client;



PNB_client = JOIN PNB_Tous_les_contrat BY (cdSi,noPse,mois), PNB_Contrat BY (cdSi,noPse,moisTraitement);

PNB_client1 = FOREACH PNB_client GENERATE mois AS mois, PNB_Contrat::cdSi AS cdSi, noCtrScr AS noCtrScr, PNB_Contrat::noPse AS noPse, age AS age, 
cdSex AS cdSex, cdInseeCsp AS cdInseeCsp, noStrGtn AS noStrGtn, cotMacDo AS cotMacDo, PNB AS PNB, PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS, cdSFam AS cdSFam;

PNB_client1 = DISTINCT PNB_client1;

SPLIT PNB_client1 INTO 
mois201505 IF mois =='20150531', 
mois201506 IF mois =='20150630', 
mois201507 IF mois =='20150731', 
mois201508 IF mois =='20150831', 
mois201509 IF mois =='20150930', 
mois201510 IF mois =='20151031', 
mois201511 IF mois =='20151130', 
mois201512 IF mois =='20151231', 
mois201601 IF mois =='20160131', 
mois201602 IF mois =='20160229',
mois201603 IF mois =='20160331',
mois201604 IF mois =='20160430';

clients_mois201505_06 = JOIN mois201505 BY (cdSi, noPse) FULL, mois201506 BY (cdSi, noPse);
clients_mois201506_07 = JOIN mois201506 BY (cdSi, noPse) FULL, mois201507 BY (cdSi, noPse);
clients_mois201507_08 = JOIN mois201507 BY (cdSi, noPse) FULL, mois201508 BY (cdSi, noPse);
clients_mois201508_09 = JOIN mois201508 BY (cdSi, noPse) FULL, mois201509 BY (cdSi, noPse);
clients_mois201509_10 = JOIN mois201509 BY (cdSi, noPse) FULL, mois201510 BY (cdSi, noPse);
clients_mois201510_11 = JOIN mois201510 BY (cdSi, noPse) FULL, mois201511 BY (cdSi, noPse);
clients_mois201511_12 = JOIN mois201511 BY (cdSi, noPse) FULL, mois201512 BY (cdSi, noPse);
clients_mois201512_01 = JOIN mois201512 BY (cdSi, noPse) FULL, mois201601 BY (cdSi, noPse);
clients_mois201601_02 = JOIN mois201601 BY (cdSi, noPse) FULL, mois201602 BY (cdSi, noPse);
clients_mois201602_03 = JOIN mois201602 BY (cdSi, noPse) FULL, mois201603 BY (cdSi, noPse);
clients_mois201603_04 = JOIN mois201603 BY (cdSi, noPse) FULL, mois201604 BY (cdSi, noPse);


SPLIT clients_mois201505_06 INTO
clients_sortants_mois201505_06 IF mois201506::cdSi IS NULL,
clients_entrants_mois201505_06 IF mois201505::cdSi IS NULL;

SPLIT clients_mois201506_07 INTO
clients_sortants_mois201506_07 IF mois201507::cdSi IS NULL,
clients_entrants_mois201506_07 IF mois201506::cdSi IS NULL;

SPLIT clients_mois201507_08 INTO
clients_sortants_mois201507_08 IF mois201508::cdSi IS NULL,
clients_entrants_mois201507_08 IF mois201507::cdSi IS NULL;

SPLIT clients_mois201508_09 INTO
clients_sortants_mois201508_09 IF mois201509::cdSi IS NULL,
clients_entrants_mois201508_09 IF mois201508::cdSi IS NULL;

SPLIT clients_mois201509_10 INTO
clients_sortants_mois201509_10 IF mois201510::cdSi IS NULL,
clients_entrants_mois201509_10 IF mois201509::cdSi IS NULL;

SPLIT clients_mois201510_11 INTO
clients_sortants_mois201510_11 IF mois201511::cdSi IS NULL,
clients_entrants_mois201510_11 IF mois201510::cdSi IS NULL;

SPLIT clients_mois201511_12 INTO
clients_sortants_mois201511_12 IF mois201512::cdSi IS NULL,
clients_entrants_mois201511_12 IF mois201511::cdSi IS NULL;

SPLIT clients_mois201512_01 INTO
clients_sortants_mois201512_01 IF mois201601::cdSi IS NULL,
clients_entrants_mois201512_01 IF mois201512::cdSi IS NULL;

SPLIT clients_mois201601_02 INTO
clients_sortants_mois201601_02 IF mois201602::cdSi IS NULL,
clients_entrants_mois201601_02 IF mois201601::cdSi IS NULL;

SPLIT clients_mois201602_03 INTO
clients_sortants_mois201602_03 IF mois201603::cdSi IS NULL,
clients_entrants_mois201602_03 IF mois201602::cdSi IS NULL;

SPLIT clients_mois201603_04 INTO
clients_sortants_mois201603_04 IF mois201604::cdSi IS NULL,
clients_entrants_mois201603_04 IF mois201603::cdSi IS NULL;


client1 = FOREACH clients_entrants_mois201505_06 GENERATE '2015-06-30' AS mois, 
mois201506::cdSi AS cdSi,
mois201506::noCtrScr AS noCtrScr,
mois201506::noPse AS noPse,
mois201506::age AS age,
mois201506::cdSex AS cdSex,
mois201506::cdInseeCsp AS cdInseeCsp,
mois201506::noStrGtn AS noStrGtn,
mois201506::cotMacDo AS cotMacDo,
mois201506::PNB AS PNB, 
mois201506::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201506::cdSFam AS cdSFam;

client2 = FOREACH clients_entrants_mois201506_07 GENERATE '2015-07-31' AS mois, 
mois201507::cdSi AS cdSi,
mois201507::noCtrScr AS noCtrScr,
mois201507::noPse AS noPse,
mois201507::age AS age,
mois201507::cdSex AS cdSex,
mois201507::cdInseeCsp AS cdInseeCsp,
mois201507::noStrGtn AS noStrGtn,
mois201507::cotMacDo AS cotMacDo,
mois201507::PNB AS PNB, 
mois201507::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201507::cdSFam AS cdSFam;

client3 = FOREACH clients_entrants_mois201507_08 GENERATE '2015-08-31' AS mois, 
mois201508::cdSi AS cdSi,
mois201508::noCtrScr AS noCtrScr,
mois201508::noPse AS noPse,
mois201508::age AS age,
mois201508::cdSex AS cdSex,
mois201508::cdInseeCsp AS cdInseeCsp,
mois201508::noStrGtn AS noStrGtn,
mois201508::cotMacDo AS cotMacDo,
mois201508::PNB AS PNB, 
mois201508::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201508::cdSFam AS cdSFam;

client4 = FOREACH clients_entrants_mois201508_09 GENERATE '2015-09-30' AS mois, 
mois201509::cdSi AS cdSi,
mois201509::noCtrScr AS noCtrScr,
mois201509::noPse AS noPse,
mois201509::age AS age,
mois201509::cdSex AS cdSex,
mois201509::cdInseeCsp AS cdInseeCsp,
mois201509::noStrGtn AS noStrGtn,
mois201509::cotMacDo AS cotMacDo,
mois201509::PNB AS PNB, 
mois201509::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201509::cdSFam AS cdSFam;

client5 = FOREACH clients_entrants_mois201509_10 GENERATE '2015-10-31' AS mois, 
mois201510::cdSi AS cdSi,
mois201510::noCtrScr AS noCtrScr,
mois201510::noPse AS noPse,
mois201510::age AS age,
mois201510::cdSex AS cdSex,
mois201510::cdInseeCsp AS cdInseeCsp,
mois201510::noStrGtn AS noStrGtn,
mois201510::cotMacDo AS cotMacDo,
mois201510::PNB AS PNB, 
mois201510::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201510::cdSFam AS cdSFam;

client6 = FOREACH clients_entrants_mois201510_11 GENERATE '2015-11-30' AS mois, 
mois201511::cdSi AS cdSi,
mois201511::noCtrScr AS noCtrScr,
mois201511::noPse AS noPse,
mois201511::age AS age,
mois201511::cdSex AS cdSex,
mois201511::cdInseeCsp AS cdInseeCsp,
mois201511::noStrGtn AS noStrGtn,
mois201511::cotMacDo AS cotMacDo,
mois201511::PNB AS PNB, 
mois201511::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201511::cdSFam AS cdSFam;

client7 = FOREACH clients_entrants_mois201511_12 GENERATE '2015-12-31' AS mois, 
mois201512::cdSi AS cdSi,
mois201512::noCtrScr AS noCtrScr,
mois201512::noPse AS noPse,
mois201512::age AS age,
mois201512::cdSex AS cdSex,
mois201512::cdInseeCsp AS cdInseeCsp,
mois201512::noStrGtn AS noStrGtn,
mois201512::cotMacDo AS cotMacDo,
mois201512::PNB AS PNB, 
mois201512::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201512::cdSFam AS cdSFam;

client8 = FOREACH clients_entrants_mois201512_01 GENERATE '2016-01-31' AS mois, 
mois201601::cdSi AS cdSi,
mois201601::noCtrScr AS noCtrScr,
mois201601::noPse AS noPse,
mois201601::age AS age,
mois201601::cdSex AS cdSex,
mois201601::cdInseeCsp AS cdInseeCsp,
mois201601::noStrGtn AS noStrGtn,
mois201601::cotMacDo AS cotMacDo,
mois201601::PNB AS PNB, 
mois201601::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201601::cdSFam AS cdSFam;

client9 = FOREACH clients_entrants_mois201601_02 GENERATE '2016-02-29' AS mois, 
mois201602::cdSi AS cdSi,
mois201602::noCtrScr AS noCtrScr,
mois201602::noPse AS noPse,
mois201602::age AS age,
mois201602::cdSex AS cdSex,
mois201602::cdInseeCsp AS cdInseeCsp,
mois201602::noStrGtn AS noStrGtn,
mois201602::cotMacDo AS cotMacDo,
mois201602::PNB AS PNB, 
mois201602::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201602::cdSFam AS cdSFam;

client10 = FOREACH clients_entrants_mois201602_03 GENERATE '2016-03-31' AS mois, 
mois201603::cdSi AS cdSi,
mois201603::noCtrScr AS noCtrScr,
mois201603::noPse AS noPse,
mois201603::age AS age,
mois201603::cdSex AS cdSex,
mois201603::cdInseeCsp AS cdInseeCsp,
mois201603::noStrGtn AS noStrGtn,
mois201603::cotMacDo AS cotMacDo,
mois201603::PNB AS PNB, 
mois201603::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201603::cdSFam AS cdSFam;

client11 = FOREACH clients_entrants_mois201603_04 GENERATE '2016-04-30' AS mois, 
mois201604::cdSi AS cdSi,
mois201604::noCtrScr AS noCtrScr,
mois201604::noPse AS noPse,
mois201604::age AS age,
mois201604::cdSex AS cdSex,
mois201604::cdInseeCsp AS cdInseeCsp,
mois201604::noStrGtn AS noStrGtn,
mois201604::cotMacDo AS cotMacDo,
mois201604::PNB AS PNB, 
mois201604::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201604::cdSFam AS cdSFam;
--tous les nouveaux clients par mois
clients_entrants = UNION client1,client2,client3,client4,client5,client6,client7,client8,client9,client10,client11;


contrats_mois201505_06 = JOIN mois201505 BY (cdSi, noCtrScr) FULL, mois201506 BY (cdSi, noCtrScr);
contrats_mois201506_07 = JOIN mois201506 BY (cdSi, noCtrScr) FULL, mois201507 BY (cdSi, noCtrScr);
contrats_mois201507_08 = JOIN mois201507 BY (cdSi, noCtrScr) FULL, mois201508 BY (cdSi, noCtrScr);
contrats_mois201508_09 = JOIN mois201508 BY (cdSi, noCtrScr) FULL, mois201509 BY (cdSi, noCtrScr);
contrats_mois201509_10 = JOIN mois201509 BY (cdSi, noCtrScr) FULL, mois201510 BY (cdSi, noCtrScr);
contrats_mois201510_11 = JOIN mois201510 BY (cdSi, noCtrScr) FULL, mois201511 BY (cdSi, noCtrScr);
contrats_mois201511_12 = JOIN mois201511 BY (cdSi, noCtrScr) FULL, mois201512 BY (cdSi, noCtrScr);
contrats_mois201512_01 = JOIN mois201512 BY (cdSi, noCtrScr) FULL, mois201601 BY (cdSi, noCtrScr);
contrats_mois201601_02 = JOIN mois201601 BY (cdSi, noCtrScr) FULL, mois201602 BY (cdSi, noCtrScr);
contrats_mois201602_03 = JOIN mois201602 BY (cdSi, noCtrScr) FULL, mois201603 BY (cdSi, noCtrScr);
contrats_mois201603_04 = JOIN mois201603 BY (cdSi, noCtrScr) FULL, mois201604 BY (cdSi, noCtrScr);

SPLIT contrats_mois201505_06 INTO
contrats_sortants_mois201505_06 IF mois201506::cdSi IS NULL,
contrats_entrants_mois201505_06 IF mois201505::cdSi IS NULL;

SPLIT contrats_mois201506_07 INTO
contrats_sortants_mois201506_07 IF mois201507::cdSi IS NULL,
contrats_entrants_mois201506_07 IF mois201506::cdSi IS NULL;

SPLIT contrats_mois201507_08 INTO
contrats_sortants_mois201507_08 IF mois201508::cdSi IS NULL,
contrats_entrants_mois201507_08 IF mois201507::cdSi IS NULL;

SPLIT contrats_mois201508_09 INTO
contrats_sortants_mois201508_09 IF mois201509::cdSi IS NULL,
contrats_entrants_mois201508_09 IF mois201508::cdSi IS NULL;

SPLIT contrats_mois201509_10 INTO
contrats_sortants_mois201509_10 IF mois201510::cdSi IS NULL,
contrats_entrants_mois201509_10 IF mois201509::cdSi IS NULL;

SPLIT contrats_mois201510_11 INTO
contrats_sortants_mois201510_11 IF mois201511::cdSi IS NULL,
contrats_entrants_mois201510_11 IF mois201510::cdSi IS NULL;

SPLIT contrats_mois201511_12 INTO
contrats_sortants_mois201511_12 IF mois201512::cdSi IS NULL,
contrats_entrants_mois201511_12 IF mois201511::cdSi IS NULL;

SPLIT contrats_mois201512_01 INTO
contrats_sortants_mois201512_01 IF mois201601::cdSi IS NULL,
contrats_entrants_mois201512_01 IF mois201512::cdSi IS NULL;

SPLIT contrats_mois201601_02 INTO
contrats_sortants_mois201601_02 IF mois201602::cdSi IS NULL,
contrats_entrants_mois201601_02 IF mois201601::cdSi IS NULL;

SPLIT contrats_mois201602_03 INTO
contrats_sortants_mois201602_03 IF mois201603::cdSi IS NULL,
contrats_entrants_mois201602_03 IF mois201602::cdSi IS NULL;

SPLIT contrats_mois201603_04 INTO
contrats_sortants_mois201603_04 IF mois201604::cdSi IS NULL,
contrats_entrants_mois201603_04 IF mois201603::cdSi IS NULL;


contrat1 = FOREACH contrats_entrants_mois201505_06 GENERATE '2015-06-30' AS mois, 
mois201506::cdSi AS cdSi,
mois201506::noCtrScr AS noCtrScr,
mois201506::noPse AS noPse,
mois201506::age AS age,
mois201506::cdSex AS cdSex,
mois201506::cdInseeCsp AS cdInseeCsp,
mois201506::noStrGtn AS noStrGtn,
mois201506::cotMacDo AS cotMacDo,
mois201506::PNB AS PNB, 
mois201506::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201506::cdSFam AS cdSFam;

contrat2 = FOREACH contrats_entrants_mois201506_07 GENERATE '2015-07-31' AS mois, 
mois201507::cdSi AS cdSi,
mois201507::noCtrScr AS noCtrScr,
mois201507::noPse AS noPse,
mois201507::age AS age,
mois201507::cdSex AS cdSex,
mois201507::cdInseeCsp AS cdInseeCsp,
mois201507::noStrGtn AS noStrGtn,
mois201507::cotMacDo AS cotMacDo,
mois201507::PNB AS PNB, 
mois201507::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201507::cdSFam AS cdSFam;

contrat3 = FOREACH contrats_entrants_mois201507_08 GENERATE '2015-08-31' AS mois, 
mois201508::cdSi AS cdSi,
mois201508::noCtrScr AS noCtrScr,
mois201508::noPse AS noPse,
mois201508::age AS age,
mois201508::cdSex AS cdSex,
mois201508::cdInseeCsp AS cdInseeCsp,
mois201508::noStrGtn AS noStrGtn,
mois201508::cotMacDo AS cotMacDo,
mois201508::PNB AS PNB, 
mois201508::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201508::cdSFam AS cdSFam;

contrat4 = FOREACH contrats_entrants_mois201508_09 GENERATE '2015-09-30' AS mois, 
mois201509::cdSi AS cdSi,
mois201509::noCtrScr AS noCtrScr,
mois201509::noPse AS noPse,
mois201509::age AS age,
mois201509::cdSex AS cdSex,
mois201509::cdInseeCsp AS cdInseeCsp,
mois201509::noStrGtn AS noStrGtn,
mois201509::cotMacDo AS cotMacDo,
mois201509::PNB AS PNB, 
mois201509::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201509::cdSFam AS cdSFam;

contrat5 = FOREACH contrats_entrants_mois201509_10 GENERATE '2015-10-31' AS mois, 
mois201510::cdSi AS cdSi,
mois201510::noCtrScr AS noCtrScr,
mois201510::noPse AS noPse,
mois201510::age AS age,
mois201510::cdSex AS cdSex,
mois201510::cdInseeCsp AS cdInseeCsp,
mois201510::noStrGtn AS noStrGtn,
mois201510::cotMacDo AS cotMacDo,
mois201510::PNB AS PNB, 
mois201510::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201510::cdSFam AS cdSFam;

contrat6 = FOREACH contrats_entrants_mois201510_11 GENERATE '2015-11-30' AS mois, 
mois201511::cdSi AS cdSi,
mois201511::noCtrScr AS noCtrScr,
mois201511::noPse AS noPse,
mois201511::age AS age,
mois201511::cdSex AS cdSex,
mois201511::cdInseeCsp AS cdInseeCsp,
mois201511::noStrGtn AS noStrGtn,
mois201511::cotMacDo AS cotMacDo,
mois201511::PNB AS PNB, 
mois201511::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201511::cdSFam AS cdSFam;

contrat7 = FOREACH contrats_entrants_mois201511_12 GENERATE '2015-12-31' AS mois, 
mois201512::cdSi AS cdSi,
mois201512::noCtrScr AS noCtrScr,
mois201512::noPse AS noPse,
mois201512::age AS age,
mois201512::cdSex AS cdSex,
mois201512::cdInseeCsp AS cdInseeCsp,
mois201512::noStrGtn AS noStrGtn,
mois201512::cotMacDo AS cotMacDo,
mois201512::PNB AS PNB, 
mois201512::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201512::cdSFam AS cdSFam;

contrat8 = FOREACH contrats_entrants_mois201512_01 GENERATE '2016-01-31' AS mois, 
mois201601::cdSi AS cdSi,
mois201601::noCtrScr AS noCtrScr,
mois201601::noPse AS noPse,
mois201601::age AS age,
mois201601::cdSex AS cdSex,
mois201601::cdInseeCsp AS cdInseeCsp,
mois201601::noStrGtn AS noStrGtn,
mois201601::cotMacDo AS cotMacDo,
mois201601::PNB AS PNB, 
mois201601::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201601::cdSFam AS cdSFam;

contrat9 = FOREACH contrats_entrants_mois201601_02 GENERATE '2016-02-29' AS mois, 
mois201602::cdSi AS cdSi,
mois201602::noCtrScr AS noCtrScr,
mois201602::noPse AS noPse,
mois201602::age AS age,
mois201602::cdSex AS cdSex,
mois201602::cdInseeCsp AS cdInseeCsp,
mois201602::noStrGtn AS noStrGtn,
mois201602::cotMacDo AS cotMacDo,
mois201602::PNB AS PNB, 
mois201602::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201602::cdSFam AS cdSFam;

contrat10 = FOREACH contrats_entrants_mois201602_03 GENERATE '2016-03-31' AS mois, 
mois201603::cdSi AS cdSi,
mois201603::noCtrScr AS noCtrScr,
mois201603::noPse AS noPse,
mois201603::age AS age,
mois201603::cdSex AS cdSex,
mois201603::cdInseeCsp AS cdInseeCsp,
mois201603::noStrGtn AS noStrGtn,
mois201603::cotMacDo AS cotMacDo,
mois201603::PNB AS PNB, 
mois201603::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201603::cdSFam AS cdSFam;

contrat11 = FOREACH contrats_entrants_mois201603_04 GENERATE '2016-04-30' AS mois, 
mois201604::cdSi AS cdSi,
mois201604::noCtrScr AS noCtrScr,
mois201604::noPse AS noPse,
mois201604::age AS age,
mois201604::cdSex AS cdSex,
mois201604::cdInseeCsp AS cdInseeCsp,
mois201604::noStrGtn AS noStrGtn,
mois201604::cotMacDo AS cotMacDo,
mois201604::PNB AS PNB, 
mois201604::PNB_TOUS_CONTRATS AS PNB_TOUS_CONTRATS,
mois201604::cdSFam AS cdSFam;
--tous les nouveaux contrats par mois
contrats_entrants = UNION contrat1,contrat2,contrat3,contrat4,contrat5,contrat6,contrat7,contrat8,contrat9,contrat10,contrat11;

tous_nouveaux_contrats = JOIN contrats_entrants BY (cdSi, noPse) LEFT OUTER, clients_entrants BY (cdSi, noPse);

SPLIT tous_nouveaux_contrats INTO 
anciens_clients IF clients_entrants::cdSi IS NULL,
nouveaux_clients IF clients_entrants::cdSi IS NOT NULL;

anciens_clients_grp = GROUP anciens_clients BY contrats_entrants::mois;
nb_ancien_client_par_mois = FOREACH anciens_clients_grp GENERATE group AS mois, COUNT_STAR(anciens_clients) AS nb;

nb_client_par_mois = ORDER nb_client_par_mois BY mois;
nb_ancien_client_par_mois = ORDER nb_ancien_client_par_mois BY mois;

clients_entrants = ORDER clients_entrants BY mois;
contrats_entrants = ORDER contrats_entrants BY mois;

anciens_clients = ORDER anciens_clients BY contrats_entrants::mois;
nouveaux_clients = ORDER nouveaux_clients BY contrats_entrants::mois;

STORE nb_client_par_mois INTO '/hdfs/staging/out/e1225/model_nb_client_par_mois' using PigStorage(';');
STORE nb_ancien_client_par_mois INTO '/hdfs/staging/out/e1225/model_nb_ancien_client_par_mois' using PigStorage(';');

STORE clients_entrants INTO '/hdfs/staging/out/e1225/model_clients_entrants' using PigStorage(';');
STORE contrats_entrants INTO '/hdfs/staging/out/e1225/model_contrats_entrants' using PigStorage(';');

STORE anciens_clients INTO '/hdfs/staging/out/e1225/model_anciens_clients' using PigStorage(';');
STORE nouveaux_clients INTO '/hdfs/staging/out/e1225/model_nouveaux_clients' using PigStorage(';');
