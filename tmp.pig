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

-- On ne garde que les champs utiles au comptage des contrats, code SI et numéro de contrat
PNB_Contrat_1 = FOREACH PNB_Contrat GENERATE
moisTraitement AS mois, 
daOuv AS daOuv,
cdSi AS cdSi, 
noCtrScr AS noCtrScr, 
cdFam AS cdFam,
cdSFam AS cdSFam,
noPse AS noPse, 
age AS age, 
cdSex AS cdSex, 
cdInseeCsp AS cdInseeCsp, 
noStrGtn AS noStrGtn, 
cotMacDo AS cotMacDo;

-- On fait un distinct pour supprimer les doublons
PNB_Contrat_1 = DISTINCT PNB_Contrat_1;

SPLIT PNB_Contrat_1 INTO 
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
