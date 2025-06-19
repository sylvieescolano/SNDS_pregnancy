/*Objectif: associer les séjours d'accouchement avec 1 ou plusieurs séjours nouveau-né
Etapes:
-Identifier les séjours nouveau né
-Garder séjours accouchement dans la base (ENV, MN, IMGAPR22)
-Chainage à partir de ID MAM ENF et ETA NUM
-Exploration du premier chainage et filtrage du/des ID MAM ENF suspects
-Filtre sur delta en jours entre séjour nouveau né et date évenement maternel

-Retrait des NIR ANO 17 BB associés à plusieurs séj mat

Puis chainage avec IR BEN R (la table peut évoluer avec les MAJ)

-Exploration sej mat associés à plusieurs séjours NN 

variables selon qualité du chainage :
- IRBENR_OK (NA171 présent dans IRBENR)
-BNA_OK



*/

title;
proc datasets lib=orauser kill;
run;quit;

proc datasets lib=work kill;
run;quit;

/*CHOIX DU REPERTOIRE ET AJOUT DES MACROS*/
/*EQ*/
/*libname rep'/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024';
%INCLUDE '/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024/macros.sas';
/*SE*/
libname rep'/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024';
%INCLUDE '/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024/macros.sas';






/* liste des finess geographiques APHP, APHM et HCL a  supprimer pour eviter les doublons */
%let finess_out = '130780521' '130783236' '130783293' '130784234' '130804297'
                  '600100101' '750041543' '750100018' '750100042' '750100075'
                  '750100083' '750100091' '750100109' '750100125' '750100166'
                  '750100208' '750100216' '750100232' '750100273' '750100299'
                  '750801441' '750803447' '750803454' '910100015' '910100023'
                  '920100013' '920100021' '920100039' '920100047' '920100054'
                  '920100062' '930100011' '930100037' '930100045' '940100027'
                  '940100035' '940100043' '940100050' '940100068' '950100016'
                  '690783154' '690784137' '690784152' '690784178' '690787478'
                  '830100558';

/*Récupérer les séjours nouveaux-nés*/
/*Attention par rapport à la fiche SNDS la variable mode d'entrée ENT_MOD
peut être 8 (domicile) ou N (naissance)*/


/* Macro pour recuperer et filtrer sejour naissance dans le PMSI avec Choix de l'année */


%macro sejN(aaaa);

/*MODIFIER nom du dernier lot si annee incomplete*/ 
%if &aaaa.=2024 %then %let aa=24/*_09*/;
%else %let aa = %substr(&aaaa.,3,2); 

proc sql; create table work.sejN_v1_&aaaa. as
select 

/*Variables apparaissant en 2012 et 2014)*/
%if &aaaa.>2012 %then %do; 
t2.COH_NAI_RET, t2.COH_SEX_RET, 
%end;

%if &aaaa.>2014 %then %do; t1.typ_gen_rsa, t2.rng_nai, %end;

/*Variable de chainage mere enfant a partir de MCO 2013 (NIR ANO MAM jusqu'en 2019 puis ID MAM ENF)*/
%if &aaaa.>2012 and &aaaa.<2019 %then %do;
t2.NIR_ANO_MAM as ID_MAM_ENF, %end;

%if &aaaa.>2018 %then %do; t2.ID_MAM_ENF, %end;

t1.eta_num, t1.rsa_num,t1.sej_typ, t1.grg_ghm, t1.del_reg_ent, t1.age_ges, 
t1.cod_sex, t1.poi_nai, 
t2.nir_ano_17, t2.exe_soi_dtd, t2.exe_soi_dtf,


t2.FHO_RET, t2.NIR_RET, t2.NAI_RET, t2.SEX_RET, t2.DAT_RET, t2.SEJ_RET, t2.PMS_RET
from oravue.t_mco&aa.b as t1, oravue.t_mco&aa.c as t2

  WHERE (t1.ETA_NUM = t2.ETA_NUM AND t1.RSA_NUM = t2.RSA_NUM) 
AND (t1.GRG_GHM LIKE '15%' AND t1.AGE_JOU = 0 
AND (t1.ENT_MOD = '8' OR t1.ENT_MOD = 'N' ) 
AND ( t1.POI_NAI >= 500 OR t1.AGE_GES >= 22 ));
quit;



/* Filtrage sur GHM en erreurs, FINESS à retirer, codes retours*/

data work.sejN_&aaaa. (keep= eta_num rsa_num grg_ghm exe_soi_dtd exe_soi_dtf 
 nir_ano_17 id_mam_enf del_reg_ent age_ges poi_nai cod_sex rng_nai indsej);

set work.sejN_v1_&aaaa. (where=(
%if &aaaa.>2012 %then %do; 
COH_NAI_RET = '0' AND COH_SEX_RET = '0' AND
%end;
 FHO_RET = '0' AND NIR_RET = '0' AND
NAI_RET = '0' AND SEX_RET = '0' AND DAT_RET = '0' AND SEJ_RET = '0' AND PMS_RET = '0'
AND (NIR_ANO_17 not in ('xxxxxxxxxxxxxxxxx' 'XXXXXXXXXXXXXXXXD'))
AND (SEJ_TYP <> 'B' or SEJ_TYP is null)
%if &aaaa.>2014 %then %do; 
AND (TYP_GEN_RSA = '0') 
%end;));

if cmiss(eta_num,rsa_num,exe_soi_dtd) = 0 then indsej=eta_num||rsa_num||exe_soi_dtd; else delete;
if eta_num in (&finess_out.) then delete;
if grg_ghm =: "90" then delete;
run;

%mend;

%macro loop;
    %do year = 2013 %to 2024;
        %sejN (aaaa=&year.);
    %end;
%mend loop;

%loop;

/*Reunir tous les séjours de naissance*/


data work.sej_N;
	set WORK.SEJN_2: ;
run; 




/*repérage des séjours avec doublons indsej*/
PROC SQL;
   CREATE TABLE WORK.view_doublons_indsej AS 
   SELECT t1.*,
          /* COUNT_of_indsej */
            (COUNT(t1.indsej)) AS COUNT_of_indsej
      FROM WORK.sej_N t1
      GROUP BY t1.indsej
      HAVING (CALCULATED COUNT_of_indsej) > 1
      ORDER BY t1.indsej;
QUIT;


/*retrait doublons*/
PROC SQL;
   CREATE TABLE work.base1_N AS 
   SELECT t1.*
      FROM work.sej_N t1
	  LEFT JOIN WORK.view_doublons_indsej t2
      ON t1.indsej = t2.indsej
	  WHERE t2.indsej IS NULL
      ;
QUIT;



/*Compter les ID MAM ENF manquant dans base NN*/
proc sql ;
select distinct count(*) 
from work.base1_N
where ID_mam_enf is missing;
quit;





/**CHAINAGE DES ACCOUCHEMENTS**/

/*Récuperer base sej grossesse pour les issues "accouchements" (ENV,MN, IMGAPR22)
(+ retirer si ID MAM ENF manquant)*/

TITLE ;
PROC SQL;
   CREATE TABLE WORK.sej_acc_brut AS 
   SELECT *
      FROM rep.baseG t1
      WHERE t1.issue IN ('ENV','MN','IMGAPR22');
QUIT;
proc freq data=WORK.sej_acc_brut ;
		tables issue ;
	run;

/*exclusion des sejours avec ID MAM ENF manquants*/
title "apres exclusion ID MAM ENF manquants";
PROC SQL;
   CREATE TABLE WORK.sej_acc AS 
   SELECT *
      FROM rep.baseG t1
      WHERE t1.issue IN ('ENV','MN','IMGAPR22')
AND t1.ID_MAM_ENF NOT IS MISSING;
QUIT;

proc freq data=WORK.sej_acc ;
		tables issue ;
	run;





/*PREMIER TEST DE CHAINAGE*/
/*right join des 2 tables SUR ID_mam_enf uniquement et ETA_NUM
ATTENTION les variables provenant des séjours nouveau né sont suivies par '1'
sauf POI_NAI et RNG_NAI*/


PROC SQL;
   CREATE TABLE WORK.chainage1 AS 
   SELECT t1.AGE_ANN, 
          t1.AGE_GES, 
          t1.BEN_NIR_ANO, 
          t1.dat_evt, 
          t1.dureeG, 
          t1.EXE_SOI_DTD, 
          t1.EXE_SOI_DTF,
 			t1.sor_mod, 
          t1.GRG_GHM, 
          t1.ID_MAM_ENF, 
          t1.indsej, 
          t1.issue, 
          t1.lmp_calc, 
          t1.NIR_ANO_17, 
          t1.RSA_NUM, 
          t1.ETA_NUM, 
          t2.ETA_NUM AS ETA_NUM1, 
          t2.AGE_GES AS AGE_GES1,  
          t2.DEL_REG_ENT AS DEL_REG_ENT1, 
          t2.EXE_SOI_DTD AS EXE_SOI_DTD1, 
          t2.EXE_SOI_DTF AS EXE_SOI_DTF1, 
          t2.GRG_GHM AS GRG_GHM1, 
          t2.indsej AS indsej1, 
          t2.NIR_ANO_17 AS NIR_ANO_171, 
          t2.RSA_NUM AS RSA_NUM1, 
		  t2.COD_SEX AS COD_SEX1,
		  t2.POI_NAI,
		  t2.RNG_NAI,

          /* Delta */
            (datepart(t2.EXE_SOI_DTD) - t1.dat_evt) AS Delta
      FROM WORK.base1_N t2
           RIGHT JOIN WORK.SEJ_ACC t1 ON (t2.ETA_NUM = t1.ETA_NUM) AND (t2.ID_MAM_ENF = t1.ID_MAM_ENF)
      ORDER BY Delta DESC,
               t1.ETA_NUM,
               t1.RSA_NUM;
QUIT;




/*Comptage du nb de sejours sans chainage par type d'ISSUE */
PROC SQL;
   CREATE TABLE WORK.count_chainage AS 
   SELECT DISTINCT /* CHAINAGE */
                     (t1.indsej1 is NOT missing) AS CHAINAGE, 
          t1.indsej, 
          t1.issue
      FROM WORK.CHAINAGE1 t1
ORDER BY t1.issue;
QUIT;

title"Nb de sej_ACC selon chainage";

proc freq data=WORK.count_chainage;
by issue;
		tables CHAINAGE;
	run;







/*Macro d'EXPLORATION des tables chainage:

	Visualiser pour chaque indsej (index sejour mat):
-le nb de indsej1 (index sejour NN)
-le nb de NIRANO171 (NIR NN)
(jumeaux ou triplés de meme sexe peuvent avoir le meme NIR)
_ le nb de Poids de Naissance distincts (permet de différencier les NN)
-le max de RNG_NAI (pour exploration car utilisation non recommandée)-
-le min et max de Delta (nb de jours entre date evt et debut sejour NN)*/

%MACRO explo(table);
PROC SQL;
   CREATE TABLE WORK.explo_&table. AS 
   SELECT t1.indsej, 
          /* COUNT_DISTINCT_of_indsej1 */
            (COUNT(DISTINCT(t1.indsej1))) AS Nb_sej_NN_distincts, 
          /* COUNT_DISTINCT_of_BEN_NIR_ANO1 */
            (COUNT(DISTINCT(t1.NIR_ANO_171))) AS Nb_NIR_ANO_171_distincts, 
			(COUNT(DISTINCT(t1.POI_NAI))) AS Nb_POI_NAI_distincts, 
			(MAX(t1.RNG_NAI)) AS MAX_RNG_NAI, 
          /* MIN_of_Delta */
            (MIN(t1.Delta)) AS MIN_of_Delta, 
          /* MAX_of_Delta */
            (MAX(t1.Delta)) AS MAX_of_Delta
      FROM work.&table. t1
      GROUP BY t1.indsej
      ORDER BY Nb_NIR_ANO_171_distincts DESC,
               Nb_sej_NN_distincts DESC;
quit;
%mend;

%explo(chainage1);



title"Nb de sej_NN par sej maternel apres chainage sur IDMAM ENF ET ETA NUM";
proc freq data=work.explo_CHAINAGE1;
		tables Nb_sej_NN_distincts;
	run;






/*repérage des anomalies sur ID MAM ENF 
	Pour un ETA NUM, un ID MAM ENF et une dat_evt, cb d'indsej différents 
	retrouvés par le premier chainage (nb de sejours maternels)*/

PROC SQL;
   CREATE TABLE WORK.anom_id AS 
   SELECT t1.ETA_NUM, 
          t1.ID_MAM_ENF, 
          t1.dat_evt, 
          /* COUNT_DISTINCT_of_indsej */
            (COUNT(DISTINCT(t1.indsej))) AS COUNT_DISTINCT_of_indsej
      FROM WORK.CHAINAGE1 t1
      GROUP BY t1.ETA_NUM,
               t1.ID_MAM_ENF,
               t1.dat_evt
      ORDER BY COUNT_DISTINCT_of_indsej DESC;
QUIT;




/*Identification du ou des ID mam enf associés a plus d'un sej par date
et par établissement */
PROC SQL;
   CREATE TABLE WORK.ID_a_exclure AS 
   SELECT DISTINCT t1.ID_MAM_ENF
      FROM WORK.ANOM_ID t1
      WHERE t1.COUNT_DISTINCT_of_indsej NOT = 1;
QUIT;






/*CHAINAGE 2 :Filtre en retirant ID MAM ENF suspect */

PROC SQL;
   CREATE TABLE WORK.CHAINAGE2 AS 
   SELECT *
      FROM WORK.CHAINAGE1 t1
      WHERE t1.ID_MAM_ENF NOT in 
(select id_mam_enf from WORK.ID_a_exclure);
QUIT;


/*explo CHAINAGE2*/

%explo(chainage2);


title"Nb de sej_NN par sej maternel apres chainage sur IDMAM ENF ET ETA NUM
 et retrait IDMAMENF suspects";
proc freq data=work.explo_CHAINAGE2;
		tables Nb_sej_NN_distincts;
	run;







/*	Exploration pour choix du delta entre sej NN et sejour mat
	Table des Delta quand Nb_sej_NN_distincts =1 
(cad quand 1 seul sejour nouveau né retrouvé par sejour maternel avec IDMAMENF seulement)*/

PROC SQL;
   CREATE TABLE WORK.delta_CHAINAGE2 AS 
   SELECT t1.indsej, 
          t1.MAX_of_Delta AS Delta
      FROM WORK.EXPLO_CHAINAGE2 t1
      WHERE t1.Nb_sej_NN_distincts = 1
      ORDER BY t1.MAX_of_Delta;
QUIT;


title"Nb de sej_ACC selon delta séjour nouveau né
(quand un seul sejour nouveau né par séjour mat)";
proc freq data=WORK.delta_CHAINAGE2;
tables Delta;	run;



/*CHAINAGE3: Retrait des Delta non compris entre -1 et 1 */

PROC SQL;
   CREATE TABLE WORK.chainage3 AS 
   SELECT *
      FROM WORK.CHAINAGE2 t1
      WHERE t1.Delta BETWEEN -1 AND 1
      ORDER BY t1.indsej;
QUIT;

/*Exploration suite au chainage3*/


%explo(chainage3);

title"Nb de sej_NN par sej maternel apres
filtre sur delta entre -1 et +1";
proc freq data=WORK.explo_chainage3;
		tables Nb_sej_NN_distincts;
	run;




/*selon issue*/

PROC SQL;
   CREATE TABLE WORK.EXPLO_CHAINAGE3_issue AS 
   SELECT DISTINCT t1.indsej, 
          t1.Nb_sej_NN_distincts, 
          t2.issue
      FROM WORK.EXPLO_CHAINAGE3 t1
           INNER JOIN WORK.CHAINAGE3 t2 ON (t1.indsej = t2.indsej);
QUIT;

	title"Nb de sej_NN par sej maternel apres
filtre sur delta entre -1 et +1";
proc freq data=WORK.explo_chainage3_issue;
		tables Nb_sej_NN_distincts*issue;
	run;








/** FILTRE NIR nouveaux nés **/


/*Compter le nb de BEN NIR ANO (ID mat) pour chaque NIR ANO 171 (ID BB)*/

PROC SQL;
   CREATE TABLE WORK.count_CHAINAGE3 AS 
   SELECT /* COUNT_of_BEN_NIR_ANO */
            (COUNT (distinct(t1.BEN_NIR_ANO))) AS COUNT_of_BEN_NIR_ANO, 
          t1.NIR_ANO_171
      FROM WORK.CHAINAGE3 t1
      GROUP BY t1.NIR_ANO_171
      ORDER BY COUNT_of_BEN_NIR_ANO DESC;
QUIT;



/*CHAINAGE4: retrait des ID BB associés à plusieurs ID mat associés à plusieurs séjours NN*/

PROC SQL;
   CREATE TABLE WORK.chainage4 AS 
   SELECT t2.*
      FROM WORK.COUNT_chainage3 t1
           INNER JOIN WORK.chainage3 t2 ON (t1.NIR_ANO_171 = t2.NIR_ANO_171)
      WHERE t1.COUNT_of_BEN_NIR_ANO <2;
QUIT;



%explo(chainage4);


/*CHAINAGE 5 retrait des sejours associés à >2 NIR ANO 17 BB
(seuls 2 PSA possibles pour triplés/quadruplés/quintuplés)*/


PROC SQL;
   CREATE TABLE WORK.chainage5 AS 
   SELECT t2.*,
   t1.nb_sej_NN_distincts AS Nb_NN
      FROM WORK.explo_chainage4 t1
           INNER JOIN WORK.chainage4 t2 ON (t1.indsej = t2.indsej)
      WHERE t1.nb_NIR_ANO_171_distincts <3;
QUIT;



/*Comptage du nb de sejours chaines apres chainage 5 par type d'ISSUE */
PROC SQL;
   CREATE TABLE WORK.count_chainage5 AS 
   SELECT DISTINCT
          t1.indsej, 
		  t1.nb_nn,
          t1.issue,
		  year(dat_evt) as annee
      FROM WORK.CHAINAGE5 t1
ORDER BY t1.issue;
QUIT;

title"Nb de sejours chaines apres chainage5";

proc freq data=WORK.count_chainage5;
		tables issue;
	run;

title "Grossesses multiples selon issue";
proc freq data=WORK.count_chainage5;
		tables nb_nn*issue;
	run;










/*Jointure des NIR_ANO_17 presents dans chainage5 avec les PSA de IR_BEN_R*/

/*ATTENTION les MAJ de IRBENR peuvent faire varier légèrement les sorties*/

/*Passage en table ORACLE pour la jointure (via ORAUSER)*/

PROC SQL;
   CREATE TABLE orauser.chainage5 AS 
   SELECT t1.*
      FROM WORK.chainage5 t1;
QUIT;

PROC SQL;
   CREATE TABLE WORK.SEJ_N_BEN AS 
   SELECT t1.BEN_CDI_NIR, 
          t1.BEN_NIR_ANO, 
          t1.BEN_NIR_PSA, 
          t1.BEN_RNG_GEM,
		  t2.indsej,
		  t2.issue,
		('1') as IRBENR_OK
      FROM oravue.IR_BEN_R t1 
           INNER JOIN WORK.chainage5 t2 ON (t1.BEN_NIR_PSA = t2.NIR_ANO_171)
      ORDER BY t1.BEN_NIR_ANO
	;
QUIT;

/*Nb de séjours avec PSA présent dans IR BEN R par type d'issue*/
PROC SQL;
   CREATE TABLE WORK.nb_sejour AS 
   SELECT DISTINCT /* COUNT_DISTINCT_of_indsej */
                     (COUNT(DISTINCT(t1.indsej))) AS COUNT_DISTINCT_of_indsej, 
          t1.issue
      FROM WORK.SEJ_N_BEN t1
      GROUP BY t1.issue;
QUIT;


title"Nb de BEN_NIR_PSA distincts présents dans IR_BEN_R";
proc sql;
    select count(distinct ben_nir_psa) from WORK.SEJ_N_BEN; quit;

%doublons(work.sej_n_ben);



/*Comparaison du nb de NN identifié a partir du chainage et de la catégorie attribuée
par la macro doublons */

PROC SQL;
   CREATE TABLE WORK.COMPARATIF AS 
   SELECT DISTINCT t2.indsej, 
          t2.indsej1, 
          t2.NIR_ANO_171, 
          t2.issue, 
          t2.POI_NAI, 
          t2.COD_SEX1, 
          t1.classe, 
          t2.Nb_NN
      FROM WORK.DOUBLE_RANG2 t1
           RIGHT JOIN WORK.CHAINAGE5 t2 ON (t1.BEN_NIR_PSA = t2.NIR_ANO_171)
      ORDER BY t2.Nb_NN DESC,
               t2.indsej;
QUIT;


title"Catégorie d'apres IRBENR pour les ENV et naissances uniques";
proc freq data=WORK.comparatif (where=(nb_nn=1 and issue='ENV'));
		tables nb_nn*classe;
	run;



/*Pour les ENV avec jumeaux avec Nb_NN=2*/

/*Sexe identique*/
PROC SQL;
   CREATE TABLE WORK.comparatif_jum_samesex AS 
   SELECT DISTINCT t1.indsej, 
          /* COUNT_DISTINCT_of_COD_SEX1 */
            (COUNT(DISTINCT(t1.COD_SEX1))) AS COUNT_DISTINCT_of_COD_SEX1, 
          /* COUNT_DISTINCT_of_classe */
            (COUNT(DISTINCT(t1.classe))) AS COUNT_DISTINCT_of_classe, 
          /* classe */
            ((MAX(t1.classe)) || '/' || (MIN(t1.classe))) AS classe
      FROM WORK.COMPARATIF t1
      WHERE t1.Nb_NN = 2 AND t1.issue = 'ENV'
      GROUP BY t1.indsej
      HAVING (CALCULATED COUNT_DISTINCT_of_COD_SEX1) = 1;
QUIT;



title"Catégorie d'apres IRBENR pour les ENV de jumeaux de meme sexe";
proc freq data=WORK.comparatif_jum_samesex  ;
		tables classe;
	run;

/*Sexe distinct*/
PROC SQL;
   CREATE TABLE WORK.comparatif_jum_diffsex AS 
   SELECT DISTINCT t1.indsej, 
          /* COUNT_DISTINCT_of_COD_SEX1 */
            (COUNT(DISTINCT(t1.COD_SEX1))) AS COUNT_DISTINCT_of_COD_SEX1, 
          /* COUNT_DISTINCT_of_classe */
            (COUNT(DISTINCT(t1.classe))) AS COUNT_DISTINCT_of_classe, 
          /* classe */
            ((MAX(t1.classe)) || '/' || (MIN(t1.classe))) AS classe
      FROM WORK.COMPARATIF t1
      WHERE t1.Nb_NN = 2 AND t1.issue = 'ENV'
      GROUP BY t1.indsej
      HAVING (CALCULATED COUNT_DISTINCT_of_COD_SEX1) = 2;
QUIT;



title"Catégorie d'apres IRBENR pour les ENV de jumeaux de sexe different";
proc freq data=WORK.comparatif_jum_diffsex  ;
		tables classe;
	run;









/*ajouter catégorie obtenue apres chainage IRBENR et macro doublons*/

PROC SQL;
   CREATE TABLE WORK.CHAINAGE6 AS 
   SELECT t1.*,
          t2.classe AS classNN, 
          /* TOP_CME */
            (1) AS TOP_CME
      FROM work.CHAINAGE5 t1
           LEFT JOIN WORK.COMPARATIF t2 ON (t1.indsej1 = t2.indsej1)
      ORDER BY t1.dat_evt;
QUIT;



/*Ajout de variable IRBENR_OK = PSA retrouvé dans referentiel IRBENR*/
PROC SQL;
   CREATE TABLE WORK.CHAINAGE7 AS 
   SELECT DISTINCT t1.*, 
			(t1.classNN IS NOT MISSING) AS IRBENR_OK
      FROM WORK.CHAINAGE6 t1
           LEFT JOIN WORK.SEJ_N_BEN t2 ON (t1.NIR_ANO_171 = t2.BEN_NIR_PSA)
           ;
QUIT;






/*AJOUT BEN NIR ANO ENFANT*/


/* Récupérer variables d'interet + changement format COD_SEX1
PASSAGE ORAUSER*/
PROC SQL;
   CREATE TABLE orauser.chainage8 AS 
   SELECT t1.classNN, 
          t1.indsej, 
          t1.indsej1, 
          t1.NIR_ANO_171, 
          /* COD_SEX1 */
            (input(t1.COD_SEX1, 1.)) AS COD_SEX1
      FROM WORK.CHAINAGE7 t1;
QUIT;

PROC SQL;
   CREATE TABLE WORK.CHAINAGE9 AS 
   SELECT DISTINCT t1.indsej, 
          t1.indsej1, 
          t1.NIR_ANO_171, 
          t1.COD_SEX1, 
          t1.classNN, 
          /* BEN_NIR_ANO1 */
            (CASE 
                WHEN (t1.classNN='unique' OR t1.classNN='jumeaux à tort') THEN t2.BEN_NIR_ANO 
                ELSE ''
            END) AS BEN_NIR_ANO1
      FROM orauser.chainage8 t1
           LEFT JOIN oravue.ir_ben_r t2 ON (t1.NIR_ANO_171 = t2.BEN_NIR_PSA) AND (t1.COD_SEX1 = 
          t2.BEN_SEX_COD);
QUIT;


/*Trouver les BNA1(BB) avec plusieurs indsej (sej mat) */

PROC SQL;
   CREATE TABLE WORK.count_CHAINAGE9 AS 
   SELECT t1.BEN_NIR_ANO1, 
          /* COUNT_DISTINCT_of_indsej */
            (COUNT(DISTINCT(t1.indsej))) AS COUNT_DISTINCT_of_indsej
      FROM WORK.CHAINAGE9 t1
	  where t1.BEN_NIR_ANO1 is not missing 
      GROUP BY t1.BEN_NIR_ANO1
      ORDER BY COUNT_DISTINCT_of_indsej DESC;
QUIT;


PROC SQL;
   CREATE TABLE WORK.dup_CHAINAGE9 AS 
   SELECT t1.BEN_NIR_ANO1
      FROM WORK.COUNT_CHAINAGE9 t1
      WHERE t1.COUNT_DISTINCT_of_indsej > 1;
QUIT;
/*Retirer de chainage 9 les sejours avec BNA1 associé a plusieurs séjours mat*/

PROC SQL;
   CREATE TABLE WORK.chainage10 AS 
   SELECT t1.*
      FROM WORK.chainage9 t1
           LEFT JOIN WORK.dup_CHAINAGE9 t2 ON (t1.BEN_NIR_ANO1 = t2.BEN_NIR_ANO1)
      WHERE t2.BEN_NIR_ANO1 IS NULL;
QUIT;








PROC SQL;
   CREATE TABLE REP.chainageME_ALL AS 
   SELECT DISTINCT 
   t2.dureeG, 
          t2.dat_evt, 
		  YEAR(dat_evt) AS Annee, 
          t2.lmp_calc, 
          t2.indsej, 
          t2.BEN_NIR_ANO, 
          t2.NIR_ANO_17, 
          t2.RSA_NUM, 
          t2.AGE_ANN, 
          t2.AGE_GES, 
          t2.Delta, 
          t2.ETA_NUM, 
          t2.EXE_SOI_DTD, 
          t2.EXE_SOI_DTF, 
		  t2.sor_mod, 
          t2.GRG_GHM, 
          t2.GRG_GHM1, 
          t2.ID_MAM_ENF, 
          t2.issue, 
          t2.Nb_NN, 
          t2.COD_SEX1, 
          t2.POI_NAI, 
          t2.indsej1, 
          t2.RSA_NUM1, 
          t2.NIR_ANO_171, 
          t2.EXE_SOI_DTD1, 
          t2.EXE_SOI_DTF1, 
          t2.IRBENR_OK, 
          t2.classNN,
          t2.Nb_NN, 
          t1.BEN_NIR_ANO1, 
          /* BNA_OK */
            (CASE WHEN t1.BEN_NIR_ANO1 is not missing THEN 1 ELSE 0 END) AS BNA_OK
      FROM WORK.CHAINAGE10 t1
           LEFT JOIN WORK.CHAINAGE7 t2 ON (t1.indsej1 = t2.indsej1);
QUIT;





/*Compte le nb de sej mat avec BNA OK par issue*/


PROC SQL;
   CREATE TABLE WORK.count_BNAOK AS 
   SELECT t1.issue, 
          /* COUNT_DISTINCT_of_indsej */
            (COUNT(DISTINCT(t1.indsej))) AS COUNT_DISTINCT_of_indsej
      FROM rep.CHAINAGEME_ALL t1
      WHERE t1.BNA_OK = 1
      GROUP BY t1.issue;
QUIT;




/*Nb de sejours avec au moins un PSA présent dans IRBENR par type d'issue*/
PROC SQL;
   CREATE TABLE WORK.count_IRBENROK AS 
   SELECT /* COUNT_DISTINCT_of_indsej */
            (COUNT(DISTINCT(t1.indsej))) AS COUNT_DISTINCT_of_indsej, 
          t1.issue
      FROM rep.CHAINAGEME_ALL t1
	      WHERE t1.IRBENR_OK=1
      GROUP BY t1.issue;
QUIT;



/*Ajouter la variable TOP_CME DANS BASEG: séjour présent dans la base chainée avec séjours NN*/
PROC SQL;
   CREATE TABLE rep.baseG AS 
   SELECT distinct T1.*, 
(CASE WHEN t2.indsej1 is not missing THEN 1 ELSE 0 END)AS TOP_CME
      FROM REP.BASEG t1
	      LEFT JOIN WORK.CHAINAGE10 T2 ON (t1.indsej=t2.indsej)
      ;
QUIT;
