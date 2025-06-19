/*objectifs: à partir des séjours de grossesse extraits du PMSI, construire une table
avec une ligne par séjour apres 3 etapes:
-filtre sur AGE
- CHAINAGE AVEC IR BEN R puis elimination des identifiants NIR ANO 17 non présents dans IR BEN R 
OU correspondant  a des jumelles certaines ou incertaines
OU non associés à un BEN NIR ANO (nouvel identifiant de référence)
*/



/* Suppression fichiers ORAUSER  ET WORK*/
proc datasets lib=orauser kill;
run;quit;

proc datasets lib=work kill;
run;quit;


/**TABLE REUNISSANT TOUS LES SEJOURS extraits du PMSI: BASE1**/

/*CHOIX DU REPERTOIRE*/
/*EQ*/
/*libname rep'/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024';
/*SE*/
libname rep'/home/sas/42a000245310899/sasdata/REPMEDGR/Base_Grossesse/Construction_base_20132024';

title"Nb de NIR_ANO_17 distincts dans la base initiale";
proc sql;
    select count(distinct NIR_ANO_17) from rep.base1; quit;




/***FILTRE SUR LES NIR ***/

/*Jointure des NIR_ANO_17 presents dans base1 avec les PSA de IR_BEN_R*/
/*ATTENTION les MAJ de IRBENR peuvent faire varier légèrement les sorties/

/*Passage en table ORACLE pour la jointure (via ORAUSER): liste des NA17 de BASE1*/

PROC SQL;
   CREATE TABLE orauser.base1 AS 
   SELECT DISTINCT t1.NIR_ANO_17
      FROM rep.base1 t1;
QUIT;


/*Récupérer tous les BNA associés aux PSA de SEJ_G_PMSI*/
PROC SQL;
   CREATE TABLE orauser.BNA_SEJG_PMSI AS 
   SELECT DISTINCT t2.BEN_NIR_ANO
      FROM orauser.base1 t1
           INNER JOIN oravue.ir_ben_r t2 ON (t1.NIR_ANO_17 = t2.BEN_NIR_PSA)
      WHERE t2.BEN_NIR_ANO NOT IS MISSING;
QUIT;

/*Récupérer tous les PSA de IRBENR associés à ces BNA + infos de la table*/
PROC SQL;
   CREATE TABLE WORK.SEJ_G_BEN AS 
   SELECT DISTINCT t1.BEN_CDI_NIR, 
          t1.BEN_NIR_ANO, 
          t1.BEN_NIR_PSA, 
          t1.BEN_RNG_GEM,
		  t1.BEN_SEX_COD,
		  t1.BEN_NAI_MOI,
		  t1.BEN_NAI_ANN,
		  t1.BEN_DCD_DTE
      FROM oravue.ir_ben_r t1 
           INNER JOIN orauser.BNA_SEJG_PMSI t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO)
      ORDER BY t1.BEN_NIR_ANO
	;
QUIT;








/*Macro de SPF pour comptage doublons*/

%macro doublons (table);
	/*% nir_ano renseigné*/
	title "Effectifs par identifiant et % de BEN_NIR_ANO renseignés";

	proc sql;
		select count(distinct BEN_NIR_ANO) as nb_ano, count(distinct BEN_NIR_PSA) as nb_psa, count(distinct BEN_NIR_PSA||put(BEN_RNG_GEM,1.)) as nb_gem, count(distinct BEN_NIR_ANO)/count(distinct BEN_NIR_PSA||put(BEN_RNG_GEM,1.))*100 as perc_ano from &table;
	quit; /*doublons de BEN_NIR_PSA*/

	proc sql;
		create table double_psa as select distinct BEN_NIR_ANO,count(distinct BEN_NIR_PSA) as nb_psa from &table where BEN_NIR_ANO is not null group by BEN_NIR_ANO;
	quit;

	title "Nombre d'ouvreurs de droit par individu (nb de BEN_NIR_PSA par BEN_NIR_ANO) pour les BEN_NIR_ANO connus";

	proc freq data=double_psa;
		tables nb_psa;
	run; /*doublons de rangs gémellaires*/

	proc sql;
		create table double_rang as select distinct BEN_NIR_PSA,count(distinct BEN_RNG_GEM) as nb_rangs, count(distinct BEN_NIR_ANO) as nb_ano, max(case when BEN_NIR_ANO is null then 1 else 0 end) as ano_vide from &table group by BEN_NIR_PSA;
	quit;

	proc sql;
		create table double_rang2 as select distinct BEN_NIR_PSA, case when nb_rangs=1 then 'unique' when nb_rangs>1 and nb_ano>1 then 'jumeaux' when nb_rangs>1 and nb_ano=1 and ano_vide=0 then 'jumeaux à tort' else 'NSP' end as classe from double_rang group by BEN_NIR_PSA;
	quit;

	title "Nombre de rangs gémellaires par BEN_NIR_PSA (jumeaux ou jumeaux à tort quand BEN_NIR_ANO connu)";

	proc freq data=double_rang2;
		tables classe;
	run;

%mend;

%doublons(work.sej_g_ben);

/*----------------------------------------------*/
/*On garde les PSA "jumeaux"*/

PROC SQL;
   CREATE TABLE work.psa_jum AS 
   SELECT t1.BEN_NIR_PSA
      FROM WORK.DOUBLE_RANG2 t1
      WHERE t1.classe IN ('jumeaux');
QUIT;

/*Récupérer les BNA correspondants*/
PROC SQL;
   CREATE TABLE WORK.BNA_a_exclure AS 
   SELECT DISTINCT t2.BEN_NIR_ANO
      FROM WORK.PSA_JUM t1
           INNER JOIN WORK.SEJ_G_BEN t2 ON (t1.BEN_NIR_PSA = t2.BEN_NIR_PSA);
QUIT;



/*Modifier SEJ_G_BEN */
PROC SQL;
   CREATE TABLE WORK.SEJ_G_BEN2 AS 
   SELECT DISTINCT t2.*
      FROM WORK.BNA_a_exclure t1
           RIGHT JOIN WORK.SEJ_G_BEN t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO)
WHERE t1.BEN_NIR_ANO IS NULL;
QUIT;

TITLE "après suppression jumeaux";
%doublons(work.sej_g_ben2);



/*Recuperer les ID de SEJ G BEN 2 (sans jumeaux) avec jointure NA17 base1*/
PROC SQL;
   CREATE TABLE WORK.chainage_sejG_PMSI AS 
   SELECT DISTINCT t2.NIR_ANO_17, 
          t1.BEN_NIR_ANO
      FROM WORK.SEJ_G_BEN2 t1
           RIGHT JOIN REP.base1 t2 ON (t1.BEN_NIR_PSA = t2.NIR_ANO_17);
QUIT;




/*Descriptif de la table obtenue*/

	/*% nir_ano renseigné, nb PSA seul, nb PSA, nb BNA*/
	title "Effectifs par identifiant et % de BEN_NIR_ANO renseignés (par PSA)
psa_seul=nb de PSA non associés à BEN NIR ANO";


	proc sql;
    select 
        count(distinct BEN_NIR_ANO) as nb_ano, 
        count(distinct NIR_ANO_17) as nb_psa,
        count(distinct case when BEN_NIR_ANO is null then NIR_ANO_17 end) as psa_seul,
        count(distinct BEN_NIR_ANO) / count(distinct NIR_ANO_17) * 100 as perc_ano
    from WORK.chainage_sejG_PMSI;
quit;


/*% Nb de PSA par BNA*/
	proc sql;
		create table double_psa as select distinct BEN_NIR_ANO,
count(distinct NIR_ANO_17) as nb_psa 
from WORK.chainage_sejG_PMSI where BEN_NIR_ANO is not null group by BEN_NIR_ANO;
	quit;

	title "Nombre d'ouvreurs de droit par individu (nb de BEN_NIR_PSA par BEN_NIR_ANO) pour les BEN_NIR_ANO connus";

	proc freq data=double_psa;
		tables nb_psa;
	run;




/*Merge avec table SejG pour obtenir la table finale des ID:
	PSA présent dans IR BEN R
	Retrait des jumelles 
	Retrait des PSA non associés à BEN NIR ANO (environ 4%)*/

PROC SQL;
   CREATE TABLE WORK.sej_G_filtre AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, t1.BEN_SEX_COD, t1.BEN_NAI_MOI,  t1.BEN_NAI_ANN,  t1.BEN_DCD_DTE,
t2.SOR_MOD,
			t2.AGE_ANN, t2.AGE_GES, t2.dat_evt, t2.DEL_REG_ENT, 
          t2.dureeG, t2.ENT_DAT_DEL,  t2.ETA_NUM,  t2.EXE_SOI_DTD,  t2.EXE_SOI_DTF, 
          t2.GRG_GHM,  t2.ID_MAM_ENF, t2.indsej,  t2.issue,  t2.lmp_calc,  t2.NIR_ANO_17, 
          t2.RSA_NUM
      FROM rep.base1 t2
           INNER JOIN work.sej_g_ben2 t1 ON (t2.NIR_ANO_17 = t1.BEN_NIR_PSA);
QUIT;









title "Variable AGE_ANN";
/*Variable AGE_ANN (pas de filtre car sera effectué à partir de l'age calculé avec IRBENR)*/
proc means data=work.sej_G_filtre mean median min max q1 q3;
var age_ann ;  run;

proc freq data=work.sej_G_filtre;
tables age_ann ;  run;


/*Calcul age a partir de BEN NAI ANN ET BEN NAI MOI de IRBEN R
(code d'apres HAS "snds-indicateurs-parcours-bpco")*/

data work.sej_G_filtreage;
	set work.sej_G_filtre;
	length age 3.;
	* Âge;
	if 1900 <= input(BEN_NAI_ANN, 4.) <= year(today()) then
		do;
			if 1 <= input(BEN_NAI_MOI, 2.) <= 12 then
				Age = int(yrdif(mdy(input(BEN_NAI_MOI, 2.), 15, input(BEN_NAI_ANN, 4.)), DAT_EVT, 'ACTUAL'));
			else
				Age = int(yrdif(mdy(06, 15, input(BEN_NAI_ANN, 4.)), DAT_EVT, 'ACTUAL'));
		end;
	else Age = .;
	age_diff= Age_ANN-Age;
run;

title "Différence entre AGE_ANN(PMSI) et âge calculé à partir de IRBENR";
proc freq data=work.sej_G_filtreage;
tables age_diff; run;

/*(99.99% max 1 an de différence = on garde AGE_ANN (PMSI)*/

/*On retire BEN NAI ANN et BEN NAI MOI+ ON GARDE LE MAX DE BEN DCD DATE=DCD_DTE
(ie date la plus recente en cas de doublon)*/

PROC SQL;
   CREATE TABLE WORK.sej_g_filtre2 AS 
   SELECT t1.AGE_ANN, 
          t1.AGE_GES, 
          /* DCD_DTE */
            datepart(MAX(t1.BEN_DCD_DTE)) AS BEN_DCD_DTE format=ddmmyy10. , 
          t1.BEN_NIR_ANO,
          t1.DAT_EVT, 
          t1.DEL_REG_ENT, 
          t1.DUREEG, 
          t1.ENT_DAT_DEL, 
          t1.ETA_NUM, 
          t1.EXE_SOI_DTD, 
          t1.EXE_SOI_DTF, 
		  t1.SOR_MOD,
          t1.GRG_GHM, 
          t1.ID_MAM_ENF, 
          t1.INDSEJ, 
          t1.ISSUE, 
t1.LMP_CALC, 
          t1.NIR_ANO_17, 
          t1.RSA_NUM
      FROM WORK.SEJ_G_FILTRE t1
      GROUP BY t1.AGE_ANN,  t1.AGE_GES,t1.BEN_NIR_ANO,
               t1.BEN_SEX_COD,
               t1.DAT_EVT,
               t1.DEL_REG_ENT,
               t1.DUREEG,
               t1.ENT_DAT_DEL,
               t1.ETA_NUM,
               t1.EXE_SOI_DTD,
               t1.EXE_SOI_DTF,
			   t1.SOR_MOD,
               t1.GRG_GHM,
               t1.ID_MAM_ENF,
               t1.INDSEJ,
               t1.ISSUE,
               t1.LMP_CALC,
               t1.NIR_ANO_17,
               t1.RSA_NUM;
QUIT;



PROC SQL;
 CREATE TABLE WORK.count_sej_g_filtre2 AS 
   SELECT t1.issue, 
      
            (COUNT(t1.issue)) AS COUNT_of_issue
      FROM WORK.sej_g_filtre2 t1
      GROUP BY t1.issue
	  order by
case when issue = 'ENV' then 1
when issue = 'MN' then 2
when issue = 'IMGAPR22' then 3 
when issue = 'IMGAVT22' then 4
when issue = 'IVG' then 5 
when issue = 'FCS' then 6
when issue = 'GEU' then 7 
when issue = 'AUTRES' then 8 END;
QUIT;




/*BASE 2 : Apres traitement des NIR ET FILTRE SUR L'AGE :12-59*/

data REP.BASE2 (where=(12<=age_ann<60)); set WORK.sej_g_filtre2 ;run; 



PROC SQL;
 CREATE TABLE WORK.COUNT_BASE2 AS 
   SELECT t1.issue, 
      
            (COUNT(t1.issue)) AS COUNT_of_issue
      FROM REP.BASE2 t1
      GROUP BY t1.issue
	  order by
case when issue = 'ENV' then 1
when issue = 'MN' then 2
when issue = 'IMGAPR22' then 3 
when issue = 'IMGAVT22' then 4
when issue = 'IVG' then 5 
when issue = 'FCS' then 6
when issue = 'GEU' then 7 
when issue = 'AUTRES' then 8 END;
QUIT;
