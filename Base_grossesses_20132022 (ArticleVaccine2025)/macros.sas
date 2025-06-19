/*Macro de Santé Publique France pour le comptage des doublons à partir de IRBENR*/
%macro doublons (table);
	/*% nir_ano renseigné*/
	title "Effectifs par identifiant et % de BEN_NIR_ANO renseignés";

	proc sql;
		select count(distinct BEN_NIR_ANO) as nb_ano, count(distinct BEN_NIR_PSA) as nb_psa, count(distinct BEN_NIR_PSA||put(BEN_RNG_GEM,1.)) as nb_gem, count(distinct BEN_NIR_ANO)/count(distinct BEN_NIR_PSA||put(BEN_RNG_GEM,1.))*100 as perc_ano from &table;
	quit; 

	/*doublons de BEN_NIR_PSA*/

	proc sql;
		create table double_psa as select distinct BEN_NIR_ANO,count(distinct BEN_NIR_PSA) as nb_psa from &table where BEN_NIR_ANO is not null group by BEN_NIR_ANO;
	quit;

	title "Nombre d'ouvreurs de droit par individu (nb de BEN_NIR_PSA par BEN_NIR_ANO) pour les BEN_NIR_ANO connus";

	proc freq data=double_psa;
		tables nb_psa;
	run; 

	/*doublons de rangs gémellaires*/

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

/* Creation d'une macro pour identifier les séjours uniques dans table A pour liste CCAM définie
+ Garder la variable "ENT_DAT_DEL" avec la valeur minimale (1er acte du séjour)*/

%macro sejours_CCAM(nom=, liste=);
PROC SQL;
   CREATE TABLE WORK.CCAM_&nom AS 
   SELECT DISTINCT t1.ETA_NUM, 
          t1.RSA_NUM, 
          /* MIN_of_ENT_DAT_DEL */
            (MIN(t1.ENT_DAT_DEL)) FORMAT=4. AS ENT_DAT_DEL
      FROM ORAVUE.T_MCO&aa.A t1
      WHERE t1.CDC_ACT in (&liste)
      GROUP BY t1.ETA_NUM,
               t1.RSA_NUM;
QUIT;
%mend sejours_CCAM;


/*Creation d'une macro pour identifier les séjours uniques à partir d'une macro définissant les DP 
dans tables B et UM*/

%macro sejours_DP(nom=, macro=);

PROC SQL;
   CREATE TABLE WORK.DP_&nom AS 
      SELECT DISTINCT eta_num, rsa_num
      FROM oravue.t_mco&aa.b
      WHERE &macro
      	UNION
      	SELECT DISTINCT eta_num, rsa_num
      	FROM oravue.t_mco&aa.um
     	 WHERE &macro;
QUIT;

%mend sejours_DP;


/*Creation d'une macro pour identifier les séjours uniques à partir d'une macro définissant les DAS
dans table D*/

%macro sejours_DAS(nom=, macro=);


PROC SQL;
   CREATE TABLE WORK.DAS_&nom AS 
   SELECT DISTINCT t1.ETA_NUM, t1.RSA_NUM
      FROM ORAVUE.T_MCO&aa.D t1
      WHERE &macro;
QUIT;
%mend sejours_DAS;



/* Macro pour recuperer et filtrer sejour naissance dans le PMSI avec Choix de l'année
(Premiere ligne a modifier si utilisation de table partielle)*/

%macro sejN(aaaa);

%if &aaaa.=012023 %then %let aa=23_01;
%else %let aa = %substr(&aaaa.,3,2); 

proc sql; create table work.sejN_v1_&aaaa. as
select t1.eta_num, t1.rsa_num,t1.sej_typ,t1.typ_gen_rsa, t1.grg_ghm, t1.del_reg_ent, t1.age_ges, 
t1.cod_sex, t1.poi_nai, t2.rng_nai,
t2.nir_ano_17, t2.exe_soi_dtd, t2.exe_soi_dtf, t2.ID_MAM_ENF, t2.COH_NAI_RET, t2.COH_SEX_RET,
t2.FHO_RET, t2.NIR_RET, t2.NAI_RET, t2.SEX_RET, t2.DAT_RET, t2.SEJ_RET, t2.PMS_RET
from oravue.t_mco&aa.b as t1, oravue.t_mco&aa.c as t2

  WHERE (t1.ETA_NUM = t2.ETA_NUM AND t1.RSA_NUM = t2.RSA_NUM) 
AND (t1.GRG_GHM LIKE '15%' AND t1.AGE_JOU = 0 
AND (t1.ENT_MOD = '8' OR t1.ENT_MOD = 'N' ) 
AND ( t1.POI_NAI >= 500 OR t1.AGE_GES >= 22 ));
quit;



/* Filtrage sur GHM en erreurs, FINESS à retirer, codes retours*/

data work.sejN_&aaaa. (keep= eta_num rsa_num grg_ghm exe_soi_dtd exe_soi_dtf 
 nir_ano_17 id_mam_enf del_reg_ent age_ges poi_nai cod_sex rng_nai);

set work.sejN_v1_&aaaa. (where=(COH_NAI_RET = '0' AND COH_SEX_RET = '0' AND FHO_RET = '0' AND NIR_RET = '0' AND
NAI_RET = '0' AND SEX_RET = '0' AND DAT_RET = '0' AND SEJ_RET = '0' AND PMS_RET = '0'
AND (NIR_ANO_17 not in ('xxxxxxxxxxxxxxxxx' 'XXXXXXXXXXXXXXXXD'))
AND (SEJ_TYP <> 'B' or SEJ_TYP is null)
AND (TYP_GEN_RSA = '0')));

if eta_num in (&finess_out.) then delete;
if grg_ghm =: "90" then delete;
run;

%mend;


/*MACRO POUR LA RECHERCHE DE CODES CIM DANS MCO POUR UNE POPULATION DONNEE (une ou PLUSIEURS ANNEES)
VARIABLES D'ENTREE
table_id= nom de la table comprenant la liste des ID de la population
nom_id= nom de la variable ID dans la table à chainer au NIR_ANO_17 du MCO
annee_list= liste des codes années à rechercher (par défaut: 21 22 23_01)

VARIABLES DE SORTIE

CIM_MCO= Table avec une ligne par ID, CIM, 
type de diag et date_diag (date d'entrée du séjour correspondant)*/

%macro recherche_cim(table_id= , nom_id= , annee_list= 21 22 23_01);

%local i next_name;
%let i=1;
%do %while (%scan(&annee_list, &i) ne );
   %let an = %scan(&annee_list, &i);

   /*Rechercher tous les CIM pour tous les types de diag pour tous les sej associés aux ID*/
PROC SQL;
   CREATE TABLE WORK.ENV_CIM&an. AS 
   SELECT DISTINCT t1.&nom_id., 
          t3.DGN_PAL, 
          t3.DGN_REL, 
          t4.DGN_PAL AS DGN_PAL_UM, 
          t4.DGN_REL AS DGN_REL_UM, 
          t5.ASS_DGN, 
          /* date_diag */
            (datepart(t2.exe_soi_dtd)) FORMAT=DDMMYY10. AS date_diag
      FROM &table_id. t1
           INNER JOIN ORAVUE.T_MCO&an.C t2 ON (t1.&nom_id. = t2.NIR_ANO_17)
           LEFT JOIN ORAVUE.T_MCO&an.B t3 ON (t2.ETA_NUM = t3.ETA_NUM) AND (t2.RSA_NUM = t3.RSA_NUM)
           LEFT JOIN ORAVUE.T_MCO&an.UM t4 ON (t2.ETA_NUM = t4.ETA_NUM) AND (t2.RSA_NUM = t4.RSA_NUM)
           LEFT JOIN ORAVUE.T_MCO&an.D t5 ON (t2.ETA_NUM = t5.ETA_NUM) AND (t2.RSA_NUM = t5.RSA_NUM)
     WHERE COH_NAI_RET = '0' AND COH_SEX_RET = '0' AND FHO_RET = '0' AND NIR_RET = '0' AND
           NAI_RET = '0' AND SEX_RET = '0' AND DAT_RET = '0' AND SEJ_RET = '0' AND PMS_RET = '0'
           AND (SEJ_TYP <> 'B' or SEJ_TYP is null)
           AND (TYP_GEN_RSA = '0')
           and t2.eta_num not in ('130780521' '130783236' '130783293' '130784234' '130804297'
                             '600100101' '750041543' '750100018' '750100042' '750100075'
                             '750100083' '750100091' '750100109' '750100125' '750100166'
                             '750100208' '750100216' '750100232' '750100273' '750100299'
                             '750801441' '750803447' '750803454' '910100015' '910100023'
                             '920100013' '920100021' '920100039' '920100047' '920100054'
                             '920100062' '930100011' '930100037' '930100045' '940100027'
                             '940100035' '940100043' '940100050' '940100068' '950100016'
                             '690783154' '690784137' '690784152' '690784178' '690787478'
                             '830100558')
           AND GRG_GHM NOT LIKE ('90%');
QUIT;


/*Passage WIDE TO LONG*/
PROC SQL;
	CREATE TABLE work.long&an. AS
		(SELECT &nom_id., date_diag, "DGN_PAL" as DIAG_TYPE,
			DGN_PAL as CIM FROM WORK.ENV_CIM&an.)
			UNION
		(SELECT &nom_id., date_diag, "DGN_PAL_UM" as DIAG_TYPE,
			DGN_PAL_UM as CIM FROM WORK.ENV_CIM&an.)

			UNION
		(SELECT &nom_id., date_diag, "ASS_DGN" as DIAG_TYPE,
			ASS_DGN as CIM FROM WORK.ENV_CIM&an.)
			UNION
		(SELECT &nom_id., date_diag, "DGN_REL" as DIAG_TYPE,
			DGN_REL as CIM FROM WORK.ENV_CIM&an.)

			UNION
		(SELECT &nom_id., date_diag, "DGN_REL_UM" as DIAG_TYPE,
			DGN_REL_UM as CIM FROM WORK.ENV_CIM&an.);
QUIT;

/*Retrait des lignes avec CIM manquants*/
PROC SQL;
   CREATE TABLE WORK.cim__&an. AS 
   SELECT DISTINCT t1.CIM, 
          t1.date_diag, 
          t1.DIAG_TYPE, 
          t1.&nom_id.
      FROM WORK.LONG&an. t1
      WHERE t1.CIM NOT IS MISSING;
QUIT;
 %let i = %eval(&i + 1);
%end;

	/* On concatne toutes les tables*/
	data CIM_MCO ;
		set WORK.CIM__:;
	run;
%mend recherche_cim;
