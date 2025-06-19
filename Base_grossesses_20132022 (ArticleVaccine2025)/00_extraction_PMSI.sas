/*OBJECTIF: extraction de tous les sejours d'issues de grossesses
(EOPE: END of Pregnancy Event)
à partir des tables du PMSI MCO
Creation base 1 : issues après filtrage , dates entre '1Jan2013'd AND '31Dec2022'*/




/*Choix du répertoire pour enregistrer les tables finales*/
libname rep'/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base';




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





/* choix des codes actes CCAM (table C) */

/* Liste des actes CCAM d'interruption de grossesse */
%let liste_ccamIG ='JNJD001' 'JNJD002' 'JNJP001' ;

/* Liste des actes CCAM d'accouchement*/

%let liste_ccamACC ='JQGA002' 'JQGA003' 'JQGA004' 'JQGA005' 
'JQGD001' 'JQGD002' 'JQGD003' 'JQGD004' 'JQGD005' 'JQGD006' 
'JQGD007' 'JQGD008' 'JQGD009' 'JQGD010' 'JQGD012' 'JQGD013';

/* Liste des actes CCAM pour GEU(table C) */

%let liste_ccamGEU ='JJFA001' 'JJFC001' 'JJJA002' 'JJJC002'
'JJLJ001' 'JJPA001' 'JJPC001' 'JQGA001' ;




/* choix des codes de diagnostic à rechercher en DP (table B et UM) */

/* Avortement médical */
%macro O04_cod_DP (dgn=DGN_PAL);
    &dgn. like 'O04%' 
%mend;


/*(Apres 2019, distinction entre IMG<22SA et IVG)*/





/* Grossesse extra utérine */
%macro GEU_cod_DP(dgn=DGN_PAL);
    &dgn. like 'O00%'
%mend;

/* Fausse couche spontanée */
%macro FCS_cod_DP(dgn=DGN_PAL);
    &dgn. like 'O03%'
%mend;

/*AUTRES (Mole et autres produits anormaux)*/

%macro AUT_cod_DP(dgn=DGN_PAL);
    &dgn. like 'O01%' or &dgn. like 'O02%'
%mend;




/* choix des codes de diagnostic à rechercher en DAS (table D) */

/* Interruption médicale de grossesse */
%macro IMG_cod_DAS (dgn=);
	&dgn. like 'Z3711' or &dgn. like 'Z3731' or &dgn. like 'Z3741'
or &dgn. like 'Z3761' or &dgn. like 'Z3771'
%mend;

/* Mort-né */
%macro MN_cod_DAS (dgn=);
	&dgn. like 'Z3710' or &dgn. like 'Z3730' or &dgn. like 'Z3740'
or &dgn. like 'Z3760' or &dgn. like 'Z3770'
%mend;

/* Accouchement */
%macro ACC_cod_DAS (dgn=);
	&dgn. like 'Z37%' or &dgn. like 'Z3900%'
%mend;

/* Interruption volontaire de grossesse */
%macro IVG_cod_DAS (dgn=);
	&dgn. like 'Z640'
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
dans table B (recommandations de l'ATIH= RSA)*/

%macro sejours_DP(nom=, macro=);

PROC SQL;
   CREATE TABLE WORK.DP_&nom AS 
      SELECT DISTINCT eta_num, rsa_num
      FROM oravue.t_mco&aa.b
      WHERE &macro
      /*Si on souhaite les DP de table UM également */ 
/*UNION
      	SELECT DISTINCT eta_num, rsa_num
      	FROM oravue.t_mco&aa.um
     	 WHERE &macro*/
;
QUIT;

%mend sejours_DP;


/*Creation d'une macro pour identifier les séjours uniques à partir d'une macro définissant les DAS
dans table D (+ DP des tables UM si besoin)*/

%macro sejours_DAS(nom=, macro=);


PROC SQL;
   CREATE TABLE WORK.DAS_&nom AS 
   SELECT DISTINCT t1.ETA_NUM, t1.RSA_NUM
      FROM ORAVUE.T_MCO&aa.D t1
      WHERE %&macro.(dgn=ass_dgn)
      /*Si on souhaite les DP de table UM également */ 
/*UNION
      	SELECT DISTINCT eta_num, rsa_num
      	FROM oravue.t_mco&aa.um
     	 WHERE %&macro.(dgn=dgn_pal)*/
;
QUIT;
%mend sejours_DAS;



/*Creation d'une macro pour effectuer la jointure avec tables BC à partir d'une table WORK.table*/

%macro jointure_BC(nom=, table=);

/* Passage ORAUSER*/
data orauser.&table;set work.&table;run; 

/* Jointure avec table B et C*/
proc sql; create table work.BC_&nom as
select 

/*Variables apparaissant en 2012 et 2014)*/
%if &aaaa.>2012 %then %do; 
t2.COH_NAI_RET, t2.COH_SEX_RET, 
%end;

%if &aaaa.>2014 %then %do; t1.typ_gen_rsa, %end;

/*Variable de chainage mere enfant a partir de MCO 2013 (NIR ANO MAM jusqu'en 2019 puis ID MAM ENF)*/
%if &aaaa.>2012 and &aaaa.<2019 %then %do;
t2.NIR_ANO_MAM as ID_MAM_ENF, %end;

%if &aaaa.>2018 %then %do; t2.ID_MAM_ENF, %end;

t1.eta_num, t1.rsa_num,t1.sej_typ, t1.grg_ghm, t1.del_reg_ent, t1.age_ges, 
t1.cod_sex, t1.age_ann, t1.sor_mod,
t2.nir_ano_17, t2.exe_soi_dtd, t2.exe_soi_dtf, t2.FHO_RET, t2.NIR_RET, t2.NAI_RET, 
t2.SEX_RET, t2.DAT_RET, t2.SEJ_RET, t2.PMS_RET,
t4.ENT_DAT_DEL

from oravue.t_mco&aa.b as t1, oravue.t_mco&aa.c as t2,ORAUSER.&table as t4
    where (t1.eta_num = t4.eta_num and t1.rsa_num = t4.rsa_num)
        and (t2.eta_num = t4.eta_num and t2.rsa_num = t4.rsa_num);
quit;
%mend jointure_BC;



/**MACRO SEJOURS DE GROSSESSE A PARTIR DU PMSI**/
/*ATTENTION PAS DE FILTRAGE SUR AGE ET SEXE*/



%macro GROSSESSES_PMSI (aaaa=);


/* Suppression fichiers ORAUSER et WORK */
proc datasets lib=orauser kill;
run;quit;

proc datasets lib=work kill;
run;quit;

title ;

/*definition des macros pour années aaaa ou sous tables aa_01*/

%if &aaaa.=2023 %then %let aa=23_09;
%else %let aa = %substr(&aaaa.,3,2); 



/*IDENTIFICATION DES IMG APRES 22SA
(Acte d’accouchement
ET un code de la liste (Z37.11, Z37.31, Z37.41, Z37.61, Z37.71) en DA)*/


%sejours_CCAM(nom=ACC,liste=&liste_ccamACC);
%sejours_DAS(nom=IMGAPR22,macro=IMG_cod_DAS);


proc sort data=work.CCAM_ACC; by eta_num rsa_num; run;
proc sort data=work.DAS_IMGAPR22; by eta_num rsa_num; run;


data work.mergeIMGAPR22; merge work.CCAM_ACC (in=a) work.DAS_IMGAPR22(in=b);
by eta_num rsa_num;
if a and b;
run;




/*Jointure avec tables B et C puis filtre sur l'AG ou DDR (si AG manquant)*/

%jointure_BC(nom=IMGAPR22,table=mergeIMGAPR22);

data BC_IMGapr22_filtreAG;
set BC_IMGAPR22;

if not missing (age_ges) then do;
	if (age_ges <22 or age_ges>43)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <154 or del_reg_ent>301 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='IMGAPR22';
format issue $20.;
run;




/******************************************************************/

/** IDENTIFICATION DES MORTS NES HORS IMG  
(Acte d’accouchement ET DAS Z37.10, Z37.30, Z37.40, Z37.60, Z37.70)*/

%sejours_DAS(nom=MN,macro=MN_cod_DAS);

proc sort data=work.DAS_MN; by eta_num rsa_num; run;


data work.mergeMN; merge work.CCAM_ACC (in=a) work.DAS_MN(in=b);
by eta_num rsa_num;
if a and b;
run;



/*Jointure avec tables B et C puis filtre sur l'AG ou DDR (si AG manquant)*/

%jointure_BC(nom=MN,table=mergeMN);

data BC_MN_filtreAG;
set BC_MN;

if not missing (age_ges) then do;
	if (age_ges <22 or age_ges>43)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <154 or del_reg_ent>301 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='MN';
format issue $20.;
run;



/********************************************/

/** IDENTIFICATION DES NAISSANCES VIVANTES **/


/*Identification des sejours d'accouchement
(Acte d’accouchement
ET DAS Z37, Z3900 
WITHOUT diagnoses indicative of stillbirth or therapeutic abortions >22 weeks*/

%sejours_DAS(nom=ACC,macro=ACC_cod_DAS);

proc sort data=work.DAS_ACC; by eta_num rsa_num; run;



data work.mergeACC; merge work.CCAM_ACC (in=a) work.DAS_ACC(in=b)
work.DAS_IMGAPR22(in=c) work.DAS_MN(in=d);
by eta_num rsa_num;
if a and b and not c and not d;
run;



/*Jointure avec tables B et C puis filtre sur l'AG ou DDR (si AG manquant)*/

%jointure_BC(nom=ACC,table=mergeACC);

data BC_ACC_filtreAG;
set BC_ACC;

if not missing (age_ges) then do;
	if (age_ges <22 or age_ges>43)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <154 or del_reg_ent>301 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='ENV';
format issue $20.;

run;












/** IDENTIFICATION DES IVG 
Avant 2019 : DPO04 ET ACTE IG ET DAS Z640**/


%if &aaaa.<=2019 %then %do;

%sejours_DP(nom=IG,macro=%O04_cod_DP);
%sejours_CCAM(nom=IG,liste=&liste_ccamIG);
%sejours_DAS(nom=IVG,macro=IVG_cod_DAS);


/* Jointure des 3 tables DP CCAM DAS (a AND b AND c) */


proc sort data=work.DP_IG; by eta_num rsa_num; run;
proc sort data=work.CCAM_IG; by eta_num rsa_num; run;
proc sort data=work.DAS_IVG; by eta_num rsa_num; run;



data work.mergeIVG; merge work.DP_IG (in=a) work.CCAM_IG (in=b)work.DAS_IVG(in=c);
by eta_num rsa_num;
if a and b and c;
run;

%end;



/*Après 2019*/

%if &aaaa.>2019 %then %do;

%sejours_CCAM(nom=IG,liste=&liste_ccamIG);
proc sort data=work.CCAM_IG; by eta_num rsa_num; run;

/*DP en 'O04-0'*/
PROC SQL;
   CREATE TABLE WORK.DP_O04 AS 
      SELECT DISTINCT eta_num, rsa_num,dgn_pal
      FROM oravue.t_mco&aa.b
      WHERE dgn_pal like 'O04%';
QUIT;

PROC SQL;
   CREATE TABLE WORK.DP_IVGAPR2019 AS 
   SELECT t1.ETA_NUM, 
          t1.RSA_NUM
      FROM WORK.DP_O04 t1
      WHERE t1.DGN_PAL LIKE "O04_0";
QUIT;


/* Jointure des 2 tables DP CCAM  (a AND b) */
proc sort data=work.DP_IVGAPR2019; by eta_num rsa_num; run;


data work.mergeIVG; merge work.DP_IVGAPR2019 (in=a) work.CCAM_IG (in=b);
by eta_num rsa_num;
if a and b;
run;

%end;




/*Jointure avec tables B et C puis filtre sur l'AG (jusqu'a 16SA)
ou DDR (si AG manquant)*/
%jointure_BC(nom=IVG,table=mergeIVG);

data BC_IVG_filtreAG;
set BC_IVG;

if not missing (age_ges) then do;
	if (age_ges <3 or age_ges>16)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <21 or del_reg_ent>111 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='IVG';
format issue $20.;
run;




/** IDENTIFICATION DES IMG AVANT 22 SA
Avant 2019 : DPO04 ET ACTE IG SANS DAS Z640 **/


%if &aaaa.<=2019 %then %do;
/* Jointure des 3 tables DP CCAM DAS (a AND b AND NOT c) */


data work.mergeIMGavt22; merge work.DP_IG (in=a) work.CCAM_IG (in=b)work.DAS_IVG(in=c);
by eta_num rsa_num;
if a and b and not c;
run;

%end;

%if &aaaa.>2019 %then %do;

PROC SQL;
   CREATE TABLE WORK.DP_IMGAVT22APR2019 AS 
   SELECT t1.ETA_NUM, 
          t1.RSA_NUM
      FROM WORK.DP_O04 t1
      WHERE t1.DGN_PAL LIKE "O04_1"
OR t1.DGN_PAL LIKE "O04_2"
OR t1.DGN_PAL LIKE "O04_3";
QUIT;


/* Jointure des 3 tables DP CCAM DAS (a AND b AND NOT C) */

%sejours_DAS(nom=IVG,macro=IVG_cod_DAS);
proc sort data=work.DAS_IVG; by eta_num rsa_num; run;

proc sort data=work.DP_IMGAVT22APR2019; by eta_num rsa_num; run;

data work.mergeIMGavt22; merge work.DP_IMGAVT22APR2019 (in=a) 
work.CCAM_IG (in=b) work.DAS_IVG(in=c);

by eta_num rsa_num;
if a and b and not c;
run;


%end;



/*Jointure avec tables B et C puis filtre sur l'AG ou DDR (si AG manquant)*/
%jointure_BC(nom=IMGavt22,table=mergeIMGavt22);

data BC_IMGavt22_filtreAG;
set BC_IMGavt22;

if not missing (age_ges) then do;
	if (age_ges <3 or age_ges>21)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <21 or del_reg_ent>153 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='IMGAVT22';
format issue $20.;
run;




/** IDENTIFICATION DES GEU
(DP en O00)**/

%sejours_DP(nom=GEU,macro=%GEU_cod_DP);
%sejours_CCAM(nom=GEU,liste=&liste_ccamGEU);


/* Jointure des 2 tables DP CCAM (a and b )
On ajoute la table des acte GEU pour obtenir la date de l'acte si existant*/

proc sort data=work.DP_GEU; by eta_num rsa_num; run;
proc sort data=work.CCAM_GEU; by eta_num rsa_num; run;


data work.mergeGEU; merge work.DP_GEU (in=a) work.CCAM_GEU (in=b);
by eta_num rsa_num;
if a ;
run;

/*Jointure avec tables B et C puis filtre sur l'AG ou DDR (si AG manquant)*/
%jointure_BC(nom=GEU,table=mergeGEU);

data BC_GEU_filtreAG;
set BC_GEU;

if not missing (age_ges) then do;
	if (age_ges <3 or age_ges>21)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <21 or del_reg_ent>153 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='GEU';
format issue $20.;
run;





/** IDENTIFICATION DES FCS **/

%sejours_DP(nom=FCS,macro=%FCS_cod_DP);

/*rajouter ENT_DAT_DEL*/
data DP_FCS;set DP_FCS;ENT_DAT_DEL=.;format ENT_DAT_DEL 3.;run;

/*Jointure avec tables B et C puis filtre sur l'AG ou DDR (si AG manquant)*/
%jointure_BC(nom=FCS,table=DP_FCS);

data BC_FCS_filtreAG;
set BC_FCS;

if not missing (age_ges) then do;
	if (age_ges <3 or age_ges>21)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <21 or del_reg_ent>153 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='FCS';
format issue $20.;
run;


/** IDENTIFICATION DES AUTRES ISSUES**/

%sejours_DP(nom=AUT,macro=%AUT_cod_DP(dgn=dgn_pal));
/*rajouter ENT_DAT_DEL*/
data DP_AUT;set DP_AUT;ENT_DAT_DEL=.;format ENT_DAT_DEL 3.;run;

/*Jointure avec tables B et C puis filtre sur l'AG ou DDR (si AG manquant)*/
%jointure_BC(nom=AUT,table=DP_AUT);

data BC_AUT_filtreAG;
set BC_AUT;

if not missing (age_ges) then do;
	if (age_ges <3 or age_ges>21)then do;DELETE;end;
	else dureeG=(age_ges*7)+3;
	end;

else do; if (del_reg_ent <21 or del_reg_ent>153 or del_reg_ent=.) then do;DELETE;end;
		else dureeG=del_reg_ent;end;

issue='AUTRES';
format issue $20.;
run;






/**TABLE REUNISSANT TOUS LES SEJOURS **/
/* +ajout date evenement de fin de grossesse calculée + LMP calculée + index (indsej car 'INDEX' reserve pour ORACLE */

data sej_G;
set BC_IMGAPR22_filtreAG BC_MN_filtreAG BC_ACC_filtreAG
BC_IMGavt22_filtreAG BC_IVG_filtreAG BC_GEU_filtreAG BC_FCS_filtreAG BC_AUT_filtreAG;

dat_evt=sum(datepart(exe_soi_dtd),ent_dat_del);
lmp_calc=dat_evt-dureeG;
format dat_evt ddmmyy8. lmp_calc ddmmyy8. issue $20.;
if cmiss(eta_num,rsa_num,exe_soi_dtd) = 0 then indsej=eta_num||rsa_num||exe_soi_dtd; else delete;
run;

/*Filtrages*/

/* Filtrage sur GHM en erreurs, FINESS à retirer, codes retours*/

data sej_G_filtre1 (keep= indsej eta_num rsa_num grg_ghm exe_soi_dtd exe_soi_dtf sor_mod ent_dat_del 
dat_evt nir_ano_17 cod_sex id_mam_enf del_reg_ent age_ges age_ann cod_sex lmp_calc issue dureeG);
set sej_G (where=(

%if &aaaa.>2012 %then %do; 
COH_NAI_RET = '0' AND COH_SEX_RET = '0' AND
%end;

FHO_RET = '0' AND NIR_RET = '0' AND NAI_RET = '0' AND SEX_RET = '0' AND DAT_RET = '0' AND SEJ_RET = '0' AND PMS_RET = '0'
AND (NIR_ANO_17 not in ('xxxxxxxxxxxxxxxxx' 'XXXXXXXXXXXXXXXXD'))
AND (SEJ_TYP <> 'B' or SEJ_TYP is null)

%if &aaaa.>2014 %then %do; 
AND (TYP_GEN_RSA = '0') 
%end;
));

if eta_num in (&finess_out.) then delete;
if grg_ghm =: "90" then delete;
run;





/*repérage des séjours avec plus d'une issue*/
PROC SQL;
   CREATE TABLE WORK.index_doublons AS 
   SELECT t1.indsej, 
          /* COUNT_of_issue */
            (COUNT(t1.issue)) AS COUNT_of_issue
      FROM WORK.sej_G_filtre1 t1
      GROUP BY t1.indsej
      HAVING (CALCULATED COUNT_of_issue) NOT = 1;
QUIT;

/*Pour visualiser les doublons*/
PROC SQL;
   CREATE TABLE WORK.view_DOUBLONS AS 
   SELECT t2.issue, 
          t2.indsej
      FROM WORK.INDEX_DOUBLONS t1
           INNER JOIN WORK.SEJ_G_FILTRE1 t2 ON (t1.indsej = t2.indsej)
      ORDER BY t2.indsej;
QUIT;

/*Table finale après suppression des doublons*/
proc sort data=work.sej_G_filtre1; by indsej; run;
proc sort data=work.index_doublons; by indsej; run;

data work.sej_G_&aaaa.(keep= indsej eta_num rsa_num grg_ghm exe_soi_dtd exe_soi_dtf sor_mod ent_dat_del 
dat_evt nir_ano_17 cod_sex id_mam_enf del_reg_ent age_ges age_ann lmp_calc issue dureeG); merge work.sej_G_filtre1 (in=a) work.index_doublons (in=b);
by indsej;
if a and not b;
run;




/*Enregistrement de la table finale dans répertoire projet*/

data rep.sejG_&aaaa.;
set work.sej_G_&aaaa.;
run;




%mend;



/*MACRO pour chaque table désirée et union des 3 tables*/


%macro loop;
    %do year = 2012 %to 2023;
        %GROSSESSES_PMSI (aaaa=&year.);
    %end;
%mend loop;

%loop;






%macro loop_count;
%do year = 2012 %to 2023;
PROC SQL;
 CREATE TABLE WORK.COUNT_SEJG_&year. AS 
   SELECT t1.issue, 
      
            (COUNT(t1.issue)) AS COUNT_of_issue
      FROM rep.SEJG_&year. t1
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
%end;
%mend loop_count;
%loop_count;





/*Supprimer table 2012 (pas d'ID MAM ENF)*/

proc datasets lib=rep ;
    	delete sejg_2012  ;


/*agreger toutes les tables 2013-2023*/
data work.PMSI_2013_2023;
	set REP.sejg_:;
run;

/*supprimer tables partielles*/


proc datasets lib=rep ;
    	delete sejg_:  ;



/****************************************************/
/*CREATION TABLE 2013-2022*/



/*Filtre sur les dates d'evt*/
		
PROC SQL;
   CREATE TABLE WORK.PMSI_2013_2022 AS 
   SELECT t1.*,
          /* annee */
            (YEAR(t1.dat_evt)) AS annee
      FROM WORK.PMSI_2013_2023 t1
      WHERE t1.dat_evt BETWEEN '1Jan2013'd AND '31Dec2022'd
      ORDER BY t1.dat_evt;
QUIT;


/*Recherche de doublons indsej*/
PROC SQL;
   CREATE TABLE WORK.doublons_indsej AS 
   SELECT t1.indsej, 
          /* COUNT_of_indsej */
            (COUNT(t1.indsej)) AS COUNT_of_indsej
      FROM WORK.PMSI_2013_2022 t1
      GROUP BY t1.indsej
      HAVING (CALCULATED COUNT_of_indsej) > 1;
QUIT;





/*retrait doublons ET ENREGISTREMENT DANS REP*/
PROC SQL;
   CREATE TABLE REP.BASE1 AS 
   SELECT t1.*
      FROM WORK.PMSI_2013_2022 t1
	  LEFT JOIN WORK.doublons_indsej t2
      ON t1.indsej = t2.indsej
	  WHERE t2.indsej IS NULL
      ;
QUIT;







/*COMPTAGE DES ISSUES*/




PROC SQL;
 CREATE TABLE WORK.COUNT_SEJG_2013_2022 AS 
   SELECT t1.issue, 
      
            (COUNT(t1.issue)) AS COUNT_of_issue
      FROM REP.BASE1 t1
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



/*VERIFS*/


/*Variable COD_SEX*/
proc freq data=REP.BASE1;
tables cod_sex ; run;

/*Variable AGE_ANN
(pas de filtre car sera effectué à partir de l'age calculé avec IRBENR)*/
proc means data=REP.BASE1 mean median min max q1 q3;
var age_ann ;  run;



/*Verification GHM*/
PROC SQL;
   CREATE TABLE WORK.count_GHM AS 
   SELECT t1.issue, 
          t1.GRG_GHM, 
            (COUNT(t1.GRG_GHM)) AS COUNT_of_GRG_GHM
      FROM REP.BASE1 t1
      GROUP BY t1.issue,
               t1.GRG_GHM
      ORDER BY t1.issue,
               COUNT_of_GRG_GHM DESC;
QUIT;


/*Ajouter libellés GHM*/
PROC SQL;
   CREATE TABLE WORK.COUNT_GHMLIB AS 
   SELECT DISTINCT t2.COUNT_of_GRG_GHM, 
          t2.GRG_GHM, 
          t2.issue, 
          /* MAX_of_lib_ghm */
            (MAX(t1.lib_ghm)) AS MAX_of_lib_ghm
      FROM RFCOMMUN.GHM_V9A2022 t1
           RIGHT JOIN WORK.COUNT_GHM t2 ON (t1.code_ghm = t2.GRG_GHM)
      GROUP BY t2.COUNT_of_GRG_GHM,
               t2.GRG_GHM,
               t2.issue
      ORDER BY t2.issue,
               t2.COUNT_of_GRG_GHM DESC;
QUIT;











