/*Chainage de BASEG avec la table vaccination COVID (IR VAC F)*/


proc datasets lib=orauser kill;
run;quit;

proc datasets lib=work kill;
run;quit;

libname rep '/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base';


/*Passage Orauser*/

PROC SQL;
   CREATE TABLE ORAUSER.baseg AS 
   SELECT * FROM rep.baseg;
QUIT;


/***CODE POUR EXTRAIRE VACCINATIONS ASSOCIEES AUX SEJOURS MAT***/
/*Extraire la table IRVACF sur les dates d'intérêt*/

/*Jointure avec table des SEJ MAT*/
/*PROC SQL;
   CREATE TABLE WORK.chainage_vac AS 
   SELECT t1.BEN_NIR_ANO, 
          t2.INJ_VAC_NOM, 
          t2.PHA_PRS_C13, 
          t2.EXE_SOI_DTD
	FROM ORAUSER.baseG t1
           LEFT JOIN CONSOPAT.IR_VAC_F t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO)
;
QUIT;

/*Enregistrer table dans répertoire REPMEDGR*/
/*PROC SQL;
   CREATE TABLE REP.CHAINAGE_VAC AS 
   SELECT DISTINCT t1.*
      FROM WORK.CHAINAGE_VAC t1;
QUIT;


title;

/*verifier qu'il n'y a pas de valeurs manquantes hors personnes non vaccinées*/
PROC MEANS DATA=rep.chainage_vac min max NMISS;
RUN;
/*verifier qu'il n'y a pas de valeurs manquantes hors personnes non vaccinées*/
PROC SQL;
   CREATE TABLE WORK.manquants AS 
   SELECT /* NMISS_of_EXE_SOI_DTD */
            (NMISS(t1.EXE_SOI_DTD)) AS NMISS_of_EXE_SOI_DTD, 
          /* NMISS_of_PHA_PRS_C13 */
            (NMISS(t1.PHA_PRS_C13)) AS NMISS_of_PHA_PRS_C13
      FROM rep.chainage_vac t1;
QUIT;
            
       



/*Compter le nombre de dates d'injection distinctes par BENNIRANO*/
PROC SQL;
   CREATE TABLE WORK.count_dtd_par_BNA AS 
   SELECT t1.BEN_NIR_ANO, 
          /* COUNT_DISTINCT_of_EXE_SOI_DTD */
            (COUNT(DISTINCT(t1.EXE_SOI_DTD))) AS COUNT
      FROM REP.chainage_vac t1
      GROUP BY t1.BEN_NIR_ANO
      ORDER BY COUNT DESC;
QUIT;


PROC SQL;
   CREATE TABLE WORK.COUNT2 AS 
   SELECT t1.COUNT, 
          /* NB_BNA */
            (COUNT(t1.COUNT)) AS NB_BNA
      FROM WORK.COUNT_DTD_PAR_BNA t1
      GROUP BY t1.COUNT;
QUIT;

/*type de vaccins*/
PROC SQL;
   CREATE TABLE WORK.type_vaccins AS 
   SELECT DISTINCT t1.PHA_PRS_C13, 
          t1.INJ_VAC_NOM
      FROM rep.chainage_vac t1;
QUIT;

/*Format nom des vaccins */
proc format;
value $nom_vac

'3400930219553'='MOD'
'3400930222256'='AZD'
'3400930222232'='JJJ'
'3400930243411'='BNT'
'3400930243404'='PBNT'
'3400930245774'='NVX'

'3400930259528', '3400930266472'='BIMOD'

'3400930262009'='BIBNT'

'3400930264157'='SAN'
'3400930277409'='NVXX'

'3400930279038'='BNTX'

'3400930279069' '3400930279076'='PBNTX'
;
run;

/*SI 2 vaccins differents meme jour = UNK*/



/*RNA=Nucleic acid-based
 NVV=Viral vector
 PRB= Protein-based*/

proc format;
value $type_vac
'3400930219553','3400930259528','3400930266472','3400930262009',
'3400930243404','3400930243411', '3400930279038', '3400930279069' '3400930279076' = 'RNA'
'3400930222256','3400930222232' = 'NVV'
'3400930245774','3400930264157', '3400930277409' = 'PRB'

;
run;



/*** On récupère les variables d'intéret  **/
proc sql;
create table work.vac as
select t2.ben_nir_ano, t2.pha_prs_c13, t2.exe_soi_dtd
from  rep.chainage_vac as t2 ;
;
quit;

/** ajout des variables nom et type selon le format établi**/
data work.vac1; set work.vac;
nom_vac=put(pha_prs_c13, nom_vac.);
type_vac=put(pha_prs_c13, type_vac.);
run;


/*** TRAITEMENT DES DOUBLONS (2 vaccins différents pour un couple BNA/date)***/

/*DOUBLONS: 2 noms de vaccin pour un BNA sur une journée?*/

/*Repérage des doublons*/
PROC SQL;
   CREATE TABLE WORK.doublons_vac AS 
   SELECT t1.BEN_NIR_ANO, 
          t1.EXE_SOI_DTD, 
          /* COUNT_DISTINCT_of_PHA_PRS_C13 */
            (COUNT(DISTINCT(t1.PHA_PRS_C13))) AS COUNT_DISTINCT_of_PHA_PRS_C13
      FROM WORK.VAC1 t1
      GROUP BY t1.BEN_NIR_ANO,
               t1.EXE_SOI_DTD
      ORDER BY COUNT_DISTINCT_of_PHA_PRS_C13 DESC;
QUIT;

/*Visualiser table avec doublons*/
PROC SQL;
   CREATE TABLE WORK.explo_DOUBLONS_VAC AS 
   SELECT t2.BEN_NIR_ANO, 
          t2.EXE_SOI_DTD, 
          t2.nom_vac, 
          t2.PHA_PRS_C13, 
          t2.type_vac
      FROM WORK.DOUBLONS_VAC t1
           INNER JOIN WORK.VAC1 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO) AND (T1.EXE_SOI_DTD=t2.EXE_SOI_DTD)
      WHERE t1.COUNT_DISTINCT_of_PHA_PRS_C13 > 1;
QUIT;


/*Remplacer nom_vac par "UNK"(UNKNOWN) pour les doublons*/

PROC SQL;
   CREATE TABLE WORK.DOUBLONS_VAC2 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.EXE_SOI_DTD, 
          t1.type_vac, 
          /* nom_vac */
            ('UNK') AS nom_vac LENGTH=8
      FROM WORK.EXPLO_DOUBLONS_VAC t1;
QUIT;


/*DOUBLONS: 2 TYPES de vaccin pour un BNA sur une journée?*/

PROC SQL;
   CREATE TABLE WORK.DOUBLONS_VAC3 AS 
   SELECT /* COUNT_DISTINCT_of_type_vac */
            (COUNT(DISTINCT(t1.type_vac))) AS COUNT_DISTINCT_of_type_vac, 
          t1.BEN_NIR_ANO, 
          t1.EXE_SOI_DTD, 
          t1.nom_vac, 
          t1.type_vac
      FROM WORK.DOUBLONS_VAC2 t1
      GROUP BY t1.BEN_NIR_ANO,
               t1.EXE_SOI_DTD;
QUIT;

/*Si 2 types différents, remplacer par "UNK"*/
data work.DOUBLONS_VAC4; set work.DOUBLONS_VAC3;
if COUNT_DISTINCT_of_type_vac=1 then do; 
type_vac=type_vac;
end;
if COUNT_DISTINCT_of_type_vac=2 then do; 
type_vac='UNK';
end;

RUN;





/*Garder une ligne par date d'injection*/
PROC SQL;
   CREATE TABLE WORK.doublons_filtre AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.EXE_SOI_DTD, 
          t1.nom_vac, 
          t1.type_vac
      FROM WORK.DOUBLONS_VAC4 t1;
QUIT;



/*table sans doublons*/
PROC SQL;
   CREATE TABLE WORK.sans_DOUBLONS_VAC AS 
   SELECT distinct t2.BEN_NIR_ANO, 
          t2.EXE_SOI_DTD, 
          t2.nom_vac, 
          t2.PHA_PRS_C13, 
          t2.type_vac
      FROM WORK.DOUBLONS_VAC t1
           INNER JOIN WORK.VAC1 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO) AND (T1.EXE_SOI_DTD=t2.EXE_SOI_DTD)
      WHERE t1.COUNT_DISTINCT_of_PHA_PRS_C13 = 1
OR t1.COUNT_DISTINCT_of_PHA_PRS_C13 = 0;
QUIT;




/*reunir les deux tables*/
data WORK.vac_filtre;
set WORK.doublons_filtre WORK.sans_DOUBLONS_VAC;
run;




/*Delai entre 2 injections*/

proc sort data=WORK.vac_filtre; by ben_nir_ano exe_soi_dtd; run;
data vac2;
    set WORK.vac_filtre;
    by ben_nir_ano exe_soi_dtd;
	 delai=exe_soi_dtd - lag(exe_soi_dtd);

if first.ben_nir_ano then do;
    delai=.;
    end;
run;


proc freq data=vac2;
tables delai;
run;


/*Retirer vacc si délai <=14j*/

PROC SQL;
   CREATE TABLE WORK.vac3 AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.delai, 
          t1.EXE_SOI_DTD, 
          t1.nom_vac, 
          t1.PHA_PRS_C13, 
          t1.type_vac
      FROM WORK.VAC2 t1
      WHERE t1.delai > 14 OR t1.delai IS MISSING;
QUIT;

/*numeroter les injections pour chaque BNA + retirer les injections >2022 */
proc sort data=WORK.vac3; by ben_nir_ano exe_soi_dtd; run;
data vac4;
    set WORK.vac3 (where=(exe_soi_dtd is missing or (exe_soi_dtd between '01jan2013'd and '31dec2022'd)));
    by ben_nir_ano exe_soi_dtd;
    num_vac +1;

if (first.ben_nir_ano and exe_soi_dtd ne .) then do;
    num_vac=1;
    end;

if (first.ben_nir_ano and exe_soi_dtd = .) then do;
    num_vac=.;
    end;
run;










/*Table des séjours mats*/
PROC SQL;
   CREATE TABLE WORK.sej_mat AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.indsej, 
          t1.lmp_calc, 
          t1.dat_evt, 
          t1.issue
      FROM rep.baseg t1
      ORDER BY t1.BEN_NIR_ANO,
               t1.dat_evt;
QUIT;

/*Jointure avec table des vaccins*/
PROC SQL;
   CREATE TABLE WORK.chainage_vac2 AS 
   SELECT distinct t1.BEN_NIR_ANO, 
          t1.dat_evt, 
          t1.indsej, 
          t1.issue, 
          t1.lmp_calc, 
          t2.EXE_SOI_DTD, 
          t2.nom_vac, 
          t2.PHA_PRS_C13, 
          t2.type_vac, 
		  t2.num_vac
      FROM WORK.SEJ_MAT t1
           left JOIN WORK.VAC4 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO) 
      ORDER BY t1.BEN_NIR_ANO,
               t2.num_vac;
QUIT;

data work.chainage_vac3; set work.chainage_vac2;
length timing $10.;
if exe_soi_dtd ^=. and exe_soi_dtd <lmp_calc-28 then do; 
timing='before';
end;
if exe_soi_dtd >=lmp_calc-28 and exe_soi_dtd<dat_evt then do; 
timing='during';
end;

if LMP_calc+14>exe_soi_dtd >=lmp_calc-28 and exe_soi_dtd<dat_evt then do; 
trimester='T0';
end;
if LMP_calc+98>exe_soi_dtd >=LMP_calc+14 and exe_soi_dtd<dat_evt then do; 
trimester='T1';
end;
if LMP_calc+196>exe_soi_dtd >=LMP_calc+98 and exe_soi_dtd<dat_evt then do; 
trimester='T2';
end;
if dat_evt>=exe_soi_dtd >=LMP_calc+196  then do; 
trimester='T3';
end;

if exe_soi_dtd >=dat_evt then do; 
timing='after';
end;
RUN;


/*enregistrement de la table chainée*/

data rep.vacCOVID; set work.chainage_vac3;run;





/*Compter le nombre de dates d'injection distinctes par BENNIRANO*/


PROC SQL;
   CREATE TABLE WORK.nb_inj_par_BNA3 AS 
   SELECT             (max(t1.num_vac)) AS n_inj, 
          t1.BEN_NIR_ANO
      FROM WORK.vac4 t1
      GROUP BY t1.BEN_NIR_ANO
      ORDER BY n_inj DESC;
QUIT;
title "Effectifs par nombre de doses durant la période d'étude";
proc freq data=WORK.nb_inj_par_BNA3;
tables n_inj;

run;




/*Nombre injections selon timing pour les grossesses coincidant avec la periode vaccinale(>27122020)*/

PROC SQL;
   CREATE TABLE WORK.nb_inj_timing AS 
   SELECT distinct t1.indsej, t1.timing,
          /* COUNT_DISTINCT_of_EXE_SOI_DTD */
            (COUNT(DISTINCT(t1.EXE_SOI_DTD))) AS n_inj
      FROM WORK.chainage_VAC3 t1
	  WHERE t1.dat_evt > '27Dec2020'd
      GROUP BY t1.indsej, t1.timing
ORDER BY indsej, timing, n_inj;
QUIT;



/*garder une ligne par indsej avec les différents timings*/
data timing_inj(keep=indsej long);
  do until (last.indsej);
    set work.nb_inj_timing;
    by indsej;
    length long $200;
    long= catx(' ', long, n_inj, ' ', timing);
  end;
run;




title "Effectifs par nombre de doses selon timing";

proc freq data=WORK.timing_inj (where=(long like '%during%'));
tables long;
run;


proc freq data=WORK.timing_inj (where=(long not like '%during%' and long not like '0'));
tables long;
run;




/****** Template 3b:code pour obtenir les profils vaccinaux*******/

/*récuperer patientes exposées*/
PROC SQL;
   CREATE TABLE WORK.base_vac_exposed AS 
   SELECT t1.*
      FROM WORK.chainage_vac3 t1
      WHERE t1.timing = 'during';
QUIT;




proc sort data=WORK.base_vac_exposed; by indsej; run;

DATA work.profils;
SET WORK.base_vac_exposed;
BY indsej  exe_soi_dtd;
length v_profile $25.;
IF  FIRST.indsej THEN DO; 
v_profile=trim(nom_vac)||put(num_vac,1.);
retain v_profile;
END;
else do;
v_profile=trim(v_profile)||trim(nom_vac)||put(num_vac,1.); 
retain v_profile;
end;
run;




PROC SQL;
   CREATE TABLE WORK.PROFILS2 AS 
   SELECT distinct t1.BEN_NIR_ANO, 
   t1.indsej,
          /* MAX_of_v_profile */
            (MAX(t1.v_profile)) AS v_profile
      FROM WORK.PROFILS t1
      GROUP BY t1.ben_nir_ano, t1.indsej;
QUIT;



/*sauvegarde profils individuels durant grossesse + variable
non-ARN : au moins 1 vaccin non ARN pendant grossesse
+ date du premier vaccin pendant grossesse*/
PROC SQL;
   CREATE TABLE work.profils_vac_during AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
   t1.indsej,
   t2.firstvac,
          t1.v_profile, 
          /* non_ARN */
            (t1.v_profile like '%AZD%' OR t1.v_profile like '%JJJ%' OR t1.v_profile like '%NVX%' 
OR t1.v_profile like '%SAN%' )
AS non_ARN
      FROM WORK.PROFILS2 t1
INNER JOIN (
   SELECT t1.BEN_NIR_ANO, 
   t1.indsej,
          /* firstvac_during */
            (MIN(t1.EXE_SOI_DTD)) FORMAT=DDMMYYS10. AS firstvac
      FROM WORK.BASE_VAC_EXPOSED t1
      GROUP BY t1.indsej) t2

ON T1.BEN_NIR_ano=t2.ben_nir_ano and t1.indsej=t2.indsej;
QUIT;

/*nb de non_arn*/
proc freq data=WORK.profils_vac_during;
tables non_arn;

run;



/*FREQUENCE DES PROFILS*/

PROC SQL;
   CREATE TABLE WORK.count_PROFILS2 AS 
   SELECT t1.v_profile, 
          /* COUNT_of_BEN_NIR_ANO */
            (COUNT(t1.INDSEJ)) AS COUNT_of_BEN_NIR_ANO
      FROM WORK.PROFILS2 t1
      GROUP BY t1.v_profile;
QUIT;


proc freq data=WORK.profils2;
tables v_profile;

run;


/*BNA avec Grossesses durant periode vaccinale*/

PROC SQL;
   CREATE TABLE WORK.POP AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.timing
      FROM WORK.CHAINAGE_VAC3 t1
      WHERE t1.dat_evt > '27Dec2020'd;
QUIT;


/*garder une ligne par BNA avec les différents timings*/
data timing(keep=BEN_nir_ano long);
  do until (last.ben_nir_ano);
    set work.pop;
    by ben_nir_ano;
    length long $200;
    long= catx(' ', long, timing);
  end;
run;

proc freq data=WORK.timing;
tables long;

run;









