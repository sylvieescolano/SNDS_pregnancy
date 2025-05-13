/*Objectifs : obtenir pour chaque indsej la commune + l'indice de deefavorisation associé 
(on garde le plus bas en cas de doublon)
+ la variable CMU (au moins une conso dans l'année)*/

proc datasets lib=orauser kill;
run;quit;

proc datasets lib=work kill;
run;quit;

libname rep '/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base';


/*Récupérer infos des tables EXTRACTION_PATIENTS de l'annnées de l'evt*/

/*Pour les evenements en 2021 : TABLE EXTRACTION PATIENTS 2021*/



%macro extraction;

proc datasets lib=orauser kill;run;quit;

proc datasets lib=work kill;run;quit;

%do annee = 2013 %to 2022;
PROC SQL;
   CREATE TABLE ORAUSER.baseg&annee. AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.indsej,
		  t1.annee
      FROM rep.baseg t1
      WHERE t1.Annee = &annee.;
QUIT;
PROC SQL;
   CREATE TABLE WORK.tmp&annee. AS 
   SELECT distinct t1.BEN_NIR_ANO,
			t1.indsej, 
			t1.annee,
          t2.ben_cmu_top, 
          t2.depcom, 
          t2.quint_defa,
		  &annee AS SOURCE_EP
      FROM orauser.baseG&annee. t1
           left JOIN CONSOPAT.EXTRACTION_PATIENTS&annee.TR t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO)
		   ;
      
QUIT;
%end;

data EXTRACTION1;
		set work.tmp:;
	run;

%mend;

*%extraction;

/*enregistrer la table dans REPMEDGR(traitement long)*/

/*data rep.EXTRACTION1; set work.extraction1;run;*/

data work.EXTRACTION1; set rep.extraction1;run;

/*Doublons CMU ou depcom*/

PROC SQL;
   CREATE TABLE WORK.QUERY AS 
   SELECT t1.BEN_NIR_ANO, 
          t1.INDSEJ, 
          /* COUNT_DISTINCT_of_ben_cmu_top */
            (COUNT(DISTINCT(t1.ben_cmu_top))) AS COUNT_DISTINCT_of_ben_cmu_top, 
          /* COUNT_DISTINCT_of_depcom */
            (COUNT(DISTINCT(t1.depcom))) AS COUNT_DISTINCT_of_depcom
      FROM work.EXTRACTION1 t1
      GROUP BY t1.BEN_NIR_ANO,
               t1.INDSEJ;
QUIT;

PROC SQL;
   CREATE TABLE WORK.doublons AS 
   SELECT t1.BEN_NIR_ANO, 
          t1.COUNT_DISTINCT_of_ben_cmu_top, 
          t1.COUNT_DISTINCT_of_depcom, 
          t1.INDSEJ
      FROM WORK.QUERY t1
      ORDER BY t1.COUNT_DISTINCT_of_depcom DESC,
               t1.COUNT_DISTINCT_of_ben_cmu_top DESC;
QUIT;

proc freq data=work.doublons ; 
tables COUNT_DISTINCT_of_ben_cmu_top COUNT_DISTINCT_of_depcom;run;


/*Garder pour chaque indsej le max de CMU_TOP et de quint_defa (+defavorisé)*/
PROC SQL;
   CREATE TABLE WORK.max AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.INDSEJ, 
          /* MAX_of_quint_defa */
            (MAX(t1.quint_defa)) AS MAX_of_quint_defa, 
          /* MAX_of_ben_cmu_top */
            (MAX(t1.ben_cmu_top)) AS MAX_of_ben_cmu_top
      FROM work.extraction1 t1
      GROUP BY t1.BEN_NIR_ANO,
               t1.INDSEJ;
QUIT;


/*Garder un seul depcom par indsej parmi les depcom correspondants au quintdefa max*/

PROC SQL;
   CREATE TABLE WORK.unique AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t1.INDSEJ, 
          t1.MAX_of_ben_cmu_top AS ben_cmu_top, 
          t1.MAX_of_quint_defa AS quint_defa, 
          /* MIN_of_depcom */
            (MIN(t2.depcom)) AS depcom
      FROM WORK.MAX t1, WORK.EXTRACTION1 t2
      WHERE (t1.INDSEJ = t2.INDSEJ AND t1.MAX_of_quint_defa = t2.quint_defa)
      GROUP BY t1.BEN_NIR_ANO,
               t1.INDSEJ,
               t1.MAX_of_ben_cmu_top,
               t1.MAX_of_quint_defa;
QUIT;





/*Ajout taille Unité Urbaine (classification urban/rural)*/

PROC SQL;
   CREATE TABLE WORK.taille_uu AS 
   SELECT t1.BEN_NIR_ANO, 
   t1.indsej,
   t1.ben_cmu_top,
t1.quint_defa,
          t1.depcom, 
          t2.taille_uu
      FROM WORK.unique t1
           LEFT JOIN CONSOPAT.DEFA_UU2009 t2 ON (t1.depcom = t2.depcom)
      ORDER BY t2.taille_uu;
QUIT;


/*récupérer uniquement la variable numérique*/

data work.taille_uu2;
	set work.taille_uu;

indice_uu = input(substr(taille_uu, 1, 1),1.);

	run;
		


/*ajout variable 'urban_rural" : "rural" = indice_uu en 0 uniquement*/


data work.taille_uu3 (drop=taille_uu);
	set WORK.taille_uu2;

format urban_rural $8.;

if indice_uu = . then do; urban_rural=''; end;
else if indice_uu<1 then do; urban_rural='rural'; end;
	else do;urban_rural='urban';end;
	run;
	


/*TABLE FINALE*/
data rep.sociodemo; set taille_uu3;run;



title "Effectifs totaux";
PROC FREQ data=WORK.taille_uu3; tables ben_cmu_top quint_defa urban_rural;run;







































