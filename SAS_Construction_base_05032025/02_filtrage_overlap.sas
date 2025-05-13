/*objectifs: à partir de la base 2:
- elimination des doublons de sejours pour chaque BEN NIR ANO
(respect d'un délai >0 entre le LMP calculé d'une issue et la date d'issue
de la grossesse précédente)*/



/* Suppression fichiers ORAUSER  ET WORK*/
proc datasets lib=orauser kill;
run;quit;

proc datasets lib=work kill;
run;quit;

/**TABLE REUNISSANT TOUS LES SEJOURS extraits du PMSI apres filtrage NIR+AGE: BASE2**/

/*CHOIX DU REPERTOIRE*/
libname rep'/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base';




/*Nombre de séjours par BEN NIR ANO */

PROC SQL;
   CREATE TABLE WORK.countSEJ AS 
   SELECT /* COUNT_of_indsej */
            (COUNT(t1.indsej)) AS COUNT_of_indsej, 
          t1.BEN_NIR_ANO
      FROM REP.BASE2 t1
      GROUP BY t1.BEN_NIR_ANO
      ORDER BY COUNT_of_indsej DESC;
QUIT;

PROC SQL;
   CREATE TABLE WORK.countSEJ2 AS 
   SELECT /* COUNT_of_COUNT_of_indsej */
            (COUNT(t1.COUNT_of_indsej)) AS nb_BENNIRANO, 
          t1.COUNT_of_indsej AS nb_sej
      FROM WORK.countSEJ t1
      GROUP BY t1.COUNT_of_indsej;
QUIT;










/*** TRAITEMENT DES DOUBLONS (plrs séjours pour un BNA) ***/

/*Tous les séjours avec doublons*/

PROC SQL;
   CREATE TABLE WORK.doublons AS 
   SELECT t1.AGE_ANN, 
          t1.AGE_GES, 
          t1.BEN_DCD_DTE, 
          t1.BEN_NIR_ANO,  
          t1.DAT_EVT, 
          t1.DEL_REG_ENT, 
          t1.DUREEG, 
          t1.ENT_DAT_DEL, 
          t1.ETA_NUM, 
          t1.EXE_SOI_DTD, 
          t1.EXE_SOI_DTF, 
		  t1.sor_mod,
          t1.GRG_GHM, 
          t1.ID_MAM_ENF, 
		  t1.INDSEJ,
          /* COUNT_of_INDSEJ */
            (COUNT(t1.INDSEJ)) AS COUNT_of_INDSEJ, 
          t1.ISSUE, 
          t1.LMP_CALC, 
          t1.NIR_ANO_17, 
          t1.RSA_NUM
      FROM rep.base2 t1
      GROUP BY t1.BEN_NIR_ANO
      HAVING (CALCULATED COUNT_of_INDSEJ) > 1;
QUIT;




/*MACRO DE TRAITEMENT DES DOUBLONS
(num_end= nombre maximum d'evenements a identifier)*/

/*Choix du delai minimum (en j) entre dat_evt et LMP evenement suivant*/
%let del=0;

proc sort data=WORK.doublons; by ben_nir_ano dat_evt; run;


/*V2*/
%macro process_events(num_end);

/*initialisation*/

data tmp1;
            set work.doublons;
			by ben_nir_ano;

           
            delta_evtprec_lmp = lmp_calc - lag(dat_evt);
            delta_evt1_lmp = lmp_calc - first.dat_evt;

            /* Condition pour changer num_evt = delai >del*/
            if delta_evt1_lmp > &del. then do;
                num_evt = 2;
            end;

            /* Initialisation lors de la première observation pour chaque groupe */
            if first.ben_nir_ano then do;
                delta_evtprec_lmp = .;
                delta_evt1_lmp = .;
                first.dat_evt = dat_evt;
                num_evt = 1;
            end;
        run;


    %do i=2 %to &num_end.;

		data sejevt_aumoins_&i.;
		set tmp%eval(&i.-1);
		if num_evt ^= &i. then delete;
		run;

				
        data work.sej_evt%eval(&i.-1);
            set tmp%eval(&i.-1);
            if num_evt =&i. or num_evt = . then delete;
        run;
		

        /* Création du dataset temporaire */

		    data tmp&i.;
            set sejevt_aumoins_&i.;
			by ben_nir_ano;

            

            delta_evtprec_lmp = lmp_calc - lag(dat_evt);
            delta_evt&i._lmp = lmp_calc - first.dat_evt;

            /* Condition pour changer num_evt = delai >del*/
            if delta_evt&i._lmp > &del. then do;
                num_evt = &i. + 1;
            end;
			else do; num_evt=.;end;

            /* Initialisation lors de la première observation pour chaque groupe */
            if first.ben_nir_ano then do;
                delta_evtprec_lmp = .;
                
                delta_evt&i._lmp = .;
                first.dat_evt = dat_evt;
                num_evt = &i.;
            end;
        run;

       
	    %end;

 /* Création de la table agrégée sej_evt_all */
    data work.sej_evt_all;
        set %do j=1 %to %eval(&num_end. - 1); work.sej_evt&j. %end;;
    run;

%mend;



%process_events(24); 

/*Connaitre nombre max d'évenements*/
PROC SQL;
   CREATE TABLE WORK.max_evt AS 
   SELECT /* MAX_of_num_evt */
            (MAX(t1.num_evt)) AS MAX_of_num_evt
      FROM WORK.SEJ_EVT_ALL t1;
QUIT;




 
 /*Ajout de la table tmp1 avec ts les sejours des doublons*/

proc sort data=WORK.sej_evt_all; by ben_nir_ano dat_evt; run;
 PROC SQL;
   CREATE TABLE WORK.doublons_nonfiltre AS 
   SELECT t2.AGE_ANN, 
          t2.AGE_GES, 
          t2.DEL_REG_ENT, 
          t2.dureeG, 
          t2.ENT_DAT_DEL, 
          t2.ETA_NUM, 
          t2.EXE_SOI_DTD, 
          t2.EXE_SOI_DTF, 
		  t2.sor_mod,
          t2.GRG_GHM, 
		  t2.BEN_DCD_DTE,
          t2.ID_MAM_ENF, 
          t2.RSA_NUM, 
          t2.indsej, 
          t2.NIR_ANO_17, 
          t2.BEN_NIR_ANO, 
          t2.issue, 
          t2.lmp_calc, 
          t2.dat_evt, 
          t1.num_evt
      FROM WORK.SEJ_EVT_ALL t1
           RIGHT JOIN WORK.doublons t2 ON (t1.indsej = t2.indsej)
      ORDER BY t2.BEN_NIR_ANO,
               t2.dat_evt;
QUIT;


/*Ajouter le numero du sejour pour comparer avec numero d'evenement*/
proc sort data=work.doublons_nonfiltre; by ben_nir_ano dat_evt; run;
data work.doublons_nonfiltre;
set work.doublons_nonfiltre;
by ben_nir_ano;
num_sej +1;
if first.ben_nir_ano then do;
                   num_sej = 1;end;
              
            run;





/*Compter le Nb de séjours etnb d'evenements par BNA*/
PROC SQL;
   CREATE TABLE WORK.COUNT_sej_evt AS 
   SELECT /* MAX_of_num_evt */
            (MAX(t1.num_evt)) AS MAX_of_num_evt, 
          /* MAX_of_num_sej */
            (MAX(t1.num_sej)) AS MAX_of_num_sej, 
          t1.BEN_NIR_ANO
      FROM WORK.DOUBLONS_NONFILTRE t1
      GROUP BY t1.BEN_NIR_ANO;
QUIT;

PROC SQL;
   CREATE TABLE WORK.COUNT_sej_evt2 AS 
   SELECT /* COUNT_of_BEN_NIR_ANO */
            (COUNT(t1.BEN_NIR_ANO)) AS COUNT_of_BEN_NIR_ANO, 
          t1.MAX_of_num_evt, 
          t1.MAX_of_num_sej
      FROM WORK.COUNT_sej_evt t1
      GROUP BY t1.MAX_of_num_evt,
               t1.MAX_of_num_sej;
QUIT;




/*Récupérer séjours sans doublons BEN NIR ANO*/

PROC SQL;
   CREATE TABLE WORK.nodoublons AS 
   SELECT t1.AGE_ANN, 
          t1.AGE_GES, 
          t1.BEN_DCD_DTE, 
          t1.BEN_NIR_ANO, 
          t1.DAT_EVT, 
          t1.DEL_REG_ENT, 
          t1.DUREEG, 
          t1.ENT_DAT_DEL, 
          t1.ETA_NUM, 
          t1.EXE_SOI_DTD, 
          t1.EXE_SOI_DTF, 
		  t1.sor_mod,
          t1.GRG_GHM, 
          t1.ID_MAM_ENF, 
		  t1.INDSEJ,
          /* COUNT_of_INDSEJ */
            (COUNT(t1.INDSEJ)) AS COUNT_of_INDSEJ, 
          t1.ISSUE, 
          t1.LMP_CALC, 
          t1.NIR_ANO_17, 
          t1.RSA_NUM
      FROM rep.base2 t1
      GROUP BY t1.BEN_NIR_ANO
      HAVING (CALCULATED COUNT_of_INDSEJ) = 1;
QUIT;


/*Filtrer la table des séjours doublons en gardant seulement les evts distincts*/
PROC SQL;
   CREATE TABLE WORK.doublons_filtre AS 
   SELECT t1.*
      FROM WORK.DOUBLONS_NONFILTRE t1
      WHERE t1.num_evt NOT = .;
QUIT;






/*Base finale avec une ligne par séjour retenu
avant eventuel filtre sur age/LMP/date evt*/

data rep.baseG (drop=count_of_indsej);
set nodoublons doublons_filtre;
Annee = YEAR(dat_EVT);
run;






/*Nombre de BNA distincts dans la base*/
PROC SQL;
   CREATE TABLE WORK.count_bna_gvdn AS 
   SELECT /* COUNT_DISTINCT_of_BEN_NIR_ANO */
            (COUNT(DISTINCT(t1.BEN_NIR_ANO))) AS COUNT_DISTINCT_of_BEN_NIR_ANO
      FROM REP.BASEG t1;
QUIT;

title "Répartition par type d'issue par année";
/*Compter nb évenements par type d'issue*/
proc freq data=REP.BASEG ;
		tables issue*Annee;
	run;


%macro loop_count_issue;
%do year = 2013 %to 2022;
PROC SQL;
 CREATE TABLE WORK.COUNT_SEJG_&year. AS 
   SELECT t1.issue, 
      
            (COUNT(t1.issue)) AS COUNT_of_issue
      FROM rep.baseG t1
	  WHERE ANNEE=&year.
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
%mend loop_count_issue;
%loop_count_issue;


