
/*REPERAGE DECES NEONAT
(pour les ENV et à partir du BEN NIR ANO)*/

/*On utilise pas la table des causes de deces dans ce programme car années 2021 2022 2023 non présentes*/


/*On rcupre l'information du dcs dans le PMSI*/

proc datasets lib=orauser kill;
run;quit;

proc datasets lib=work kill;
run;quit;
libname rep '/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse/Construction_base';

/*Librairie du referentiel IR BEN R (la table évolue, sauvegarde du 02/02/2024 dans repmedgr)*/
libname repmedgr '/home/sas/42a001279410899/sasdata/REPMEDGR';


/*Créer table de correspondance BNA/PSA à partir du référentiel*/
PROC SQL;
   CREATE TABLE orauser.corresp_id_patient AS 
   SELECT DISTINCT t1.BEN_NIR_ANO1 AS BEN_NIR_ANO, 
          t2.BEN_NIR_PSA
      FROM rep.CHAINAGEME_ALL t1
           LEFT JOIN REPMEDGR.IR_BEN_R_02022024 t2 ON (t1.BEN_NIR_ANO1 = t2.BEN_NIR_ANO)
WHERE t1.issue='ENV' and t1.BNA_OK=1;
QUIT;




/*MACRO POUR IDENTIFIER LES DECES A PARTIR DES 4 TABLES DU PMSI PAR LE MODE DE SORTIE
(d'apres code de la HAS)
Variable d'entrée = liste des identifiants aa des tables MCO demandées*/




%macro deces_PMSI(name_list= 13 14 15 16 17 18 19 20 21 22 23_09);

%local i next_name;
%let i=1;
%do %while (%scan(&name_list, &i) ne );
   %let an = %scan(&name_list, &i);


	/*Dans le PMSI HAD*/		
		proc sql;

					CREATE TABLE deces_PMSI_HAD_&an. AS (
						SELECT DISTINCT
							a.BEN_NIR_ANO,
							c.EXE_SOI_DTD,
							c.EXE_SOI_DTF AS BEN_DCD_DTE
						FROM orauser.corresp_id_patient a
							INNER JOIN oravue.T_HAD&an.C c
								ON a.BEN_NIR_PSA = c.NIR_ANO_17
							INNER JOIN oravue.T_HAD&an.B b
								ON	c.ETA_NUM_EPMSI = b.ETA_NUM_EPMSI
								AND	c.RHAD_NUM = b.RHAD_NUM
						WHERE b.SOR_MOD = '9' 				
							AND NIR_RET = '0' AND NAI_RET = '0' AND SEX_RET = '0' AND SEJ_RET = '0' AND FHO_RET = '0' 
							AND PMS_RET = '0' AND DAT_RET = '0' %if &an. > 12 %then %do; AND COH_NAI_RET = '0' 
							AND COH_SEX_RET = '0' %end;);
					

			
		quit;

		data deces_PMSI_HAD_&an. (rename = (EXE_SOI_DTD2 = EXE_SOI_DTD BEN_DCD_DTE2 = BEN_DCD_DTE));
			set deces_PMSI_HAD_&an.;
			length EXE_SOI_DTD2 BEN_DCD_DTE2 4.;
			EXE_SOI_DTD2 = datepart(EXE_SOI_DTD);
			BEN_DCD_DTE2 = datepart(BEN_DCD_DTE);
			source='HAD';
			format EXE_SOI_DTD2 BEN_DCD_DTE2 ddmmyy10.;
			drop EXE_SOI_DTD BEN_DCD_DTE;
		run;

		/* Dans le PMSI - MCO*/		
		proc sql;

				CREATE TABLE deces_PMSI_MCO_&an. AS 
						SELECT DISTINCT
							a.BEN_NIR_ANO,
							c.EXE_SOI_DTD,
							c.EXE_SOI_DTF AS BEN_DCD_DTE
						FROM orauser.corresp_id_patient a
							INNER JOIN oravue.T_MCO&an.C c
								ON a.BEN_NIR_PSA = c.NIR_ANO_17
							INNER JOIN oravue.T_MCO&an.B b
								ON	c.ETA_NUM = b.ETA_NUM
								AND	c.RSA_NUM = b.RSA_NUM
						WHERE b.SOR_MOD = '9' 
							AND NIR_RET = '0' AND NAI_RET = '0' AND SEX_RET = '0' AND SEJ_RET = '0' AND FHO_RET = '0' 
							AND PMS_RET = '0' AND DAT_RET = '0' %if &an. > 12 %then %do; AND COH_NAI_RET = '0' 
							AND COH_SEX_RET = '0' %end;
							AND c.ETA_NUM NOT IN ('130780521', '130783236', '130783293', '130784234', '130804297', '600100101', 
								'750041543', '750100018', '750100042', '750100075', '750100083', '750100091', '750100109', 
								'750100125', '750100166', '750100208', '750100216', '750100232', '750100273', '750100299', 
								'750801441', '750803447', '750803454', '910100015', '910100023', '920100013', '920100021', 
								'920100039', '920100047', '920100054', '920100062', '930100011', '930100037', '930100045', 
								'940100027', '940100035', '940100043', '940100050', '940100068', '950100016', '690783154', 
								'690784137', '690784152', '690784178', '690787478', '830100558')
							AND GRG_GHM NOT LIKE ('90%')
							AND (SEJ_TYP <>'B' OR SEJ_TYP IS NULL) ;


		quit;

		data deces_PMSI_MCO_&an. (rename = (EXE_SOI_DTD2 = EXE_SOI_DTD BEN_DCD_DTE2 = BEN_DCD_DTE));
			set deces_PMSI_MCO_&an.;
			length EXE_SOI_DTD2 BEN_DCD_DTE2 4.;
			EXE_SOI_DTD2 = datepart(EXE_SOI_DTD);
			BEN_DCD_DTE2 = datepart(BEN_DCD_DTE);
			source='MCO';
			format EXE_SOI_DTD2 BEN_DCD_DTE2 ddmmyy10.;
			drop EXE_SOI_DTD BEN_DCD_DTE;
		run;

		/* Dans le PMSI - RIP*/
		proc sql;

			
				CREATE TABLE deces_PMSI_RIP_&an. AS  
						SELECT DISTINCT
							a.BEN_NIR_ANO,
							c.EXE_SOI_DTD,
							c.EXE_SOI_DTF AS BEN_DCD_DTE
						FROM orauser.corresp_id_patient a
							INNER JOIN oravue.T_RIP&an.C c
								ON a.BEN_NIR_PSA = c.NIR_ANO_17
							INNER JOIN oravue.T_RIP&an.RSA rsa
								ON	c.ETA_NUM_EPMSI = rsa.ETA_NUM_EPMSI
								AND	c.RIP_NUM = rsa.RIP_NUM
						WHERE rsa.SOR_MOD = '9' 
							AND NIR_RET = '0' AND NAI_RET = '0' AND SEX_RET = '0' AND SEJ_RET = '0' AND FHO_RET = '0' 
							AND PMS_RET = '0' AND DAT_RET = '0' %if &an. > 12 %then %do; AND COH_NAI_RET = '0' 
							AND COH_SEX_RET = '0' %end;
						;


		quit;

		data deces_PMSI_RIP_&an. (rename = (EXE_SOI_DTD2 = EXE_SOI_DTD BEN_DCD_DTE2 = BEN_DCD_DTE));
			set deces_PMSI_RIP_&an.;
			length EXE_SOI_DTD2 BEN_DCD_DTE2 4.;
			EXE_SOI_DTD2 = datepart(EXE_SOI_DTD);
			BEN_DCD_DTE2 = datepart(BEN_DCD_DTE);
			source='RIP';
			format EXE_SOI_DTD2 BEN_DCD_DTE2 ddmmyy10.;
			drop EXE_SOI_DTD BEN_DCD_DTE;
		run;

		/* Dans le PMSI - SSR*/	
		proc sql;


				CREATE TABLE deces_PMSI_SSR_&an. AS  (
						SELECT DISTINCT
							a.BEN_NIR_ANO,
							c.EXE_SOI_DTD,
							c.EXE_SOI_DTF AS BEN_DCD_DTE
						FROM orauser.corresp_id_patient a
							INNER JOIN oravue.T_SSR&an.C c
								ON a.BEN_NIR_PSA = c.NIR_ANO_17
							INNER JOIN oravue.T_SSR&an.B b
								ON	c.ETA_NUM = b.ETA_NUM
								AND	c.RHA_NUM = b.RHA_NUM
						WHERE b.SOR_MOD = '9' 
							AND NIR_RET = '0' AND NAI_RET = '0' AND SEX_RET = '0' AND SEJ_RET = '0' AND FHO_RET = '0' 
							AND PMS_RET = '0' AND DAT_RET = '0' %if &an. > 12 %then %do; AND COH_NAI_RET = '0' 
							AND COH_SEX_RET = '0' %end;
						);


		quit;

		data deces_PMSI_SSR_&an. (rename = (EXE_SOI_DTD2 = EXE_SOI_DTD BEN_DCD_DTE2 = BEN_DCD_DTE));
			set deces_PMSI_SSR_&an.;
			length EXE_SOI_DTD2 BEN_DCD_DTE2 4.;
			EXE_SOI_DTD2 = datepart(EXE_SOI_DTD);
			BEN_DCD_DTE2 = datepart(BEN_DCD_DTE);
			source='SSR';
			format EXE_SOI_DTD2 BEN_DCD_DTE2 ddmmyy10.;
			drop EXE_SOI_DTD BEN_DCD_DTE;
		run;




   %let i = %eval(&i + 1);
%end;

	/* On concatne toutes les tables*/
	data deces_PMSI (keep= BEN_NIR_ANO BEN_DCD_DTE SOURCE);
		set deces_PMSI_:;
	run;

	/* On supprime les tables temporaires*/
	*proc datasets library = work memtype = data nolist;
	*delete deces_PMSI_:;
	*run; *quit;


%mend deces_PMSI;

%deces_PMSI;


/*On garde BNA et date de deces*/
PROC SQL;
   CREATE TABLE WORK.DECES AS 
   SELECT DISTINCT            BEN_NIR_ANO, BEN_DCD_DTE
      FROM WORK.DECES_PMSI;
QUIT;

/*Vérifier l'absence de doublons de BNA*/
PROC SQL;
   CREATE TABLE WORK.verif_doublons AS 
   SELECT             (COUNT(t1.BEN_NIR_ANO)) AS COUNT_of_BEN_NIR_ANO, 
                    (COUNT(DISTINCT(t1.BEN_NIR_ANO))) AS COUNT_DISTINCT_of_BEN_NIR_ANO
      FROM WORK.DECES T1;
QUIT;

/*On garde la premiere date de deces pour les doublons (et on retire dates manquantes)*/

PROC SQL;
   CREATE TABLE WORK.deces_PMSI AS
   SELECT   BEN_NIR_ANO,
   MIN(t1.BEN_dcd_DTE)FORMAT=DDMMYY10.  AS BEN_DCD_DTE,
   'PMSI' AS SOURCE
      FROM WORK.DECES T1
	  WHERE BEN_DCD_DTE IS NOT MISSING
GROUP BY T1.ben_nir_ano;
QUIT;







/*deces provenant de IRBENR (01jan1600=valeur manquante)*/


PROC SQL;
   CREATE TABLE WORK.DECES_IRBENR AS 
   SELECT DISTINCT t1.BEN_NIR_ANO1 AS BEN_NIR_ANO, 
          datepart(t2.BEN_dcd_dte)FORMAT=DDMMYY10. AS BEN_DCD_DTE , 
		  'IRBENR' as SOURCE
      FROM rep.CHAINAGEME_ALL t1
           LEFT JOIN REPMEDGR.IR_BEN_R_02022024 t2 ON (t1.BEN_NIR_ANO1 = t2.BEN_NIR_ANO)
WHERE t1.issue='ENV' and t1.BNA_OK=1 AND T2.ben_dcd_dte>'01jan1600'd;
QUIT;






/*Joindre les 2 tables et compter le nombre de doublons (2 sources différentes ou 2 dates de déces differentes par BNA)*/

data work.deces2; set work.deces_IRBENR work.deces_PMSI; run;

PROC SQL;
   CREATE TABLE WORK.count_DECES AS 
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          
            (COUNT(DISTINCT(t1.SOURCE))) AS COUNT_DISTINCT_of_SOURCE, 
        
            (COUNT(DISTINCT(t1.BEN_DCD_DTE))) AS COUNT_DISTINCT_of_BEN_DCD_DTE
      FROM WORK.DECES2 t1
      GROUP BY t1.BEN_NIR_ANO
      ORDER BY COUNT_DISTINCT_of_SOURCE DESC,
               COUNT_DISTINCT_of_BEN_DCD_DTE DESC;
QUIT;

proc freq data=count_deces;  tables COUNT_DISTINCT_of_BEN_DCD_DTE COUNT_DISTINCT_of_SOURCE; run;



/* Pour les BNA avec 2 dates distinctes, garder si source=IRBENR
(date la +recente si 2 dates IRBENR) */

	PROC SQL;
   CREATE TABLE WORK.DECES3 AS 
   (SELECT DISTINCT t1.BEN_NIR_ANO, 
          max(t2.BEN_DCD_DTE) FORMAT=DDMMYY10. as ben_dcd_dte
      FROM WORK.COUNT_DECES t1
           INNER JOIN WORK.DECES2 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO)
      WHERE t1.COUNT_DISTINCT_of_BEN_DCD_DTE > 1 AND t2.SOURCE = 'IRBENR')
	  union(
   SELECT DISTINCT t1.BEN_NIR_ANO, 
          t2.BEN_DCD_DTE
      FROM WORK.COUNT_DECES t1
           INNER JOIN WORK.DECES2 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO)
      WHERE t1.COUNT_DISTINCT_of_BEN_DCD_DTE =1);
QUIT;



/*ajout de la nouvelle variable dans la base chainage mere enfant*/


PROC SQL;
   CREATE TABLE WORK.tmp AS 
   SELECT distinct t2.* ,
          t1.BEN_DCD_DTE AS date_deces1
        
      FROM WORK.DECES3 t1
           RIGHT JOIN rep.chainageme_all t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO1);
QUIT;



/*Ajout de la variable maternal_death dans base chainage si deces le jour de l'evt 
ou dans les 28j
+ retirer la date de deces si <dat_evt (8 dates)*/





data rep.chainageme_all; set work.tmp;
length neonat_death 3.;

if date_deces1 < dat_evt then do; 
date_deces1=.;
end;

if date_deces1 >= dat_evt  and date_deces1 <=dat_evt + 28 then do; 
neonat_death=1;
end;
else do; neonat_death=0;end;
RUN;




/*compte evenements */

PROC SQL;
   CREATE TABLE WORK.compte_decesNN AS 
   SELECT /* COUNT_of_BEN_NIR_ANO */
            (COUNT(t1.BEN_NIR_ANO)) AS COUNT
      FROM rep.chainageme_all t1
      WHERE t1.neonat_death=1;
QUIT;




title 'neonat_death parmi ENV';
proc freq data= rep.chainageme_all (where=(issue='ENV')); 
tables neonat_death *annee;
RUN;
