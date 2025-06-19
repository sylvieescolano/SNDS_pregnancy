/*RECHERCHE DE TOUS LES CODES CIM ASSOCIES AUX PATIENTES DE LA BASE
pour IDENTIFICATION DES PRINCIPAUX OUTCOMES POUR LES MERES ET FOETUS*/

proc datasets lib=orauser kill;
run;quit;



libname rep '/home/sas/42a001279410899/sasdata/REPMEDGR/Base_Grossesse';
%INCLUDE '/home/sas/42a001279410899/sasdata/REPMEDGR/macros.sas';

/*Librairie du referentiel IR BEN R (la table évolue, sauvegarde du 02/02/2024 dans repmedgr)*/
libname repMEDGR '/home/sas/42a001279410899/sasdata/REPMEDGR';

/*Récuperer tous les PSA de IRBENR02022024 associés aux BNA de BASEG*/
PROC SQL;
   CREATE TABLE WORK.PSA_MAT AS 
   SELECT DISTINCT t1.BEN_NIR_ANO as BNA_base, 
   	t2.BEN_NIR_ANO,
          t2.BEN_NIR_PSA
      FROM rep.baseG t1
           INNER JOIN repMEDGR.ir_ben_r_02022024 t2 ON (t1.BEN_NIR_ANO = t2.BEN_NIR_ANO);
QUIT;

/*Liste des PSA*/
PROC SQL;
   CREATE TABLE ORAUSER.LIST_psa_mat AS 
   SELECT DISTINCT  t1.BEN_NIR_PSA
      FROM WORK.PSA_MAT t1;
           
QUIT;

/*MACRO POUR LA RECHERCHE DE CODES CIM DANS MCO POUR UNE POPULATION DONNEE (une ou PLUSIEURS ANNEES)
VARIABLES D'ENTREE
table_id= nom de la table *** ORAUSER *** comprenant la liste des ID de la population
nom_id= nom de la variable ID dans la table à chainer au NIR_ANO_17 du MCO
annee_list= liste des codes années à rechercher (ICI: 13 14 15 16 17 18 19 20 21 22 23_09)

VARIABLES DE SORTIE

CIM_MCO= Table avec une ligne par ID, CIM, 
type de diag et date_diag (date d'entrée du séjour correspondant)*/



*%recherche_cim (table_id=ORAUSER.LIST_psa_mat , nom_id=BEN_NIR_PSA , 
annee_list= 13 14 15 16 17 18 19 20 21 22 23_09);




/*Joindre TABLE CIM_MCO avec BNA comme identifiant et 
sauvegarder la table dans REP en gardant les diag <=2022*/

PROC SQL;
   CREATE TABLE REP.cim_MAT AS 
   SELECT DISTINCT t2.BEN_NIR_ANO, 
          t1.CIM, 
			T1.diag_type,
          t1.date_diag
      FROM WORK.CIM_MCO t1
	 
           INNER JOIN WORK.PSA_MAT t2 ON (t1.BEN_NIR_PSA = t2.BEN_NIR_PSA)
 WHERE t1.DATE_DIAG<='31dec2022'd;
QUIT;
