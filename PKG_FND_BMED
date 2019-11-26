create or replace PACKAGE SOGG_OUTELAB.PKG_FND_BMED
	/**
	* redditivita fondi bmed
	*/
IS

	-- codici pagamento di bonifico e assegno per i fondi MIF
	-- codBon4MIF  CONSTANT SOGG_OUTELAB.TB_CHAR2 := SOGG_OUTELAB.TB_CHAR2('03', '09', 'A3', 'BB', 'BE', 'BH', 'BI', 'PL', 'RP');
	-- codAss4MIF  CONSTANT SOGG_OUTELAB.TB_CHAR2 := SOGG_OUTELAB.TB_CHAR2('01', '02', '48');

	-- codici pagamento di bonifico e assegno per i fondi MGF
	-- codBonAss4MGF  CONSTANT SOGG_OUTELAB.TB_CHAR2 := SOGG_OUTELAB.TB_CHAR2('01', '02', '03', '04', '08', '09', '10', '12', '14', '15', '21', '22', '23',
	-- '31', '32', '33', '34', '39', '48', '49', '50', '72', '73', '77', '78', '82', '83', '89', 'A3', 'AR', 'BI', 'BL', 'BN', 'BO', 'BP', 'CR', 'D1', 'D2',
	-- 'D3', 'D4', 'D5', 'FA', 'FB', 'FC', 'FD', 'FE', 'FF', 'IS', 'IT', 'IV', 'IW', 'IX', 'IY', 'IZ', 'M3', 'PI', 'PR', 'PY', 'RP', 'SA', 'SI', 'SV');

  k_pagam_assegno  constant varchar2(50) := 'CAUSALE_ASSEGNO';
  k_pagam_bonifico constant varchar2(50) := 'CAUSALE_BONIFICO';

  tpPagam_ASS_MGF SOGG_OUTELAB.TB_CHAR2 := SOGG_OUTELAB.TB_CHAR2();
  tpPagam_BON_MGF SOGG_OUTELAB.TB_CHAR2 := SOGG_OUTELAB.TB_CHAR2();
  tpPagam_ASS_MIF SOGG_OUTELAB.TB_CHAR2 := SOGG_OUTELAB.TB_CHAR2();
  tpPagam_BON_MIF SOGG_OUTELAB.TB_CHAR2 := SOGG_OUTELAB.TB_CHAR2();

    GLOBAL_REC_TYPE        CHAR(20) := 'FND_BMED';

	GLOBAL_REC_TYPE_PARENT CHAR(20) := 'FND_BMED_PAD';
 	------------------------------------------------------------------------------------------------------
	--------- CURSORI
	------------------------------------------------------------------------------------------------------
    CURSOR c_gruppo_ognimese(cRec SOGG_OUTELAB.RT_OUTPUT_ONLINE)
    IS

        SELECT COD_GRUP_FONDO_OGNMS
        FROM CAT_PROD.DETT_FONDO_OGNMS
        WHERE cod_isin   = cRec.COD_ISIN
        AND GSTD_F_ESIST = 'S';


	CURSOR C_DT_ACC_CONTR(cRec SOGG_OUTELAB.TB_OUTPUT_ONLINE)
	IS

		SELECT TRIM(PROD_C)
			||'-'
			||TRIM(CONTR_N) CHIAVE,
			Cn.Contr_F_Blocco_Contr COD_BLOCCO_CONTR,
			(SELECT COUNT(1)
			FROM CLL.MODPG mp
			WHERE mp.gstd_f_esist    ='S'
			AND mp.contr_n           =cn.contr_n
			AND mp.MODPG_D_REVOCA_RID>TRUNC(SYSDATE)+1
			AND mp.prod_c            =cn.prod_c
			) RID_ATTIVE,
		/*
        (SELECT COUNT(1)
		FROM SOGG_OUTELAB.Vw_Dett_Legm_Iniz_Comm ic, 
			cll.contr cn1
		WHERE ic.COD_INIZ_COMM='B2'
		AND cn1.prod_c        =ic.prod_c_dis
		AND cn1.contr_n       =ic.contr_n_dis
		AND cn1.CONTR_C_SERV  =cn.prod_c
		AND cn1.CONTR_N_SERV  =cn.contr_n
		) IIS_ATTIVI*/
        0 IIS_ATTIVI
	FROM CLL.CONTR CN
	WHERE
		(
			CN.PROD_C, CN.CONTR_N
		)
		IN
		(SELECT A1.PROD_C, A1.CONTR_N FROM (TABLE(cRec)) A1
		)
	ORDER BY Cn.Gstd_F_Esist ASC ;

		CURSOR c_descrizioni_fp
		IS

			SELECT COD_FONDO_PADRE prod_c,
				DES_SICAV nome,
				COD_TIPO_ADES_FONDO
			FROM
				(SELECT a1.*,
					Row_Number () Over (partition BY COD_FONDO_PADRE order by ult_dt DESC) Rn
				FROM
					(SELECT df.COD_FONDO_PADRE,
						ac.DES_SICAV,
						MAX(df.GSTD_D_ULT_MODF_RECORD) ult_dt,
						CASE
							WHEN SUM(DECODE(df.COD_TIPO_ADES_FONDO,'PAC',1,0)) = 0
							THEN 'PIC'
							ELSE 'PAC'
						END COD_TIPO_ADES_FONDO
					FROM CAT_PROD.Anag_Fondo_Unif af,
						CAT_PROD.Dett_Fondo_Bmed df,
						CAT_PROD.Anag_Sicav ac
					WHERE df.cod_isin       =af.cod_isin
					AND df.COD_FONDO_PADRE <> '-'
					AND ac.COD_SICAV        =af.cod_sicav
					GROUP BY df.COD_FONDO_PADRE,
						ac.DES_SICAV
					) a1
				) a2
			WHERE rn=1
			UNION ALL
			SELECT 'FC0', 'Sistema Mediolanum Fondi Italia','PIC' FROM dual --prodotti vecchi non anagrafati
			UNION ALL
			SELECT 'EL0',
				'Sistema Mediolanum Fondi Italia',
				'PIC'
			FROM dual --prodotti vecchi non anagrafati
				;

	CURSOR c_datianagrafici
	IS
		with cedole_base_annua as
		(
		select distinct MAX(RENDIMENTO_ANNUO) KEEP (DENSE_RANK FIRST order by PERIODO_RIFERIMENTO_AL DESC )
		OVER (PARTITION BY NOME_FONDO) AS RENDIMENTO_ANNUO,
		NOME_FONDO,
		fa.PROD_C PROD_C
		FROM CLL.RENDIMENTO_CEDOLA r, CLL.FOL_ANAGRAFICA fa
		where r.rendimento_annuo is not null and r.gstd_f_esist = 'S'
		AND r.nome_fondo = TRIM(fa.PROD_C_ESTERN)
		)
		select anag.*, cedole_base_annua.RENDIMENTO_ANNUO from
		(
									  SELECT a1.*,
													COALESCE (trim(af.NOME_FONDO), trim(af.NOME_FONDO_SHORT)) nome,
													 CAST(DECODE(af.cod_soc_gest,'MEDIO','IRL','MGF','ITA','MG','IRL','ERR') AS CHAR(3)) stato,
			  PROD_C_ESTERN
			  --cedole_base_annua.RENDIMENTO_ANNUO
													--qui Ã¿Â¨ possibile aggiungere tutti i dati di anagrafica fondi bmed necessari
									  FROM
													(SELECT PROD_C,
																   COD_ISIN,
																   FLG_PIR,
																   COD_TIPO_ADES_FONDO
													FROM CAT_PROD.Dett_Fondo_Bmed DF
													WHERE NOT EXISTS
																   (SELECT 1
																   FROM CAT_PROD.Legm_Fondo_Ncoll_Fondo A2
																   WHERE A2.PROD_C_NCOLL=DF.PROD_C
																   )
									  UNION ALL
									  SELECT PROD_C_NCOLL,
													COD_ISIN,
													CAST('N' AS CHAR(1)) FLG_PIR,
													NULL
									  FROM CAT_PROD.Legm_Fondo_Ncoll_Fondo LF
													) a1, CAT_PROD.Anag_Fondo_Unif af
									  WHERE
																   af.cod_isin=a1.cod_isin
		) anag
		LEFT OUTER JOIN cedole_base_annua
		ON anag.PROD_C = cedole_base_annua.PROD_C;




			CURSOR c_totali_mandato(inputTable IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE, p_flgNascZero CHAR)
			IS

				SELECT COD_GEOG_FONDO,
					'X' as FLG_PIR, --#97903 visualizzazione PIR per ogni singolo fondo Italia e non su fondo Padre
					CONTR_N,
					PROD_C_PADRE,
					FLG_CHIUSO,
					SUM(IMP_INT_VERS  +IMP_SALDO_VERS*(max_DAT_RIFE-dat_rife)) IMP_INT_VERS,
					SUM(IMP_INT_INVEST+IMP_SALDO_INVEST*(max_DAT_RIFE-dat_rife)) IMP_INT_INVEST,
					SUM(IMP_SALDO_VERS) IMP_SALDO_VERS,
					SUM(IMP_SALDO_INVEST) IMP_SALDO_INVEST,
					SUM(IMP_TOT_VERS_NO_CONVS) IMP_TOT_VERS_NO_CONVS,
					SUM(IMP_TOT_INVEST_NO_CONVS) IMP_TOT_INVEST_NO_CONVS,
					SUM(IMP_TOT_RIMBO_NO_CONVS) IMP_TOT_RIMBO_NO_CONVS,
					SUM(IMP_PRVNT_PASS) IMP_PRVNT_PASS,
					SUM(IMP_PRVNT_FUT) IMP_PRVNT_FUT,
					SUM(IMP_CONTVAL_T1) IMP_CONTVAL_T1,
					SUM(IMP_CONTVAL_T2) IMP_CONTVAL_T2,
					MAX(DAT_RIFE) DAT_RIFE,
					MIN(DAT_APER_CONTR) DAT_APER_CONTR,
					MAX(DAT_CHIU_CONTR) DAT_CHIU_CONTR,
					MIN(DAT_PRIMO_MOVI) DAT_PRIMO_MOVI,
					MAX(DAT_ULT_MOVI) DAT_ULT_MOVI,
					MAX(QTA_QUOTA_T2) QTA_QUOTA_T2,
					CASE
						WHEN SUM(zero) >0
						THEN 'N'
						ELSE 'S'
					END FLG_ZERO
				FROM
					(SELECT COD_GEOG_FONDO,
						contr_n,
						PROD_C_PADRE,
						(MAX(DAT_RIFE) over (partition BY contr_n, PROD_C_PADRE, FLG_PIR, FLG_CHIUSO) ) max_DAT_RIFE,
						IMP_TOT_VERS_NO_CONVS,
						IMP_TOT_INVEST_NO_CONVS,
						IMP_TOT_RIMBO_NO_CONVS,
						IMP_PRVNT_PASS,
						IMP_PRVNT_FUT,
						DAT_RIFE,
						IMP_INT_VERS,
						IMP_SALDO_VERS,
						IMP_INT_INVEST,
						IMP_SALDO_INVEST,
						IMP_CONTVAL_T1,
						IMP_CONTVAL_T2,
						DAT_PRIMO_MOVI,
						DAT_ULT_MOVI,
						DAT_APER_CONTR,
						DAT_CHIU_CONTR,
						FLG_CHIUSO,
						QTA_QUOTA_T2,
						CASE
							WHEN FLG_ZERO = 'N'
							THEN 1
							ELSE 0
						END zero
					FROM (TABLE (inputTable)) it
					WHERE COD_GEOG_FONDO='ITA'
					AND nvl(trim(LOB_C),'XX')<> 'NOSEE'
					AND FLG_ZERO       IN ('N', CAST((
								CASE
									WHEN p_flgNascZero='N'
									THEN 'S'
									ELSE 'N'
								END) AS CHAR(1)) )
					AND PROD_C_PADRE IS NOT NULL
					) a1
			GROUP BY COD_GEOG_FONDO,
				CONTR_N,
				PROD_C_PADRE,
				FLG_CHIUSO
			UNION ALL
			SELECT COD_GEOG_FONDO,
				FLG_PIR,
				CONTR_N,
				PROD_C_PADRE,
				CAST((
						CASE
							WHEN SUM(chiuso)=0
							THEN 'S'
							ELSE 'N'
						END) AS CHAR(1)) FLG_CHIUSO,
				SUM(IMP_INT_VERS  +IMP_SALDO_VERS*(max_DAT_RIFE-dat_rife)) IMP_INT_VERS,
				SUM(IMP_INT_INVEST+IMP_SALDO_INVEST*(max_DAT_RIFE-dat_rife)) IMP_INT_INVEST,
				SUM(IMP_SALDO_VERS) IMP_SALDO_VERS,
				SUM(IMP_SALDO_INVEST) IMP_SALDO_INVEST,
				SUM(IMP_TOT_VERS_NO_CONVS) IMP_TOT_VERS_NO_CONVS,
				SUM(IMP_TOT_INVEST_NO_CONVS) IMP_TOT_INVEST_NO_CONVS,
				SUM(IMP_TOT_RIMBO_NO_CONVS) IMP_TOT_RIMBO_NO_CONVS,
				SUM(IMP_PRVNT_PASS) IMP_PRVNT_PASS,
				SUM(IMP_PRVNT_FUT) IMP_PRVNT_FUT,
				SUM(IMP_CONTVAL_T1) IMP_CONTVAL_T1,
				SUM(IMP_CONTVAL_T2) IMP_CONTVAL_T2,
				MAX(DAT_RIFE) DAT_RIFE,
				MIN(DAT_APER_CONTR) DAT_APER_CONTR,
				MAX(DAT_CHIU_CONTR) DAT_CHIU_CONTR,
				MIN(DAT_PRIMO_MOVI) DAT_PRIMO_MOVI,
				MAX(DAT_ULT_MOVI) DAT_ULT_MOVI,
				MAX(QTA_QUOTA_T2) QTA_QUOTA_T2,
				CASE
					WHEN SUM(zero) >0
					THEN 'N'
					ELSE 'S'
				END FLG_ZERO
			FROM
				(SELECT COD_GEOG_FONDO,
					contr_n,
					PROD_C_PADRE,
					(MAX(DAT_RIFE) over (partition BY contr_n, PROD_C_PADRE, FLG_PIR) ) max_DAT_RIFE,
					IMP_TOT_VERS_NO_CONVS,
					IMP_TOT_INVEST_NO_CONVS,
					IMP_TOT_RIMBO_NO_CONVS,
					IMP_PRVNT_PASS,
					IMP_PRVNT_FUT,
					DAT_RIFE,
					IMP_INT_VERS,
					IMP_SALDO_VERS,
					IMP_INT_INVEST,
					IMP_SALDO_INVEST,
					IMP_CONTVAL_T1,
					IMP_CONTVAL_T2,
					DAT_PRIMO_MOVI,
					DAT_ULT_MOVI,
					DAT_APER_CONTR,
					DAT_CHIU_CONTR,
					DECODE (FLG_CHIUSO, 'N', 1, 0) chiuso,
					QTA_QUOTA_T2,
					FLG_PIR,
					CASE
						WHEN FLG_ZERO = 'N'
						THEN 1
						ELSE 0
					END zero
				FROM (TABLE (inputTable)) it
				WHERE COD_GEOG_FONDO='IRL'
				AND nvl(trim(LOB_C),'XX')<> 'NOSEE'
				AND FLG_ZERO       IN ('N', CAST((
							CASE
								WHEN p_flgNascZero='N'
								THEN 'S'
								ELSE 'N'
							END) AS CHAR(1)) )
				AND PROD_C_PADRE IS NOT NULL
				) a1
			GROUP BY FLG_PIR,
				COD_GEOG_FONDO,
				CONTR_N,
				PROD_C_PADRE ;
                
			CURSOR c_load_redd_gior(chiavi SOGG_OUTELAB.TB_KEY_CONTR, t DATE)
			IS

				SELECT IMP_PLUS_MINUS_VERS,
					IMP_PLUS_MINUS_INVEST,
					PROD_C,
					PROD_C_PADRE,
					CONTR_N,
					COD_GEOG_FONDO,
					FLG_PIR,
					DAT_RIFE,
					DAT_APER_CONTR,
					DAT_CHIU_CONTR,
					DAT_PRIMO_MOVI,
					DAT_ULT_MOVI,
					IMP_TOT_VERS,
					IMP_TOT_VERS_NO_CONVS,
					IMP_TOT_INVEST,
					IMP_TOT_INVEST_NO_CONVS,
					IMP_TOT_RIMBO,
					IMP_TOT_RIMBO_NO_CONVS,
					IMP_PRVNT_PASS,
					IMP_PRVNT_FUT,
					QTA_QUOTA_T2,
					IMP_INT_VERS,
					IMP_SALDO_VERS,
					IMP_INT_INVEST,
					IMP_SALDO_INVEST,
					IMP_CONTVAL,
					QTA_QUOTA_SALDO,
					PRC_REND_VPATR_VERS_ST,
					PRC_REND_VPATR_VERS_ANNUAL,
					PRC_REND_VPATR_INVEST_ST,
					PRC_REND_VPATR_INVEST_ANNUAL,
					PRC_REND_MWRR_VERS_ST,
					PRC_REND_MWRR_VERS_ANNUAL,
					PRC_REND_MWRR_INVEST_ST,
					PRC_REND_MWRR_INVEST_ANNUAL
				FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_BMED
				WHERE DAT_RIFE=T
				AND
					(
						PROD_C, CONTR_N
					)
					IN
					(SELECT A1.PROD_C, A1.CONTR_N FROM (TABLE(CHIAVI)) A1
					)
			UNION ALL
			SELECT IMP_PLUS_MINUS_VERS,
				IMP_PLUS_MINUS_INVEST,
				PROD_C,
				PROD_C_PADRE,
				CONTR_N,
				COD_GEOG_FONDO,
				FLG_PIR,
				DAT_RIFE,
				DAT_APER_CONTR,
				DAT_CHIU_CONTR,
				DAT_PRIMO_MOVI,
				DAT_ULT_MOVI,
				IMP_TOT_VERS,
				IMP_TOT_VERS_NO_CONVS,
				IMP_TOT_INVEST,
				IMP_TOT_INVEST_NO_CONVS,
				IMP_TOT_RIMBO,
				IMP_TOT_RIMBO_NO_CONVS,
				IMP_PRVNT_PASS,
				IMP_PRVNT_FUT,
				QTA_QUOTA_T2,
				IMP_INT_VERS,
				IMP_SALDO_VERS,
				IMP_INT_INVEST,
				IMP_SALDO_INVEST,
				IMP_CONTVAL,
				QTA_QUOTA_SALDO,
				PRC_REND_VPATR_VERS_ST,
				PRC_REND_VPATR_VERS_ANNUAL,
				PRC_REND_VPATR_INVEST_ST,
				PRC_REND_VPATR_INVEST_ANNUAL,
				PRC_REND_MWRR_VERS_ST,
				PRC_REND_MWRR_VERS_ANNUAL,
				PRC_REND_MWRR_INVEST_ST,
				PRC_REND_MWRR_INVEST_ANNUAL
			FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_CHIU_BMED A3
			WHERE GSTD_D_ULT_MODF_RECORD<T+2
			AND
				(
					PROD_C, CONTR_N
				)
				IN
				(SELECT A1.PROD_C, A1.CONTR_N FROM (TABLE(CHIAVI)) A1
				)
			AND NOT EXISTS
				(SELECT 1
				FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_BMED A4
				WHERE A4.DAT_RIFE=T
				AND
					(
						A4.PROD_C, A4.CONTR_N
					)
					IN
					(SELECT A1.PROD_C, A1.CONTR_N FROM (TABLE(CHIAVI)) A1
					)
				AND A4.PROD_C =A3.PROD_C
				AND A4.CONTR_N=A3.CONTR_N
				) ;


				CURSOR eventi_fondi_bmed(chiavi SOGG_OUTELAB.TB_KEY_CONTR)
				IS

				SELECT
					PROD_C,
					CONTR_N,
					CAPITALE_VERSATO,
					CAPITALE_DA_REINVESTIMENTO,
					CAPITALE_INVESTITO,
					RIMBORSI_VERSO_C_C,
					RIMBORSI_PER_REINVESTIMENTO,
					COD_TIPO_PAGAM_PRVNT,
					PROVENTI,
					DATA_RIFERIMENTO,
					RIT_FISC,
					DATA_INC_PROV,
					EVENTIF_I_CTRV_LORDO,
					EVENTIF_C_TIP_PG,
					EVENTIF_Q_QTE,
					EVENTIF_I_LORDO,
					EVENTIF_X_TIP_OPERZ,
					DATA_DISINVESTIMENTO,
					QUANTITA,
					COD_SIGLA_INIZTV_COMMRC_FONDO,
					NUM_ISTA_CALL_CENTER,
					EVENTIF_D_VLT_INC
				 FROM (
					SELECT
						/*+ PARALLEL(8) */
						COALESCE (ia.prod_c, ef.prod_c) prod_c,
						COALESCE (ia.contr_n, ef.contr_n) contr_n,
						COALESCE (ia.CAPITALE_VERSATO, 0) CAPITALE_VERSATO,
						COALESCE (ia.CAPITALE_DA_REINVESTIMENTO, 0) CAPITALE_DA_REINVESTIMENTO,
						COALESCE (ia.CAPITALE_INVESTITO, 0) CAPITALE_INVESTITO,
						COALESCE (ia.RIMBORSI_VERSO_C_C, 0) RIMBORSI_VERSO_C_C,
						COALESCE (ia.RIMBORSI_PER_REINVESTIMENTO, 0) RIMBORSI_PER_REINVESTIMENTO,
						COALESCE (ia.COD_TIPO_PAGAM_PRVNT, CAST('03' AS CHAR(2)) ) COD_TIPO_PAGAM_PRVNT,
						COALESCE (ia.PROVENTI_LORDI_DISTRIBUITI,0) + COALESCE (ia.PROVENTI_IN_DISTRIBUZIONE,0) PROVENTI,
						COALESCE (es.DAT_DELIB_DISTR, ia.DATA_RIFERIMENTO, ef.EVENTIF_D_OPERZ_DECR) DATA_RIFERIMENTO,
						ROUND(COALESCE (CASE WHEN es.EVENTIFESTESA_C_DIV = 'ITL' THEN es.EVENTIFESTESA_I_RITFISC / 1936.27 ELSE es.EVENTIFESTESA_I_RITFISC END, 0), 2) RIT_FISC,
						CASE
							WHEN COALESCE (ia.PROVENTI_LORDI_DISTRIBUITI,0) + COALESCE (ia.PROVENTI_IN_DISTRIBUZIONE,0) > 0
							THEN ia.DATA_RIFERIMENTO
							ELSE NULL
						END DATA_INC_PROV,
						ROUND(DECODE(ef.EVENTIF_C_DIV, 'ITL', ef.EVENTIF_I_CTRV_LORDO / 1936.27, ef.EVENTIF_I_CTRV_LORDO), 2) EVENTIF_I_CTRV_LORDO,
						ef.EVENTIF_C_TIP_PG,
						ef.EVENTIF_Q_QTE,
						ROUND(DECODE(ef.EVENTIF_C_DIV, 'ITL', ef.EVENTIF_I_LORDO / 1936.27, ef.EVENTIF_I_LORDO), 2) EVENTIF_I_LORDO,
						ef.EVENTIF_X_TIP_OPERZ,
						(SELECT PT.DATA_DISINVESTIMENTO
						 FROM CLL.FOL_PORTFOLIO_CLIENTE PT
						 WHERE PT.PROD_C = COALESCE(IA.PROD_C,EF.PROD_C)
						 AND PT.CONTR_N = COALESCE(IA.CONTR_N, EF.CONTR_N)
						 AND PT.GSTD_F_ESIST = 'S') DATA_DISINVESTIMENTO,
                        (SELECT PT.QUANTITA
						 FROM CLL.FOL_PORTFOLIO_CLIENTE PT
						 WHERE PT.PROD_C = COALESCE(IA.PROD_C,EF.PROD_C)
						 AND PT.CONTR_N = COALESCE(IA.CONTR_N, EF.CONTR_N)
						 AND PT.GSTD_F_ESIST = 'S') QUANTITA,
						es.COD_SIGLA_INIZTV_COMMRC_FONDO,
						es.NUM_ISTA_CALL_CENTER,
						ef.EVENTIF_D_VLT_INC
					FROM
						(SELECT a1.*
						FROM (TABLE(chiavi)) lc,
							CLL.Motredd_Info_Aggiuntive_Fondi a1
						WHERE a1.gstd_f_esist='S'
						AND a1.prod_c        =lc.prod_c
						AND a1.contr_n       =lc.contr_n
						) ia
				FULL OUTER JOIN
					(SELECT a1.*
					FROM (TABLE(chiavi)) lc,
						cll.eventif a1
					WHERE a1.gstd_f_esist='S'
					AND a1.prod_c        =lc.prod_c
					AND a1.contr_n       =lc.contr_n
					) ef
				ON
					(
						Ef.Eventif_N_Operz=ia.NUMERO_OPERAZIONE
					AND ef.prod_c      =ia.prod_c
					AND ef.contr_n     =ia.contr_n
					)
				LEFT OUTER JOIN
					(SELECT a1.*
					FROM (TABLE(chiavi)) lc,
						cll.eventifestesa a1
					WHERE a1.gstd_f_esist     ='S'
					AND a1.EVENTIF_X_TIP_OPERZ='PR-'
					AND a1.prod_c             =lc.prod_c
					AND a1.contr_n            =lc.contr_n
					) es
				ON
					(
						es.EVENTIF_N_OPERZ=CAST(COALESCE (ia.NUMERO_OPERAZIONE, ef.Eventif_N_Operz) AS CHAR(13))
					AND es.prod_c      =ia.prod_c
					AND es.contr_n     =ia.contr_n
					)
				UNION ALL
				SELECT
						COALESCE (ia.prod_c, ef.prod_c) prod_c,
						COALESCE (ia.contr_n, ef.contr_n) contr_n,
						COALESCE (ia.CAPITALE_VERSATO, 0) CAPITALE_VERSATO,
						COALESCE (ia.CAPITALE_DA_REINVESTIMENTO, 0) CAPITALE_DA_REINVESTIMENTO,
						COALESCE (ia.CAPITALE_INVESTITO, 0) CAPITALE_INVESTITO,
						COALESCE (ia.RIMBORSI_VERSO_C_C, 0) RIMBORSI_VERSO_C_C,
						COALESCE (ia.RIMBORSI_PER_REINVESTIMENTO, 0) RIMBORSI_PER_REINVESTIMENTO,
						COALESCE (ia.COD_TIPO_PAGAM_PRVNT, CAST('03' AS CHAR(2)) ) COD_TIPO_PAGAM_PRVNT,
						COALESCE (ia.PROVENTI_LORDI_DISTRIBUITI,0) + COALESCE (ia.PROVENTI_IN_DISTRIBUZIONE,0) PROVENTI,
						COALESCE (es.DAT_DELIB_DISTR, ia.DATA_RIFERIMENTO, ef.EVENTIF_D_OPERZ_DECR) DATA_RIFERIMENTO,
						ROUND(COALESCE (CASE WHEN es.EVENTIFESTESA_C_DIV = 'ITL' THEN es.EVENTIFESTESA_I_RITFISC / 1936.27 ELSE es.EVENTIFESTESA_I_RITFISC END, 0), 2) RIT_FISC,
						CASE
							WHEN COALESCE (ia.PROVENTI_LORDI_DISTRIBUITI,0) + COALESCE (ia.PROVENTI_IN_DISTRIBUZIONE,0) > 0
							THEN ia.DATA_RIFERIMENTO
							ELSE NULL
						END DATA_INC_PROV,
						ROUND(DECODE(ef.EVENTIF_C_DIV, 'ITL', ef.EVENTIF_I_CTRV_LORDO / 1936.27, ef.EVENTIF_I_CTRV_LORDO), 2) EVENTIF_I_CTRV_LORDO,
						ef.EVENTIF_C_TIP_PG,
						ef.EVENTIF_Q_QTE,
						ROUND(DECODE(ef.EVENTIF_C_DIV, 'ITL', ef.EVENTIF_I_LORDO / 1936.27, ef.EVENTIF_I_LORDO), 2) EVENTIF_I_LORDO,
						ef.EVENTIF_X_TIP_OPERZ,
						(SELECT PT.DATA_DISINVESTIMENTO
						 FROM CLL.FOL_PORTFOLIO_CLIENTE PT
						 WHERE PT.PROD_C = COALESCE(IA.PROD_C,EF.PROD_C)
						 AND PT.CONTR_N = COALESCE(IA.CONTR_N, EF.CONTR_N)
						 AND PT.GSTD_F_ESIST = 'S') DATA_DISINVESTIMENTO,
                        (SELECT PT.QUANTITA
						 FROM CLL.FOL_PORTFOLIO_CLIENTE PT
						 WHERE PT.PROD_C = COALESCE(IA.PROD_C,EF.PROD_C)
						 AND PT.CONTR_N = COALESCE(IA.CONTR_N, EF.CONTR_N)
						 AND PT.GSTD_F_ESIST = 'S') QUANTITA,
						 es.COD_SIGLA_INIZTV_COMMRC_FONDO,
						 es.NUM_ISTA_CALL_CENTER,
						 ef.EVENTIF_D_VLT_INC
					FROM
						(SELECT a1.*
						FROM (TABLE(chiavi)) lc,
							CLL.MOTREDD_INFO_AGG_FONDI_RETTF a1
						WHERE a1.gstd_f_esist='S'
						AND a1.prod_c        =lc.prod_c
						AND a1.contr_n       =lc.contr_n
						) ia
				FULL OUTER JOIN
					(SELECT a1.*
					FROM (TABLE(chiavi)) lc,
						cll.eventif_rettf a1
					WHERE a1.gstd_f_esist='S'
					AND a1.prod_c        =lc.prod_c
					AND a1.contr_n       =lc.contr_n
					) ef
				ON
					(
						Ef.Eventif_N_Operz =ia.NUMERO_OPERAZIONE
					AND ef.prod_c      	   =ia.prod_c
					AND ef.contr_n         =ia.contr_n
					)
				LEFT OUTER JOIN
					(SELECT a1.*
					FROM (TABLE(chiavi)) lc,
						cll.eventifestesa a1
					WHERE a1.gstd_f_esist     ='S'
					AND a1.EVENTIF_X_TIP_OPERZ='PR-'
					AND a1.prod_c             =lc.prod_c
					AND a1.contr_n            =lc.contr_n
					) es
				ON
					(
						es.EVENTIF_N_OPERZ=CAST(COALESCE (ia.NUMERO_OPERAZIONE, ef.Eventif_N_Operz) AS CHAR(13))
					AND es.prod_c      =ia.prod_c
					AND es.contr_n     =ia.contr_n
					)
				) SQ
				ORDER BY DATA_RIFERIMENTO;

				CURSOR c_chiavi_del_giorno(blkl NUMBER, c_COD_TIPO_RECORD CHAR)
				IS

					SELECT SOGG_OUTELAB.RT_KEY_CONTR(a1.prod_c, a1.contr_n)
					FROM
						( SELECT DISTINCT lc.prod_c,
							lc.contr_n
						FROM SOGG_OUTELAB.DETT_LOG_MOVI_CONTR lc
						WHERE Dat_Rife    <= TRUNC(SYSDATE)
						AND COD_TIPO_RECORD=c_COD_TIPO_RECORD
						AND gstd_f_esist   ='S'
						) a1
				WHERE rownum<=blkl ;

            CURSOR c_contr_massivo(chiavi SOGG_OUTELAB.TB_KEY_CONTR)
            IS

                SELECT cn.*
                FROM (TABLE(chiavi)) ks,
                    cll.contr cn
                WHERE cn.prod_c=ks.prod_c
                AND cn.contr_n =ks.contr_n ;

  CURSOR C_Causali_Pagam is
	SELECT TRIM(COD_CAMPO_MWRR) chiave,
	DES_CAMPO_MWRR descrizione, COD_TIPO_DOM_MWRR tipo
	FROM SOGG_OUTELAB.MULTIDOM_MWRR
	WHERE COD_TIPO_DOM_MWRR in ('CAUS_P_MIF','CAUS_P_MGF')
	ORDER BY COD_TIPO_DOM_MWRR, DES_CAMPO_MWRR;

				  cursor c_eventif_loi (in_prod_c char, in_contr_n char, data_rife  DATE, t2 DATE) is
					  SELECT MIN(EF.EVENTIF_D_OPERZ_DECR) EVENTIF_D_OPERZ_DECR
					  FROM CLL.EVENTIF EF
					  WHERE EF.PROD_C = in_prod_c
					  AND EF.CONTR_N = in_contr_n
					  AND EF.EVENTIF_X_TIP_OPERZ IN ('415',  '421')
					  AND EF.EVENTIF_D_OPERZ_DECR BETWEEN data_rife AND t2;


				CURSOR c_load_aper_fnd_bmed
				IS

				WITH controvalori AS (
					(SELECT pr.contr_n_padre,
						pr.PROD_C_PADRE PROD_C_PADRE,
						pr.prod_c_figlio1,
						MIN(Pr.Controvalore) Controvalore
					FROM ser_mifid.prodotti pr
					WHERE pr.RUOLO                IN ('I','P')
					AND pr.PORTAFOGLIO             ='A'
					AND pr.GSTD_D_ULT_MODF_RECORD >= (
							CASE
								WHEN
									(SELECT COUNT(1)
										FROM CLL.Motredd_Config_Redditivita
										WHERE Identificativo ='ENVIRONMENT'
										AND valore           ='DEV'
									)
								=0
							THEN TRUNC(CURRENT_DATE)
							ELSE to_date('20130323','YYYYMMDD')
						END )
					AND pr.GSTD_D_ULT_MODF_RECORD < (
							CASE
								WHEN
									(SELECT COUNT(1)
										FROM CLL.Motredd_Config_Redditivita
										WHERE Identificativo ='ENVIRONMENT'
										AND valore           ='DEV'
									)
									=0
								THEN TRUNC(CURRENT_DATE)
								ELSE to_date('20130323','YYYYMMDD')
							END ) + 1
					AND 1=1
					GROUP BY pr.contr_n_padre,
						pr.PROD_C_PADRE,
						pr.prod_c_figlio1
					) )
				SELECT
					/*+ PARALLEL(8) */
					tf.*,
					COALESCE (a1.CONTROVALORE,0) IMP_CONTVAL,
					COALESCE (tf.POSIZF_I_DISPN_CONTAB,0) QTA_QUOTA_SALDO,
					TRUNC(CURRENT_DATE)-1 DAT_RIFE,
					TRUNC(pt.DATA_DISINVESTIMENTO) DATA_DISINVESTIMENTO,
					COALESCE (pt.QUANTITA,0) QUANTITA
				FROM
					(SELECT tf.*,
						pf.POSIZF_I_DISPN_CONTAB POSIZF_I_DISPN_CONTAB
					FROM SOGG_OUTELAB.SALDO_TOT_FONDO_BMED tf,
						cll.posizf pf
					WHERE pf.contr_n       = tf.contr_n
					AND pf.prod_c          = tf.prod_c
					AND tf.dat_chiu_contr IS NULL
					AND pf.GSTD_F_ESIST    ='S'
					) tf
				LEFT OUTER JOIN controvalori a1
				ON A1.Prod_C_Figlio1 = tf.prod_c
				AND a1.contr_n_padre = tf.contr_n
				LEFT OUTER JOIN CLL.FOL_PORTFOLIO_CLIENTE pt
				ON pt.prod_c = tf.prod_c
				AND pt.contr_n = tf.contr_n
				AND pt.gstd_f_esist = 'S'
				ORDER BY tf.contr_n,
					tf.COD_GEOG_FONDO ;





	type tb_datianagrafici IS TABLE OF c_datianagrafici%rowtype INDEX BY VARCHAR(11);

	type tb_datianagrafici_pl IS TABLE OF c_datianagrafici%rowtype;

	datianagrafici tb_datianagrafici;

	type tb_descpadre IS TABLE OF c_descrizioni_fp%rowtype INDEX BY VARCHAR(11);

	type tb_descpadre_pl IS TABLE OF c_descrizioni_fp%rowtype;

	descpadre tb_descpadre;

	type tb_myIndex IS TABLE OF NUMBER INDEX BY VARCHAR(32);

	type tb_contr IS TABLE OF cll.contr%rowtype INDEX BY VARCHAR(32);

	tppag_storni SOGG_OUTELAB.TB_CHAR3 := NULL;

	tppag_convers SOGG_OUTELAB.TB_CHAR3 := NULL;

	tpmovi_pos CONSTANT SOGG_OUTELAB.TB_CHAR3 := SOGG_OUTELAB.TB_CHAR3('390', '391', '392', '393', '400', '401', '402', '403', '404', '405', '406', '407', '414', '416', '418', '419', '420', '513', '514', '524', '525', '530', '540', '541', '542', '543', '544', '545', '546', '547', '548', '549', '550', '551', '552', '553', '554', '555', '556', '557', '558', '559', '560', '561', '562', '564', '567', '570', '571', '572', '573', '574', '577', '578', '581', '582', '600', '602');

	/**
	* codici movimento di disinvestimento, al momento non sono stati censiti sul DBAZ da cui la necessita' di codificarli
	*/

	tpmovi_neg CONSTANT SOGG_OUTELAB.TB_CHAR3 := SOGG_OUTELAB.TB_CHAR3('408', '409', '410', '411', '412', '413', '415', '417', '421', '565', '566', '575', '576', '585', '586');

	FUNCTION loi_non_raggiunta (in_prod_c CHAR, in_contr_n CHAR, data_rife DATE, t2 DATE) RETURN DATE;


PROCEDURE add_caus_tipoPagam;

PROCEDURE calc_redd_giornaliero(
		chiavi IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
		dt_rif DATE,
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
		flgNascChiusi CHAR,
		flgNascZero   CHAR);


	PROCEDURE agg_tab_totali_gg(
			bulk_limit IN NUMBER,
			esito OUT CHAR,
			num_rec_elab OUT INTEGER,
			num_rec_scart OUT INTEGER,
			num_rec_warn OUT INTEGER);


	PROCEDURE agg_tab_totali(
			totali IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_CONTR );

	PROCEDURE legg_contr_info(
			chiavi        IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
			contr_attuali IN OUT nocopy tb_contr );
            
	PROCEDURE calc_totali_fondi_bmed(
			in_cTotaliFondiBMED IN OUT nocopy tb_myIndex,
			cTotaliFondiBMED    IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_CONTR,
			cEventoBmed         IN OUT nocopy eventi_fondi_bmed%rowtype,
			contr_attuali       IN OUT nocopy tb_contr,
			p_t1          DATE DEFAULT NULL,
			p_t2          DATE DEFAULT NULL,
			codicePeriodo CHAR DEFAULT NULL );            
            
	PROCEDURE calc_redd_aperti(
			bulk_limit IN NUMBER,
			esito OUT CHAR,
			num_rec_elab OUT INTEGER,
			num_rec_scart OUT INTEGER,
			num_rec_warn OUT INTEGER);            

	PROCEDURE ins_dati_online(
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
			dest IN CHAR);            


           
END;
/

create or replace PACKAGE BODY SOGG_OUTELAB.PKG_FND_BMED
IS


PROCEDURE popola_gruppo_ognimese(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE)
IS
	gruppo_ognimese NUMBER;
BEGIN

	FOR i IN 1 .. cRec.COUNT
	LOOP
		BEGIN
			gruppo_ognimese := NULL;
			OPEN c_gruppo_ognimese(cRec(i));

			FETCH c_gruppo_ognimese INTO gruppo_ognimese;

			cRec(i).GRUP_OGNMS := gruppo_ognimese;
			CLOSE c_gruppo_ognimese;

		EXCEPTION

		WHEN no_data_found THEN
			CLOSE c_gruppo_ognimese;

		END;

	END LOOP;

END;

PROCEDURE popola_dati_contratto(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE)
IS
	l_chiave VARCHAR(32);
type tb_dt_acc
IS
	TABLE OF C_DT_ACC_CONTR%rowtype;
type tb_dt_acc_in
IS
	TABLE OF C_DT_ACC_CONTR%rowtype INDEX BY VARCHAR(32);
	dati_accessori tb_dt_acc;
	dati_accessori_in tb_dt_acc_in;
BEGIN
	OPEN C_DT_ACC_CONTR(cRec);

	FETCH C_DT_ACC_CONTR bulk collect INTO dati_accessori;

	CLOSE C_DT_ACC_CONTR;

	FOR i IN 1 .. dati_accessori.count
	LOOP
		dati_accessori_in(dati_accessori(i).chiave):=dati_accessori(i);

	END LOOP;

	FOR i IN 1 .. cRec.COUNT
	LOOP
		l_chiave             := TRIM(cRec(i).PROD_C)||'-'||TRIM(cRec(i).CONTR_N);
		cRec(i).FLG_BLOC_VEND:='N';
		cRec(i).FLG_RID_ATTIV:='N';

		IF dati_accessori_in.exists(l_chiave) THEN

			IF dati_accessori_in(l_chiave).COD_BLOCCO_CONTR NOT IN ('N','0',' ') THEN
				cRec(i).FLG_BLOC_VEND :='S';

			END IF;

			IF dati_accessori_in(l_chiave).RID_ATTIVE>0 THEN
				cRec(i).FLG_RID_ATTIV                  :='S';

			END IF;

			IF dati_accessori_in(l_chiave).IIS_ATTIVI>0 THEN
				cRec(i).FLG_ATTIV_IIS                  :='S';

			END IF;

		END IF;

	END LOOP;

END;


PROCEDURE loadDatiAnagrafici
IS
	prodisin tb_datianagrafici_pl;
	descpadre_l tb_descpadre_pl;
BEGIN
	OPEN c_datianagrafici;

	FETCH c_datianagrafici bulk collect INTO prodisin;

	CLOSE c_datianagrafici;

	FOR i IN 1 .. prodisin.COUNT
	LOOP
		datianagrafici(trim(prodisin(i).prod_c)):=prodisin(i);

	END LOOP;
	OPEN c_descrizioni_fp;

	FETCH c_descrizioni_fp bulk collect INTO descpadre_l;

	CLOSE c_descrizioni_fp;

	FOR i IN 1 .. descpadre_l.COUNT
	LOOP
		descpadre(trim(descpadre_l(i).prod_c)):=descpadre_l(i);

	END LOOP;

END;


PROCEDURE popola_dati_accessori(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE)
IS
	prod_c_trim VARCHAR(11);
BEGIN

	IF datianagrafici.count = 0 THEN
		loadDatiAnagrafici;

	END IF;

	FOR i IN 1 .. cRec.COUNT
	LOOP
		prod_c_trim := trim(cRec(i).PROD_C);

		IF datianagrafici.exists(prod_c_trim) AND cRec(i).REC_TYPE='FND_BMED' THEN
			cRec(i).COD_ISIN                                        :=datianagrafici(prod_c_trim).cod_isin;
			cRec(i).DEC_DESC                                        :=datianagrafici(prod_c_trim).nome;
			cRec(i).PRC_REND_PRVNT_ANNUAL                           := datianagrafici(prod_c_trim).RENDIMENTO_ANNUO;

			IF datianagrafici(prod_c_trim).COD_TIPO_ADES_FONDO='PAC' THEN
				cRec(i).FLG_PAC                                 := 'S';

			ELSE
				cRec(i).FLG_PAC := 'N';

			END IF;

		END IF;

		IF descpadre.exists(prod_c_trim) AND cRec(i).REC_TYPE='FND_BMED_PAD' THEN
			cRec(i).DEC_DESC                                   :=descpadre(prod_c_trim).nome;

			IF descpadre(prod_c_trim).COD_TIPO_ADES_FONDO='PAC' THEN
				cRec(i).FLG_PAC                            := 'S';

			ELSE
				cRec(i).FLG_PAC := 'N';

			END IF;

		END IF;

	END LOOP;

END;

PROCEDURE calc_fondi_padre(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
		dat_rif     DATE,
		flgNascZero CHAR)
IS
type tb_tot
IS
	TABLE OF c_totali_mandato%rowtype;
	totaliFondiPadre tb_tot;
BEGIN
	--dbms_output.put_line('calc_fondi_padre');
	OPEN c_totali_mandato(cRec, flgNascZero);

	FETCH c_totali_mandato bulk collect INTO totaliFondiPadre;

	CLOSE c_totali_mandato;

	FOR i IN 1 .. totaliFondiPadre.COUNT
	LOOP
		cRec.extend(1);
		cRec(cRec.last)                  := SOGG_OUTELAB.RT_OUTPUT_ONLINE( NULL );
		cRec(cRec.last).REC_TYPE         := GLOBAL_REC_TYPE_PARENT;
		cRec(cRec.last).prod_c           := totaliFondiPadre(i).PROD_C_PADRE;
		cRec(cRec.last).contr_n          := totaliFondiPadre(i).contr_n;
		cRec(cRec.last).COD_GEOG_FONDO   := totaliFondiPadre(i).COD_GEOG_FONDO;
		cRec(cRec.last).FLG_PIR          := totaliFondiPadre(i).FLG_PIR;
		cRec(cRec.last).FLG_ZERO         := totaliFondiPadre(i).FLG_ZERO;
		cRec(cRec.last).FLG_CHIUSO       := totaliFondiPadre(i).FLG_CHIUSO;
		cRec(cRec.last).DAT_RIFE         := totaliFondiPadre(i).DAT_RIFE;
		cRec(cRec.last).DAT_APER_CONTR   := totaliFondiPadre(i).DAT_APER_CONTR;
		cRec(cRec.last).DAT_CHIU_CONTR   := totaliFondiPadre(i).DAT_CHIU_CONTR;
		cRec(cRec.last).DAT_PRIMO_MOVI   := totaliFondiPadre(i).DAT_PRIMO_MOVI;
		cRec(cRec.last).DAT_ULT_MOVI     := totaliFondiPadre(i).DAT_ULT_MOVI;
		cRec(cRec.last).IMP_TOT_VERS     := totaliFondiPadre(i).IMP_TOT_VERS_NO_CONVS;
		cRec(cRec.last).IMP_TOT_INVEST   := totaliFondiPadre(i).IMP_TOT_INVEST_NO_CONVS;
		cRec(cRec.last).IMP_TOT_RIMBO    := totaliFondiPadre(i).IMP_TOT_RIMBO_NO_CONVS;
		cRec(cRec.last).IMP_PRVNT_PASS   := totaliFondiPadre(i).IMP_PRVNT_PASS;
		cRec(cRec.last).IMP_PRVNT_FUT    := totaliFondiPadre(i).IMP_PRVNT_FUT;
		cRec(cRec.last).IMP_CONTVAL_T1   := totaliFondiPadre(i).IMP_CONTVAL_T1;
		cRec(cRec.last).IMP_CONTVAL_T2   := totaliFondiPadre(i).IMP_CONTVAL_T2;
		cRec(cRec.last).IMP_INT_VERS     := totaliFondiPadre(i).IMP_INT_VERS;
		cRec(cRec.last).IMP_SALDO_VERS   := totaliFondiPadre(i).IMP_SALDO_VERS;
		cRec(cRec.last).IMP_INT_INVEST   := totaliFondiPadre(i).IMP_INT_INVEST;
		cRec(cRec.last).IMP_SALDO_INVEST := totaliFondiPadre(i).IMP_SALDO_INVEST;
		SOGG_OUTELAB.calc_flg_stato(cRec(cRec.last), dat_rif);
		SOGG_OUTELAB.calc_indici_redd(cRec(cRec.last),'N');

	END LOOP;

END;

PROCEDURE aggiustaFlagStatoIrlanda(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE)
IS
type tp_chiaviNonZero
IS
	TABLE OF BOOLEAN INDEX BY VARCHAR(32);
	chiaviNonZero tp_chiaviNonZero;
BEGIN
	--dbms_output.put_line('aggiustaFlagStatoIrlanda');

	FOR i IN 1 .. cRec.COUNT
	LOOP

		IF cRec(i).COD_GEOG_FONDO                                          = 'IRL' AND cRec(i).FLG_CHIUSO = 'N' AND cRec(i).FLG_ZERO = 'N' THEN
			chiaviNonZero(trim(cRec(i).PROD_C_PADRE)||trim(cRec(i).CONTR_N)) := true;

		END IF;

	END LOOP;

	FOR i IN 1 .. cRec.COUNT
	LOOP

		IF chiaviNonZero.exists(trim(cRec(i).PROD_C_PADRE)||trim(cRec(i).CONTR_N)) THEN
			cRec(i).FLG_CHIUSO:='N';
			cRec(i).FLG_ZERO  :='N';

		END IF;

	END LOOP;

END;



PROCEDURE calc_redd_giornaliero(
		chiavi IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
		dt_rif DATE,
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
		flgNascChiusi CHAR,
		flgNascZero   CHAR)
IS
type tb_redd
IS
	TABLE OF c_load_redd_gior%rowtype;
	dati_giornaliero tb_redd;
	dati_output SOGG_OUTELAB.TB_OUTPUT_ONLINE;
	lobcode CHAR(5):='FND';
BEGIN
	dati_output := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	dbms_output.put_line('calc_redd_giornaliero dt_rif:' || TO_CHAR(dt_rif,'YYYY-MM-DD HH24:MI:SS'));
	OPEN c_load_redd_gior(chiavi, dt_rif);

	FETCH c_load_redd_gior bulk collect INTO dati_giornaliero;

	CLOSE c_load_redd_gior;

	FOR i IN 1 .. dati_giornaliero.COUNT
	LOOP
		dati_output.extend(1);
		dati_output(dati_output.last)                         := SOGG_OUTELAB.RT_OUTPUT_ONLINE( lobcode );
		dati_output(dati_output.last).REC_TYPE                := GLOBAL_REC_TYPE;
		dati_output(dati_output.last).PROD_C                  := dati_giornaliero(i).PROD_C ;
		dati_output(dati_output.last).PROD_C_PADRE            := dati_giornaliero(i).PROD_C_PADRE ;
		dati_output(dati_output.last).CONTR_N                 := dati_giornaliero(i).CONTR_N ;
		dati_output(dati_output.last).COD_GEOG_FONDO          := dati_giornaliero(i).COD_GEOG_FONDO ;
		dati_output(dati_output.last).FLG_PIR                 := dati_giornaliero(i).FLG_PIR ;
		dati_output(dati_output.last).DAT_RIFE                := dati_giornaliero(i).DAT_RIFE ;
		dati_output(dati_output.last).DAT_APER_CONTR          := dati_giornaliero(i).DAT_APER_CONTR ;
		dati_output(dati_output.last).DAT_CHIU_CONTR          := dati_giornaliero(i).DAT_CHIU_CONTR ;
		dati_output(dati_output.last).DAT_PRIMO_MOVI          := dati_giornaliero(i).DAT_PRIMO_MOVI ;
		dati_output(dati_output.last).DAT_ULT_MOVI            := dati_giornaliero(i).DAT_ULT_MOVI ;
		dati_output(dati_output.last).IMP_TOT_VERS            := dati_giornaliero(i).IMP_TOT_VERS ;
		dati_output(dati_output.last).IMP_TOT_VERS_NO_CONVS   := dati_giornaliero(i).IMP_TOT_VERS_NO_CONVS ;
		dati_output(dati_output.last).IMP_TOT_INVEST          := dati_giornaliero(i).IMP_TOT_INVEST ;
		dati_output(dati_output.last).IMP_TOT_INVEST_NO_CONVS := dati_giornaliero(i).IMP_TOT_INVEST_NO_CONVS ;
		dati_output(dati_output.last).IMP_TOT_RIMBO           := dati_giornaliero(i).IMP_TOT_RIMBO ;
		dati_output(dati_output.last).IMP_TOT_RIMBO_NO_CONVS  := dati_giornaliero(i).IMP_TOT_RIMBO_NO_CONVS ;
		dati_output(dati_output.last).IMP_PRVNT_PASS          := dati_giornaliero(i).IMP_PRVNT_PASS ;
		dati_output(dati_output.last).IMP_PRVNT_FUT           := dati_giornaliero(i).IMP_PRVNT_FUT ;
		dati_output(dati_output.last).QTA_QUOTA_T1            := 0 ;
		dati_output(dati_output.last).QTA_QUOTA_T2            := dati_giornaliero(i).QTA_QUOTA_SALDO ;
		dati_output(dati_output.last).IMP_INT_VERS            := dati_giornaliero(i).IMP_INT_VERS ;
		dati_output(dati_output.last).IMP_SALDO_VERS          := dati_giornaliero(i).IMP_SALDO_VERS ;
		dati_output(dati_output.last).IMP_INT_INVEST          := dati_giornaliero(i).IMP_INT_INVEST ;
		dati_output(dati_output.last).IMP_SALDO_INVEST        := dati_giornaliero(i).IMP_SALDO_INVEST ;
		dati_output(dati_output.last).IMP_CONTVAL_T1          := 0 ;
		dati_output(dati_output.last).IMP_CONTVAL_T2          := dati_giornaliero(i).IMP_CONTVAL ;
		dati_output(dati_output.last).VAL_QUO_T2              :=

		CASE
		WHEN dati_giornaliero(i).QTA_QUOTA_SALDO<>0 THEN
			dati_giornaliero(i).IMP_CONTVAL/dati_giornaliero(i).QTA_QUOTA_SALDO

		ELSE
			NULL

		END;
		dati_output(dati_output.last).QTA_QUOTA_SALDO              := NULL ;
		dati_output(dati_output.last).PRC_REND_VPATR_VERS_ST       := dati_giornaliero(i).PRC_REND_VPATR_VERS_ST ;
		dati_output(dati_output.last).PRC_REND_VPATR_VERS_ANNUAL   := dati_giornaliero(i).PRC_REND_VPATR_VERS_ANNUAL ;
		dati_output(dati_output.last).PRC_REND_VPATR_INVEST_ST     := dati_giornaliero(i).PRC_REND_VPATR_INVEST_ST ;
		dati_output(dati_output.last).PRC_REND_VPATR_INVEST_ANNUAL := dati_giornaliero(i).PRC_REND_VPATR_INVEST_ANNUAL ;
		dati_output(dati_output.last).PRC_REND_MWRR_VERS_ST        := dati_giornaliero(i).PRC_REND_MWRR_VERS_ST ;
		dati_output(dati_output.last).PRC_REND_MWRR_VERS_ANNUAL    := dati_giornaliero(i).PRC_REND_MWRR_VERS_ANNUAL ;
		dati_output(dati_output.last).PRC_REND_MWRR_INVEST_ST      := dati_giornaliero(i).PRC_REND_MWRR_INVEST_ST ;
		dati_output(dati_output.last).PRC_REND_MWRR_INVEST_ANNUAL  := dati_giornaliero(i).PRC_REND_MWRR_INVEST_ANNUAL ;
		dati_output(dati_output.last).IMP_PLUS_MINUS_VERS          := dati_giornaliero(i).IMP_PLUS_MINUS_VERS ;
		dati_output(dati_output.last).IMP_PLUS_MINUS_INVEST        := dati_giornaliero(i).IMP_PLUS_MINUS_INVEST ;
		--#92764 - Calcolo differenza tra versato e rimborsi per DashboardFondi
		dati_output(dati_output.last).DIFF_VERS_RIMB 			   := dati_giornaliero(i).IMP_TOT_VERS - dati_giornaliero(i).IMP_TOT_RIMBO;
		SOGG_OUTELAB.calc_flg_stato(dati_output(dati_output.last), dt_rif);

	END LOOP;
	aggiustaFlagStatoIrlanda(dati_output);
	calc_fondi_padre(dati_output, dt_rif, flgNascZero);
	popola_dati_accessori(dati_output);
	popola_dati_contratto(dati_output);
	popola_gruppo_ognimese(dati_output);
	cRec := cRec multiset

	UNION ALL dati_output;

END;


PROCEDURE agg_tab_totali_gg(
		bulk_limit IN NUMBER,
		esito OUT CHAR,
		num_rec_elab OUT INTEGER,
		num_rec_scart OUT INTEGER,
		num_rec_warn OUT INTEGER)
IS
	chiavi_del_giorno SOGG_OUTELAB.TB_KEY_CONTR;
	ultimoRun DATE;
	rec_eventi_fondi_bmed eventi_fondi_bmed%rowtype;
	in_cTotaliFondiBMED tb_myIndex;
	cTotaliFondiBMED SOGG_OUTELAB.TB_TOTALS_CONTR;
	contr_attuali tb_contr;
type tb_eventi_fondi_bmed
IS
	TABLE OF eventi_fondi_bmed%rowtype;
	eventi_grezzi tb_eventi_fondi_bmed;
	myerrmsg VARCHAR(255);
BEGIN
	BEGIN

		INSERT
		INTO SOGG_OUTELAB.DETT_LOG_MOVI_CONTR
			(
				PROD_C,
				CONTR_N,
				DAT_RIFE,
				COD_TIPO_RECORD,
				GSTD_X_USER,
				GSTD_D_INS_RECORD,
				GSTD_F_ESIST
			)
		SELECT prod_c,
			contr_n,
			TRUNC(sysdate),
			'FND-BMED',
			'REC_PROV',
			sysdate,
			'S'
		FROM
			(SELECT COALESCE (A1.PROD_C, A2.PROD_C) PROD_C,
				COALESCE (A1.CONTR_N, A2.CONTR_N) CONTR_N,
				COALESCE (A1.VAL,0) VAL_SAL,
				COALESCE (A2.VAL,0) VAL_EVE
			FROM
				(SELECT PROD_C,
					CONTR_N,
					IMP_PRVNT_FUT VAL
				FROM SOGG_OUTELAB.SALDO_TOT_FONDO_BMED
				WHERE IMP_PRVNT_FUT>0
				AND GSTD_F_ESIST   ='S'
				) A1
			LEFT OUTER JOIN
				(SELECT PROD_C,
					CONTR_N,
					SUM(EVENTIF_I_LORDO) VAL
				FROM CLL.EVENTIF
				WHERE GSTD_F_ESIST     ='S'
				AND EVENTIF_D_VLT_INC  >TRUNC(SYSDATE)
				AND EVENTIF_X_TIP_OPERZ='PR-'
				GROUP BY PROD_C,
					CONTR_N
				) A2
			ON
				(
					A1.PROD_C    =A2.PROD_C
				AND A1.CONTR_N=A2.CONTR_N
				)
			) B1
		WHERE VAL_SAL-VAL_EVE<>0 ;
		COMMIT;

/*
		INSERT
		INTO SOGG_OUTELAB.DETT_LOG_MOVI_CONTR
			(
				DAT_RIFE,
				COD_TIPO_RECORD,
				PROD_C,
				CONTR_N
			)
		SELECT TRUNC(SYSDATE) DAT_RIFE,
			'FND-BMED' COD_TIPO_RECORD,
			S.PROD_C,
			S.CONTR_N
		FROM SOGG_OUTELAB.SALDO_TOT_FONDO_BMED S
		INNER JOIN CLL.CONTR CN
		ON
			(
				S.PROD_C     = CN.PROD_C
			AND S.CONTR_N = CN.CONTR_N
			)
		WHERE S.DAT_CHIU_CONTR IS NULL
			--AND CN.CONTR_D_ESTI > to_date('17530101','YYYYMMDD') and TRUNC(CN.CONTR_D_ESTI) < TRUNC(sysdate)
		AND CN. CONTR_C_STATO = '7' ;
		COMMIT;
*/

	EXCEPTION

	WHEN OTHERS THEN
		myerrmsg := SQLERRM;

		INSERT
		INTO SOGG_OUTELAB.DETT_LOG_ERR
			(
				PROD_C,
				CONTR_N,
				COD_ERR_ELAB,
				DES_ERR_ELAB,
				GSTD_D_ULT_MODF_RECORD,
				GSTD_D_INS_RECORD,
				GSTD_X_TIP_MODF,
				GSTD_X_USER,
				GSTD_M_NOM_ULT_MODF,
				GSTD_F_ESIST
			)
			VALUES
			(
				' ',
				' ',
				81684,
				SUBSTR('err_rec_prv '
				|| myerrmsg, 1, 255),
				sysdate,
				sysdate,
				'I',
				'batch',
				'batch',
				'S'
			);
		COMMIT;

	END;
	cTotaliFondiBMED := SOGG_OUTELAB.TB_TOTALS_CONTR();
	ultimoRun        := fetchDataUltimoRun('FND-BMED');
	sogg_outelab.ins_punt_elab('FND-BMED');
	LOOP
		cTotaliFondiBMED := SOGG_OUTELAB.TB_TOTALS_CONTR();
		OPEN c_chiavi_del_giorno(bulk_limit,'FND-BMED');

		FETCH c_chiavi_del_giorno bulk collect INTO chiavi_del_giorno;

		CLOSE c_chiavi_del_giorno;

		IF (chiavi_del_giorno.count=0) THEN
			EXIT;

		END IF;
		cTotaliFondiBMED.delete;
		contr_attuali.delete;

		UPDATE SOGG_OUTELAB.DETT_LOG_MOVI_CONTR LC
		SET LC.gstd_f_esist       ='N' ,
			LC.GSTD_M_NOM_ULT_MODF   ='ESEGUITA',
			LC.GSTD_D_ULT_MODF_RECORD=sysdate
		WHERE LC.Dat_Rife        <= TRUNC(SYSDATE)
		AND LC.COD_TIPO_RECORD    ='FND-BMED'
		AND
			(
				LC.PROD_C, LC.CONTR_N
			)
			IN
			(SELECT T1.PROD_C, T1.CONTR_N FROM (TABLE(chiavi_del_giorno)) T1
			) ;

		legg_contr_info(chiavi_del_giorno, contr_attuali);
		num_rec_elab := 0;
		OPEN eventi_fondi_bmed(chiavi_del_giorno);
		LOOP

			FETCH eventi_fondi_bmed BULK COLLECT
			INTO eventi_grezzi LIMIT bulk_limit;
			EXIT
		WHEN eventi_grezzi.COUNT = 0;
			num_rec_elab           := num_rec_elab + eventi_grezzi.COUNT;

			FOR indiceMovimenti IN 1 .. eventi_grezzi.COUNT
			LOOP
				calc_totali_fondi_bmed( in_cTotaliFondiBMED, cTotaliFondiBMED, eventi_grezzi(indiceMovimenti), contr_attuali );

			END LOOP;

		END LOOP;
		CLOSE eventi_fondi_bmed;
		agg_tab_totali(cTotaliFondiBMED);
		COMMIT;

	END LOOP;
	esito := 'S';

END;

PROCEDURE agg_tab_totali(
		totali IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_CONTR )
IS
BEGIN
	merge INTO SOGG_OUTELAB.SALDO_TOT_FONDO_BMED ds USING
	(SELECT * FROM TABLE(totali)
	) ap ON
	(
		ap.prod_c=ds.prod_c AND ap.contr_n=ds.contr_n
	)
WHEN MATCHED THEN
	UPDATE
	SET ds.GSTD_D_ULT_MODF_RECORD =SYSDATE,
		ds."DAT_APER_CONTR"          = ap."DAT_APER_CONTR" ,
		ds."COD_GEOG_FONDO"          = ap."COD_GEOG_FONDO" ,
		ds."FLG_PIR"                 = ap."FLG_PIR" ,
		ds."DAT_CHIU_CONTR"          = ap."DAT_CHIU_CONTR" ,
		ds."DAT_PRIMO_MOVI"          = ap."DAT_PRIMO_MOVI" ,
		ds."DAT_ULT_MOVI"            = ap."DAT_ULT_MOVI" ,
		ds."IMP_TOT_VERS"            = ap."IMP_TOT_VERS" ,
		ds."IMP_TOT_VERS_NO_CONVS"   = ap."IMP_TOT_VERS_NO_CONVS" ,
		ds."IMP_TOT_INVEST"          = ap."IMP_TOT_INVEST" ,
		ds."IMP_TOT_INVEST_NO_CONVS" = ap."IMP_TOT_INVEST_NO_CONVS" ,
		ds."IMP_TOT_RIMBO"           = ap."IMP_TOT_RIMBO" ,
		ds."IMP_TOT_RIMBO_NO_CONVS"  = ap."IMP_TOT_RIMBO_NO_CONVS" ,
		ds."IMP_PRVNT_PASS"          = ap."IMP_PRVNT_PASS" ,
		ds."IMP_PRVNT_FUT"           = ap."IMP_PRVNT_FUT" ,
		ds."QTA_QUOTA_T2"            = ap."QTA_QUOTA_T2" ,
		ds."IMP_INT_VERS"            = ap."IMP_INT_VERS" ,
		ds."IMP_SALDO_VERS"          = ap."IMP_SALDO_VERS" ,
		--  ds."IMP_INT_VERS_NC" = ap."IMP_INT_VERS_NC" ,
		--  ds."IMP_SALDO_VERS_NC" = ap."IMP_SALDO_VERS_NC" ,
		ds."IMP_INT_INVEST"   = ap."IMP_INT_INVEST" ,
		ds."IMP_SALDO_INVEST" = ap."IMP_SALDO_INVEST"
		--  ds."IMP_INT_INVEST_NC" = ap."IMP_INT_INVEST_NC" ,
		--  ds."IMP_SALDO_INVEST_NC" = ap."IMP_SALDO_INVEST_NC"
	WHERE ap.DAT_APER_CONTR IS NOT NULL
	AND ap.DAT_PRIMO_MOVI   IS NOT NULL WHEN NOT MATCHED THEN
	INSERT
		(
			"PROD_C" ,
			"PROD_C_PADRE" ,
			"CONTR_N" ,
			"COD_GEOG_FONDO" ,
			GSTD_F_ESIST ,
			GSTD_X_TIP_MODF ,
			GSTD_D_ULT_MODF_RECORD ,
			GSTD_X_USER ,
			GSTD_M_NOM_ULT_MODF ,
			"DAT_APER_CONTR" ,
			"DAT_CHIU_CONTR" ,
			"DAT_PRIMO_MOVI" ,
			"DAT_ULT_MOVI" ,
			"IMP_TOT_VERS" ,
			"IMP_TOT_VERS_NO_CONVS" ,
			"IMP_TOT_INVEST" ,
			"IMP_TOT_INVEST_NO_CONVS" ,
			"IMP_TOT_RIMBO" ,
			"IMP_TOT_RIMBO_NO_CONVS" ,
			"IMP_PRVNT_PASS" ,
			"IMP_PRVNT_FUT" ,
			"QTA_QUOTA_T2" ,
			"IMP_INT_VERS" ,
			"IMP_SALDO_VERS" ,
			"IMP_INT_INVEST" ,
			"IMP_SALDO_INVEST",
			"FLG_PIR"
		)
		VALUES
		(
			ap."PROD_C" ,
			ap."PROD_C_PADRE" ,
			ap."CONTR_N" ,
			ap."COD_GEOG_FONDO" ,
			'S' ,
			'I' ,
			SYSDATE ,
			'MOT-BMED' ,
			'MOT-BMED' ,
			ap."DAT_APER_CONTR" ,
			ap."DAT_CHIU_CONTR" ,
			ap."DAT_PRIMO_MOVI" ,
			ap."DAT_ULT_MOVI" ,
			ap."IMP_TOT_VERS" ,
			ap."IMP_TOT_VERS_NO_CONVS" ,
			ap."IMP_TOT_INVEST" ,
			ap."IMP_TOT_INVEST_NO_CONVS" ,
			ap."IMP_TOT_RIMBO" ,
			ap."IMP_TOT_RIMBO_NO_CONVS" ,
			ap."IMP_PRVNT_PASS" ,
			ap."IMP_PRVNT_FUT" ,
			ap."QTA_QUOTA_T2" ,
			ap."IMP_INT_VERS" ,
			ap."IMP_SALDO_VERS" ,
			ap."IMP_INT_INVEST" ,
			ap."IMP_SALDO_INVEST",
			ap."FLG_PIR"
		)
	WHERE ap.DAT_APER_CONTR IS NOT NULL
	AND ap.DAT_PRIMO_MOVI   IS NOT NULL ;

END;


PROCEDURE legg_contr_info(
		chiavi        IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
		contr_attuali IN OUT nocopy tb_contr )
IS
type tb_contr
IS
	TABLE OF CLL.CONTR%rowtype;
	contrattiLetti tb_contr;
	contractKey VARCHAR(32);
BEGIN
	OPEN c_contr_massivo(chiavi);

	FETCH c_contr_massivo bulk collect INTO contrattiLetti;

	CLOSE c_contr_massivo;
	contr_attuali.delete;

	FOR indiceMovimenti IN 1 .. contrattiLetti.COUNT
	LOOP
		contractKey               := trim(contrattiLetti(indiceMovimenti).CONTR_N)||'-'||trim(contrattiLetti(indiceMovimenti).PROD_C);
		contr_attuali(contractKey):=contrattiLetti(indiceMovimenti);

	END LOOP;

END;

PROCEDURE calc_totali_fondi_bmed(
		in_cTotaliFondiBMED IN OUT nocopy tb_myIndex,
		cTotaliFondiBMED    IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_CONTR,
		cEventoBmed         IN OUT nocopy eventi_fondi_bmed%rowtype,
		contr_attuali       IN OUT nocopy tb_contr,
		p_t1          DATE DEFAULT NULL,
		p_t2          DATE DEFAULT NULL,
		codicePeriodo CHAR DEFAULT NULL )

IS

	contractKey         VARCHAR(32);
	totaliCruscottokey  VARCHAR(32);
	contractIndex       NUMBER;
	isProvento          BOOLEAN;
	isProventoIncassato BOOLEAN;
	isLoi               BOOLEAN;
	isPrenotata         BOOLEAN;
	isFusione           BOOLEAN;
	isStorno            BOOLEAN;
	isConversione       BOOLEAN;
	isNelPeriodo        BOOLEAN;
	segno               NUMBER;
	deltaT              NUMBER;
	t1                  DATE;
	t2                  DATE;
	maxProvMGF			NUMBER(4,2);
	minProvMIF			NUMBER(4,2);
	maxProvMIF_BON		NUMBER(4,2);
	maxProvMIF_ASS		NUMBER(4,2);
	--PROVENTI_NETTI		NUMBER;
	DATA_DISI_LOI 		DATE;

BEGIN

	maxProvMGF		:= TO_NUMBER('50', '9999.99');
	minProvMIF	 	:= TO_NUMBER('0.05', '9999.99');
	maxProvMIF_BON	:= TO_NUMBER('5', '9999.99');
	maxProvMIF_ASS	:= TO_NUMBER('5.80', '9999.99');

	tppag_storni  := COALESCE (tppag_storni, SOGG_OUTELAB.fetchChar3Config('FONDI_ITIR_STORNI'));
	tppag_convers := COALESCE (tppag_convers, SOGG_OUTELAB.fetchChar3Config('UMBR_FUND_CAUS_CONVERS'));

	IF datianagrafici.count = 0 THEN
		loadDatiAnagrafici;

	END IF;
	--dbms_output.put_line('MOVIMENTO');
	t1                 := COALESCE (p_t1, to_date('17530101','YYYYMMDD'));
	t2                 := COALESCE (p_t2, TRUNC(SYSDATE)-1);
	isNelPeriodo       :=false;
	isProventoIncassato:=false;

	IF cEventoBmed.DATA_RIFERIMENTO>t1 AND cEventoBmed.DATA_RIFERIMENTO<=t2 THEN

		isNelPeriodo := true;

	END IF;
	--dbms_output.put_line('calc_totali_fondi_bmed - begin');
	contractKey        := trim(cEventoBmed.CONTR_N)||'-'||trim(cEventoBmed.PROD_C);
	totaliCruscottokey := trim(cEventoBmed.CONTR_N)||'-'||trim(cEventoBmed.PROD_C)||'-'|| trim(codicePeriodo);

	IF in_cTotaliFondiBMED.EXISTS(totaliCruscottokey) THEN
		contractIndex := in_cTotaliFondiBMED(totaliCruscottokey);

	ELSE
		cTotaliFondiBMED.extend();
		in_cTotaliFondiBMED(totaliCruscottokey) := cTotaliFondiBMED.last;
		contractIndex                           := cTotaliFondiBMED.last;
		cTotaliFondiBMED(contractIndex)         := SOGG_OUTELAB.RT_TOTALS_CONTR(cEventoBmed.PROD_C, cEventoBmed.CONTR_N);

	--IF spostato per #94490
		IF datianagrafici.EXISTS(trim(cEventoBmed.prod_c)) THEN
			cTotaliFondiBMED(contractIndex).COD_GEOG_FONDO := datianagrafici(trim(cEventoBmed.prod_c)).stato;
			cTotaliFondiBMED(contractIndex).FLG_PIR        := datianagrafici(trim(cEventoBmed.prod_c)).FLG_PIR;

		ELSE
			cTotaliFondiBMED(contractIndex).COD_GEOG_FONDO := 'ERR';

		END IF;

		IF contr_attuali.EXISTS(contractKey) THEN
			cTotaliFondiBMED(contractIndex).DAT_APER_CONTR := contr_attuali(contractKey).CONTR_D_SSCRIZ;

			IF contr_attuali(contractKey).CONTR_D_ESTI      <> to_date('99991231','YYYYMMDD') AND contr_attuali(contractKey).CONTR_D_ESTI <> to_date('17530101','YYYYMMDD') THEN
				cTotaliFondiBMED(contractIndex).DAT_CHIU_CONTR := contr_attuali(contractKey).CONTR_D_ESTI;

			END IF;

		--modifica raggruppamento Fondi Italia #94490

            IF TRIM(cTotaliFondiBMED(contractIndex).COD_GEOG_FONDO) = 'ITA' THEN

                IF trim(contr_attuali(contractKey).CONTR_C_CONV) IS NOT NULL AND trim(contr_attuali(contractKey).PROD_C)<>trim(contr_attuali(contractKey).CONTR_C_CONV) THEN
                   cTotaliFondiBMED(contractIndex).PROD_C_PADRE := trim(contr_attuali(contractKey).CONTR_C_CONV);
                ELSE
                   cTotaliFondiBMED(contractIndex).PROD_C_PADRE := 'ITFP'||trim(contr_attuali(contractKey).PROD_C); --correttiva x visualizzazione fondi padre a FE
                END IF;

            ELSE
               cTotaliFondiBMED(contractIndex).PROD_C_PADRE :=

                CASE
                WHEN trim(contr_attuali(contractKey).CONTR_C_SERV) IS NOT NULL AND trim(contr_attuali(contractKey).PROD_C)<>trim(contr_attuali(contractKey).CONTR_C_SERV) THEN
                    trim(contr_attuali(contractKey).CONTR_C_SERV)
                WHEN trim(contr_attuali(contractKey).CONTR_C_CONV) IS NOT NULL AND trim(contr_attuali(contractKey).PROD_C)<>trim(contr_attuali(contractKey).CONTR_C_CONV) THEN
                    trim(contr_attuali(contractKey).CONTR_C_CONV)
                END;
            END IF;

			-- ref 65056

			IF cTotaliFondiBMED(contractIndex).PROD_C_PADRE IS NULL THEN
				cTotaliFondiBMED(contractIndex).PROD_C_PADRE   := 'ITFP';

			END IF;

			cTotaliFondiBMED(contractIndex).DATA_DISINVESTIMENTO := cEventoBmed.DATA_DISINVESTIMENTO;
			cTotaliFondiBMED(contractIndex).QUANTITA := cEventoBmed.QUANTITA;

		END IF;

	END IF;

	-- rfc #73046 cruscotti setters

	IF p_t1 IS NOT NULL AND p_t2 IS NOT NULL AND codicePeriodo IS NOT NULL THEN
		cTotaliFondiBMED(contractIndex).COD_PERIOD_ELAB := codicePeriodo;
		cTotaliFondiBMED(contractIndex).DAT_INIZ_PERIOD := trunc(p_t1);
		cTotaliFondiBMED(contractIndex).DAT_FINE_PERIOD := trunc(p_t2);

	END IF;

	isProvento := cEventoBmed.PROVENTI > 0;


	IF isProvento AND isNelPeriodo THEN

	if tpPagam_ASS_MGF.count = 0 and tpPagam_BON_MGF.count = 0 and tpPagam_ASS_MIF.count = 0 and tpPagam_BON_MIF.count = 0  then

      		add_caus_tipoPagam();

   	end if;


		IF (cEventoBmed.DATA_INC_PROV >= t2) THEN

		--if this code is used for the periodic service differ the check from future events (that are always to add) and the post-t2 events. now they are the same to trunc(Curent_date)

			cTotaliFondiBMED(contractIndex).IMP_PRVNT_FUT := cTotaliFondiBMED(contractIndex).IMP_PRVNT_FUT + cEventoBmed.PROVENTI;

		--elsif cEventoBmed.COD_TIPO_PAGAM_PRVNT         IS NULL OR cEventoBmed.COD_TIPO_PAGAM_PRVNT     IN ('02', '03') THEN

		elsif (cEventoBmed.COD_TIPO_PAGAM_PRVNT IS NULL OR
		    cEventoBmed.COD_TIPO_PAGAM_PRVNT MEMBER OF tpPagam_BON_MIF OR cEventoBmed.COD_TIPO_PAGAM_PRVNT MEMBER OF tpPagam_ASS_MIF  OR
			cEventoBmed.COD_TIPO_PAGAM_PRVNT MEMBER OF tpPagam_BON_MGF OR cEventoBmed.COD_TIPO_PAGAM_PRVNT MEMBER OF tpPagam_ASS_MGF) THEN

		--#92682 -- non vanno considerati i proventi che a causa dell'importo esiguo non vengono distribuiti ma reinvestiti nel fondo:
		-- Fondi MGF <=> COD_GEOG_FONDO='ITA';  mentre Fondi MIF <=> COD_GEOG_FONDO='IRL'
		-- Dalla data di delibera stacco proventi e fino alla data del pagamento per i fondi comuni a distribuzione (MIF/MGF) il comportamento dovra' rimanere come da AS-IS

		--PROVENTI_NETTI := cEventoBmed.EVENTIF_I_LORDO - cEventoBmed.RIT_FISC;

		--#EVO-122010 - Proventi Fondi al lordo della fiscalita	--> considera PROVENTI in luogo di PROVENTI_NETTI
		-- Dalla data pagamento quindi non dovranno pi¿¿ essere visibili nella cella proventi ma avranno effetto solo sul ctv
		-- fondi MGF -->	proventi < 50¿¿¿
		-- fondi MIF -->	proventi < 0,05¿¿¿
        -- fondi MIF -->    Se la distribuzione avviene tramite assegno saranno reinvestiti i proventi di importo netto totale compreso tra 0,05¿¿¿ e 5,80¿¿¿
		-- fondi MIF -->    Se la distribuzione avviene tramite bonifico saranno reinvestiti i proventi di importo totale compreso tra 0,05¿¿¿ e 5¿¿¿
		if  (cTotaliFondiBMED(contractIndex).COD_GEOG_FONDO = 'ITA' and cEventoBmed.PROVENTI <= maxProvMGF)
		    OR
			 (cTotaliFondiBMED(contractIndex).COD_GEOG_FONDO = 'IRL' AND
			 ((cEventoBmed.PROVENTI < minProvMIF) OR (cEventoBmed.COD_TIPO_PAGAM_PRVNT MEMBER OF tpPagam_ASS_MIF and cEventoBmed.PROVENTI > minProvMIF and cEventoBmed.PROVENTI <= maxProvMIF_ASS)  --assegno
			  OR (cEventoBmed.COD_TIPO_PAGAM_PRVNT MEMBER OF tpPagam_BON_MIF and cEventoBmed.PROVENTI > minProvMIF and cEventoBmed.PROVENTI <= maxProvMIF_BON)) --bon
			  )
			then NULL;

		 else

			cTotaliFondiBMED(contractIndex).IMP_PRVNT_PASS := cTotaliFondiBMED(contractIndex).IMP_PRVNT_PASS + cEventoBmed.PROVENTI;
			isProventoIncassato := true;

		end if;


	   END IF;

	elsif NOT isProvento THEN

		-- #98968 - CONTRIBUZIONE DEI MOVIMENTI LOI AI FINI DEL CALCOLO MWRR

		isLoi         := cEventoBmed.EVENTIF_X_TIP_OPERZ IN ('420', '421', '416', '415');

		--RFC#92846

		isPrenotata   := (cEventoBmed.EVENTIF_Q_QTE = 0 OR cEventoBmed.EVENTIF_I_CTRV_LORDO=0) AND NOT isLoi;
		isFusione     := cEventoBmed.EVENTIF_C_TIP_PG = 'BT';
		isStorno      := cEventoBmed.EVENTIF_C_TIP_PG MEMBER OF tppag_storni;
		isConversione := cEventoBmed.EVENTIF_C_TIP_PG MEMBER OF tppag_convers;

		IF cEventoBmed.EVENTIF_X_TIP_OPERZ MEMBER OF tpmovi_pos THEN
			segno := 1;

		elsif cEventoBmed.EVENTIF_X_TIP_OPERZ MEMBER OF tpmovi_neg THEN
			segno := -1;

		ELSE
			segno := 0;

		END IF;

		IF NOT isLoi AND NOT isPrenotata AND isNelPeriodo AND NOT isFusione THEN

			IF isStorno THEN
				cTotaliFondiBMED(contractIndex).IMP_TOT_VERS            := cTotaliFondiBMED(contractIndex).IMP_TOT_VERS            - cEventoBmed.RIMBORSI_VERSO_C_C;
				cTotaliFondiBMED(contractIndex).IMP_TOT_VERS_NO_CONVS   := cTotaliFondiBMED(contractIndex).IMP_TOT_VERS_NO_CONVS   - cEventoBmed.RIMBORSI_VERSO_C_C;
				cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST          := cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST          - cEventoBmed.RIMBORSI_VERSO_C_C;
				cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST_NO_CONVS := cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST_NO_CONVS - cEventoBmed.RIMBORSI_VERSO_C_C;

			ELSE
				cTotaliFondiBMED(contractIndex).IMP_TOT_VERS          := cTotaliFondiBMED(contractIndex).IMP_TOT_VERS          + cEventoBmed.CAPITALE_VERSATO;
				cTotaliFondiBMED(contractIndex).IMP_TOT_VERS          := cTotaliFondiBMED(contractIndex).IMP_TOT_VERS          + cEventoBmed.CAPITALE_DA_REINVESTIMENTO;
				cTotaliFondiBMED(contractIndex).IMP_TOT_VERS_NO_CONVS := cTotaliFondiBMED(contractIndex).IMP_TOT_VERS_NO_CONVS + cEventoBmed.CAPITALE_VERSATO;
				cTotaliFondiBMED(contractIndex).IMP_TOT_VERS_NO_CONVS := cTotaliFondiBMED(contractIndex).IMP_TOT_VERS_NO_CONVS +

				CASE
				WHEN NOT isConversione THEN
					cEventoBmed.CAPITALE_DA_REINVESTIMENTO
				ELSE
					0
				END;
				cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST          := cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST          + cEventoBmed.CAPITALE_INVESTITO;
				cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST_NO_CONVS := cTotaliFondiBMED(contractIndex).IMP_TOT_INVEST_NO_CONVS +

				CASE
				WHEN NOT isConversione THEN
					cEventoBmed.CAPITALE_INVESTITO
				ELSE
					0
				END;
				cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO          := cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO          + cEventoBmed.RIMBORSI_VERSO_C_C;
				cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO          := cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO          + cEventoBmed.RIMBORSI_PER_REINVESTIMENTO;
				cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO_NO_CONVS := cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO_NO_CONVS + cEventoBmed.RIMBORSI_VERSO_C_C;
				cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO_NO_CONVS := cTotaliFondiBMED(contractIndex).IMP_TOT_RIMBO_NO_CONVS +

				CASE
				WHEN NOT isConversione THEN
					cEventoBmed.RIMBORSI_PER_REINVESTIMENTO
				ELSE
					0
				END;

			END IF;
			--#92764 - Calcolo differenza tra versato e rimborsi per DashboardFondi
			cTotaliFondiBMED(contractIndex).DIFF_VERS_RIMB := cTotaliFondiBMED(contractIndex).DIFF_VERS_RIMB + COALESCE(cEventoBmed.CAPITALE_VERSATO, 0) + COALESCE(cEventoBmed.CAPITALE_DA_REINVESTIMENTO, 0) - COALESCE(cEventoBmed.RIMBORSI_VERSO_C_C, 0) - COALESCE(cEventoBmed.RIMBORSI_PER_REINVESTIMENTO, 0);

		END IF;

	END IF;

	IF isProventoIncassato OR
		(
			NOT isProvento AND NOT isLoi AND NOT isPrenotata AND isNelPeriodo AND NOT isFusione
		)
		THEN

		IF cTotaliFondiBMED(contractIndex).DAT_PRIMO_MOVI IS NULL THEN
			cTotaliFondiBMED(contractIndex).DAT_PRIMO_MOVI   := cEventoBmed.DATA_RIFERIMENTO;

		ELSE
			deltaT                                         := cEventoBmed.DATA_RIFERIMENTO                   -cTotaliFondiBMED(contractIndex).DAT_ULT_MOVI;
			cTotaliFondiBMED(contractIndex).IMP_INT_VERS   := cTotaliFondiBMED(contractIndex).IMP_INT_VERS   + cTotaliFondiBMED(contractIndex).IMP_SALDO_VERS * deltaT;
			cTotaliFondiBMED(contractIndex).IMP_INT_INVEST := cTotaliFondiBMED(contractIndex).IMP_INT_INVEST + cTotaliFondiBMED(contractIndex).IMP_SALDO_INVEST * deltaT;

		END IF;
		cTotaliFondiBMED(contractIndex).DAT_ULT_MOVI     := cEventoBmed.DATA_RIFERIMENTO;
		cTotaliFondiBMED(contractIndex).IMP_SALDO_VERS   := cTotaliFondiBMED(contractIndex).IMP_SALDO_VERS   - cEventoBmed.PROVENTI ;
		cTotaliFondiBMED(contractIndex).IMP_SALDO_INVEST := cTotaliFondiBMED(contractIndex).IMP_SALDO_INVEST - cEventoBmed.PROVENTI ;

		--non e' necessaria la condizionalita'¿¿ sugli storni perche' abbattono il capitale iniziale

		cTotaliFondiBMED(contractIndex).IMP_SALDO_VERS   := cTotaliFondiBMED(contractIndex).IMP_SALDO_VERS   + cEventoBmed.CAPITALE_VERSATO + cEventoBmed.CAPITALE_DA_REINVESTIMENTO - cEventoBmed.RIMBORSI_PER_REINVESTIMENTO - cEventoBmed.RIMBORSI_VERSO_C_C ;
		cTotaliFondiBMED(contractIndex).IMP_SALDO_INVEST := cTotaliFondiBMED(contractIndex).IMP_SALDO_INVEST + cEventoBmed.CAPITALE_INVESTITO - cEventoBmed.RIMBORSI_PER_REINVESTIMENTO - cEventoBmed.RIMBORSI_VERSO_C_C ;

	END IF;

	-- #98968 - CONTRIBUZIONE DEI MOVIMENTI LOI AI FINI DEL CALCOLO MWRR

	-- Se la chiamata ¿¿ giornaliera controllo che t1 ¿¿ la data minima di default

	 DATA_DISI_LOI := loi_non_raggiunta(cEventoBmed.PROD_C, cEventoBmed.CONTR_N, cEventoBmed.DATA_RIFERIMENTO, t2);

	IF t1 = to_date('17530101','YYYYMMDD') THEN

		IF NOT isLoi AND NOT isPrenotata AND cEventoBmed.DATA_RIFERIMENTO <= t2 THEN

			cTotaliFondiBMED(contractIndex).QTA_QUOTA_T2 := cTotaliFondiBMED(contractIndex).QTA_QUOTA_T2 + cEventoBmed.EVENTIF_Q_QTE * segno;

		END IF;

		IF NOT isLoi AND NOT isPrenotata AND cEventoBmed.DATA_RIFERIMENTO <= t1 THEN

			cTotaliFondiBMED(contractIndex).QTA_QUOTA_T1 := cTotaliFondiBMED(contractIndex).QTA_QUOTA_T1 + cEventoBmed.EVENTIF_Q_QTE * segno;

		END IF;

	ELSE

			IF NOT isPrenotata AND cEventoBmed.DATA_RIFERIMENTO <= t2 THEN

			  cTotaliFondiBMED(contractIndex).QTA_QUOTA_T2 := cTotaliFondiBMED(contractIndex).QTA_QUOTA_T2 + cEventoBmed.EVENTIF_Q_QTE * segno;

			END IF;

			IF NOT isLoi AND NOT isPrenotata AND cEventoBmed.DATA_RIFERIMENTO <= t1 THEN

			  cTotaliFondiBMED(contractIndex).QTA_QUOTA_T1 := cTotaliFondiBMED(contractIndex).QTA_QUOTA_T1 + cEventoBmed.EVENTIF_Q_QTE * segno;

			END IF;

			IF (isLoi AND NVL(DATA_DISI_LOI, to_date('31/12/9999','DD/MM/YYYY')) > t2) AND NOT isPrenotata AND cEventoBmed.DATA_RIFERIMENTO <= t1 THEN

				cTotaliFondiBMED(contractIndex).QTA_QUOTA_T1 := cTotaliFondiBMED(contractIndex).QTA_QUOTA_T1 + cEventoBmed.EVENTIF_Q_QTE * segno;

			END IF;

	END IF;

END;

PROCEDURE add_caus_TipoPagam
IS
  tpTipoPagam C_Causali_Pagam%rowtype;

BEGIN
	OPEN C_Causali_Pagam;
	LOOP
		FETCH C_Causali_Pagam INTO tpTipoPagam;
		EXIT
	WHEN C_Causali_Pagam%NOTFOUND;


      if ( tpTipoPagam.tipo='CAUS_P_MIF') then

         if(tpTipoPagam.descrizione=k_pagam_assegno) then
           tpPagam_ASS_MIF.extend(1);
           tpPagam_ASS_MIF(tpPagam_ASS_MIF.last) := tpTipoPagam.chiave;
        end if;

          if (tpTipoPagam.descrizione=k_pagam_bonifico ) then
            tpPagam_BON_MIF.extend(1);
            tpPagam_BON_MIF(tpPagam_BON_MIF.last) := tpTipoPagam.chiave;
         end if;

    end if;

      if (  tpTipoPagam.tipo='CAUS_P_MGF') then

        if (tpTipoPagam.descrizione=k_pagam_assegno ) then
         tpPagam_ASS_MGF.extend(1);
         tpPagam_ASS_MGF(tpPagam_ASS_MGF.last) := tpTipoPagam.chiave;
        end if;

       if (tpTipoPagam.descrizione=k_pagam_bonifico ) then
        tpPagam_BON_MGF.extend(1);
        tpPagam_BON_MGF(tpPagam_BON_MGF.last) := tpTipoPagam.chiave;
       end if;

    end if;

  END LOOP;

 CLOSE C_Causali_Pagam;

END add_caus_TipoPagam;


FUNCTION loi_non_raggiunta (in_prod_c CHAR, in_contr_n CHAR, data_rife DATE, t2 DATE) RETURN DATE IS

data_loi date;

BEGIN

	OPEN c_eventif_loi(in_prod_c, in_contr_n, data_rife, t2);

		LOOP

			FETCH c_eventif_loi into data_loi ;
			exit when c_eventif_loi%NOTFOUND;
		END LOOP;

	CLOSE c_eventif_loi;

	RETURN data_loi;

END;

PROCEDURE calc_redd_aperti
	(
		bulk_limit IN NUMBER,
		esito OUT CHAR,
		num_rec_elab OUT INTEGER,
		num_rec_scart OUT INTEGER,
		num_rec_warn OUT INTEGER
	)
IS
	dati_base_fnd_bmed c_load_aper_fnd_bmed%rowtype;
	recordTarget SOGG_OUTELAB.TB_OUTPUT_ONLINE;
	commitFreq  NUMBER;
	commitCount NUMBER;
BEGIN
	--dbms_output.put_line('test');
	recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	commitFreq   :=10000;
	commitCount  :=1;
	OPEN c_load_aper_fnd_bmed;
	LOOP

		FETCH c_load_aper_fnd_bmed INTO dati_base_fnd_bmed;
		EXIT
	WHEN c_load_aper_fnd_bmed%NOTFOUND;
		recordTarget.extend(1);
		recordTarget(recordTarget.last)                         := SOGG_OUTELAB.RT_OUTPUT_ONLINE('FND');
		recordTarget(recordTarget.last).PROD_C                  := dati_base_fnd_bmed.PROD_C ;
		recordTarget(recordTarget.last).PROD_C_PADRE            := dati_base_fnd_bmed.PROD_C_PADRE ;
		recordTarget(recordTarget.last).CONTR_N                 := dati_base_fnd_bmed.CONTR_N ;
		recordTarget(recordTarget.last).COD_GEOG_FONDO          := dati_base_fnd_bmed.COD_GEOG_FONDO ;
		recordTarget(recordTarget.last).FLG_PIR                 := dati_base_fnd_bmed.FLG_PIR ;
		--#90383 - Calcolo rendimento MWRR del singolo comparto a zero nel giornaliero
		recordTarget(recordTarget.last).DAT_RIFE                :=

        CASE WHEN dati_base_fnd_bmed.QTA_QUOTA_T2 > 0 THEN
            dati_base_fnd_bmed.DAT_RIFE
        ELSE CASE WHEN dati_base_fnd_bmed.QUANTITA = 0 AND dati_base_fnd_bmed.QTA_QUOTA_SALDO = 0 THEN
					dati_base_fnd_bmed.DATA_DISINVESTIMENTO
			 ELSE
					dati_base_fnd_bmed.DAT_RIFE
			 END
        END;

		recordTarget(recordTarget.last).DAT_APER_CONTR          := dati_base_fnd_bmed.DAT_APER_CONTR ;
		recordTarget(recordTarget.last).DAT_CHIU_CONTR          := dati_base_fnd_bmed.DAT_CHIU_CONTR ;
		recordTarget(recordTarget.last).DAT_PRIMO_MOVI          := dati_base_fnd_bmed.DAT_PRIMO_MOVI ;
		recordTarget(recordTarget.last).DAT_ULT_MOVI            := dati_base_fnd_bmed.DAT_ULT_MOVI ;
		recordTarget(recordTarget.last).IMP_TOT_VERS            := dati_base_fnd_bmed.IMP_TOT_VERS ;
		recordTarget(recordTarget.last).IMP_TOT_VERS_NO_CONVS   := dati_base_fnd_bmed.IMP_TOT_VERS_NO_CONVS ;
		recordTarget(recordTarget.last).IMP_TOT_INVEST          := dati_base_fnd_bmed.IMP_TOT_INVEST ;
		recordTarget(recordTarget.last).IMP_TOT_INVEST_NO_CONVS := dati_base_fnd_bmed.IMP_TOT_INVEST_NO_CONVS ;
		recordTarget(recordTarget.last).IMP_TOT_RIMBO           := dati_base_fnd_bmed.IMP_TOT_RIMBO ;
		recordTarget(recordTarget.last).IMP_TOT_RIMBO_NO_CONVS  := dati_base_fnd_bmed.IMP_TOT_RIMBO_NO_CONVS ;
		recordTarget(recordTarget.last).IMP_PRVNT_PASS          := dati_base_fnd_bmed.IMP_PRVNT_PASS ;
		recordTarget(recordTarget.last).IMP_PRVNT_FUT           := dati_base_fnd_bmed.IMP_PRVNT_FUT ;
		recordTarget(recordTarget.last).QTA_QUOTA_T2            := dati_base_fnd_bmed.QTA_QUOTA_T2 ;
		recordTarget(recordTarget.last).IMP_SALDO_VERS          := dati_base_fnd_bmed.IMP_SALDO_VERS ;
		recordTarget(recordTarget.last).IMP_SALDO_INVEST        := dati_base_fnd_bmed.IMP_SALDO_INVEST ;
		recordTarget(recordTarget.last).IMP_CONTVAL_T1          := 0 ;
		recordTarget(recordTarget.last).IMP_CONTVAL_T2          := dati_base_fnd_bmed.IMP_CONTVAL ;
		recordTarget(recordTarget.last).QTA_QUOTA_SALDO         := dati_base_fnd_bmed.QTA_QUOTA_SALDO ;
		recordTarget(recordTarget.last).IMP_INT_VERS            := dati_base_fnd_bmed.IMP_INT_VERS ;
		recordTarget(recordTarget.last).IMP_INT_INVEST          := dati_base_fnd_bmed.IMP_INT_INVEST ;
		--SOGG_OUTELAB.calc_flg_stato(recordTarget(recordTarget.last));
		--commentata perche' fase batch e non vengono salvati i flag di stato
		SOGG_OUTELAB.calc_indici_redd(recordTarget(recordTarget.last));

		--#90383 - reinserisco la dat_rife (T2) in modo che il comparto disinvestito venga estratto nel giornaliero
		recordTarget(recordTarget.last).DAT_RIFE                := dati_base_fnd_bmed.DAT_RIFE ;

		IF mod(commitCount,commitFreq)=0 THEN
			ins_dati_online(recordTarget,'A');
			recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();

		END IF;
		commitCount := commitCount + 1;

	END LOOP;
	CLOSE c_load_aper_fnd_bmed;
	ins_dati_online(recordTarget,'A');

END;

PROCEDURE ins_dati_online
	(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
		dest IN CHAR
	)
IS
BEGIN

	INSERT
	INTO SOGG_OUTELAB.DETT_LOG_ERR
		(
			PROD_C,
			CONTR_N,
			COD_ERR_ELAB,
			DES_ERR_ELAB,
			GSTD_D_INS_RECORD,
			GSTD_X_TIP_MODF,
			GSTD_X_USER,
			GSTD_M_NOM_ULT_MODF
		)
	SELECT ap.prod_c,
		ap.contr_n,
		0,
		'motore fondi bmed: riga duplicata in inserimento redd aperti',
		SYSDATE,
		'I',
		'BATCH',
		'BATCH'
	FROM
		(SELECT prod_c,
			contr_n
		FROM TABLE (cRec)
		GROUP BY prod_c,
			contr_n
		HAVING COUNT(1)>1
		) ap;

	IF dest='A' THEN
		merge INTO SOGG_OUTELAB.DETT_REDDTA_CONTR_BMED ds USING
		(SELECT *
		FROM TABLE (cRec) a1
		WHERE
			(
				a1.prod_c, a1.contr_n
			)
			NOT IN
			(SELECT prod_c,
				contr_n
			FROM TABLE (cRec)
			GROUP BY prod_c,
				contr_n
			HAVING COUNT(1)>1
			)
		) ap ON
		(
			ds.prod_c=ap.prod_c AND ds.contr_n=ap.contr_n AND ds.DAT_RIFE=ap.DAT_RIFE
		)
	WHEN NOT MATCHED THEN
		INSERT
			(
				ds.PROD_C ,
				ds.PROD_C_PADRE ,
				ds.CONTR_N ,
				ds.COD_GEOG_FONDO ,
				ds.FLG_PIR,
				ds.DAT_RIFE ,
				ds.GSTD_F_ESIST ,
				ds.GSTD_X_TIP_MODF ,
				ds.GSTD_D_ULT_MODF_RECORD ,
				ds.GSTD_X_USER ,
				ds.GSTD_M_NOM_ULT_MODF ,
				ds.DAT_APER_CONTR ,
				ds.DAT_CHIU_CONTR ,
				ds.DAT_PRIMO_MOVI ,
				ds.DAT_ULT_MOVI ,
				ds.IMP_TOT_VERS ,
				ds.IMP_TOT_VERS_NO_CONVS ,
				ds.IMP_TOT_INVEST ,
				ds.IMP_TOT_INVEST_NO_CONVS ,
				ds.IMP_TOT_RIMBO ,
				ds.IMP_TOT_RIMBO_NO_CONVS ,
				ds.IMP_PRVNT_PASS ,
				ds.IMP_PRVNT_FUT ,
				ds.QTA_QUOTA_T2 ,
				ds.IMP_INT_VERS ,
				ds.IMP_SALDO_VERS ,
				ds.IMP_INT_INVEST ,
				ds.IMP_SALDO_INVEST ,
				ds.IMP_CONTVAL ,
				ds.QTA_QUOTA_SALDO ,
				ds.PRC_REND_VPATR_VERS_ST ,
				ds.PRC_REND_VPATR_VERS_ANNUAL ,
				ds.PRC_REND_VPATR_INVEST_ST ,
				ds.PRC_REND_VPATR_INVEST_ANNUAL ,
				ds.PRC_REND_MWRR_VERS_ST ,
				ds.PRC_REND_MWRR_VERS_ANNUAL ,
				ds.PRC_REND_MWRR_INVEST_ST ,
				ds.PRC_REND_MWRR_INVEST_ANNUAL,
				ds.IMP_PLUS_MINUS_VERS,
				ds.IMP_PLUS_MINUS_INVEST
			)
			VALUES
			(
				ap.PROD_C ,
				ap.PROD_C_PADRE ,
				ap.CONTR_N ,
				ap.COD_GEOG_FONDO ,
				ap.FLG_PIR,
				ap.DAT_RIFE ,
				'S' ,
				'I' ,
				SYSDATE ,
				'MOT-BMED' ,
				'MOT-BMED' ,
				ap.DAT_APER_CONTR ,
				ap.DAT_CHIU_CONTR ,
				ap.DAT_PRIMO_MOVI ,
				ap.DAT_ULT_MOVI ,
				ap.IMP_TOT_VERS ,
				ap.IMP_TOT_VERS_NO_CONVS ,
				ap.IMP_TOT_INVEST ,
				ap.IMP_TOT_INVEST_NO_CONVS ,
				ap.IMP_TOT_RIMBO ,
				ap.IMP_TOT_RIMBO_NO_CONVS ,
				ap.IMP_PRVNT_PASS ,
				ap.IMP_PRVNT_FUT ,
				ap.QTA_QUOTA_T2 ,
				ap.IMP_INT_VERS ,
				ap.IMP_SALDO_VERS ,
				ap.IMP_INT_INVEST ,
				ap.IMP_SALDO_INVEST ,
				ap.IMP_CONTVAL_T2 ,
				ap.QTA_QUOTA_SALDO ,
				ap.PRC_REND_VPATR_VERS_ST ,
				ap.PRC_REND_VPATR_VERS_ANNUAL ,
				ap.PRC_REND_VPATR_INVEST_ST ,
				ap.PRC_REND_VPATR_INVEST_ANNUAL ,
				ap.PRC_REND_MWRR_VERS_ST ,
				ap.PRC_REND_MWRR_VERS_ANNUAL ,
				ap.PRC_REND_MWRR_INVEST_ST ,
				ap.PRC_REND_MWRR_INVEST_ANNUAL,
				ap.IMP_PLUS_MINUS_VERS,
				ap.IMP_PLUS_MINUS_INVEST
			);

	elsif dest='C' THEN
		merge INTO SOGG_OUTELAB.DETT_REDDTA_CONTR_CHIU_BMED ds USING
		(SELECT *
			FROM TABLE (cRec) a1
			WHERE
				(
					a1.prod_c, a1.contr_n
				)
				NOT IN
				(SELECT prod_c,
					contr_n
				FROM TABLE (cRec)
				GROUP BY prod_c,
					contr_n
				HAVING COUNT(1)>1
				)
		)
		ap ON
		(
			ds.prod_c=ap.prod_c AND ds.contr_n=ap.contr_n
		)
	WHEN MATCHED THEN
		UPDATE
		SET ds."PROD_C_PADRE"             =ap."PROD_C_PADRE",
			ds."COD_GEOG_FONDO"              =ap."COD_GEOG_FONDO",
			ds."FLG_PIR"                     =ap."FLG_PIR",
			ds."DAT_RIFE"                    =ap."DAT_RIFE",
			ds."GSTD_F_ESIST"                ='S',
			ds."GSTD_X_TIP_MODF"             ='U',
			ds."GSTD_D_ULT_MODF_RECORD"      =SYSDATE,
			ds."GSTD_X_USER"                 ='MOT-BMED',
			ds."GSTD_M_NOM_ULT_MODF"         ='MOT-BMED',
			ds."DAT_APER_CONTR"              =ap."DAT_APER_CONTR",
			ds."DAT_CHIU_CONTR"              =ap."DAT_CHIU_CONTR",
			ds."DAT_PRIMO_MOVI"              =ap."DAT_PRIMO_MOVI",
			ds."DAT_ULT_MOVI"                =ap."DAT_ULT_MOVI",
			ds."IMP_TOT_VERS"                =ap."IMP_TOT_VERS",
			ds."IMP_TOT_VERS_NO_CONVS"       =ap."IMP_TOT_VERS_NO_CONVS",
			ds."IMP_TOT_INVEST"              =ap."IMP_TOT_INVEST",
			ds."IMP_TOT_INVEST_NO_CONVS"     =ap."IMP_TOT_INVEST_NO_CONVS",
			ds."IMP_TOT_RIMBO"               =ap."IMP_TOT_RIMBO",
			ds."IMP_TOT_RIMBO_NO_CONVS"      =ap."IMP_TOT_RIMBO_NO_CONVS",
			ds."IMP_PRVNT_PASS"              =ap."IMP_PRVNT_PASS",
			ds."IMP_PRVNT_FUT"               =ap."IMP_PRVNT_FUT",
			ds."QTA_QUOTA_T2"                =ap."QTA_QUOTA_T2",
			ds."IMP_INT_VERS"                =ap."IMP_INT_VERS",
			ds."IMP_SALDO_VERS"              =ap."IMP_SALDO_VERS",
			ds."IMP_INT_INVEST"              =ap."IMP_INT_INVEST",
			ds."IMP_SALDO_INVEST"            =ap."IMP_SALDO_INVEST",
			ds."IMP_CONTVAL"                 =ap."IMP_CONTVAL_T2",
			ds."QTA_QUOTA_SALDO"             =ap."QTA_QUOTA_SALDO",
			ds."PRC_REND_VPATR_VERS_ST"      =ap."PRC_REND_VPATR_VERS_ST",
			ds."PRC_REND_VPATR_VERS_ANNUAL"  =ap."PRC_REND_VPATR_VERS_ANNUAL",
			ds."PRC_REND_VPATR_INVEST_ST"    =ap."PRC_REND_VPATR_INVEST_ST",
			ds."PRC_REND_VPATR_INVEST_ANNUAL"=ap."PRC_REND_VPATR_INVEST_ANNUAL",
			ds."PRC_REND_MWRR_VERS_ST"       =ap."PRC_REND_MWRR_VERS_ST",
			ds."PRC_REND_MWRR_VERS_ANNUAL"   =ap."PRC_REND_MWRR_VERS_ANNUAL",
			ds."PRC_REND_MWRR_INVEST_ST"     =ap."PRC_REND_MWRR_INVEST_ST",
			ds."PRC_REND_MWRR_INVEST_ANNUAL" =ap."PRC_REND_MWRR_INVEST_ANNUAL",
			ds."IMP_PLUS_MINUS_VERS"         =ap."IMP_PLUS_MINUS_VERS",
			ds."IMP_PLUS_MINUS_INVEST"       =ap."IMP_PLUS_MINUS_INVEST" WHEN NOT MATCHED THEN
		INSERT
			(
				ds.PROD_C ,
				ds.PROD_C_PADRE ,
				ds.CONTR_N ,
				ds.COD_GEOG_FONDO ,
				ds.FLG_PIR,
				ds.DAT_RIFE ,
				ds.GSTD_F_ESIST ,
				ds.GSTD_X_TIP_MODF ,
				ds.GSTD_D_ULT_MODF_RECORD ,
				ds.GSTD_X_USER ,
				ds.GSTD_M_NOM_ULT_MODF ,
				ds.DAT_APER_CONTR ,
				ds.DAT_CHIU_CONTR ,
				ds.DAT_PRIMO_MOVI ,
				ds.DAT_ULT_MOVI ,
				ds.IMP_TOT_VERS ,
				ds.IMP_TOT_VERS_NO_CONVS ,
				ds.IMP_TOT_INVEST ,
				ds.IMP_TOT_INVEST_NO_CONVS ,
				ds.IMP_TOT_RIMBO ,
				ds.IMP_TOT_RIMBO_NO_CONVS ,
				ds.IMP_PRVNT_PASS ,
				ds.IMP_PRVNT_FUT ,
				ds.QTA_QUOTA_T2 ,
				ds.IMP_INT_VERS ,
				ds.IMP_SALDO_VERS ,
				ds.IMP_INT_INVEST ,
				ds.IMP_SALDO_INVEST ,
				ds.IMP_CONTVAL ,
				ds.QTA_QUOTA_SALDO ,
				ds.PRC_REND_VPATR_VERS_ST ,
				ds.PRC_REND_VPATR_VERS_ANNUAL ,
				ds.PRC_REND_VPATR_INVEST_ST ,
				ds.PRC_REND_VPATR_INVEST_ANNUAL ,
				ds.PRC_REND_MWRR_VERS_ST ,
				ds.PRC_REND_MWRR_VERS_ANNUAL ,
				ds.PRC_REND_MWRR_INVEST_ST ,
				ds.PRC_REND_MWRR_INVEST_ANNUAL,
				ds.IMP_PLUS_MINUS_VERS,
				ds.IMP_PLUS_MINUS_INVEST
			)
			VALUES
			(
				ap.PROD_C ,
				ap.PROD_C_PADRE ,
				ap.CONTR_N ,
				ap.COD_GEOG_FONDO ,
				ap.FLG_PIR,
				ap.DAT_RIFE ,
				'S' ,
				'I' ,
				SYSDATE ,
				'MOT-BMED' ,
				'MOT-BMED' ,
				ap.DAT_APER_CONTR ,
				ap.DAT_CHIU_CONTR ,
				ap.DAT_PRIMO_MOVI ,
				ap.DAT_ULT_MOVI ,
				ap.IMP_TOT_VERS ,
				ap.IMP_TOT_VERS_NO_CONVS ,
				ap.IMP_TOT_INVEST ,
				ap.IMP_TOT_INVEST_NO_CONVS ,
				ap.IMP_TOT_RIMBO ,
				ap.IMP_TOT_RIMBO_NO_CONVS ,
				ap.IMP_PRVNT_PASS ,
				ap.IMP_PRVNT_FUT ,
				ap.QTA_QUOTA_T2 ,
				ap.IMP_INT_VERS ,
				ap.IMP_SALDO_VERS ,
				ap.IMP_INT_INVEST ,
				ap.IMP_SALDO_INVEST ,
				ap.IMP_CONTVAL_T2 ,
				ap.QTA_QUOTA_SALDO ,
				ap.PRC_REND_VPATR_VERS_ST ,
				ap.PRC_REND_VPATR_VERS_ANNUAL ,
				ap.PRC_REND_VPATR_INVEST_ST ,
				ap.PRC_REND_VPATR_INVEST_ANNUAL ,
				ap.PRC_REND_MWRR_VERS_ST ,
				ap.PRC_REND_MWRR_VERS_ANNUAL ,
				ap.PRC_REND_MWRR_INVEST_ST ,
				ap.PRC_REND_MWRR_INVEST_ANNUAL,
				ap.IMP_PLUS_MINUS_VERS,
				ap.IMP_PLUS_MINUS_INVEST
			);

	END IF;
	COMMIT;

END;

END;
/
