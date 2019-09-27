SET define OFF
 
CREATE OR REPLACE PACKAGE SOGG_OUTELAB.PKG_BANCA
IS
	attivaDebug char(1) := 'N'; -- 22022019
	progrDebug number := 0;   -- 22022019
	------------------------------------------------------------------------------------------------------
	--------- CURSORI
	------------------------------------------------------------------------------------------------------
    type tb_rollingWindow
    IS
	TABLE OF Sogg_Outelab.RT_ROLLING_DATES INDEX BY VARCHAR(11);

	dataRollingWindow tb_rollingWindow;

	CURSOR c_posiztit_qtaQuota_tit(datiRedd SOGG_OUTELAB.RT_TOTALS_TIT)
	IS

		SELECT pt.POSIZTIT_Q_TIT QTA_QUOTA
		FROM CLL.POSIZTIT pt
		WHERE pt.GSTD_F_ESIST            ='S'
		AND datiRedd.PROD_C              = pt.PROD_C
		AND datiRedd.CONTR_N             = pt.CONTR_N
		AND datiRedd.COD_TIT_INTERN      = pt.ANAATTIVFIN_C_CODICE_TIT_TRAN
		AND datiRedd.COD_INDICE_EMIS_TIT = pt.ANAATTIVFIN_X_INDICT_EMISS;

	CURSOR c_singoloDatoAnagrafico(COD_TIT_INTERN IN CHAR, COD_INDICE_EMIS_TIT IN CHAR)
	IS

		SELECT af.ANAATTIVFIN_C_CODICE_TIT_TRAN
			||'-'
			||af.ANAATTIVFIN_X_INDICT_EMISS chiave,
			af.ANAATTIVFIN_C_CODICE_TIT_TRAN COD_TIT_INTERN,
			af.ANAATTIVFIN_X_INDICT_EMISS COD_INDICE_EMIS_TIT,
			af.ANAATTIVFIN_X_DESCR NOME,
			af.ANAATTIVFIN_X_GRADO_LIQ COD_GRAD_LIQ,
			TRIM(af.ANAATTIVFIN_C) ISIN,
			af.ANAATTIVFIN_X_ESP_PREZ AF_ESPPREZ,
			af.ANAATTIVFIN_D_SCAD,
            CASE WHEN af.ANAATTIVFIN_X_GRADO_LIQ = 3 
                THEN 'S'
                ELSE 'N'
            END FLAG_TITOLO_LIQUIDO
		FROM CLL.Anaattivfin af
		WHERE af.ANAATTIVFIN_C_CODICE_TIT_TRAN = COD_TIT_INTERN
		AND af.ANAATTIVFIN_X_INDICT_EMISS      = COD_INDICE_EMIS_TIT;

	CURSOR c_datianagrafici
	IS

		SELECT af.ANAATTIVFIN_C_CODICE_TIT_TRAN
			||'-'
			||af.ANAATTIVFIN_X_INDICT_EMISS chiave,
			af.ANAATTIVFIN_C_CODICE_TIT_TRAN COD_TIT_INTERN,
			af.ANAATTIVFIN_X_INDICT_EMISS COD_INDICE_EMIS_TIT,
			af.ANAATTIVFIN_X_DESCR NOME,
			af.ANAATTIVFIN_X_GRADO_LIQ COD_GRAD_LIQ,
			TRIM(af.ANAATTIVFIN_C) ISIN,
			af.ANAATTIVFIN_X_ESP_PREZ AF_ESPPREZ,
			af.ANAATTIVFIN_D_SCAD,
            CASE WHEN af.ANAATTIVFIN_X_GRADO_LIQ = 3 
                THEN 'S'
                ELSE 'N'
            END FLAG_TITOLO_LIQUIDO            
		FROM CLL.Anaattivfin af
		WHERE af.ANAATTIVFIN_C_CODICE_TIT_TRAN IN
			(SELECT CAST (DES_CAMPO_MWRR AS CHAR (7))
			FROM SOGG_OUTELAB.Multidom_Mwrr
			WHERE Cod_Tipo_Dom_Mwrr='TIT_FREQ'
			);

	CURSOR c_load_chiu_gg(ultimoRun DATE)
	IS

		SELECT ST.*
		FROM SOGG_OUTELAB.SALDO_TOT_TIT ST
		WHERE ST.GSTD_F_ESIST         = 'S'
		AND ST.GSTD_D_ULT_MODF_RECORD > ultimoRun ----SOGG_OUTELAB.FETCHDATAULTIMORUN('TITOLI','S')
		AND NOT EXISTS
			(SELECT 1
			FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_TIT SA
			WHERE SA.DAT_RIFE          = TRUNC(SYSDATE)-1
			AND SA.PROD_C              = ST.PROD_C
			AND SA.CONTR_N             = ST.CONTR_N
			AND SA.COD_TIT_INTERN      = ST.COD_TIT_INTERN
			AND SA.COD_INDICE_EMIS_TIT = ST.COD_INDICE_EMIS_TIT
			AND SA.GSTD_F_ESIST        ='S'
			)
	AND NOT EXISTS
		(SELECT 1
		FROM SOGG_OUTELAB.Dett_Log_Err dr
		WHERE dr.prod_c         = st.prod_c
		AND dr.contr_n          = st.contr_n
		AND dr.COD_TIT_INTERN   = ST.COD_TIT_INTERN 
        AND dr.COD_INDICE_EMIS_TIT = ST.COD_INDICE_EMIS_TIT
		AND Dr.GSTD_D_INS_RECORD>TRUNC(sysdate)
		);

	CURSOR c_load_redd_gior(chiavi SOGG_OUTELAB.TB_KEY_CONTR, t DATE)
	IS

		SELECT PROD_C,
			CONTR_N,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT,
			DAT_RIFE,
			DAT_APER_CONTR,
			DAT_CHIU_CONTR,
			DAT_PRIMO_MOVI,
			DAT_ULT_MOVI,
			IMP_TOT_VERS,
			IMP_TOT_INVEST,
			IMP_TOT_RIMBO,
			IMP_PRVNT_PASS,
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
			PRC_REND_MWRR_INVEST_ANNUAL,
			COD_TIPO_TIT_REDDTA,
			IMP_PLUS_MINUS_VERS,
			IMP_PLUS_MINUS_INVEST
		FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_TIT
		WHERE TRUNC(dat_rife) =t
		AND
			(
				prod_c, contr_n
			)
			IN
			(SELECT a1.prod_c, a1.contr_n FROM (TABLE(chiavi)) a1
			)
	AND GSTD_F_ESIST = 'S'
	UNION ALL
	SELECT PROD_C,
		CONTR_N,
		COD_TIT_INTERN,
		COD_INDICE_EMIS_TIT,
		COD_TRANS_ACQU_PCT,
		DAT_RIFE,
		DAT_APER_CONTR,
		DAT_CHIU_CONTR,
		DAT_PRIMO_MOVI,
		DAT_ULT_MOVI,
		IMP_TOT_VERS,
		IMP_TOT_INVEST,
		IMP_TOT_RIMBO,
		IMP_PRVNT_PASS,
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
		PRC_REND_MWRR_INVEST_ANNUAL,
		COD_TIPO_TIT_REDDTA,
--		CASE WHEN COD_TIPO_TIT_REDDTA = 'FNDIM'
--			THEN IMP_CONTVAL+IMP_TOT_RIMBO-IMP_TOT_VERS
--			ELSE 0
--		END IMP_PLUS_MINUS_VERS,
--		CASE WHEN COD_TIPO_TIT_REDDTA = 'FNDIM'
--			THEN IMP_CONTVAL+IMP_TOT_RIMBO-IMP_TOT_INVEST
--			ELSE 0
--		END IMP_PLUS_MINUS_INVEST
		NVL(IMP_CONTVAL,0)+IMP_TOT_RIMBO-IMP_TOT_VERS	IMP_PLUS_MINUS_VERS,
		NVL(IMP_CONTVAL,0)+IMP_TOT_RIMBO-IMP_TOT_INVEST	IMP_PLUS_MINUS_INVEST
	FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_CHIU_TIT a3
	WHERE a3.gstd_f_esist     = 'S'
	AND GSTD_D_ULT_MODF_RECORD<t+2
	--AND COD_TIPO_TIT_REDDTA    IN ('FNDIM', 'PCT')
	AND COD_TIPO_TIT_REDDTA    NOT IN ('NOSEE')
	AND
		(
			prod_c, contr_n
		)
		IN
		(SELECT a1.prod_c, a1.contr_n FROM (TABLE(chiavi)) a1
		)
	AND NOT EXISTS
		(SELECT 1
		FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_TIT a4
		WHERE a4.gstd_f_esist = 'S'
		AND a4.dat_rife       =t
		AND
			(
				a4.prod_c, a4.contr_n
			)
			IN
			(SELECT a1.prod_c, a1.contr_n FROM (TABLE(chiavi)) a1
			)
		AND a4.prod_c             =a3.prod_c
		AND a4.contr_n            =a3.contr_n
		AND a4.COD_TIT_INTERN     =a3.COD_TIT_INTERN
		AND a4.COD_INDICE_EMIS_TIT=a3.COD_INDICE_EMIS_TIT
		);
		
	CURSOR c_load_val_quo_per_tit(chiavi SOGG_OUTELAB.TB_TOTALS_TIT, t DATE)
	IS

	WITH CONTRATTI AS
		(SELECT KS.PROD_C,
			KS.CONTR_N,
			KS.COD_TIT_INTERN,
			KS.COD_INDICE_EMIS_TIT
		FROM (TABLE(CHIAVI)) KS
		),
		qt_grezze_val AS
		(SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			QUOTAZ_C_DIV,
			QUOTAZ_C_COD_UIC,
			QUOTAZ_C_INDEMIS
		FROM CLL.QUOTAZ_OLD QT
		WHERE gstd_f_esist='S'
		AND
			(
				qt.QUOTAZ_C_COD_UIC, qt.QUOTAZ_C_INDEMIS
			)
			IN
			(SELECT ct.COD_TIT_INTERN,
				ct.COD_INDICE_EMIS_TIT
			FROM CONTRATTI ct
			)
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		UNION ALL
		SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			QUOTAZ_C_DIV,
			QUOTAZ_C_COD_UIC,
			QUOTAZ_C_INDEMIS
		FROM CLL.QUOTAZ QT
		WHERE gstd_f_esist='S'
		AND
			(
				qt.QUOTAZ_C_COD_UIC, qt.QUOTAZ_C_INDEMIS
			)
			IN
			(SELECT ct.COD_TIT_INTERN,
				ct.COD_INDICE_EMIS_TIT
			FROM CONTRATTI ct
			)
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		),
		qt_pulite_val AS
		(SELECT QUOTAZ_I_VAL_MERCATO,
			QUOTAZ_C_DIV,
			QUOTAZ_C_COD_UIC,
			QUOTAZ_C_INDEMIS,
			QUOTAZ_D
		FROM qt_grezze_val qg
		WHERE NOT EXISTS
			(SELECT 1
			FROM qt_grezze_val q1
			WHERE q1.QUOTAZ_C_COD_UIC=qg.QUOTAZ_C_COD_UIC
			AND q1.QUOTAZ_C_INDEMIS  =qg.QUOTAZ_C_INDEMIS
			AND q1.QUOTAZ_D          >qg.QUOTAZ_D
			)
		),
		qt_grezze_divise AS
		(SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			PRODATTFIN_C
		FROM CLL.QUOTAZ_OLD QT
		WHERE gstd_f_esist   ='S'
		AND qt.PRODATTFIN_C IN
			(SELECT QUOTAZ_C_DIV FROM qt_pulite_val
			)
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		UNION ALL
		SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			PRODATTFIN_C
		FROM CLL.QUOTAZ QT
		WHERE gstd_f_esist   ='S'
		AND qt.PRODATTFIN_C IN
			(SELECT QUOTAZ_C_DIV FROM qt_pulite_val
			)
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		),
		qt_pulite_divise AS
		(SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			PRODATTFIN_C
		FROM qt_grezze_divise qg
		WHERE NOT EXISTS
			(SELECT 1
			FROM qt_grezze_divise q1
			WHERE q1.PRODATTFIN_C=qg.PRODATTFIN_C
			AND q1.QUOTAZ_D      >qg.QUOTAZ_D
			)
		)
	SELECT TRIM(a1.QUOTAZ_C_COD_UIC)
		||'-'
		||TRIM(TO_CHAR(a1.QUOTAZ_C_INDEMIS,'00')) "KEY",
		CASE
			WHEN a1.QUOTAZ_C_DIV      ='EUR'
			OR b1.QUOTAZ_I_VAL_MERCATO=0
			THEN a1.QUOTAZ_I_VAL_MERCATO
			ELSE a1.QUOTAZ_I_VAL_MERCATO/b1.QUOTAZ_I_VAL_MERCATO
		END val_eur
	FROM qt_pulite_val a1
	LEFT JOIN qt_pulite_divise b1
	ON
		(
			a1.QUOTAZ_C_DIV=b1.PRODATTFIN_C
		) ;


	CURSOR c_load_val_quo_per_tit_crusc(t DATE)
	IS
		WITH qt_grezze_val AS
		(SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			QUOTAZ_C_DIV,
			QUOTAZ_C_COD_UIC,
			QUOTAZ_C_INDEMIS
		FROM CLL.QUOTAZ_OLD QT
		WHERE gstd_f_esist='S'
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		UNION ALL
		SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			QUOTAZ_C_DIV,
			QUOTAZ_C_COD_UIC,
			QUOTAZ_C_INDEMIS
		FROM CLL.QUOTAZ QT
		WHERE gstd_f_esist='S'
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		),
		qt_pulite_val AS
		(SELECT QUOTAZ_I_VAL_MERCATO,
			QUOTAZ_C_DIV,
			QUOTAZ_C_COD_UIC,
			QUOTAZ_C_INDEMIS,
			QUOTAZ_D
		FROM qt_grezze_val qg
		WHERE NOT EXISTS
			(SELECT 1
			FROM qt_grezze_val q1
			WHERE q1.QUOTAZ_C_COD_UIC=qg.QUOTAZ_C_COD_UIC
			AND q1.QUOTAZ_C_INDEMIS  =qg.QUOTAZ_C_INDEMIS
			AND q1.QUOTAZ_D          >qg.QUOTAZ_D
			)
		),
		qt_grezze_divise AS
		(SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			PRODATTFIN_C
		FROM CLL.QUOTAZ_OLD QT
		WHERE gstd_f_esist   ='S'
		AND qt.PRODATTFIN_C IN
			(SELECT QUOTAZ_C_DIV FROM qt_pulite_val
			)
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		UNION ALL
		SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			PRODATTFIN_C
		FROM CLL.QUOTAZ QT
		WHERE gstd_f_esist   ='S'
		AND qt.PRODATTFIN_C IN
			(SELECT QUOTAZ_C_DIV FROM qt_pulite_val
			)
		AND qt.quotaz_d<=t
		AND qt.quotaz_d > t-30
		AND 1           =1
		),
		qt_pulite_divise AS
		(SELECT QUOTAZ_D,
			QUOTAZ_I_VAL_MERCATO,
			PRODATTFIN_C
		FROM qt_grezze_divise qg
		WHERE NOT EXISTS
			(SELECT 1
			FROM qt_grezze_divise q1
			WHERE q1.PRODATTFIN_C=qg.PRODATTFIN_C
			AND q1.QUOTAZ_D      >qg.QUOTAZ_D
			)
		)
	SELECT TRIM(a1.QUOTAZ_C_COD_UIC)
		||'-'
		||TRIM(TO_CHAR(a1.QUOTAZ_C_INDEMIS,'00')) "KEY",
		CASE
			WHEN a1.QUOTAZ_C_DIV      ='EUR'
			OR b1.QUOTAZ_I_VAL_MERCATO=0
			THEN a1.QUOTAZ_I_VAL_MERCATO
			ELSE a1.QUOTAZ_I_VAL_MERCATO/b1.QUOTAZ_I_VAL_MERCATO
		END val_eur
	FROM qt_pulite_val a1
	LEFT JOIN qt_pulite_divise b1
	ON
		(
			a1.QUOTAZ_C_DIV=b1.PRODATTFIN_C
		) ;

	CURSOR c_load_val_quo_per(chiavi SOGG_OUTELAB.TB_TOTALS_TIT, t DATE)
	IS

		SELECT  KEY , VAL_EUR
		FROM (
				SELECT
				TRIM(QT.QUOTAZ_C_COD_UIC)
				||'-'
				||TRIM(TO_CHAR(QT.QUOTAZ_C_INDEMIS,'00')) "KEY",
					CASE WHEN (TRUNC(QT.QUOTAZ_D) < to_date('01/01/2002','dd/mm/yyyy') AND QT.QUOTAZ_C_DIV IS NULL) THEN QT.QUOTAZ_I_VAL_MERCATO/1936.27
						WHEN QT.QUOTAZ_C_DIV = 'EUR' THEN QT.QUOTAZ_I_VAL_MERCATO
						WHEN QT.QUOTAZ_C_DIV = 'ITL' THEN QT.QUOTAZ_I_VAL_MERCATO/1936.27
						ELSE QT.QUOTAZ_I_VAL_MERCATO
					END  VAL_EUR,
				Row_Number () over (partition BY QT.QUOTAZ_C_COD_UIC order by qt.quotaz_d DESC, qt.GSTD_D_ULT_MODF_RECORD DESC) rn
				FROM (SELECT QUOTAZ_C_INDEMIS, QUOTAZ_C_COD_UIC, QUOTAZ_D, QUOTAZ_C_DIV, QUOTAZ_I_VAL_MERCATO, GSTD_D_ULT_MODF_RECORD, GSTD_F_ESIST FROM CLL.QUOTAZ  WHERE GSTD_F_ESIST ='S'
				UNION
				SELECT QUOTAZ_C_INDEMIS, QUOTAZ_C_COD_UIC, QUOTAZ_D, QUOTAZ_C_DIV, QUOTAZ_I_VAL_MERCATO, GSTD_D_ULT_MODF_RECORD, GSTD_F_ESIST FROM CLL.QUOTAZ_OLD WHERE GSTD_F_ESIST ='S') QT
				WHERE QT.GSTD_F_ESIST ='S'
				AND
					(
					QT.QUOTAZ_C_COD_UIC, QT.QUOTAZ_C_INDEMIS
					)
				IN
				(SELECT KS.COD_TIT_INTERN,
				COALESCE (TO_NUMBER(REGEXP_REPLACE(KS.COD_INDICE_EMIS_TIT, '[^0-9]+', '')) , 0)
				FROM (TABLE(CHIAVI)) KS
				)
				AND QT.QUOTAZ_D <= T
				AND QT.QUOTAZ_D > T-31
			)
		WHERE RN = 1;

	CURSOR c_load_aper_tit
	IS

		SELECT tf.*,
			ROUND(IMP_CONTVAL+IMP_TOT_RIMBO-IMP_TOT_VERS, 2) IMP_PLUS_MINUS_VERS,
			ROUND(IMP_CONTVAL+IMP_TOT_RIMBO-IMP_TOT_INVEST, 2) IMP_PLUS_MINUS_INVEST
		FROM
			(SELECT tf.PROD_C,
				tf.CONTR_N,
				tf.COD_TIT_INTERN,
				tf.COD_INDICE_EMIS_TIT,
				tf.COD_TRANS_ACQU_PCT,
				tf.DAT_APER_CONTR,
				tf.DAT_CHIU_CONTR,
				tf.DAT_PRIMO_MOVI,
				tf.DAT_ULT_MOVI,
				tf.IMP_TOT_VERS,
				tf.IMP_TOT_INVEST,
				tf.IMP_TOT_RIMBO,
				tf.IMP_PRVNT_PASS,
				tf.QTA_QUOTA_T2,
				tf.IMP_INT_VERS,
				tf.IMP_SALDO_VERS,
				tf.IMP_INT_INVEST,
				tf.IMP_SALDO_INVEST,
				tf.COD_TIPO_TIT_REDDTA,
				tf.GSTD_M_NOM_ULT_MODF,
				tf.GSTD_X_USER,
				tf.GSTD_D_ULT_MODF_RECORD,
				tf.GSTD_D_INS_RECORD,
				tf.GSTD_X_TIP_MODF,
				tf.GSTD_F_ESIST,
				COALESCE (a1.CONTROVALORE,0) IMP_CONTVAL,
				COALESCE (a1.QUANTITA,0) QTA_QUOTA_SALDO,
				COALESCE(a1.DT_RIF , TRUNC(CURRENT_DATE)-1) DAT_RIFE,
				COALESCE(tf.QTA_SALDO_GIORNL_QUOTA_ZERO, 0) QTA_SALDO_GIORNL_QUOTA_ZERO
			FROM SOGG_OUTELAB.SALDO_TOT_TIT tf
				LEFT OUTER JOIN
				(SELECT pr.rubrica contr_n,
					pr.PROD_C_PADRE PROD_C,
					CAST(pr.prod_c_figlio1 AS CHAR(7)) COD_TIT_INTERN,
					CAST(pr.prod_c_figlio2 AS CHAR(2)) COD_INDICE_EMIS_TIT,
					TRUNC(CURRENT_DATE)-1 dt_rif,
					MIN (Pr.Controvalore) Controvalore,
					MIN (pr.QUANTITA) QUANTITA
				FROM ser_mifid.prodotti pr
				WHERE pr.RUOLO                IN ('I','P')
				AND pr.PORTAFOGLIO            IN ('P','A')
				AND pr.prod_c_padre            ='BAN12'
				AND pr.DESCR_CONTR_PADRE      IN ('FONDIIMM','INVESTIMENTI', 'DEPTITOLI', 'FONDITERZI')
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
				GROUP BY pr.rubrica,
					pr.PROD_C_PADRE,
					pr.prod_c_figlio1,
					pr.prod_c_figlio2
				) a1
			ON A1.prod_c                = tf.prod_c
			AND a1.contr_n              = tf.contr_n
			AND a1.COD_TIT_INTERN       =tf.COD_TIT_INTERN
			AND a1.COD_INDICE_EMIS_TIT  =tf.COD_INDICE_EMIS_TIT
			WHERE tf.GSTD_F_ESIST       = 'S'
			AND tf.COD_TIPO_TIT_REDDTA <> 'PCT'
			AND tf.QTA_QUOTA_T2         > 0
			UNION ALL
			SELECT tf2.PROD_C,
				tf2.CONTR_N,
				tf2.COD_TIT_INTERN,
				tf2.COD_INDICE_EMIS_TIT,
				tf2.COD_TRANS_ACQU_PCT,
				tf2.DAT_APER_CONTR,
				tf2.DAT_CHIU_CONTR,
				tf2.DAT_PRIMO_MOVI,
				tf2.DAT_ULT_MOVI,
				tf2.IMP_TOT_VERS,
				tf2.IMP_TOT_INVEST,
				tf2.IMP_TOT_RIMBO,
				tf2.IMP_PRVNT_PASS,
				tf2.QTA_QUOTA_T2,
				tf2.IMP_INT_VERS,
				tf2.IMP_SALDO_VERS,
				tf2.IMP_INT_INVEST,
				tf2.IMP_SALDO_INVEST,
				tf2.COD_TIPO_TIT_REDDTA,
				tf2.GSTD_M_NOM_ULT_MODF,
				tf2.GSTD_X_USER,
				tf2.GSTD_D_ULT_MODF_RECORD,
				tf2.GSTD_D_INS_RECORD,
				tf2.GSTD_X_TIP_MODF,
				tf2.GSTD_F_ESIST,
				tf2.IMP_CONTVAL,
				tf2.QTA_QUOTA_T2 QTA_QUOTA_SALDO,
				TRUNC(SYSDATE)-1 DAT_RIFE,
				COALESCE(tf2.QTA_SALDO_GIORNL_QUOTA_ZERO, 0) QTA_SALDO_GIORNL_QUOTA_ZERO
			FROM SOGG_OUTELAB.SALDO_TOT_TIT tf2
			WHERE tf2.gstd_f_esist                                           = 'S'
			AND COALESCE(DAT_CHIU_CONTR, to_date('01-01-9999','dd-mm-yyyy')) > sysdate
			AND tf2.COD_TIPO_TIT_REDDTA                                      = 'PCT'
			) tf
		ORDER BY tf.contr_n ;		--
		-- cursor c_load_chiu_fnd_terzi is
		--  Select
		--    tf.*
		--  From
		--    SOGG_OUTELAB.SALDO_TOT_FONDO_TERZI tf
		--  where
		--    tf.gstd_f_esist='S' and
		--    not exists (
		--   select 1
		--   from
		--     CLL.posizft pf
		--   where
		--     pf.gstd_f_esist='S' and
		--     pf.prod_c=tf.prod_c and
		--     pf.contr_n=tf.contr_n and
		--     pf.ANAATTIVFIN_C_CODICE_TIT_TRAN=tf.COD_TIT_INTERN and
		--     pf.ANAATTIVFIN_X_INDICT_EMISS=tf.COD_INDICE_EMIS_TIT and
		--     Pf.POSIZFT_Q_TIT>0
		--    )
		-- ;
		--


		CURSOR c_contr_massivo(chiavi SOGG_OUTELAB.TB_KEY_CONTR)
		IS

			SELECT cn.*
			FROM (TABLE(chiavi)) ks,
				cll.contr cn
			WHERE cn.prod_c=ks.prod_c
			AND cn.contr_n =ks.contr_n ;

		CURSOR c_chiavi_del_giorno(blk NUMBER, c_COD_TIPO_RECORD CHAR)
		IS

			SELECT SOGG_OUTELAB.RT_KEY_TIT(a1.prod_c, a1.contr_n, a1.COD_TIT_INTERN, a1.COD_INDICE_EMIS_TIT, a1.COD_TRANS_ACQU_PCT)
			FROM
				( SELECT DISTINCT lc.prod_c,
					lc.contr_n,
					lc.COD_TIT_INTERN,
					lc.COD_INDICE_EMIS_TIT,
					lc.COD_TRANS_ACQU_PCT
				FROM SOGG_OUTELAB.DETT_LOG_MOVI_CONTR_FT_TIT lc
				WHERE lc.Dat_Rife    <= TRUNC(SYSDATE)
				AND lc.gstd_f_esist   ='S'
				AND lc.COD_TIPO_RECORD=c_COD_TIPO_RECORD
				AND NOT EXISTS
					(SELECT 1
					FROM cll.contr cn
					WHERE cn.prod_c =lc.prod_c
					AND cn.contr_n  =lc.contr_n
					AND
						(
							cn.CONTR_C_CAT IN('0722','0723','0725','0729')
						OR
							(
								cn.CONTR_C_CAT     = '0738'
							AND cn.CONTR_C_SCAT = '000'
							)
						)
					)
				) a1
			WHERE rownum<=blk ;

 	CURSOR c_chiavi_giorno_cruscotti(blk NUMBER, c_COD_TIPO_RECORD CHAR)
	IS
			SELECT SOGG_OUTELAB.RT_KEY_TIT(a1.prod_c, a1.contr_n, a1.COD_TIT_INTERN, a1.COD_INDICE_EMIS_TIT, a1.COD_TRANS_ACQU_PCT)
			FROM
				( SELECT DISTINCT lc.prod_c,
					lc.contr_n,
					lc.COD_TIT_INTERN,
					lc.COD_INDICE_EMIS_TIT,
					lc.COD_TRANS_ACQU_PCT
				FROM SOGG_OUTELAB.DETT_LOG_MOVI_CONTR_CRUSCTO lc
				WHERE --lc.Dat_Rife    <= TRUNC(SYSDATE) AND
				 lc.gstd_f_esist   ='S'
				AND lc.COD_TIPO_RECORD=c_COD_TIPO_RECORD
				AND NOT EXISTS
					(SELECT 1
					FROM cll.contr cn
					WHERE cn.prod_c =lc.prod_c
					AND cn.contr_n  =lc.contr_n
					AND
						(
							cn.CONTR_C_CAT IN('0722','0723','0725','0729')
						OR
							(
								cn.CONTR_C_CAT     = '0738'
							AND cn.CONTR_C_SCAT = '000'
							)
						)
					)
				) a1
			WHERE rownum<=blk ;



--il cursore di estrazione eventi per i titoli Ã¿Â¨ la UNION di 3 query:
-- 1) estrazione eventi dei SOLI Real Estate (cll.operztit in join con la CLL.DECODRE)
-- 2) estrazione eventi dei titoli (eccetto Real Estate) posteriori al CUTOFF
-- 3) estrazione dell'evento CUTOFF
			CURSOR eventi_titoli(chiavi SOGG_OUTELAB.TB_KEY_TIT)
			IS

				SELECT *
				FROM
					(
                    SELECT A.*
					FROM
						(SELECT ot.prod_c prod_c,
							ot.CONTR_N CONTR_N,
							trim(ot.ANAATTIVFIN_C_CODICE_TIT_TRAN) ANAATTIVFIN_C_CODICE_TIT_TRAN,
							trim(ot.ANAATTIVFIN_X_INDICT_EMISS) ANAATTIVFIN_X_INDICT_EMISS,
							CASE
								WHEN OPERZTIT_N_PCT IN (1,2)
								THEN OPERZTIT_D_VLT
								WHEN OPERZTIT_C_CAUS = 'YB' THEN OPERZTIT_D_VLT
								ELSE LEAST(OPERZTIT_D_REGIST, OPERZTIT_D_VLT)
							END DAT_RIF,
							ROUND(ABS(OT.OPERZTIT_I_CTRV/DECODE(OPERZTIT_C_DIV,'EUR',1,'ITL',1936.27,DECODE(OPERZTIT_I_CAMBIO,0,NULL,OPERZTIT_I_CAMBIO) ) ), 3) IMP_NETTO,
							ROUND(ABS(OT.OPERZTIT_I_IMP /DECODE(OPERZTIT_C_DIV,'EUR',1,'ITL',1936.27,DECODE(OPERZTIT_I_CAMBIO,0,NULL,OPERZTIT_I_CAMBIO) ) ), 3) IMP_LORDO,
							CASE
								WHEN OT.OPERZTIT_S_SEGNO=1
								THEN -1
								ELSE 1
							END SEGNO,
							OT.OPERZTIT_S_SEGNO OPERZTIT_S_SEGNO,
							trim(a.ANAATTIVFIN_X_TIPOL_TIT) AF_TPPOL_TIT,
							trim(a.ANAATTIVFIN_X_COLL_BANCA) AF_COLLBANCA,
							CASE
								WHEN OPERZTIT_N_PCT = '1'
								THEN TRIM(OPERZTIT_C_TRANSID)
								ELSE TRIM(OPERZTIT_C_OPERID)
							END PCT_ACQ_TRANSID,
							TRIM(OPERZTIT_N_PCT) OPERZTIT_N_PCT,
							OT.OPERZTIT_Q_QTA OPERZTIT_Q_QTA,
							NULL COD_SOC_GEST,
							TRIM(OPERZTIT_C_CAUS) OPERZTIT_C_CAUS,
							COALESCE(operztit_i_spese, 0) operztit_i_spese,
							COALESCE(operztit_i_comms, 0) operztit_i_comms,
							COALESCE(operztit_i_boll, 0) operztit_i_boll,
							COALESCE(operztit_i_ctrv ,0) operztit_i_ctrv,
							OT.operztit_i_imp,
							OT.OPERZTIT_C_DIV,
							OT.OPERZTIT_I_CAMBIO,
							OT.operztit_d_vlt,
							a.ANAATTIVFIN_D_SCAD D_SCAD_CERT,
							trim(a.ANAATTIVFIN_C) ANAATTIVFIN_C,
							a.ANAATTIVFIN_X_ESP_PREZ AF_ESPPREZ,
							OT.COD_MOTV_VERSMT, -- 22022019
							OT.OPERZTIT_C_DIV_REG, -- 22022019
							OT.OPERZTIT_D_REGIST --12072019 Estraggo questa data che uso per alcune regole su movimenti anomali
						FROM --(TABLE(chiavi)) ch,
							CLL.OPERZTIT OT,
							CLL.ANAATTIVFIN A
						WHERE OT.GSTD_F_ESIST ='S'
						AND a.GSTD_F_ESIST    ='S'
						--AND OT.prod_C         ='BAN12'
						--AND CH.prod_C         ='BAN12'
						--AND ot.contr_n        =ch.contr_n
                        and (ot.contr_n, ot.prod_c) in (select distinct contr_n, prod_c from (TABLE(chiavi)) ch)
						AND A.ANAATTIVFIN_C_CODICE_TIT_TRAN = ot.ANAATTIVFIN_C_CODICE_TIT_TRAN
						AND A.ANAATTIVFIN_X_INDICT_EMISS    = ot.ANAATTIVFIN_X_INDICT_EMISS
						) A
				INNER JOIN CLL.DECODRE B
				ON A.ANAATTIVFIN_C_CODICE_TIT_TRAN                                                  = trim(B.DECODRE_C_CODICE_TIT_TRAN)
                AND A.ANAATTIVFIN_X_INDICT_EMISS                                                    = NVL(trim(B.COD_INDICE_EMIS_TIT),'00')
				WHERE A.DAT_RIF < to_date('01/01/2013','dd/MM/yyyy')

                UNION ALL

                    SELECT A.*
					FROM
						(SELECT ot.prod_c prod_c,
							ot.CONTR_N CONTR_N,
							trim(ot.ANAATTIVFIN_C_CODICE_TIT_TRAN) ANAATTIVFIN_C_CODICE_TIT_TRAN,
							trim(ot.ANAATTIVFIN_X_INDICT_EMISS) ANAATTIVFIN_X_INDICT_EMISS,
							CASE
								WHEN OPERZTIT_N_PCT IN (1,2)
								THEN OPERZTIT_D_VLT
								WHEN OPERZTIT_C_CAUS = 'YB' THEN OPERZTIT_D_VLT
								ELSE LEAST(OPERZTIT_D_REGIST, OPERZTIT_D_VLT)
							END DAT_RIF,
							ROUND(ABS(OT.OPERZTIT_I_CTRV/DECODE(OPERZTIT_C_DIV,'EUR',1,'ITL',1936.27,DECODE(OPERZTIT_I_CAMBIO,0,NULL,OPERZTIT_I_CAMBIO) ) ), 3) IMP_NETTO,
							ROUND(ABS(OT.OPERZTIT_I_IMP /DECODE(OPERZTIT_C_DIV,'EUR',1,'ITL',1936.27,DECODE(OPERZTIT_I_CAMBIO,0,NULL,OPERZTIT_I_CAMBIO) ) ), 3) IMP_LORDO,
							CASE
								WHEN OT.OPERZTIT_S_SEGNO=1
								THEN -1
								ELSE 1
							END SEGNO,
							OT.OPERZTIT_S_SEGNO OPERZTIT_S_SEGNO,
							trim(a.ANAATTIVFIN_X_TIPOL_TIT) AF_TPPOL_TIT,
							trim(a.ANAATTIVFIN_X_COLL_BANCA) AF_COLLBANCA,
							CASE
								WHEN OPERZTIT_N_PCT = '1'
								THEN TRIM(OPERZTIT_C_TRANSID)
								ELSE TRIM(OPERZTIT_C_OPERID)
							END PCT_ACQ_TRANSID,
							TRIM(OPERZTIT_N_PCT) OPERZTIT_N_PCT,
							OT.OPERZTIT_Q_QTA OPERZTIT_Q_QTA,
							NULL COD_SOC_GEST,
							TRIM(OPERZTIT_C_CAUS) OPERZTIT_C_CAUS,
							COALESCE(operztit_i_spese, 0) operztit_i_spese,
							COALESCE(operztit_i_comms, 0) operztit_i_comms,
							COALESCE(operztit_i_boll, 0) operztit_i_boll,
							COALESCE(operztit_i_ctrv ,0) operztit_i_ctrv,
							OT.operztit_i_imp,
							OT.OPERZTIT_C_DIV,
							OT.OPERZTIT_I_CAMBIO,
							OT.operztit_d_vlt,
							a.ANAATTIVFIN_D_SCAD D_SCAD_CERT,
							trim(a.ANAATTIVFIN_C) ANAATTIVFIN_C,
							a.ANAATTIVFIN_X_ESP_PREZ AF_ESPPREZ,
							OT.COD_MOTV_VERSMT, -- 22022019
							OT.OPERZTIT_C_DIV_REG, -- 22022019
							OT.OPERZTIT_D_REGIST --12072019 Estraggo questa data che uso per alcune regole su movimenti anomali
						FROM --(TABLE(chiavi)) ch,
							CLL.OPERZTIT OT,
							CLL.ANAATTIVFIN A
						WHERE OT.GSTD_F_ESIST ='S'
						AND a.GSTD_F_ESIST    ='S'
						--AND OT.prod_C         ='BAN12'
						--AND CH.prod_C         ='BAN12'
						--AND ot.contr_n        =ch.contr_n
                        and (ot.contr_n, ot.prod_c) in (select distinct contr_n, prod_c from (TABLE(chiavi)) ch)
						AND A.ANAATTIVFIN_C_CODICE_TIT_TRAN = ot.ANAATTIVFIN_C_CODICE_TIT_TRAN
						AND A.ANAATTIVFIN_X_INDICT_EMISS    = ot.ANAATTIVFIN_X_INDICT_EMISS
						) A
				WHERE A.DAT_RIF >= to_date('01/01/2013','dd/MM/yyyy')

                UNION ALL

				SELECT pt.prod_c PROD_C,
					pt.CONTR_N CONTR_N,
					trim(pt.ANAATTIVFIN_C_CODICE_TIT_TRAN) ANAATTIVFIN_C_CODICE_TIT_TRAN,
					trim(pt.ANAATTIVFIN_X_INDICT_EMISS) ANAATTIVFIN_X_INDICT_EMISS,
					to_date('01/01/2013','dd/MM/yyyy') DAT_RIF,
                    ROUND(ABS((pt.posiztit_q_tit + pt.POSIZTIT_Q_RICV - pt.POSIZTIT_Q_CONSGN) * nvl(qt.QUOTAZ_I_VAL_MERCATO,0)/NVL(qtCambio.QUOTAZ_I_VAL_MERCATO,1)),3)  IMP_NETTO,
                    ROUND(ABS((pt.posiztit_q_tit + pt.POSIZTIT_Q_RICV - pt.POSIZTIT_Q_CONSGN) * nvl(qt.QUOTAZ_I_VAL_MERCATO,0)/NVL(qtCambio.QUOTAZ_I_VAL_MERCATO,1)),3)  IMP_LORDO,
					1 SEGNO,
					'0' OPERZTIT_S_SEGNO,
					trim(a.ANAATTIVFIN_X_TIPOL_TIT) AF_TPPOL_TIT,
					trim(a.ANAATTIVFIN_X_COLL_BANCA) AF_COLLBANCA,
					'0' PCT_ACQ_TRANSID,
					'0' OPERZTIT_N_PCT,
					pt.posiztit_q_tit + pt.POSIZTIT_Q_RICV - pt.POSIZTIT_Q_CONSGN OPERZTIT_Q_QTA,
					NULL COD_SOC_GEST,
					'CUTOFF' OPERZTIT_C_CAUS,
					0 operztit_i_spese,
					0 operztit_i_comms,
					0 operztit_i_boll,
					0 operztit_i_ctrv,
                    ROUND(ABS((pt.posiztit_q_tit + pt.POSIZTIT_Q_RICV - pt.POSIZTIT_Q_CONSGN) * nvl(qt.QUOTAZ_I_VAL_MERCATO,0)/NVL(qtCambio.QUOTAZ_I_VAL_MERCATO,1)),3) operztit_i_imp,
					'CUTOFF' OPERZTIT_C_DIV,
					1 OPERZTIT_I_CAMBIO,
					to_date('01/01/2013','dd/MM/yyyy') operztit_d_vlt,
					NULL D_SCAD_CERT,
					trim(a.ANAATTIVFIN_C) ANAATTIVFIN_C,
					a.ANAATTIVFIN_X_ESP_PREZ AF_ESPPREZ,
					' ' COD_MOTV_VERSMT, -- 22022019
					' ' OPERZTIT_C_DIV_REG, -- 22022019
					to_date('01/01/2013','dd/MM/yyyy') as OPERZTIT_D_REGIST --12072019 Estraggo questa data che uso per alcune regole su movimenti anomali
                    FROM
                    (select * from CLL.POSIZTIT_INIZ_MWRR where (prod_c, contr_n) in
						(select  PROD_C, CONTR_N from (TABLE(chiavi)))
						and (posiztit_q_tit + POSIZTIT_Q_RICV - POSIZTIT_Q_CONSGN) > 0
                    )pt
					INNER JOIN 	CLL.ANAATTIVFIN a on
                        a.ANAATTIVFIN_C_CODICE_TIT_TRAN = pt.ANAATTIVFIN_C_CODICE_TIT_TRAN and
                        a.ANAATTIVFIN_X_INDICT_EMISS    = pt.ANAATTIVFIN_X_INDICT_EMISS
                    LEFT OUTER JOIN CLL.QUOTAZ_OLD qt on  -- outer join perche' non sempre in CLL.QUOTAZ_OLD esiste la quotazione del titolo nel periodo CUTOFF
                        pt.ANAATTIVFIN_C_CODICE_TIT_TRAN  = qt.QUOTAZ_C_COD_UIC and
                        pt.ANAATTIVFIN_X_INDICT_EMISS = qt.QUOTAZ_C_INDEMIS
						AND QUOTAZ_D <=   to_date('01012013','ddmmyyyy')
                        AND QUOTAZ_D >   trunc(to_date('01012013','ddmmyyyy')-30)
                        and  (qt.QUOTAZ_D, qt.QUOTAZ_C_COD_UIC) in
                        (
                            select max(QUOTAZ_D), QUOTAZ_C_COD_UIC from CLL.QUOTAZ_OLD
                            where QUOTAZ_C_COD_UIC = pt.ANAATTIVFIN_C_CODICE_TIT_TRAN
                            AND QUOTAZ_C_INDEMIS = pt.ANAATTIVFIN_X_INDICT_EMISS
                            AND QUOTAZ_D <=   to_date('01012013','ddmmyyyy')
                            AND QUOTAZ_D >   trunc(to_date('01012013','ddmmyyyy')-30)
                            group by QUOTAZ_C_COD_UIC
                        )
                    LEFT OUTER JOIN CLL.QUOTAZ_OLD qtCambio on  --estrazione del cambio dalla CLL.QUOTAZ_OLD.
						 qtCambio.quotaz_d 			= to_date('31122012','ddmmyyyy') --qt.quotaz_d
                         and qtCambio.QUOTAZ_C_COD_UIC = '       '
						 and qtCambio.QUOTAZ_C_DIV	= a.ANAATTIVFIN_X_DIV_QUOT
						 and qtCambio.PRODATTFIN_C 	= a.ANAATTIVFIN_X_DIV_QUOT
                    WHERE a.GSTD_F_ESIST                  ='S'
                    )
				ORDER BY DAT_RIF, OPERZTIT_I_IMP, OPERZTIT_S_SEGNO; -- 25042019 il segno serve per il cambio codice titolo per metterli sempre in ordine a parita di data e importo

				CURSOR config_cur(codTipo VARCHAR2)
				IS

					SELECT TRIM(COD_CAMPO_MWRR) tipo_op_key,
						TRIM(DES_CAMPO_MWRR) tp_op_val
					FROM SOGG_OUTELAB.MULTIDOM_MWRR
					WHERE COD_TIPO_DOM_MWRR = CAST(codTipo AS CHAR(10 byte));

				CURSOR is_real_estate_cur
				IS

					SELECT trim(DECODRE_C_CODICE_TIT_TRAN) tit_tran, rownum FROM CLL.DECODRE;

				CURSOR c_posiztit(cRec SOGG_OUTELAB.RT_OUTPUT_ONLINE)
				IS

					SELECT
						CASE
							WHEN pt.POSIZTIT_Q_ALTRI_IMP>0
							AND pt.gstd_f_esist         ='S'
							THEN 'S'
							ELSE 'N'
						END FLG_BLOC_VEND,
						POSIZTIT_I_PREZ_MED_CA,
						DECODE(trim(Pt.Posiztit_C_Div), 'EUR', 1, pt.VAL_TASSO_CAMBIO_MED) PT_TMC,
						POSIZTIT_D_PCTSCAD DAT_SCAD_PCT
					FROM cll.posiztit pt
					WHERE pt.prod_c                     =cRec.PROD_C
					AND pt.contr_n                      =cRec.CONTR_N
					AND pt.gstd_f_esist                 ='S'
					AND pt.ANAATTIVFIN_C_CODICE_TIT_TRAN=cRec.COD_TIT_INTERN
					AND pt.ANAATTIVFIN_X_INDICT_EMISS   =cRec.COD_INDICE_EMIS_TIT;
					
					 --cursore di estrazione lista contr_n-cod_tit_intern-cod_indice_emis fondo re di partenza 
                    --e cod_tit_intern-cod_indice_emis_tit fondo RE di arrivo
                    CURSOR c_re_cambio_isin
                    IS
                    select 
                    trim(CONTR_N) CONTR_N,
                    COD_TIT_INTERN_SORG,
                    COD_INDICE_EMIS_TIT_SORG,
                    COD_TIT_INTERN_DEST,
                    COD_INDICE_EMIS_TIT_DEST
                    FROM SOGG_OUTELAB.DETT_TRASFER_TIT_RE;

				
		/*rfc 90228 CR 152 - il cursore seleziona prod_c e contr_n SOLO dei dossier privi di titoli sottostanti
				La select estrae da cll.contrcli attraverso outer join dalla saldo i dati relativi ai dossier*/
				CURSOR c_aggDossierApertoNonMov(datiredd SOGG_OUTELAB.TB_OUTPUT_ONLINE, chiavi SOGG_OUTELAB.TB_KEY_CONTR, t2 date, t1 date)
                IS
					SELECT ccontr.PROD_C 	PROD_C,
					ccontr.CONTR_N 			CONTR_N,
					ccontr.CONTR_D_CARICAM  DAT_APER_CONTR,
					ccontr.CONTR_D_ESTI 	DAT_CHIU_CONTR
                FROM CLL.CONTR ccontr
                LEFT OUTER JOIN (select prod_c, contr_n FROM TABLE(Datiredd) where NVL(TRIM(REC_TYPE), 'NOSEE') != 'NOSEE') dtr
				ON (ccontr.contr_n = dtr.contr_n AND ccontr.prod_c = dtr.prod_c)
                INNER JOIN (TABLE(chiavi)) ks ON (ccontr.prod_c = ks.prod_c AND ccontr.contr_n = ks.contr_n)
                WHERE ccontr.CONTR_C_CAT  IN
				(
					SELECT CAST (COD_CAMPO_MWRR AS CHAR (4))
					FROM SOGG_OUTELAB.Multidom_Mwrr
					WHERE Cod_Tipo_Dom_Mwrr='C_DOS_TIT'
				)
                AND ccontr.prod_c = 'BAN12'
                AND dtr.contr_n IS NULL
				and t1 <= ccontr.CONTR_D_ESTI
				and t2 >= ccontr.CONTR_D_CARICAM
				;


	CURSOR c_load_saldo_cruscotti(T1 DATE, T2 DATE, codicePeriodo VARCHAR2, tranche in number, modulo in number)
	IS
	
	with cc as
        (
           select cli_c, contr.contr_n, 'BAN12' as prod_c
            from contrcli, contr where  trim(contrcli.prod_c) = 'BAN12'  -- non togliere le trim sui prod_c, servono per forzare a non usare l'indice
			and contr.prod_c = 'BAN12' 
            and contrcli.contr_n = contr.contr_n_serv
			and contrcli.CONTRCLI_C_RUOLO_CONTR in ('I', 'C', 'P')
            union 
            select cli_c, contr_n, 'BAN12' as prod_c
            from contrcli where  trim(prod_c)=  'BAN12'   -- non togliere le trim sui prod_c, servono per forzare a non usare l'indice
			and contrcli.CONTRCLI_C_RUOLO_CONTR in ('I', 'C', 'P')			
        )
		SELECT distinct CC.CLI_C,
			SF.PROD_C ,
			COD_PERIOD_ELAB ,
			SF.CONTR_N ,
			SF.COD_TIT_INTERN ,            
			SF.COD_INDICE_EMIS_TIT, 
            SF.COD_TRANS_ACQU_PCT,
            SF.COD_TIPO_PROD_INVEST COD_TIPO_TIT_REDDTA,
            --NVL(SF.COD_TIPO_TIT_REDDTA, 'NOSEE') COD_TIPO_PROD_INVEST,
            CASE WHEN ANAATTIVFIN_X_RAGGR_TIT IN ('11','26','61','76') THEN 'WAR'
            WHEN ANAATTIVFIN_X_RAGGR_TIT IN ('27','77') THEN 'ETF'
            ELSE SF.COD_TIPO_PROD_INVEST
            END COD_TIPO_PROD_INVEST,
			DAT_APER_CONTR ,
			DAT_CHIU_CONTR ,
			IMP_TOT_VERS ,
			IMP_TOT_VERS_NO_CONVS ,
			IMP_TOT_INVEST ,
			IMP_TOT_INVEST_NO_CONVS ,
			IMP_TOT_RIMBO ,
			IMP_TOT_RIMBO_NO_CONVS ,
			IMP_PRVNT_PASS ,
			IMP_PRVNT_FUT ,
			QTA_QUOTA_T2 ,
			DAT_INIZ_PERIOD ,
			DAT_FINE_PERIOD ,
			QTA_QUOTA_T1,
			DAT_PRIMO_MOVI,
			DAT_ULT_MOVI,
			IMP_SALDO_VERS,
			IMP_SALDO_INVEST,
			IMP_INT_VERS,
			IMP_INT_INVEST,
            QTA_QUOTE_GIOR_ZERO QTA_SALDO_GIORNL_QUOTA_ZERO, 
            a.ANAATTIVFIN_X_ESP_PREZ
		FROM SOGG_OUTELAB.SALDO_TIT_PERIOD SF
		INNER JOIN CC
		ON SF.PROD_C           = CC.PROD_C
		AND SF.CONTR_N         = CC.CONTR_N
        INNER JOIN CLL.ANAATTIVFIN A
        ON  a.ANAATTIVFIN_C_CODICE_TIT_TRAN=SF.COD_TIT_INTERN
        AND a.ANAATTIVFIN_X_INDICT_EMISS   =SF.COD_INDICE_EMIS_TIT
		WHERE SF.FLG_ELAB      = 'N'  --16032019
		AND SF.DAT_INIZ_PERIOD = T1
		AND SF.DAT_FINE_PERIOD = T2
		AND SF.COD_PERIOD_ELAB = CAST(codicePeriodo AS VARCHAR2(3))
		AND nvl(DAT_CHIU_CONTR,T1+1) > T1 --16032019
		AND SF.DAT_APER_CONTR <= SF.DAT_FINE_PERIOD
		and mod(ORA_HASH(cli_c), modulo) = tranche -- Andd 91518, estraggo SOLO tranche che e in lavorazione
		ORDER BY CLI_C;


			--------------------------------------------------------------------------------------------------------
			----------- TIPI
			--------------------------------------------------------------------------------------------------------
				type tb_datianagrafici
		IS
			TABLE OF c_datianagrafici%rowtype INDEX BY VARCHAR(11);
		type tb_datianagrafici_pl
	IS
		TABLE OF c_datianagrafici%rowtype;
		datianagrafici tb_datianagrafici;
	type tb_val_quo
IS
	TABLE OF NUMBER INDEX BY VARCHAR(11);
	--
	--
type tb_myIndex
IS
	TABLE OF NUMBER INDEX BY VARCHAR(64);
type tipo_op_index
IS
	TABLE OF VARCHAR(32) INDEX BY VARCHAR(32);
	
type tipo_op_index_n IS TABLE OF number INDEX BY VARCHAR(6); -- 12032019
	
type is_real_estate_index
IS
	TABLE OF NUMBER INDEX BY VARCHAR(7);
	tipo_op_cedola_list_re tipo_op_index;
	tipo_op_cedola_list_tit tipo_op_index;
    tipo_op_regole_causali tipo_op_index;  -- 22022019 
	tipo_op_cambi tipo_op_index_n;  	-- 12032019
	tipo_bi_list_tit tipo_op_index;
	is_real_estate_list is_real_estate_index;
	tpmovi_proventi      CONSTANT SOGG_OUTELAB.TB_CHAR4 := SOGG_OUTELAB.TB_CHAR4('XQ', 'DIVD', 'XH', 'CEDD', 'XH');
	tpmovi_rimborsi_cert CONSTANT SOGG_OUTELAB.TB_CHAR4 := SOGG_OUTELAB.TB_CHAR4('XC','XD');
type tb_contr
IS
	TABLE OF cll.contr%rowtype INDEX BY VARCHAR(32);


--array associativo che conterrÃ  la lista contr_n-cod_tit_intern-cod_indice_emis fondo re di partenza e cod_tit_intern-cod_indice_emis_tit
--fondo RE di arrivo
type re_cambio_isin is table of c_re_cambio_isin%ROWTYPE INDEX BY VARCHAR(40);
lista_re_cambio_isin re_cambio_isin;

	
	type tb_DOS 
    IS
        TABLE OF c_aggDossierApertoNonMov%rowtype;

    --array contenenti il valore quota titoli per cruscotti
	quote_t1_crusc tb_val_quo;
	quote_t2_crusc tb_val_quo;


	time_id_valore_quota_t1 DATE;
	time_id_valore_quota_t2 DATE;
    
	type qta_holes
IS
	TABLE OF DATE INDEX BY VARCHAR(100) ;
	tb_qta_holes qta_holes;   

	--------------------------------------------------------------------------------------------------------
	----------- PROCEDURE
	--------------------------------------------------------------------------------------------------------
	--
	--
	--

	PROCEDURE calc_totali_titoli(
			in_cTotaliTitoli IN OUT nocopy tb_myIndex,
			cTotaliTitoli    IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT,
			cEventoTitoli    IN OUT nocopy eventi_titoli%rowtype,
			p_t1 DATE DEFAULT NULL,
			p_t2 DATE DEFAULT NULL,
			codicePeriodo CHAR DEFAULT NULL );

	PROCEDURE agg_tab_totali_gg(
			bulk_limit IN NUMBER,
			esito OUT CHAR,
			num_rec_elab OUT INTEGER,
			num_rec_scart OUT INTEGER,
			num_rec_warn OUT INTEGER);

	PROCEDURE agg_tab_totali(
			totali IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT );

	PROCEDURE ins_dati_online(
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
			dest IN CHAR);

	PROCEDURE calc_redd_aperti(
			bulk_limit IN NUMBER,
			esito OUT CHAR,
			num_rec_elab OUT INTEGER,
			num_rec_scart OUT INTEGER,
			num_rec_warn OUT INTEGER);

	PROCEDURE calc_redd_chiusi_gg(
			bulk_limit IN NUMBER,
			esito OUT CHAR,
			num_rec_elab OUT INTEGER,
			num_rec_scart OUT INTEGER,
			num_rec_warn OUT INTEGER);
	--
	--

	PROCEDURE calc_redd_periodo(
			chiavi IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
			t1     IN DATE,
			t2     IN DATE,
			cRec   IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE );

	PROCEDURE legg_val_quota_per_tit(
			chiavi IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT,
			t      IN DATE,
			quote OUT tb_val_quo);

	PROCEDURE legg_val_quota_per_tit_crusc(
			t      IN DATE,
			quote OUT tb_val_quo);            

	PROCEDURE aggiorna_real_estate_list;

	PROCEDURE aggiorna_tipo_cedola_list_re;

	PROCEDURE aggiorna_tipo_cedola_list_tit;

	PROCEDURE aggiorna_bond_isin_tit;

	PROCEDURE calc_redd_giornaliero(
			chiavi IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
			dt_rif DATE,
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE);

	PROCEDURE legg_contr_info(
			chiavi        IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
			contr_attuali IN OUT nocopy tb_contr );

	PROCEDURE loadInizialeDatiAnagrafici;
	PROCEDURE load_regole_causali; -- 22022019
	PROCEDURE load_cambi; -- 12032019

	PROCEDURE loadSingoloDatoAngrafico(
			COD_TIT_INTERN      IN CHAR,
			COD_INDICE_EMIS_TIT IN CHAR);

	PROCEDURE popola_dati_accessori_per(
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE);

	PROCEDURE popola_dati_anagrafici(
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE);

	PROCEDURE popola_dati_anagrafici_daily(
			cRec IN OUT nocopy SOGG_OUTELAB.RT_OUTPUT_ONLINE);

	PROCEDURE popola_da_posiztit_daily(
			cRec IN OUT nocopy SOGG_OUTELAB.RT_OUTPUT_ONLINE);

	PROCEDURE popola_flag_evento_barriera(
			cRec IN OUT nocopy SOGG_OUTELAB.RT_OUTPUT_ONLINE);

	PROCEDURE Elimina_Pct_Malformati(
			datiRedd IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT );

	PROCEDURE Update_qtaQuotaT2_titles(
			datiRedd IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT );
			
	PROCEDURE aggDossierApertoNonMov(
			chiavi IN OUT NoCopy SOGG_OUTELAB.TB_KEY_CONTR,
			datiredd IN OUT Nocopy Sogg_Outelab.Tb_Output_Online,
			t2 date,
			t1 date default NULL);			

	PROCEDURE aggiorna_lista_re_cambio_isin;  
    
    PROCEDURE verificaCambioIsinRE(
            in_cTotaliTitoli IN OUT nocopy tb_myIndex,
			cTotaliTitoli    IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT,
			cEventoTitoli    IN OUT nocopy eventi_titoli%rowtype,
            contractKey      IN VARCHAR,
            contractIndex    IN NUMBER);




	PROCEDURE agg_tab_totali_gg_cruscotti(
			bulk_limit IN NUMBER,
			esito OUT CHAR,
			num_rec_elab OUT INTEGER,
			num_rec_scart OUT INTEGER,
			num_rec_warn OUT INTEGER,
			typeParamT2 NUMBER DEFAULT 0,
			typeParamT1 NUMBER DEFAULT 0);


	PROCEDURE GET_ROLLING_WINDOWS(
			typeParamT2 NUMBER DEFAULT 0,
			typeParamT1 NUMBER DEFAULT 0);


	PROCEDURE agg_tab_totali_cruscotti(
			totali IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT );


	PROCEDURE calc_redd_dett_cruscotti(
			bulk_limit    IN NUMBER,
			t1            IN DATE,
			t2            IN DATE,
			codicePeriodo IN VARCHAR2,
			esito         IN OUT CHAR,
			num_rec_elab  IN OUT INTEGER,
			num_rec_scart IN OUT INTEGER,
			num_rec_warn  IN OUT INTEGER);


	PROCEDURE call_calc_redd_dett_cruscotti(
			bulk_limit IN NUMBER,
			esito OUT CHAR,
			num_rec_elab OUT INTEGER,
			num_rec_scart OUT INTEGER,
			num_rec_warn OUT INTEGER,
			typeParamT2 NUMBER DEFAULT 1,
			typeParamT1 NUMBER DEFAULT 0);

	PROCEDURE ins_dati_ol_cruscotti(
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
			dest IN CHAR);    
			
END;
/
show error;
