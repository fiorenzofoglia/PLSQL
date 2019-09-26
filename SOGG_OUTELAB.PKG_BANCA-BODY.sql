SET define OFF
 

CREATE OR REPLACE PACKAGE BODY SOGG_OUTELAB.PKG_BANCA
IS


	PROCEDURE loadSingoloDatoAngrafico(
			COD_TIT_INTERN      IN CHAR,
			COD_INDICE_EMIS_TIT IN CHAR)
	IS
		datoAnagrafico c_singolodatoanagrafico%ROWTYPE;
	BEGIN
		OPEN c_singolodatoanagrafico(COD_TIT_INTERN, COD_INDICE_EMIS_TIT);
		LOOP

			FETCH c_singolodatoanagrafico INTO datoAnagrafico;
			EXIT
		WHEN c_singolodatoanagrafico%NOTFOUND;
			datianagrafici(datoAnagrafico.chiave):=datoAnagrafico;

		END LOOP;
		CLOSE c_singolodatoanagrafico;

	END;

	PROCEDURE loadInizialeDatiAnagrafici
	IS
		prodisin tb_datianagrafici_pl;
	BEGIN
		OPEN c_datianagrafici;

		FETCH c_datianagrafici bulk collect INTO prodisin;

		CLOSE c_datianagrafici;

		FOR i IN 1 .. prodisin.COUNT
		LOOP
			datianagrafici(prodisin(i).chiave):=prodisin(i);

		END LOOP;

	END;

	PROCEDURE popola_dati_anagrafici(
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE)
	IS
		l_chiave VARCHAR(11);
	BEGIN

		IF datianagrafici.count = 0 THEN
			loadInizialeDatiAnagrafici;

		END IF;

		FOR i IN 1 .. cRec.COUNT
		LOOP
			l_chiave := trim(cRec(i).COD_TIT_INTERN)||'-'||trim(cRec(i).COD_INDICE_EMIS_TIT);

			IF NOT datianagrafici.EXISTS(l_chiave) THEN
				loadSingoloDatoAngrafico(cRec(i).COD_TIT_INTERN,cRec(i).COD_INDICE_EMIS_TIT);

			END IF;
			cRec(i).DEC_DESC     :=datianagrafici(l_chiave).NOME;
			cRec(i).COD_GRAD_LIQ := datianagrafici(l_chiave).COD_GRAD_LIQ;
			cRec(i).COD_ISIN     := datianagrafici(l_chiave).ISIN;
			--cRec(i).FLAG_TITOLO_LIQUIDO     := datianagrafici(l_chiave).FLAG_TITOLO_LIQUIDO;            
			-- setting non anagrafic field VAL_CAR here for avoiding extra access to table

            IF datianagrafici(l_chiave).FLAG_TITOLO_LIQUIDO = 'S' THEN
                cRec(i).FLG_GNR_01 := 'S';
            ELSE
                cRec(i).FLG_GNR_01 := 'N';           
            END IF;

			IF datianagrafici(l_chiave).AF_ESPPREZ <> 'L' AND cRec(i).VAL_CAR IS NOT NULL AND cRec(i).REC_TYPE IN ('OBBLI','MPB') THEN
				cRec(i).VAL_CAR                       := ROUND(cRec(i).VAL_CAR                                     /100, 3);

			ElsIf(Crec(I).Rec_Type IN ('CERT','PCT')) THEN
				Crec(I).Val_Car := Crec(I).Imp_Tot_Vers;

			END IF;
			
			IF Crec(I).REC_TYPE      ='CERT' THEN
                Crec(I).DAT_SCAD_CONTR := datianagrafici(l_chiave).ANAATTIVFIN_D_SCAD;
            END IF;

			IF Crec(I).REC_TYPE      ='CERT' THEN
                Crec(I).DAT_SCAD_CONTR := datianagrafici(l_chiave).ANAATTIVFIN_D_SCAD;
            END IF;

		END LOOP;

	END;

	PROCEDURE popola_dati_anagrafici_daily(
			cRec IN OUT nocopy SOGG_OUTELAB.RT_OUTPUT_ONLINE)
	IS
		l_chiave VARCHAR(11);
	BEGIN

		IF datianagrafici.count = 0 THEN
			loadInizialeDatiAnagrafici;

		END IF;
		l_chiave := trim(cRec.COD_TIT_INTERN)||'-'||trim(cRec.COD_INDICE_EMIS_TIT);

		IF NOT datianagrafici.EXISTS(l_chiave) THEN
			loadSingoloDatoAngrafico(cRec.COD_TIT_INTERN,cRec.COD_INDICE_EMIS_TIT);

		END IF;
		cRec.DEC_DESC     :=datianagrafici(l_chiave).NOME;
		cRec.COD_GRAD_LIQ := datianagrafici(l_chiave).COD_GRAD_LIQ;
		cRec.COD_ISIN     := datianagrafici(l_chiave).ISIN;
		-- setting non anagrafic field VAL_CAR here for avoiding extra access to table

        IF datianagrafici(l_chiave).FLAG_TITOLO_LIQUIDO = 'S' THEN
            cRec.FLG_GNR_01 := 'S';
        ELSE
            cRec.FLG_GNR_01 := 'N';           
        END IF;

		IF datianagrafici(l_chiave).AF_ESPPREZ <> 'L' AND cRec.VAL_CAR IS NOT NULL AND cRec.REC_TYPE IN ('OBBLI','MPB') THEN
			cRec.VAL_CAR                          := ROUND(cRec.VAL_CAR                                  /100, 3);

		END IF;

		IF cRec.REC_TYPE      ='CERT' THEN
			cRec.DAT_SCAD_CONTR := datianagrafici(l_chiave).ANAATTIVFIN_D_SCAD;

		END IF;

	END;

	PROCEDURE popola_dati_accessori_per(
			cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE)
	IS
	BEGIN
		popola_dati_anagrafici(cRec);

	END;

	PROCEDURE popola_da_posiztit_daily(
			cRec IN OUT nocopy SOGG_OUTELAB.RT_OUTPUT_ONLINE)
	IS
		flag_bloc_vend CHAR(1);
		PMC            NUMBER;
		PT_TMC         NUMBER;
		DAT_SCAD_PCT   DATE;
	BEGIN
		BEGIN
			flag_bloc_vend := 'N';
			PMC            := 0;
			OPEN c_posiztit(cRec);

			FETCH c_posiztit INTO flag_bloc_vend, PMC, PT_TMC, DAT_SCAD_PCT;
			-- RFC 67917 DEF 38527 for open PCTs

			IF cRec.REC_TYPE IN ('PCT') AND cRec.DAT_SCAD_CONTR IS NULL THEN
				cRec.DAT_SCAD_CONTR                                := DAT_SCAD_PCT;

			END IF;
			cRec.FLG_BLOC_VEND        := flag_bloc_vend;
			cRec.IMP_PRZ_MEDIO_DI_CAR := PMC /
			(

				CASE
				WHEN PT_TMC=0 THEN
					NULL
				ELSE
					PT_TMC
				END);

			IF cRec.REC_TYPE                  IN ('OBBLI','AZION', 'FNDTR', 'MPB') THEN
				cRec.VAL_CAR := cRec.QTA_QUOTA_T2 *cRec.IMP_PRZ_MEDIO_DI_CAR;

			END IF;
			CLOSE c_posiztit;

		EXCEPTION

		WHEN no_data_found THEN
			cRec.FLG_BLOC_VEND := 'N';
			CLOSE c_posiztit;

		END;

	END;

	PROCEDURE popola_flag_evento_barriera(
			cRec IN OUT nocopy SOGG_OUTELAB.RT_OUTPUT_ONLINE)
	IS
	BEGIN
		NULL;
		--Come da vecchio servizio, viene cablato a 'N'. Il valore Ã¨ giÃ  inserito come default all'interno di RT_OUTPUT_ONLINE
		--FOR i IN 1 .. cRec.COUNT
		--LOOP
		--cRec(i).FLG_EVEN_BAR := 'N';
		--END LOOP;

	END;

PROCEDURE calc_totali_titoli(
			in_cTotaliTitoli IN OUT nocopy tb_myIndex,
			cTotaliTitoli    IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT,
			cEventoTitoli    IN OUT nocopy eventi_titoli%rowtype,
			p_t1 DATE DEFAULT NULL,
			p_t2 DATE DEFAULT NULL,
            codicePeriodo CHAR DEFAULT NULL)
	IS
		contractKey       VARCHAR(100);
		contractIndex     NUMBER;
		isNelPeriodo      BOOLEAN;
		isTrasferito      BOOLEAN;
		isPct             BOOLEAN;
		isCert            BOOLEAN;
		isAzion           BOOLEAN;
		isObblig          BOOLEAN;
		isFondiTerzi      BOOLEAN;
		isMpb             BOOLEAN;
		isRealEstate      BOOLEAN;
		segno             NUMBER;
		deltaT            NUMBER;
		t1                DATE;
		t2                DATE;
		imp_versato       NUMBER;
		imp_investito     NUMBER;
		imp_rimborsato    NUMBER;
		imp_cedole_re     NUMBER;
		imp_cedole_tit    NUMBER;
		imp_trasferito    NUMBER;
		tipo_oper         NUMBER;
		contrval_pct      NUMBER;
		tasso_cambio      NUMBER;
		bi_isbond         CHAR(1);
		qta_saldo_hole_t1 DATE;
		imp_netto         NUMBER;
        imp_lordo         NUMBER;
		tmpChiave varchar2(12);  -- 22022019
		regola varchar2(10); 	-- 22022019
		quantita number;   	-- 22022019
		OPERZTIT_Q_QTA   NUMBER;
        OPERZTIT_I_IMP   NUMBER;
        OPERZTIT_I_CTRV  NUMBER;
		ev_AF_ESPPREZ    char(1);
BEGIN
	t2             := COALESCE (p_t2, TRUNC(SYSDATE)-1);
	isNelPeriodo   :=false;
	isTrasferito   :=false;
	isPct          := false;
	isCert         := false;
	isAzion        := false;
	isObblig       := false;
	isFondiTerzi   := false;
	isMpb          := false;
	isRealEstate   := false;
	imp_trasferito := 0;
	imp_versato    := 0;
	imp_investito  := 0;
	imp_rimborsato := 0;
	imp_cedole_re  := 0;
	imp_cedole_tit := 0;
	tipo_oper      := 1;
	contrval_pct   := 0;
	tasso_cambio   := 0;
	bi_isbond      := 'N';
	imp_netto      := cEventoTitoli.IMP_NETTO;
    imp_lordo      := cEventoTitoli.IMP_LORDO;
    OPERZTIT_I_IMP := cEventoTitoli.OPERZTIT_I_IMP;
    OPERZTIT_I_CTRV := cEventoTitoli.OPERZTIT_I_CTRV;
    OPERZTIT_Q_QTA := cEventoTitoli.OPERZTIT_Q_QTA;
	ev_AF_ESPPREZ  := coalesce(cEventoTitoli.AF_ESPPREZ,' ');
	--IL PCT_ACQ_TRANSID viene usato solo dai PCT. Inoltre, ho verificato che per alcuni altri titoli ci sono degli eventi che, a paritÃ  di titoli, a volve
	--scendono null e a volte a 000000000000000000.

	
	IF tipo_op_regole_causali.count = 0 THEN  load_regole_causali; END IF; -- 22022019  BEGIN
		tmpChiave := rpad(cEventoTitoli.OPERZTIT_C_CAUS,4)||rpad(nvl(cEventoTitoli.COD_MOTV_VERSMT,' '),2)||cEventoTitoli.OPERZTIT_S_SEGNO; 	
	IF tipo_op_regole_causali.EXISTS(tmpChiave) THEN
		regola := tipo_op_regole_causali(tmpChiave);		
	elsif (cEventoTitoli.OPERZTIT_C_CAUS = 'CUTOFF') then
		regola := 999;
	else

		if(attivaDebug  = 'S' ) then
			insert into sogg_outelab.dett_log_err (PROD_C,  CONTR_N,  COD_TIT_INTERN,  COD_INDICE_EMIS_TIT,  	COD_ERR_ELAB,
			DES_ERR_ELAB,
			GSTD_X_TIP_MODF,  GSTD_X_USER,  GSTD_M_NOM_ULT_MODF)
			values  (trim(cEventoTitoli.PROD_C), trim(cEventoTitoli.contr_n), cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN, cEventoTitoli.ANAATTIVFIN_X_INDICT_EMISS,progrDebug,
			'Regola ['|| tmpChiave ||'] non trovata.' , 'I', 'calc_tt_titoli', 'calc_tt_titoli' );

			progrDebug := progrDebug+1;
		end if;
		return;
	end if;  -- 22022019 END

	IF tipo_op_cambi.count = 0 THEN  load_cambi; END IF; -- 12032019  BEGIN
		--IL PCT_ACQ_TRANSID viene usato solo dai PCT. Inoltre, ho verificato che per alcuni altri titoli ci sono degli eventi che, a paritÿ?ÿ  di titoli, a volve
		--scendono null e a volte a 000000000000000000.
    imp_netto      := cEventoTitoli.IMP_NETTO;
    imp_lordo      := cEventoTitoli.IMP_LORDO;
    -- OPERZTIT_Q_QTA := 0; -- 16032019

	IF NOT
		(
			cEventoTitoli.OPERZTIT_N_PCT IN (1,2)
		)
		THEN
		cEventoTitoli.PCT_ACQ_TRANSID := '0';

	END IF;
		--Fondi terzi in giacenza da non considerare mai

		IF cEventoTitoli.AF_TPPOL_TIT = '07' and NOT (is_real_estate_list.EXISTS(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN)) THEN 

			RETURN;

END IF;
--identificativo univoco del prodotto - Accodato anche il 'codicePeriodo', necessario se processiamo i Cruscotti (e' null se non processiamo i cruscotti)
contractKey := trim(cEventoTitoli.CONTR_N)||'-'|| trim(cEventoTitoli.PROD_C)||'-'|| trim(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN)||'-'|| trim(cEventoTitoli.ANAATTIVFIN_X_INDICT_EMISS||'-'|| trim(cEventoTitoli.PCT_ACQ_TRANSID)||'-'|| trim(codicePeriodo)) ;

IF in_cTotaliTitoli.EXISTS(contractKey) THEN
	contractIndex := in_cTotaliTitoli(contractKey);

ELSE
	cTotaliTitoli.extend();
	in_cTotaliTitoli(contractKey) := cTotaliTitoli.last;
	contractIndex                 := cTotaliTitoli.last;
	cTotaliTitoli(contractIndex)  := SOGG_OUTELAB.RT_TOTALS_TIT( cEventoTitoli.PROD_C, cEventoTitoli.CONTR_N, cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN, cEventoTitoli.ANAATTIVFIN_X_INDICT_EMISS, cEventoTitoli.PCT_ACQ_TRANSID, NULL );

END IF;
-- Setting BI_ISBOND

IF tipo_bi_list_tit.EXISTS(cEventoTitoli.ANAATTIVFIN_C) THEN
	bi_isbond := 'S';

ELSE
	bi_isbond := 'N';

END IF;
-------SEZIONE DI CLASSIFICAZIONE PRODOTTI - START --------

IF is_real_estate_list.EXISTS(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN) THEN
	cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA := 'FNDIM';
	isRealEstate                                     := true;

ELSIF (cEventoTitoli.OPERZTIT_N_PCT IN (1,2)) THEN
	cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA := 'PCT';
	isPct                                            := true;

elsif cEventoTitoli.AF_COLLBANCA IN ('E','I') AND bi_isbond<>'S' THEN
	cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA          := 'CERT';
	isCert                                                    := true;

elsif cEventoTitoli.AF_TPPOL_TIT IN ('01', '02', '03') THEN

	IF bi_isbond                                       ='S' THEN
		cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA := 'MPB';
		isMpb                                            := true;

	ELSE
		cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA := 'OBBLI';
		isObblig                                         := true;

	END IF;

elsif cEventoTitoli.AF_TPPOL_TIT                   = '04' THEN
	cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA := 'AZION';
	isAzion                                          := true;

END IF;
-------SEZIONE DI CLASSIFICAZIONE PRODOTTI - END --------
-- setting t1 so as to exclude FNDIM from cut-off 2013
IF cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA = 'FNDIM' THEN
	t1                                                := COALESCE (p_t1, to_date('17530101','YYYYMMDD'));
ELSE
	t1 := COALESCE (p_t1, to_date('20130101','YYYYMMDD'));

END IF;


IF cEventoTitoli.DAT_RIF>=t1 AND cEventoTitoli.DAT_RIF<=t2 THEN
	isNelPeriodo           := true;

END IF;
--il segno dell'operazione. Il valore non e' quello della tabella ma e' un valore normalizzato con una decode all'interno della query
segno := cEventoTitoli.SEGNO;
-------SETTAGGI COMUNI A TUTTI I PRODOTTI - START --------

--TASSO DI CAMBIO
tasso_cambio := 0;

IF cEventoTitoli.OPERZTIT_C_DIV ='ITL' THEN
	tasso_cambio                  := 1/1936.27;

elsif cEventoTitoli.OPERZTIT_C_DIV = 'EUR' THEN
	tasso_cambio                     := 1;
elsif tipo_op_cambi.EXISTS(cEventoTitoli.OPERZTIT_C_DIV) then
	tasso_cambio                     := 1/tipo_op_cambi(cEventoTitoli.OPERZTIT_C_DIV);
ELSE

	IF cEventoTitoli.OPERZTIT_I_CAMBIO = 0 THEN
		tasso_cambio                     := 0;

	ELSE
		tasso_cambio := 1/cEventoTitoli.OPERZTIT_I_CAMBIO;

	END IF;

END IF;


-- 22022019 DA TOGLIERE PER ATTITIVA DINAMICIZZAZIONE CAUSALI
--IF tipo_op_cedola_list_tit.EXISTS(cEventoTitoli.operztit_c_caus) THEN
--	imp_cedole_tit := ABS(cEventoTitoli.operztit_i_ctrv);
--    segno          :=0;

--END IF;

--CEDOLE PER RE
IF tipo_op_cedola_list_re.EXISTS(cEventoTitoli.operztit_c_caus) THEN
	imp_cedole_re := ABS(cEventoTitoli.operztit_i_ctrv)*tasso_cambio;

	segno         :=0;

END IF;

--defect 52146: Gli eventi di CUTOFF non devono essere considerati per i Fondi Immobiliari
IF cEventoTitoli.OPERZTIT_C_CAUS = 'CUTOFF' AND isRealEstate THEN
    RETURN;
END IF;


-- defect 43279
-- per l'evento CUTOFF la cEventoTitoli.IMP_NETTO = posiztit_iniz_mwrr.posiztit_q_tit * QUOTAZ_I_VAL_MERCATO (= cEventoTitoli.IMP_LORDO)
-- ma per le OBBLIGAZIONI, i CERTIFICATES e le OBBLIGAZIONI STRUTTURATE le quote presenti nella posiztit_q_tit sono moltiplicate per 100.
--NOTA: viene utilizzata una variabile di appoggio invece che effettuare, come si faceva precedentemente,
--		cEventoTitoli.IMP_NETTO     := cEventoTitoli.IMP_NETTO/100;
--      cEventoTitoli.IMP_LORDO     := cEventoTitoli.IMP_LORDO/100;
--In quanto questo, nelle chiamate a ripetizione alla calc_totali_titoli nei cruscotti, determinava che ad ogni chiamata successiva
--della calc_totali_titoli veniva passato un evento che aveva un imp_netto sempre maggiore.
IF cEventoTitoli.OPERZTIT_C_CAUS = 'CUTOFF' AND cEventoTitoli.AF_ESPPREZ != 'L' AND cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA IN ('OBBLI','MPB','CERT') THEN
		IMP_NETTO     := cEventoTitoli.IMP_NETTO/100;
        IMP_LORDO     := cEventoTitoli.IMP_LORDO/100;
        OPERZTIT_I_IMP := cEventoTitoli.OPERZTIT_I_IMP/100;
        OPERZTIT_I_CTRV := cEventoTitoli.OPERZTIT_I_CTRV/100;

END IF;

/*  x def. 57166
--IF cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA IN ('CERT','OBBLI','MPB') and cEventoTitoli.AF_ESPPREZ != 'L' THEN
		-- per OBBLI, CERT e MPB le quote memorizzate in tabella sono moltiplicate per 100, quindi:
--        OPERZTIT_Q_QTA     := cEventoTitoli.OPERZTIT_Q_QTA/100;
--END IF;
*/


-- setting correct IMP_LORDO

IF IMP_LORDO = 0 AND IMP_NETTO > 0 THEN
	IMP_LORDO  := cEventoTitoli.operztit_i_spese + cEventoTitoli.operztit_i_comms + cEventoTitoli.operztit_i_boll + IMP_NETTO;

ELSIF IMP_LORDO > 0 THEN
	IMP_LORDO     := IMP_LORDO;

ELSE
	IMP_LORDO := 0;

END IF;

--se il segno e' positivo, sono soldi che entrano nel prodotto. Altrimenti, sono
--soldi che escono e sono dei rimborsi
	imp_cedole_tit 	:= 0;  -- Inizializzo le variabili, prima di applicare le regole
	imp_versato 	:= 0;
	imp_investito   := 0;
	imp_rimborsato  := 0;
	quantita		:= 0;
if(regola = 999) then
	imp_versato 	:= IMP_NETTO; -- per il cutoff lascio la vecchia logica
	imp_investito 	:= IMP_NETTO; -- per il cutoff lascio la vecchia logica
	quantita		:= OPERZTIT_Q_QTA;
elsif regola = 1 then
	-- Versamenti e aumenti di capitale a pagamento
	imp_versato 	:= 0-OPERZTIT_I_IMP;  -- E' negativo in tabella quindi qui diventa positivo
	imp_investito   := 0-(OPERZTIT_I_IMP + cEventoTitoli.OPERZTIT_I_SPESE + cEventoTitoli.OPERZTIT_I_COMMS);
	quantita		:= OPERZTIT_Q_QTA;
elsif regola = 2 then
	-- Dividendi storni
	imp_cedole_tit 	:= 0-OPERZTIT_I_CTRV;
	imp_cedole_re   := imp_cedole_tit;
elsif regola = 3 then
	-- Sottoscrizione
--	if(cEventoTitoli.OPERZTIT_C_DIV_REG = 'EUR') then cEventoTitoli.OPERZTIT_I_CAMBIO := 1;
--	elsif (tipo_op_cambi.EXISTS(cEventoTitoli.OPERZTIT_C_DIV_REG)) then cEventoTitoli.OPERZTIT_I_CAMBIO := tipo_op_cambi(cEventoTitoli.OPERZTIT_C_DIV_REG);
--	end if;
	imp_versato 	:= 0-OPERZTIT_I_IMP  ;
	imp_investito   := 0-(OPERZTIT_I_IMP  + cEventoTitoli.OPERZTIT_I_SPESE + cEventoTitoli.OPERZTIT_I_COMMS);
	quantita		:= OPERZTIT_Q_QTA;
elsif regola = 4 then
	-- POOL FACTOR
	imp_rimborsato  := OPERZTIT_I_CTRV;
elsif regola = 5 then
	-- Dividendi
	imp_cedole_tit 	:= OPERZTIT_I_CTRV;
	imp_cedole_re   := imp_cedole_tit;
elsif regola = 6 then
	-- Compensi titoli
	imp_rimborsato  := OPERZTIT_I_CTRV - cEventoTitoli.OPERZTIT_I_SPESE - cEventoTitoli.OPERZTIT_I_COMMS;
	quantita		:= 0-OPERZTIT_Q_QTA;
elsif regola = 7 then
	--Prelievi e trasferimenti
	if(cEventoTitoli.OPERZTIT_C_DIV = 'EUR') then cEventoTitoli.OPERZTIT_I_CAMBIO := 1;
	elsif (tipo_op_cambi.EXISTS(cEventoTitoli.OPERZTIT_C_DIV)) then cEventoTitoli.OPERZTIT_I_CAMBIO := tipo_op_cambi(cEventoTitoli.OPERZTIT_C_DIV);
	elsif (cEventoTitoli.OPERZTIT_I_CAMBIO = 0) then cEventoTitoli.OPERZTIT_I_CAMBIO := 1;
	end if;
	imp_rimborsato  := OPERZTIT_I_CTRV/cEventoTitoli.OPERZTIT_I_CAMBIO -  cEventoTitoli.OPERZTIT_I_SPESE - cEventoTitoli.OPERZTIT_I_COMMS;
	quantita		:= 0-OPERZTIT_Q_QTA;
elsif regola = 8 then
	-- Per espansione futura, solo increase quantita
	quantita		:= OPERZTIT_Q_QTA;
elsif regola = 9 then
	-- Per espansione futura, solo decrease quantita
	quantita		:= 0-OPERZTIT_Q_QTA;
elsif regola = 10 then
    imp_versato     := OPERZTIT_I_CTRV;
    imp_investito   := OPERZTIT_I_CTRV - cEventoTitoli.OPERZTIT_I_SPESE - cEventoTitoli.OPERZTIT_I_COMMS;
    quantita		:= OPERZTIT_Q_QTA;
elsif regola = 11 then
	-- Versamenti e aumenti di capitale a pagamento (copia della 1 ma considerando il cambio)
	if(cEventoTitoli.OPERZTIT_C_DIV_REG = 'EUR') then cEventoTitoli.OPERZTIT_I_CAMBIO := 1;	
	elsif (cEventoTitoli.OPERZTIT_I_CAMBIO = 0) then cEventoTitoli.OPERZTIT_I_CAMBIO := 1;
	end if;
	imp_versato 	:= 0-(OPERZTIT_I_IMP/cEventoTitoli.OPERZTIT_I_CAMBIO);  -- E' negativo in tabella quindi qui diventa positivo
	imp_investito   := 0-((OPERZTIT_I_IMP/cEventoTitoli.OPERZTIT_I_CAMBIO) + cEventoTitoli.OPERZTIT_I_SPESE + cEventoTitoli.OPERZTIT_I_COMMS);
	quantita		:= OPERZTIT_Q_QTA;
elsif regola = 12 then
	-- Versamenti e aumenti di capitale a pagamento (copia della 11 ma considerando il cambio solo per i movimenti post 19/10/2017)
	if(cEventoTitoli.OPERZTIT_C_DIV_REG = 'EUR' or cEventoTitoli.OPERZTIT_I_CAMBIO = 0 or cEventoTitoli.OPERZTIT_D_REGIST < to_date('19102017', 'ddmmyyyy') ) then 
		cEventoTitoli.OPERZTIT_I_CAMBIO := 1;	 
	end if;	
	imp_versato 	:= 0-(OPERZTIT_I_IMP/cEventoTitoli.OPERZTIT_I_CAMBIO);  -- E' negativo in tabella quindi qui diventa positivo
	imp_investito   := 0-((OPERZTIT_I_IMP/cEventoTitoli.OPERZTIT_I_CAMBIO) + cEventoTitoli.OPERZTIT_I_SPESE + cEventoTitoli.OPERZTIT_I_COMMS);
	quantita		:= OPERZTIT_Q_QTA;
elsif regola = 71 then
	--Prelievi e trasferimenti, copia della 7 ma applicando il cambio solo per i movimenti post 19/10/2017
	if(cEventoTitoli.OPERZTIT_C_DIV = 'EUR' or cEventoTitoli.OPERZTIT_I_CAMBIO = 0 or cEventoTitoli.OPERZTIT_D_REGIST < to_date('19102017', 'ddmmyyyy')) then 
		cEventoTitoli.OPERZTIT_I_CAMBIO := 1;
	elsif (tipo_op_cambi.EXISTS(cEventoTitoli.OPERZTIT_C_DIV)) then 
		cEventoTitoli.OPERZTIT_I_CAMBIO := tipo_op_cambi(cEventoTitoli.OPERZTIT_C_DIV);	
	end if;
	imp_rimborsato  := OPERZTIT_I_CTRV/cEventoTitoli.OPERZTIT_I_CAMBIO -  cEventoTitoli.OPERZTIT_I_SPESE - cEventoTitoli.OPERZTIT_I_COMMS;
	quantita		:= 0-OPERZTIT_Q_QTA;
end if;

/*
IF segno        > 0 THEN
	imp_versato   := ABS(cEventoTitoli.IMP_NETTO) + cEventoTitoli.operztit_i_spese + cEventoTitoli.operztit_i_comms; --imp_versato   := ABS(cEventoTitoli.IMP_LORDO);
	imp_investito := ABS(cEventoTitoli.IMP_NETTO);

ELSIF segno      < 0 THEN
	imp_rimborsato := ABS(cEventoTitoli.IMP_NETTO) - cEventoTitoli.operztit_i_spese - cEventoTitoli.operztit_i_comms;

END IF;
*/

IF cTotaliTitoli(contractIndex).DAT_APER_CONTR IS NULL THEN
	Ctotalititoli(Contractindex).DAT_APER_CONTR   := Ceventotitoli.DAT_RIF;

END IF;

-- variabile utilizzata per determinare IMP_CONT_VAL_T1 e IMP_CONT_VAL_T2 nella calc_redd_periodo(  per i 'CERT', 'OBBLI' e 'MPB'
	-- se EV_AF_ESPPREZ<> 'L' la QTA_QUOTA_T1 e T2 viene divisa per cento nel calcolo dei relativi Controvalori
	cTotaliTitoli(contractIndex).EV_AF_ESPPREZ    := cEventoTitoli.AF_ESPPREZ;


-------SETTAGGI COMUNI A TUTTI I PRODOTTI - START --------

IF isNelPeriodo THEN
	------------SEZIONE COMUNE - START ------------------
	-- setting the date for holes-timeperiod calculation in case qta is zero

	IF tb_qta_holes.exists(contractKey) THEN
		qta_saldo_hole_t1 := tb_qta_holes(contractKey);
		--tb_qta_holes.delete();

	END IF;


	IF cTotaliTitoli(contractIndex).DAT_PRIMO_MOVI IS NULL THEN
		cTotaliTitoli(contractIndex).DAT_PRIMO_MOVI   := cEventoTitoli.DAT_RIF;
		cTotaliTitoli(contractIndex).QTA_SALDO_GIORNL_QUOTA_ZERO := 0; -- inizializzazione dei giorni a zero per il primo movimento del periodo
		tb_qta_holes(contractKey) := NULL;
		qta_saldo_hole_t1 := null;

	ELSE
		deltaT                                      := cEventoTitoli.DAT_RIF                       -cTotaliTitoli(contractIndex).DAT_ULT_MOVI;
		-- al calcolo dellintegrale non deve contribuire l'intervallo di tempo in cui un titolo e' a quota zero, quindi in tal caso azzeriamo deltaT
		IF qta_saldo_hole_t1  is not null THEN
            deltaT := 0;
        END IF;
		cTotaliTitoli(contractIndex).IMP_INT_VERS   := cTotaliTitoli(contractIndex).IMP_INT_VERS   + cTotaliTitoli(contractIndex).IMP_SALDO_VERS * nvl(deltaT,0);
		cTotaliTitoli(contractIndex).IMP_INT_INVEST := cTotaliTitoli(contractIndex).IMP_INT_INVEST + cTotaliTitoli(contractIndex).IMP_SALDO_INVEST * nvl(deltaT,0);

	END IF;

	------------SEZIONE COMUNE - END ------------------
	------------SEZIONE REAL ESTATE - START ------------------

	IF isRealEstate THEN
		cTotaliTitoli(contractIndex).IMP_TOT_VERS     := cTotaliTitoli(contractIndex).IMP_TOT_VERS     + imp_versato;
		cTotaliTitoli(contractIndex).IMP_TOT_INVEST   := cTotaliTitoli(contractIndex).IMP_TOT_INVEST   + imp_investito;
		cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    + imp_rimborsato;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   - imp_cedole_re ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST - imp_cedole_re ;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   + imp_versato - imp_rimborsato ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + imp_investito - imp_rimborsato ;

		/*IF tipo_op_cedola_list_re.EXISTS(cEventoTitoli.operztit_c_caus) THEN
			cTotaliTitoli(contractIndex).IMP_PRVNT_PASS := cTotaliTitoli(contractIndex).IMP_PRVNT_PASS + cEventoTitoli.operztit_i_ctrv*tasso_cambio;
 		END IF;*/
		cTotaliTitoli(contractIndex).IMP_PRVNT_PASS := cTotaliTitoli(contractIndex).IMP_PRVNT_PASS + imp_cedole_tit; -- 22022019
		
		--verificaCambioIsinRE(in_cTotaliTitoli, cTotaliTitoli, cEventoTitoli, contractKey, contractIndex);
		--la verificaCambioIsinRE viene portata fuori dal periodico per intercettare fusioni verificatesi PRIMA di t1
	

		------------SEZIONE REAL ESTATE - END ------------------
	------------SEZIONE PCT - START ------------------

	elsif isPct THEN
       IF cEventoTitoli.OPERZTIT_N_PCT                    = 1 THEN
                cTotaliTitoli(contractIndex).EV_PCT_1    :=1;               
        ELSIF cEventoTitoli.OPERZTIT_N_PCT                    = 2 THEN
                cTotaliTitoli(contractIndex).EV_PCT_2    :=1;                 
        END IF;
/*		cTotaliTitoli(contractIndex).COD_TRANS_ACQU_PCT := trim(SUBSTR(cEventoTitoli.PCT_ACQ_TRANSID,1,25));

		IF cEventoTitoli.OPERZTIT_N_PCT                    = 1 THEN
			cTotaliTitoli(contractIndex).IMP_TOT_VERS        := cTotaliTitoli(contractIndex).IMP_TOT_VERS + imp_versato; -- 22022019
			cTotaliTitoli(contractIndex).IMP_TOT_INVEST      := cTotaliTitoli(contractIndex).IMP_TOT_VERS;
			cTotaliTitoli(contractIndex).DAT_APER_CONTR      := cEventoTitoli.OPERZTIT_D_VLT;
			Ctotalititoli(Contractindex).Pct_Num_Transazioni := Ctotalititoli(Contractindex).Pct_Num_Transazioni + 1;
			cTotaliTitoli(contractIndex).IMP_SALDO_VERS      := cTotaliTitoli(contractIndex).IMP_SALDO_VERS + cTotaliTitoli(contractIndex).IMP_TOT_VERS;
            cTotaliTitoli(contractIndex).IMP_SALDO_INVEST    := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + cTotaliTitoli(contractIndex).IMP_TOT_INVEST;
		END IF;

		IF cEventoTitoli.OPERZTIT_N_PCT               = 2 THEN
			contrval_pct                                := ROUND(ABS(cEventoTitoli.operztit_i_imp)              *tasso_cambio, 2);
			cTotaliTitoli(contractIndex).IMP_CONTVAL    := COALESCE(cTotaliTitoli(contractIndex).IMP_CONTVAL,0) + contrval_pct;
			cTotaliTitoli(contractIndex).DAT_CHIU_CONTR := cEventoTitoli.OPERZTIT_D_VLT;

			IF cEventoTitoli.operztit_d_vlt              < TRUNC(CURRENT_DATE) THEN
				cTotaliTitoli(contractIndex).IMP_TOT_RIMBO := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO + imp_rimborsato; 


			END IF;
			cTotaliTitoli(contractIndex).IMP_SALDO_VERS      := cTotaliTitoli(contractIndex).IMP_SALDO_VERS - cTotaliTitoli(contractIndex).IMP_TOT_RIMBO;
			cTotaliTitoli(contractIndex).IMP_SALDO_INVEST    := cTotaliTitoli(contractIndex).IMP_SALDO_VERS;
			Ctotalititoli(Contractindex).Pct_Num_Transazioni := Ctotalititoli(Contractindex).Pct_Num_Transazioni + 1;


		END IF;
*/		------------SEZIONE PCT - END ------------------
		------------SEZIONE CERTIFICATES - START -------

	elsif isCert THEN		
		cTotaliTitoli(contractIndex).DAT_APER_CONTR := LEAST(COALESCE(cTotaliTitoli(contractIndex).DAT_APER_CONTR,to_date('01-01-9999','dd-mm-yyyy')), cEventoTitoli.OPERZTIT_D_VLT);
		cTotaliTitoli(contractIndex).IMP_PRVNT_PASS := cTotaliTitoli(contractIndex).IMP_PRVNT_PASS + imp_cedole_tit; -- 22022019
		cTotaliTitoli(contractIndex).IMP_TOT_VERS 	:= cTotaliTitoli(contractIndex).IMP_TOT_VERS + imp_versato; -- 22022019
		cTotaliTitoli(contractIndex).IMP_TOT_RIMBO 	:= cTotaliTitoli(contractIndex).IMP_TOT_RIMBO + imp_rimborsato; -- 22022019

		cTotaliTitoli(contractIndex).IMP_TOT_INVEST   := cTotaliTitoli(contractIndex).IMP_TOT_VERS;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   + imp_versato - imp_rimborsato ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + imp_investito - imp_rimborsato ;
		------------SEZIONE CERTIFICATES - END ---------------
		------------SEZIONE OBBLIGAZIONI - START -------------

	elsif isObblig THEN

		cTotaliTitoli(contractIndex).DAT_APER_CONTR := LEAST(COALESCE(cTotaliTitoli(contractIndex).DAT_APER_CONTR,to_date('01-01-9999','dd-mm-yyyy')), cEventoTitoli.OPERZTIT_D_VLT);
		cTotaliTitoli(contractIndex).IMP_PRVNT_PASS := cTotaliTitoli(contractIndex).IMP_PRVNT_PASS + imp_cedole_tit; -- 22022019
		cTotaliTitoli(contractIndex).IMP_TOT_VERS     := cTotaliTitoli(contractIndex).IMP_TOT_VERS     + imp_versato;
		cTotaliTitoli(contractIndex).IMP_TOT_INVEST   := cTotaliTitoli(contractIndex).IMP_TOT_INVEST   + imp_investito;
		cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    + imp_rimborsato;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   + imp_versato - imp_rimborsato ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + imp_investito - imp_rimborsato ;	

		------------SEZIONE OBBLIGAZIONI - END -------------
		------------SEZIONE MPB - START --------------------

	elsif isMpb THEN

		cTotaliTitoli(contractIndex).DAT_APER_CONTR := LEAST(COALESCE(cTotaliTitoli(contractIndex).DAT_APER_CONTR,to_date('01-01-9999','dd-mm-yyyy')), cEventoTitoli.OPERZTIT_D_VLT);
		cTotaliTitoli(contractIndex).IMP_PRVNT_PASS := cTotaliTitoli(contractIndex).IMP_PRVNT_PASS + imp_cedole_tit; -- 22022019
		cTotaliTitoli(contractIndex).IMP_TOT_VERS     := cTotaliTitoli(contractIndex).IMP_TOT_VERS   + imp_versato;
		cTotaliTitoli(contractIndex).IMP_TOT_INVEST   := cTotaliTitoli(contractIndex).IMP_TOT_INVEST + imp_investito;
		cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO  + imp_rimborsato; -- in precedenza per MPB i rimborsi non venivano considerati
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   + imp_versato - imp_rimborsato ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + imp_investito - imp_rimborsato ;

		------------SEZIONE MPB - END --------------------
		------------SEZIONE AZIONI - START ---------------

	elsif isAzion THEN
		cTotaliTitoli(contractIndex).DAT_APER_CONTR := LEAST(COALESCE(cTotaliTitoli(contractIndex).DAT_APER_CONTR,to_date('01-01-9999','dd-mm-yyyy')), cEventoTitoli.OPERZTIT_D_VLT);

		cTotaliTitoli(contractIndex).IMP_PRVNT_PASS := cTotaliTitoli(contractIndex).IMP_PRVNT_PASS + imp_cedole_tit; -- 22022019
		cTotaliTitoli(contractIndex).IMP_TOT_VERS     := cTotaliTitoli(contractIndex).IMP_TOT_VERS     + imp_versato;
		cTotaliTitoli(contractIndex).IMP_TOT_INVEST   := cTotaliTitoli(contractIndex).IMP_TOT_INVEST   + imp_investito;
		cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    + imp_rimborsato;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   + imp_versato - imp_rimborsato ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + imp_investito - imp_rimborsato ;
		------------SEZIONE AZIONI - END ---------------

	elsif isFondiTerzi THEN
		cTotaliTitoli(contractIndex).DAT_APER_CONTR := LEAST(COALESCE(cTotaliTitoli(contractIndex).DAT_APER_CONTR,to_date('01-01-9999','dd-mm-yyyy')), cEventoTitoli.OPERZTIT_D_VLT);
		
		IF tipo_op_cedola_list_tit.EXISTS(cEventoTitoli.operztit_c_caus) THEN
			cTotaliTitoli(contractIndex).IMP_PRVNT_PASS := cTotaliTitoli(contractIndex).IMP_PRVNT_PASS + operztit_i_ctrv*tasso_cambio;

		END IF;
		cTotaliTitoli(contractIndex).IMP_TOT_VERS     := cTotaliTitoli(contractIndex).IMP_TOT_VERS     + imp_versato;
		cTotaliTitoli(contractIndex).IMP_TOT_INVEST   := cTotaliTitoli(contractIndex).IMP_TOT_INVEST   + imp_investito;
		cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    + imp_rimborsato;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST - imp_cedole_tit ;
		cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   + imp_versato - imp_rimborsato ;
		cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + imp_investito - imp_rimborsato ;


	END IF;
	if  cTotaliTitoli(contractIndex).QTA_QUOTA_T2 <> 0 then
            cTotaliTitoli(contractIndex).DAT_ULT_MOVI := cEventoTitoli.DAT_RIF; 

    elsif cTotaliTitoli(contractIndex).QTA_QUOTA_T2 =0 then
            if regola not in ('2', '5') then
                cTotaliTitoli(contractIndex).DAT_ULT_MOVI := cEventoTitoli.DAT_RIF;  
            end if;
    end if;


END IF;  ---isNelPeriodo


--la verificaCambioIsinRE viene portata fuori dal periodico per intercettare fusioni verificatesi PRIMA di t1
IF cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA ='FNDIM'  and cEventoTitoli.DAT_RIF <= t2 THEN
	verificaCambioIsinRE(in_cTotaliTitoli, cTotaliTitoli, cEventoTitoli, contractKey, contractIndex);
end if;

IF cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA IN ('CERT','OBBLI','MPB','AZION') THEN

		IF cEventoTitoli.DAT_RIF                   <t1 THEN
		cTotaliTitoli(contractIndex).QTA_QUOTA_T1 := cTotaliTitoli(contractIndex).QTA_QUOTA_T1 + quantita; -- 22022019
		END IF;

		IF cEventoTitoli.DAT_RIF                   <=t2 THEN
		cTotaliTitoli(contractIndex).QTA_QUOTA_T2 := cTotaliTitoli(contractIndex).QTA_QUOTA_T2 + quantita; -- 22022019



	END IF;

END IF;

IF cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA IN ('FNDIM') THEN
-- si escludono i movimenti di tipo cedola ('CEDD', 'DIVD', 'XH', 'XQ', 'XY') e anche (rfc 110777) cambio ISIN ('XZ') dal calcolo delle quote:
	IF NOT tipo_op_cedola_list_re.EXISTS(cEventoTitoli.operztit_c_caus) and cEventoTitoli.operztit_c_caus <> 'XZ' THEN

		IF cEventoTitoli.DAT_RIF                   <=t2 THEN
			cTotaliTitoli(contractIndex).QTA_QUOTA_T2 := cTotaliTitoli(contractIndex).QTA_QUOTA_T2 + (OPERZTIT_Q_QTA * segno);

		END IF;

		IF cEventoTitoli.DAT_RIF                   <=t1 THEN
			cTotaliTitoli(contractIndex).QTA_QUOTA_T1 := cTotaliTitoli(contractIndex).QTA_QUOTA_T1 + (OPERZTIT_Q_QTA * segno);

		END IF;

	END IF;

END IF;

--- Valorizzazione degli importi del PCT a prescindere dal periodo di elaborazione
IF isPct THEN
       cTotaliTitoli(contractIndex).COD_TRANS_ACQU_PCT := trim(SUBSTR(cEventoTitoli.PCT_ACQ_TRANSID,1,25));

        IF cEventoTitoli.OPERZTIT_N_PCT                    = 1   THEN
                cTotaliTitoli(contractIndex).IMP_TOT_VERS        := cTotaliTitoli(contractIndex).IMP_TOT_VERS  + imp_versato;
                cTotaliTitoli(contractIndex).IMP_TOT_INVEST      := cTotaliTitoli(contractIndex).IMP_TOT_INVEST + imp_investito;
                cTotaliTitoli(contractIndex).DAT_APER_CONTR      := cEventoTitoli.OPERZTIT_D_VLT;
--20190521      cTotaliTitoli(contractIndex).IMP_SALDO_VERS      := cTotaliTitoli(contractIndex).IMP_SALDO_VERS + cTotaliTitoli(contractIndex).IMP_TOT_VERS;
--20190521      cTotaliTitoli(contractIndex).IMP_SALDO_INVEST    := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST + cTotaliTitoli(contractIndex).IMP_TOT_INVEST;
                Ctotalititoli(Contractindex).Pct_Num_Transazioni := Ctotalititoli(Contractindex).Pct_Num_Transazioni + 1;
                cTotaliTitoli(contractIndex).DAT_EV_PCT_1        :=cEventoTitoli.DAT_RIF;
        END IF;

        IF cEventoTitoli.OPERZTIT_N_PCT               = 2   THEN
                contrval_pct                                := ROUND(ABS(cEventoTitoli.operztit_i_imp)              *tasso_cambio, 2);
                cTotaliTitoli(contractIndex).IMP_CONTVAL    := COALESCE(cTotaliTitoli(contractIndex).IMP_CONTVAL,0) + contrval_pct;
                cTotaliTitoli(contractIndex).DAT_CHIU_CONTR := cEventoTitoli.OPERZTIT_D_VLT;
                -- condizione necessaria per recuperare il rimborsato anche dei PCT che risultano aperti in un periodico
				--IF cEventoTitoli.operztit_d_vlt              < TRUNC(CURRENT_DATE) THEN
                    cTotaliTitoli(contractIndex).IMP_TOT_RIMBO := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO + imp_rimborsato;
                --END IF;
--20190521      cTotaliTitoli(contractIndex).IMP_SALDO_VERS      := cTotaliTitoli(contractIndex).IMP_SALDO_VERS - cTotaliTitoli(contractIndex).IMP_TOT_RIMBO;
--20190521      cTotaliTitoli(contractIndex).IMP_SALDO_INVEST    := cTotaliTitoli(contractIndex).IMP_SALDO_VERS;
                Ctotalititoli(Contractindex).Pct_Num_Transazioni := Ctotalititoli(Contractindex).Pct_Num_Transazioni + 1;
                cTotaliTitoli(contractIndex).DAT_EV_PCT_2        :=cEventoTitoli.DAT_RIF;
        END IF;

--
         IF cTotaliTitoli(contractIndex).EV_PCT_1 = 1 then
 -- CASO 1)  T1 <= Ta e T2 >= Ts      is nel period - visualizzazione analoga al daily
--Il periodo di analisi selezionato comprende tutti i movimenti del contratto, il PCT mostra gli stessi importi visualizzabili senza la applicazione del filtro
--Il campo 'Capitale Iniziale' contiene il valore zero '0'.
           if cTotaliTitoli(contractIndex).EV_PCT_2 = 1 then -----Caso 1)
				--dbms_output.put_line(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN||' - Caso1');
				cTotaliTitoli(contractIndex).IMP_CONTVAL_T1 := 0;
--20190524  	cTotaliTitoli(contractIndex).IMP_CONTVAL_T2 :=cTotaliTitoli(contractIndex).IMP_TOT_RIMBO; -- richiesto dal FE di CR
				cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_VERS_PCT := ROUND(cTotaliTitoli(contractIndex).IMP_TOT_RIMBO-cTotaliTitoli(contractIndex).IMP_TOT_VERS, 2);
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_INVEST_PCT := ROUND(cTotaliTitoli(contractIndex).IMP_TOT_RIMBO-cTotaliTitoli(contractIndex).IMP_TOT_INVEST, 2);
				-- per elaborazione cruscotti i rimborsi non si annullano
--20190524  	IF codicePeriodo IS NOT NULL THEN       -- stiamo elaborando i cruscotti
--20190524  	    cTotaliTitoli(contractIndex).IMP_CONTVAL_T2 := 0; --perche' nel periodo t1,t2 e' stato sia aperto che chiuso
--20190524  	ELSE
--20190524  		cTotaliTitoli(contractIndex).IMP_TOT_RIMBO    := 0; -- richiesto dal FE di CR
--20190524  	END IF;
				cTotaliTitoli(contractIndex).IMP_SALDO_VERS      := cTotaliTitoli(contractIndex).IMP_SALDO_VERS   - cTotaliTitoli(contractIndex).IMP_TOT_RIMBO; --20190521    
				cTotaliTitoli(contractIndex).IMP_SALDO_INVEST    := cTotaliTitoli(contractIndex).IMP_SALDO_INVEST - cTotaliTitoli(contractIndex).IMP_TOT_RIMBO; --20190521    
--CASO 6)	T1 < Ta < T2 < Ts      Il periodo di analisi comprende solo il movimento di acquisto del PCT
--In questo caso il PCT mostreraÂ  gli stessi importi visualizzabili senza applicazione di filtro, ad eccezione del capitale iniziale che e' '0'
--Capitale Versato (Valore di Acquisto) valorizzato come nel caso di PCT aperto senza applicazione di filtro di periodo, con importo versato dal cliente alla sottoscrizione del contratto;
--Capitale a Scadenza valorizzato come nel caso di PCT aperto senza applicazione di filtro di periodo, con importo dovuto al cliente alla scadenza del contratto;
            elsif cTotaliTitoli(contractIndex).EV_PCT_2 = 0 then  --caso 6)
				--dbms_output.put_line(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN||' - Caso6 ');
                cTotaliTitoli(contractIndex).IMP_CONTVAL_T1 := 0;
                cTotaliTitoli(contractIndex).IMP_CONTVAL_T2 :=cTotaliTitoli(contractIndex).IMP_TOT_RIMBO; -- richiesto dal FE di CR
				--cTotaliTitoli(contractIndex).IMP_CONTVAL_T2 :=cTotaliTitoli(contractIndex).IMP_CONTVAL; -- richiesto dal FE di CR  modificato in contval 20042019
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_VERS_PCT := ROUND(cTotaliTitoli(contractIndex).IMP_TOT_RIMBO - cTotaliTitoli(contractIndex).IMP_TOT_VERS, 2); 
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_INVEST_PCT := ROUND(cTotaliTitoli(contractIndex).IMP_TOT_RIMBO - cTotaliTitoli(contractIndex).IMP_TOT_INVEST, 2); --20190308
                IF codicePeriodo IS NULL THEN       --non stiamo elaborando i cruscotti
                    cTotaliTitoli(contractIndex).IMP_TOT_RIMBO  := 0; -- richiesto dal FE di CR
				--ELSE
					-- 20042019 Siamo nel cruscotto, non ho a disposizione il ctv a T2 quindi lo metto nei rimborsi
					--cTotaliTitoli(contractIndex).IMP_TOT_RIMBO  :=  nvl(cTotaliTitoli(contractIndex).IMP_CONTVAL_T2,0); --25042019, aggiunto nvl altrimenti il null poi invalida i calcoli
                END IF;
                
                cTotaliTitoli(contractIndex).IMP_SALDO_VERS      := cTotaliTitoli(contractIndex).IMP_TOT_VERS;  --20190521  
				cTotaliTitoli(contractIndex).IMP_SALDO_INVEST    := cTotaliTitoli(contractIndex).IMP_TOT_VERS;  --20190521  
                
            end if;

        ELSE            --(quando EV_PCT_1 = 0)

--CASO 4)   Ta < T1 < Ts <= T2     Il periodo di analisi comprende solo il movimento di rimborso del PCT
--Capitale Iniziale pari a importo versato in fase di sottoscrizione;
--Capitale Versato (Valore di Acquisto) pari a zero;
--Capitale a Scadenza valorizzato come nel caso di PCT chiuso senza applicazione di filtro di periodo, con importo dovuto al cliente alla scadenza del contratto;
            if cTotaliTitoli(contractIndex).EV_PCT_2 = 1 and t1<cTotaliTitoli(contractIndex).DAT_EV_PCT_2 and t2>= cTotaliTitoli(contractIndex).DAT_EV_PCT_2 then -----Caso 4)
				--dbms_output.put_line(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN||' - Caso4');
				cTotaliTitoli(contractIndex).IMP_CONTVAL_T1 :=  cTotaliTitoli(contractIndex).IMP_TOT_VERS;
				cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_VERS_PCT   := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO - cTotaliTitoli(contractIndex).IMP_TOT_VERS;
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_INVEST_PCT := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO - cTotaliTitoli(contractIndex).IMP_TOT_INVEST;   --20190308
				IF codicePeriodo IS NULL THEN       --non stiamo elaborando i cruscotti
                    cTotaliTitoli(contractIndex).IMP_TOT_VERS        := 0;
                    cTotaliTitoli(contractIndex).IMP_TOT_INVEST      := 0;
                END IF;
                
                cTotaliTitoli(contractIndex).IMP_SALDO_VERS      :=  cTotaliTitoli(contractIndex).IMP_TOT_RIMBO; --20190521  
				cTotaliTitoli(contractIndex).IMP_SALDO_INVEST    :=  cTotaliTitoli(contractIndex).IMP_TOT_RIMBO; --20190521  

--CASO 2)   T1 >= Ts il PCT e' nei chiusi, con Capitale Iniziale=0        Il periodo di analisi e' successivo alla data di scadenza del PCT
--il contratto e' visibile solo se e' applicato il filtro Includi contratti chiusi. Il saldo del PCT considera tutti i movimenti.
--Il campo 'Capitale Iniziale' contiene il valore zero '0'.
            elsif t1>=cTotaliTitoli(contractIndex).DAT_EV_PCT_2  AND cTotaliTitoli(contractIndex).EV_PCT_1 <> 1 then -----Caso 2)
				--dbms_output.put_line(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN||' - Caso2');
                cTotaliTitoli(contractIndex).IMP_CONTVAL_T1:=0;
				cTotaliTitoli(contractIndex).IMP_CONTVAL_T2:=0;
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_VERS_PCT 	:= ROUND(cTotaliTitoli(contractIndex).IMP_TOT_RIMBO -cTotaliTitoli(contractIndex).IMP_TOT_VERS, 2);
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_INVEST_PCT  := ROUND(cTotaliTitoli(contractIndex).IMP_TOT_RIMBO -cTotaliTitoli(contractIndex).IMP_TOT_INVEST, 2);   --20190308
  

--CASO 5)    Ta < T1 < T2 < Ts 			Periodo(T1,T2) interno a (Ta,Ts), quindi interno al periodo di vita del PCT
--Il periodo di analisi e' compreso tra la data apertura e la data scadenza del PCT. In questo caso il PCT mostreraÂ :
--Capitale Iniziale pari allÃ¿Â¿importo versato in fase di sottoscrizione;
--Capitale Versato (Valore di Acquisto) pari a zero;
--Capitale a Scadenza valorizzato come nel caso di PCT aperto senza applicazione di filtro di periodo, con importo dovuto al cliente alla scadenza del contratto;
            elsif cTotaliTitoli(contractIndex).EV_PCT_2 = 0 and t1>cTotaliTitoli(contractIndex).DAT_EV_PCT_1 and t2<cTotaliTitoli(contractIndex).DAT_EV_PCT_2 then --caso5)
				--dbms_output.put_line(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN||' - Caso5');
                cTotaliTitoli(contractIndex).IMP_CONTVAL_T1  :=  cTotaliTitoli(contractIndex).IMP_TOT_VERS;
                cTotaliTitoli(contractIndex).IMP_CONTVAL_T2  :=  cTotaliTitoli(contractIndex).IMP_TOT_RIMBO; --   modificato in contval 20042019 - riportato rimborso il 20190529
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_VERS_PCT   := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO - cTotaliTitoli(contractIndex).IMP_TOT_VERS;
                cTotaliTitoli(contractIndex).IMP_PLUS_MINUS_INVEST_PCT := cTotaliTitoli(contractIndex).IMP_TOT_RIMBO - cTotaliTitoli(contractIndex).IMP_TOT_INVEST;   --20190308
                IF codicePeriodo IS NULL THEN       --non stiamo elaborando i cruscotti
                    cTotaliTitoli(contractIndex).IMP_TOT_VERS        := 0;  -- richiesti dal FE di CR
                    cTotaliTitoli(contractIndex).IMP_TOT_INVEST      := 0;
                    cTotaliTitoli(contractIndex).IMP_TOT_RIMBO  	 := 0;
                END IF;

--CASO 3)     Il periodo di analisi e' precedente all acquisto del PCT. In questo caso il PCT non e' visibile
            elsif cTotaliTitoli(contractIndex).EV_PCT_2 = 0 and t1<cTotaliTitoli(contractIndex).DAT_EV_PCT_1 then
				--dbms_output.put_line(cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN||' - Caso3');
                Ctotalititoli(Contractindex).Pct_Num_Transazioni := Ctotalititoli(Contractindex).Pct_Num_Transazioni + 1;

                end if;
         END IF;
 END IF;
-------------------------END  Blocco elaborazione PCT  -----------------


IF cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA IN ('PCT') THEN

	IF cEventoTitoli.DAT_RIF                   <=t2 THEN
		cTotaliTitoli(contractIndex).QTA_QUOTA_T2 := cTotaliTitoli(contractIndex).QTA_QUOTA_T2 + quantita; -- 22022019

	END IF;

	IF cEventoTitoli.DAT_RIF                   <=t1 THEN
		cTotaliTitoli(contractIndex).QTA_QUOTA_T1 := cTotaliTitoli(contractIndex).QTA_QUOTA_T1 + quantita; -- 22022019

	END IF;

END IF;
-- setting the timegap between holes in the selling and purchasing time of the title
-- not applicable to daily call and not applicable to PCTs and FNDRE

IF NOT cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA IN ('PCT', 'FNDIM' ) THEN
	-- saving the date of selling the title by checking if qta is zero

	IF cTotaliTitoli(contractIndex).QTA_QUOTA_T2 = 0 THEN
		if regola not in ('2', '5') then			-- per escludere i proventi/dividendi come ultimo evento
			tb_qta_holes(contractKey)                  := cEventoTitoli.DAT_RIF;
		end if;

	END IF;
	-- setting the timegap between selling and next purchase of the same title

	IF cTotaliTitoli(contractIndex).QTA_QUOTA_T2               > 0 AND qta_saldo_hole_t1 IS NOT NULL THEN
		cTotaliTitoli(contractIndex).QTA_SALDO_GIORNL_QUOTA_ZERO := NVL(cTotaliTitoli(contractIndex).QTA_SALDO_GIORNL_QUOTA_ZERO, 0) + (cEventoTitoli.DAT_RIF - qta_saldo_hole_t1);
        tb_qta_holes(contractKey) := NULL;
	END IF;
END IF;




cTotaliTitoli(contractIndex).COD_GEOG_FONDO := cEventoTitoli.COD_SOC_GEST;
-- required for PCTs

IF isPCT = false THEN		--- settaggio della DAT_CHIU_CONTR quando la QTA_QUOTA_T2 = 0

			IF cTotaliTitoli(contractIndex).QTA_QUOTA_T2    = 0 THEN
				cTotaliTitoli(contractIndex).DAT_CHIU_CONTR   := cEventoTitoli.DAT_RIF;
				--cTotaliTitoli(contractIndex).IMP_SALDO_VERS   := 0;
				--cTotaliTitoli(contractIndex).IMP_SALDO_INVEST := 0;

			ELSE
				cTotaliTitoli(contractIndex).DAT_CHIU_CONTR := NULL;

			END IF;


END IF;

--sezione per cruscotti
IF t1 IS NOT NULL AND t2 IS NOT NULL AND codicePeriodo IS NOT NULL THEN
    cTotaliTitoli(contractIndex).COD_PERIOD_ELAB := codicePeriodo;
    cTotaliTitoli(contractIndex).DAT_INIZ_PERIOD := trim(p_t1);
    cTotaliTitoli(contractIndex).DAT_FINE_PERIOD := trim(p_t2);

END IF;

-- 22022019 BEGIN
if(attivaDebug  = 'S') then
	insert into sogg_outelab.dett_log_err (PROD_C,  CONTR_N,  COD_TIT_INTERN,  COD_INDICE_EMIS_TIT,  	COD_ERR_ELAB,
	DES_ERR_ELAB,
	GSTD_X_TIP_MODF,  GSTD_X_USER,  GSTD_M_NOM_ULT_MODF)
	values  (trim(cEventoTitoli.PROD_C), trim(cEventoTitoli.CONTR_N), cEventoTitoli.ANAATTIVFIN_C_CODICE_TIT_TRAN, cEventoTitoli.ANAATTIVFIN_X_INDICT_EMISS,progrDebug,
	substr(cEventoTitoli.DAT_RIF ||' 	' ||
	cEventoTitoli.OPERZTIT_C_CAUS ||' 	' ||
	regola ||' 	' ||
	imp_rimborsato ||' 	' ||
	imp_investito ||' 	' ||
	imp_versato ||' 	' ||
	imp_cedole_tit ||' 	' ||
	quantita ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_INT_VERS ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_INT_INVEST ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_TOT_VERS ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_TOT_INVEST ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_TOT_RIMBO ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_PRVNT_PASS ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_SALDO_VERS ||' 	' ||
	cTotaliTitoli(contractIndex).IMP_SALDO_INVEST||' 	' ||
	cTotaliTitoli(contractIndex).QTA_SALDO_GIORNL_QUOTA_ZERO,1,254), 'I', 'calc_tt_titoli' , 'calc_tt_titoli' ); 
	progrDebug := progrDebug+1;
	commit;
end if;

END;


PROCEDURE agg_tab_totali_gg(
		bulk_limit IN NUMBER,
		esito OUT CHAR,
		num_rec_elab OUT INTEGER,
		num_rec_scart OUT INTEGER,
		num_rec_warn OUT INTEGER )
IS
	chiavi_del_giorno SOGG_OUTELAB.TB_KEY_TIT;
	ultimoRun DATE;
	rec_eventi_titoli eventi_titoli%rowtype;
	in_cTotaliTitoli tb_myIndex;
	cTotaliTitoli SOGG_OUTELAB.TB_TOTALS_TIT;
type tb_eventi_titoli
IS
	TABLE OF eventi_titoli%rowtype;
	eventi_grezzi tb_eventi_titoli;
	myerrmsg VARCHAR(255);
BEGIN
	BEGIN

		INSERT
		INTO SOGG_OUTELAB.DETT_LOG_MOVI_CONTR_FT_TIT
			(
				PROD_C,
				CONTR_N,
				COD_TIT_INTERN,
				COD_INDICE_EMIS_TIT,
				COD_TRANS_ACQU_PCT,
				DAT_RIFE,
				COD_TIPO_RECORD,
				GSTD_X_USER,
				GSTD_D_INS_RECORD,
				GSTD_F_ESIST
			)
		SELECT prod_c,
			contr_n,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT,
			TRUNC(sysdate),
			'TITOLI',
			'REC_PROV',
			sysdate,
			'S'
		FROM
			(SELECT A1.PROD_C,
				A1.CONTR_N,
				A1.COD_TIT_INTERN,
				A1.COD_INDICE_EMIS_TIT,
				A1.COD_TRANS_ACQU_PCT,
				A1.DAT_CHIU_CONTR
			FROM
				(SELECT ST.PROD_C,
					ST.CONTR_N,
					ST.COD_TIT_INTERN,
					ST.COD_INDICE_EMIS_TIT,
					ST.COD_TRANS_ACQU_PCT,
					ST.DAT_CHIU_CONTR
				FROM SOGG_OUTELAB.SALDO_TOT_TIT ST
				WHERE 1                                                              =1
				AND COALESCE(ST.DAT_CHIU_CONTR, to_date('01-01-1753','dd-mm-yyyy')) <= sysdate
				AND ST.COD_TIPO_TIT_REDDTA                                           = 'PCT'
				AND NOT EXISTS
					(SELECT 1
					FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_CHIU_TIT CT
					WHERE CT.COD_TIPO_TIT_REDDTA = 'PCT'
					AND ST.PROD_C                = CT.PROD_C
					AND ST.CONTR_N               = CT.CONTR_N
					AND ST.COD_TIT_INTERN        = CT.COD_TIT_INTERN
					AND ST.COD_INDICE_EMIS_TIT   = CT.COD_INDICE_EMIS_TIT
					)
				) A1
			) B1;
		COMMIT;

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

	ultimoRun := fetchDataUltimoRun('TITOLI');
	sogg_outelab.ins_punt_elab('TITOLI');
	num_rec_elab  := 0;
	cTotaliTitoli := SOGG_OUTELAB.TB_TOTALS_TIT();
	--LISTA DI REAL ESTATE
	aggiorna_real_estate_list;
	--LISTA CAUSALI CEDOLE REAL ESTATE
	aggiorna_tipo_cedola_list_re;
	--LISTA CAUSALI CEDOLE TITOLI NON REAL ESTATE
	aggiorna_tipo_cedola_list_tit;
	--LIST BOND_ISIN for Titles
	aggiorna_bond_isin_tit;
	--LISTA fondi sorgente/destinazione per fondi RE soggetti a cambio ISIN
	aggiorna_lista_re_cambio_isin;
	LOOP
		OPEN c_chiavi_del_giorno(bulk_limit,'TITOLI');

		FETCH c_chiavi_del_giorno bulk collect INTO chiavi_del_giorno;

		CLOSE c_chiavi_del_giorno;

		IF (chiavi_del_giorno.count = 0) THEN
			EXIT;

		END IF;

		UPDATE SOGG_OUTELAB.DETT_LOG_MOVI_CONTR_FT_TIT LC
		SET LC.GSTD_F_ESIST       ='N',
			LC.GSTD_M_NOM_ULT_MODF   ='ESEGUITA',
			LC.GSTD_D_ULT_MODF_RECORD=sysdate
		WHERE LC.Dat_Rife        <= TRUNC(SYSDATE)
		AND LC.COD_TIPO_RECORD    ='TITOLI'
		AND
			(
				lc.prod_c, lc.contr_n, lc.COD_TIT_INTERN, lc.COD_INDICE_EMIS_TIT, lc.COD_TRANS_ACQU_PCT
			)
			IN
			(SELECT t1.prod_c,
				t1.contr_n,
				t1.COD_TIT_INTERN,
				t1.COD_INDICE_EMIS_TIT,
				t1.COD_TRANS_ACQU_PCT
			FROM (TABLE(chiavi_del_giorno)) T1
			) ;


		-- 26042019  Gestisco update dei contratti completi
		UPDATE SOGG_OUTELAB.DETT_LOG_MOVI_CONTR_FT_TIT LC
		SET LC.GSTD_F_ESIST       ='N',
			LC.GSTD_M_NOM_ULT_MODF   ='ESEGUITA',
			LC.GSTD_D_ULT_MODF_RECORD=sysdate
		WHERE LC.Dat_Rife        <= TRUNC(SYSDATE)
		AND LC.COD_TIPO_RECORD    ='TITOLI'
		AND lc.COD_TIT_INTERN is null
		AND
		(
			lc.prod_c, lc.contr_n
		)
		IN
		(SELECT t1.prod_c,
			t1.contr_n
		FROM (TABLE(chiavi_del_giorno)) T1
		) ;

		cTotaliTitoli.delete;
		in_cTotaliTitoli.delete;
        
        --viene cancellato l'array per il calcolo dei giorni a zero. Essendo un array globale, se non cancellato verrebbe
        --mantenuto a livello di sessione tra una chiamatae l'altra falsando i dati.
        tb_qta_holes.delete();        
        
		OPEN eventi_titoli(chiavi_del_giorno);
		LOOP

			FETCH eventi_titoli BULK COLLECT INTO eventi_grezzi LIMIT bulk_limit;
			EXIT
		WHEN eventi_grezzi.COUNT = 0;
			num_rec_elab           := num_rec_elab + eventi_grezzi.COUNT;

			FOR indiceMovimenti IN 1 .. eventi_grezzi.COUNT
			LOOP
				calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti) );

			END LOOP;

		END LOOP;
		CLOSE eventi_titoli;
		Elimina_PCT_malformati(Ctotalititoli);
		-- #86146 - setting Qta_quota_t2 for titles by accessing cll.posiztit table
		Update_qtaQuotaT2_titles(Ctotalititoli);
		agg_tab_totali(cTotaliTitoli);
		COMMIT;

	END LOOP;

EXCEPTION

WHEN OTHERS THEN
	CLOSE c_chiavi_del_giorno;
	CLOSE config_cur;
	CLOSE is_real_estate_cur;
	CLOSE eventi_titoli;

END;

PROCEDURE agg_tab_totali(
		totali IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT )
IS
BEGIN

	INSERT
	INTO SOGG_OUTELAB.DETT_LOG_ERR
		(
			PROD_C ,
			CONTR_N ,
			COD_TIT_INTERN ,
			COD_INDICE_EMIS_TIT ,
			COD_ERR_ELAB ,
			DES_ERR_ELAB ,
			GSTD_D_ULT_MODF_RECORD ,
			GSTD_D_INS_RECORD ,
			GSTD_X_TIP_MODF ,
			GSTD_X_USER ,
			GSTD_M_NOM_ULT_MODF ,
			GSTD_F_ESIST
		)
	SELECT PROD_C,
		CONTR_N,
		COD_TIT_INTERN,
		COD_INDICE_EMIS_TIT,
		79369,
		'errore merge saldi titoli - duplicati. cod pct '
		||trim(COD_TRANS_ACQU_PCT),
		sysdate,
		sysdate,
		'I',
		'batch',
		'bach',
		'S'
	FROM
		(SELECT PROD_C,
			CONTR_N,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT
		FROM TABLE(totali) t2
		GROUP BY PROD_C,
			CONTR_N,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT
		HAVING COUNT(1)>1
		);

	INSERT
	INTO SOGG_OUTELAB.DETT_LOG_ERR
		(
			PROD_C ,
			CONTR_N ,
			COD_TIT_INTERN ,
			COD_INDICE_EMIS_TIT ,
			COD_ERR_ELAB ,
			DES_ERR_ELAB ,
			GSTD_D_ULT_MODF_RECORD ,
			GSTD_D_INS_RECORD ,
			GSTD_X_TIP_MODF ,
			GSTD_X_USER ,
			GSTD_M_NOM_ULT_MODF ,
			GSTD_F_ESIST
		)
	SELECT PROD_C,
		CONTR_N,
		COD_TIT_INTERN,
		COD_INDICE_EMIS_TIT,
		79369,
		'merge totali - null pct  '
		||trim(COD_TRANS_ACQU_PCT)
		||'cod:'
		||
		CASE
			WHEN ap1.CONTR_N IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.PROD_C IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.DAT_APER_CONTR IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.DAT_PRIMO_MOVI IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.DAT_ULT_MOVI IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.COD_INDICE_EMIS_TIT IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.COD_TIT_INTERN IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		--case when ap1.COD_TRANS_ACQU_PCT  is null then '1' else '0' end ||
		CASE
			WHEN ap1.IMP_INT_INVEST IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.IMP_INT_VERS IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.IMP_PRVNT_PASS IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.IMP_SALDO_INVEST IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.IMP_SALDO_VERS IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.IMP_TOT_INVEST IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.IMP_TOT_RIMBO IS NULL
			THEN '1'
			ELSE '0'
		END
		||
		CASE
			WHEN ap1.IMP_TOT_VERS IS NULL
			THEN '1'
			ELSE '0'
		END ,
		sysdate,
		sysdate,
		'I',
		'batch',
		'bach',
		'S'
	FROM
		(SELECT ap.*
		FROM TABLE(totali) ap
		WHERE ap.CONTR_N          IS NULL
		OR ap.PROD_C              IS NULL
		OR ap.DAT_APER_CONTR      IS NULL
		OR ap.DAT_PRIMO_MOVI      IS NULL
		OR ap.DAT_ULT_MOVI        IS NULL
		OR ap.COD_INDICE_EMIS_TIT IS NULL
		OR ap.COD_TIT_INTERN      IS NULL
		OR ap.IMP_INT_INVEST      IS NULL
		OR ap.IMP_INT_VERS        IS NULL
		OR ap.IMP_PRVNT_PASS      IS NULL
		OR ap.IMP_SALDO_INVEST    IS NULL
		OR ap.IMP_SALDO_VERS      IS NULL
		OR ap.IMP_TOT_INVEST      IS NULL
		OR ap.IMP_TOT_RIMBO       IS NULL
		OR ap.IMP_TOT_VERS        IS NULL
		) ap1;

	MERGE INTO sogg_outelab.saldo_tot_tit ds USING
	(SELECT *
	FROM TABLE(totali) t1
	WHERE
		(
			PROD_C, CONTR_N, COD_TIT_INTERN, COD_INDICE_EMIS_TIT, COD_TRANS_ACQU_PCT
		)
		NOT IN
		(SELECT PROD_C,
			CONTR_N,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT
		FROM TABLE(totali) t2
		GROUP BY PROD_C,
			CONTR_N,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT
		HAVING COUNT(1)>1
		)
	) ap ON
	(
		ap.prod_c=ds.prod_c AND ap.contr_n=ds.contr_n AND ap.cod_tit_intern=ds.cod_tit_intern AND ap.cod_indice_emis_tit=ds.cod_indice_emis_tit AND trim(COALESCE (ap.COD_TRANS_ACQU_PCT,'0')) = trim(COALESCE(ds.COD_TRANS_ACQU_PCT,'0'))
	)
WHEN matched THEN
	UPDATE
	SET ds.gstd_d_ult_modf_record=sysdate,
		ds."DAT_APER_CONTR"         =ap."DAT_APER_CONTR",
		ds."DAT_CHIU_CONTR"         =ap."DAT_CHIU_CONTR",
		ds."DAT_PRIMO_MOVI"         =ap."DAT_PRIMO_MOVI",
		ds."DAT_ULT_MOVI"           =ap."DAT_ULT_MOVI",
		ds."IMP_TOT_VERS"           =ap."IMP_TOT_VERS",
		ds."IMP_TOT_INVEST"         =ap."IMP_TOT_INVEST",
		ds."IMP_TOT_RIMBO"          =ap."IMP_TOT_RIMBO",
		ds."IMP_PRVNT_PASS"         =ap."IMP_PRVNT_PASS",
		ds."QTA_QUOTA_T2"           =ap."QTA_QUOTA_T2",
		ds."IMP_INT_VERS"           =ap."IMP_INT_VERS",
		ds."IMP_SALDO_VERS"         =ap."IMP_SALDO_VERS",
		ds."IMP_INT_INVEST"         =ap."IMP_INT_INVEST",
		ds."IMP_SALDO_INVEST"       =ap."IMP_SALDO_INVEST",
		ds."IMP_CONTVAL"            =ap."IMP_CONTVAL",
		ds."COD_TIPO_TIT_REDDTA"    =ap."COD_TIPO_TIT_REDDTA",
        ds."QTA_SALDO_GIORNL_QUOTA_ZERO" = ap."QTA_SALDO_GIORNL_QUOTA_ZERO"
	WHERE ap.CONTR_N            IS NOT NULL
	AND ap.PROD_C               IS NOT NULL
	AND ap.DAT_APER_CONTR       IS NOT NULL
	AND ap.DAT_PRIMO_MOVI       IS NOT NULL
	AND ap.DAT_ULT_MOVI         IS NOT NULL
	AND ap.COD_INDICE_EMIS_TIT  IS NOT NULL
	AND ap.COD_TIT_INTERN       IS NOT NULL
	AND ap.IMP_INT_INVEST       IS NOT NULL
	AND ap.IMP_INT_VERS         IS NOT NULL
	AND ap.IMP_PRVNT_PASS       IS NOT NULL
	AND ap.IMP_SALDO_INVEST     IS NOT NULL
	AND ap.IMP_SALDO_VERS       IS NOT NULL
	AND ap.IMP_TOT_INVEST       IS NOT NULL
	AND ap.IMP_TOT_RIMBO        IS NOT NULL
	AND ap.IMP_TOT_VERS         IS NOT NULL WHEN NOT matched THEN
	INSERT
		(
			"PROD_C",
			"CONTR_N",
			"COD_TIT_INTERN",
			"COD_INDICE_EMIS_TIT",
			gstd_f_esist,
			gstd_x_tip_modf,
			gstd_d_ult_modf_record,
			gstd_x_user,
			gstd_m_nom_ult_modf,
			"DAT_APER_CONTR",
			"DAT_CHIU_CONTR",
			"DAT_PRIMO_MOVI",
			"DAT_ULT_MOVI",
			"IMP_TOT_VERS",
			"IMP_TOT_INVEST",
			"IMP_TOT_RIMBO",
			"IMP_PRVNT_PASS",
			"QTA_QUOTA_T2",
			"IMP_INT_VERS",
			"IMP_SALDO_VERS",
			"IMP_INT_INVEST",
			"IMP_SALDO_INVEST",
			"COD_TRANS_ACQU_PCT",
			"COD_TIPO_TIT_REDDTA",
			"IMP_CONTVAL",
            "QTA_SALDO_GIORNL_QUOTA_ZERO"
		)
		VALUES
		(
			ap."PROD_C",
			ap."CONTR_N",
			ap."COD_TIT_INTERN",
			ap."COD_INDICE_EMIS_TIT",
			'S',
			'I',
			sysdate,
			'TITOLI',
			'TITOLI',
			ap."DAT_APER_CONTR",
			ap."DAT_CHIU_CONTR",
			ap."DAT_PRIMO_MOVI",
			ap."DAT_ULT_MOVI",
			ap."IMP_TOT_VERS",
			ap."IMP_TOT_INVEST",
			ap."IMP_TOT_RIMBO",
			ap."IMP_PRVNT_PASS",
			ap."QTA_QUOTA_T2",
			ap."IMP_INT_VERS",
			ap."IMP_SALDO_VERS",
			ap."IMP_INT_INVEST",
			ap."IMP_SALDO_INVEST",
			COALESCE(ap."COD_TRANS_ACQU_PCT", '0'),
			ap."COD_TIPO_TIT_REDDTA",
			ap."IMP_CONTVAL",
            ap."QTA_SALDO_GIORNL_QUOTA_ZERO"
		)
	WHERE ap.CONTR_N           IS NOT NULL
	AND ap.PROD_C              IS NOT NULL
	AND ap.DAT_APER_CONTR      IS NOT NULL
	AND ap.DAT_PRIMO_MOVI      IS NOT NULL
	AND ap.DAT_ULT_MOVI        IS NOT NULL
	AND ap.COD_INDICE_EMIS_TIT IS NOT NULL
	AND ap.COD_TIT_INTERN      IS NOT NULL
	AND ap.IMP_INT_INVEST      IS NOT NULL
	AND ap.IMP_INT_VERS        IS NOT NULL
	AND ap.IMP_PRVNT_PASS      IS NOT NULL
	AND ap.IMP_SALDO_INVEST    IS NOT NULL
	AND ap.IMP_SALDO_VERS      IS NOT NULL
	AND ap.IMP_TOT_INVEST      IS NOT NULL
	AND ap.IMP_TOT_RIMBO       IS NOT NULL
	AND ap.IMP_TOT_VERS        IS NOT NULL;

END;

PROCEDURE ins_dati_online
	(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
		dest IN CHAR
	)
IS
	counter NUMBER;
BEGIN

	INSERT
	INTO SOGG_OUTELAB.DETT_LOG_ERR
		(
			PROD_C,
			CONTR_N,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_ERR_ELAB,
			DES_ERR_ELAB,
			GSTD_D_INS_RECORD,
			GSTD_X_TIP_MODF,
			GSTD_X_USER,
			GSTD_M_NOM_ULT_MODF
		)
	SELECT ap.prod_c,
		ap.contr_n,
		ap.COD_TIT_INTERN,
		ap.COD_INDICE_EMIS_TIT,
		0,
		'motore titoli: riga duplicata in inserimento redd - '
		|| dest,
		SYSDATE,
		'I',
		'BATCH',
		'BATCH'
	FROM
		(SELECT prod_c,
			contr_n,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT
		FROM TABLE (cRec)
		GROUP BY prod_c,
			contr_n,
			COD_TIT_INTERN,
			COD_INDICE_EMIS_TIT,
			COD_TRANS_ACQU_PCT
		HAVING COUNT(1)>1
		) ap;

	IF dest='A' THEN

		INSERT
		INTO SOGG_OUTELAB.DETT_REDDTA_CONTR_TIT
			(
				PROD_C,
				CONTR_N,
				COD_TIT_INTERN,
				COD_INDICE_EMIS_TIT,
				COD_TRANS_ACQU_PCT,
				DAT_RIFE,
				GSTD_F_ESIST,
				GSTD_X_TIP_MODF,
				GSTD_D_ULT_MODF_RECORD,
				GSTD_X_USER,
				GSTD_M_NOM_ULT_MODF,
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
				IMP_PLUS_MINUS_INVEST,
				QTA_SALDO_GIORNL_QUOTA_ZERO
			)
		SELECT ap.PROD_C,
			ap.CONTR_N,
			ap.COD_TIT_INTERN,
			ap.COD_INDICE_EMIS_TIT,
			ap.COD_TRANS_ACQU_PCT,
			ap.DAT_RIFE,
			'S',
			'I',
			SYSDATE,
			'MOT-TIT',
			'MOT-TIT',
			ap.DAT_APER_CONTR,
			ap.DAT_CHIU_CONTR,
			ap.DAT_PRIMO_MOVI,
			ap.DAT_ULT_MOVI,
			ap.IMP_TOT_VERS,
			ap.IMP_TOT_INVEST,
			ap.IMP_TOT_RIMBO,
			ap.IMP_PRVNT_PASS,
			ap.QTA_QUOTA_T2,
			ap.IMP_INT_VERS,
			ap.IMP_SALDO_VERS,
			ap.IMP_INT_INVEST,
			ap.IMP_SALDO_INVEST,
			ap.IMP_CONTVAL_T2,
			ap.QTA_QUOTA_SALDO,
			ap.PRC_REND_VPATR_VERS_ST,
			ap.PRC_REND_VPATR_VERS_ANNUAL,
			ap.PRC_REND_VPATR_INVEST_ST,
			ap.PRC_REND_VPATR_INVEST_ANNUAL,
			ap.PRC_REND_MWRR_VERS_ST,
			ap.PRC_REND_MWRR_VERS_ANNUAL,
			ap.PRC_REND_MWRR_INVEST_ST,
			ap.PRC_REND_MWRR_INVEST_ANNUAL,
			ap.COD_TIPO_TIT_REDDTA,
			ap.IMP_PLUS_MINUS_VERS,
			ap.IMP_PLUS_MINUS_INVEST,
			ap.QTA_SALDO_GIORNL_QUOTA_ZERO
		FROM TABLE (cRec) ap
		WHERE
			(
				ap.prod_c, ap.contr_n, ap.COD_TIT_INTERN, ap.COD_INDICE_EMIS_TIT, ap.COD_TRANS_ACQU_PCT
			)
			NOT IN
			(SELECT prod_c,
				contr_n,
				COD_TIT_INTERN,
				COD_INDICE_EMIS_TIT,
				COD_TRANS_ACQU_PCT
			FROM TABLE (cRec)
			GROUP BY prod_c,
				contr_n,
				COD_TIT_INTERN,
				COD_INDICE_EMIS_TIT,
				COD_TRANS_ACQU_PCT
			HAVING COUNT(1)>1
			)
		AND NOT EXISTS
			(SELECT 1
			FROM SOGG_OUTELAB.DETT_REDDTA_CONTR_TIT ds
			WHERE ds.prod_c           =ap.prod_c
			AND ds.contr_n            =ap.contr_n
			AND ds.COD_TIT_INTERN     =ap.COD_TIT_INTERN
			AND ds.COD_INDICE_EMIS_TIT=ap.COD_INDICE_EMIS_TIT
			AND ds.COD_TRANS_ACQU_PCT =ap.COD_TRANS_ACQU_PCT
			AND ds.DAT_RIFE           =ap.DAT_RIFE
			);
		/*
		merge INTO SOGG_OUTELAB.DETT_REDDTA_CONTR_TIT ds USING
		(SELECT *
		FROM TABLE (cRec) a1
		WHERE
		(
		a1.prod_c, a1.contr_n, a1.COD_TIT_INTERN, a1.COD_INDICE_EMIS_TIT, a1.COD_TRANS_ACQU_PCT
		)
		NOT IN
		(SELECT prod_c,
		contr_n,
		COD_TIT_INTERN,
		COD_INDICE_EMIS_TIT,
		COD_TRANS_ACQU_PCT
		FROM TABLE (cRec)
		GROUP BY prod_c,
		contr_n,
		COD_TIT_INTERN,
		COD_INDICE_EMIS_TIT,
		COD_TRANS_ACQU_PCT
		HAVING COUNT(1)>1
		)
		) ap ON
		(
		ds.dat_rife=TRUNC(sysdate)-1 AND ap.prod_c=ds.prod_c AND ap.contr_n=ds.contr_n AND ap.cod_tit_intern=ds.cod_tit_intern AND ap.cod_indice_emis_tit=ds.cod_indice_emis_tit AND trim(COALESCE (ap.COD_TRANS_ACQU_PCT,'0')) = trim(COALESCE(ds.COD_TRANS_ACQU_PCT,'0'))
		)
		WHEN NOT MATCHED THEN
		INSERT
		(
		PROD_C,
		CONTR_N,
		COD_TIT_INTERN,
		COD_INDICE_EMIS_TIT,
		COD_TRANS_ACQU_PCT,
		DAT_RIFE,
		GSTD_F_ESIST,
		GSTD_X_TIP_MODF,
		GSTD_D_ULT_MODF_RECORD,
		GSTD_X_USER,
		GSTD_M_NOM_ULT_MODF,
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
		)
		VALUES
		(
		ap.PROD_C,
		ap.CONTR_N,
		ap.COD_TIT_INTERN,
		ap.COD_INDICE_EMIS_TIT,
		ap.COD_TRANS_ACQU_PCT,
		ap.DAT_RIFE,
		'S',
		'I',
		SYSDATE,
		'MOT-TIT',
		'MOT-TIT',
		ap.DAT_APER_CONTR,
		ap.DAT_CHIU_CONTR,
		ap.DAT_PRIMO_MOVI,
		ap.DAT_ULT_MOVI,
		ap.IMP_TOT_VERS,
		ap.IMP_TOT_INVEST,
		ap.IMP_TOT_RIMBO,
		ap.IMP_PRVNT_PASS,
		ap.QTA_QUOTA_T2,
		ap.IMP_INT_VERS,
		ap.IMP_SALDO_VERS,
		ap.IMP_INT_INVEST,
		ap.IMP_SALDO_INVEST,
		ap.IMP_CONTVAL_T2,
		ap.QTA_QUOTA_SALDO,
		ap.PRC_REND_VPATR_VERS_ST,
		ap.PRC_REND_VPATR_VERS_ANNUAL,
		ap.PRC_REND_VPATR_INVEST_ST,
		ap.PRC_REND_VPATR_INVEST_ANNUAL,
		ap.PRC_REND_MWRR_VERS_ST,
		ap.PRC_REND_MWRR_VERS_ANNUAL,
		ap.PRC_REND_MWRR_INVEST_ST,
		ap.PRC_REND_MWRR_INVEST_ANNUAL,
		ap.COD_TIPO_TIT_REDDTA,
		ap.IMP_PLUS_MINUS_VERS,
		ap.IMP_PLUS_MINUS_INVEST
		);
		*/

	elsif dest='C' THEN

		SELECT COUNT(*)
		INTO counter
		FROM TABLE (cRec);

		merge INTO SOGG_OUTELAB.DETT_REDDTA_CONTR_CHIU_TIT ds USING
		(SELECT *
		FROM TABLE (cRec) a1
		WHERE
			(
				a1.prod_c, a1.contr_n, a1.COD_TIT_INTERN, a1.COD_INDICE_EMIS_TIT, a1.COD_TRANS_ACQU_PCT
			)
			NOT IN
			(SELECT prod_c,
				contr_n,
				COD_TIT_INTERN,
				COD_INDICE_EMIS_TIT,
				COD_TRANS_ACQU_PCT
			FROM TABLE (cRec)
			GROUP BY prod_c,
				contr_n,
				COD_TIT_INTERN,
				COD_INDICE_EMIS_TIT,
				COD_TRANS_ACQU_PCT
			HAVING COUNT(1)>1
			)
		) ap ON
		(
			--sui chiusi Ã¿Â¿Ã¿Â¨ corretto che non sia inserita riferimento
			ap.prod_c=ds.prod_c AND ap.contr_n=ds.contr_n AND ap.cod_tit_intern=ds.cod_tit_intern AND ap.cod_indice_emis_tit=ds.cod_indice_emis_tit AND trim(COALESCE (ap.COD_TRANS_ACQU_PCT,'0')) = trim(COALESCE(ds.COD_TRANS_ACQU_PCT,'0'))
		)
	WHEN MATCHED THEN
		UPDATE
		SET ds."DAT_RIFE"                 =ap."DAT_RIFE",
			ds."GSTD_F_ESIST"                ='S',
			ds."GSTD_X_TIP_MODF"             ='U',
			ds."GSTD_D_ULT_MODF_RECORD"      =SYSDATE,
			ds."GSTD_X_USER"                 ='MOT-TIT',
			ds."GSTD_M_NOM_ULT_MODF"         ='MOT-TIT',
			ds."DAT_APER_CONTR"              =ap."DAT_APER_CONTR",
			ds."DAT_CHIU_CONTR"              =ap."DAT_CHIU_CONTR",
			ds."DAT_PRIMO_MOVI"              =ap."DAT_PRIMO_MOVI",
			ds."DAT_ULT_MOVI"                =ap."DAT_ULT_MOVI",
			ds."IMP_TOT_VERS"                =ap."IMP_TOT_VERS",
			ds."IMP_TOT_INVEST"              =ap."IMP_TOT_INVEST",
			ds."IMP_TOT_RIMBO"               =ap."IMP_TOT_RIMBO",
			ds."IMP_PRVNT_PASS"              =ap."IMP_PRVNT_PASS",
			ds."QTA_QUOTA_T2"                =ap."QTA_QUOTA_T2",
			ds."IMP_INT_VERS"                =ap."IMP_INT_VERS",
			ds."IMP_SALDO_VERS"              =ap."IMP_SALDO_VERS",
			ds."IMP_INT_INVEST"              =ap."IMP_INT_INVEST",
			ds."IMP_SALDO_INVEST"            =ap."IMP_SALDO_INVEST",
			ds."IMP_CONTVAL"                 =NVL(ap."IMP_CONTVAL_T2",0),
			ds."QTA_QUOTA_SALDO"             =ap."QTA_QUOTA_SALDO",
			ds."PRC_REND_VPATR_VERS_ST"      =ap."PRC_REND_VPATR_VERS_ST",
			ds."PRC_REND_VPATR_VERS_ANNUAL"  =ap."PRC_REND_VPATR_VERS_ANNUAL",
			ds."PRC_REND_VPATR_INVEST_ST"    =ap."PRC_REND_VPATR_INVEST_ST",
			ds."PRC_REND_VPATR_INVEST_ANNUAL"=ap."PRC_REND_VPATR_INVEST_ANNUAL",
			ds."PRC_REND_MWRR_VERS_ST"       =ap."PRC_REND_MWRR_VERS_ST",
			ds."PRC_REND_MWRR_VERS_ANNUAL"   =ap."PRC_REND_MWRR_VERS_ANNUAL",
			ds."PRC_REND_MWRR_INVEST_ST"     =ap."PRC_REND_MWRR_INVEST_ST",
			ds."PRC_REND_MWRR_INVEST_ANNUAL" =ap."PRC_REND_MWRR_INVEST_ANNUAL",
			ds."COD_TIPO_TIT_REDDTA"         =ap."COD_TIPO_TIT_REDDTA" WHEN NOT MATCHED THEN
		INSERT
			(
				PROD_C,
				CONTR_N,
				COD_TIT_INTERN,
				COD_INDICE_EMIS_TIT,
				COD_TRANS_ACQU_PCT,
				DAT_RIFE,
				GSTD_F_ESIST,
				GSTD_X_TIP_MODF,
				GSTD_D_ULT_MODF_RECORD,
				GSTD_X_USER,
				GSTD_M_NOM_ULT_MODF,
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
				COD_TIPO_TIT_REDDTA
			)
			VALUES
			(
				ap.PROD_C,
				ap.CONTR_N,
				ap.COD_TIT_INTERN,
				ap.COD_INDICE_EMIS_TIT,
				ap.COD_TRANS_ACQU_PCT,
				ap.DAT_RIFE,
				'S',
				'I',
				SYSDATE,
				'MOT-TIT',
				'MOT-TIT',
				ap.DAT_APER_CONTR,
				ap.DAT_CHIU_CONTR,
				ap.DAT_PRIMO_MOVI,
				ap.DAT_ULT_MOVI,
				ap.IMP_TOT_VERS,
				ap.IMP_TOT_INVEST,
				ap.IMP_TOT_RIMBO,
				ap.IMP_PRVNT_PASS,
				ap.QTA_QUOTA_T2,
				ap.IMP_INT_VERS,
				ap.IMP_SALDO_VERS,
				ap.IMP_INT_INVEST,
				ap.IMP_SALDO_INVEST,
				NVL(ap.IMP_CONTVAL_T2,0),
				ap.QTA_QUOTA_SALDO,
				ap.PRC_REND_VPATR_VERS_ST,
				ap.PRC_REND_VPATR_VERS_ANNUAL,
				ap.PRC_REND_VPATR_INVEST_ST,
				ap.PRC_REND_VPATR_INVEST_ANNUAL,
				ap.PRC_REND_MWRR_VERS_ST,
				ap.PRC_REND_MWRR_VERS_ANNUAL,
				ap.PRC_REND_MWRR_INVEST_ST,
				ap.PRC_REND_MWRR_INVEST_ANNUAL,
				ap.COD_TIPO_TIT_REDDTA
			);

	END IF;
	COMMIT;

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
	dati_base_tit c_load_aper_tit%rowtype;
	recordTarget SOGG_OUTELAB.TB_OUTPUT_ONLINE;
	commitFreq  NUMBER;
	commitCount NUMBER;
BEGIN
	recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	commitFreq   :=10000;
	commitCount  :=1;
	OPEN c_load_aper_tit;
	LOOP

		FETCH c_load_aper_tit INTO dati_base_tit;
		EXIT
	WHEN c_load_aper_tit%NOTFOUND;
		recordTarget.extend(1);

		recordTarget(recordTarget.last)                       := SOGG_OUTELAB.RT_OUTPUT_ONLINE('FND');
		recordTarget(recordTarget.last).PROD_C                := dati_base_tit.PROD_C ;
		recordTarget(recordTarget.last).CONTR_N               := dati_base_tit.CONTR_N ;
		recordTarget(recordTarget.last).COD_TIT_INTERN        := dati_base_tit.COD_TIT_INTERN;
		recordTarget(recordTarget.last).COD_INDICE_EMIS_TIT   := dati_base_tit.COD_INDICE_EMIS_TIT;
		recordTarget(recordTarget.last).COD_TRANS_ACQU_PCT    := dati_base_tit.COD_TRANS_ACQU_PCT;
		recordTarget(recordTarget.last).DAT_RIFE              := dati_base_tit.DAT_RIFE ;
		recordTarget(recordTarget.last).DAT_APER_CONTR        := dati_base_tit.DAT_APER_CONTR ;
		recordTarget(recordTarget.last).DAT_CHIU_CONTR        := dati_base_tit.DAT_CHIU_CONTR ;
		recordTarget(recordTarget.last).DAT_PRIMO_MOVI        := dati_base_tit.DAT_PRIMO_MOVI ;
		recordTarget(recordTarget.last).DAT_ULT_MOVI          := dati_base_tit.DAT_ULT_MOVI ;
		recordTarget(recordTarget.last).IMP_TOT_VERS          := dati_base_tit.IMP_TOT_VERS ;
		recordTarget(recordTarget.last).IMP_TOT_INVEST        := dati_base_tit.IMP_TOT_INVEST ;
		recordTarget(recordTarget.last).IMP_TOT_RIMBO         := dati_base_tit.IMP_TOT_RIMBO ;
		recordTarget(recordTarget.last).IMP_PRVNT_PASS        := dati_base_tit.IMP_PRVNT_PASS ;
		recordTarget(recordTarget.last).QTA_QUOTA_T2          := dati_base_tit.QTA_QUOTA_T2 ;
		recordTarget(recordTarget.last).IMP_SALDO_VERS        := dati_base_tit.IMP_SALDO_VERS ;
		recordTarget(recordTarget.last).IMP_SALDO_INVEST      := dati_base_tit.IMP_SALDO_INVEST ;
		recordTarget(recordTarget.last).IMP_CONTVAL_T1        := 0 ;
		recordTarget(recordTarget.last).IMP_CONTVAL_T2        := dati_base_tit.IMP_CONTVAL ;
		recordTarget(recordTarget.last).QTA_QUOTA_SALDO       := dati_base_tit.QTA_QUOTA_SALDO ;
		recordTarget(recordTarget.last).IMP_INT_VERS          := dati_base_tit.IMP_INT_VERS ;
		recordTarget(recordTarget.last).IMP_INT_INVEST        := dati_base_tit.IMP_INT_INVEST ;
		recordTarget(recordTarget.last).COD_TIPO_TIT_REDDTA   := dati_base_tit.COD_TIPO_TIT_REDDTA ;
		recordTarget(recordTarget.last).IMP_PLUS_MINUS_VERS   := dati_base_tit.IMP_PLUS_MINUS_VERS ;
		recordTarget(recordTarget.last).IMP_PLUS_MINUS_INVEST := dati_base_tit.IMP_PLUS_MINUS_INVEST ;
		recordTarget(recordTarget.last).QTA_SALDO_GIORNL_QUOTA_ZERO := dati_base_tit.QTA_SALDO_GIORNL_QUOTA_ZERO ;
		SOGG_OUTELAB.calc_indici_redd(recordTarget(recordTarget.last));
		--sovrascrizione plus_minus in caso di PCT aperti

		IF recordTarget(recordTarget.last).COD_TIPO_TIT_REDDTA  = 'PCT' THEN
			recordTarget(recordTarget.last).IMP_PLUS_MINUS_VERS   := recordTarget(recordTarget.last).IMP_CONTVAL_T2 - recordTarget(recordTarget.last).IMP_SALDO_VERS;
			recordTarget(recordTarget.last).IMP_PLUS_MINUS_INVEST := recordTarget(recordTarget.last).IMP_PLUS_MINUS_VERS;

		END IF;

		IF mod(commitCount,commitFreq)=0 THEN
			ins_dati_online(recordTarget,'A');
			recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();

		END IF;
		commitCount := commitCount + 1;

	END LOOP;
	CLOSE c_load_aper_tit;
	ins_dati_online(recordTarget,'A');

END;

PROCEDURE calc_redd_chiusi_gg(
		bulk_limit IN NUMBER,
		esito OUT CHAR,
		num_rec_elab OUT INTEGER,
		num_rec_scart OUT INTEGER,
		num_rec_warn OUT INTEGER)
IS
	recordTarget SOGG_OUTELAB.TB_OUTPUT_ONLINE;
	commitFreq  NUMBER;
	commitCount NUMBER;
	ultimoRun   DATE;
	dati_base_titoli_c c_load_chiu_gg%rowtype;
BEGIN
	recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	commitFreq   :=10000;
	commitCount  :=1;
	ultimoRun    := fetchDataUltimoRun('TITOLI','S');

	OPEN c_load_chiu_gg(ultimoRun);
	LOOP

		FETCH c_load_chiu_gg INTO dati_base_titoli_c;
		EXIT
	WHEN c_load_chiu_gg%NOTFOUND;
		recordTarget.extend(1);
		recordTarget(recordTarget.last)                     := SOGG_OUTELAB.RT_OUTPUT_ONLINE('FND');
		recordTarget(recordTarget.last).COD_TIPO_TIT_REDDTA := dati_base_titoli_c.COD_TIPO_TIT_REDDTA;
		recordTarget(recordTarget.last).PROD_C              := dati_base_titoli_c.PROD_C;
		recordTarget(recordTarget.last).CONTR_N             := dati_base_titoli_c.CONTR_N;
		recordTarget(recordTarget.last).COD_TIT_INTERN      := dati_base_titoli_c.COD_TIT_INTERN;
		recordTarget(recordTarget.last).COD_INDICE_EMIS_TIT := dati_base_titoli_c.COD_INDICE_EMIS_TIT;
		recordTarget(recordTarget.last).COD_TRANS_ACQU_PCT  := dati_base_titoli_c.COD_TRANS_ACQU_PCT;
		recordTarget(recordTarget.last).DAT_APER_CONTR      := dati_base_titoli_c.DAT_APER_CONTR;
		recordTarget(recordTarget.last).DAT_CHIU_CONTR      := dati_base_titoli_c.DAT_CHIU_CONTR;
		recordTarget(recordTarget.last).DAT_RIFE            := dati_base_titoli_c.DAT_ULT_MOVI;
		recordTarget(recordTarget.last).DAT_PRIMO_MOVI      := dati_base_titoli_c.DAT_PRIMO_MOVI;
		recordTarget(recordTarget.last).DAT_ULT_MOVI        := dati_base_titoli_c.DAT_ULT_MOVI;
		recordTarget(recordTarget.last).IMP_TOT_VERS        := dati_base_titoli_c.IMP_TOT_VERS;
		recordTarget(recordTarget.last).IMP_TOT_INVEST      := dati_base_titoli_c.IMP_TOT_INVEST;
		recordTarget(recordTarget.last).IMP_TOT_RIMBO       := dati_base_titoli_c.IMP_TOT_RIMBO;
		recordTarget(recordTarget.last).IMP_PRVNT_PASS      := dati_base_titoli_c.IMP_PRVNT_PASS;
		recordTarget(recordTarget.last).QTA_QUOTA_T2        := dati_base_titoli_c.QTA_QUOTA_T2;
		recordTarget(recordTarget.last).IMP_SALDO_VERS      := dati_base_titoli_c.IMP_SALDO_VERS;
		recordTarget(recordTarget.last).IMP_SALDO_INVEST    := dati_base_titoli_c.IMP_SALDO_INVEST;
		recordTarget(recordTarget.last).IMP_CONTVAL_T1      := 0;
		
		recordTarget(recordTarget.last).IMP_CONTVAL_T2      := 0;
/*		--20042019 -- Per i pct faccio un eccezione e considero il controvalore a T2 percheÃ¿Â¨ altrimenti il  rendimento e' -100 dato che i rimborsi
		-- nella calc_totali_titoli sono stati forzati a 0
        IF recordTarget(recordTarget.last).COD_TIPO_TIT_REDDTA  != 'PCT' THEN
            recordTarget(recordTarget.last).IMP_CONTVAL_T2      := 0;
        else
        -- a seconda di uno dei 6 casi in cui ricade un PCT posso avere i rimborsi valorizzati oppure no
            if recordTarget(recordTarget.last).IMP_TOT_RIMBO = 0 then
                recordTarget(recordTarget.last).IMP_CONTVAL_T2      := dati_base_titoli_c.IMP_CONTVAL;
            else
                recordTarget(recordTarget.last).IMP_CONTVAL_T2      := dati_base_titoli_c.IMP_TOT_RIMBO;
            end if;
        end if;*/
		recordTarget(recordTarget.last).QTA_QUOTA_SALDO     := 0;
		recordTarget(recordTarget.last).IMP_INT_VERS        := dati_base_titoli_c.IMP_INT_VERS;
		recordTarget(recordTarget.last).IMP_INT_INVEST      := dati_base_titoli_c.IMP_INT_INVEST;
        recordTarget(recordTarget.last).QTA_SALDO_GIORNL_QUOTA_ZERO := dati_base_titoli_c.QTA_SALDO_GIORNL_QUOTA_ZERO ;
		SOGG_OUTELAB.calc_flg_stato(recordTarget(recordTarget.last), recordTarget(recordTarget.last).DAT_CHIU_CONTR);
		SOGG_OUTELAB.calc_indici_redd(recordTarget(recordTarget.last));
		--sovrascrizione plus_minus in caso di PCT aperti

		IF recordTarget(recordTarget.last).COD_TIPO_TIT_REDDTA  = 'PCT' THEN
			recordTarget(recordTarget.last).IMP_PLUS_MINUS_VERS   := recordTarget(recordTarget.last).IMP_TOT_RIMBO - recordTarget(recordTarget.last).IMP_SALDO_VERS;
			recordTarget(recordTarget.last).IMP_PLUS_MINUS_INVEST := recordTarget(recordTarget.last).IMP_PLUS_MINUS_VERS;

		END IF;

		IF mod(commitCount,commitFreq)=0 THEN
			ins_dati_online(recordTarget,'C');
			recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();

		END IF;
		commitCount := commitCount + 1;

	END LOOP;
	CLOSE c_load_chiu_gg;
	ins_dati_online(recordTarget,'C');

END;

PROCEDURE calc_redd_periodo(
		chiavi IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
		t1     IN DATE,
		t2     IN DATE,
		cRec   IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE )
IS
	local_redd_data SOGG_OUTELAB.TB_OUTPUT_ONLINE;
	rec_eventi_titoli eventi_titoli%rowtype;
	in_cTotaliTitoli tb_myIndex;
	cTotaliTitoli SOGG_OUTELAB.TB_TOTALS_TIT;
	contr_attuali tb_contr;
type tb_eventi_titoli
IS
	TABLE OF eventi_titoli%rowtype;
	eventi_grezzi tb_eventi_titoli;
	quote_t1 tb_val_quo;
	quote_t2 tb_val_quo;
	chiavi_tit SOGG_OUTELAB.TB_KEY_TIT;
	chiaveProdotto VARCHAR(32);
	contractKey    VARCHAR(32);
BEGIN
	tipo_op_regole_causali.delete; --11072019 Ripulisco l array delle causali, in modo che gli update vengano presi a caldo e non risentano dell effetto cache
	cTotaliTitoli   := SOGG_OUTELAB.TB_TOTALS_TIT();
	local_redd_data := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	chiavi_tit      := SOGG_OUTELAB.TB_KEY_TIT();
	legg_contr_info(chiavi, contr_attuali);
	--LISTA DI REAL ESTATE
	aggiorna_real_estate_list;
	--LISTA CAUSALI CEDOLE REAL ESTATE
	aggiorna_tipo_cedola_list_re;
	--LISTA CAUSALI CEDOLE TITOLI NON REAL ESTATE
	aggiorna_tipo_cedola_list_tit;
	--LIST BOND_ISIN for Titles
	aggiorna_bond_isin_tit;
    --viene cancellato l'array per il calcolo dei giorni a zero. Essendo un array globale, se non cancellato verrebbe
    --mantenuto a livello di sessione tra una chiamatae l'altra falsando i dati.    
    tb_qta_holes.delete();    
    

    --LISTA fondi sorgente/destinazione per fondi RE soggetti a cambio ISIN
    aggiorna_lista_re_cambio_isin;

    --viene cancellato l'array per il calcolo dei giorni a zero. Essendo un array globale, se non cancellato verrebbe
    --mantenuto a livello di sessione tra una chiamatae l'altra falsando i dati.    
    tb_qta_holes.delete();    
    

    --LISTA fondi sorgente/destinazione per fondi RE soggetti a cambio ISIN
    aggiorna_lista_re_cambio_isin;

    --viene cancellato l'array per il calcolo dei giorni a zero. Essendo un array globale, se non cancellato verrebbe
    --mantenuto a livello di sessione tra una chiamatae l'altra falsando i dati.
    tb_qta_holes.delete();


	FOR i IN 1 .. chiavi.COUNT
	LOOP
		contractKey := trim(chiavi(i).CONTR_N)||'-'||trim(chiavi(i).PROD_C);

		IF chiavi(i).prod_c='BAN12' AND contr_attuali.EXISTS(contractKey) AND NOT
			(
				contr_attuali(contractKey).CONTR_C_CAT IN('0722','0723','0725','0729') OR
				(
					contr_attuali(contractKey).CONTR_C_CAT = '0738' AND contr_attuali(contractKey).CONTR_C_SCAT = '000'
				)
			)
			THEN
			chiavi_tit.extend(1);
			chiavi_tit(chiavi_tit.last) := SOGG_OUTELAB.RT_KEY_TIT(chiavi(i).prod_c, chiavi(i).contr_n, NULL, NULL);

		END IF;

	END LOOP;
    
    --viene cancellato l'array per il calcolo dei giorni a zero. Essendo un array globale, se non cancellato verrebbe
    --mantenuto a livello di sessione tra una chiamatae l'altra falsando i dati.
    tb_qta_holes.delete();    
    
	OPEN eventi_titoli(chiavi_tit);
	LOOP

		FETCH eventi_titoli BULK COLLECT INTO eventi_grezzi;
		EXIT
	WHEN eventi_grezzi.COUNT = 0;

		FOR indiceMovimenti IN 1 .. eventi_grezzi.COUNT
		LOOP
			calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2 );

		END LOOP;

	END LOOP;
	CLOSE eventi_titoli;
	Elimina_PCT_malformati(Ctotalititoli);
	--legg_val_quota_per_re(cTotaliTitoli, t1, quote_t1_re);
	--legg_val_quota_per_re(cTotaliTitoli, t2, quote_t2_re);

	legg_val_quota_per_tit(cTotaliTitoli, t1, quote_t1);
	legg_val_quota_per_tit(cTotaliTitoli, t2, quote_t2);

	FOR i IN 1 .. cTotaliTitoli.COUNT
	LOOP
		local_redd_data.extend(1);
        
        IF cTotaliTitoli(i).DAT_PRIMO_MOVI IS NULL THEN
            cTotaliTitoli(i).DAT_PRIMO_MOVI   := t1;
        END IF;
        
        
		chiaveProdotto := trim(cTotaliTitoli(i).COD_TIT_INTERN)||'-'||cTotaliTitoli(i).COD_INDICE_EMIS_TIT;

		IF(cTotaliTitoli(i).COD_TIPO_TIT_REDDTA          = 'FNDIM') THEN
			local_redd_data(local_redd_data.last)          := SOGG_OUTELAB.RT_OUTPUT_ONLINE('FND');
			local_redd_data(local_redd_data.last).REC_TYPE := 'FND_IMMB';

		ELSE

			IF ( NVL(cTotaliTitoli(i).DAT_CHIU_CONTR, TO_DATE('31/12/9999', 'DD/MM/YYYY')) < TO_DATE('01/01/2013', 'DD/MM/YYYY') OR cTotaliTitoli(i).DAT_APER_CONTR > t2 ) THEN
				local_redd_data(local_redd_data.last)                                        := SOGG_OUTELAB.RT_OUTPUT_ONLINE('NOSEE');

			ELSIF(cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('PCT', 'MPB', 'CERT')) THEN
					local_redd_data(local_redd_data.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('INVBAN');

            ELSIF(cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('OBBLI', 'AZION')) THEN
                local_redd_data(local_redd_data.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('TIT');

            ELSE
					local_redd_data(local_redd_data.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('NOSEE');
			END IF;
			
			-- def. 50029: I titoli e gli investimenti bancari attivi prima della data di cut-off NON devono essere mostrati
           	IF ( NVL(cTotaliTitoli(i).DAT_CHIU_CONTR, TO_DATE('31/12/9999', 'DD/MM/YYYY')) < TO_DATE('01/01/2013', 'DD/MM/YYYY') OR cTotaliTitoli(i).DAT_APER_CONTR > t2 
				--OR cTotaliTitoli(i).DAT_CHIU_CONTR < t1) THEN   
				--20190529 -- i PCT chiusi prima dell'inizio periodo devono essere visibili a FE tra i chiusi (regola 2):   
				OR (cTotaliTitoli(i).DAT_CHIU_CONTR < t1 AND  cTotaliTitoli(i).COD_TIPO_TIT_REDDTA <> 'PCT') ) THEN 
            local_redd_data(local_redd_data.last).REC_TYPE :=  'NOSEE';
            ELSE
			-- defect 34089,34246
			-- latest rfc 86777 tit-invban periodic call modifications
			local_redd_data(local_redd_data.last).REC_TYPE := cTotaliTitoli(i).COD_TIPO_TIT_REDDTA;
			END IF;

		END IF;
		local_redd_data(local_redd_data.last).PROD_C  := cTotaliTitoli(i).PROD_C ;
		local_redd_data(local_redd_data.last).CONTR_N := cTotaliTitoli(i).CONTR_N ;
		contractKey                                   := trim(cTotaliTitoli(i).CONTR_N)||'-'||trim(cTotaliTitoli(i).PROD_C);

		IF(contr_attuali.EXISTS(contractKey)) THEN
			local_redd_data(local_redd_data.last).CONTR_N_PADRE := COALESCE(TRIM(contr_attuali(contractKey).CONTR_N_SERV), local_redd_data(local_redd_data.last).CONTR_N);

		END IF;
		local_redd_data(local_redd_data.last).COD_TIT_INTERN      := cTotaliTitoli(i).COD_TIT_INTERN;
		local_redd_data(local_redd_data.last).COD_INDICE_EMIS_TIT := cTotaliTitoli(i).COD_INDICE_EMIS_TIT;
		local_redd_data(local_redd_data.last).DAT_APER_CONTR      := cTotaliTitoli(i).DAT_APER_CONTR;
		local_redd_data(local_redd_data.last).DAT_CHIU_CONTR      := cTotaliTitoli(i).DAT_CHIU_CONTR;

		IF t1                                                < to_date('01/01/2013','dd/MM/yyyy') OR cTotaliTitoli(i).QTA_QUOTA_T1 < 0 THEN
			local_redd_data(local_redd_data.last).QTA_QUOTA_T1 := 0;

		ELSE
			local_redd_data(local_redd_data.last).QTA_QUOTA_T1 := COALESCE (cTotaliTitoli(i).QTA_QUOTA_T1 , 0);

		END IF;
        
		--- Ai REAL ESTATE non si applica il CUTOFF
		--	sono da considerare anche gli eventi precedenti al 01/01/2013 e la QTA_QUOTA_T1 non si deve annullare prima del CUTOFF
        IF (t1 < to_date('01/01/2013','dd/MM/yyyy') and cTotaliTitoli(i).COD_TIPO_TIT_REDDTA = 'FNDIM' ) THEN 
			local_redd_data(local_redd_data.last).QTA_QUOTA_T1 := COALESCE (cTotaliTitoli(i).QTA_QUOTA_T1 , 0);
        end if;
        
		local_redd_data(local_redd_data.last).DAT_PRIMO_MOVI :=

		CASE
		WHEN local_redd_data(local_redd_data.last).QTA_QUOTA_T1>0 OR cTotaliTitoli(i).DAT_PRIMO_MOVI IS NULL THEN
			t1
		ELSE
			cTotaliTitoli(i).DAT_PRIMO_MOVI
		END;
		local_redd_data(local_redd_data.last).DAT_ULT_MOVI :=

		CASE
		WHEN cTotaliTitoli(i).DAT_ULT_MOVI IS NULL THEN
			t2
		ELSE
			cTotaliTitoli(i).DAT_ULT_MOVI
		END;
		local_redd_data(local_redd_data.last).DAT_RIFE :=

		CASE
		WHEN cTotaliTitoli(i).QTA_QUOTA_T2>0 THEN
			t2
		ELSE
			local_redd_data(local_redd_data.last).DAT_ULT_MOVI
		END;
		local_redd_data(local_redd_data.last).IMP_TOT_VERS            := COALESCE (cTotaliTitoli(i).IMP_TOT_VERS , 0);
		local_redd_data(local_redd_data.last).IMP_TOT_INVEST          := COALESCE (cTotaliTitoli(i).IMP_TOT_INVEST , 0);
		local_redd_data(local_redd_data.last).IMP_TOT_RIMBO           := COALESCE (cTotaliTitoli(i).IMP_TOT_RIMBO , 0);
		local_redd_data(local_redd_data.last).IMP_TOT_VERS_NO_CONVS   := COALESCE (cTotaliTitoli(i).IMP_TOT_VERS , 0);
		local_redd_data(local_redd_data.last).IMP_TOT_INVEST_NO_CONVS := COALESCE (cTotaliTitoli(i).IMP_TOT_INVEST , 0);
		local_redd_data(local_redd_data.last).IMP_TOT_RIMBO_NO_CONVS  := COALESCE (cTotaliTitoli(i).IMP_TOT_RIMBO , 0);
		local_redd_data(local_redd_data.last).IMP_PRVNT_PASS          := COALESCE (cTotaliTitoli(i).IMP_PRVNT_PASS , 0);
		local_redd_data(local_redd_data.last).IMP_PRVNT_FUT           := COALESCE (cTotaliTitoli(i).IMP_PRVNT_FUT , 0);
		local_redd_data(local_redd_data.last).VAL_QUO_T1              :=
			CASE
			WHEN quote_t1.exists(chiaveProdotto) THEN
				quote_t1(chiaveProdotto)
			ELSE
				NULL
			END;



		-- calcolo del Capitale iniziale IMP_CONTVAL_T1	
		IF cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('PCT') THEN
			local_redd_data(local_redd_data.last).IMP_CONTVAL_T1 := COALESCE (cTotaliTitoli(i).IMP_CONTVAL_T1,0); ----il controvalore T1 per i PCT e' elaborato nella calc_totali_titoli
        ELSE
			--def. 50472: per Titoli e INV Bancari il CONTROVALORE a data T1 deve essere valorizzato solo dopo la data di CUTOFF
			IF t1 > to_Date('01-01-2013', 'dd-mm-yyyy') THEN
                IF cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('OBBLI', 'CERT', 'MPB') AND cTotaliTitoli(i).ev_AF_ESPPREZ != 'L' THEN --per 'OBBLI', 'CERT' e 'MPB' dividiamo QTA_QUOTA_T2 per 100
                    local_redd_data(local_redd_data.last).IMP_CONTVAL_T1 := COALESCE (local_redd_data(local_redd_data.last).VAL_QUO_T1*(local_redd_data(local_redd_data.last).QTA_QUOTA_T1/100),0);
                ELSE
                    local_redd_data(local_redd_data.last).IMP_CONTVAL_T1 := COALESCE (local_redd_data(local_redd_data.last).VAL_QUO_T1*local_redd_data(local_redd_data.last).QTA_QUOTA_T1,0);
                END IF;    
			ELSE
				local_redd_data(local_redd_data.last).IMP_CONTVAL_T1 := 0;
			END IF;
		END IF;
		-- calcolo del Capitale iniziale IMP_CONTVAL_T1	per i Real Estate, che non hanno CUTOFF
		IF cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('FNDIM') THEN
			local_redd_data(local_redd_data.last).IMP_CONTVAL_T1 := COALESCE (local_redd_data(local_redd_data.last).VAL_QUO_T1*local_redd_data(local_redd_data.last).QTA_QUOTA_T1,0);
		END IF;

		IF cTotaliTitoli(i).QTA_QUOTA_T2                     > 0 THEN
			local_redd_data(local_redd_data.last).QTA_QUOTA_T2 := cTotaliTitoli(i).QTA_QUOTA_T2;
			---defect 45033: si pone dat_chiu_contr a Null perché nella calc_totali_titoli se nel periodo la QTA_QUOTA_T2 = 0 viene chiuso il contratto
			IF cTotaliTitoli(i).COD_TIPO_TIT_REDDTA NOT IN ('PCT') THEN  ---defect 52144: la data chiusura dei PCT non deve essere null perche è la data SCADENZA del PCT
                local_redd_data(local_redd_data.last).dat_chiu_contr := Null;
            END IF; 

		ELSE
			local_redd_data(local_redd_data.last).QTA_QUOTA_T2 := 0;

		END IF;
		local_redd_data(local_redd_data.last).VAL_QUO_T2 :=
			CASE
			WHEN quote_t2.exists(chiaveProdotto) THEN
				quote_t2(chiaveProdotto)
			ELSE
				NULL
			END;


		-- calcolo del Controvalore IMP_CONTVAL_T2	
		IF cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('PCT') THEN
            local_redd_data(local_redd_data.last).IMP_CONTVAL_T2   := COALESCE (cTotaliTitoli(i).IMP_CONTVAL_T2,0); --il controvalore T2 per i PCT e' elaborato nella calc_totali_titoli
        ELSIF cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('OBBLI', 'CERT', 'MPB') AND cTotaliTitoli(i).ev_AF_ESPPREZ != 'L' THEN --per 'OBBLI', 'CERT' e 'MPB' dividiamo QTA_QUOTA_T2 per 100
			local_redd_data(local_redd_data.last).IMP_CONTVAL_T2   := COALESCE (local_redd_data(local_redd_data.last).VAL_QUO_T2*(local_redd_data(local_redd_data.last).QTA_QUOTA_T2/100), 0);
		ELSE
			local_redd_data(local_redd_data.last).IMP_CONTVAL_T2   := COALESCE (local_redd_data(local_redd_data.last).VAL_QUO_T2*local_redd_data(local_redd_data.last).QTA_QUOTA_T2, 0);
		END IF;		
		local_redd_data(local_redd_data.last).QTA_QUOTA_SALDO  := NULL;
		local_redd_data(local_redd_data.last).IMP_SALDO_VERS   := COALESCE (cTotaliTitoli(i).IMP_SALDO_VERS, 0)    + COALESCE (local_redd_data(local_redd_data.last).IMP_CONTVAL_T1, 0);
		local_redd_data(local_redd_data.last).IMP_SALDO_INVEST := COALESCE (cTotaliTitoli(i).IMP_SALDO_INVEST , 0) + COALESCE (local_redd_data(local_redd_data.last).IMP_CONTVAL_T1, 0);		
		local_redd_data(local_redd_data.last).QTA_SALDO_GIORNL_QUOTA_ZERO := COALESCE(Ctotalititoli(I).QTA_SALDO_GIORNL_QUOTA_ZERO, 0);
        local_redd_data(local_redd_data.last).IMP_INT_VERS                := COALESCE (cTotaliTitoli(i).IMP_INT_VERS, 0)   + COALESCE (( local_redd_data(local_redd_data.last).IMP_CONTVAL_T1 * (local_redd_data(local_redd_data.last).DAT_ULT_MOVI - local_redd_data(local_redd_data.last).DAT_PRIMO_MOVI - local_redd_data(local_redd_data.last).QTA_SALDO_GIORNL_QUOTA_ZERO)),0); 
        local_redd_data(local_redd_data.last).IMP_INT_INVEST              := COALESCE (cTotaliTitoli(i).IMP_INT_INVEST, 0) + COALESCE (( local_redd_data(local_redd_data.last).IMP_CONTVAL_T1 * (local_redd_data(local_redd_data.last).DAT_ULT_MOVI - local_redd_data(local_redd_data.last).DAT_PRIMO_MOVI - local_redd_data(local_redd_data.last).QTA_SALDO_GIORNL_QUOTA_ZERO)),0);
		
		IF cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('PCT') THEN   --def. 52153 MWRR Titoli
			-- i PCT vengono considerati ancora aperti se la data_scadenza è <= della t2. Siccome per la calc_flg_stato vale lo STRETTAMENTE minore si deve portare la t2 indietro di un giorno
			SOGG_OUTELAB.calc_flg_stato(local_redd_data(local_redd_data.last), trunc(t2-1));
        ELSE
			SOGG_OUTELAB.calc_flg_stato(local_redd_data(local_redd_data.last), t2);
        END IF;
		SOGG_OUTELAB.calc_indici_redd(local_redd_data(local_redd_data.last));
		---settaggio PLUS MINUS		
		IF(cTotaliTitoli(i).COD_TIPO_TIT_REDDTA IN ('PCT')) THEN
            local_redd_data(local_redd_data.last).DAT_SCAD_CONTR := local_redd_data(local_redd_data.last).DAT_CHIU_CONTR;
			-- valorizzazione del PLUS MINUS solo per i PCT. Per gli altri prodotti avviene in post processing: PROCEDURE calcola_plusminus_Titoli            
            local_redd_data(local_redd_data.last).IMP_PLUS_MINUS_VERS   := COALESCE(Ctotalititoli(I).IMP_PLUS_MINUS_VERS_PCT,0);
			local_redd_data(local_redd_data.last).IMP_PLUS_MINUS_INVEST := COALESCE(Ctotalititoli(I).IMP_PLUS_MINUS_INVEST_PCT,0);          
        END IF;

	END LOOP;
	
	
    -- RFC#110777 def. 64192 
	-- Se siamo nel caso di CAMBIO CODICE ISIN per un real estate FondoA --> FondoB ed il fondo origine 'FondoA' POSSIEDE UN CAPITALE INIZIALE
	-- Il capitale iniziale del FondoA deve diventare il capitale iniziale del FondoB, quindi essere passato al fondo destinazione 'FondoB'.
	-- Quindi si integra la procedura verificaCambioIsinRE che, essendo nella calc_totali_titoli, non calcola i controvalori.
	-- Nella verificaCambioIsinRE viene intercettato l'indice del fondo origine e con esso risaliamo ad IMP_CONTVAL_T1 del fondo origine
	-- che sara' riversato nel controvalore T1 del fondo destinazione. Infine oscuriamo il fondo origine ('NOSEE').
	-- Viene rifatto il ciclo, perche' possiamo avere un fondo origine con indice successivo al fondo di destinazione, partendo dalla fine (REVERSE). 
	FOR i IN REVERSE 1 .. local_redd_data.COUNT
    loop
    -- nella variabile numerica FNDIM_INDEX e' stato immagazzinato l'indice del fondo origine dalla VerificaCambioIsinRe nella della calc_totali_titoli.
	-- l'elaborazione che segue viene quindi effettuata solo se: 
	-- 1) FNDIM_INDEX e' valorizzato 
	-- 2) esiste un controvalore a T1 del fondo origine
            if cTotaliTitoli(i).FNDIM_INDEX <> 0 and local_redd_data(ctotaliTitoli(i).FNDIM_INDEX).IMP_CONTVAL_T1 >0 then
				local_redd_data(i).IMP_CONTVAL_T1   := local_redd_data(ctotaliTitoli(i).FNDIM_INDEX).IMP_CONTVAL_T1;
				-- resetto come DAT_PRIMO_MOVI quella del fondo origine, per calcolare correttamente l'MWRR, visto che ho acquisito l'IMP_SALDO_VERS del fondo origine 
                local_redd_data(i).DAT_PRIMO_MOVI:=local_redd_data(ctotaliTitoli(i).FNDIM_INDEX).DAT_PRIMO_MOVI;
				
				           if NVL(local_redd_data(i).IMP_SALDO_VERS,0)=0 then
                --dbms_output.put_line('NVL(local_redd_data(i).IMP_SALDO_VERS,0)=0    '||local_redd_data(i).IMP_SALDO_VERS);
                    local_redd_data(i).IMP_SALDO_VERS := COALESCE (cTotaliTitoli(i).IMP_SALDO_VERS, 0)    + local_redd_data(i).IMP_CONTVAL_T1;
                    local_redd_data(i).IMP_SALDO_INVEST := COALESCE (cTotaliTitoli(i).IMP_SALDO_INVEST, 0)    + local_redd_data(i).IMP_CONTVAL_T1;
                    local_redd_data(i).IMP_INT_VERS       := local_redd_data(i).IMP_SALDO_VERS *   (local_redd_data(i).DAT_ULT_MOVI - local_redd_data(i).DAT_PRIMO_MOVI);
                    local_redd_data(i).IMP_INT_INVEST     := local_redd_data(i).IMP_SALDO_INVEST * (local_redd_data(i).DAT_ULT_MOVI - local_redd_data(i).DAT_PRIMO_MOVI);
                end if;
							
				-- dopo aver importato i valori salienti viene oscurato il fondo origine per le varie lob
                local_redd_data(ctotaliTitoli(i).FNDIM_INDEX).REC_TYPE :='NOSEE';				   
                -- e per la lob investimenti
                local_redd_data(ctotaliTitoli(i).FNDIM_INDEX).LOB_C :='NOSEE';
				--per non oscurare un fondo risultante da un cambio ISIN di tipo A->B->A: devo riportare il fondo finale A a non essere NOSEE 
				local_redd_data(i).REC_TYPE :='FND_IMMB';
				--per non oscurare un fondo risultante da un cambio ISIN di tipo A->B->A
				local_redd_data(i).LOB_C :='FND';
                
				cTotaliTitoli(i).FNDIM_INDEX := 0;
                  
                -- infine vengono calcolate la VP e il MWRR del Fondo destinazione
				SOGG_OUTELAB.calc_indici_redd(local_redd_data(i));
            end if;    
    end loop;     	-- end RFC#110777
		
	popola_dati_accessori_per(local_redd_data);
	cRec := cRec multiset

	UNION ALL local_redd_data;

END;

PROCEDURE legg_val_quota_per_tit(
		chiavi IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT,
		t      IN DATE,
		quote OUT tb_val_quo)
IS
type ct
IS
	TABLE OF c_load_val_quo_per_tit%rowtype;
	cv ct;
BEGIN
	OPEN c_load_val_quo_per_tit(chiavi, t);

	FETCH c_load_val_quo_per_tit bulk collect INTO cv;

	CLOSE c_load_val_quo_per_tit;

	FOR i IN 1 .. cv.COUNT
	LOOP
		quote( cv(i)."KEY" ) := cv(i)."VAL_EUR";

	END LOOP;

END;


PROCEDURE legg_val_quota_per_tit_crusc(
		t      IN DATE,
		quote OUT tb_val_quo)
IS
type ct
IS
	TABLE OF c_load_val_quo_per_tit_crusc%rowtype;
	cv ct;
BEGIN
	OPEN c_load_val_quo_per_tit_crusc(t);

	FETCH c_load_val_quo_per_tit_crusc bulk collect INTO cv;

	CLOSE c_load_val_quo_per_tit_crusc;

	FOR i IN 1 .. cv.COUNT
	LOOP
		quote( cv(i)."KEY" ) := cv(i)."VAL_EUR";

	END LOOP;

END;
PROCEDURE calc_redd_giornaliero(
		chiavi IN OUT nocopy SOGG_OUTELAB.TB_KEY_CONTR,
		dt_rif DATE,
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE)
IS
type tb_redd
IS
	TABLE OF c_load_redd_gior%rowtype;
	dati_giornaliero tb_redd;
	dati_output SOGG_OUTELAB.TB_OUTPUT_ONLINE;
	lobcode CHAR(5):='FND';
	contr_attuali tb_contr;
	contractKey VARCHAR(32);
BEGIN
	legg_contr_info(chiavi, contr_attuali);
	dati_output := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	OPEN c_load_redd_gior(chiavi, dt_rif);

	FETCH c_load_redd_gior bulk collect INTO dati_giornaliero;

	CLOSE c_load_redd_gior;

	FOR i IN 1 .. dati_giornaliero.COUNT
	LOOP
		dati_output.extend(1);

		IF(dati_giornaliero(i).COD_TIPO_TIT_REDDTA = 'FNDIM') THEN
			dati_output(dati_output.last)            := SOGG_OUTELAB.RT_OUTPUT_ONLINE('FND');
			dati_output(dati_output.last).REC_TYPE   := 'FND_IMMB';

		ELSE

			IF NVL(dati_giornaliero(i).DAT_CHIU_CONTR, TO_DATE('31/12/9999', 'DD/MM/YYYY')) < TO_DATE('01/01/2013', 'DD/MM/YYYY') THEN
				dati_output(dati_output.last)                                                 := SOGG_OUTELAB.RT_OUTPUT_ONLINE('NOSEE');

			ELSE

				IF(dati_giornaliero(i).COD_TIPO_TIT_REDDTA IN ('PCT', 'MPB', 'CERT')) THEN
					dati_output(dati_output.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('INVBAN');

				ELSIF(dati_giornaliero(i).COD_TIPO_TIT_REDDTA IN ('OBBLI', 'AZION')) THEN
					dati_output(dati_output.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('TIT');
				ELSE    					--inizializzo eventuali NOSEE provenienti dalla tabella di saldo
                    dati_output(dati_output.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('NOSEE');

				END IF;

			END IF;
			dati_output(dati_output.last).REC_TYPE := dati_giornaliero(i).COD_TIPO_TIT_REDDTA;

		END IF;
		dati_output(dati_output.last).PROD_C  := dati_giornaliero(i).PROD_C;
		dati_output(dati_output.last).CONTR_N := dati_giornaliero(i).CONTR_N;
		contractKey                           := trim(dati_output(dati_output.last).CONTR_N)||'-'||trim(dati_output(dati_output.last).PROD_C);

		IF(contr_attuali.EXISTS(contractKey)) THEN
			dati_output(dati_output.last).CONTR_N_PADRE := COALESCE(TRIM(contr_attuali(contractKey).CONTR_N_SERV), dati_output(dati_output.last).CONTR_N);

		END IF;
		dati_output(dati_output.last).COD_TIT_INTERN          := dati_giornaliero(i).COD_TIT_INTERN;
		dati_output(dati_output.last).COD_INDICE_EMIS_TIT     := dati_giornaliero(i).COD_INDICE_EMIS_TIT;
		dati_output(dati_output.last).DAT_RIFE                := dati_giornaliero(i).DAT_RIFE;
		dati_output(dati_output.last).DAT_APER_CONTR          := dati_giornaliero(i).DAT_APER_CONTR;
		dati_output(dati_output.last).DAT_CHIU_CONTR          := dati_giornaliero(i).DAT_CHIU_CONTR;
		dati_output(dati_output.last).DAT_SCAD_CONTR          := dati_giornaliero(i).DAT_CHIU_CONTR;
		dati_output(dati_output.last).DAT_PRIMO_MOVI          := dati_giornaliero(i).DAT_PRIMO_MOVI;
		dati_output(dati_output.last).DAT_ULT_MOVI            := dati_giornaliero(i).DAT_ULT_MOVI;
		dati_output(dati_output.last).IMP_TOT_VERS            := dati_giornaliero(i).IMP_TOT_VERS;
		dati_output(dati_output.last).IMP_TOT_INVEST          := dati_giornaliero(i).IMP_TOT_INVEST;
		dati_output(dati_output.last).IMP_TOT_VERS_NO_CONVS   := dati_giornaliero(i).IMP_TOT_VERS;
		dati_output(dati_output.last).IMP_TOT_INVEST_NO_CONVS := dati_giornaliero(i).IMP_TOT_INVEST;
		dati_output(dati_output.last).IMP_TOT_RIMBO_NO_CONVS  := dati_giornaliero(i).IMP_TOT_RIMBO;
		dati_output(dati_output.last).IMP_TOT_RIMBO           := dati_giornaliero(i).IMP_TOT_RIMBO;
		dati_output(dati_output.last).IMP_PRVNT_PASS          := dati_giornaliero(i).IMP_PRVNT_PASS;
		dati_output(dati_output.last).QTA_QUOTA_T1            := 0;
		dati_output(dati_output.last).QTA_QUOTA_T2            := dati_giornaliero(i).QTA_QUOTA_T2;
		dati_output(dati_output.last).IMP_INT_VERS            := dati_giornaliero(i).IMP_INT_VERS;
		dati_output(dati_output.last).IMP_SALDO_VERS          := dati_giornaliero(i).IMP_SALDO_VERS;
		dati_output(dati_output.last).IMP_INT_INVEST          := dati_giornaliero(i).IMP_INT_INVEST;
		dati_output(dati_output.last).IMP_SALDO_INVEST        := dati_giornaliero(i).IMP_SALDO_INVEST;
		dati_output(dati_output.last).IMP_CONTVAL_T1          := 0;
		dati_output(dati_output.last).IMP_CONTVAL_T2          := dati_giornaliero(i).IMP_CONTVAL;
		dati_output(dati_output.last).VAL_QUO_T2              :=

		CASE
		WHEN dati_output(dati_output.last).QTA_QUOTA_T2 <> 0 AND dati_giornaliero(i).IMP_CONTVAL <> 0 THEN
			CASE
			WHEN dati_giornaliero(i).COD_TIPO_TIT_REDDTA = 'OBBLI' THEN
				(dati_giornaliero(i).IMP_CONTVAL*100)/dati_output(dati_output.last).QTA_QUOTA_T2

			ELSE
				dati_giornaliero(i).IMP_CONTVAL/dati_output(dati_output.last).QTA_QUOTA_T2

			END

		ELSE
			NULL

		END;
		dati_output(dati_output.last).IMP_PLUS_MINUS_INVEST        := dati_giornaliero(i).IMP_PLUS_MINUS_INVEST;
		dati_output(dati_output.last).IMP_PLUS_MINUS_VERS          := dati_giornaliero(i).IMP_PLUS_MINUS_VERS;
		dati_output(dati_output.last).QTA_QUOTA_SALDO              := NULL;
		dati_output(dati_output.last).PRC_REND_VPATR_VERS_ST       := dati_giornaliero(i).PRC_REND_VPATR_VERS_ST;
		dati_output(dati_output.last).PRC_REND_VPATR_VERS_ANNUAL   := dati_giornaliero(i).PRC_REND_VPATR_VERS_ANNUAL;
		dati_output(dati_output.last).PRC_REND_VPATR_INVEST_ST     := dati_giornaliero(i).PRC_REND_VPATR_INVEST_ST;
		dati_output(dati_output.last).PRC_REND_VPATR_INVEST_ANNUAL := dati_giornaliero(i).PRC_REND_VPATR_INVEST_ANNUAL;


		IF dati_output(dati_output.last).REC_TYPE                   = 'PCT' THEN 				--IN('FND_IMMB','PCT')
			dati_output(dati_output.last).PRC_REND_MWRR_VERS_ST       := NULL;
			dati_output(dati_output.last).PRC_REND_MWRR_VERS_ANNUAL   := NULL;
			dati_output(dati_output.last).PRC_REND_MWRR_INVEST_ST     := NULL;
			dati_output(dati_output.last).PRC_REND_MWRR_INVEST_ANNUAL := NULL;

		ELSE
			dati_output(dati_output.last).PRC_REND_MWRR_VERS_ST       := dati_giornaliero(i).PRC_REND_MWRR_VERS_ST;
			dati_output(dati_output.last).PRC_REND_MWRR_VERS_ANNUAL   := dati_giornaliero(i).PRC_REND_MWRR_VERS_ANNUAL;
			dati_output(dati_output.last).PRC_REND_MWRR_INVEST_ST     := dati_giornaliero(i).PRC_REND_MWRR_INVEST_ST;
			dati_output(dati_output.last).PRC_REND_MWRR_INVEST_ANNUAL := dati_giornaliero(i).PRC_REND_MWRR_INVEST_ANNUAL;

		END IF;
		SOGG_OUTELAB.calc_flg_stato(dati_output(dati_output.last), dt_rif);
		popola_da_posiztit_daily(dati_output(dati_output.last));
		popola_dati_anagrafici_daily(dati_output(dati_output.last));
		popola_flag_evento_barriera(dati_output(dati_output.last));

		IF dati_output(dati_output.last).Rec_Type IN ('CERT','PCT') THEN
			dati_output(dati_output.last).VAL_CAR := dati_output(dati_output.last).IMP_TOT_VERS;

		END IF;
		dati_output(dati_output.last).IMP_GNR_01 := CLL.PKG_CALCOLO_REDDITIVITA_V2.CALC_REDD_INDEX_TITOLO(dati_output(dati_output.last).IMP_CONTVAL_T2, dati_output(dati_output.last).VAL_CAR, 0);

		IF dati_output(dati_output.last).IMP_GNR_01 IS NOT NULL THEN
			dati_output(dati_output.last).IMP_GNR_02   := CLL.PKG_CALCOLO_REDDITIVITA_V2.CALC_REDD_COMPLEX_INDEX_TITOLO(dati_output(dati_output.last).IMP_GNR_01, dati_output(dati_output.last).DAT_APER_CONTR, dt_rif);

		ELSE
			dati_output(dati_output.last).IMP_GNR_01 := NULL;

		END IF;

	END LOOP;
	cRec := cRec multiset

	UNION ALL dati_output;

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

PROCEDURE aggiorna_real_estate_list
IS
	row_is_real_estate_cur is_real_estate_cur%ROWTYPE;
BEGIN

	IF is_real_estate_list.COUNT = 0 THEN
		OPEN is_real_estate_cur;
		LOOP

			FETCH is_real_estate_cur INTO row_is_real_estate_cur;
			EXIT
		WHEN is_real_estate_cur%NOTFOUND;
			is_real_estate_list(row_is_real_estate_cur.tit_tran) := row_is_real_estate_cur.rownum;

		END LOOP;
		CLOSE is_real_estate_cur;

	END IF;

END;

PROCEDURE aggiorna_tipo_cedola_list_re
IS
	row_config_cur config_cur%ROWTYPE;
	tipo_op_re_key VARCHAR(50);
	tipo_op_value  VARCHAR(50);
BEGIN

	IF tipo_op_cedola_list_re.COUNT = 0 THEN
		--tipo operazione per RE
		OPEN config_cur('CAUS_FN_RE');
		LOOP

			FETCH config_cur INTO row_config_cur;
			EXIT
		WHEN config_cur%NOTFOUND;
			tipo_op_re_key                         := row_config_cur.tipo_op_key;
			tipo_op_value                          := trim(row_config_cur.tipo_op_key);
			tipo_op_cedola_list_re(tipo_op_re_key) := tipo_op_value;

		END LOOP;
		CLOSE config_cur;

	END IF;

END;

PROCEDURE load_regole_causali
IS
	row_config_cur config_cur%ROWTYPE;
	tipo_op_re_key VARCHAR(50);
	tipo_op_value  VARCHAR(50);
BEGIN

	IF tipo_op_regole_causali.COUNT = 0 THEN
		--tipo operazione per RE
		OPEN config_cur('REG_TIT');
		LOOP

			FETCH config_cur INTO row_config_cur;
			EXIT
		WHEN config_cur%NOTFOUND;
			tipo_op_re_key                         := row_config_cur.tipo_op_key;
			tipo_op_value                          := trim(row_config_cur.tp_op_val);
			tipo_op_regole_causali(tipo_op_re_key) := tipo_op_value;
			--dbms_output.put_line('Load causali '||tipo_op_re_key||'-'||tipo_op_value);

		END LOOP;
		CLOSE config_cur;

	END IF;

END;
PROCEDURE load_cambi
IS
	row_config_cur config_cur%ROWTYPE;
	tipo_op_re_key VARCHAR(50);
	tipo_op_value  VARCHAR(50);
BEGIN

	IF tipo_op_cambi.COUNT = 0 THEN
		--tipo operazione per RE
		for c1 in 
		(
			select prodattfin_c, QUOTAZ_I_VAL_MERCATO from cll.foto_quotaz where prodattfin_c in(
				select cast(cod_campo_mwrr as char(15)) from SOGG_OUTELAB.MULTIDOM_MWRR where cod_tipo_dom_mwrr = 'VAL_TIT'
			)
		)
		LOOP		
			tipo_op_re_key                         := trim(c1.prodattfin_c);
			tipo_op_value                          := c1.QUOTAZ_I_VAL_MERCATO;
			tipo_op_cambi(tipo_op_re_key) := tipo_op_value;
			--dbms_output.put_line('Load cambi '||tipo_op_re_key||'-'||tipo_op_value);

		END LOOP;		

	END IF;

END load_cambi;
PROCEDURE aggiorna_tipo_cedola_list_tit
IS
	row_config_cur config_cur%ROWTYPE;
	tipo_op_tit_key VARCHAR(50);
	tipo_op_value   VARCHAR(50);
BEGIN

	IF tipo_op_cedola_list_tit.COUNT = 0 THEN
		--tipo operazione per TIT
		OPEN config_cur('CAUS_TIT');
		LOOP

			FETCH config_cur INTO row_config_cur;
			EXIT
		WHEN config_cur%NOTFOUND;
			tipo_op_tit_key                          := row_config_cur.tipo_op_key;
			tipo_op_value                            := trim(row_config_cur.tipo_op_key);
			tipo_op_cedola_list_tit(tipo_op_tit_key) := tipo_op_value;

		END LOOP;
		CLOSE config_cur;

	END IF;

END;

PROCEDURE aggiorna_bond_isin_tit
IS
	row_config_cur config_cur%ROWTYPE;
	tipo_bi_key   VARCHAR(50);
	tipo_bi_value VARCHAR(50);
BEGIN

	IF tipo_bi_list_tit.COUNT = 0 THEN
		--bi per TIT
		OPEN config_cur('BOND_ISIN');
		LOOP

			FETCH config_cur INTO row_config_cur;
			EXIT
		WHEN config_cur%NOTFOUND;
			--tipo_bi_key                   := row_config_cur.tipo_op_key; --Rilascio fix defect 36139
			tipo_bi_key                   := trim(row_config_cur.tp_op_val);
			tipo_bi_value                 := trim(row_config_cur.tp_op_val);
			tipo_bi_list_tit(tipo_bi_key) := tipo_bi_value;

		END LOOP;
		CLOSE config_cur;

	END IF;

END;

PROCEDURE Elimina_Pct_Malformati(
		datiRedd IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT )
IS
BEGIN

	FOR i IN 1 .. datiRedd.COUNT
	LOOP

		IF (Datiredd(I).Cod_Tipo_Tit_Reddta = 'PCT' AND Datiredd(I).Pct_Num_Transazioni != 2) THEN
			Datiredd(I).Cod_Tipo_Tit_Reddta   := 'NOSEE';

		END IF;

	END LOOP;

END;

PROCEDURE Update_qtaQuotaT2_titles(
		datiRedd IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT )
IS
	QTA_QUOTA NUMBER;
BEGIN

	FOR i IN 1 .. datiRedd.COUNT
	LOOP
		BEGIN

			IF datiRedd(i).COD_TIPO_TIT_REDDTA NOT IN ('NOSEE','PCT') THEN
				QTA_QUOTA := 0;
				OPEN c_posiztit_qtaQuota_tit(datiRedd(i));

				FETCH c_posiztit_qtaQuota_tit INTO QTA_QUOTA;

				datiRedd(i).QTA_QUOTA_T2 := COALESCE(QTA_QUOTA,0);
				CLOSE c_posiztit_qtaQuota_tit;

				IF datiRedd(i).QTA_QUOTA_T2  = 0 THEN
					datiRedd(i).DAT_CHIU_CONTR := datiRedd(i).DAT_ULT_MOVI;

				END IF;

			END IF;

		EXCEPTION

		WHEN OTHERS THEN
			datiRedd(i).QTA_QUOTA_T2   := 0;
			datiRedd(i).DAT_CHIU_CONTR := datiRedd(i).DAT_ULT_MOVI;
			CLOSE c_posiztit_qtaQuota_tit;

		END;

	END LOOP;

END;

PROCEDURE aggiorna_lista_re_cambio_isin
IS
	row_cambio_isin c_re_cambio_isin%ROWTYPE;
	lista_key VARCHAR(40);
BEGIN
    --nel caso in cui l'array con la lista fondi partenza/arrivo per il cambio ISIN non sia stata ancora caricata,
    --viene aperto il cursore e l'array riempito con il risultato del cursore
    --DBMS_OUTPUT.PUT_LINE('lista_re_cambio_isin.COUNT: '||lista_re_cambio_isin.COUNT);
	IF lista_re_cambio_isin.COUNT = 0 THEN
        --DBMS_OUTPUT.PUT_LINE('ARRAY ISIN RE VUOTO');
		OPEN c_re_cambio_isin;
		LOOP

			FETCH c_re_cambio_isin INTO row_cambio_isin;
			EXIT
            WHEN c_re_cambio_isin%NOTFOUND;
            --alla chiave viene aggiunto in fondo uno -0 per farlo collimare con la chiave creata in calc_totali, che contiene anche il COD_TRANS_ACQU_PCT,
            --necessario per i PCT ma non utile in questo caso
            lista_key := row_cambio_isin.CONTR_N||'-'||'BAN12'||'-'||row_cambio_isin.COD_TIT_INTERN_SORG||'-'||row_cambio_isin.COD_INDICE_EMIS_TIT_SORG||'-0-';
			lista_re_cambio_isin(lista_key) := row_cambio_isin;
            --DBMS_OUTPUT.PUT_LINE('CHIAVE: '||lista_key||' '||'VALORE: '||row_cambio_isin.COD_TIT_INTERN_DEST||'-'||row_cambio_isin.COD_INDICE_EMIS_TIT_DEST);
		END LOOP;
		CLOSE c_re_cambio_isin;

	END IF;

END;

/*rfc 90228 - CR152 - estrazione dei dossier privi di titoli sottostanti - REC_TYPE='DOS'*/
-- Abbiamo 3 casi: 1. DT aperto ma mai movimento; 2. DT chiuso;  3. DT aperto ma con tutti i titoli disinvestiti.
PROCEDURE aggDossierApertoNonMov(
			chiavi IN OUT NoCopy SOGG_OUTELAB.TB_KEY_CONTR,
			datiRedd IN OUT Nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
			t2 date,
			t1 date default NULL)
IS
    type tb_DOS
    IS
        TABLE OF c_aggDossierApertoNonMov%rowtype;
    contr_DOS tb_DOS;
    dati_output SOGG_OUTELAB.TB_OUTPUT_ONLINE;
    tt1 date;
    tt2 date;

BEGIN
    dati_output:=SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	tt1 := coalesce(t1, to_date('01011753','ddmmyyyy'));
    tt2 :=t2;

    OPEN c_aggDossierApertoNonMov(datiRedd,chiavi,tt2,tt1);
    FETCH c_aggDossierApertoNonMov BULK COLLECT INTO contr_DOS;
    CLOSE c_aggDossierApertoNonMov;

    for i in 1 .. contr_DOS.count
    LOOP
        dati_output.extend(1);
        dati_output(dati_output.last):= SOGG_OUTELAB.RT_OUTPUT_ONLINE( NULL );
        dati_output(dati_output.last).PROD_C := contr_DOS(i).PROD_C;
        dati_output(dati_output.last).CONTR_N := contr_DOS(i).CONTR_N;
        dati_output(dati_output.last).CONTR_N_PADRE := contr_DOS(i).CONTR_N;
        dati_output(dati_output.last).REC_TYPE := 'DOS';
        dati_output(dati_output.last).DAT_APER_CONTR := contr_DOS(i).DAT_APER_CONTR;
        dati_output(dati_output.last).DAT_CHIU_CONTR := contr_DOS(i).DAT_CHIU_CONTR;
		-- un DOS può essere aperto o chiuso quindi dobbiamo settare il flag FLG_CHIUSO:
		SOGG_OUTELAB.calc_flg_stato(dati_output(dati_output.last), tt2);
    END LOOP;

    datiRedd := datiRedd multiset UNION ALL dati_output;

END;   --PROCEDURE aggDossierApertoNonMov




PROCEDURE verificaCambioIsinRE(
        in_cTotaliTitoli IN OUT nocopy tb_myIndex,
        cTotaliTitoli    IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT,
        cEventoTitoli    IN OUT nocopy eventi_titoli%rowtype,
        contractKey      IN VARCHAR,
        contractIndex    IN NUMBER)
IS  
        contractKeyDestRECambioISIN VARCHAR(100);
        contractIndexDestRECambioISIN NUMBER;
BEGIN

        --CR102 - Per i fondi real estate (RE) sono individuabili degli eventi di cambio codice titolo riferiti agli anni 2010, 2011, 2012 e 2016. 
        -- Con il cambio codice titolo viene spostata la quantitÃ  da un titolo origine A ad un titolo destinazione B; il titolo originario A diviene 
        --totalmente disinvestito a favore del titolo destinazione B. I titoli origine e destinazione appartengono allo stesso dossier titoli (DT). 
        --Questi eventi sono identificabili attraverso unÂ¿analisi delle movimentazioni, utilizzando come causale il codice Â¿XZÂ¿.         
        --Nel caso in cui intercetto un movimento di cambio isin, tale evento sarÃ  l'ultimo evento che verrÃ  eseguito sul prodotto. Arrivato questo
        --evento, il valore aggregato di versato, rimborsi, proventi, verranno sommati sul titolo di destinazione di quella che Ã¨, a tutti gli 
        --effetti, una specie di fusione.
        --Nel caso in cui sia un evento di cambio isin di disinvestimento, cioè avvenuto sul fondo sorgente
        --DBMS_OUTPUT.PUT_LINE('cEventoTitoli.OPERZTIT_C_CAUS '||cEventoTitoli.OPERZTIT_C_CAUS);
        IF cEventoTitoli.OPERZTIT_C_CAUS = 'XZ' AND cEventoTitoli.SEGNO < 0 THEN
        --cEventoTitoli.OPERZTIT_C_CAUS = 'XQ' THEN
            --DBMS_OUTPUT.PUT_LINE('cEventoTitoli.OPERZTIT_C_CAUS = XZ');
            --DBMS_OUTPUT.PUT_LINE(contractKey);
            --verifico che il prodotto sia tra quelli listati nella tabella e soggetti a fusione verso un fondo di destinazione
            IF lista_re_cambio_isin.EXISTS(contractKey) THEN
                --creo la chiave del prodotto di destinazione
                --DBMS_OUTPUT.PUT_LINE('IF lista_re_cambio_isin.EXISTS(contractKey) THEN');
                contractKeyDestRECambioISIN := lista_re_cambio_isin(contractKey).CONTR_N||'-'||'BAN12'||'-'||lista_re_cambio_isin(contractKey).COD_TIT_INTERN_DEST ||'-'||lista_re_cambio_isin(contractKey).COD_INDICE_EMIS_TIT_DEST||'-0-';
                --se il prodotto di destinazione è già presente nell'array dei totali non lo creo, prendo solo la chiave per accedere
                --al prodotto destinazione successivamente e sommare dentro i valori del prodotto origine
                --DBMS_OUTPUT.PUT_LINE('contractKeyDestRECambioISIN');
                IF in_cTotaliTitoli.EXISTS(contractKeyDestRECambioISIN) THEN
                    contractIndexDestRECambioISIN := in_cTotaliTitoli(contractKeyDestRECambioISIN);
                    --DBMS_OUTPUT.PUT_LINE('IF in_cTotaliTitoli.EXISTS(contractKeyDestRECambioISIN)');
                    --Nel caso di cambio A->B->A (casistica successiva al 2016), il fondo A al primo cambio è stato settato a NOSEE e bisogna ora riattivarlo
                    --per farlo vedere
                    cTotaliTitoli(contractIndexDestRECambioISIN).COD_TIPO_TIT_REDDTA := 'FNDIM';
                --se il prodotto di destinazione non Ã¨ presente nell'array dei totali, provvedo a crearlo perchÃ© mi serve immediatamente
                --per sommarci dentro i valori del prodotto origine. Questo non ha impatto sul prodotto di destinazione: quando il primo evento verrÃ  per il 
                --prodotto di destinazione, semplicemente il codice troverÃ  la relativa riga nell'array giÃ  creata.
                ELSE
 
                    cTotaliTitoli.extend();
                    in_cTotaliTitoli(contractKeyDestRECambioISIN) := cTotaliTitoli.last;
                    cTotaliTitoli(cTotaliTitoli.last)  := SOGG_OUTELAB.RT_TOTALS_TIT( 'BAN12', lista_re_cambio_isin(contractKey).CONTR_N, lista_re_cambio_isin(contractKey).COD_TIT_INTERN_DEST, lista_re_cambio_isin(contractKey).COD_INDICE_EMIS_TIT_DEST, NULL, NULL );
                    cTotaliTitoli(cTotaliTitoli.last).COD_TIPO_TIT_REDDTA := 'FNDIM';

                    --contractIndexDestRECambioISIN := in_cTotaliTitoli(contractKeyDestRECambioISIN);
                    --DBMS_OUTPUT.PUT_LINE('cTotaliTitoli.extend();');
                    contractIndexDestRECambioISIN := in_cTotaliTitoli(contractKeyDestRECambioISIN);
                END IF;                
                -- vengono aggiunti al fondo di destinazione i valori del fondo sorgente
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_TOT_VERS     := cTotaliTitoli(contractIndexDestRECambioISIN).IMP_TOT_VERS + cTotaliTitoli(contractIndex).IMP_TOT_VERS;
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_TOT_INVEST     := cTotaliTitoli(contractIndexDestRECambioISIN).IMP_TOT_INVEST + cTotaliTitoli(contractIndex).IMP_TOT_INVEST;
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_TOT_RIMBO     := cTotaliTitoli(contractIndexDestRECambioISIN).IMP_TOT_RIMBO + cTotaliTitoli(contractIndex).IMP_TOT_RIMBO;
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_SALDO_VERS     := cTotaliTitoli(contractIndexDestRECambioISIN).IMP_SALDO_VERS + cTotaliTitoli(contractIndex).IMP_SALDO_VERS;
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_SALDO_INVEST     := cTotaliTitoli(contractIndexDestRECambioISIN).IMP_SALDO_INVEST + cTotaliTitoli(contractIndex).IMP_SALDO_INVEST;
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_PRVNT_PASS     :=cTotaliTitoli(contractIndexDestRECambioISIN).IMP_PRVNT_PASS +  cTotaliTitoli(contractIndex).IMP_PRVNT_PASS;
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_INT_VERS     :=cTotaliTitoli(contractIndexDestRECambioISIN).IMP_INT_VERS+cTotaliTitoli(contractIndex).IMP_INT_VERS;
                cTotaliTitoli(contractIndexDestRECambioISIN).IMP_INT_INVEST     :=cTotaliTitoli(contractIndexDestRECambioISIN).IMP_INT_INVEST+cTotaliTitoli(contractIndex).IMP_INT_INVEST;
                cTotaliTitoli(contractIndexDestRECambioISIN).QTA_SALDO_GIORNL_QUOTA_ZERO := cTotaliTitoli(contractIndexDestRECambioISIN).QTA_SALDO_GIORNL_QUOTA_ZERO+cTotaliTitoli(contractIndex).QTA_SALDO_GIORNL_QUOTA_ZERO;
				cTotaliTitoli(contractIndexDestRECambioISIN).QTA_QUOTA_T2 := cTotaliTitoli(contractIndex).QTA_QUOTA_T2 + cTotaliTitoli(contractIndexDestRECambioISIN).QTA_QUOTA_T2 ;
				-- la data apertura contratto deve essere la MIN tra quella del fondo Origine e del fondo Target, dato che si puÃ² avere un fondo
				-- Target aperto prima del fondo cosiddetto Origine, cioÃ¨ del fondo che confluira' in quello Target
				
				-- RFC#110777 def. 64192. Recupero del controvalore IMP_CONT_VAL_T1 in un periodico: il Fondo Target eredita il Capitale Iniziale dal Fondo Origine, 
				-- Viene salvatato l'indice del fondo di origine nella variabile FNDIM_INDEX 
				-- in modo tale da poter recuperare, se presente, il valore del Capitale Iniziale del Fondo Origine da passare
				-- integralmente al Fondo Target all'interno della procedura calc_redd_periodo (nella calc_totali_titoli i controvalori non sono calcolati)
                cTotaliTitoli(contractIndexDestRECambioISIN).FNDIM_INDEX := contractIndex;
        
                cTotaliTitoli(contractIndexDestRECambioISIN).DAT_APER_CONTR :=
                CASE
                    WHEN SIGN(COALESCE(cTotaliTitoli(contractIndexDestRECambioISIN).DAT_APER_CONTR,TO_DATE('99990101','yyyymmdd')) - cTotaliTitoli(contractIndex).DAT_APER_CONTR) >=0 THEN
                        cTotaliTitoli(contractIndex).DAT_APER_CONTR
                    ELSE
                        cTotaliTitoli(contractIndexDestRECambioISIN).DAT_APER_CONTR
                END;
				-- per un corretto calcolo dell'MWRR la data primo movimento deve essere la MIN tra quella del fondo Origine e quella del fondo Target
				cTotaliTitoli(contractIndexDestRECambioISIN).DAT_PRIMO_MOVI     :=
                CASE
                    WHEN SIGN(COALESCE(cTotaliTitoli(contractIndexDestRECambioISIN).DAT_PRIMO_MOVI,TO_DATE('99990101','yyyymmdd')) - cTotaliTitoli(contractIndex).DAT_PRIMO_MOVI) >=0 THEN
                        cTotaliTitoli(contractIndex).DAT_PRIMO_MOVI
                    ELSE
                        cTotaliTitoli(contractIndexDestRECambioISIN).DAT_PRIMO_MOVI

                END;
                cTotaliTitoli(contractIndexDestRECambioISIN).DAT_ULT_MOVI     := cEventoTitoli.DAT_RIF;
				 --Il fondo sorgente viene reso invisibile e vengono azzerati tutti i valori relativi ad esso
                cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA := 'NOSEE';
                --DBMS_OUTPUT.PUT_LINE('cTotaliTitoli(contractIndex).COD_TIPO_TIT_REDDTA := NOSEE');
				cTotaliTitoli(contractIndex).IMP_TOT_VERS			:=0;
				cTotaliTitoli(contractIndex).IMP_TOT_INVEST			:=0;
				cTotaliTitoli(contractIndex).IMP_TOT_RIMBO			:=0;
				cTotaliTitoli(contractIndex).IMP_SALDO_VERS			:=0;
				cTotaliTitoli(contractIndex).IMP_SALDO_INVEST		:=0;
				cTotaliTitoli(contractIndex).IMP_PRVNT_PASS			:=0;
				cTotaliTitoli(contractIndex).IMP_INT_VERS			:=0;
				cTotaliTitoli(contractIndex).IMP_INT_INVEST			:=0;
                cTotaliTitoli(contractIndex).QTA_SALDO_GIORNL_QUOTA_ZERO := 0;
				
				--	RFC#110777 def. 64192. Viene azzerata solo la QTA_QUOTA_T2 del fondo origine.
                --  Se si azzerasse anche la QTA_QUOTA_T1 non si potrebbe calcolare l'IMP_CONTVAL_T1 da riversare integralmente nel Fondo Target
				cTotaliTitoli(contractIndex).QTA_QUOTA_T2 :=0;  

            END IF;

        END IF;


END;

PROCEDURE agg_tab_totali_gg_cruscotti(
		bulk_limit IN NUMBER,
		esito OUT CHAR,
		num_rec_elab OUT INTEGER,
		num_rec_scart OUT INTEGER,
		num_rec_warn OUT INTEGER,
		typeParamT2 NUMBER DEFAULT 0,
		typeParamT1 NUMBER DEFAULT 0)
IS
    chiavi_del_giorno SOGG_OUTELAB.TB_KEY_TIT;
	ultimoRun DATE;
	ROWCOUNT  NUMBER := 0;
	rec_eventi_titoli eventi_titoli%rowtype;
	in_cTotaliTitoli tb_myIndex;
	cTotaliTitoli SOGG_OUTELAB.TB_TOTALS_TIT;
	contr_attuali tb_contr;
	intervalloDaLAvorare SOGG_OUTELAB.MULTIDOM_MWRR.COD_CAMPO_MWRR%type; 
    counter NUMBER;
type tb_eventi_titoli
IS
	TABLE OF eventi_titoli%rowtype;
	eventi_grezzi tb_eventi_titoli;
	myerrmsg VARCHAR(255);
	t1       DATE;
	t2       DATE;
	rollingWindow Sogg_Outelab.RT_ROLLING_DATES;
BEGIN

	cTotaliTitoli := SOGG_OUTELAB.TB_TOTALS_TIT();

    counter := 0;

	-- MARKING THE START OF THE PROCESS
	SOGG_OUTELAB.MARKING_CRUSCOTTI_BEGIN('TIT-SAL-C');

	--LISTA DI REAL ESTATE
	aggiorna_real_estate_list;
	--LISTA CAUSALI CEDOLE REAL ESTATE
	aggiorna_tipo_cedola_list_re;


	--LISTA CAUSALI CEDOLE TITOLI NON REAL ESTATE
	aggiorna_tipo_cedola_list_tit;
	--LIST BOND_ISIN for Titles

	aggiorna_bond_isin_tit;
    --LISTA fondi sorgente/destinazione per fondi RE soggetti a cambio ISIN
    aggiorna_lista_re_cambio_isin;

	-- Andd 91518, verifica parametrizzazione su intervallo temporale
	select nvl(max(COD_CAMPO_MWRR),'ALL') into intervalloDaLAvorare from SOGG_OUTELAB.MULTIDOM_MWRR where COD_TIPO_DOM_MWRR = 'IT___CRUSC';
    --intervalloDaLAvorare := 'ALL';

	IF typeParamT1 > 0 THEN
		GET_ROLLING_WINDOWS(typeParamT2, typeParamT1);

	ELSE
		GET_ROLLING_WINDOWS(typeParamT2);

	END IF;

	ultimoRun        := fetchDataUltimoRun('TIT-C');
	sogg_outelab.ins_punt_elab('TIT-C');

	LOOP

        cTotaliTitoli.delete;
		in_cTotaliTitoli.delete;

   		counter := counter + 1;
        -- extracting keys from DETT_LOG_MOVI_CONTR_CRUSCTO
		OPEN c_chiavi_giorno_cruscotti(bulk_limit,'TITOLI');

		FETCH c_chiavi_giorno_cruscotti bulk collect INTO chiavi_del_giorno;

		CLOSE c_chiavi_giorno_cruscotti;


		IF chiavi_del_giorno.count = 0 OR counter = 3 THEN
			EXIT;
		END IF;

   --     IF counter = 2 THEN
   --         SOGG_OUTELAB.stmp_rec_redd_tot(cTotaliTitoli);
   --     END IF;

		--legg_contr_info(chiavi_del_giorno, contr_attuali);
		num_rec_elab := 0;


        --viene cancellato l'array per il calcolo dei giorni a zero. Essendo un array globale, se non cancellato verrebbe
        --mantenuto a livello di sessione tra una chiamatae l'altra falsando i dati.
        tb_qta_holes.delete();

		OPEN eventi_titoli(chiavi_del_giorno);

		LOOP

			FETCH eventi_titoli BULK COLLECT INTO eventi_grezzi LIMIT bulk_limit;
			EXIT
		WHEN eventi_grezzi.COUNT = 0;
			num_rec_elab           := num_rec_elab + eventi_grezzi.COUNT;



			FOR indiceMovimenti IN 1 .. eventi_grezzi.COUNT
			LOOP

				-- If run is for customized timespan, no other timespans are executed and vice versa

				IF typeParamT1 > 0 THEN
					-- For customizable timespan
					--calling events aggregation method for customized timespan
					rollingWindow := dataRollingWindow('PC');
					t1            := rollingWindow.DAT_INIZ_PERIOD;
					t2            := rollingWindow.DAT_FINE_PERIOD;
					IF t1<t2 THEN
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'PC' );
					END IF;


				ELSE



					-- for last month
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('M1', 'ALL')) then
						rollingWindow := dataRollingWindow('M1');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'M1' );
					end if;


					-- for last 3 month
					--calling events aggregation method for 3 months timespan

					if(intervalloDaLAvorare in ('M3', 'ALL')) then
						rollingWindow := dataRollingWindow('M3');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'M3' );
					end if;
					-- for last 6 month					
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('M6', 'ALL')) then
						rollingWindow := dataRollingWindow('M6');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'M6' );
					end if;
					-- for 1 year
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('Y1', 'ALL')) then
						rollingWindow := dataRollingWindow('Y1');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'Y1' );
					end if;
					-- for last 3 years
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('Y3', 'ALL')) then
						rollingWindow := dataRollingWindow('Y3');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'Y3' );
					end if;
					-- for last 5 years
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('Y5', 'ALL')) then
						rollingWindow := dataRollingWindow('Y5');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'Y5' );
					end if;
					-- for last 10 years
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('Y10', 'ALL')) then
						rollingWindow := dataRollingWindow('Y10');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'Y10' );
					end if;
					-- for last 15 years					
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('Y15', 'ALL')) then
						rollingWindow := dataRollingWindow('Y15');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'Y15' );
					end if;
					-- for last 20 years
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('Y20', 'ALL')) then
						rollingWindow := dataRollingWindow('Y20');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'Y20' );
					end if;
					-- from start of the year
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('YTD', 'ALL')) then
						rollingWindow := dataRollingWindow('YTD');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'YTD' );
					end if;


					-- Since Inception
					--calling events aggregation method for one month timespan
					if(intervalloDaLAvorare in ('SI', 'ALL')) then
						rollingWindow := dataRollingWindow('SI');
						t1            := rollingWindow.DAT_INIZ_PERIOD;
						t2            := rollingWindow.DAT_FINE_PERIOD;
						calc_totali_titoli( in_cTotaliTitoli, cTotaliTitoli, eventi_grezzi(indiceMovimenti), t1, t2, 'SI' );
					end if;

				END IF;

			END LOOP;

		END LOOP;


		CLOSE eventi_titoli;
		-- call to proc to insert/update data into saldo table
		agg_tab_totali_cruscotti(cTotaliTitoli);

        --SOGG_OUTELAB.stmp_rec_redd_tot(cTotaliTitoli);

		UPDATE SOGG_OUTELAB.DETT_LOG_MOVI_CONTR_CRUSCTO LC
		SET LC.gstd_f_esist       ='N' ,
			LC.GSTD_M_NOM_ULT_MODF   ='ESEGUITA'
			--, LC.GSTD_D_ULT_MODF_RECORD=sysdate
		WHERE LC.Dat_Rife        <= TRUNC(SYSDATE) AND
		 LC.COD_TIPO_RECORD    ='TITOLI'
		AND
			(
				LC.PROD_C, LC.CONTR_N
			)
			IN
			(SELECT T1.PROD_C, T1.CONTR_N FROM (TABLE(chiavi_del_giorno)) T1
			) ;
		COMMIT;

    END LOOP;
	esito := 'S';



EXCEPTION

WHEN OTHERS THEN

    DBMS_OUTPUT.PUT_LINE('An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);

    --CLOSE eventi_titoli;

	SELECT COUNT(*)
	INTO ROWCOUNT
	FROM SOGG_OUTELAB.SALDO_TIT_PERIOD
	WHERE FLG_ELAB = 'N';
	-- MARKING THE END OF THE PROCESS
	SOGG_OUTELAB.MARKING_CRUSCOTTI_END('TIT-C', ROWCOUNT, 0);

	SELECT COUNT(*)
	INTO ROWCOUNT
	FROM SOGG_OUTELAB.SALDO_TIT_PERIOD
	WHERE FLG_ELAB = 'N';
	-- MARKING THE END OF THE PROCESS
	SOGG_OUTELAB.MARKING_CRUSCOTTI_END('TIT-SAL-C', ROWCOUNT, 0);

END;

/* rfc #73046 cruscotti proc to insert/update data into saldo table */

PROCEDURE GET_ROLLING_WINDOWS(
		typeParamT2 NUMBER DEFAULT 0,
		typeParamT1 NUMBER DEFAULT 0)
IS
	chiavi_window SOGG_OUTELAB.RT_ROLLING_DATES;
	t1 DATE;
	t2 DATE;
BEGIN
	dbms_output.put_line('GET_ROLLING_WINDOWS');

	-- for 1 month

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -1-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('M1');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 3 month

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -3-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('M3');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 6 month

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -6-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('M6');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 12 months - 1 year

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -12-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('Y1');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 3 years

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -36-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('Y3');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 5 years

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -60-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('Y5');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 10 years

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -120-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('Y10');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 15 years

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -180-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('Y15');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

	-- for last 20 years

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -240-typeParamT2))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('Y20');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;


	-- from start of the year

	SELECT last_day(add_months(sysdate,0-typeParamT2)),
		TRUNC (SYSDATE , 'YEAR')           -1
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('YTD');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;


	-- Since Inception

	SELECT last_day(add_months(sysdate,0-typeParamT2)),
		to_date('17530101','YYYYMMDD')
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('SI');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;


	-- for customized timespan

	SELECT last_day(add_months(sysdate,0-typeParamT2)) ,
		last_day(add_months(sysdate,       -1- typeParamT1))
	INTO t2,
		t1
	FROM dual;

	chiavi_window                                          := SOGG_OUTELAB.RT_ROLLING_DATES('PC');
	chiavi_window.DAT_INIZ_PERIOD                          := TRUNC(t1);
	chiavi_window.DAT_FINE_PERIOD                          := TRUNC(t2);
	dataRollingWindow(trim(chiavi_window.COD_PERIOD_ELAB)) := chiavi_window;

END;



PROCEDURE agg_tab_totali_cruscotti(
		totali IN OUT nocopy SOGG_OUTELAB.TB_TOTALS_TIT )
IS
BEGIN

	INSERT
			INTO sogg_outelab.dett_log_err
		(PROD_C,
		contr_n,
		cod_tit_intern,
		cod_indice_emis_tit,
		cod_err_elab,
		des_err_elab,
		gstd_d_ult_modf_record,
		gstd_d_ins_record,
		gstd_x_tip_modf,
		gstd_x_user,
		gstd_m_nom_ult_modf,
		gstd_f_esist) SELECT
			prod_c,
			contr_n,
			cod_tit_intern,
			cod_indice_emis_tit,
			0,
			'doppio: '
			|| cod_period_elab
			|| dat_fine_period
			|| dat_iniz_period
        || NVL(TRIM(cod_trans_acqu_pct), 0),
			SYSDATE,
			SYSDATE,
			'I',
			'TIT',
			'TIT',
			'S'
		FROM
        TABLE ( totali )
		GROUP BY
			cod_period_elab,
			prod_c,
			contr_n,
			dat_fine_period,
			dat_iniz_period,
			cod_tit_intern,
			cod_indice_emis_tit,
        NVL(TRIM(cod_trans_acqu_pct), 0)
		having count(1) > 1;
		COMMIT;



	INSERT
	INTO SOGG_OUTELAB.SALDO_TIT_PERIOD DS
		(
			COD_PERIOD_ELAB,
			DAT_RIFE,
			DAT_INIZ_PERIOD,
			DAT_FINE_PERIOD,
			PROD_C,
			CONTR_N,
            COD_TIT_INTERN,
            COD_INDICE_EMIS_TIT,
            COD_TRANS_ACQU_PCT,
			DAT_APER_CONTR,
			DAT_CHIU_CONTR,
			DAT_PRIMO_MOVI,
			DAT_ULT_MOVI,
			IMP_TOT_INVEST,
			IMP_TOT_INVEST_NO_CONVS,
			IMP_TOT_VERS,
			IMP_TOT_VERS_NO_CONVS,
			IMP_TOT_RIMBO,
			IMP_TOT_RIMBO_NO_CONVS,
			IMP_INT_INVEST,
			IMP_INT_VERS,
			IMP_SALDO_INVEST,
			IMP_SALDO_VERS,
			IMP_PRVNT_FUT,
			IMP_PRVNT_PASS,
			FLG_ELAB,
			QTA_QUOTA_T1,
			QTA_QUOTA_T2,
			GSTD_D_INS_RECORD,
			GSTD_D_ULT_MODF_RECORD,
			GSTD_F_ESIST,
			GSTD_M_NOM_ULT_MODF,
			GSTD_X_TIP_MODF,
			GSTD_X_USER,
            COD_TIPO_PROD_INVEST,
            QTA_QUOTE_GIOR_ZERO
		)
	SELECT distinct ap.COD_PERIOD_ELAB,
		TRUNC(SYSDATE),
		ap.DAT_INIZ_PERIOD,
		ap.DAT_FINE_PERIOD,
		ap.PROD_C,
		ap.CONTR_N,
        ap.COD_TIT_INTERN,
        ap.COD_INDICE_EMIS_TIT,
        nvl(TRIM(ap.COD_TRANS_ACQU_PCT), 0),
		ap.DAT_APER_CONTR,
		ap.DAT_CHIU_CONTR,
		ap.DAT_PRIMO_MOVI,
		ap.DAT_ULT_MOVI,
		ap.IMP_TOT_INVEST,
		ap.IMP_TOT_INVEST_NO_CONVS,
		ap.IMP_TOT_VERS,
		ap.IMP_TOT_VERS_NO_CONVS,
		ap.IMP_TOT_RIMBO,
		ap.IMP_TOT_RIMBO_NO_CONVS,
		ap.IMP_INT_INVEST,
		ap.IMP_INT_VERS,
		ap.IMP_SALDO_INVEST,
		ap.IMP_SALDO_VERS,
		ap.IMP_PRVNT_FUT,
		ap.IMP_PRVNT_PASS,
		'N',
		ap.QTA_QUOTA_T1,
		ap.QTA_QUOTA_T2,
		SYSDATE,
		SYSDATE,
		'S',
		'MOT-TIT',
		'I',
		'MOT-TIT',
        ap.COD_TIPO_TIT_REDDTA,
        ap.QTA_SALDO_GIORNL_QUOTA_ZERO
	FROM TABLE(totali) AP
	WHERE NOT EXISTS
		(SELECT 1
		FROM SOGG_OUTELAB.SALDO_TIT_PERIOD DS
		WHERE ap.prod_c        =ds.prod_c
		AND ap.contr_n         =ds.contr_n
		AND ap.COD_TIT_INTERN         =ds.COD_TIT_INTERN
		AND ap.COD_INDICE_EMIS_TIT    =ds.COD_INDICE_EMIS_TIT
		AND nvl(TRIM(ap.COD_TRANS_ACQU_PCT), 0)    = ds.COD_TRANS_ACQU_PCT
		AND ds.COD_PERIOD_ELAB = ap.COD_PERIOD_ELAB
		AND DS.DAT_INIZ_PERIOD = ap.DAT_INIZ_PERIOD
		AND DS.DAT_FINE_PERIOD = ap.DAT_FINE_PERIOD
		) 
and
        (COD_PERIOD_ELAB, PROD_C, CONTR_N, DAT_FINE_PERIOD, DAT_INIZ_PERIOD, COD_TIT_INTERN, COD_INDICE_EMIS_TIT, NVL(TRIM(COD_TRANS_ACQU_PCT),0)) not in
        (
        select COD_PERIOD_ELAB, PROD_C, CONTR_N, DAT_FINE_PERIOD, DAT_INIZ_PERIOD, COD_TIT_INTERN, COD_INDICE_EMIS_TIT, NVL(TRIM(COD_TRANS_ACQU_PCT),0)
  FROM
            TABLE ( totali )
  GROUP BY
    cod_period_elab,
    prod_c,
    contr_n,
    dat_fine_period,
    dat_iniz_period,
    cod_tit_intern,
    cod_indice_emis_tit,
            NVL(TRIM(cod_trans_acqu_pct),0)
        having count(1) > 1
        )

        ;
	COMMIT;

END;

PROCEDURE calc_redd_dett_cruscotti(
		bulk_limit    IN NUMBER,
		t1            IN DATE,
		t2            IN DATE,
		codicePeriodo IN VARCHAR2,
		esito         IN OUT CHAR,
		num_rec_elab  IN OUT INTEGER,
		num_rec_scart IN OUT INTEGER,
		num_rec_warn  IN OUT INTEGER)
IS
	recordTarget SOGG_OUTELAB.TB_OUTPUT_ONLINE;
	--base_data_bmed_cruscotti c_load_saldo_cruscotti%rowtype;
	commitFreq  NUMBER;
	commitCount NUMBER;
	clic_last   CHAR(11) := 'DUMMY';
	trancheDaLAvorare SOGG_OUTELAB.MULTIDOM_MWRR.COD_CAMPO_MWRR%type; 
	dimensioneModulo  SOGG_OUTELAB.MULTIDOM_MWRR.COD_CAMPO_MWRR%type; 
    base_data_tit_cruscotti c_load_saldo_cruscotti%rowtype;
    chiaveProdotto VARCHAR(32);
    contractKey    VARCHAR(32);
BEGIN

	recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
	commitFreq   :=bulk_limit;
	commitCount  :=1;

	-- Andd 91518, estraggo la tranche che e in lavorazione
	select nvl(max(COD_CAMPO_MWRR),'0') into trancheDaLAvorare from SOGG_OUTELAB.MULTIDOM_MWRR where COD_TIPO_DOM_MWRR = 'TR___CRUSC';
	select nvl(max(COD_CAMPO_MWRR),'24') into dimensioneModulo from SOGG_OUTELAB.MULTIDOM_MWRR where COD_TIPO_DOM_MWRR = 'MD___CRUSC';

	-- checking if quota values are already evaluated for given timespan t1	

	IF time_id_valore_quota_t1 != t1 OR NOT quote_t1_crusc.exists(quote_t1_crusc.first) THEN
		-- loading prod_c-quota values into assoc array for t1

        --legg_val_quota_per_re_crusc(t1, quote_t1_crusc);
        legg_val_quota_per_tit_crusc(t1, quote_t1_crusc);

		time_id_valore_quota_t1 := t1;

	END IF;
	-- checking if quota values are already evaluated for given timespan t2

	IF time_id_valore_quota_t2 != t2 OR NOT quote_t2_crusc.exists(quote_t2_crusc.first) THEN
		-- loading prod_c-quota values into assoc array for t2

        --legg_val_quota_per_re_crusc(t2, quote_t2_crusc);
        legg_val_quota_per_tit_crusc(t2, quote_t2_crusc);

		time_id_valore_quota_t2 := t2;

	END IF;

	OPEN c_load_saldo_cruscotti(trim(t1),trim(t2), codicePeriodo, trancheDaLAvorare, dimensioneModulo);

	LOOP

		FETCH c_load_saldo_cruscotti INTO base_data_tit_cruscotti;
		EXIT
	WHEN c_load_saldo_cruscotti%NOTFOUND;

        recordTarget.extend(1);

 		chiaveProdotto := trim(base_data_tit_cruscotti.COD_TIT_INTERN)||'-'||base_data_tit_cruscotti.COD_INDICE_EMIS_TIT;

		IF(base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA          = 'FNDIM') THEN
			recordTarget(recordTarget.last)          := SOGG_OUTELAB.RT_OUTPUT_ONLINE('FND'); --16032019
			recordTarget(recordTarget.last).REC_TYPE := 'FND_IMMB';

		ELSE

			IF ( NVL(base_data_tit_cruscotti.DAT_CHIU_CONTR, TO_DATE('31/12/9999', 'DD/MM/YYYY')) < TO_DATE('01/01/2013', 'DD/MM/YYYY') OR base_data_tit_cruscotti.DAT_APER_CONTR > t2 ) THEN
				recordTarget(recordTarget.last)                                        := SOGG_OUTELAB.RT_OUTPUT_ONLINE('NOSEE');

			ELSIF(base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('PCT', 'MPB', 'CERT')) THEN
					recordTarget(recordTarget.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('INVBAN');

            ELSIF(base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('OBBLI', 'AZION')) THEN
					recordTarget(recordTarget.last) := SOGG_OUTELAB.RT_OUTPUT_ONLINE('TIT');

			ELSE
                recordTarget(recordTarget.last)                                        := SOGG_OUTELAB.RT_OUTPUT_ONLINE('NOSEE');

			END IF;
			-- defect 34089,34246
			-- latest rfc 86777 tit-invban periodic call modifications
			--recordTarget(recordTarget.last).REC_TYPE := base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA;

		END IF;

        recordTarget(recordTarget.last).CLI_C  := base_data_tit_cruscotti.CLI_C ;

		recordTarget(recordTarget.last).PROD_C  := base_data_tit_cruscotti.PROD_C ;
		recordTarget(recordTarget.last).CONTR_N := base_data_tit_cruscotti.CONTR_N ;
        recordTarget(recordTarget.last).COD_PERIOD_ELAB := base_data_tit_cruscotti.COD_PERIOD_ELAB;
        recordTarget(recordTarget.last).DAT_INIZ_PERIOD := base_data_tit_cruscotti.DAT_INIZ_PERIOD;
        recordTarget(recordTarget.last).DAT_FINE_PERIOD := base_data_tit_cruscotti.DAT_FINE_PERIOD;
        recordTarget(recordTarget.last).COD_TIPO_PROD_INVEST := COALESCE(base_data_tit_cruscotti.COD_TIPO_PROD_INVEST, 'NOSEE');

        contractKey                                   := trim(base_data_tit_cruscotti.CONTR_N)||'-'||trim(base_data_tit_cruscotti.PROD_C);

/*      DA CAPIRE SE SERVE
		IF(contr_attuali.EXISTS(contractKey)) THEN
			local_redd_data(local_redd_data.last).CONTR_N_PADRE := COALESCE(TRIM(contr_attuali(contractKey).CONTR_N_SERV), local_redd_data(local_redd_data.last).CONTR_N);

		END IF;

*/        


		recordTarget(recordTarget.last).COD_TIT_INTERN      := base_data_tit_cruscotti.COD_TIT_INTERN;
		recordTarget(recordTarget.last).COD_INDICE_EMIS_TIT := base_data_tit_cruscotti.COD_INDICE_EMIS_TIT;
		recordTarget(recordTarget.last).COD_TRANS_ACQU_PCT := base_data_tit_cruscotti.COD_TRANS_ACQU_PCT;
		recordTarget(recordTarget.last).DAT_APER_CONTR      := base_data_tit_cruscotti.DAT_APER_CONTR;
		recordTarget(recordTarget.last).DAT_CHIU_CONTR      := base_data_tit_cruscotti.DAT_CHIU_CONTR;


		IF t1 < to_date('01/01/2013','dd/MM/yyyy') OR base_data_tit_cruscotti.QTA_QUOTA_T1 < 0 THEN
			recordTarget(recordTarget.last).QTA_QUOTA_T1 := 0;

		ELSE
			recordTarget(recordTarget.last).QTA_QUOTA_T1 := COALESCE (base_data_tit_cruscotti.QTA_QUOTA_T1 , 0);

		END IF;
		recordTarget(recordTarget.last).DAT_PRIMO_MOVI :=

		CASE
		WHEN recordTarget(recordTarget.last).QTA_QUOTA_T1>0 OR base_data_tit_cruscotti.DAT_PRIMO_MOVI IS NULL THEN
			t1
		ELSE
			base_data_tit_cruscotti.DAT_PRIMO_MOVI
		END;
		recordTarget(recordTarget.last).DAT_ULT_MOVI :=

		CASE
		WHEN base_data_tit_cruscotti.DAT_ULT_MOVI IS NULL THEN
			t2
		ELSE
			base_data_tit_cruscotti.DAT_ULT_MOVI
		END;
		recordTarget(recordTarget.last).DAT_RIFE :=

		CASE
		WHEN base_data_tit_cruscotti.QTA_QUOTA_T2>0 THEN
			t2
		ELSE
			recordTarget(recordTarget.last).DAT_ULT_MOVI
		END;
		recordTarget(recordTarget.last).IMP_TOT_VERS            := COALESCE (base_data_tit_cruscotti.IMP_TOT_VERS , 0);
		recordTarget(recordTarget.last).IMP_TOT_INVEST          := COALESCE (base_data_tit_cruscotti.IMP_TOT_INVEST , 0);
		recordTarget(recordTarget.last).IMP_TOT_RIMBO           := COALESCE (base_data_tit_cruscotti.IMP_TOT_RIMBO , 0);
		recordTarget(recordTarget.last).IMP_TOT_VERS_NO_CONVS   := COALESCE (base_data_tit_cruscotti.IMP_TOT_VERS , 0);
		recordTarget(recordTarget.last).IMP_TOT_INVEST_NO_CONVS := COALESCE (base_data_tit_cruscotti.IMP_TOT_INVEST , 0);
		recordTarget(recordTarget.last).IMP_TOT_RIMBO_NO_CONVS  := COALESCE (base_data_tit_cruscotti.IMP_TOT_RIMBO , 0);
		recordTarget(recordTarget.last).IMP_PRVNT_PASS          := COALESCE (base_data_tit_cruscotti.IMP_PRVNT_PASS , 0);
		recordTarget(recordTarget.last).IMP_PRVNT_FUT           := COALESCE (base_data_tit_cruscotti.IMP_PRVNT_FUT , 0);


		recordTarget(recordTarget.last).VAL_QUO_T1              :=
		(

			CASE
			WHEN quote_t1_crusc.exists(chiaveProdotto) THEN
				quote_t1_crusc(chiaveProdotto)
			ELSE
				NULL
			END);

		      	-- 20042019 Aggiustamenti per i PCT che hanno una gestione particolare
        IF (base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('PCT')) THEN
				--dbms_output.put_line(recordTarget(recordTarget.last).COD_TIT_INTERN || ' Ã¨ un PCT');
			    if(recordTarget(recordTarget.last).DAT_CHIU_CONTR < t1) then
                    -- Aperto e chiuso prima del periodo di osservazione
					--dbms_output.put_line(recordTarget(recordTarget.last).COD_TIT_INTERN || ' C1');
                    recordTarget(recordTarget.last).IMP_CONTVAL_T1 := 0;
                    recordTarget(recordTarget.last).IMP_CONTVAL_T2 := 0;
                elsif (recordTarget(recordTarget.last).DAT_APER_CONTR > t2) then
                    -- Aperto DOPO il periodo di osservazione
					--dbms_output.put_line(recordTarget(recordTarget.last).COD_TIT_INTERN || ' C2');
                    recordTarget(recordTarget.last).IMP_CONTVAL_T1 := 0;
                    recordTarget(recordTarget.last).IMP_CONTVAL_T2 := 0;
                elsif  (recordTarget(recordTarget.last).DAT_APER_CONTR < t1 and recordTarget(recordTarget.last).DAT_CHIU_CONTR between t1 AND t2) then
                    -- Aperto prima e chiuso nel periodo
					--dbms_output.put_line(recordTarget(recordTarget.last).COD_TIT_INTERN || ' C3');
                    recordTarget(recordTarget.last).IMP_CONTVAL_T1 := recordTarget(recordTarget.last).IMP_TOT_VERS;
                    recordTarget(recordTarget.last).IMP_TOT_VERS := 0;
					recordTarget(recordTarget.last).IMP_TOT_VERS_NO_CONVS := 0;
                    recordTarget(recordTarget.last).IMP_CONTVAL_T2 := 0;
                elsif  (recordTarget(recordTarget.last).DAT_APER_CONTR between t1 AND t2 and recordTarget(recordTarget.last).DAT_CHIU_CONTR > t2 ) then
                    -- Aperto nel periodo e chiuso DOPO
					--dbms_output.put_line(recordTarget(recordTarget.last).COD_TIT_INTERN || ' C4');
                    recordTarget(recordTarget.last).IMP_CONTVAL_T1 := 0;
                    recordTarget(recordTarget.last).IMP_CONTVAL_T2 := recordTarget(recordTarget.last).IMP_TOT_RIMBO;
                    recordTarget(recordTarget.last).IMP_TOT_RIMBO := 0;
					recordTarget(recordTarget.last).IMP_TOT_RIMBO_NO_CONVS := 0;
					---dbms_output.put_line(recordTarget(recordTarget.last).COD_TIT_INTERN || ' Rimborsi a 0 e ctv a T2: '|| recordTarget(recordTarget.last).IMP_CONTVAL_T2);
                end if;
        end if;

		-- calcolo del Capitale iniziale IMP_CONTVAL_T1
			IF base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('PCT') THEN
			recordTarget(recordTarget.last).IMP_CONTVAL_T1 := COALESCE (recordTarget(recordTarget.last).IMP_CONTVAL_T1,0);
			ELSE
			--def. 50472: per Titoli e INV Bancari il CONTROVALORE a data T1 deve essere valorizzato solo dopo la data di CUTOFF
			IF t1 > to_Date('01-01-2013', 'dd-mm-yyyy') THEN
				IF base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('OBBLI', 'CERT', 'MPB') AND base_data_tit_cruscotti.ANAATTIVFIN_X_ESP_PREZ <> 'L' THEN
                   recordTarget(recordTarget.last).IMP_CONTVAL_T1 := COALESCE (recordTarget(recordTarget.last).VAL_QUO_T1*recordTarget(recordTarget.last).QTA_QUOTA_T1/100,0);
                ELSE
		recordTarget(recordTarget.last).IMP_CONTVAL_T1 := COALESCE (recordTarget(recordTarget.last).VAL_QUO_T1*recordTarget(recordTarget.last).QTA_QUOTA_T1,0);
               END IF;
			ELSE
				recordTarget(recordTarget.last).IMP_CONTVAL_T1 := 0;
			END IF;

		END IF;
		-- I Real Estate non hanno CUTOFF quindi IMP_CONTVAL_T1 deve essere calcolato esternamente al blocco di IF precedente
		IF base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('FND') THEN
			recordTarget(recordTarget.last).IMP_CONTVAL_T1 := COALESCE (recordTarget(recordTarget.last).VAL_QUO_T1*recordTarget(recordTarget.last).QTA_QUOTA_T1,0);
        END IF;
		IF base_data_tit_cruscotti.QTA_QUOTA_T2                     > 0 THEN
			recordTarget(recordTarget.last).QTA_QUOTA_T2 := base_data_tit_cruscotti.QTA_QUOTA_T2;

		ELSE
			recordTarget(recordTarget.last).QTA_QUOTA_T2 := 0;

		END IF;
		recordTarget(recordTarget.last).VAL_QUO_T2 :=
		(

			CASE
			WHEN quote_t2_crusc.exists(chiaveProdotto) THEN
				quote_t2_crusc(chiaveProdotto)
			ELSE
				NULL
			END);
		-- calcolo del Controvalore IMP_CONTVAL_T2
        IF base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('PCT') THEN
			recordTarget(recordTarget.last).IMP_CONTVAL_T2 := COALESCE (recordTarget(recordTarget.last).IMP_CONTVAL_T2,0);
        ELSE
            IF base_data_tit_cruscotti.COD_TIPO_TIT_REDDTA IN ('OBBLI', 'CERT', 'MPB') AND base_data_tit_cruscotti.ANAATTIVFIN_X_ESP_PREZ <> 'L' THEN  --per 'OBBLI', 'CERT' e 'MPB' dividiamo QTA_QUOTA_T2 per 100
                recordTarget(recordTarget.last).IMP_CONTVAL_T2   := COALESCE (recordTarget(recordTarget.last).VAL_QUO_T2*recordTarget(recordTarget.last).QTA_QUOTA_T2/100, 0);
            ELSE
		recordTarget(recordTarget.last).IMP_CONTVAL_T2   := COALESCE (recordTarget(recordTarget.last).VAL_QUO_T2*recordTarget(recordTarget.last).QTA_QUOTA_T2, 0);
            END IF;
        END IF;
		recordTarget(recordTarget.last).QTA_QUOTA_SALDO  := NULL;
		recordTarget(recordTarget.last).IMP_SALDO_VERS   := COALESCE (base_data_tit_cruscotti.IMP_SALDO_VERS, 0)    + COALESCE (recordTarget(recordTarget.last).IMP_CONTVAL_T1, 0);
		recordTarget(recordTarget.last).IMP_SALDO_INVEST := COALESCE (base_data_tit_cruscotti.IMP_SALDO_INVEST , 0) + COALESCE (recordTarget(recordTarget.last).IMP_CONTVAL_T1, 0);
		SOGG_OUTELAB.calc_flg_stato(recordTarget(recordTarget.last), t2);
		recordTarget(recordTarget.last).QTA_SALDO_GIORNL_QUOTA_ZERO := COALESCE(base_data_tit_cruscotti.QTA_SALDO_GIORNL_QUOTA_ZERO, 0);
		recordTarget(recordTarget.last).IMP_INT_VERS                := COALESCE (base_data_tit_cruscotti.IMP_INT_VERS, 0)   + COALESCE (( recordTarget(recordTarget.last).IMP_CONTVAL_T1 * (recordTarget(recordTarget.last).DAT_ULT_MOVI - recordTarget(recordTarget.last).DAT_PRIMO_MOVI - recordTarget(recordTarget.last).QTA_SALDO_GIORNL_QUOTA_ZERO) ),0);
		recordTarget(recordTarget.last).IMP_INT_INVEST              := COALESCE (base_data_tit_cruscotti.IMP_INT_INVEST, 0) + COALESCE (( recordTarget(recordTarget.last).IMP_CONTVAL_T1 * (recordTarget(recordTarget.last).DAT_ULT_MOVI - recordTarget(recordTarget.last).DAT_PRIMO_MOVI - recordTarget(recordTarget.last).QTA_SALDO_GIORNL_QUOTA_ZERO) ),0);		    

		SOGG_OUTELAB.calc_indici_redd(recordTarget(recordTarget.last));

/*		---settaggio PLUS MINUS per PCT
		IF recordTarget(recordTarget.last).COD_TIPO_TIT_REDDTA = 'PCT' THEN
            recordTarget(recordTarget.last).DAT_SCAD_CONTR := recordTarget(recordTarget.last).DAT_CHIU_CONTR;
			-- valorizzazione del PLUS MINUS solo per i PCT. Per gli altri prodotti avviene in post processing: PROCEDURE calcola_plusminus_Titoli
            recordTarget(recordTarget.last).IMP_PLUS_MINUS_VERS   := COALESCE(base_data_tit_cruscotti.IMP_PLUS_MINUS_VERS_PCT,0);
			recordTarget(recordTarget.last).IMP_PLUS_MINUS_INVEST := base_data_tit_cruscotti.IMP_PLUS_MINUS_VERS;
        END IF;
*/
		IF recordTarget(recordTarget.last).CLI_C <> clic_last THEN
			clic_last                               := recordTarget(recordTarget.last).CLI_C;
			commitCount                             := commitCount + 1;

		END IF;

		IF recordTarget(recordTarget.last).CLI_C <> clic_last THEN
			clic_last                               := recordTarget(recordTarget.last).CLI_C;
			commitCount                             := commitCount + 1;

		END IF;

		IF mod(commitCount,commitFreq)=0 THEN
            SOGG_OUTELAB.PKG_PST_PRC.CALC_REDD_LOB_CONTR_N(recordTarget, 'N');
            SOGG_OUTELAB.PKG_PST_PRC.CALC_REDD_AGG_TIPO_PROD_INVEST(recordTarget, 'N');            
			ins_dati_ol_cruscotti(recordTarget,'A');
			recordTarget := SOGG_OUTELAB.TB_OUTPUT_ONLINE();
			--fatto l'inserimento si incrementa il commitCount per fare in modo che, per ogni prodotto successivo al primo
			--dello stesso cliente, non continui a fare inserimenti: se non venisse incrementato in pratica verrebbe fatto
			--una insert per ogni prodotto di quel cliente
			commitCount                             := commitCount + 1;
		END IF;

		IF recordTarget(recordTarget.last).CLI_C <> clic_last THEN
			clic_last                               := recordTarget(recordTarget.last).CLI_C;
			commitCount                             := commitCount + 1;

		END IF;
	END LOOP;
	CLOSE c_load_saldo_cruscotti;

    --Creazione aggregati per contr_n - in pratica aggregati relativi ai dossier.
    SOGG_OUTELAB.PKG_PST_PRC.CALC_REDD_LOB_CONTR_N(recordTarget, 'N');
    SOGG_OUTELAB.PKG_PST_PRC.CALC_REDD_AGG_TIPO_PROD_INVEST(recordTarget, 'N');
    ins_dati_ol_cruscotti(recordTarget,'A');
END;


PROCEDURE call_calc_redd_dett_cruscotti(
		bulk_limit IN NUMBER,
		esito OUT CHAR,
		num_rec_elab OUT INTEGER,
		num_rec_scart OUT INTEGER,
		num_rec_warn OUT INTEGER,
		typeParamT2 NUMBER DEFAULT 1,
		typeParamT1 NUMBER DEFAULT 0)
IS
	codicePeriodo CHAR;
	t1            DATE;
	t2            DATE;
	rollingWindow Sogg_Outelab.RT_ROLLING_DATES;
	ROWCOUNT NUMBER := 0;
	intervalloDaLAvorare SOGG_OUTELAB.MULTIDOM_MWRR.COD_CAMPO_MWRR%type; 
BEGIN
	dbms_output.put_line('call_calc_redd_dett_cruscotti');
	-- MARKING THE START OF THE PROCESS
	SOGG_OUTELAB.MARKING_CRUSCOTTI_BEGIN('TIT-DET-C');

	-- Andd 91518, verifica parametrizzazione su intervallo temporale
	select nvl(max(COD_CAMPO_MWRR),'ALL') into intervalloDaLAvorare from SOGG_OUTELAB.MULTIDOM_MWRR where COD_TIPO_DOM_MWRR = 'IT___CRUSC';

	-- Check if it is the regular run or for customized timespan

	IF typeParamT1 > 0 THEN
		GET_ROLLING_WINDOWS(typeParamT2, typeParamT1);

	ELSE
		GET_ROLLING_WINDOWS(typeParamT2);

	END IF;
	-- If run is for customized timespan, no other timespans are executed and vice versa

	IF typeParamT1 > 0 THEN
		-- for Customizable timespan
		rollingWindow := dataRollingWindow('PC');
		t1            := rollingWindow.DAT_INIZ_PERIOD;
		t2            := rollingWindow.DAT_FINE_PERIOD;
		IF t1<t2 THEN
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'PC',esito,num_rec_elab, num_rec_scart, num_rec_warn);
		END IF;

	ELSE
		-- for last month
		if(intervalloDaLAvorare in ('M1', 'ALL')) then
			rollingWindow := dataRollingWindow('M1');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'M1',esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- for 3 month
		if(intervalloDaLAvorare in ('M3', 'ALL')) then
			rollingWindow := dataRollingWindow('M3');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'M3', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- for 6 months
		if(intervalloDaLAvorare in ('M6', 'ALL')) then
			rollingWindow := dataRollingWindow('M6');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'M6', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		--for 1 year
		if(intervalloDaLAvorare in ('Y1', 'ALL')) then
			rollingWindow := dataRollingWindow('Y1');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'Y1', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- for 3 years
		if(intervalloDaLAvorare in ('Y3', 'ALL')) then
			rollingWindow := dataRollingWindow('Y3');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'Y3', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- for 5 years
		if(intervalloDaLAvorare in ('Y5', 'ALL')) then
			rollingWindow := dataRollingWindow('Y5');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'Y5', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- for 10 years
		if(intervalloDaLAvorare in ('Y10', 'ALL')) then
			rollingWindow := dataRollingWindow('Y10');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'Y10', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- for 15 years
		if(intervalloDaLAvorare in ('Y15', 'ALL')) then
			rollingWindow := dataRollingWindow('Y15');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'Y15', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- for 20 years
		if(intervalloDaLAvorare in ('Y20', 'ALL')) then
			rollingWindow := dataRollingWindow('Y20');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'Y20', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- from start of the year
		if(intervalloDaLAvorare in ('YTD', 'ALL')) then
			rollingWindow := dataRollingWindow('YTD');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'YTD', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;
		-- Since Inception
		if(intervalloDaLAvorare in ('SI', 'ALL')) then
			rollingWindow := dataRollingWindow('SI');
			t1            := rollingWindow.DAT_INIZ_PERIOD;
			t2            := rollingWindow.DAT_FINE_PERIOD;
			calc_redd_dett_cruscotti(bulk_limit, t1,t2,'SI', esito,num_rec_elab, num_rec_scart, num_rec_warn);
		end if;

	END IF;

	SELECT COUNT(*)
	INTO ROWCOUNT
	FROM SOGG_OUTELAB.DETT_REDDTA_FONDI_BMED_PERIOD
	WHERE DAT_FINE_PERIOD = t2;
	-- MARKING THE END OF THE PROCESS
	SOGG_OUTELAB.MARKING_CRUSCOTTI_END('TIT-DET-C', ROWCOUNT, 0);

END;

PROCEDURE ins_dati_ol_cruscotti(
		cRec IN OUT nocopy SOGG_OUTELAB.TB_OUTPUT_ONLINE,
		dest IN CHAR)
IS
varSemaforo varchar2(1);
BEGIN

    --Semaforo. Nel caso venga inserito un valore con 2 byte nel campo des_campo_mwrr la insert nella variabile va in errore e l'esecuzione
    --si ferma
    select max(des_campo_mwrr) into varSemaforo from SOGG_OUTELAB.MULTIDOM_MWRR where cod_tipo_dom_mwrr ='SEMAFORO' and cod_campo_mwrr ='0';


	INSERT
	INTO SOGG_OUTELAB.DETT_LOG_ERR
		(
			PROD_C,
			CONTR_N,
            COD_TIT_INTERN,
            COD_INDICE_EMIS_TIT,
			COD_ERR_ELAB,
			DES_ERR_ELAB,
			GSTD_D_INS_RECORD,
			GSTD_X_TIP_MODF,
			GSTD_X_USER,
			GSTD_M_NOM_ULT_MODF
		)
	SELECT ap.prod_c,
		ap.contr_n,
        ap.cod_tit_intern,
        ap.cod_indice_emis_tit,
		0,
		'motore titoli CRUSCOTTI: riga duplicata in inserimento redd aperti',
		SYSDATE,
		'I',
		'BATCH',
		'BATCH'
	FROM
		(SELECT 
            CLI_C,
            PROD_C,
			CONTR_N,
            COD_TIT_INTERN,
            COD_INDICE_EMIS_TIT,
            COD_TRANS_ACQU_PCT,
            COD_PERIOD_ELAB,
            DAT_INIZ_PERIOD,
            DAT_FINE_PERIOD,
            COD_TIPO_PROD_INVEST
		FROM TABLE (cRec)
		GROUP BY 
            CLI_C,
			PROD_C,
			CONTR_N,
            COD_TIT_INTERN,
            COD_INDICE_EMIS_TIT,
            COD_TRANS_ACQU_PCT,
            COD_PERIOD_ELAB,
            DAT_INIZ_PERIOD,
            DAT_FINE_PERIOD,
            COD_TIPO_PROD_INVEST
		HAVING COUNT(1)>1
		) ap;


	INSERT
	INTO SOGG_OUTELAB.DETT_LOG_ERR
		(
			PROD_C,
			CONTR_N,
            COD_TIT_INTERN,
            COD_INDICE_EMIS_TIT,
			COD_ERR_ELAB,
			DES_ERR_ELAB,
			GSTD_D_INS_RECORD,
			GSTD_X_TIP_MODF,
			GSTD_X_USER,
			GSTD_M_NOM_ULT_MODF
		)
	SELECT ap.prod_c,
		ap.contr_n,
        ap.cod_tit_intern,
        ap.cod_indice_emis_tit,
		0,
		'motore titoli CRUSCOTTI: valore null',
		SYSDATE,
		'I',
		'BATCH',
		'BATCH'
	FROM
		(SELECT 
            CLI_C,
            PROD_C,
			CONTR_N,
            COD_TIT_INTERN,
            COD_INDICE_EMIS_TIT,
            COD_TRANS_ACQU_PCT,
            COD_PERIOD_ELAB,
            DAT_INIZ_PERIOD,
            DAT_FINE_PERIOD,
            DAT_RIFE
		FROM TABLE (cRec)
        where 
        CLI_C is null OR
        PROD_C is null OR
        CONTR_N is null OR
        COD_TIT_INTERN is null OR
        COD_INDICE_EMIS_TIT is null OR
        COD_TRANS_ACQU_PCT is null OR
        COD_PERIOD_ELAB is null OR
        DAT_INIZ_PERIOD is null OR
        DAT_FINE_PERIOD is null OR
        COD_TIPO_PROD_INVEST is null OR
        DAT_RIFE is null
        ) ap;




	IF dest='A' THEN


		INSERT
		INTO SOGG_OUTELAB.DETT_REDDTA_TIT_PERIOD
			(
        CLI_C
        ,COD_PERIOD_ELAB
        ,PROD_C
        ,CONTR_N
        ,COD_TIT_INTERN
        ,COD_INDICE_EMIS_TIT
        ,DAT_INIZ_PERIOD
        ,DAT_FINE_PERIOD
        ,COD_TRANS_ACQU_PCT
        ,COD_TIPO_PROD_INVEST
        ,DAT_RIFE
        ,COD_LINEA_BUSINESS
        ,DAT_APER_CONTR
        ,DAT_CHIU_CONTR
        ,DAT_PRIMO_MOVI
        ,DAT_ULT_MOVI
        ,IMP_TOT_INVEST
        ,IMP_TOT_INVEST_NO_CONVS
        ,IMP_TOT_VERS
        ,IMP_TOT_VERS_NO_CONVS
        ,IMP_TOT_RIMBO
        ,IMP_TOT_RIMBO_NO_CONVS
        ,IMP_PRVNT_FUT
        ,IMP_PRVNT_PASS
        ,QTA_QUOTA_T1
        ,QTA_QUOTA_T2
        ,IMP_INT_VERS
        ,IMP_SALDO_VERS
        ,IMP_INT_INVEST
        ,IMP_SALDO_INVEST
        ,IMP_CONTVAL_T1
        ,IMP_CONTVAL_T2
        ,IMP_PLUS_MINUS_INVEST
        ,IMP_PLUS_MINUS_VERS
        ,PRC_REND_VPATR_INVEST_ANNUAL
        ,PRC_REND_VPATR_INVEST_ST
        ,PRC_REND_VPATR_VERS_ANNUAL
        ,PRC_REND_VPATR_VERS_ST
        ,PRC_REND_MWRR_INVEST_ANNUAL
        ,PRC_REND_MWRR_INVEST_ST
        ,PRC_REND_MWRR_VERS_ANNUAL
        ,PRC_REND_MWRR_VERS_ST
        ,FLG_CHIUSO
        ,FLG_ZERO
        ,DES_LINEA_BUSINESS
        ,IMP_MEDIO_VERS
        ,IMP_MEDIO_INVEST
        ,GSTD_M_NOM_ULT_MODF
        ,GSTD_X_USER
        ,GSTD_D_ULT_MODF_RECORD
        ,GSTD_D_INS_RECORD
        ,GSTD_X_TIP_MODF
        ,GSTD_F_ESIST
                    )
         SELECT
        distinct
        ap.CLI_C                        ,
        ap.COD_PERIOD_ELAB              ,
        ap.PROD_C                       ,
        ap.CONTR_N                      ,
        ap.COD_TIT_INTERN               ,
        ap.COD_INDICE_EMIS_TIT          ,
        ap.DAT_INIZ_PERIOD              ,
        ap.DAT_FINE_PERIOD              ,
        ap.COD_TRANS_ACQU_PCT           ,
        ap.COD_TIPO_PROD_INVEST         ,
        ap.DAT_RIFE                     ,
        ap.LOB_C                        ,
        ap.DAT_APER_CONTR               ,
        ap.DAT_CHIU_CONTR               ,
        ap.DAT_PRIMO_MOVI               ,
        ap.DAT_ULT_MOVI                 ,
        ap.IMP_TOT_INVEST               ,
        ap.IMP_TOT_INVEST_NO_CONVS      ,
        ap.IMP_TOT_VERS                 ,
        ap.IMP_TOT_VERS_NO_CONVS        ,
        ap.IMP_TOT_RIMBO                ,
        ap.IMP_TOT_RIMBO_NO_CONVS       ,
        ap.IMP_PRVNT_FUT                ,
        ap.IMP_PRVNT_PASS               ,
        ap.QTA_QUOTA_T1                 ,
        ap.QTA_QUOTA_T2                 ,
        ap.IMP_INT_VERS                 ,
        ap.IMP_SALDO_VERS               ,
        ap.IMP_INT_INVEST               ,
        ap.IMP_SALDO_INVEST             ,
        ap.IMP_CONTVAL_T1               ,
        ap.IMP_CONTVAL_T2               ,
        ap.IMP_PLUS_MINUS_INVEST        ,
        ap.IMP_PLUS_MINUS_VERS          ,
        ap.PRC_REND_VPATR_INVEST_ANNUAL ,
        ap.PRC_REND_VPATR_INVEST_ST     ,
        ap.PRC_REND_VPATR_VERS_ANNUAL   ,
        ap.PRC_REND_VPATR_VERS_ST       ,
        ap.PRC_REND_MWRR_INVEST_ANNUAL  ,
        ap.PRC_REND_MWRR_INVEST_ST      ,
        ap.PRC_REND_MWRR_VERS_ANNUAL    ,
        ap.PRC_REND_MWRR_VERS_ST        ,
        ap.FLG_CHIUSO                   ,
        ap.FLG_ZERO                     ,
        ap.DES_LINEA_BUSINESS           ,
        ap.IMP_MEDIO_VERS               ,
        ap.IMP_MEDIO_INVEST             ,
        'MOT-BMED',
        'MOT-BMED',
        SYSDATE,
        SYSDATE,
        'I',
        'S'
		FROM TABLE (cRec) ap
		WHERE
			(
				ap.cli_c, trim(ap.prod_c), trim(ap.contr_n), trim(ap.cod_tit_intern), trim(ap.cod_indice_emis_tit), trim(ap.cod_trans_acqu_pct), trim(ap.COD_PERIOD_ELAB), ap.DAT_INIZ_PERIOD, AP.DAT_FINE_PERIOD, trim(ap.COD_TIPO_PROD_INVEST)
			)
			NOT IN

			(SELECT
                cli_c,
				TRIM(prod_c),
				TRIM(contr_n),
                TRIM(cod_tit_intern),
                TRIM(cod_indice_emis_tit),
                TRIM(cod_trans_acqu_pct),
				TRIM(COD_PERIOD_ELAB),
				DAT_INIZ_PERIOD,
				DAT_FINE_PERIOD,
                TRIM(COD_TIPO_PROD_INVEST)
			FROM TABLE (cRec)
			GROUP BY cli_c,
                TRIM(prod_c),
				TRIM(contr_n),
                TRIM(cod_tit_intern),
                TRIM(cod_indice_emis_tit),
                TRIM(cod_trans_acqu_pct),
                TRIM(COD_PERIOD_ELAB),
				DAT_INIZ_PERIOD,
				DAT_FINE_PERIOD,
                TRIM(COD_TIPO_PROD_INVEST)
			HAVING COUNT(1)>1
			)
		AND NOT EXISTS
			(SELECT 1
			FROM SOGG_OUTELAB.DETT_REDDTA_TIT_PERIOD ds
			WHERE ap.cli_c         =ds.cli_c
			AND TRIM(ap.prod_c)          =TRIM(ds.prod_c)
			AND TRIM(ap.contr_n)         =TRIM(ds.contr_n)
			AND TRIM(ap.cod_tit_intern)  =TRIM(ds.cod_tit_intern)
			AND TRIM(ap.cod_indice_emis_tit) =TRIM(ds.cod_indice_emis_tit)
			AND TRIM(ap.cod_trans_acqu_pct) = TRIM(ds.cod_trans_acqu_pct)
			AND TRIM(ds.COD_PERIOD_ELAB) = TRIM(ap.COD_PERIOD_ELAB)
			AND DS.DAT_INIZ_PERIOD = ap.DAT_INIZ_PERIOD
			AND DS.DAT_FINE_PERIOD = ap.DAT_FINE_PERIOD
            AND TRIM(ds.COD_TIPO_PROD_INVEST) = TRIM(ap.COD_TIPO_PROD_INVEST)
			);


	END IF;
	COMMIT;

END;

END;
/
show error;
