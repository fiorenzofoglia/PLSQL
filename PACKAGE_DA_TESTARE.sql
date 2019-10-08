create or replace PACKAGE             UT3.PACKAGE_OPERAZIONI AS

procedure moltiplica(add1 in number, add2 in number, ris out number);

procedure dividi(add1 in number, add2 in number, ris out number);

END PACKAGE_DA_TESTARE;
/

create or replace PACKAGE BODY UT3.PACKAGE_DA_TESTARE IS 

  procedure moltiplica(add1 in number, add2 in number, ris out number)
    AS
    BEGIN
        ris := add1*add2;
    END;

  procedure dividi(add1 in number, add2 in number, ris out number)
    AS
    BEGIN
        ris := add1/add2;
        IF ris = 6 THEN
            DBMS_OUTPUT.PUT_LINE('il risultato non è sei');
        END IF;
        
    END;

END PACKAGE_OPERAZIONI;
/
