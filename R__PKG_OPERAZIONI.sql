create or replace PACKAGE PKG_OPERAZIONI AS

PROCEDURE MOLTIPLICA(add1 in number, add2 in number, ris out number);

PROCEDURE DIVIDI(add1 in number, add2 in number, ris out number);

END PKG_OPERAZIONI;
/

create or replace PACKAGE BODY PKG_OPERAZIONI IS

  PROCEDURE MOLTIPLICA(add1 in number, add2 in number, ris out number)
    AS
    BEGIN
        ris := add1*add2;
    END;

  PROCEDURE DIVIDI(add1 in number, add2 in number, ris out number)
    AS
    BEGIN
        ris := add1/add2;
        IF ris = 6 THEN
            DBMS_OUTPUT.PUT_LINE('il risultato non Ã¨ sei');
        END IF;

    END;

END PKG_OPERAZIONI;
/
