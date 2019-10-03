create or replace PACKAGE UT3.PACKAGE_DA_TESTARE AS 
 
--inserito commento
--inserito secondo commento
  procedure moltiplica(add1 in number, add2 in number, ris out number);

END PACKAGE_DA_TESTARE;
/

create or replace PACKAGE BODY UT3.PACKAGE_DA_TESTARE IS 

  procedure moltiplica(add1 in number, add2 in number, ris out number)
    AS
    BEGIN
        ris := add1*add2;
    END;

END PACKAGE_DA_TESTARE;
/
