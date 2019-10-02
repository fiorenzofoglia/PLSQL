create or replace PACKAGE     PACKAGE_DA_TESTARE AS 

  procedure moltiplica(add1 in number, add2 in number, ris out number);

END PACKAGE_DA_TESTARE;

create or replace PACKAGE BODY     test_package_da_testare IS

   /* generated by utPLSQL for SQL Developer on 2019-10-01 16:58:27 */

   --
   -- test moltiplica case 1: ...
   --
   PROCEDURE moltiplica IS
      l_actual   INTEGER := 0;
      l_expected INTEGER := 1;
   BEGIN
      -- populate actual
      package_da_testare.moltiplica(1,2,l_actual);

      -- populate expected
      l_expected := 23;

      -- assert
      ut.expect(l_actual).to_equal(l_expected);
   END moltiplica;

END test_package_da_testare;
