# utl-flag-the-visit-with-the-first-occurence-of-the-maximum-result-sql-dow-sort-relational
    Flag the visit with the first occurance of the maximum result sql dow sort relational

    Improved Solutions

          5. Mark -No index needed. Use Record Id and poiunt=record id to modily master
             Keintz, Mark @  mkeintz@wharton.upenn.edu

                Identify record ids 50 seconds
                Modify maste         0 seconds
                                    ===========
                                     50 seconds

          6. Paull - DATA DOES NOT HAVE TO BE SORTED

                Paul Dorfman @ sashole@bellsouth.net

                HASH                72 Seconds
                MODIFY               0 Seconds

          7. Bart - Nice addition 'CUROBS' to prevent possible corruption of master when using 'modify'.
             There is not rollback with 'modify?'
             Bartosz Jablonski    yabwon@gmail.com
             May also be able to do someting along the lines of.
             if _iorc_ then do;
                _error_=0;
                ** put system message and any user messaging;
                leave;
             end;

                HASH                90 Seconds    * not sure exactly why this took longer;
                MODIFY               0 Seconds


    github
    https://tinyurl.com/ycnoshxp
    https://github.com/rogerjdeangelis/utl-flag-the-visit-with-the-first-occurence-of-the-maximum-result-sql-dow-sort-relational

    inspired by
    https://tinyurl.com/ydcv6b4k
    https://communities.sas.com/t5/SAS-Programming/calculate-first-maximum-result/m-p/523468

    I changed this problem to update the flag on the master to 'Y' at
    the first occuarnce of a maximum. Not exacly a fair comparison because the
    other solutions did not use the input with an index. However the result does hold for
    many problems.

    Benchmarks  Set flag to 'Y' for first occurance of maximum by test.
                Small data 14gb and 450 million observations
    ==================================================================

        1. Using a relational schema (sort of). Preferred for many reasons)

           Index    31 seconds  (ran input with and without creating index)
           DOW     132 seconds
           Modify    0          (update master - less than .5 seconds very few updates)
                   ============
                   162 seconds

        2. Single DOW Loop (no index) KSharpe and Mark Keintz DOW (REALLY UNFAIR COMPARISON)

            DOW     227 seconds (because output was 450 million obs)

                    https://communities.sas.com/t5/user/viewprofilepage/user-id/18408
                    https://communities.sas.com/t5/user/viewprofilepage/user-id/31461


        3. Sort View Join (no index - not fair to Reeza - REALLY UNFAIR)
           Reeza
           https://communities.sas.com/t5/user/viewprofilepage/user-id/13879

           SORT     273
           View     0
           SORT     267
                    ====
                    540

        4. SQL (no index - I should reprogram to use index - too lazy - REALLY UNFAIR)
           (Good to have pure SQL solution should be competitve with index especially in Teradata or Exadata)
           Novinosrin
           https://communities.sas.com/t5/user/viewprofilepage/user-id/138205

           gave up after 1800 seconds


    The preferred method is not only faster than all the other methods below, uses less space,
    lends itself to a relational schema and minimizes data integrity issues.

    Sometimes Oracle and Teradata will automaticall build a temp index for problems like thi.

    It is better NOT to make a copy of teh master 14gb fact table.
    The output here is the much smaller flagged dimension table.
    ans the fact table with flagged first max value.


    NOTE ABOUT BIG DATA
    -------------------
    It would be nice is ops used the sizes below instead od big data for all four sizws.
    This isjust a guideline with lots of exceptions.

       1. Tiny data :  Less than 10gb.
       2. Small data :  10gb - 100gb.
       3. Moderate data 100gb - 1Tb.
       4. Big data > 1Tb (probably need a robust server)



    INPUT  Small data 14gb and 450 million observations
    ====================================================

    Distance beteen flags is much greater. Eaxample to demonstrate technique)


     WORK.HAVE total obs=450 million           | RULES
                                               |
      ID     TEST    VISIT     RESULT  FLAG    | FLAG

      101    rbc     visit1      222     N     |   N
      101    rbc     visit2      222     N     |   N
      101    rbc     visit3      300     N     |   Y     ** first occur of max   update N to Y

      101    wbc     visit1      222     N     |   N
      101    wbc     visit2      222     N     |   N
      101    wbc     visit3      300     N     |   Y     ** first occur of max   update N to Y
      101    wbc     visit4      300     N     |   N

      102    rbc     visit1      222     N     |   N
      102    rbc     visit2      222     N     |   N
      102    rbc     visit3      300     N     |   Y     ** first occur of max

      102    wbc     visit1      222     N     |   N
      102    wbc     visit2      222     N     |   N
      102    wbc     visit3      300     N     |   N
      102    wbc     visit4      400     N     |   Y     ** first occur of max
      102    wbc     visit5      400     N     |   N



     EXAMPLE MASTER and small dimension table (Small data 14gb and 450 million observations)
     ---------------------------------------------------------------------------------------

     WORK.WANT total obs=450 million obs

      ID     TEST    VISIT     RESULT   FLAG

      101    rbc     visit1      222      N
      101    rbc     visit2      222      N
      101    rbc     visit3      300      Y
      101    wbc     visit1      222      N
      101    wbc     visit2      222      N
      101    wbc     visit3      300      Y
      101    wbc     visit4      300      N
      102    rbc     visit1      222      N
      102    rbc     visit2      222      N
      102    rbc     visit3      300      Y
      102    wbc     visit1      222      N
      102    wbc     visit2      222      N
      102    wbc     visit3      300      N
      102    wbc     visit4      400      Y
      102    wbc     visit5      400      N

    RELATIONAL DIMENSION TABLE UPDATE MASTER WHERE MASTER.RID = WANT.RID

    Up to 40 obs from WANT total obs=4

    Obs       RID       VISIT     ID    TEST    RESULT    FLAG

     1      40000001      3      101    rbc       300      Y
     2     100000001      3      101    wbc       300      Y
     3     180000001      3      102    rbc       300      Y
     4     260000001      4      102    wbc       400      Y



    PROCESS
    =======

    1. Using a relational schema (sort of). Preferred for many reasons)
    ---------------------------------------------------------------------

    proc datasets lib=work;
     delete want have;
    run;quit;

    data have(sortedby=rid /* index=(rid/unique) */) ;
      retain rid 0 flag 'N';
      length visit id 3 test $3;
      input id test$  visit  result ;
      do rec=1 to 30e6;
         rid=rid+1;
         output;
      end;
      drop rec;
    cards4 ;
    101 rbc 1 222
    101 rbc 2 222
    101 rbc 3 300
    101 wbc 1 222
    101 wbc 2 222
    101 wbc 3 300
    101 wbc 4 300
    102 rbc 1 222
    102 rbc 2 222
    102 rbc 3 300
    102 wbc 1 222
    102 wbc 2 222
    102 wbc 3 300
    102 wbc 4 400
    102 wbc 5 400
    ;;;;
    run;quit;

    NOTE: The data set WORK.HAVE has 450000000 observations and 6 variables.
    NOTE: DATA statement used (Total process time):
          real time           1:16.82
          cpu time            56.08 seconds

    data want;
     do until(last.test);
      set have ;
      by id test notsorted;
      if result > max then max=max(max,result);
     end;
     do until(last.test);
      set have ;
      by id test notsorted;
      if result=max and not beenthere then do;flag='Y';beenthere=1;output;end;
      else flag='';
     end;
     drop max beenthere;
    run;

    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: The data set WORK.WANT has 4 observations and 6 variables.
    NOTE: DATA statement used (Total process time):
          real time           2:11.69
          cpu time            1:35.08

    data have ;
      retain flag '';
      modify have want(keep=rid flag);
      by rid;
    run;quit;

    NOTE: There were 1 observations read from the data set WORK.HAVE.
    NOTE: The data set WORK.HAVE has been updated.

    There were 4 observations rewritten,

    0 observations added and 0 observations deleted.
    NOTE: There were 4 observations read from the data set WORK.WANT.
    NOTE: DATA statement used (Total process time):
          real time           0.04 seconds
          cpu time            0.00 seconds



    2. Single DOW Loop (no index) KSharpe and Mark Keitz DOW (REALLY UNFAIR)
    -------------------------------------------------------------------------

    proc sql;
      drop index rid from have;
    ;quit;

    data want;
     do until(last.test);
      set have ;
      by id test notsorted;
      if result > max then max=max(max,result);
     end;
     do until(last.test);
      set have ;
      by id test notsorted;
      if result=max and not beenthere then do;flag='Y';beenthere=1;end;
      else flag='';
      output;
     end;
     drop max beenthere;
    run;

    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: The data set WORK.WANT has 450000000 observations and 6 variables.
    NOTE: DATA statement used (Total process time):
          real time           3:46.70
          cpu time            2:08.60



    3. Sort View Join (no index - not fair to Reeza - REALLY UNFAIR)
    =================================================================

    proc datasets lib=work;
     delete havSrt;
    run;quit;

    proc sort data=have out=havSrt;
        by id test result descending visit;
    run;

    proc datasets lib=work;
     delete want;
    run;quit;

    %let beg=%sysfunc(time());

    data want_vue/view=want_vue;
        set havSrt;
        by id test;

        if first.test then
            flag='Y';
    run;

    proc sort data=want_vue out=want;
        by id test visit;
    run;

    %put %sysevalf( %sysfunc(time()) - &beg );

    sort
    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: The data set WORK.HAVSRT has 450000000 observations and 6 variables.
    NOTE: PROCEDURE SORT used (Total process time):
          real time           4:32.22
          cpu time            5:01.81

    sort
    NOTE: There were 450000000 observations read from the data set WORK.HAVSRT.
    NOTE: The data set WORK.WANT has 450000000 observations and 6 variables.
    NOTE: PROCEDURE SORT used (Total process time):
          real time           4:30.60
          cpu time            5:19.74


    4. SQL (no index - I should reprogram to use index - too lazy - REALLY UNFAIR)
    -------------------------------------------------------------------------------

    proc sql;
       create
          table want(drop=mm) as
       select
          *
         ,ifc(visit=min(visit) and mm=result,'Y',' ') as Flag
       from
          (select
              *
             ,ifn(max(result)=result, max(result),.) as mm
          from
              have
          group
              by id,test
          )
       group
          by id, test, mm
       order
          by id, test, visit;
    quit;



    5. Mark -No index needed. Use Record Id and poiunt=record id to modily master
       Keintz, Mark @  mkeintz@wharton.upenn.edu
       ---------------------------------------------------------------------------

          Identify record ids 50 seconds
          Modify maste         0 seconds
                              ===========
                              50 seconds

    %let beg=%sysfunc(time());

    data need (keep=rid);
      set have;
      by id test;
      if first.test then do;_max=result; rid=_n_;end;
      retain _max rid;
      if result>_max then do;_max=result; rid=_n_;end;
      if last.test;
    run;

    data have;
      set need;
      modify have point=rid;
      flag='Y';
      replace;
    run;

    %put %sysevalf( %sysfunc(time()) - &beg );

    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: The data set WORK.NEED has 4 observations and 1 variables.
    NOTE: DATA statement used (Total process time):
          real time           50.18 seconds
          cpu time            49.88 seconds


    89    data have;
    90      set need;
    91      modify have point=rid;
    92      flag='Y';
    93      replace;
    94    run;

    NOTE: The variable RID exists on an input data set, but was also specified in an I/O statement option.
    NOTE: There were 4 observations read from the data set WORK.NEED.
    NOTE: The data set WORK.HAVE has been updated.  There were 4 observations rewritten, 0 observations added and 0
    NOTE: DATA statement used (Total process time):
          real time           0.03 seconds
          cpu time            0.00 seconds


    6. Paull - DATA DOES NOT HAVE TO BE SORTED
    -------------------------------------------

          Paul Dorfman @ sashole@bellsouth.net

          HASH                72 Seconds
          MODIFY               0 Seconds


    The main drag here is the need for two passes through the data.
    That can be eliminated by using a small hash (small because by design it will contain
    only a single item from each [id,test] key group):

    %let beg=%sysfunc(time());

    data _null_ ;
      dcl hash h () ;
      h.definekey ("id", "test") ;
      h.definedata ("res", "vis", "rid") ;
      h.definedone () ;
      do _n_ = 1 by 1 until (z) ;
        set have end = z ;
        f = h.find() ;
        if f ne 0 or (f = 0 and (result > res or (result = res and visit < vis))) then do ;
          res = result ;
          vis = visit ;
          rid = _n_ ;
          h.replace() ;
        end ;
      end ;
      h.output (dataset: "rid (keep = rid)") ;
    run ;

    data have ;
      set rid ;
      modify have point = rid ;
      flag = "Y" ;
      replace ;
    run ;

    %put %sysevalf( %sysfunc(time()) - &beg );

    Note that the above approach doesn't presume the input either sorted or indexed,
    and it does the whole job in a single pass. I've tested it against the 450,000,000
    input records (on a 10 year old ThinkPad laptop I'm using to type this) and got 72
    seconds for the hash step and 0 (but of course) seconds for the MODIFY step.
    I'd be interested to know how it fares against the other techniques on your awesome hardware.


    NOTE: The data set WORK.RID has 4 observations and 1 variables.
    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: DATA statement used (Total process time):
         real time           1:11.77
         cpu time            1:11.24



    14   data have ;
    15     set rid ;
    16     modify have point = rid ;
    17     flag = "Y" ;
    18     replace ;
    19   run ;

    NOTE: The variable RID exists on an input data set, but was also specified in an I/O statement
    NOTE: There were 4 observations read from the data set WORK.RID.
    NOTE: The data set WORK.HAVE has been updated.  There were 4 observations rewritten, 0 observat
    NOTE: DATA statement used (Total process time):
         real time           0.01 seconds
         cpu time            0.01 second



    20   %put %sysevalf( %sysfunc(time()) - &beg );
    71.7910001277996



    7. Bart - Nice addition 'CUROBS' to prevent possible corruption of master when using 'modfy'.
       There is not rollback with 'modify?'
       Bartosz Jablonski    yabwon@gmail.com
       -----------------------------------------------------------------------------------------



    If I may add small piece to "robustness"? Since there is is MODIFY in use
    (i.e. programmer is capable to work with it), there could be MODIFY & DELETE in use,
    so I would add CUROBS= option, to be sure that read rid is ok.

    all the best
    Bart

    data _null_ ;
      dcl hash h () ;
      h.definekey ("id", "test") ;
      h.definedata ("res", "vis", "rid") ;
      h.definedone () ;
      do _n_ = 1 by 1 until (z) ;
        set have end = z CUROBS=CUROBS ;
        f = h.find() ;
        if f ne 0 or (f = 0 and (result > res or (result = res and visit < vis))) then do ;
          res = result ;
          vis = visit ;
          rid = CUROBS ; /* instead of _n_ */

          h.replace() ;
        end ;
      end ;
      h.output (dataset: "rid (keep = rid)") ;
    run ;


    if _iorc_ then do;
       _error_=0;
       ** put system message and any user messaging;
       leave;
    end;

    NOTE: The data set WORK.RID has 4 observations and 1 variables.
    NOTE: There were 450000000 observations read from the data set WORK.HAVE.
    NOTE: DATA statement used (Total process time):
          real time           1:30.26
          cpu time            1:29.57


    OUTPUT

     see above

    *                _               _       _
     _ __ ___   __ _| | _____     __| | __ _| |_ __ _
    | '_ ` _ \ / _` | |/ / _ \   / _` |/ _` | __/ _` |
    | | | | | | (_| |   <  __/  | (_| | (_| | || (_| |
    |_| |_| |_|\__,_|_|\_\___|   \__,_|\__,_|\__\__,_|

    ;
    * also above;

    proc datasets lib=work;
     delete want have;
    run;quit;

    data have(sortedby=rid index=(rid/unique)) ;
      retain rid 0 flag 'N';
      length visit id 3 test $3;
      input id test$  visit  result ;
      do rec=1 to 30e6;
         rid=rid+1;
         output;
      end;
      drop rec;
    cards4 ;
    101 rbc 1 222
    101 rbc 2 222
    101 rbc 3 300
    101 wbc 1 222
    101 wbc 2 222
    101 wbc 3 300
    101 wbc 4 300
    102 rbc 1 222
    102 rbc 2 222
    102 rbc 3 300
    102 wbc 1 222
    102 wbc 2 222
    102 wbc 3 300
    102 wbc 4 400
    102 wbc 5 400
    ;;;;
    run;quit;

