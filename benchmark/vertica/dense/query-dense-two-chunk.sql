drop table t1;
select clear_caches();
select clear_caches();
select clear_caches();
\timing
select A1 INTO TEMP TABLE t1 ON COMMIT PRESERVE ROWS from :tablename where x>=0 and x<2500 and y>=0 and y<2000;
select count(*) from t1;
