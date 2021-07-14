/*
--產生全部的組合
create or replace function pyarrcombin (lst char[])
returns setof char[]
as $$
  from itertools import combinations
  return [list(t) for r in range(1, len(lst)+1) for t in combinations(lst,r)]
$$ language plpythonu;

--產生符合指定的數目元素
create or replace function pyarrcombin (lst char[], n int)
returns setof char[]
as $$
  from itertools import combinations
  return [list(t) for t in combinations(lst,n)]
$$ language plpythonu;

--產生符合指定的數目元素,且包含指定元素
create or replace function pyarrcombin (lst char[], n int, f char)
returns setof char[]
as $$
  from itertools import combinations
  return [list(t) for t in combinations(lst,n) if f in t]
$$ language plpythonu;
*/

SELECT security_db.func_drop_func_with_any_arg( 'db_owner','proc_estimate_dkey');

CREATE OR REPLACE FUNCTION db_owner.proc_estimate_dkey(v_DatabaseName varchar(128), v_TableName varchar(128), v_dtColumnName varchar(128), v_NoOfDkeyColumns int default 3, v_flag text default 'xxx')
  RETURNS text
  LANGUAGE plpgsql
  VOLATILE
AS $$

DECLARE
  v_ColumnArray text;
  v_dkey text;
  v_est_query text;
  v_SqlStmt text;
  v_DataType text;
  v_MaxOcfDate char(8);

  ----------debug parameter----------
  v_debug_date        text;   -- OMNIWARESOFT: the date of execution
  v_debug_start_time  text;   -- OMNIWARESOFT: the time of the execution(procedure)
  v_debug_func_schema text;
  v_debug_func_name   text;
  v_debug_msg_id      text;
  v_debug_msg_desc    text;
  v_t1                text;
  v_t2                text;
  v_t3                text;
  v_debug_me          text := null;

BEGIN
  --------------------------debug---------------------------
  SELECT * FROM db_owner.func_init_dateandtime_out() INTO v_debug_date, v_debug_start_time;
  GET DIAGNOSTICS v_debug_func_name = PG_CONTEXT;
  SELECT * FROM db_owner.func_fname_sname_out(v_debug_func_name) INTO v_debug_func_schema, v_debug_func_name;
  v_debug_msg_id := 'PARAM';
  v_debug_msg_desc := 'SELECT ' || v_debug_func_schema || '.' || v_debug_func_name || '(''' || $1 || ''',''' || $2 || ''',''' || $3 || ''',''' || $4 || ''')';
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, v_debug_msg_id, v_debug_msg_desc) INTO v_debug_me;
  --------------------------debug---------------------------

  select string_agg(column_name, ',') into v_ColumnArray
    from information_schema.columns
   where table_schema = lower(v_DatabaseName)
     and table_name = lower(v_TableName);

  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00010', 'v_ColumnArray='''|| v_ColumnArray ||'''') into v_debug_me;
  IF v_FLAG = '00020' then return 'xxx'; end if;
  --------------------------debug---------------------------

  drop table if exists tmp_dkey_combinations;
  create temp table tmp_dkey_combinations as
--    select pyarrcombin(string_to_array(v_ColumnArray, ','), v_NoOfDkeyColumns, v_dtColumnName::char varying)
    select pyarrcombin(string_to_array(v_ColumnArray, ','), v_NoOfDkeyColumns)
  distributed randomly;

  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00020', 'create temp table tmp_dkey_combinations') into v_debug_me;
  IF v_FLAG = '00030' then return 'xxx'; end if;
  --------------------------debug---------------------------

  drop table if exists tmp_dkey_combinations2;
  create temp table tmp_dkey_combinations2 as
    select replace(replace(pyarrcombin::text, '{', ''), '}', '') as dkey
      from tmp_dkey_combinations
  distributed randomly;

  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00030', 'create temp table tmp_dkey_combinations2') into v_debug_me;
  IF v_FLAG = '00040' then return 'xxx'; end if;
  --------------------------debug---------------------------

  drop table if exists tmp_dkey_est_result;
  create temp table tmp_dkey_est_result as
    select dkey, null::int as Max_Seg_Rows, null::int as Min_Seg_Rows, null::numeric as Pcnt_Diff_Bt_Max_Min
      from tmp_dkey_combinations2
  distributed randomly;

 --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00040', 'create temp table tmp_dkey_est_result') into v_debug_me;
  IF v_FLAG = '00050' then return 'xxx'; end if;
  --------------------------debug---------------------------

  v_SqlStmt = 'select pg_typeof('||v_dtColumnName||') from '||v_DatabaseName||'.'||v_TableName||' limit 1;';

  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00050', 'v_SqlStmt='''|| v_SqlStmt ||'''') into v_debug_me;
  IF v_FLAG = '00060' then return 'xxx'; end if;
  --------------------------debug---------------------------

  execute v_SqlStmt into v_DataType;

  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00060', 'v_DataType='''|| v_DataType ||'''') into v_debug_me;
  IF v_FLAG = '00070' then return 'xxx'; end if;
  --------------------------debug---------------------------

  if v_DataType = 'character' then
    v_SqlStmt = 'select max('||v_dtColumnName||') from '||v_DatabaseName||'.'||v_TableName||';';
  else -- v_DataType = 'timestamp without time zone'
    v_SqlStmt = 'select to_char(max('||v_dtColumnName||'), ''yyyymmdd'') from '||v_DatabaseName||'.'||v_TableName||';';
  end if;

  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00070', 'v_SqlStmt='''|| v_SqlStmt ||'''') into v_debug_me;
  IF v_FLAG = '00080' then return 'xxx'; end if;
  --------------------------debug---------------------------

  execute v_SqlStmt into v_MaxOcfDate;

 --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00080', 'v_MaxOcfDate='''|| v_MaxOcfDate ||'''') into v_debug_me;
  IF v_FLAG = '00090' then return 'xxx'; end if;
  --------------------------debug---------------------------

  for v_dkey, v_est_query in
    select dkey,
           'select max(c) as _Max_Seg_Rows, min(c) as _Min_Seg_Rows, (max(c)-min(c))*100.0/max(c) as _Pcnt_Diff_Bt_Max_Min from (select '||dkey||', count(*) c from '||lower(v_DatabaseName)||'.'||lower(v_TableName)||' where '||v_dtColumnName||' = '||quote_literal(v_MaxOcfDate)||' group by '||dkey||') as t' as est_query
      from tmp_dkey_combinations2 --limit 10
  loop
  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00090', 'v_est_query='''|| v_est_query ||'''') into v_debug_me;
  IF v_FLAG = '00100' then return 'xxx'; end if;
  --------------------------debug---------------------------

    execute 'drop table if exists t1; create temp table t1 as '||v_est_query||' distributed randomly;';

    update tmp_dkey_est_result
       set Max_Seg_Rows = _Max_Seg_Rows,
           Min_Seg_Rows = _Min_Seg_Rows,
           Pcnt_Diff_Bt_Max_Min = _Pcnt_Diff_Bt_Max_Min
      from t1
     where dkey = v_dkey;

  end loop;

  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00100', 'update tmp_dkey_est_result') into v_debug_me;
  IF v_FLAG = '00110' then return 'xxx'; end if;
  --------------------------debug---------------------------


  PERFORM db_owner.func_py_insert_debug('insert into db_owner.debug_log_s '|| v_debug_me ||')');
  RETURN 'S';

  EXCEPTION WHEN OTHERS THEN
  PERFORM db_owner.func_py_insert_debug('insert into db_owner.debug_log_f '|| v_debug_me ||')');
  GET STACKED DIAGNOSTICS v_t1 = MESSAGE_TEXT,v_t2 = PG_EXCEPTION_DETAIL, v_t3 = PG_EXCEPTION_CONTEXT;
  v_t1 := v_t1 || '  ' || v_t2 || '  ' || v_t3;
  RAISE EXCEPTION '%', v_t1;

END; $$
;

/*
select db_owner.proc_estimate_dkey('db_owner', 'svel_2side', 'svel_2side_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'svel_2side_ah', 'svel_2side_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'result_c7_10_for_141', 'svel_2side_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'hpoi', 'hpoi_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'hpoi', 'hpoi_yymmdd', 4);
select db_owner.proc_estimate_dkey('db_owner', 'm_oab', 'm_oab_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'm_oab_ah', 'm_oab_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'ocf_hpoi', 'hpoi_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'htop10', 'htop10_yymmdd');
select db_owner.proc_estimate_dkey('db_owner', 'omhtop10', 'omhtop10_yymmdd');
--select * from tmp_dkey_combinations;
--select * from tmp_dkey_combinations2;
select count(*) from tmp_dkey_combinations2;
select * from tmp_dkey_est_result order by 4;
--select * from t1;

select * from db_owner.debug_log_s
 where debug_date = to_char(current_date, 'yyyymmdd')
   and debug_func_schema = 'db_owner'
   and debug_func_name = 'proc_estimate_dkey'
 order by debug_w_time desc;

select * from db_owner.debug_log_f
 where debug_date = to_char(current_date, 'yyyymmdd')
   and debug_func_schema = 'db_owner'
   and debug_func_name = 'proc_estimate_dkey'
 order by debug_w_time desc;
*/