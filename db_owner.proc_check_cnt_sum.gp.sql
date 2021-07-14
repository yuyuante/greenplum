SELECT security_db.func_drop_func_with_any_arg( 'db_owner','proc_check_cnt_sum');

CREATE OR REPLACE FUNCTION db_owner.proc_check_cnt_sum(v_DatabaseName varchar(128), v_TableName varchar(128), v_BeginTime varchar(8) default '', v_EndTime varchar(8) default '', v_flag text default 'xxx')
  RETURNS text
  LANGUAGE plpgsql
  VOLATILE
AS $$

DECLARE
  v_CreateTableStmt VARCHAR(8192);
  v_CreateTableField VARCHAR(8192);
  v_SelectStmt VARCHAR(8192);
  v_SelectField VARCHAR(8192);
  v_DataDtColumnName VARCHAR(8192);
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


  SELECT LOWER(v_DatabaseName), LOWER(v_TableName) into v_DatabaseName, v_TableName;
  IF v_DatabaseName = 'temp' THEN
    SELECT table_schema into v_DatabaseName FROM information_schema.tables WHERE table_name = v_TableName;
  END IF;

  SELECT TRIM(trailing ',' from (STRING_AGG(pa.attname||' numeric', ',' order by pa.attnum) ::VARCHAR(4096))) into v_CreateTableField
    FROM pg_catalog.pg_attribute AS pa
      LEFT JOIN pg_catalog.pg_type AS pt
        ON pa.atttypid  =  pt.oid
   WHERE pa.attrelid = (v_DatabaseName||'.'||v_TableName)::regclass
     AND pa.attnum > 0
     AND pt.typname IN ('float4', 'float8', 'int2', 'int4', 'int8', 'numeric');
  IF v_CreateTableField IS NULL THEN
    RAISE EXCEPTION '%', v_DatabaseName||'.'||v_TableName||' has no numeric columns';
  END IF;

  v_CreateTableStmt := 'DROP TABLE IF EXISTS tmp_RESULT; CREATE TEMP TABLE tmp_RESULT (_count_ numeric, '||v_CreateTableField||') ON COMMIT PRESERVE ROWS;';
  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00010', 'v_CreateTableStmt='''|| v_CreateTableStmt ||'''') into v_debug_me;
  IF v_FLAG = '00020' then return 'xxx'; end if;
  --------------------------debug---------------------------
  EXECUTE v_CreateTableStmt;

  --------------------------flag---------------------------
  IF v_FLAG = '00030' then return 'xxx'; end if;
  --------------------------flag---------------------------

  --SELECT TRIM(trailing ',' from (STRING_AGG('SUM(CAST(COALESCE('||pa.attname||', 0) as numeric)) AS sum_'||pa.attname, ',' order by pa.attnum) ::VARCHAR(4096))) into v_SelectField
  SELECT STRING_AGG('SUM(CAST(COALESCE('||pa.attname||', 0) as numeric)) AS sum_'||pa.attname, ',' order by pa.attnum) into v_SelectField
    FROM pg_catalog.pg_attribute AS pa
      LEFT JOIN pg_catalog.pg_type AS pt
        ON pa.atttypid  =  pt.oid
   WHERE pa.attrelid = (v_DatabaseName||'.'||v_TableName)::regclass
     AND pa.attnum > 0
     AND pt.typname IN ('float4', 'float8', 'int2', 'int4', 'int8', 'numeric');

  IF v_DatabaseName LIKE '%temp%' OR (LOWER(v_BeginTime) = 'all' AND LOWER(v_EndTime) = 'all')THEN
    v_SelectStmt := 'INSERT INTO tmp_RESULT SELECT COUNT(*) as _count_, '||v_SelectField||' FROM '||v_DatabaseName||'.'||v_TableName||';';
  ELSE
    v_DataDtColumnName := '';
    SELECT datadtcolumnname INTO v_DataDtColumnName
      FROM twfe_xls.tablelist
     WHERE schemaname = v_DatabaseName AND tablename = v_TableName;

    v_SelectStmt := 'INSERT INTO tmp_RESULT SELECT COUNT(*) as _count_, '||v_SelectField||' FROM '||v_DatabaseName||'.'||v_TableName||' WHERE '||v_DataDtColumnName||' BETWEEN '''||v_BeginTime||''' AND '''||v_EndTime||''';';
  END IF;
  --------------------------debug---------------------------
  SELECT db_owner.func_gen_debug_me(v_debug_me, v_debug_date, v_debug_start_time, v_debug_func_schema, v_debug_func_name, '00030', 'v_SelectStmt='''|| v_SelectStmt ||'''') into v_debug_me;
  IF v_FLAG = '00040' then return 'xxx'; end if;
  --------------------------debug---------------------------
  EXECUTE v_SelectStmt;

  --------------------------flag---------------------------
  IF v_FLAG = '00050' then return 'xxx'; end if;
  --------------------------flag---------------------------


  PERFORM db_owner.func_py_insert_debug('insert into db_owner.debug_log_s '|| v_debug_me ||')');
  RETURN 'S';

  EXCEPTION WHEN OTHERS THEN
  PERFORM db_owner.func_py_insert_debug('insert into db_owner.debug_log_f '|| v_debug_me ||')');
  GET STACKED DIAGNOSTICS v_t1 = MESSAGE_TEXT,v_t2 = PG_EXCEPTION_DETAIL, v_t3 = PG_EXCEPTION_CONTEXT;
  v_t1 := v_t1 || '  ' || v_t2 || '  ' || v_t3;
  RAISE EXCEPTION '%', v_t1;

END; $$
