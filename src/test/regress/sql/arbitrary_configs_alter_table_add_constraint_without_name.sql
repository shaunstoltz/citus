--
-- MULTI_ALTER_TABLE_ADD_CONSTRAINTS_WITHOUT_NAME
--
-- Test checks whether constraints of distributed tables can be adjusted using
-- the ALTER TABLE ... ADD without specifying a name.
SET search_path TO sc1;

-- Check "ADD PRIMARY KEY"
ALTER TABLE products ADD PRIMARY KEY(product_no);
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
	      WHERE rel.relname = 'products';

ALTER TABLE products DROP CONSTRAINT products_pkey;

ALTER TABLE products ADD PRIMARY KEY(product_no);


-- Check "ADD PRIMARY KEY" with reference table
-- Check for name collisions
ALTER TABLE products_ref_3 ADD CONSTRAINT products_ref_pkey PRIMARY KEY(name);
ALTER TABLE products_ref_2 ADD CONSTRAINT products_ref_pkey1 PRIMARY KEY(name);
ALTER TABLE products_ref ADD PRIMARY KEY(name);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname = 'products_ref';

ALTER TABLE products_ref DROP CONSTRAINT products_ref_pkey2;

-- Check with max table name (63 chars)
ALTER TABLE verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon ADD PRIMARY KEY(product_no);

-- Constraint should be created on the coordinator with a shortened name
SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
              WHERE rel.relname LIKE 'very%';

ALTER TABLE verylonglonglonglonglonglonglonglonglonglonglonglonglonglonglon DROP CONSTRAINT verylonglonglonglonglonglonglonglonglonglonglonglonglonglo_pkey;

-- Test the scenario where a partitioned distributed table has a child with max allowed name
-- Verify that we switch to sequential execution mode to avoid deadlock in this scenario
ALTER TABLE dist_partitioned_table ADD PRIMARY KEY(partition_col);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = 'dist_partitioned_table';

ALTER TABLE dist_partitioned_table DROP CONSTRAINT dist_partitioned_table_pkey;

-- Test primary key name is generated by postgres for citus local table.
ALTER TABLE citus_local_table ADD PRIMARY KEY(id);

SELECT con.conname
    FROM pg_catalog.pg_constraint con
      INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
      INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
          WHERE rel.relname = 'citus_local_table';
