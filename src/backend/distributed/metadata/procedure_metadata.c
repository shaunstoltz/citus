/*
 * procedure_metadata.c
 *   Functions to load metadata on distributed stored procedures.
 */
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"


#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "commands/sequence.h"
#include "distributed/citus_procedures.h"
#include "distributed/colocation_utils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/procedure_metadata.h"
#include "distributed/reference_table_utils.h"
#include "distributed/worker_transaction.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"


static Index IndexOfArgumentName(HeapTuple procedureTuple, char *argumentName);
static uint32 ColocationIdForFunction(Oid functionId, Oid distributionArgumentType,
									  char *colocateWithTableName);
static void InsertDistributedProcedureRecord(DistributedProcedureRecord *record);
static void CreateFunctionOnAllNodes(Oid functionId);
static Oid CitusInternalNamespaceId(void);
static Oid CitusProceduresRelationId(void);
static Oid CitusProceduresFunctionIdIndexId(void);


PG_FUNCTION_INFO_V1(distribute_function);


Datum
distribute_function(PG_FUNCTION_ARGS)
{
	Oid functionId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	text *colocateWithTableNameText = PG_GETARG_TEXT_P(2);

	Index distributionArgIndex = -1;
	Oid distributionArgType = InvalidOid;
	char *colocateWithTableName = NULL;
	HeapTuple procedureTuple = NULL;
	Form_pg_proc procedureStruct;
	int colocationId = INVALID_COLOCATION_ID;
	DistributedProcedureRecord *procedureRecord = NULL;

	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
	if (!HeapTupleIsValid(procedureTuple))
	{
		elog(ERROR, "cache lookup failed for function %u", functionId);
	}

	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	if (!PG_ARGISNULL(1))
	{
		char *distributionColumnName = text_to_cstring(distributionColumnText);
		distributionArgIndex = IndexOfArgumentName(procedureTuple,
												   distributionColumnName);
		distributionArgType = procedureStruct->proargtypes.values[distributionArgIndex];
	}

	colocateWithTableName = text_to_cstring(colocateWithTableNameText);

	colocationId = ColocationIdForFunction(functionId, distributionArgType,
										   colocateWithTableName);

	procedureRecord =
		(DistributedProcedureRecord *) palloc0(sizeof(DistributedProcedureRecord));
	procedureRecord->functionId = functionId;
	procedureRecord->distributionArgumentIndex = distributionArgIndex;
	procedureRecord->colocationId = colocationId;

	InsertDistributedProcedureRecord(procedureRecord);
	CreateFunctionOnAllNodes(functionId);

	ReleaseSysCache(procedureTuple);

	PG_RETURN_BOOL(true);
}


/*
 * IndexOfArgumentName finds the index of a function argument with the given
 * name, or -1 if it cannot be found.
 */
static Index
IndexOfArgumentName(HeapTuple procedureTuple, char *argumentName)
{
	Datum proargnames = 0;
	Datum proargmodes = 0;
	int numberOfArguments = 0;
	bool isNull = false;
	char **argumentNames = NULL;
	Index argumentIndex = 0;

	proargnames = SysCacheGetAttr(PROCNAMEARGSNSP, procedureTuple,
								  Anum_pg_proc_proargnames, &isNull);
	if (isNull)
	{
		return -1;
	}

	proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, procedureTuple,
								  Anum_pg_proc_proargmodes, &isNull);
	if (isNull)
	{
		return -1;
	}

	numberOfArguments = get_func_input_arg_names(proargnames, proargmodes,
												 &argumentNames);
	for (argumentIndex = 0; argumentIndex < numberOfArguments; argumentIndex++)
	{
		char *currentArgumentName = argumentNames[argumentIndex];

		if (strncmp(currentArgumentName, argumentName, NAMEDATALEN) == 0)
		{
			return argumentIndex;
		}
	}

	return -1;
}


/*
 * ColocationIdForFunction
 */
static uint32
ColocationIdForFunction(Oid functionId, Oid distributionArgumentType,
						char *colocateWithTableName)
{
	uint32 colocationId = INVALID_COLOCATION_ID;

	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the colocation table
	 * until this transaction is committed.
	 */
	Relation pgDistColocation = heap_open(DistColocationRelationId(), ExclusiveLock);

	bool createdColocationGroup = false;

	if (distributionArgumentType == InvalidOid)
	{
		return CreateReferenceTableColocationId();
	}
	if (pg_strncasecmp(colocateWithTableName, "default", NAMEDATALEN) == 0)
	{
		/* check for default colocation group */
		colocationId = ColocationId(ShardCount, ShardReplicationFactor,
									distributionArgumentType);

		if (colocationId == INVALID_COLOCATION_ID)
		{
			colocationId = CreateColocationGroup(ShardCount, ShardReplicationFactor,
												 distributionArgumentType);
			createdColocationGroup = true;
		}
	}
	else
	{
		text *colocateWithTableNameText = cstring_to_text(colocateWithTableName);
		Oid sourceRelationId = ResolveRelationId(colocateWithTableNameText, false);
		Var *sourceDistributionColumn = DistPartitionKey(sourceRelationId);

		if (distributionArgumentType != sourceDistributionColumn->vartype)
		{
			char *functionName = get_func_name(functionId);
			char *relationName = get_rel_name(sourceRelationId);

			ereport(ERROR, (errmsg("cannot colocate function %s with table %s",
								   functionName, relationName),
							errdetail("Distribution column types don't match for "
									  "%s and %s.", functionName, relationName)));
		}

		colocationId = TableColocationId(sourceRelationId);
	}

	/*
	 * If we created a new colocation group then we need to keep the lock to
	 * prevent a concurrent create_distributed_table call from creating another
	 * colocation group with the same parameters. If we're using an existing
	 * colocation group then other transactions will use the same one.
	 */
	if (createdColocationGroup)
	{
		/* keep the exclusive lock */
		heap_close(pgDistColocation, NoLock);
	}
	else
	{
		/* release the exclusive lock */
		heap_close(pgDistColocation, ExclusiveLock);
	}

	return colocationId;
}


/*
 * InsertDistributedProcedureRecord inserts a record into citus.procedures recording
 * that the function should be distributed and
 */
static void
InsertDistributedProcedureRecord(DistributedProcedureRecord *record)
{
	Relation citusProcedures = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple procedureTuple = NULL;
	Form_pg_proc procedureStruct;
	HeapTuple distributedProcedureTuple = NULL;
	Datum values[Natts_citus_procedures];
	bool isNulls[Natts_citus_procedures];
	Oid schemaId = InvalidOid;
	Name procedureName = NULL;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(record->functionId));
	if (!HeapTupleIsValid(procedureTuple))
	{
		elog(ERROR, "cache lookup failed for function %u", record->functionId);
	}

	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);
	schemaId = procedureStruct->pronamespace;
	procedureName = &procedureStruct->proname;

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_citus_procedures_function_id - 1] = ObjectIdGetDatum(record->functionId);
	values[Anum_citus_procedures_schema_name - 1] = ObjectIdGetDatum(schemaId);
	values[Anum_citus_procedures_procedure_name - 1] = NameGetDatum(procedureName);
	values[Anum_citus_procedures_distribution_arg - 1] =
		Int32GetDatum(record->distributionArgumentIndex);
	values[Anum_citus_procedures_colocation_id - 1] = Int32GetDatum(record->colocationId);

	citusProcedures = heap_open(CitusProceduresRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(citusProcedures);
	distributedProcedureTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(citusProcedures, distributedProcedureTuple);

	CitusInvalidateRelcacheByRelid(CitusProceduresRelationId());

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();

	/* close relation */
	heap_close(citusProcedures, NoLock);

	ReleaseSysCache(procedureTuple);
}


/*
 * CreateFunctionOnAllNodes creates a given function on all nodes.
 */
static void
CreateFunctionOnAllNodes(Oid functionId)
{
	char *userName = NULL;
	char *command = CreateFunctionCommand(functionId);
	int parameterCount = 0;

	SendCommandToWorkersParams(WORKERS_WITH_METADATA, userName, command, parameterCount,
							   NULL, NULL);
}


/*
 * CreateFunctionCommand
 */
char *
CreateFunctionCommand(Oid functionId)
{
	Datum functionIdDatum = ObjectIdGetDatum(functionId);
	Datum commandDatum = DirectFunctionCall1(pg_get_functiondef, functionIdDatum);

	return TextDatumGetCString(commandDatum);
}


/*
 * CitusInternalNamespaceId returns the OID of the citus_internal namespace.
 *
 * TODO: caching
 */
static Oid
CitusInternalNamespaceId(void)
{
	bool missingOK = false;

	return get_namespace_oid("citus_internal", missingOK);
}


/*
 * CitusProceduresRelationId returns the OID of citus.procedures.
 *
 * TODO: caching
 */
static Oid
CitusProceduresRelationId(void)
{
	return get_relname_relid("procedures", CitusInternalNamespaceId());
}


/*
 * CitusProceduresFunctionIdIndexId returns the OID of citus.procedures.
 *
 * TODO: caching
 */
static Oid
CitusProceduresFunctionIdIndexId(void)
{
	return get_relname_relid("procedures_function_id_idx", CitusInternalNamespaceId());
}


/*
 * LoadDistributedProcedureRecord loads a record from citus.procedures.
 *
 * TODO: caching
 */
DistributedProcedureRecord *
LoadDistributedProcedureRecord(Oid functionId)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;
	Relation citusProcedures = NULL;
	DistributedProcedureRecord *record = NULL;
	TupleDesc tupleDescriptor = NULL;

	ScanKeyInit(&scanKey[0], Anum_citus_procedures_function_id,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(functionId));

	citusProcedures = heap_open(CitusProceduresRelationId(), AccessShareLock);
	tupleDescriptor = RelationGetDescr(citusProcedures);

	scanDescriptor = systable_beginscan(citusProcedures,
										CitusProceduresFunctionIdIndexId(),
										true, NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for function %d ",
							   functionId)));
	}

	if (HeapTupleIsValid(heapTuple))
	{
		Datum datumArray[Natts_citus_procedures];
		bool isNullArray[Natts_citus_procedures];

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

		record =
			(DistributedProcedureRecord *) palloc0(sizeof(DistributedProcedureRecord));
		record->functionId =
			DatumGetUInt32(datumArray[Anum_citus_procedures_function_id - 1]);
		record->distributionArgumentIndex =
			DatumGetUInt32(datumArray[Anum_citus_procedures_distribution_arg - 1]);
		record->colocationId =
			DatumGetInt32(datumArray[Anum_citus_procedures_colocation_id - 1]);
	}

	systable_endscan(scanDescriptor);
	heap_close(citusProcedures, NoLock);

	return record;
}
