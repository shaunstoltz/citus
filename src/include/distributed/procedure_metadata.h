/*
 * procedure_metadata.h
 *   Types and functions to load metadata about distributed stored procedures.
 */
#ifndef PROCEDURE_METADATA_H
#define PROCEDURE_METADATA_H


typedef struct DistributedProcedureRecord
{
	Oid functionId;
	int distributionArgumentIndex;
	int colocationId;
} DistributedProcedureRecord;


extern DistributedProcedureRecord * LoadDistributedProcedureRecord(Oid functionId);
extern char * CreateFunctionCommand(Oid functionId);


#endif /* PROCEDURE_METADATA_H */
