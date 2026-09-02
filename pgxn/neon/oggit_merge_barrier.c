#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "oggit_merge_barrier.h"

static THR_LOCAL bool oggit_apply_merge = false;
static THR_LOCAL ProcessUtility_hook_type previous_process_utility_hook = NULL;
static THR_LOCAL ExecutorCheckPerms_hook_type previous_executor_check_perms_hook = NULL;

static bool
oggit_merge_barrier_internal_schema_name(const char *schema_name)
{
	return schema_name != NULL &&
		(strcmp(schema_name, "oggit") == 0 ||
		 strcmp(schema_name, "_oggit") == 0 ||
		 strcmp(schema_name, "neon") == 0 ||
		 strncmp(schema_name, "oggit_", strlen("oggit_")) == 0 ||
		 strncmp(schema_name, "neon_", strlen("neon_")) == 0 ||
		 strncmp(schema_name, "fdw_oggit_", strlen("fdw_oggit_")) == 0);
}

static bool
oggit_merge_barrier_internal_schema(Oid namespace_oid)
{
	char *schema_name;
	bool internal;

	if (!OidIsValid(namespace_oid))
		return false;

	schema_name = get_namespace_name(namespace_oid);
	if (schema_name == NULL)
		return false;
	internal = oggit_merge_barrier_internal_schema_name(schema_name);
	pfree(schema_name);
	return internal;
}

static bool
oggit_merge_barrier_active(void)
{
	Oid namespace_oid;
	Oid relation_oid;
	Relation relation;
	TableScanDesc scan;
	HeapTuple tuple;
	bool active = false;

	if (oggit_apply_merge || !IsTransactionState())
		return false;

	namespace_oid = get_namespace_oid("oggit", true);
	if (!OidIsValid(namespace_oid))
		return false;
	relation_oid = get_relname_relid("merge_write_barrier", namespace_oid);
	if (!OidIsValid(relation_oid))
		return false;

	/* Executor hooks cannot safely recurse through SPI on openGauss. */
	relation = heap_open(relation_oid, AccessShareLock);
	scan = heap_beginscan(relation, SnapshotNow, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool is_null;
		Datum value = heap_getattr(tuple, 2, RelationGetDescr(relation), &is_null);

		if (!is_null && DatumGetBool(value))
		{
			active = true;
			break;
		}
	}
	heap_endscan(scan);
	heap_close(relation, AccessShareLock);

	return active;
}

static void
oggit_merge_barrier_reject_write(const char *kind)
{
	if (!oggit_merge_barrier_active())
		return;

	ereport(ERROR,
			(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
			 errmsg("oggit merge is blocked; this branch writes are temporarily disabled"),
			 errdetail("%s is rejected until the active merge is continued or aborted.", kind)));
}

void
oggit_merge_barrier_check_executor(QueryDesc *query_desc)
{
	EState *estate;
	int i;

	if (oggit_apply_merge || query_desc == NULL)
		return;
	if (query_desc->operation != CMD_INSERT &&
		query_desc->operation != CMD_UPDATE &&
		query_desc->operation != CMD_DELETE &&
		query_desc->operation != CMD_MERGE &&
		query_desc->operation != CMD_TRUNCATE)
		return;

	estate = query_desc->estate;
	if (estate != NULL && estate->es_result_relations != NULL &&
		estate->es_num_result_relations > 0)
	{
		bool all_internal = true;

		for (i = 0; i < estate->es_num_result_relations; i++)
		{
			Relation relation = estate->es_result_relations[i].ri_RelationDesc;
			Oid namespace_oid = relation != NULL ? RelationGetNamespace(relation) : InvalidOid;
			bool internal = relation != NULL &&
				oggit_merge_barrier_internal_schema(namespace_oid);

			if (!internal)
			{
				all_internal = false;
				break;
			}
		}
		if (all_internal)
			return;
	}

	oggit_merge_barrier_reject_write("DML");
}

static bool
oggit_merge_barrier_check_permissions(List *range_table, bool abort_on_violation)
{
	ListCell *cell;
	bool allowed = true;

	if (previous_executor_check_perms_hook != NULL)
		allowed = previous_executor_check_perms_hook(range_table, abort_on_violation);
	if (!allowed || oggit_apply_merge)
		return allowed;

	foreach(cell, range_table)
	{
		RangeTblEntry *entry = (RangeTblEntry *) lfirst(cell);
		AclMode write_permissions = ACL_INSERT | ACL_UPDATE | ACL_DELETE;

		if (entry->rtekind != RTE_RELATION ||
			(entry->requiredPerms & write_permissions) == 0)
			continue;
		if (!oggit_merge_barrier_internal_schema(get_rel_namespace(entry->relid)))
		{
			oggit_merge_barrier_reject_write("DML");
			break;
		}
	}

	return allowed;
}

static bool
oggit_merge_barrier_internal_range_var(const RangeVar *relation)
{
	return relation != NULL &&
		oggit_merge_barrier_internal_schema_name(relation->schemaname);
}

static bool
oggit_merge_barrier_internal_utility(Node *parse_tree)
{
	ListCell *cell;

	if (IsA(parse_tree, CreateSchemaStmt))
		return oggit_merge_barrier_internal_schema_name(
			((CreateSchemaStmt *) parse_tree)->schemaname);
	if (IsA(parse_tree, CreateStmt))
		return oggit_merge_barrier_internal_range_var(
			((CreateStmt *) parse_tree)->relation);
	if (IsA(parse_tree, AlterTableStmt))
		return oggit_merge_barrier_internal_range_var(
			((AlterTableStmt *) parse_tree)->relation);
	if (IsA(parse_tree, IndexStmt))
		return oggit_merge_barrier_internal_range_var(
			((IndexStmt *) parse_tree)->relation);
	if (IsA(parse_tree, TruncateStmt))
	{
		foreach(cell, ((TruncateStmt *) parse_tree)->relations)
		{
			if (!oggit_merge_barrier_internal_range_var((RangeVar *) lfirst(cell)))
				return false;
		}
		return true;
	}
	if (IsA(parse_tree, DropStmt))
	{
		DropStmt *statement = (DropStmt *) parse_tree;

		foreach(cell, statement->objects)
		{
			List *names = (List *) lfirst(cell);
			Value *schema;

			if (statement->removeType == OBJECT_SCHEMA)
				schema = (Value *) linitial(names);
			else if (list_length(names) >= 2)
				schema = (Value *) linitial(names);
			else
				return false;
			if (!oggit_merge_barrier_internal_schema_name(strVal(schema)))
				return false;
		}
		return true;
	}
	return false;
}

static void
oggit_merge_barrier_process_utility(processutility_context *processutility_cxt,
								 DestReceiver *dest,
#ifdef PGXC
								 bool sent_to_remote,
#endif
								 char *completion_tag,
								 ProcessUtilityContext context,
								 bool is_ctas)
{
	Node *parse_tree = processutility_cxt != NULL ? processutility_cxt->parse_tree : NULL;

	if (!oggit_apply_merge && parse_tree != NULL &&
		!IsA(parse_tree, VariableSetStmt) &&
		!IsA(parse_tree, VariableShowStmt) &&
		!IsA(parse_tree, TransactionStmt) &&
		!oggit_merge_barrier_internal_utility(parse_tree) &&
		!CommandIsReadOnly(parse_tree))
		oggit_merge_barrier_reject_write("DDL or write utility command");

	if (previous_process_utility_hook != NULL)
		previous_process_utility_hook(processutility_cxt, dest,
#ifdef PGXC
								  sent_to_remote,
#endif
								  completion_tag, context, is_ctas);
	else
		standard_ProcessUtility(processutility_cxt, dest,
#ifdef PGXC
							sent_to_remote,
#endif
							completion_tag, context, is_ctas);
}

void
pg_init_oggit_merge_barrier(void)
{
	DefineCustomBoolVariable("neon.oggit_apply_merge",
							 "Allow the current transaction to apply an oggit merge",
							 NULL,
							 &oggit_apply_merge,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	if (ProcessUtility_hook != oggit_merge_barrier_process_utility)
	{
		previous_process_utility_hook = ProcessUtility_hook;
		ProcessUtility_hook = oggit_merge_barrier_process_utility;
	}
	if (ExecutorCheckPerms_hook != oggit_merge_barrier_check_permissions)
	{
		previous_executor_check_perms_hook = ExecutorCheckPerms_hook;
		ExecutorCheckPerms_hook = oggit_merge_barrier_check_permissions;
	}
}
