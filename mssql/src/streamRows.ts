import * as common from "@data-heaving/common";
import * as sql from "@data-heaving/common-sql";
import * as validation from "@data-heaving/common-validation";
import * as api from "@data-heaving/source-sql";
import * as types from "./types";
import * as read from "./read";
import { isDeepStrictEqual } from "util";
import * as t from "io-ts";

type TableReadConfig = Omit<types.MainConfig, "connection">;

export interface TableInput {
  tableID: types.TableID;
}
export interface TableReadContext<TInput extends TableInput = TableInput> {
  tableID: types.TableID;
  tableProcessingStartTime: Date;
  input: TInput;
}

// export interface TableReadContextWithMDFromQuery {
//   tableMD: read.MSSQLRecordSet;
// }

export type TableAndMetadata = TableInput & {
  tableMD: sql.TableMetaData;
};

export type TableChangeTrackingReadContext<
  TInput extends TableAndMetadata = TableAndMetadata
> = TableReadContext<TInput> &
  TableAndMetadata & {
    input: TInput;
    allColumns: ReadonlyArray<string>;
  };

// type TableReadArg = sql.DatumProcessorFactoryArg<types.TableID>;

export function tableMetaDataInServer(
  connectionPool: types.MSSQLConnectionPool,
  eventEmitter: sql.SQLEventEmitter,
): common.TPipelineFactory<
  TableReadConfig,
  ReadonlyArray<TableAndMetadata>,
  TableAndMetadata
> {
  return (datumStoringFactory) => {
    return async (config) => {
      const storing = datumStoringFactory();
      await sql.useConnectionPoolAsync(
        connectionPool,
        eventEmitter,
        async (connection) => {
          // The query for table information returns 1 row per column, however our datum is single table info
          // So just buffer everything to memory (we assume server's won't hold gigabytes worth of *metadata*), and 'stream' later
          const allTables = common.deduplicate(
            [
              ...(await getExplicitTables(config, connection)),
              ...(await getTablesOfSchemas(config, connection)),
            ],
            ({ tableID }) => types.getFullTableName(tableID),
          );
          await common.runPipelineWithBufferedData(
            allTables,
            storing,
            allTables,
            1, // TODO for now, hard-coded concurrency of level 1
          );
        },
      );
    };
  };
}

export function rowsInTable(
  connectionPool: types.MSSQLConnectionPool,
): RowsInTableBuilder {
  return {
    fullLoad: (eventEmitter, getBehaviourInfo) => {
      // table = validation.decodeOrThrow(types.tableID.decode, table); // Verify that db/schema/table names don't contain forbidden characters etc.
      return api.createRowIteratingPipelineFactory(
        (tableInput) => ({
          input: tableInput,
          tableID: tableInput.tableID,
          tableProcessingStartTime: new Date(),
        }),
        connectionPool,
        eventEmitter,
        async (context, connection, getCurrentStoring, endOrReset) => {
          validation.decodeOrThrow(types.tableID.decode, context.tableID); // Verify that db/schema/table names don't contain forbidden characters etc.
          const eventArgBase = createEventArgBase(context);
          const { intermediateRowEventInterval } =
            typeof getBehaviourInfo === "object"
              ? getBehaviourInfo
              : getBehaviourInfo(context);
          eventEmitter.emit("tableExportStart", eventArgBase);
          let error: unknown = undefined;
          let sqlRowsProcessedTotal = 0;
          try {
            return await sql.streamQuery({
              connection,
              sqlCommand: `SELECT * FROM ${types.getFullTableName(
                context.tableID,
              )}`,
              onRow: (row, controlFlow) => {
                sqlRowsProcessedTotal = api.processRowForEventEmitter(
                  eventEmitter,
                  eventArgBase,
                  intermediateRowEventInterval,
                  sqlRowsProcessedTotal,
                );
                getCurrentStoring().processor(row, controlFlow);
              },
              onDone: endOrReset,
            });
          } catch (e) {
            error = e;
            throw e;
          } finally {
            eventEmitter.emit("tableExportEnd", {
              ...eventArgBase,
              sqlRowsProcessedTotal,
              durationInMs:
                new Date().valueOf() -
                context.tableProcessingStartTime.valueOf(),
              errors: error ? [error] : [],
            });
          }
        },
        () => Promise.resolve(undefined),
      );
    },
    incrementalLoadWithChangeTrackingColumn: (
      eventEmitter,
      getBehaviourInfo,
    ) => {
      return api.createRowIteratingPipelineFactory(
        (tableInput) => ({
          input: tableInput,
          tableID: tableInput.tableID,
          tableProcessingStartTime: new Date(),
        }),
        connectionPool,
        eventEmitter,
        async (context, connection, getCurrentStoring, endOrReset) => {
          validation.decodeOrThrow(types.tableID.decode, context.tableID); // Verify that db/schema/table names don't contain forbidden characters etc.
          const eventArgBase = createEventArgBase(context);
          const {
            changedAtColumnName,
            changeTrackingStorage,
            changeTrackingValidation,
            intermediateRowEventInterval,
            prepareChangeTrackingForSerialization,
          } = getBehaviourInfo(context);
          validation.decodeOrThrow(
            types.identifier.decode,
            changedAtColumnName,
          ); // Verify that column name doesn't contain forbidden characters etc.
          const ctInfo = await api.prepareChangeTracking<
            t.TypeOf<typeof changeTrackingValidation>
          >({
            validation: changeTrackingValidation,
            storage: changeTrackingStorage,
          });
          const eventArg = {
            ...eventArgBase,
            changeTrackingVersion: undefined,
            previousChangeTrackingVersion: ctInfo.previousChangeTrackingVersion,
          };
          eventEmitter.emit("tableChangeTrackVersionSeen", eventArg);

          let maxChangedAt: unknown | undefined = undefined;
          eventEmitter.emit("tableExportStart", eventArgBase);
          let error: unknown = undefined;
          let sqlRowsProcessedTotal = 0;
          try {
            await read.streamQueryWithRowMD({
              connection,
              sqlCommand: `SELECT * FROM ${types.getFullTableName(
                context.tableID,
              )}${
                ctInfo?.previousChangeTrackingVersion !== undefined
                  ? ` WHERE [${changedAtColumnName}] > '${ctInfo.previousChangeTrackingVersion}'`
                  : ""
              }`,
              onRowMD: (tableMD) =>
                tableMD.findIndex((md) => md.name === changedAtColumnName),
              onRow: (row, controlFlow, changedAtColumnIndex: number) => {
                const currentChangedAt = row[changedAtColumnIndex];
                if (
                  !maxChangedAt ||
                  (currentChangedAt as any) > (maxChangedAt as any) // eslint-disable-line @typescript-eslint/no-explicit-any
                ) {
                  maxChangedAt = currentChangedAt;
                }
                sqlRowsProcessedTotal = api.processRowForEventEmitter(
                  eventEmitter,
                  eventArgBase,
                  intermediateRowEventInterval,
                  sqlRowsProcessedTotal,
                );
                getCurrentStoring().processor(row, controlFlow);
              },
              onDone: endOrReset,
            });
          } catch (e) {
            error = e;
            throw e;
          } finally {
            eventEmitter.emit("tableExportEnd", {
              ...eventArgBase,
              sqlRowsProcessedTotal,
              durationInMs:
                new Date().valueOf() -
                context.tableProcessingStartTime.valueOf(),
              errors: error ? [error] : [],
            });
          }

          return {
            eventArg,
            ctInfo,
            maxChangedAt,
            prepareChangeTrackingForSerialization,
          };
        },
        async (
          _,
          {
            eventArg,
            ctInfo,
            maxChangedAt,
            prepareChangeTrackingForSerialization,
          },
        ) => {
          // After successful run, and after connection has been closed, remember to upload change tracking information
          if (prepareChangeTrackingForSerialization) {
            maxChangedAt = prepareChangeTrackingForSerialization(maxChangedAt); // e.g. create ISO formatted timestamp from Date object.
          }
          if (
            ctInfo.changeTrackingFunctionality.validation.is(maxChangedAt) &&
            !isDeepStrictEqual(
              maxChangedAt,
              ctInfo?.previousChangeTrackingVersion,
            )
          ) {
            await ctInfo.ctStorage.writeNewDataWhenDifferent(maxChangedAt);
            eventEmitter.emit("changeTrackingVersionUploaded", {
              ...eventArg,
              changeTrackingVersion: maxChangedAt,
            });
          }
        },
      );
    },
    incrementalLoadWithSQLServerChangeTracking: (
      eventEmitter,
      getBehaviourInfo,
      processedAtColumnName = DEFAULT_PROCESSED_AT_COLUMN_NAME,
      changedAtColumnName = DEFAULT_CHANGED_AT_COLUMN_NAME,
      deletedAtColumnName = DEFAULT_DELETED_AT_COLUMN_NAME,
    ) => {
      return api.createRowIteratingPipelineFactory(
        (tableInput) => ({
          input: tableInput,
          tableID: tableInput.tableID,
          tableMD: tableInput.tableMD,
          tableProcessingStartTime: new Date(),
          allColumns: tableInput.tableMD.columnNames.concat([
            processedAtColumnName,
            changedAtColumnName,
            deletedAtColumnName,
          ]),
        }),
        connectionPool,
        eventEmitter,
        async (context, connection, getCurrentStoring, endOrReset) => {
          const { tableID, tableMD } = context;
          validation.decodeOrThrow(types.tableID.decode, tableID); // Verify that db/schema/table names don't contain forbidden characters etc.
          const eventArgBase = createEventArgBaseForChangeTracking(context);

          const {
            changeTrackingStorage,
            dontAutoEnableChangeTracking,
            intermediateRowEventInterval,
          } = getBehaviourInfo(context);

          const ctInfo = await api.prepareChangeTracking({
            validation: validation.nonEmptyString,
            storage: changeTrackingStorage,
          });

          const changeTrackingVersion = await read.checkChangeTrackingValidity({
            connection,
            previousChangeTracking: ctInfo.previousChangeTrackingVersion,
            tableID,
            isCTAlreadyEnabled: tableMD.isCTEnabled,
            dontAutoEnableChangeTracking,
          });

          const eventArg = {
            ...eventArgBase,
            changeTrackingVersion,
            previousChangeTrackingVersion: ctInfo.previousChangeTrackingVersion,
          };
          eventEmitter?.emit("tableChangeTrackVersionSeen", eventArg);
          // const { changeTrackingVersion } = ctInfo;
          const { columnNames } = tableMD;
          const outputArray = Array<unknown>(columnNames.length + 3); // Extra 3 cols for last modify + deletion times + this time
          const [thisTimeIndex, lastModifiedIndex, deletedIndex] = [
            columnNames.length,
            columnNames.length + 1,
            columnNames.length + 2,
          ]; // in outputArray
          const thisOperationStartTime = context.tableProcessingStartTime;
          // const thisOperationStartTime = common.dateToISOUTCString(
          //   thisOperationStartTimeObject,
          // );
          let sqlRowsProcessedTotal = 0;
          eventEmitter?.emit("tableExportStart", {
            ...eventArgBase,
          });
          let error: unknown = undefined;
          try {
            const seenCTVersion = await (changeTrackingVersion
              ? read.readTableWithChangeTracking
              : read.readFullTable)({
              connection,
              tableID,
              tableMD,
              changeTracking: changeTrackingVersion,
              outputArray,
              additionalInfo: {},
              rowProcessor: (rowStatus, transactionTime, controlFlow) => {
                if (rowStatus !== "invalid") {
                  // At this point, all the current row data has been set to outputArray. We need to just set additional information
                  outputArray[thisTimeIndex] = thisOperationStartTime;
                  const thisLastModified =
                    transactionTime ?? thisOperationStartTime;
                  outputArray[lastModifiedIndex] = thisLastModified;
                  outputArray[deletedIndex] =
                    rowStatus === "deleted" ? thisLastModified : null; // Set deletion time if needed
                  sqlRowsProcessedTotal = api.processRowForEventEmitter(
                    eventEmitter,
                    eventArgBase,
                    intermediateRowEventInterval,
                    sqlRowsProcessedTotal,
                  );
                } else {
                  eventEmitter?.emit("invalidRowSeen", {
                    ...eventArg,
                    currentSqlRowIndex: sqlRowsProcessedTotal,
                    row: [...outputArray],
                  }); // Create copy of our array so event handlers don't even accidentally modify it.
                }
                getCurrentStoring().processor(
                  rowStatus === "invalid" ? undefined : outputArray,
                  controlFlow,
                );
              },
              onQueryEnd: endOrReset,
            });
            return {
              ctInfo,
              seenCTVersion,
              eventArg,
            };
          } catch (e) {
            error = e;
            throw e;
          } finally {
            eventEmitter?.emit("tableExportEnd", {
              ...eventArgBase,
              sqlRowsProcessedTotal,
              durationInMs:
                new Date().valueOf() - thisOperationStartTime.valueOf(),
              errors: error ? [error] : [],
            });
          }
        },
        async (_, { ctInfo, seenCTVersion, eventArg }) => {
          // After successful run, and after connection has been closed, remember to upload change tracking information
          if (
            ctInfo.changeTrackingFunctionality.validation.is(seenCTVersion) &&
            !isDeepStrictEqual(
              seenCTVersion,
              ctInfo?.previousChangeTrackingVersion,
            )
          ) {
            await ctInfo.ctStorage.writeNewDataWhenDifferent(seenCTVersion);
            eventEmitter?.emit("changeTrackingVersionUploaded", {
              ...eventArg,
              changeTrackingVersion: seenCTVersion,
            });
          }
        },
      );
    },
  };
}

export interface RowsInTableBuilder {
  fullLoad: <TInput extends TableInput = TableInput>(
    eventEmitter: api.TableExportEventEmitter<
      TableReadContext<TInput>,
      types.TableID
    >,
    getBehaviourInfo: common.ItemOrFactory<
      { intermediateRowEventInterval: number },
      [TableReadContext<TInput>]
    >,
  ) => common.TPipelineFactory<TInput, TableReadContext<TInput>, sql.TSQLRow>;
  incrementalLoadWithChangeTrackingColumn: <
    TValidation extends t.Mixed,
    TInput extends TableInput = TableInput
  >(
    eventEmitter: api.TableExportWithChangeTrackingEventEmitter<
      TableReadContext<TInput>,
      types.TableID,
      t.TypeOf<TValidation>
    >,
    getBehaviourInfo: (
      context: TableReadContext<TInput>,
    ) => {
      changedAtColumnName: string;
      changeTrackingStorage: common.ObjectStorageFunctionality<
        t.TypeOf<TValidation>
      >;
      changeTrackingValidation: TValidation;
      prepareChangeTrackingForSerialization:
        | ((changeTracking: unknown) => t.TypeOf<TValidation>)
        | undefined;
      intermediateRowEventInterval: number;
    },
  ) => common.TPipelineFactory<TInput, TableReadContext<TInput>, sql.TSQLRow>;
  incrementalLoadWithSQLServerChangeTracking: <
    TInput extends TableAndMetadata = TableAndMetadata
  >(
    eventEmitter: api.TableExportWithChangeTrackingEventEmitter<
      TableChangeTrackingReadContext<TInput>,
      types.TableID,
      string
    >,
    getBehaviourInfo: (
      context: TableChangeTrackingReadContext<TInput>,
    ) => {
      changeTrackingStorage: common.ObjectStorageFunctionality<string>;
      dontAutoEnableChangeTracking: boolean;
      intermediateRowEventInterval: number;
    },
    processedAtColumnName?: string,
    changedAtColumnName?: string,
    deletedAtColumnName?: string,
  ) => common.TPipelineFactory<
    TInput,
    TableChangeTrackingReadContext<TInput>,
    sql.TSQLRow
  >;
}

export const DEFAULT_PROCESSED_AT_COLUMN_NAME = "__PROCESSED_AT";
export const DEFAULT_CHANGED_AT_COLUMN_NAME = "__CHANGED_AT";
export const DEFAULT_DELETED_AT_COLUMN_NAME = "__DELETED_AT";

export const getExplicitTables = async (
  { defaults, data: { tables } }: TableReadConfig,
  connection: types.MSSQLConnection,
) => {
  const explicitTables =
    tables?.tables
      .map((tableName) =>
        typeof tableName === "string"
          ? { schemaName: tables.schemaName, tableName }
          : tableName,
      )
      .map(({ schemaName, tableName, overrideDefaults }) => {
        const schemaNameFinal = schemaName ?? tables?.schemaName;
        if (!schemaNameFinal) {
          throw new Error(
            `No schema specified for table ${tableName}, and no default schema specified either.`,
          );
        }
        return {
          databaseName: tryGetDatabaseName(
            overrideDefaults,
            defaults,
            () => `table [${schemaNameFinal}].[${tableName}]`,
          ),
          schemaName: schemaNameFinal,
          tableName,
          overrideDefaults,
        };
      }) || [];
  return (await read.getTableColumnMetaData(connection, explicitTables)).map(
    ({ tableMD, tableID }) => ({
      tableMD,
      tableID,
      // intermediateRowEventInterval:
      //   common.getOrDefault(
      //     explicitTables[originalIndex]?.overrideDefaults
      //       ?.intermediateRowEventInterval,
      //     defaults?.intermediateRowEventInterval,
      //   ) || 0,
      // additionalInfo: explicitTables[originalIndex]?.overrideDefaults || {},
    }),
  );
};

export const getTablesOfSchemas = async (
  { defaults, data: { schemas } }: TableReadConfig,
  connection: types.MSSQLConnection,
) => {
  const explicitSchemas =
    schemas
      ?.map((s) => (typeof s === "string" ? { schemaName: s } : s))
      ?.map(({ schemaName, overrideDefaults }) => ({
        databaseName: tryGetDatabaseName(
          overrideDefaults,
          defaults,
          () => `schema [${schemaName}]`,
        ),
        schemaName,
        overrideDefaults,
      })) || [];
  return (await read.getTableColumnMetaData(connection, explicitSchemas)).map(
    ({ tableMD, tableID }) => ({
      tableMD,
      tableID,
      // intermediateRowEventInterval:
      //   common.getOrDefault(
      //     explicitSchemas[originalIndex]?.overrideDefaults
      //       ?.intermediateRowEventInterval,
      //     defaults?.intermediateRowEventInterval,
      //   ) || 0,
      // additionalInfo: explicitSchemas[originalIndex]?.overrideDefaults || {},
    }),
  );
};

const tryGetDatabaseName = (
  overrideDefaults: TableReadConfig["defaults"],
  globalDefaults: TableReadConfig["defaults"],
  descriptorString: () => string,
) => {
  const databaseName =
    overrideDefaults?.databaseName ?? globalDefaults?.databaseName;
  if (!databaseName) {
    throw new Error(
      `No DB specified for ${descriptorString()}, and no default DB specified either.`,
    );
  }
  return databaseName;
};

const createEventArgBase = <TInput extends TableInput>(
  context: TableReadContext<TInput>,
) => {
  const tableID = common.deepCopy(context.tableID);
  return {
    context: {
      tableID,
      input: context.input, // Let's not copy input as that messes up things like Date
      tableProcessingStartTime: context.tableProcessingStartTime,
    },
    tableID,
  };
};
const createEventArgBaseForChangeTracking = <TInput extends TableAndMetadata>(
  context: TableChangeTrackingReadContext<TInput>,
) => {
  const tableID = common.deepCopy(context.tableID);
  return {
    context: {
      tableID,
      input: context.input, // Let's not copy input as that messes up things like Date
      tableMD: common.deepCopy(context.tableMD),
      allColumns: [...context.allColumns],
      tableProcessingStartTime: context.tableProcessingStartTime,
    },
    tableID,
  };
};
