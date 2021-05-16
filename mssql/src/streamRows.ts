import * as common from "@data-heaving/common";
import * as sql from "@data-heaving/common-sql";
import * as validation from "@data-heaving/common-validation";
import * as api from "@data-heaving/source-sql";
import * as types from "./types";
import * as read from "./read";
import { isDeepStrictEqual } from "util";

type TableReadConfig = Omit<types.MainConfig, "connection">;
export interface TableReadContext {
  tableID: types.TableID;
  tableProcessingStartTime: Date;
}

export interface MetadataReadContext {
  tableID: types.TableID;
  tableMD: sql.TableMetaData;
}

export type TableChangeTrackingReadContext = MetadataReadContext &
  TableReadContext & {
    allColumns: ReadonlyArray<string>;
  };

// type TableReadArg = sql.DatumProcessorFactoryArg<types.TableID>;

export function tableMetaDataInServer(
  connectionPool: types.MSSQLConnectionPool,
  eventEmitter: sql.SQLEventEmitter,
): common.TPipelineFactory<
  TableReadConfig,
  ReadonlyArray<MetadataReadContext>,
  MetadataReadContext
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
    fullLoad: (eventEmitter) => {
      // table = validation.decodeOrThrow(types.tableID.decode, table); // Verify that db/schema/table names don't contain forbidden characters etc.
      return api.createRowIteratingPipelineFactory(
        (tableID) => ({
          tableID,
          tableProcessingStartTime: new Date(),
        }),
        connectionPool,
        eventEmitter,
        async (tableInfo, connection, getCurrentStoring, endOrReset) => {
          validation.decodeOrThrow(types.tableID.decode, tableInfo.tableID); // Verify that db/schema/table names don't contain forbidden characters etc.
          return await sql.streamQuery({
            connection,
            sqlCommand: `SELECT * FROM ${types.getFullTableName(
              tableInfo.tableID,
            )}`,
            onRow: (row, controlFlow) => {
              getCurrentStoring().processor(row, controlFlow);
            },
            onDone: endOrReset,
          });
        },
        () => Promise.resolve(undefined),
      );
    },
    incrementalLoadWithSQLServerChangeTracking: (
      eventEmitter,
      getBehaviourInfo,
    ) => {
      return api.createRowIteratingPipelineFactory(
        (input) => ({
          ...input,
          tableProcessingStartTime: new Date(),
          allColumns: input.tableMD.columnNames.concat([
            "__PROCESSED_AT",
            "__CHANGED_AT",
            "__DELETED_AT",
          ]),
        }),
        connectionPool,
        eventEmitter,
        async (context, connection, getCurrentStoring, endOrReset) => {
          const { tableID, tableMD } = context;
          validation.decodeOrThrow(types.tableID.decode, tableID); // Verify that db/schema/table names don't contain forbidden characters etc.
          const eventArgBase = {
            tableID: common.deepCopy(tableID),
            tableMD: common.deepCopy(tableMD),
          };

          const {
            changeTrackingStorage,
            dontAutoEnableChangeTracking,
            intermediateRowEventInterval,
          } = getBehaviourInfo(context);

          const ctInfo = await api.prepareChangeTracking(
            {
              validation: validation.nonEmptyString,
              storage: changeTrackingStorage,
              checkValidity: async (opts) =>
                read.checkChangeTrackingValidity({
                  ...opts,
                  tableID,
                  isCTAlreadyEnabled: tableMD.isCTEnabled,
                  dontAutoEnableChangeTracking,
                }),
            },
            connection,
          );

          const eventArg = {
            ...eventArgBase,
            changeTrackingVersion: ctInfo?.changeTrackingVersion,
            previousChangeTrackingVersion:
              ctInfo?.previousChangeTrackingVersion,
          };
          eventEmitter?.emit("tableChangeTrackVersionSeen", eventArg);
          const { changeTrackingVersion } = ctInfo;
          const { columnNames } = tableMD;
          const outputArray = Array<unknown>(columnNames.length + 3); // Extra 3 cols for last modify + deletion times + this time
          const [thisTimeIndex, lastModifiedIndex, deletedIndex] = [
            columnNames.length,
            columnNames.length + 1,
            columnNames.length + 2,
          ]; // in outputArray
          const thisOperationStartTimeObject = new Date();
          const thisOperationStartTime = common.dateToISOUTCString(
            thisOperationStartTimeObject,
          );
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
                  ++sqlRowsProcessedTotal;

                  if (
                    intermediateRowEventInterval > 0 &&
                    sqlRowsProcessedTotal % intermediateRowEventInterval === 0
                  ) {
                    eventEmitter?.emit("tableExportProgress", {
                      ...eventArg,
                      currentSqlRowIndex: sqlRowsProcessedTotal,
                    });
                  }
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
              ...eventArg,
              sqlRowsProcessedTotal,
              durationInMs:
                new Date().valueOf() - thisOperationStartTimeObject.valueOf(),
              errors: error ? [error] : [],
            });
          }
        },
        async (_, { ctInfo, seenCTVersion, eventArg }) => {
          // After successful run, and after connection has been closed, remember to upload change tracking information
          if (
            ctInfo &&
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
  fullLoad: (
    eventEmitter: sql.SQLEventEmitter,
  ) => common.TPipelineFactory<types.TableID, TableReadContext, sql.TSQLRow>;
  incrementalLoadWithSQLServerChangeTracking: (
    eventEmitter: api.SourceTableEventEmitter<types.TableID, string>,
    getBehaviourInfo: (
      context: TableChangeTrackingReadContext,
    ) => {
      changeTrackingStorage: common.ObjectStorageFunctionality<string>;
      dontAutoEnableChangeTracking: boolean;
      intermediateRowEventInterval: number;
    },
  ) => common.TPipelineFactory<
    MetadataReadContext,
    TableChangeTrackingReadContext,
    sql.TSQLRow
  >;
}

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
