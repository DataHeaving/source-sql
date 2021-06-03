import * as common from "@data-heaving/common";
import * as sql from "@data-heaving/common-sql";
import * as sqlSource from "@data-heaving/source-sql";
import * as t from "io-ts";
import test, { ExecutionContext, Implementation, SerialInterface } from "ava";
import * as mssql from "mssql";
import * as mssqlSource from "..";

export interface SQLServerInfo {
  host: string;
  port: number;
  username: string;
  password: string;
}
export const CONTEXT_KEY = "sqlServerInfo" as const;
export interface SQLServerTestContext {
  [CONTEXT_KEY]: SQLServerInfo;
}

export const TABLE_ID: mssqlSource.TableID = {
  databaseName: "dh_test_db",
  schemaName: "dbo",
  tableName: "TEST_DATA_TABLE",
};
export const TABLE_MD: sql.TableMetaData = {
  columnNames: ["ID", "DATA"],
  columnTypes: [
    {
      typeName: "INT",
      isNullable: false,
      maxLength: 0,
      precision: 0,
      scale: 0,
    },
    {
      typeName: "NVARCHAR",
      isNullable: true,
      maxLength: 0,
      precision: 0,
      scale: 0,
    },
  ],
  isCTEnabled: false,
  primaryKeyColumnCount: 1,
};
export const TABLE_DATA = [[0, "data0"] as const, [1, "data1"] as const];

export const defineSQLServerTest = (
  name: string,
  implementation: Implementation<SQLServerTestContext>,
) =>
  (test.serial as SerialInterface<SQLServerTestContext>)(name, implementation);

export function getSQLConfigFromContext(
  context: SQLServerTestContext | SQLServerInfo,
): mssql.config {
  const serverInfo =
    CONTEXT_KEY in context
      ? (context as SQLServerTestContext)[CONTEXT_KEY]
      : (context as SQLServerInfo);
  return {
    server: serverInfo.host,
    port: serverInfo.port,
    user: serverInfo.username,
    password: serverInfo.password,
    options: {
      encrypt: true,
      enableArithAbort: true,
    },
  };
}

export const getMSSQLPoolFromTestContext = (
  t: ExecutionContext<SQLServerTestContext>,
) => mssqlSource.getMSSQLPool(getSQLConfigFromContext(t.context));

export const performSQLFullLoadTest = () =>
  performSQLFullLoadTestWithCustomData(TABLE_ID, TABLE_DATA);

export const performSQLFullLoadTestWithCustomData = (
  tableID: mssqlSource.TableID,
  data: ReadonlyArray<sql.TSQLRow>,
) =>
  performSQLLoadTest<
    sqlSource.VirtualTableExportEvents<
      mssqlSource.TableReadContext,
      mssqlSource.TableID
    >,
    mssqlSource.TableInput,
    mssqlSource.TableReadContext
  >(
    [
      "sqlExecutionStarted",
      "sqlExecutionEnded",
      "tableExportStart",
      "tableExportProgress",
      "tableExportEnd",
    ] as const,
    (rowsBuilder, eventEmitter, intermediateRowEventInterval) =>
      rowsBuilder.fullLoad(eventEmitter, { intermediateRowEventInterval }),
    () => ({ tableID }),
    data,
    (pipelineInput, tableProcessingStartTime, recordedEvents) => {
      const durationInMs = getPipelineDurationInMs(recordedEvents);
      const sqlEvent = `SELECT * FROM ${mssqlSource.getFullTableName(tableID)}`;
      const tableExportEvent: sqlSource.VirtualTableExportEvents<
        mssqlSource.TableReadContext,
        mssqlSource.TableID
      >["tableExportStart"] = {
        context: {
          input: pipelineInput,
          tableID,
          tableProcessingStartTime,
        },
        tableID,
      };
      return [
        {
          eventName: "tableExportStart",
          eventArg: tableExportEvent,
        },
        {
          eventName: "sqlExecutionStarted",
          eventArg: sqlEvent,
        },
        ...data.map(
          (_, idx) =>
            ({
              eventName: "tableExportProgress",
              eventArg: {
                ...tableExportEvent,
                currentSqlRowIndex: idx,
              },
            } as const),
        ),
        {
          eventName: "sqlExecutionEnded",
          eventArg: sqlEvent,
        },
        {
          eventName: "tableExportEnd",
          eventArg: {
            ...tableExportEvent,
            errors: [],
            sqlRowsProcessedTotal: data.length,
            durationInMs,
          },
        },
      ];
    },
  );

export const performSQLIncrementalLoadTest = (
  changeTrackingStorage: common.ObjectStorageFunctionality<number>,
  dataRange?: IncrementalLoadInfo<number>,
) => {
  const expectedData = dataRange
    ? TABLE_DATA.slice(dataRange.startIndex, dataRange.elementCount)
    : TABLE_DATA;
  return performSQLIncrementalLoadTestWithCustomData<number, typeof t.Integer>(
    TABLE_ID,
    expectedData,
    dataRange?.previousChangeTrackingVersion,
    expectedData.reduce<number | undefined>(
      (max, [id]) => (max === undefined ? id : Math.max(max, id)),
      undefined,
    ),
    TABLE_MD.columnNames[0],
    changeTrackingStorage,
    t.Integer,
    undefined,
  );
};

export const performSQLIncrementalLoadTestWithCustomData = <
  TChangeTrackingDatum,
  TValidation extends t.Mixed
>(
  tableID: mssqlSource.TableID,
  expectedData: ReadonlyArray<sql.TSQLRow>,
  existingChangeTracking: TChangeTrackingDatum | undefined,
  seenChangeTracking: TChangeTrackingDatum | undefined,
  changedAtColumnName: string,
  changeTrackingStorage: common.ObjectStorageFunctionality<
    t.TypeOf<TValidation>
  >,
  changeTrackingValidation: TValidation,
  prepareChangeTrackingForSerialization:
    | ((changeTracking: unknown) => t.TypeOf<TValidation>)
    | undefined,
) =>
  performSQLLoadTest<
    sqlSource.VirtualTableExportWithChangeTrackingEvents<
      mssqlSource.TableReadContext,
      mssqlSource.TableID,
      TChangeTrackingDatum
    >,
    mssqlSource.TableInput,
    mssqlSource.TableReadContext
  >(
    [
      "sqlExecutionStarted",
      "sqlExecutionEnded",
      "tableExportStart",
      "tableExportProgress",
      "tableExportEnd",
      "tableChangeTrackVersionSeen",
      "invalidRowSeen",
      "changeTrackingVersionUploaded",
    ],
    (rowsBuilder, eventEmitter, intermediateRowEventInterval) =>
      rowsBuilder.incrementalLoadWithChangeTrackingColumn(eventEmitter, () => ({
        changedAtColumnName,
        changeTrackingStorage,
        changeTrackingValidation,
        prepareChangeTrackingForSerialization,
        intermediateRowEventInterval,
      })),
    () => ({ tableID }),
    expectedData,
    (pipelineInput, tableProcessingStartTime, recordedEvents) => {
      const hasCTUploadEvent =
        seenChangeTracking && existingChangeTracking !== seenChangeTracking;
      const durationInMs = getPipelineDurationInMs(
        recordedEvents,
        hasCTUploadEvent ? 1 : 0,
      );
      const sqlEvent = existingChangeTracking
        ? `SELECT * FROM ${mssqlSource.getFullTableName(
            tableID,
          )} WHERE [${changedAtColumnName}] > '${existingChangeTracking}'`
        : `SELECT * FROM ${mssqlSource.getFullTableName(tableID)}`;
      const context = {
        input: pipelineInput,
        tableID: pipelineInput.tableID,
        tableProcessingStartTime,
      };
      const tableExportEvent: sqlSource.VirtualTableExportEvents<
        mssqlSource.TableReadContext,
        mssqlSource.TableID
      >["tableExportStart"] = {
        context,
        tableID,
      };

      const retVal: Array<
        TRecordedEvent<
          sqlSource.VirtualTableExportWithChangeTrackingEvents<
            mssqlSource.TableReadContext,
            mssqlSource.TableID,
            TChangeTrackingDatum
          >
        >
      > = [
        {
          eventName: "tableChangeTrackVersionSeen",
          eventArg: {
            context,
            tableID,
            previousChangeTrackingVersion: existingChangeTracking,
            changeTrackingVersion: undefined,
          },
        },
        {
          eventName: "tableExportStart",
          eventArg: tableExportEvent,
        },
        {
          eventName: "sqlExecutionStarted",
          eventArg: sqlEvent,
        },
        ...expectedData.map(
          (_, idx) =>
            ({
              eventName: "tableExportProgress",
              eventArg: {
                ...tableExportEvent,
                currentSqlRowIndex: idx,
              },
            } as const),
        ),
        {
          eventName: "sqlExecutionEnded",
          eventArg: sqlEvent,
        },
        {
          eventName: "tableExportEnd",
          eventArg: {
            ...tableExportEvent,
            errors: [],
            sqlRowsProcessedTotal: expectedData.length,
            durationInMs,
          },
        },
      ];
      if (hasCTUploadEvent) {
        retVal.push({
          eventName: "changeTrackingVersionUploaded",
          eventArg: {
            context,
            tableID,
            previousChangeTrackingVersion: existingChangeTracking,
            changeTrackingVersion: seenChangeTracking,
          },
        });
      }
      return retVal;
    },
  );

export const performSQLIncrementalLoadWithNativeCTTest = (
  changeTrackingStorage: common.ObjectStorageFunctionality<string>,
  seenChangeTracking: string | undefined,
  dataRange?: IncrementalLoadInfo<string>,
) => {
  let expectedData: ReadonlyArray<ReadonlyArray<unknown>> = dataRange
    ? TABLE_DATA.slice(
        dataRange.startIndex,
        dataRange.startIndex + dataRange.elementCount,
      )
    : TABLE_DATA;
  if (dataRange?.deletionTime) {
    expectedData = expectedData.map((row) => [row[0], null]);
  }
  return performSQLIncrementalLoadWithNativeCTTestWithCustomData(
    TABLE_ID,
    TABLE_MD,
    expectedData,
    dataRange?.previousChangeTrackingVersion,
    seenChangeTracking,
    changeTrackingStorage,
    dataRange?.deletionTime,
  );
};

export const performSQLIncrementalLoadWithNativeCTTestWithCustomData = (
  tableID: mssqlSource.TableID,
  tableMD: sql.TableMetaData,
  expectedData: ReadonlyArray<sql.TSQLRow>,
  existingChangeTracking: string | undefined,
  seenChangeTracking: string | undefined,
  changeTrackingStorage: common.ObjectStorageFunctionality<string>,
  deletionTime?: Date,
  dontAutoEnableChangeTracking = false,
) => {
  const hasExistingCT = existingChangeTracking !== undefined;
  if (hasExistingCT) {
    tableMD = {
      ...tableMD,
      isCTEnabled: true,
    };
  }
  const allColumns = [
    ...tableMD.columnNames,
    mssqlSource.DEFAULT_PROCESSED_AT_COLUMN_NAME,
    mssqlSource.DEFAULT_CHANGED_AT_COLUMN_NAME,
    mssqlSource.DEFAULT_DELETED_AT_COLUMN_NAME,
  ];
  return performSQLLoadTest<
    sqlSource.VirtualTableExportWithChangeTrackingEvents<
      mssqlSource.TableChangeTrackingReadContext,
      mssqlSource.TableID,
      string
    >,
    mssqlSource.TableAndMetadata,
    mssqlSource.TableChangeTrackingReadContext
  >(
    [
      "sqlExecutionStarted",
      "sqlExecutionEnded",
      "tableExportStart",
      "tableExportProgress",
      "tableExportEnd",
      "tableChangeTrackVersionSeen",
      "invalidRowSeen",
      "changeTrackingVersionUploaded",
    ],
    (rowsBuilder, eventEmitter, intermediateRowEventInterval) =>
      rowsBuilder.incrementalLoadWithSQLServerChangeTracking(
        eventEmitter,
        () => ({
          changeTrackingStorage,
          dontAutoEnableChangeTracking: false,
          intermediateRowEventInterval,
        }),
      ),
    () => ({ tableID, tableMD }),
    (context, recordedData) =>
      expectedData.map((data, idx) => {
        const recordedDatum = recordedData[idx]!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
        return [
          ...data!, // eslint-disable-line @typescript-eslint/no-non-null-assertion
          context.tableProcessingStartTime, // Processing time
          recordedDatum[recordedDatum.length - 2], // Last changed time - from DB
          deletionTime ? deletionTime : null, // Deleted time
        ];
      }),
    (pipelineInput, tableProcessingStartTime, recordedEvents) => {
      const hasCTUploadEvent =
        seenChangeTracking && existingChangeTracking !== seenChangeTracking;
      const durationInMs = getPipelineDurationInMs(
        recordedEvents,
        hasCTUploadEvent ? 1 : 0,
      );
      const sqlEvent = existingChangeTracking
        ? `USE [${
            tableID.databaseName
          }]; SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, tc.COMMIT_TIME, ${tableMD.columnNames
            .map((c) => `t.[${c}]`)
            .join(", ")}
FROM CHANGETABLE(CHANGES ${mssqlSource.getDatabaseSpecificFullTableName(
            tableID,
          )},${existingChangeTracking}) ct
  LEFT JOIN ${mssqlSource.getDatabaseSpecificFullTableName(
    tableID,
  )} t -- Left join in order to preserve information about deleted columns
    ON t.[ID] = ct.[ID]
  LEFT JOIN sys.dm_tran_commit_table tc
    ON ct.SYS_CHANGE_VERSION = tc.COMMIT_TS`
        : `SELECT * FROM ${mssqlSource.getFullTableName(tableID)}`;
      const context = {
        input: pipelineInput,
        tableID,
        tableMD,
        tableProcessingStartTime,
        allColumns,
      };
      const tableExportEvent: sqlSource.VirtualTableExportEvents<
        mssqlSource.TableChangeTrackingReadContext,
        mssqlSource.TableID
      >["tableExportStart"] = {
        context,
        tableID,
      };

      const retVal: Array<
        TRecordedEvent<
          sqlSource.VirtualTableExportWithChangeTrackingEvents<
            mssqlSource.TableChangeTrackingReadContext,
            mssqlSource.TableID,
            string
          >
        >
      > = [
        {
          eventName: "tableChangeTrackVersionSeen",
          eventArg: {
            context,
            tableID,
            previousChangeTrackingVersion: existingChangeTracking,
            changeTrackingVersion: existingChangeTracking,
          },
        },
        {
          eventName: "tableExportStart",
          eventArg: tableExportEvent,
        },
        {
          eventName: "sqlExecutionStarted",
          eventArg: sqlEvent,
        },
        ...expectedData.map(
          (_, idx) =>
            ({
              eventName: "tableExportProgress",
              eventArg: {
                ...tableExportEvent,
                currentSqlRowIndex: idx,
              },
            } as const),
        ),
        {
          eventName: "sqlExecutionEnded",
          eventArg: sqlEvent,
        },
        {
          eventName: "tableExportEnd",
          eventArg: {
            ...tableExportEvent,
            errors: [],
            sqlRowsProcessedTotal: expectedData.length,
            durationInMs,
          },
        },
      ];
      if (hasExistingCT) {
        const ctCheckSQL = `USE [${tableID.databaseName}]; SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@tableID))`;
        retVal.unshift(
          {
            eventName: "sqlExecutionStarted",
            eventArg: ctCheckSQL,
          },
          {
            eventName: "sqlExecutionEnded",
            eventArg: ctCheckSQL,
          },
        );
      } else {
        const getTableCTSQL = `USE [${tableID.databaseName}]; SELECT CHANGE_TRACKING_CURRENT_VERSION()`;
        retVal.splice(
          2,
          0,
          {
            eventName: "sqlExecutionStarted",
            eventArg: getTableCTSQL,
          },
          {
            eventName: "sqlExecutionEnded",
            eventArg: getTableCTSQL,
          },
        );
      }
      if (!tableMD.isCTEnabled && !dontAutoEnableChangeTracking) {
        const alterTableSQL = `ALTER TABLE ${mssqlSource.getFullTableName(
          tableID,
        )}
  ENABLE CHANGE_TRACKING
  WITH (TRACK_COLUMNS_UPDATED = OFF)`;
        retVal.unshift(
          {
            eventName: "sqlExecutionStarted",
            eventArg: alterTableSQL,
          },
          {
            eventName: "sqlExecutionEnded",
            eventArg: alterTableSQL,
          },
        );
      }
      if (hasCTUploadEvent) {
        retVal.push({
          eventName: "changeTrackingVersionUploaded",
          eventArg: {
            context,
            tableID,
            previousChangeTrackingVersion: existingChangeTracking,
            changeTrackingVersion: seenChangeTracking,
          },
        });
      }
      return retVal;
    },
    () => ({ tableMD, allColumns }),
    hasExistingCT ? 2 : 0,
  );
};

export const performSQLLoadTest = <
  TEvents extends sqlSource.VirtualTableExportEvents<
    TContext,
    mssqlSource.TableID
  >,
  TInput extends mssqlSource.TableInput,
  TContext extends mssqlSource.TableReadContext<TInput>
>(
  eventNames: ReadonlyArray<keyof TEvents>,
  getDataSource: (
    rowsBuilder: mssqlSource.RowsInTableBuilder,
    eventEmitter: common.EventEmitter<TEvents>,
    intermediateRowEventInterval: number,
  ) => common.TPipelineFactory<TInput, TContext, sql.TSQLRow>,
  getInput: () => TInput,
  expectedData: common.ItemOrFactory<
    ReadonlyArray<sql.TSQLRow>,
    [TContext, ReadonlyArray<sql.TSQLRow>]
  >,
  getExpectedEvents: (
    pipelineInput: TInput,
    tableProcessingStartTime: Date,
    seenRecordedEvents: ReadonlyArray<TRecordedEvent<TEvents>>,
  ) => ReadonlyArray<TRecordedEvent<TEvents>>,
  getContextAdditionalProps?: (context: TContext) => Partial<TContext>,
  eventIndexForContext = 0,
) => async (t: ExecutionContext<SQLServerTestContext>) => {
  const eventBuilder = new common.EventEmitterBuilder<TEvents>();
  const recordedEvents: Array<TRecordedEvent<TEvents>> = [];
  for (const eventName of eventNames) {
    eventBuilder.addEventListener(eventName, (eventArg) => {
      recordedEvents.push({ eventName, eventArg });
    });
  }
  const dataSource = getDataSource(
    mssqlSource.rowsInTable(getMSSQLPoolFromTestContext(t)),
    eventBuilder.createEventEmitter(),
    1,
  );
  const dataSink: Array<sql.TSQLRow> = [];
  const pipelineInput = getInput();
  let seenContext: TContext | undefined = undefined;
  await dataSource(() => (context) => {
    seenContext = context;
    t.deepEqual(context, {
      input: pipelineInput,
      tableID: pipelineInput.tableID,
      tableProcessingStartTime: context.tableProcessingStartTime,
      ...(getContextAdditionalProps?.(context) || {}),
    } as TContext);
    return {
      storing: {
        processor: (row) => {
          dataSink.push(row ? [...row] : undefined); // Copy the data - the array elements will get overwritten for next row
        },
        end: () => {},
      },
    };
  })(pipelineInput);

  // Verify that correct data has been seen
  const actualContext =
    seenContext ||
    (recordedEvents[eventIndexForContext]
      .eventArg as TEvents["tableExportStart"]).context;
  t.deepEqual(
    dataSink as ReadonlyArray<sql.TSQLRow>,
    typeof expectedData === "function"
      ? expectedData(actualContext, dataSink)
      : expectedData,
  );

  // Verify that correct events has been seen
  t.deepEqual(
    recordedEvents as ReadonlyArray<TRecordedEvent<TEvents>>,
    getExpectedEvents(
      pipelineInput,
      actualContext.tableProcessingStartTime,
      recordedEvents,
    ),
  );
};

export interface TRecordedEvent<TEvents> {
  eventName: keyof TEvents;
  eventArg: Readonly<TEvents[keyof TEvents]>;
}

export interface IncrementalLoadInfo<TChangeTrackingDatum> {
  startIndex: number;
  elementCount: number;
  previousChangeTrackingVersion: TChangeTrackingDatum;
  deletionTime?: Date;
}

const getPipelineDurationInMs = <
  TEvents extends sqlSource.VirtualTableExportEvents<
    TContext,
    mssqlSource.TableID
  >,
  TInput extends mssqlSource.TableInput,
  TContext extends mssqlSource.TableReadContext<TInput>
>(
  recordedEvents: ReadonlyArray<TRecordedEvent<TEvents>>,
  offsetFromLast = 0,
) =>
  (recordedEvents[recordedEvents.length - 1 - offsetFromLast]
    .eventArg as Readonly<TEvents["tableExportEnd"]>).durationInMs;

// TODO maybe in test-support class, or maybe even common userlib
export function createInMemoryStorage<TValue>(
  storageID?: string,
): common.ObjectStorageFunctionality<TValue> {
  let value: TValue | undefined = undefined;
  return {
    storageID: storageID || "in-memory",
    readExistingData: () =>
      Promise.resolve(value === undefined ? undefined : JSON.stringify(value)),
    writeNewDataWhenDifferent: (newValue) => {
      value = newValue;
      return Promise.resolve();
    },
  };
}

export const performDeletionInDB = async (
  t: ExecutionContext<SQLServerTestContext>,
) => {
  const connection = await new mssql.ConnectionPool(
    getSQLConfigFromContext(t.context.sqlServerInfo),
  ).connect();
  try {
    await connection.query(
      `DELETE FROM ${mssqlSource.getFullTableName(TABLE_ID)}
  WHERE ${TABLE_MD.columnNames[0]} = ${TABLE_DATA[0][0]}`,
    );
  } finally {
    await connection.close();
  }
};
