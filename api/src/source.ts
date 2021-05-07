import { isDeepStrictEqual } from "util";
import * as t from "io-ts";
import * as common from "@data-heaving/common";
import * as common_validation from "@data-heaving/common-validation";
import * as util from "./util";
import * as events from "./events";

export interface SourceFunctionality<
  TPool,
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo
> {
  eventEmitter?: events.SourceTableEventEmitter<TTableID, TChangeTrackingDatum>;
  getSQLPool: () => util.SQLConnectionPoolAbstraction<TPool, TConnection>;
  getAllTables: (
    connection: TConnection,
  ) => Promise<
    ReadonlyArray<{
      tableID: TTableID;
      tableMD: common.TableMetaData;
      intermediateRowEventInterval: number;
      additionalInfo: TAdditionalInfo;
    }>
  >;
  // getTableIDString: (tableID: TTableID) => string;
  changeTrackingFunctionality?: ChangeTrackingFunctionality<
    TConnection,
    TTableID,
    TChangeTrackingDatum,
    TAdditionalInfo
  >;
  streamRows: StreamSQLRows<
    TConnection,
    TTableID,
    TChangeTrackingDatum,
    TAdditionalInfo,
    TChangeTrackingDatum
  >;
}

export interface ChangeTrackingFunctionality<
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo
> {
  validation: t.Type<TChangeTrackingDatum>;
  checkValidity: (opts: {
    connection: TConnection;
    tableID: TTableID;
    tableMD: common.TableMetaData;
    previousChangeTracking: TChangeTrackingDatum | undefined;
    additionalInfo: TAdditionalInfo;
  }) => Promise<TChangeTrackingDatum | undefined>;
  streamRows: StreamSQLRows<
    TConnection,
    TTableID,
    TChangeTrackingDatum,
    TAdditionalInfo,
    TChangeTrackingDatum | undefined
  >;
  storage: TableSpecificStorage<TTableID, TChangeTrackingDatum>;
}

export type TableSpecificStorage<TTableID, TStorageData> = (
  arg: DatumProcessorFactoryArg<TTableID>,
) => common.ObjectStorageFunctionality<TStorageData>;

export interface ObjectStorageFunctionality<TObject> {
  readExistingData: () => Promise<TObject | string | Buffer | undefined>;
  writeNewWhenDifferent: (md: TObject) => Promise<void>;
}

export type TSQLRow = ReadonlyArray<unknown> | undefined;

export type DatumProcessorFactoryArg<TTableID> = {
  tableID: TTableID;
  tableMD: common.TableMetaData;
  additionalColumns: ReadonlyArray<string>;
  tableProcessingStartTime: Date;
};

export type StreamSQLRows<
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo,
  TResult
> = (opts: {
  connection: TConnection;
  tableID: TTableID;
  tableMD: common.TableMetaData;
  changeTracking: TChangeTrackingDatum | undefined;
  outputArray: Array<unknown>;
  rowProcessor: (
    rowStatus: RowStatus,
    transactionTime: string | undefined | null,
    controlFlow: common.ControlFlow | undefined,
  ) => unknown;
  onQueryEnd: () => unknown;
  additionalInfo: TAdditionalInfo;
}) => Promise<TResult>;
export type RowStatus = "deleted" | "invalid" | undefined;

export const sqlSource = <
  TPool,
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo
>({
  eventEmitter,
  getSQLPool,
  getAllTables,
  changeTrackingFunctionality,
  streamRows,
}: SourceFunctionality<
  TPool,
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo
>) => {
  const sqlTracer: util.SQLTracer = eventEmitter
    ? (sql, startedOrEnded) =>
        eventEmitter.emit(
          startedOrEnded === "started"
            ? "sqlExecutionStarted"
            : "sqlExecutionEnded",
          sql,
        )
    : undefined;
  return (
    datumProcessorFactory: () => common.DatumStoringFactory<
      DatumProcessorFactoryArg<TTableID>,
      TSQLRow
    >,
  ) => () => {
    const datumStoring = datumProcessorFactory();
    return util.useConnectionPoolAsync(
      getSQLPool(),
      sqlTracer,
      async (connection) => {
        const allTables = await getAllTables(connection);

        const mdEvent: events.VirtualSourceTableEvents<
          TTableID,
          TChangeTrackingDatum
        >["dataTablesDiscovered"] = {
          tables: allTables.map(({ tableID, tableMD }) => ({
            tableID,
            tableMD,
          })),
          awaitablePromises: [],
        };
        eventEmitter?.emit("dataTablesDiscovered", mdEvent);
        await Promise.all(mdEvent.awaitablePromises);
        // We probably don't want to export all tables in parallel as that would most likely put source system under too heavy load
        // TODO maybe in the future parametrize this?
        const errors: Array<Error> = [];
        // Get metadata about *all* tables *with one query*, to reduce amount of queries we do
        // const allTableMDs = await read.getTableColumnMetaData(connection, allTables);
        // We don't do the same for change tracking versions, as single table export might take very long time (in initial load), so getting these right here might result in too much outdated version when the table actually gets its turn to perform export.
        for (const [
          tablesArrayIndex,
          { tableID, tableMD, intermediateRowEventInterval, additionalInfo },
        ] of allTables.entries()) {
          // Let's copy our info - in case some event handler modifies it, at least it won't affect our code.
          const eventArgBase = {
            tablesArrayIndex,
            tablesArrayLength: allTables.length,
            tableID: common.deepCopy(tableID),
            tableMD: common.deepCopy(tableMD),
          };
          try {
            await exportSingleTable(
              {
                changeTrackingFunctionality,
                streamRows,
              },
              {
                connection,
                datumProcessorFactory: datumStoring,
                intermediateRowEventInterval,
                additionalInfo,
                eventArgBase,
                eventEmitter,
              },
            );
          } catch (e) {
            errors.push(e);
          }
        }

        if (errors.length > 0) {
          throw new ErrorProcessedByEvents(
            `Errors occurred during table exports.`,
            errors,
          );
        }
      },
    );
  };
};

interface AuxiliaryDataRuntimeConfig<
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo
> {
  connection: TConnection;
  datumProcessorFactory: common.DatumStoringFactory<
    DatumProcessorFactoryArg<TTableID>,
    TSQLRow
  >;
  intermediateRowEventInterval: number;
  additionalInfo: TAdditionalInfo;
  eventEmitter:
    | events.SourceTableEventEmitter<TTableID, TChangeTrackingDatum>
    | undefined;
  eventArgBase: events.VirtualSourceTableEvents<
    TTableID,
    TChangeTrackingDatum
  >["tableExportStart"];
}

export const exportSingleTable = async <
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo
>(
  {
    streamRows,
    changeTrackingFunctionality,
  }: Pick<
    SourceFunctionality<
      unknown,
      TConnection,
      TTableID,
      TChangeTrackingDatum,
      TAdditionalInfo
    >,
    "streamRows" | "changeTrackingFunctionality"
  >,
  {
    connection,
    datumProcessorFactory,
    intermediateRowEventInterval,
    additionalInfo,
    eventEmitter,
    eventArgBase,
  }: AuxiliaryDataRuntimeConfig<
    TConnection,
    TTableID,
    TChangeTrackingDatum,
    TAdditionalInfo
  >,
) => {
  const { tableID, tableMD } = eventArgBase;
  const thisOperationStartTimeObject = new Date();
  const factoryArg: DatumProcessorFactoryArg<TTableID> = {
    tableID,
    tableMD,
    additionalColumns: ["__PROCESSED_AT", "__CHANGED_AT", "__DELETED_AT"],
    tableProcessingStartTime: thisOperationStartTimeObject,
  };
  const ctInfo = changeTrackingFunctionality
    ? await prepareChangeTracking(
        changeTrackingFunctionality,
        factoryArg,
        connection,
        additionalInfo,
      )
    : undefined;
  const eventArg = {
    ...eventArgBase,
    changeTrackingVersion: ctInfo?.changeTrackingVersion,
    previousChangeTrackingVersion: ctInfo?.previousChangeTrackingVersion,
  };
  eventEmitter?.emit("tableChangeTrackVersionSeen", eventArg);
  let sqlRowsProcessedTotal = 0;
  // Stream data from SQL DB via CSV transformation to blob storage
  let seenCTVersion: TChangeTrackingDatum | undefined = undefined;
  const uploadPromises: Array<ReadonlyArray<Promise<unknown>>> = [];
  eventEmitter?.emit("tableExportStart", eventArg);
  const thisOperationStartTime = common.dateToISOUTCString(
    thisOperationStartTimeObject,
  );
  let datumProcessor: common.DatumStoring<TSQLRow> | undefined = undefined;
  const onEnd = () => {
    if (datumProcessor) {
      datumProcessor.end();
      datumProcessor = undefined;
    }
  };
  // Prepare to stream data from SQL DB via CSV transformation to blob storage
  const { columnNames } = tableMD;
  const outputArray = Array<unknown>(columnNames.length + 3); // Extra 3 cols for last modify + deletion times + this time
  const [thisTimeIndex, lastModifiedIndex, deletedIndex] = [
    columnNames.length,
    columnNames.length + 1,
    columnNames.length + 2,
  ]; // in outputArray

  const errors: Array<unknown> = [];
  try {
    seenCTVersion = await (ctInfo?.changeTrackingVersion
      ? ctInfo.changeTrackingFunctionality.streamRows
      : streamRows)({
      connection,
      tableID,
      tableMD,
      changeTracking: ctInfo?.changeTrackingVersion,
      outputArray,
      rowProcessor: (rowStatus, transactionTime, controlFlow) => {
        if (rowStatus !== "invalid") {
          // At this point, all the current row data has been set to outputArray. We need to just set additional information
          outputArray[thisTimeIndex] = thisOperationStartTime;
          const thisLastModified = transactionTime ?? thisOperationStartTime;
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
        if (!datumProcessor) {
          let promises: ReadonlyArray<Promise<unknown>> | undefined = undefined;
          ({ storing: datumProcessor, promises } = datumProcessorFactory(
            {
              tableID,
              tableMD,
              additionalColumns: [
                "__PROCESSED_AT",
                "__CHANGED_AT",
                "__DELETED_AT",
              ],
              tableProcessingStartTime: thisOperationStartTimeObject,
            },
            onEnd,
          ));
          if (promises && promises.length > 0) {
            uploadPromises.push(promises);
          }
        }
        datumProcessor.processor(
          rowStatus === "invalid" ? undefined : outputArray,
          controlFlow,
        );
      },
      onQueryEnd: onEnd,
      additionalInfo,
    });
  } catch (e) {
    errors.push(e);
  }

  try {
    await Promise.all((uploadPromises || []).flat());
  } catch (e) {
    errors.push(e);
  }

  eventEmitter?.emit("tableExportEnd", {
    ...eventArg,
    sqlRowsProcessedTotal,
    durationInMs: new Date().valueOf() - thisOperationStartTimeObject.valueOf(),
    errors,
  });

  if (errors.length > 0) {
    throw new MultipleErrors(errors);
  }

  // Finally, remember to upload last seen change tracking version
  if (
    ctInfo &&
    ctInfo.changeTrackingFunctionality.validation.is(seenCTVersion) &&
    !isDeepStrictEqual(seenCTVersion, ctInfo?.previousChangeTrackingVersion)
  ) {
    await ctInfo.ctStorage.writeNewDataWhenDifferent(seenCTVersion);
    eventEmitter?.emit("changeTrackingVersionUploaded", {
      ...eventArg,
      changeTrackingVersion: seenCTVersion,
    });
  }
  return {
    previousChangeTrackingVersion: ctInfo?.previousChangeTrackingVersion,
    changeTrackingVersion: ctInfo?.changeTrackingVersion,
    sqlRowsProcessedTotal,
  };
};

export class MultipleErrors extends Error {
  public readonly errors: ReadonlyArray<unknown>;

  public constructor(errorArray: ReadonlyArray<unknown>, message?: string) {
    super(message);
    this.errors = errorArray;
  }
}

export class ErrorProcessedByEvents extends Error {
  public readonly error: Error | ReadonlyArray<Error>;

  public constructor(
    message: string,
    handledError: Error | ReadonlyArray<Error>,
  ) {
    super(message);
    this.error = handledError;
  }
}

const prepareChangeTracking = async <
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo
>(
  changeTrackingFunctionality: ChangeTrackingFunctionality<
    TConnection,
    TTableID,
    TChangeTrackingDatum,
    TAdditionalInfo
  >,
  factoryArg: DatumProcessorFactoryArg<TTableID>,
  connection: TConnection,
  additionalInfo: TAdditionalInfo,
) => {
  const ctStorage = changeTrackingFunctionality.storage(factoryArg);
  const previousChangeTrackingVersion = await common_validation.retrieveValidatedDataFromStorage(
    ctStorage.readExistingData,
    changeTrackingFunctionality.validation.decode,
  );
  const changeTrackingVersion = await changeTrackingFunctionality.checkValidity(
    {
      connection,
      tableID: factoryArg.tableID,
      tableMD: factoryArg.tableMD,
      previousChangeTracking: previousChangeTrackingVersion,
      additionalInfo,
    },
  );
  return {
    changeTrackingFunctionality,
    ctStorage,
    previousChangeTrackingVersion,
    changeTrackingVersion,
  };
};
