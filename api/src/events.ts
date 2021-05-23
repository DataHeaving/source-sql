import * as utils from "@data-heaving/common";
import * as sql from "@data-heaving/common-sql";

// These are virtual interface - no instances implementing this are ever created
export interface VirtualMetaDataTableEvents<TTableID> {
  tablesDiscovered: {
    tables: ReadonlyArray<{ tableID: TTableID; tableMD: sql.TableMetaData }>;
  };
}
export interface VirtualTableExportEvents<TContext, TTableID>
  extends sql.VirtualSQLEvents {
  tableExportStart: {
    context: TContext;
    tableID: TTableID;
  };
  tableExportProgress: VirtualTableExportEvents<
    TContext,
    TTableID
  >["tableExportStart"] & {
    currentSqlRowIndex: number;
  };
  tableExportEnd: VirtualTableExportEvents<
    TContext,
    TTableID
  >["tableExportStart"] & {
    sqlRowsProcessedTotal: number;
    durationInMs: number;
    errors: ReadonlyArray<unknown>;
  };
}

export interface VirtualTableExportWithChangeTrackingEvents<
  TContext,
  TTableID,
  TChangeTrackingDatum
> extends VirtualTableExportEvents<TContext, TTableID> {
  tableChangeTrackVersionSeen: VirtualTableExportEvents<
    TContext,
    TTableID
  >["tableExportStart"] & {
    changeTrackingVersion: TChangeTrackingDatum | undefined;
    previousChangeTrackingVersion: TChangeTrackingDatum | undefined;
  };
  invalidRowSeen: VirtualTableExportWithChangeTrackingEvents<
    TContext,
    TTableID,
    TChangeTrackingDatum
  >["tableExportProgress"] & {
    row: ReadonlyArray<unknown>;
  };
  changeTrackingVersionUploaded: VirtualTableExportWithChangeTrackingEvents<
    TContext,
    TTableID,
    TChangeTrackingDatum
  >["tableChangeTrackVersionSeen"] & {
    changeTrackingVersion: TChangeTrackingDatum;
  };
}

export type TableExportEventEmitter<TContext, TTableID> = utils.EventEmitter<
  VirtualTableExportEvents<TContext, TTableID>
>;

export type TableExportWithChangeTrackingEventEmitter<
  TContext,
  TTableID,
  TChangeTrackingDatum
> = utils.EventEmitter<
  VirtualTableExportWithChangeTrackingEvents<
    TContext,
    TTableID,
    TChangeTrackingDatum
  >
>;

export const createMetaDataEventEmitterBuilder = <TTableID>() =>
  new utils.EventEmitterBuilder<VirtualMetaDataTableEvents<TTableID>>();

export const createTableExportEventEmitterBuilder = <TContext, TTableID>() =>
  new utils.EventEmitterBuilder<VirtualTableExportEvents<TContext, TTableID>>();

export const createTableExportWithChangeTrackingEventEmitterBuilder = <
  TContext,
  TTableID,
  TChangeTrackingDatum
>() =>
  new utils.EventEmitterBuilder<
    VirtualTableExportWithChangeTrackingEvents<
      TContext,
      TTableID,
      TChangeTrackingDatum
    >
  >();

export const consoleLoggingMetaDataEventEmitterBuilder = <TTableID>(
  getTableIDString: (tableID: TTableID) => string,
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<VirtualMetaDataTableEvents<TTableID>>,
) => {
  if (!builder) {
    builder = createMetaDataEventEmitterBuilder();
  }

  const logger = utils.createConsoleLogger(logMessagePrefix);

  builder.addEventListener("tablesDiscovered", (arg) =>
    logger(
      `Discovered tables ${arg.tables
        .map(({ tableID }) => getTableIDString(tableID))
        .join(", ")}.`,
    ),
  );
};

export const consoleLoggingTableExportEventEmitterBuilder = <
  TContext,
  TTableID
>(
  getTableIDString: (tableID: TTableID) => string,
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<
    VirtualTableExportEvents<TContext, TTableID>
  >,
) => {
  if (!builder) {
    builder = createTableExportEventEmitterBuilder();
  }

  const logger = utils.createConsoleLogger(logMessagePrefix);

  builder.addEventListener("tableExportStart", (arg) =>
    logger(`Starting export for ${getTableIDString(arg.tableID)}`),
  );
  builder.addEventListener("tableExportProgress", (arg) =>
    logger(`Processed row #${arg.currentSqlRowIndex}`),
  );
  builder.addEventListener("tableExportEnd", (arg) =>
    logger(
      `Ending export for ${getTableIDString(arg.tableID)}, total rows: ${
        arg.sqlRowsProcessedTotal
      }, completed ${
        arg.errors.length > 0
          ? `with a errors ${arg.errors.join(";")}`
          : "successfully"
      }.`,
      "error" in arg,
    ),
  );

  return builder;
};

export const consoleLoggingTableExportWithChangeTrackingEventEmitterBuilder = <
  TContext,
  TTableID,
  TChangeTrackingDatum
>(
  getTableIDString: (tableID: TTableID) => string,
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<
    VirtualTableExportWithChangeTrackingEvents<
      TContext,
      TTableID,
      TChangeTrackingDatum
    >
  >,
  printInvalidRowContents?: boolean,
) => {
  if (!builder) {
    builder = createTableExportWithChangeTrackingEventEmitterBuilder();
  }

  const logger = utils.createConsoleLogger(logMessagePrefix);

  builder.addEventListener("tableChangeTrackVersionSeen", (arg) =>
    logger(
      `CT Info for ${getTableIDString(arg.tableID)}: previous CT version "${
        arg.previousChangeTrackingVersion
      }", CT from DB "${arg.changeTrackingVersion}".`,
    ),
  );
  builder.addEventListener("invalidRowSeen", (arg) =>
    logger(
      `Invalid row at ${arg.currentSqlRowIndex}${
        printInvalidRowContents === true ? arg.row.join(", ") : ""
      }!`,
      true,
    ),
  );
  builder.addEventListener(
    "changeTrackingVersionUploaded",
    ({ changeTrackingVersion, tableID }) =>
      logger(
        `For ${getTableIDString(
          tableID,
        )}, change tracking version ${changeTrackingVersion} uploaded.`,
      ),
  );

  return builder;
};

export const processRowForEventEmitter = <TContext, TTableID>(
  eventEmitter: TableExportEventEmitter<TContext, TTableID>,
  eventArgBase: VirtualTableExportEvents<
    TContext,
    TTableID
  >["tableExportStart"],
  intermediateRowEventInterval: number,
  sqlRowsProcessedTotal: number,
) => {
  if (
    intermediateRowEventInterval > 0 &&
    sqlRowsProcessedTotal % intermediateRowEventInterval === 0
  ) {
    eventEmitter.emit("tableExportProgress", {
      ...eventArgBase,
      currentSqlRowIndex: sqlRowsProcessedTotal,
    });
  }
  return ++sqlRowsProcessedTotal;
};
