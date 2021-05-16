import * as utils from "@data-heaving/common";
import * as sql from "@data-heaving/common-sql";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualMetaDataTableEvents<TTableID> {
  tablesDiscovered: {
    tables: ReadonlyArray<{ tableID: TTableID; tableMD: sql.TableMetaData }>;
  };
}
export interface VirtualSourceTableEvents<TTableID, TChangeTrackingDatum>
  extends sql.VirtualSQLEvents {
  tableExportStart: {
    tableID: TTableID;
    tableMD: sql.TableMetaData;
  };
  tableChangeTrackVersionSeen: VirtualSourceTableEvents<
    TTableID,
    TChangeTrackingDatum
  >["tableExportStart"] & {
    changeTrackingVersion: TChangeTrackingDatum | undefined;
    previousChangeTrackingVersion: TChangeTrackingDatum | undefined;
  };
  tableExportProgress: VirtualSourceTableEvents<
    TTableID,
    TChangeTrackingDatum
  >["tableChangeTrackVersionSeen"] & {
    currentSqlRowIndex: number;
  };
  invalidRowSeen: VirtualSourceTableEvents<
    TTableID,
    TChangeTrackingDatum
  >["tableExportProgress"] & {
    row: ReadonlyArray<unknown>;
  };
  changeTrackingVersionUploaded: VirtualSourceTableEvents<
    TTableID,
    TChangeTrackingDatum
  >["tableChangeTrackVersionSeen"] & {
    changeTrackingVersion: TChangeTrackingDatum;
  };
  tableExportEnd: VirtualSourceTableEvents<
    TTableID,
    TChangeTrackingDatum
  >["tableChangeTrackVersionSeen"] & {
    sqlRowsProcessedTotal: number;
    durationInMs: number;
    errors: ReadonlyArray<unknown>;
  };
}

export type SourceTableEventEmitter<
  TTableID,
  TChangeTrackingDatum
> = utils.EventEmitter<
  VirtualSourceTableEvents<TTableID, TChangeTrackingDatum>
>;

export const createMetaDataEventEmitterBuilder = <TTableID>() =>
  new utils.EventEmitterBuilder<VirtualMetaDataTableEvents<TTableID>>();

export const createEventEmitterBuilder = <TTableID, TChangeTrackingDatum>() =>
  new utils.EventEmitterBuilder<
    VirtualSourceTableEvents<TTableID, TChangeTrackingDatum>
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

export const consoleLoggingEventEmitterBuilder = <
  TTableID,
  TChangeTrackingDatum
>(
  getTableIDString: (tableID: TTableID) => string,
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<
    VirtualSourceTableEvents<TTableID, TChangeTrackingDatum>
  >,
  printInvalidRowContents?: boolean,
) => {
  if (!builder) {
    builder = createEventEmitterBuilder();
  }

  const logger = utils.createConsoleLogger(logMessagePrefix);

  builder.addEventListener("tableExportStart", (arg) =>
    logger(`Starting export for ${getTableIDString(arg.tableID)}`),
  );
  builder.addEventListener("tableChangeTrackVersionSeen", (arg) =>
    logger(
      `CT Info for ${getTableIDString(arg.tableID)}: previous CT version "${
        arg.previousChangeTrackingVersion
      }", CT enabled ${arg.tableMD.isCTEnabled}, CT from DB "${
        arg.changeTrackingVersion
      }".`,
    ),
  );
  builder.addEventListener("tableExportProgress", (arg) =>
    logger(`Processed row #${arg.currentSqlRowIndex}`),
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
