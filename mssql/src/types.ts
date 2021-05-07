import * as t from "io-ts";
import * as sql from "mssql";
import * as utils from "@data-heaving/common";
import * as validation from "@data-heaving/common-validation";
import * as api from "@data-heaving/source-sql";

export type MainConfig = utils.DeepReadOnly<t.TypeOf<typeof mainConfig>>;

// String without "]" or "'" characters
const idRegex = /^[^\]]+$/;
export const identifier = t.refinement(
  t.string,
  (id) => idRegex.test(id),
  "MSSQLIdentifier",
);

export const tableID = t.type(
  {
    databaseName: identifier,
    schemaName: identifier,
    tableName: identifier,
  },
  "MSSQLTableFullName",
);
export type TableID = t.TypeOf<typeof tableID>;

export type MSSQLConnectionPool = api.SQLConnectionPoolAbstraction<
  sql.ConnectionPool,
  MSSQLConnection
>;

export type MSSQLConnection = api.SQLConnectionAbstraction<
  sql.Request,
  sql.Request
>;

export type RowProcessingOptions = Parameters<
  api.StreamSQLRows<MSSQLConnection, TableID, string, AdditionalInfo, string>
>[0];

export type AdditionalInfo = t.TypeOf<typeof defaults>;

// Not all values from BIGINT can fit into JS 'number', so let's go with string too
const numberRegex = /^[-]?\d+$/;
export const bigInt = t.union([
  t.refinement(t.number, (num) => Number.isInteger(num)),
  t.refinement(t.string, (str) => numberRegex.test(str)),
]);
export type BigInt = t.TypeOf<typeof bigInt>;
const defaults = t.partial(
  {
    databaseName: identifier,
    dontAutoEnableChangeTracking: t.boolean,
    intermediateRowEventInterval: validation.intGtZero,
  },
  "DefaultsConfig",
);
export const overrideDefaults = t.partial(
  {
    overrideDefaults: defaults,
  },
  "OverrideDefaults",
);
export const connectionConfig = t.type(
  {
    server: validation.nonEmptyString,
    port: validation.intGtZero,
    user: validation.nonEmptyString,
    password: validation.nonEmptyString,
  },
  "SQLConnection",
);
export const mainConfig = t.intersection(
  [
    // Mandatory top-level properties
    t.type(
      {
        connection: connectionConfig,
        data: t.refinement(
          t.partial({
            schemas: t.array(
              t.union(
                [
                  // Array of strings or objects
                  validation.nonEmptyString, // Just string = schema name
                  t.intersection(
                    [
                      // Object = schema name + default overrides
                      // Schema name is mandatory
                      t.type(
                        {
                          schemaName: identifier,
                        },
                        "SchemaMandatory",
                      ),
                      // Other defaults may be overridden for all tables within the schema
                      overrideDefaults,
                    ],
                    "SchemaObject",
                  ),
                ],
                "SchemaObjectOrName",
              ),
              "SchemaList",
            ),
            tables: t.intersection(
              [
                t.type(
                  {
                    tables: t.array(
                      t.union(
                        [
                          // Array of strings or objects
                          validation.nonEmptyString, // Just string = table name
                          t.intersection(
                            [
                              // Schema and table names are mandatory
                              t.type(
                                {
                                  tableName: identifier,
                                },
                                "TableMandatory",
                              ),
                              t.partial(
                                {
                                  schemaName: identifier,
                                },
                                "TableOptional",
                              ),
                              // Other defaults may be overridden for this particular table
                              overrideDefaults,
                            ],
                            "TableObject",
                          ),
                        ],
                        "TableOjectOrName",
                      ),
                      "TableList",
                    ),
                  },
                  "TableInfoMandatory",
                ),
                t.partial(
                  {
                    schemaName: identifier,
                  },
                  "TableInfoOptional",
                ),
              ],
              "TableInfo",
            ),
          }),
          (dataObject) => !!dataObject.schemas || !!dataObject.tables,
          "DataInfo",
        ), // At least one of 'schemas' or 'tables' must be present.
      },
      "MandatoryOptions",
    ),
    // Optional top-level properties
    t.partial(
      {
        defaults,
      },
      "OptionalOptions",
    ),
  ],
  "MainConfig",
);

// These are here so that they would be exported by index.ts
export const getDatabaseSpecificFullTableName = (
  tableID: Omit<TableID, "databaseName">,
) => `[${tableID.schemaName}].[${tableID.tableName}]`;
export const getFullTableName = (tableID: TableID) =>
  `[${tableID.databaseName}].${getDatabaseSpecificFullTableName(tableID)}`;
