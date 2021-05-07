import * as t from "io-ts";
import * as types from "./types";
import * as api from "@data-heaving/source-sql";
import * as validation from "@data-heaving/common-validation";
import * as common from "@data-heaving/common";

export const getTableColumnMetaData = async (
  connection: types.MSSQLConnection,
  tables: ReadonlyArray<common.MakeOptional<types.TableID, "tableName">>, // We assume db name, schema name, and table name have all been previously validate not to include sql injections
) => {
  const retVal: Array<{
    tableMD: common.TableMetaData;
    originalIndex: number;
    tableID: types.TableID;
  }> = [];

  if (tables.length > 0) {
    const getTablesDictionaryKeyForSchema = (schemaName: string) =>
      `[${schemaName}]`;
    // Key: database name,
    // Value: dictionary, where
    //   Key: schema name + optionally table name
    //   Value: object with schema and table names, and original index to "tables" parameter of this function
    const tablesByDB = tables.reduce<{
      [dbName: string]: {
        [schemaAndTableNames: string]: {
          schemaName: string;
          tableName: string | undefined;
          arrayIndex: number;
        };
      };
    }>((dictionary, { databaseName, schemaName, tableName }, arrayIndex) => {
      common.getOrAddGeneric(dictionary, databaseName, () => ({}))[
        tableName
          ? types.getDatabaseSpecificFullTableName({ schemaName, tableName })
          : getTablesDictionaryKeyForSchema(schemaName)
      ] = {
        schemaName,
        tableName,
        arrayIndex,
      };
      return dictionary;
    }, {});

    const allTableInfoFromDB = await Promise.all(
      Object.entries(tablesByDB).map(async ([databaseName, databaseTables]) => {
        const dbTableArray = Object.values(databaseTables);
        // Fetch metadata information (primary column names) about the table
        let curIdx = 0;
        const allColumns = await api.streamQueryResults({
          connection,
          sqlCommand: `SELECT
    s.name,
    t.name,
    c.name,
    CASE WHEN ic.index_column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
    CASE WHEN ct.object_id IS NOT NULL THEN 1 ELSE 0 END AS is_tracked_by_ct,
    tt.name,
		CASE WHEN tt.name in ('nchar','nvarchar') AND c.max_length > 0 THEN c.max_length / 2 ELSE c.max_length END AS max_length,
		c.precision,
    c.scale,
    c.is_nullable
  FROM [${databaseName}].sys.columns c
    JOIN [${databaseName}].sys.tables t ON c.object_id = t.object_id
    JOIN [${databaseName}].sys.types tt ON c.system_type_id = tt.system_type_id AND c.user_type_id = tt.user_type_id
    JOIN [${databaseName}].sys.schemas s ON t.schema_id = s.schema_id
    LEFT JOIN [${databaseName}].sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1
    LEFT JOIN [${databaseName}].sys.index_columns ic ON ic.object_id = t.object_id AND i.index_id = ic.index_id AND c.column_id = ic.column_id
    LEFT JOIN [${databaseName}].sys.change_tracking_tables ct ON t.object_id = ct.object_id
  WHERE
    ${dbTableArray
      .map(({ tableName }) => {
        let conditionFragment = `s.name = @param_${curIdx}`;
        ++curIdx;
        if (tableName) {
          conditionFragment = `${conditionFragment} AND t.name = @param_${curIdx}`;
          ++curIdx;
        }
        return `(${conditionFragment})`;
      })
      .join(" OR\n")}`,
          onRow: (rowColumns) => {
            return {
              schemaName: validation.decodeOrThrow(
                validation.nonEmptyString.decode,
                rowColumns[0],
              ),
              tableName: validation.decodeOrThrow(
                validation.nonEmptyString.decode,
                rowColumns[1],
              ),
              columnName: validation.decodeOrThrow(
                validation.nonEmptyString.decode,
                rowColumns[2],
              ),
              isPrimaryKey:
                validation.decodeOrThrow(t.Integer.decode, rowColumns[3]) === 1,
              tableTrackedByCT:
                validation.decodeOrThrow(t.Integer.decode, rowColumns[4]) === 1,
              columnType: {
                typeName: validation.decodeOrThrow(
                  validation.nonEmptyString.decode,
                  rowColumns[5],
                ),
                maxLength: validation.decodeOrThrow(
                  t.Integer.decode,
                  rowColumns[6],
                ),
                precision: validation.decodeOrThrow(
                  t.Integer.decode,
                  rowColumns[7],
                ),
                scale: validation.decodeOrThrow(
                  t.Integer.decode,
                  rowColumns[8],
                ),
                isNullable: validation.decodeOrThrow(
                  t.boolean.decode,
                  rowColumns[9],
                ),
              },
            } as const;
          },
          prepareRequest: (request) => {
            let paramIdx = 0;
            for (const { schemaName, tableName } of dbTableArray) {
              request.input(`param_${paramIdx}`, schemaName);
              ++paramIdx;
              if (tableName) {
                request.input(`param_${paramIdx}`, tableName);
                ++paramIdx;
              }
            }
            return request;
          },
        });

        return allColumns.reduce<{
          [databaseSpecificTableID: string]: {
            columns: Array<{
              name: string;
              isPrimaryKey: boolean;
              columnType: common.ColumnTypeInfo;
            }>;
            isCTEnabled: boolean;
            originalIndex: number;
          } & types.TableID;
        }>((dictionary, columnInfo) => {
          const dicKey = types.getDatabaseSpecificFullTableName(columnInfo);
          const thisTableInfo = common.getOrAddGeneric(
            dictionary,
            dicKey,
            // databaseTables[].arrayIndex,
            () => ({
              columns: [],
              originalIndex: (
                databaseTables[
                  getTablesDictionaryKeyForSchema(columnInfo.schemaName)
                ] || databaseTables[dicKey]
              ).arrayIndex,
              isCTEnabled: false,
              databaseName,
              schemaName: columnInfo.schemaName,
              tableName: columnInfo.tableName,
            }),
          );
          // For some reason, eslint thinks there is some unsafe access on columns.push
          // eslint-disable-next-line
          thisTableInfo.columns.push({
            name: columnInfo.columnName,
            isPrimaryKey: columnInfo.isPrimaryKey,
            columnType: columnInfo.columnType,
          });
          thisTableInfo.isCTEnabled = columnInfo.tableTrackedByCT;
          return dictionary;
        }, {});
      }),
    );

    for (const dbSpecificInfo of allTableInfoFromDB) {
      for (const {
        columns,
        isCTEnabled,
        originalIndex,
        databaseName,
        schemaName,
        tableName,
      } of Object.values(dbSpecificInfo)) {
        // Sort columns PK first.
        columns.sort((x, y) =>
          x.isPrimaryKey === y.isPrimaryKey ? 0 : x.isPrimaryKey ? -1 : 1,
        );
        retVal.push({
          tableID: {
            databaseName,
            schemaName,
            tableName,
          },
          tableMD: {
            columnNames: columns.map(({ name }) => name),
            columnTypes: columns.map(({ columnType }) => columnType),
            primaryKeyColumnCount: columns.findIndex(
              ({ isPrimaryKey }) => !isPrimaryKey,
            ),
            isCTEnabled,
          },
          originalIndex,
        });
      }
    }
  }

  return retVal;
};

export const readFullTable = async ({
  connection,
  tableID, // We assume db name, schema name, and table name have all been previously validate not to include sql injections
  tableMD: { columnNames },
  outputArray,
  rowProcessor,
  onQueryEnd,
}: types.RowProcessingOptions) => {
  const curCTVersion = await api.getQuerySingleValue({
    connection,
    sqlCommand: `USE [${tableID.databaseName}]; SELECT CHANGE_TRACKING_CURRENT_VERSION()`,
    onRow: (value) => validation.decodeOrThrow(t.string.decode, value),
  });

  const colCount = columnNames.length;
  await api.streamQuery({
    connection,
    sqlCommand: `SELECT * FROM [${tableID.databaseName}].[${tableID.schemaName}].[${tableID.tableName}]`,
    onRow: (row, controlFlow) => {
      for (let i = 0; i < colCount; ++i) {
        outputArray[i] = row[i];
      }
      rowProcessor(undefined, undefined, controlFlow);
    },
    onDone: onQueryEnd,
  });

  return curCTVersion;
};

interface SimpleDataReadingOptions {
  connection: types.MSSQLConnection;
  tableID: types.TableID;
}

export const enableChangeTracking = async ({
  connection,
  tableID: {
    // We assume db name, schema name, and table name have all been previously validate not to include sql injections
    databaseName,
    schemaName,
    tableName,
  },
}: SimpleDataReadingOptions) => {
  await api.executeStatementNoResults({
    connection,
    sqlCommand: `ALTER TABLE [${databaseName}].[${schemaName}].[${tableName}]
  ENABLE CHANGE_TRACKING
  WITH (TRACK_COLUMNS_UPDATED = OFF)`,
  });
};

export const getMinValidTrackingVersion = async ({
  connection,
  tableID: {
    // We assume db name, schema name, and table name have all been previously validate not to include sql injections
    databaseName,
    schemaName,
    tableName,
  },
}: SimpleDataReadingOptions) =>
  (await api.getQuerySingleValue({
    connection,
    sqlCommand: `USE [${databaseName}]; SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@tableID))`,
    onRow: (val) =>
      validation.decodeOrThrow(t.union([t.string, t.null]).decode, val), // BIGINTs are strings in mssql as JS's number can not hold all values of 64bit integer
    prepareRequest: (request) => {
      request.input("tableID", `[${schemaName}].[${tableName}]`);
      return request;
    },
  })) || undefined;

export const readTableWithChangeTracking = async ({
  connection,
  tableID, // We assume db name, schema name, and table name have all been previously validate not to include sql injections
  tableMD: { columnNames, primaryKeyColumnCount: pkCount },
  changeTracking,
  outputArray,
  rowProcessor,
  onQueryEnd,
}: types.RowProcessingOptions) => {
  const extraColCount = 3; // CT version, operation, transaction time
  let maxCTVersion: bigint | null = null;
  await api.streamQuery({
    connection,
    sqlCommand: `USE [${
      tableID.databaseName
    }]; SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, FORMAT(tc.COMMIT_TIME, 'yyyy-MM-ddTHH:mm:ss.fff'), ${columnNames
      .map((colName) => `t.[${colName}]`)
      .join(", ")}
FROM CHANGETABLE(CHANGES [${tableID.schemaName}].[${
      tableID.tableName
    }],${changeTracking}) ct
  LEFT JOIN [${tableID.schemaName}].[${
      tableID.tableName
    }] t -- Left join in order to preserve information about deleted columns
    ON ${columnNames
      .slice(0, pkCount)
      .map((pkColName) => `t.[${pkColName}] = ct.[${pkColName}]`)
      .join(" AND ")}
LEFT JOIN sys.dm_tran_commit_table tc
  ON ct.SYS_CHANGE_VERSION = tc.COMMIT_TS`,
    onRow: (row, controlFlow) => {
      let rowStatus: api.RowStatus = undefined;
      switch (row[1]) {
        case "D":
          // Deletion: clear all other except last one (deletion time)
          outputArray.fill(null, extraColCount + pkCount);
          rowStatus = "deleted";
          break;
        case "I":
        case "U":
          for (let i = pkCount; i < columnNames.length; ++i) {
            outputArray[i] = row[i + extraColCount];
          }
          break;
        default:
          rowStatus = "invalid";
          break;
      }

      // Always set primary key columns
      for (let i = 0; i < pkCount; ++i) {
        outputArray[i] = row[i + extraColCount];
      }
      const curCTVersion = BigInt(row[0]);
      if (maxCTVersion === null || curCTVersion > maxCTVersion) {
        maxCTVersion = curCTVersion;
      }
      rowProcessor(rowStatus, row[2] as string, controlFlow);
    },
    onDone: onQueryEnd,
  });

  return (maxCTVersion as bigint | null)?.toString() || changeTracking; // Some compiler bug makes this always null at this point
};
