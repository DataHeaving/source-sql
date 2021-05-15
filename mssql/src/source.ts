// import * as sql from "mssql";
// import * as types from "./types";
// import * as read from "./read";
// import * as common from "@data-heaving/common";

// declare module "mssql" {
//   interface Request {
//     arrayRowMode: boolean; // For some reason this is not exposed in mssql typings
//   }
// }

// export function getMSSQLPool(config: sql.config): types.MSSQLConnectionPool {
//   return {
//     pool: new sql.ConnectionPool({
//       ...config,
//       options: {
//         encrypt: true,
//         enableArithAbort: true, // Good to specify, and also to avoid warning: tedious deprecated The default value for `config.options.enableArithAbort` will change from `false` to `true` in the next major version of `tedious`. Set the value to `true` or `false` explicitly to silence this message. node_modules/mssql/lib/tedious/connection-pool.js:61:23
//       },
//     }),
//     getConnection: async (pool, sqlTracer) => {
//       const connection = await pool.connect();
//       return [
//         {
//           createRequest: () => new sql.Request(connection),
//           defaultPrepareRequest: (req) => req,
//           streamQuery: async (
//             { request, onRow, onDone, sqlCommand },
//             controlFlow,
//           ) => {
//             let error: unknown = undefined;
//             request.stream = true;
//             request.arrayRowMode = true;
//             // request.on(
//             //   "recordset",
//             //   (columns: ReadonlyArray<sql.IColumnMetadata[string]>) => {
//             //     onRowMD?.(columns);
//             //   },
//             // );
//             request.on(
//               "row",
//               (row: ReadonlyArray<sql.IResult<unknown>["output"][string]>) => {
//                 onRow(row, controlFlow);
//               },
//             );
//             request.on("error", (seenError: unknown) => {
//               error = seenError;
//             });
//             request.on(
//               "done",
//               (done: {
//                 rowsAffected: sql.IResult<unknown>["rowsAffected"];
//               }) => {
//                 onDone?.(done.rowsAffected);
//               },
//             );

//             sqlTracer?.(sqlCommand, "started");
//             await request.query(sqlCommand);
//             sqlTracer?.(sqlCommand, "ended");

//             return error;
//           },
//         },
//         () => connection.close(),
//       ];
//     },
//   };
// }

// export const getExplicitTables = async (
//   { defaults, data: { tables } }: types.MainConfig,
//   connection: types.MSSQLConnection,
// ) => {
//   const explicitTables =
//     tables?.tables
//       .map((tableName) =>
//         typeof tableName === "string"
//           ? { schemaName: tables.schemaName, tableName }
//           : tableName,
//       )
//       .map(({ schemaName, tableName, overrideDefaults }) => {
//         const schemaNameFinal = schemaName ?? tables?.schemaName;
//         if (!schemaNameFinal) {
//           throw new Error(
//             `No schema specified for table ${tableName}, and no default schema specified either.`,
//           );
//         }
//         return {
//           databaseName: tryGetDatabaseName(
//             overrideDefaults,
//             defaults,
//             () => `table [${schemaNameFinal}].[${tableName}]`,
//           ),
//           schemaName: schemaNameFinal,
//           tableName,
//           overrideDefaults,
//         };
//       }) || [];
//   return (await read.getTableColumnMetaData(connection, explicitTables)).map(
//     ({ tableMD, tableID, originalIndex }) => ({
//       tableMD,
//       tableID,
//       intermediateRowEventInterval:
//         common.getOrDefault(
//           explicitTables[originalIndex]?.overrideDefaults
//             ?.intermediateRowEventInterval,
//           defaults?.intermediateRowEventInterval,
//         ) || 0,
//       additionalInfo: explicitTables[originalIndex]?.overrideDefaults || {},
//     }),
//   );
// };

// export const getTablesOfSchemas = async (
//   { defaults, data: { schemas } }: types.MainConfig,
//   connection: types.MSSQLConnection,
// ) => {
//   const explicitSchemas =
//     schemas
//       ?.map((s) => (typeof s === "string" ? { schemaName: s } : s))
//       ?.map(({ schemaName, overrideDefaults }) => ({
//         databaseName: tryGetDatabaseName(
//           overrideDefaults,
//           defaults,
//           () => `schema [${schemaName}]`,
//         ),
//         schemaName,
//         overrideDefaults,
//       })) || [];
//   return (await read.getTableColumnMetaData(connection, explicitSchemas)).map(
//     ({ tableMD, originalIndex, tableID }) => ({
//       tableMD,
//       tableID,
//       intermediateRowEventInterval:
//         common.getOrDefault(
//           explicitSchemas[originalIndex]?.overrideDefaults
//             ?.intermediateRowEventInterval,
//           defaults?.intermediateRowEventInterval,
//         ) || 0,
//       additionalInfo: explicitSchemas[originalIndex]?.overrideDefaults || {},
//     }),
//   ); // Notice that we are creating a *copy* of object passed as first parameter to Object.assign, as otherwise we will end up overwriting same object multiple times and erroneus data.
// };

// const tryGetDatabaseName = (
//   overrideDefaults: types.MainConfig["defaults"],
//   globalDefaults: types.MainConfig["defaults"],
//   descriptorString: () => string,
// ) => {
//   const databaseName =
//     overrideDefaults?.databaseName ?? globalDefaults?.databaseName;
//   if (!databaseName) {
//     throw new Error(
//       `No DB specified for ${descriptorString()}, and no default DB specified either.`,
//     );
//   }
//   return databaseName;
// };
