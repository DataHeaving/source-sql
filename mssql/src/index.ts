// import * as api from "@data-heaving/source-sql";
// import * as types from "./types";
// import * as util from "./source";
// import * as read from "./read";
// import * as common_validation from "@data-heaving/common-validation";
// import * as common from "@data-heaving/common";

export * from "./streamRows";
export * from "./types";
export * from "./pool";

// export interface MSSQLSourceOptions {
//   config: types.MainConfig;
//   eventEmitter?: api.SourceTableEventEmitter<types.TableID, string>;
//   // metaDataStorage: common.ObjectStorageFunctionality<api.SerializedExportedTablesMetaData>;
// }

// const mssqlSource = (opts: MSSQLSourceOptions) => ({
//   withMSSQLChangeTracking: (
//     storage: api.TableSpecificStorage<types.TableID, string>,
//   ) =>
//     finalSource(opts, {
//       validation: common_validation.nonEmptyString,
//       storage,
//       checkValidity: async ({
//         connection,
//         tableID,
//         tableMD,
//         previousChangeTracking,
//         additionalInfo,
//       }) => {
//         // See https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/change-tracking-min-valid-version-transact-sql
//         let isValid = false;
//         let changeTrackingVersion: string | undefined = undefined;
//         const isCTAlreadyEnabled = tableMD.isCTEnabled;
//         if (isCTAlreadyEnabled && (previousChangeTracking || "").length > 0) {
//           changeTrackingVersion = await read.getMinValidTrackingVersion({
//             connection,
//             tableID,
//           });
//           isValid =
//             !!changeTrackingVersion &&
//             BigInt(previousChangeTracking) >= BigInt(changeTrackingVersion);
//           if (isValid) {
//             changeTrackingVersion = previousChangeTracking;
//           } else {
//             changeTrackingVersion = undefined;
//           }
//         } else if (
//           !isCTAlreadyEnabled &&
//           !(
//             common.getOrDefault(
//               additionalInfo?.dontAutoEnableChangeTracking,
//               opts.config.defaults?.dontAutoEnableChangeTracking,
//             ) === true
//           )
//         ) {
//           await read.enableChangeTracking({ connection, tableID });
//           changeTrackingVersion = undefined;
//         }

//         return changeTrackingVersion;
//       },
//       streamRows: read.readTableWithChangeTracking,
//     }),
//   withNoChangeTracking: () => finalSource(opts, undefined),
// });

// export default mssqlSource;

// export * from "./types";

// const finalSource = (
//   { config, eventEmitter }: MSSQLSourceOptions,
//   changeTrackingFunctionality:
//     | api.ChangeTrackingFunctionality<
//         types.MSSQLConnection,
//         types.TableID,
//         string,
//         types.AdditionalInfo
//       >
//     | undefined,
// ) => {
//   return api.sqlSource({
//     eventEmitter,
//     getSQLPool: () => util.getMSSQLPool(config.connection),
//     getAllTables: async (connection) => [
//       ...(await util.getExplicitTables(config, connection)),
//       ...(await util.getTablesOfSchemas(config, connection)),
//     ],
//     // getTableIDString: types.getFullTableName,
//     streamRows: read.readFullTable,
//     changeTrackingFunctionality,
//     // mdFunctionality: metaDataStorage,
//   });
// };
