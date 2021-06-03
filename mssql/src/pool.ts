import * as mssql from "mssql";
import * as types from "./types";

declare module "mssql" {
  interface Request {
    arrayRowMode: boolean; // For some reason this is not exposed in mssql typings
  }
}

export function getMSSQLPool(config: mssql.config): types.MSSQLConnectionPool {
  return {
    pool: new mssql.ConnectionPool({
      ...config,
      options: {
        ...config.options,
        encrypt: true,
        enableArithAbort: true, // Good to specify, and also to avoid warning: tedious deprecated The default value for `config.options.enableArithAbort` will change from `false` to `true` in the next major version of `tedious`. Set the value to `true` or `false` explicitly to silence this message. node_modules/mssql/lib/tedious/connection-pool.js:61:23
      },
    }),
    getConnection: async (pool, eventEmitter) => {
      const connection = await pool.connect();
      return [
        {
          createRequest: () => new mssql.Request(connection),
          defaultPrepareRequest: (req) => req,
          streamQuery: async (
            { request, onRow, onDone, sqlCommand },
            controlFlow,
          ) => {
            let error: unknown = undefined;
            request.stream = true;
            request.arrayRowMode = true;
            // request.on(
            //   "recordset",
            //   (columns: ReadonlyArray<sql.IColumnMetadata[string]>) => {
            //     onRowMD?.(columns);
            //   },
            // );
            request.on(
              "row",
              (
                row: ReadonlyArray<mssql.IResult<unknown>["output"][string]>,
              ) => {
                onRow(row, controlFlow);
              },
            );
            request.on("error", (seenError: unknown) => {
              error = seenError;
            });
            request.on(
              "done",
              (done: {
                rowsAffected: mssql.IResult<unknown>["rowsAffected"];
              }) => {
                onDone?.(done.rowsAffected);
              },
            );

            eventEmitter?.emit("sqlExecutionStarted", sqlCommand);
            await request.query(sqlCommand);
            eventEmitter?.emit("sqlExecutionEnded", sqlCommand);

            return error;
          },
        },
        () => connection.close(),
      ];
    },
  };
}
