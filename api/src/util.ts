import * as utils from "@data-heaving/common";

export type SQLTracer =
  | ((sql: string, start: "started" | "ended") => void)
  | undefined;

export interface SQLConnectionPoolAbstraction<TPool, TConnection> {
  pool: TPool;
  getConnection: (
    pool: TPool,
    sqlTracer: SQLTracer | undefined,
  ) => Promise<[TConnection, CloseConnection]>;
}
export type CloseConnection = () => Promise<void>;

export const useConnectionPoolAsync = async <TPool, TConnection, T>(
  { pool, getConnection }: SQLConnectionPoolAbstraction<TPool, TConnection>,
  sqlTracer: SQLTracer | undefined,
  use: (connection: TConnection) => Promise<T>,
) => {
  let connection: [TConnection, CloseConnection] | undefined = undefined;
  try {
    connection = await getConnection(pool, sqlTracer);
    return await use(connection[0]);
  } finally {
    if (connection) {
      await connection[1]();
    }
  }
};

export interface SQLConnectionAbstraction<TIntermediateRequest, TFinalRequest> {
  createRequest: (sqlCommand: string) => TIntermediateRequest;
  defaultPrepareRequest: (req: TIntermediateRequest) => TFinalRequest;
  streamQuery: (
    options: QueryExecutionParametersWithRequest<TFinalRequest>,
    controlFlow: utils.ControlFlow | undefined,
  ) => Promise<unknown>;
}
export type RowTransformer<T, TRow = ReadonlyArray<unknown>> = (
  row: TRow,
  controlFlow: utils.ControlFlow | undefined,
) => T;

export interface QueryExecutionParametersBase<
  TResult = unknown,
  TRow = ReadonlyArray<unknown>
> {
  sqlCommand: string;
  onRow: RowTransformer<TResult, TRow>;
  onDone?: (rowsAffected: Array<number>) => unknown;
  // onRowMD?: (md: ReadonlyArray<sql.IColumnMetadata[string]>) => void;
  // sqlTracer: SQLTracer;
}

export type QueryExecutionParametersWithRequest<
  TFinalRequest
> = QueryExecutionParametersBase & {
  request: TFinalRequest;
};
export type QueryExecutionParameters<
  TIntermediateRequest,
  TFinalRequest,
  TResult = unknown,
  TRow = ReadonlyArray<unknown>
> = QueryExecutionParametersBase<TResult, TRow> & {
  connection: SQLConnectionAbstraction<TIntermediateRequest, TFinalRequest>;
  prepareRequest?: (request: TIntermediateRequest) => TFinalRequest;
};

export type QueryExecutionParametersSingle<
  TIntermediateRequest,
  TFinalRequest,
  TResult
> = QueryExecutionParameters<
  TIntermediateRequest,
  TFinalRequest,
  TResult,
  unknown
> & {
  strict?: boolean;
};
export const executeStatementNoResults = <TIntermediateRequest, TFinalRequest>(
  opts: Omit<
    QueryExecutionParameters<TIntermediateRequest, TFinalRequest>,
    "onRow"
  >,
) =>
  streamQuery({
    ...opts,
    onRow: () => {},
  });

export const getQuerySingleValue = async <
  TIntermediateRequest,
  TFinalRequest,
  TResult
>(
  opts: QueryExecutionParametersSingle<
    TIntermediateRequest,
    TFinalRequest,
    TResult
  >,
) => {
  let retVal: TResult | undefined = undefined;
  let resultSet = false;
  await streamQuery({
    ...opts,
    onRow: (...args) => {
      if (resultSet && opts.strict === true) {
        throw new Error("Expected exactly one row but got more.");
      } else {
        resultSet = true;
      }
      retVal = opts.onRow(args[0][0], args[1]);
    },
  });

  if (!resultSet) {
    throw new Error("Query produced no rows.");
  }

  return (retVal as unknown) as TResult;
};

export const getQuerySingleRow = async <
  TIntermediateRequest,
  TFinalRequest,
  TResult
>(
  opts: QueryExecutionParametersSingle<
    TIntermediateRequest,
    TFinalRequest,
    TResult
  >,
) => {
  let retVal: TResult | undefined = undefined;
  let resultSet = false;
  await streamQueryResults({
    ...opts,
    onRow: (...args) => {
      if (resultSet && opts.strict === true) {
        throw new Error("Expected exactly one row but got more.");
      } else {
        resultSet = true;
      }
      retVal = opts.onRow(args[0][1], args[1]);
    },
  });

  if (!resultSet) {
    throw new Error("Query produced no rows.");
  }

  return retVal;
};

export const streamQueryResults = async <
  TIntermediateRequest,
  TFinalRequest,
  TResult
>(
  opts: QueryExecutionParameters<TIntermediateRequest, TFinalRequest, TResult>,
) => {
  const values: TResult[] = [];
  await streamQuery({
    ...opts,
    onRow: (row, controlFlow) => values.push(opts.onRow(row, controlFlow)),
  });
  return values;
};

export const streamQuery = <TIntermediateRequest, TFinalRequest>(
  opts: QueryExecutionParameters<TIntermediateRequest, TFinalRequest>,
) => {
  const request = (
    opts.prepareRequest ?? opts.connection.defaultPrepareRequest
  )(opts.connection.createRequest(opts.sqlCommand));
  return streamQueryOnRequest(
    { ...opts, request },
    opts.connection.streamQuery,
  );
};

export const streamQueryOnRequest = async <TFinalRequest>(
  {
    // sqlTracer,
    sqlCommand,
    onRow,
    onDone,
    // onRowMD,
    request,
  }: QueryExecutionParametersWithRequest<TFinalRequest>,
  streamQuery: SQLConnectionAbstraction<unknown, TFinalRequest>["streamQuery"],
) => {
  // const error: unknown = undefined;
  let pauseCount = 0;
  const pause: unknown = (request as any).pause; // eslint-disable-line
  const resume: unknown = (request as any).resume; // eslint-disable-line
  const controlFlow =
    typeof pause === "function" && typeof resume === "function"
      ? {
          pause: () => {
            if (pauseCount === 0) {
              pause.apply(request);
            }
            ++pauseCount;
          },
          resume: () => {
            if (pauseCount > 0) {
              --pauseCount;
              if (pauseCount === 0) {
                resume.apply(request);
              }
            }
          },
        }
      : undefined;

  const error = await streamQuery(
    { sqlCommand, onRow, onDone, request },
    controlFlow,
  );

  if (error) {
    throw error instanceof Error ? error : new Error(`${error}`);
  }
};
