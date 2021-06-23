import test, { ExecutionContext } from "ava";
import * as spec from "..";
import * as commonSql from "@data-heaving/common-sql";

test("The createRowIteratingPipelineFactory method works as intended", async (t) => {
  const data = [[0], [1], [2]];
  const connection = createTestConnectionAbstraction(t, data);
  const input = "Input";
  const sql = "SELECT 1";
  const logs: Array<string> = [];
  const errors: Array<string> = [];
  const pipelineFactory = spec.createRowIteratingPipelineFactory(
    (input: string) => ({ input }),
    {
      pool: "Dummy",
      getConnection: (pool, eventEmitter) =>
        Promise.resolve([connection(sql, undefined), () => Promise.resolve()]),
    },
    spec
      .consoleLoggingTableExportWithChangeTrackingEventEmitterBuilder(
        (tableID: string) => tableID,
        undefined,
        undefined,
        undefined,
        {
          log: (msg) => logs.push(msg),
          error: (msg) => errors.push(msg),
        },
      )
      .createEventEmitter(),
    (ctx, connection, getCurrentStoring, endOrReset) => {
      return Promise.resolve();
    },
  );
  const pipeline = pipelineFactory(() => (datumStoringFactory) => ({
    storing: {
      processor: (datum) => {},
      end: () => {},
    },
  }));
  await pipeline(input);

  t.deepEqual(logs, []);
  t.deepEqual(errors, []);
});

function createTestConnectionAbstraction<
  TData extends ReadonlyArray<ReadonlyArray<unknown>>
>(
  t: ExecutionContext,
  data: TData,
): (
  expectedSQL: string,
  streamQueryErrorBehaviour?: {
    whenToReturnError: "beforeData" | "beforeDone" | "beforeReturn";
    errorToReturn: unknown;
  },
) => commonSql.SQLConnectionReaderAbstraction<string, string> {
  return (expectedSQL, streamQueryErrorBehaviour) => ({
    createRequest: (sql) => {
      t.deepEqual(expectedSQL, sql);
      return sql;
    },
    defaultPrepareRequest: (sql) => sql,
    streamQuery: (options) => {
      try {
        t.is(options.request, options.sqlCommand);
        if (streamQueryErrorBehaviour?.whenToReturnError === "beforeData") {
          return Promise.resolve(streamQueryErrorBehaviour?.errorToReturn);
        }
        for (const datum of data) {
          options.onRow(datum, undefined);
        }
        if (streamQueryErrorBehaviour?.whenToReturnError === "beforeDone") {
          return Promise.resolve(streamQueryErrorBehaviour?.errorToReturn);
        }

        return Promise.resolve(
          streamQueryErrorBehaviour?.whenToReturnError === "beforeReturn"
            ? streamQueryErrorBehaviour?.errorToReturn
            : undefined,
        );
      } finally {
        options.onDone?.([data.length]);
      }
    },
  });
}
