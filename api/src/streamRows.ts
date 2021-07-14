import * as common from "@data-heaving/common";
import * as sql from "@data-heaving/common-sql";

export type StreamSQLRows<
  TConnection,
  TTableID,
  TChangeTrackingDatum,
  TAdditionalInfo,
  TResult
> = (opts: {
  connection: TConnection;
  tableID: TTableID;
  tableMD: sql.TableMetaData;
  changeTracking: TChangeTrackingDatum | undefined;
  outputArray: Array<unknown>;
  rowProcessor: (
    rowStatus: RowStatus,
    transactionTime: Date | undefined | null,
    controlFlow: common.ControlFlow | undefined,
  ) => unknown;
  onQueryEnd: () => unknown;
  additionalInfo: TAdditionalInfo;
}) => Promise<TResult>;
export type RowStatus = "deleted" | "invalid" | undefined;

export function createRowIteratingPipelineFactory<
  TInput,
  TContext,
  TPool,
  TIntermediateRequest,
  TFinalRequest,
  TReturnValue
  // TPreparationValue
>(
  getContext: (input: TInput) => TContext,
  connectionPool: sql.SQLConnectionPoolAbstraction<
    TPool,
    sql.SQLConnectionReaderAbstraction<TIntermediateRequest, TFinalRequest>
  >,
  eventEmitter: sql.SQLEventEmitter,
  useConnection: (
    context: TContext,
    connection: sql.SQLConnectionReaderAbstraction<
      TIntermediateRequest,
      TFinalRequest
    >,
    getCurrentStoring: () => common.DatumStoring<sql.TSQLRow>,
    endOrReset: () => void,
    // preparationResult: TPreparationValue,
  ) => Promise<TReturnValue>,
  // prepare: () => Promise<TPreparationValue>,
  afterSuccessfulRun?: (
    context: TContext,
    // preparationResult: TPreparationValue,
    retVal: TReturnValue,
  ) => Promise<unknown>,
): common.TPipelineFactory<TInput, TContext, sql.TSQLRow> {
  return (datumStoringFactory) => {
    return async (input) => {
      const createStoring = datumStoringFactory();
      const allPromises: Array<Promise<unknown>> = [];
      const errors: Array<unknown> = [];
      let retVal: TReturnValue | undefined = undefined;
      const context = getContext(input);
      // const preparationValue = await prepare();
      try {
        let storing: common.DatumStoring<sql.TSQLRow> | undefined = undefined;
        const resetSignal = () => {
          storing?.end();
          storing = undefined;
        };
        retVal = await sql.useConnectionPoolAsync(
          connectionPool,
          eventEmitter,
          async (connection) => {
            const getCurrentStoring = () => {
              if (!storing) {
                let promise: Promise<unknown> | undefined = undefined;
                ({ storing, promise } = createStoring(context, resetSignal));
                if (promise) {
                  allPromises.push(promise);
                }
              }
              return storing;
            };
            return await useConnection(
              context,
              connection,
              getCurrentStoring,
              resetSignal,
            );
          },
        );
      } catch (e) {
        errors.push(e);
      }

      try {
        await Promise.all(allPromises);
      } catch (e) {
        errors.push(e);
      }

      if (errors.length > 0) {
        throw new MultipleErrors(errors);
      }

      if (afterSuccessfulRun) {
        await afterSuccessfulRun(context, retVal!); // eslint-disable-line @typescript-eslint/no-non-null-assertion
      }
    };
  };
}

export class MultipleErrors extends Error {
  public constructor(public readonly errors: ReadonlyArray<unknown>) {
    super(`Multiple errors occurred: ${errors.join("\n")}`);
  }
}
