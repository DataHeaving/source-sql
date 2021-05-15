import * as common from "@data-heaving/common";
import * as validation from "@data-heaving/common-validation";
import * as sql from "@data-heaving/common-sql";
import * as t from "io-ts";

export function createRowIteratingPipelineFactory<
  TContext,
  TPool,
  TIntermediateRequest,
  TFinalRequest,
  TReturnValue
  // TPreparationValue
>(
  connectionPool: sql.SQLConnectionPoolAbstraction<
    TPool,
    sql.SQLConnectionReaderAbstraction<TIntermediateRequest, TFinalRequest>
  >,
  eventEmitter: sql.SQLEventEmitter,
  contextFactory: (
    connectionEstablishmentTime: Date,
    // preparationResult: TPreparationValue,
  ) => TContext,
  useConnection: (
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
    // preparationResult: TPreparationValue,
    retVal: TReturnValue,
  ) => Promise<unknown>,
): common.TPipelineFactory<TContext, sql.TSQLRow> {
  return (datumStoringFactory) => {
    return async () => {
      const createStoring = datumStoringFactory();
      const allPromises: Array<Promise<unknown>> = [];
      const errors: Array<unknown> = [];
      let retVal: TReturnValue | undefined = undefined;
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
            const connectionEstablishmentTime = new Date();
            const getCurrentStoring = () => {
              if (!storing) {
                let promise: Promise<unknown> | undefined = undefined;
                ({ storing, promise } = createStoring(
                  contextFactory(connectionEstablishmentTime),
                  resetSignal,
                ));
                if (promise) {
                  allPromises.push(promise);
                }
              }
              return storing;
            };
            return await useConnection(
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
        throw new sql.MultipleErrors(errors);
      }

      if (afterSuccessfulRun) {
        await afterSuccessfulRun(retVal!); // eslint-disable-line @typescript-eslint/no-non-null-assertion
      }
    };
  };
}

export interface ChangeTrackingFunctionalityV2<
  TConnection,
  TChangeTrackingDatum
> {
  validation: t.Type<TChangeTrackingDatum>;
  checkValidity: (opts: {
    connection: TConnection;
    previousChangeTracking: TChangeTrackingDatum | undefined;
  }) => Promise<TChangeTrackingDatum | undefined>;
  storage: common.ObjectStorageFunctionality<TChangeTrackingDatum>;
}

export const prepareChangeTracking = async <TConnection, TChangeTrackingDatum>(
  changeTrackingFunctionality: ChangeTrackingFunctionalityV2<
    TConnection,
    TChangeTrackingDatum
  >,
  connection: TConnection,
) => {
  const ctStorage = changeTrackingFunctionality.storage;
  const previousChangeTrackingVersion = await validation.retrieveValidatedDataFromStorage(
    ctStorage.readExistingData,
    changeTrackingFunctionality.validation.decode,
  );
  const changeTrackingVersion = await changeTrackingFunctionality.checkValidity(
    {
      connection,
      previousChangeTracking: previousChangeTrackingVersion,
    },
  );
  return {
    changeTrackingFunctionality,
    ctStorage,
    previousChangeTrackingVersion,
    changeTrackingVersion,
  };
};
