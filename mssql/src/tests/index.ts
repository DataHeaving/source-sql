import * as abi from "../tests-setup/interface";

abi.defineSQLServerTest("Simple full load", abi.performSQLFullLoadTest());

abi.defineSQLServerTest(
  "Incremental load with change tracking column",
  async (t) => {
    const storage = abi.createInMemoryStorage<number>();
    // First load will be full
    await abi.performSQLIncrementalLoadTest(storage)(t);
    // The subsequent load must be empty
    await abi.performSQLIncrementalLoadTest(storage, {
      previousChangeTrackingVersion: abi.TABLE_DATA.reduce(
        (max, [id]) => Math.max(max, id),
        0,
      ),
      startIndex: 0,
      elementCount: 0,
    })(t);
  },
);

abi.defineSQLServerTest(
  "Incremental load with native change tracking functionality",
  async (t) => {
    const storage = abi.createInMemoryStorage<string>();
    // First load will be full
    await abi.performSQLIncrementalLoadWithNativeCTTest(storage, "0")(t);
    // The subsequent load must be empty
    await abi.performSQLIncrementalLoadWithNativeCTTest(storage, "0", {
      previousChangeTrackingVersion: JSON.parse(
        (await storage.readExistingData()) as string,
      ) as string,
      startIndex: 0,
      elementCount: 0,
    })(t);
  },
);
