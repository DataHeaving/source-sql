import * as abi from "../tests-setup/interface";
import * as functionality from "..";
import * as mssql from "mssql";

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

abi.defineSQLServerTest(
  "Incremental load with native change tracking retains primary key columns for deleted rows",
  async (t) => {
    const storage = abi.createInMemoryStorage<string>();
    // First load will be full
    await abi.performSQLIncrementalLoadWithNativeCTTest(storage, "0")(t);

    // Then perform deletion
    const connection = await new mssql.ConnectionPool(
      abi.getSQLConfigFromContext(t.context.sqlServerInfo),
    ).connect();
    let deletionTime: Date;
    try {
      await connection.query(
        `DELETE FROM ${functionality.getFullTableName(abi.TABLE_ID)}
    WHERE ${abi.TABLE_MD.columnNames[0]} = ${abi.TABLE_DATA[0][0]}`,
      );
      deletionTime = (((
        await connection.query(
          `SELECT COMMIT_TIME FROM ${abi.TABLE_ID.databaseName}.sys.dm_tran_commit_table`,
        )
      ).recordset[0] as unknown) as { COMMIT_TIME: Date }).COMMIT_TIME;
    } finally {
      await connection.close();
    }
    await abi.performDeletionInDB(t);

    // Then make sure CT load sees correct data
    await abi.performSQLIncrementalLoadWithNativeCTTest(storage, "1", {
      previousChangeTrackingVersion: JSON.parse(
        (await storage.readExistingData()) as string,
      ) as string,
      startIndex: 0,
      elementCount: 1,
      deletionTime,
    })(t);
  },
);
