import * as common from "@data-heaving/common";
import * as commonSql from "@data-heaving/common-sql";
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

abi.defineSQLServerTest("Metadata extraction works as intended", async (t) => {
  const eventBuilder = new common.EventEmitterBuilder<commonSql.VirtualSQLEvents>();
  const recordedEvents: Array<
    abi.TRecordedEvent<commonSql.VirtualSQLEvents>
  > = [];
  for (const eventName of [
    "sqlExecutionStarted",
    "sqlExecutionEnded",
  ] as const) {
    eventBuilder.addEventListener(eventName, (eventArg) => {
      recordedEvents.push({ eventName, eventArg });
    });
  }
  const dataSource = functionality.tableMetaDataInServer(
    abi.getMSSQLPoolFromTestContext(t),
    eventBuilder.createEventEmitter(),
  );
  const dataSink: Array<functionality.TableAndMetadata> = [];
  let seenContext:
    | ReadonlyArray<functionality.TableAndMetadata>
    | undefined = undefined;
  const expectedContext: Array<functionality.TableAndMetadata> = [
    {
      tableID: abi.TABLE_ID,
      tableMD: abi.TABLE_MD,
    },
  ];
  await dataSource(() => (context) => {
    seenContext = context;
    t.deepEqual(context, expectedContext);
    return {
      storing: {
        processor: (row) => {
          dataSink.push(row);
        },
        end: () => {},
      },
    };
  })({
    defaults: {
      databaseName: abi.TABLE_ID.databaseName,
    },
    data: {
      schemas: [abi.TABLE_ID.schemaName],
    },
  });
  const sql = `SELECT
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
FROM [dh_test_db].sys.columns c
  JOIN [dh_test_db].sys.tables t ON c.object_id = t.object_id
  JOIN [dh_test_db].sys.types tt ON c.system_type_id = tt.system_type_id AND c.user_type_id = tt.user_type_id
  JOIN [dh_test_db].sys.schemas s ON t.schema_id = s.schema_id
  LEFT JOIN [dh_test_db].sys.indexes i ON t.object_id = i.object_id AND i.is_primary_key = 1
  LEFT JOIN [dh_test_db].sys.index_columns ic ON ic.object_id = t.object_id AND i.index_id = ic.index_id AND c.column_id = ic.column_id
  LEFT JOIN [dh_test_db].sys.change_tracking_tables ct ON t.object_id = ct.object_id
WHERE
  (s.name = @param_0)`;

  t.deepEqual(recordedEvents, [
    {
      eventName: "sqlExecutionStarted",
      eventArg: sql,
    },
    {
      eventName: "sqlExecutionEnded",
      eventArg: sql,
    },
  ]);

  t.truthy(seenContext);
  t.deepEqual(dataSink, expectedContext);
});
