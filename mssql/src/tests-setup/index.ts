import test, { TestInterface } from "ava";
import { env } from "process";
import * as common from "@data-heaving/common";
import * as commonSql from "@data-heaving/common-sql";
import * as testSupport from "@data-heaving/common-test-support";
import * as abi from "./interface";
import * as mssql from "mssql";
import * as mssqlSource from "..";

const thisTest = test as TestInterface<abi.SQLServerTestContext>;

thisTest.before("Start SQL Server Container", async (t) => {
  const username = "sa"; // I think this is hard-coded into SQL Server (container)
  const password = env.SQL_SERVER_PASSWORD || "yourStrong(!)Password";
  const port = 1433;
  const {
    containerID,
    containerHostName,
    checkIsReady,
  } = await testSupport.startContainerAsync({
    image: "mcr.microsoft.com/mssql/server:2019-CU10-ubuntu-20.04",
    containerPorts: [
      {
        containerPort: port,
        checkReadyness: async (host, port) => {
          try {
            console.log("Attempting to connect to server...", host, port); // eslint-disable-line no-console
            await prepareServer({
              host,
              port,
              username,
              password,
            });
          } catch (e) {
            if (!(e instanceof mssql.MSSQLError) || e.code !== "ESOCKET") {
              console.log("SQL Server still in recovery state...", e); // eslint-disable-line no-console
            }
            throw e;
          }
        },
      },
    ],
    containerEnvironment: {
      ACCEPT_EULA: "Y",
      SA_PASSWORD: password,
    },
    networkName: env.SQL_SERVER_DOCKER_NW,
  });
  t.context.containerID = containerID;
  t.context.sqlServerInfo = {
    host: containerHostName,
    port: port,
    username,
    password,
  };

  while (!(await checkIsReady())) {
    await common.sleep(1000);
  }
});

thisTest.serial.beforeEach(async (t) => {
  await prepareDatabase(t.context.sqlServerInfo);
});

thisTest.after.always("Shut down SQL Server Container", async (t) => {
  await testSupport.stopContainerAsync(t.context.containerID);
});

const prepareServer = async (serverInfo: abi.SQLServerInfo) => {
  const connection = await new mssql.ConnectionPool(
    abi.getSQLConfigFromContext(serverInfo),
  ).connect();
  try {
    await connection.query("SELECT 1");
  } finally {
    await connection.close();
  }
};

const prepareDatabase = async (serverInfo: abi.SQLServerInfo) => {
  const connection = await new mssql.ConnectionPool(
    abi.getSQLConfigFromContext(serverInfo),
  ).connect();
  try {
    await connection.query(
      `DROP DATABASE IF EXISTS ${abi.TABLE_ID.databaseName}`,
    );
    await connection.query(`CREATE DATABASE ${abi.TABLE_ID.databaseName}`);
    await connection.query(DB_PREPARATION_SQL);
  } finally {
    await connection.close();
  }
};

const getTypeModifiers = (typeInfo: commonSql.ColumnTypeInfo) => {
  switch (typeInfo.typeName.toUpperCase()) {
    case "VARCHAR":
    case "NVARCHAR":
      return `(${typeInfo.maxLength > 0 ? typeInfo.maxLength : "MAX"})`;
    case "INT":
    case "BIGINT":
      return "";
    default:
      return typeInfo.precision > 0
        ? typeInfo.scale > 0
          ? `(${typeInfo.precision}, ${typeInfo.scale})`
          : `(${typeInfo.precision})`
        : "";
  }
};

const DB_PREPARATION_SQL = `
ALTER DATABASE ${abi.TABLE_ID.databaseName}  
SET CHANGE_TRACKING = ON  
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

CREATE TABLE ${mssqlSource.getFullTableName(
  abi.TABLE_ID,
)}(${abi.TABLE_MD.columnNames
  .map((columnName, idx) => {
    const typeInfo = abi.TABLE_MD.columnTypes[idx];
    return `${columnName} ${typeInfo.typeName}${getTypeModifiers(typeInfo)} ${
      typeInfo.isNullable ? "NULL" : "NOT NULL"
    }`;
  })
  .join(",\n")},
  PRIMARY KEY (${abi.TABLE_MD.columnNames
    .filter((c, idx) => idx < abi.TABLE_MD.primaryKeyColumnCount)
    .map((c) => c)
    .join(", ")})
);

INSERT INTO ${mssqlSource.getFullTableName(
  abi.TABLE_ID,
)}(${abi.TABLE_MD.columnNames.join(", ")}) VALUES
${abi.TABLE_DATA.map(([idCol, dataCol]) => `(${idCol}, '${dataCol}')`).join(
  ",\n",
)};`;
