import test from "ava";
import { execFile } from "child_process";
import { promisify } from "util";
import { Socket } from "net";
import * as vars from "./vars";
import * as common from "@data-heaving/common";
import * as commonSql from "@data-heaving/common-sql";
import { SocketConnectOpts } from "net";
import * as abi from "./interface";
import * as mssql from "mssql";
import * as mssqlSource from "..";

const execFileAsync = promisify(execFile);
const CONTAINER_CONTEXT_KEY = "containerID";

test.before("Start SQL Server Container", async (t) => {
  const isNetworkSpecified = vars.SQL_SERVER_DOCKER_NW.length > 0;
  const sqlServerPortString = vars.SQL_SERVER_DOCKER_PORT;
  const sqlServerEnv = {
    ACCEPT_EULA: "Y",
    SA_PASSWORD: vars.SQL_SERVER_PASSWORD,
  };
  const containerID = (
    await execFileAsync(
      "docker",
      [
        "run",
        "--rm",
        "--detach",
        ...(isNetworkSpecified ? ["--network", vars.SQL_SERVER_DOCKER_NW] : []),
        isNetworkSpecified ? "--expose" : "--publish",
        sqlServerPortString,
        ...Object.keys(sqlServerEnv).flatMap((envName) => ["--env", envName]),
        "mcr.microsoft.com/mssql/server:2019-CU10-ubuntu-20.04",
      ],
      {
        env: sqlServerEnv,
      },
    )
  ).stdout.trim(); // Remember to trim output so that trailing newline would not be included as part of container ID

  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
  (t.context as any)[CONTAINER_CONTEXT_KEY] = containerID;

  console.log("Started SQL Server container with ID", containerID); // eslint-disable-line no-console
  // Now, we must get container host name, if we had networking config specified
  const sqlServerHost = isNetworkSpecified
    ? await getSQLServerHostName(containerID)
    : "127.0.0.1";
  console.log("SQL Server host: ", sqlServerHost); // eslint-disable-line no-console
  const socket = new Socket();
  const sqlServerPort = Number.parseInt(vars.SQL_SERVER_DOCKER_PORT);

  let success = false;
  do {
    try {
      await connectAsync(socket, {
        host: sqlServerHost,
        port: sqlServerPort,
      });
      success = true;
      console.log("SQL Server is almost ready..."); // eslint-disable-line no-console
    } catch {
      console.log("Waiting for SQL Server to become ready..."); // eslint-disable-line no-console
      await common.sleep(1000);
    }
  } while (!success);

  const sqlServerInfo: abi.SQLServerInfo = {
    host: sqlServerHost,
    port: sqlServerPort,
    username: "sa", // I think this is hard-coded into SQL Server (container)
    password: vars.SQL_SERVER_PASSWORD,
  };

  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
  (t.context as any)[abi.CONTEXT_KEY] = sqlServerInfo;

  // Prepare the DB
  // Notice that on first time, we will typically get "Logon Login failed for user 'SA'. Reason: An error occurred while evaluating the password." into SQL Server logs.
  // This is related to some timing issue, and just retrying again later is sufficient.
  // There is an issue about it https://github.com/microsoft/mssql-docker/issues/55 , but this happens even on 'normal' passwords.
  success = false;
  do {
    try {
      await prepareDatabase(sqlServerInfo);
      success = true;
    } catch (e) {
      if (e instanceof mssql.ConnectionError) {
        console.log("SQL Server still in recovery state..."); // eslint-disable-line no-console
        await common.sleep(1000);
      } else {
        throw e;
      }
    }
  } while (!success);
});

test.after.always("Shut down SQL Server Container", async (t) => {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
  const containerID = (t.context as any)[CONTAINER_CONTEXT_KEY] as string;
  if (containerID) {
    await execFileAsync("docker", ["rm", "-f", containerID]);
    console.log("Removed SQL Server container", containerID); // eslint-disable-line no-console
  }
});

const getSQLServerHostName = async (containerID: string) => {
  return (
    await execFileAsync("docker", [
      "inspect",
      "--format",
      "{{range .NetworkSettings.Networks}}{{range .Aliases}}{{.}}{{end}}{{end}}",
      containerID,
    ])
  ).stdout.trim();
};

const connectAsync = (socket: Socket, opts: SocketConnectOpts) =>
  new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", (e) => {
      socket.removeListener("connect", resolve);
      reject(e);
    });
    socket.connect(opts);
  });

const prepareDatabase = async (serverInfo: abi.SQLServerInfo) => {
  const connection = await new mssql.ConnectionPool(
    abi.getSQLConfigFromContext(serverInfo),
  ).connect();
  try {
    await connection.query(`CREATE DATABASE ${abi.TABLE_ID.databaseName};`);
    // eslint-disable-next-line no-console
    console.log("SQL", DB_PREPARATION_SQL);
    // eslint-disable-next-line no-console
    console.log(
      "SQL Server Preparation result",
      await connection.query(DB_PREPARATION_SQL),
    );
  } finally {
    await connection.close();
  }
};

const getTypeModifiers = (typeInfo: commonSql.ColumnTypeInfo) => {
  switch (typeInfo.typeName) {
    case "VARCHAR":
    case "NVARCHAR":
      return `(${typeInfo.maxLength > 0 ? typeInfo.maxLength : "MAX"})`;
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
