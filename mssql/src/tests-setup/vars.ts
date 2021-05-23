import { env } from "process";

export const SQL_SERVER_DOCKER_NW = env.SQL_SERVER_DOCKER_NW || "";
export const SQL_SERVER_DOCKER_PORT = env.SQL_SERVER_DOCKER_PORT || "1433";
export const SQL_SERVER_PASSWORD =
  env.SQL_SERVER_PASSWORD || "yourStrong(!)Password";
