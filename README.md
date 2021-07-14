# Data Heaving - Data Source from SQL Databases
[![Code Coverage](https://codecov.io/gh/DataHeaving/source-sql/branch/develop/graph/badge.svg)](https://codecov.io/gh/DataHeaving/source-sql)

This repository is part of [Data Heaving project](https://github.com/DataHeaving).
There are multiple packages in the repository, all of which are related to SQL databases acting like a data source in [Data Heaving Orchestration API](https://github.com/DataHeaving/orchestration/pipelines):
- [API package](api) specifying common types and utility methods to be utilized by various SQL connections, and
- [MSSQL package](mssql) providing data source functionality for SQL Server utilizing [mssql library](https://github.com/tediousjs/node-mssql).

# Usage
All packages of Data Heaving project are published as NPM packages to public NPM repository under `@data-heaving` organization.

# More information
To learn more what Data Heaving project is all about, [see here](https://github.com/DataHeaving/orchestration).