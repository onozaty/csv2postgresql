# csv2postgresql

It is a tool to read CSV file and load it to PostgreSQL.

You do not have to create a table in advance. The table is created based on the field names described in the header of the CSV file.
If a table already exists, load it against that table.

## Usage

Java (JDK8 or higher) is required for execution.

Download the latest jar file (`csv2postgresql-x.x.x-all.jar`) from below.

* https://github.com/onozaty/csv2postgresql/releases/latest

Execute the application with the following command.

```
java -jar csv2postgresql-1.0.0-all.jar config.properties table1 data.csv
```

The arguments are as follows.
1. Configuration file path
1. Table name
1. CSV file path

When execution is complete, the number of records processed and the elapsed time are output.

```
Loading...
Loading is completed. (Number of records: 100,000 / Elapsed millsecods: 322)
```

### Configuration file

In the configuration file, write connection information of PostgreSQL and encoding of CSV file.

* `database.url` JDBC connection url
* `database.user` Database user name
* `database.password` Database user password
* `csv.encoding` CSV file encoding

The following is an example.

```
database.url=jdbc:postgresql://192.168.33.10:5432/testdb
database.user=user1
database.password=pass1
csv.encoding=UTF-8
```

### Table name

Specify the load destination table name.

If the table does not exist, create a new table. Each column is created as a `text` type.

### CSV file

CSV file header is required. Use the field names in the header to map to the table columns.
If a non-alphanumeric character is specified, it will be replaced by an underscore (`_`).

For example, if there is a field `User Name`, it will be mapped to `user_name` as a database column.

## Sample

A Vagrant environment for starting PostgreSQL, and sample configuration files and CSV files are prepared.
You can try this tool easily by using these.

If you execute `vagrant up` in the `vagrant` folder, the virtual environment(192.168.33.10) on which PostgreSQL 11 is installed starts up.

Load using configuration file and CSV file under `sample` folder.
```
java -jar csv2postgresql-1.0.0-all.jar sample/config.properties test_table sample/sample-100000.csv
```

## How to build

When building from the source code, build the application with the following command in the environment where Java (JDK 8 or higher) is installed.

```
gradlew shadowJar
```

`build/libs/csv2postgresql-x.x.x-all.jar` will be created. (x.x.x is version number)
