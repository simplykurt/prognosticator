[![Build Status](https://travis-ci.org/simplymeasured/prognosticator.png)](https://travis-ci.org/simplymeasured/prognosticator)

# prognosticator

A library to read/write data from HBase in a form that Hive understands

This bridges the gap between HBase, Hive and HCatalog. HCatalog schemas define the tables, Hive provides the query
implementation and HBase provides the storage mechanism.

Data is written from a Map, as long as it follows the schema defined in HCatalog, and it is read back into the same
Map format.

In the case of a SQL-based HiveServer2 query, the return schema in the Map is defined by the columns in the query
result.

## Usage

### Adding to your project

Add the Maven dependency:

```xml
<dependency>
    <groupId>com.simplymeasured.prognosticator</groupId>
    <artifactId>prognosticator</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

This project uses Spring for IoC, but Spring is not required.

### Query usage

You will need an instance of javax.sql.DataSource, then you can create an instance of the HiveQueryImpl class:

```java
public void executeQuery() {
	DataSource dataSource = ...;

	Map<String, Object> parameters = new HashMap<String, Object>();
	parameters.put("col1Param", 1234);

	HiveQuery query = new HiveQueryImpl(dataSource);
	QueryCursor<Map<String, Object>> cursor = query.runQuery("SELECT * FROM foo WHERE col1 = :col1Param", parameters);

	while(cursor.next()) {
		Map<String, Object> row = cursor.get();

		...
	}
}
```

# License

Apache Public License 2.0. See the LICENSE file for more details.
