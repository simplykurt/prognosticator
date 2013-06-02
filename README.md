prognosticator
==============

A library to read/write data from HBase in a form that Hive understands

This bridges the gap between HBase, Hive and HCatalog. HCatalog schemas define the tables, Hive provides the query
implementation and HBase provides the storage mechanism.

Data is written from a Map, as long as it follows the schema defined in HCatalog, and it is read back into the same
Map format.

In the case of a SQL-based HiveServer2 query, the return schema in the Map is defined by the columns in the query
result.

Usage
-----

Add the Maven dependency:

    <dependency>
        <groupId>com.simplymeasured.prognosticator</groupId>
        <artifactId>prognosticator</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>

This project uses Spring for IoC, but Spring is not required.

License
-------

Apache Public License 2.0. See the LICENSE file for more details.
