# Mini Database Engine with Octree Indexing

## Overview

This project implements a simplified database management system in Java with support for Octree indexing. The system provides core database functionalities including table creation, data insertion, deletion, updating, and querying with the ability to create and utilize multi-dimensional indices for optimized search operations.

### Code Structure

```plaintext
mini-dbms/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   ├── DBApp.java
│   │   │   ├── exception/
│   │   │   │   └── DBAppException.java
│   │   │   ├── index/
│   │   │   │   ├── Octree.java
│   │   │   │   ├── Node.java
│   │   │   │   └── Point.java
│   │   │   ├── query/
│   │   │   │   └── SQLTerm.java
│   │   │   ├── storage/
│   │   │   │   ├── Page.java
│   │   │   │   ├── Record.java
│   │   │   │   └── Table.java
│   │   │   └── util/
│   │   │       ├── Operation.java
│   │   │       ├── RowReference.java
│   │   │       └── Serializer.java
│   └── resources/
│       ├── DBApp.config
│       └── metadata.csv
└── README.md
```



## Usage Examples

### Initializing the Database

```java
DBApp dbApp = new DBApp();
dbApp.init();
```


### Creating a Table
```java
Hashtable<String, String> htblColNameType = new Hashtable<>();
htblColNameType.put("id", "java.lang.Integer");
htblColNameType.put("name", "java.lang.String");
htblColNameType.put("gpa", "java.lang.Double");

Hashtable<String, String> htblColNameMin = new Hashtable<>();
htblColNameMin.put("id", "1");
htblColNameMin.put("name", "A");
htblColNameMin.put("gpa", "0.0");

Hashtable<String, String> htblColNameMax = new Hashtable<>();
htblColNameMax.put("id", "100000");
htblColNameMax.put("name", "ZZZZZZZZZZ");
htblColNameMax.put("gpa", "4.0");

dbApp.createTable("Student", "id", htblColNameType, htblColNameMin, htblColNameMax);
```

### Creating an Index

```java
dbApp.createIndex("Student", new String[]{"name", "gpa", "id"});
javaHashtable<String, Object> htblColNameValue = new Hashtable<>();
htblColNameValue.put("id", 12345);
htblColNameValue.put("name", "John Doe");
htblColNameValue.put("gpa", 3.7);

dbApp.insertIntoTable("Student", htblColNameValue);
```

### Updating Data

```java
Hashtable<String, Object> htblColNameValue = new Hashtable<>();
htblColNameValue.put("name", "Jane Doe");
htblColNameValue.put("gpa", 3.9);

dbApp.updateTable("Student", "12345", htblColNameValue);
```

### Deleting Data

```java
Hashtable<String, Object> htblColNameValue = new Hashtable<>();
htblColNameValue.put("id", 12345);

dbApp.deleteFromTable("Student", htblColNameValue);
```

### Querying Data

```java
SQLTerm[] arrSQLTerms = new SQLTerm[2];

arrSQLTerms[0] = new SQLTerm();
arrSQLTerms[0]._strTableName = "Student";
arrSQLTerms[0]._strColumnName = "name";
arrSQLTerms[0]._strOperator = "=";
arrSQLTerms[0]._objValue = "John Doe";

arrSQLTerms[1] = new SQLTerm();
arrSQLTerms[1]._strTableName = "Student";
arrSQLTerms[1]._strColumnName = "gpa";
arrSQLTerms[1]._strOperator = "=";
arrSQLTerms[1]._objValue = new Double(1.5);

String[] strarrOperators = new String[1];
strarrOperators[0] = "AND";

Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);

while(resultSet.hasNext()) {
    System.out.println(resultSet.next());
}
```
