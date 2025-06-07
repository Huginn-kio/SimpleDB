# SimpleDB

SimpleDB is a simple database implemented in Java, with some principles referenced from MySQL, PostgreSQL, and SQLite.
It is divided into five main modules: Transaction Management, Data Management, Version Management, Index Management, and Table Management. It implements some typical database functionalities.

Project Features:

  1. Implement crash recovery via logging
  2. Utilize an LRU-based buffer pool to cache pages
  3. Supporte MVCC and two isolation levels: Read Committed and Repeatable Read
  4. Implement B+ tree index similar to MyISAM
  5. Manage table schema information and support parsing of some SQL statements




