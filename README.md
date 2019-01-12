Sometimes you need to have maximum writing speed of large amount of data into relatively slow SQL database, like Oracle, MySQL or PostgreSQL - like tens of millions of records in a minute, but at the same time you don't need high reading speed or want to deal with complexity of sharding.

One of possible solutions of that problem is to write data in some sort of in-memory queue, then gradually flush everything into your SQL db in separate threads. So that queue should start to spill data either after certain amount of time or when it's filled.

I think that usage Apache Ignite as an implementation of that queue has some benefits - it's easy to use and meets all requirements with minimal configuration:

You have to write jdbc store for every entity you'd like to have, then configure like in my example and voila - you have that buffer/queue/cache!

```java
// Max size of queue - 8Gb of system memory
storageConfiguration.getDefaultDataRegionConfiguration().setMaxSize(8L * 1024 * 1024 * 1024);
```
```java
// Keep queue in the same JVM as your main application
igniteConfiguration.setClientMode(true);
```
```java
// If particular record doesn't exist in cache - it should be written into underlying DB
cfg.setWriteThrough(true);
```
```java
// Flush data into DB asynchronously
cfg.setWriteBehindEnabled(true);
```
```java
// Expected number of records that could arrive in N amount of time
cfg.setWriteBehindFlushSize(10_000_000);
```
```java
// N amount of time after which cache must start to flush data into DB
cfg.setWriteBehindFlushFrequency(10 * 60 * 1000);
```

#### Results of my tests (Oracle Database 11g Release 2 (11.2.0.1.0)):

#### Your applcation with Ignite client in one JVM, queue in Ignite server node - in another

1 million records - 167995 ms - H2 indexing turned on/write-behind enabled
1 million records - 132101 ms - H2 indexing turned off/write-behind enabled

#### Your applcation and queue in Ignite server node in the same JVM

1 million records - 9294 ms - H2 indexing turned off/write-behind enabled

#### Pure JDBC with C3PO connection pool:

1 million records - 504936 ms
