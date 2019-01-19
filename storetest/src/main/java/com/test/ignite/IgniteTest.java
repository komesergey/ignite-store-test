package com.test.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import javax.cache.configuration.FactoryBuilder;
import java.util.Collections;
import java.util.UUID;

/**
 -Xms4g
 -Xmx10g
 -XX:+UseG1GC
 -server
 -XX:MaxMetaspaceSize=256m
 -DIGNITE_QUIET=false
 */
public class IgniteTest {

    public static void main(String... args){
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setPeerClassLoadingEnabled(false);

        // Uncomment to use as Ignite client
        // igniteConfiguration.setClientMode(true);

        DataStorageConfiguration storageConfiguration = new DataStorageConfiguration();
        // Max 8Gb of system memory
        storageConfiguration.getDefaultDataRegionConfiguration().setMaxSize(8L * 1024 * 1024 * 1024);
        // Disabled by default (must be disabled for maximum performance)
        storageConfiguration.getDefaultDataRegionConfiguration().setMetricsEnabled(false);
        // Disabled by default (must be disabled for maximum performance)
        storageConfiguration.getDefaultDataRegionConfiguration().setPersistenceEnabled(false);
        // Eviction will start by default only when 90% data region is occupied.
        storageConfiguration.getDefaultDataRegionConfiguration().setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);

        igniteConfiguration.setDataStorageConfiguration(storageConfiguration);
        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder tcpDiscoveryMulticastIpFinder = new TcpDiscoveryMulticastIpFinder();
        tcpDiscoveryMulticastIpFinder.setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
        tcpDiscoverySpi.setIpFinder(tcpDiscoveryMulticastIpFinder);
        igniteConfiguration.setDiscoverySpi(tcpDiscoverySpi);

        CacheConfiguration<String, Person> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName("Persons");
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cfg.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheJdbcPersonStore.class));
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setOnheapCacheEnabled(false);
        cfg.setWriteBehindEnabled(true);
        cfg.setWriteBehindFlushSize(10_000_000);
        // Flush whole queue once in 10 minutes
        cfg.setWriteBehindFlushFrequency(10 * 60 * 1000);

        igniteConfiguration.setCacheConfiguration(cfg);
        Ignite ignite = Ignition.start(igniteConfiguration);
        ignite.active(true);

        try (IgniteCache<String, Person> createdCache = ignite.getOrCreateCache(cfg)) {
            if (ignite.cluster().forDataNodes(createdCache.getName()).nodes().isEmpty()) {
                System.out.println();
                System.out.println(">>> Please start at least 1 remote cache node.");
                System.out.println();
            }
            long start = System.currentTimeMillis();
            System.out.println("Started");
            for(int i = 0; i < 1_000_000; i++){
                Person person = new Person(UUID.randomUUID().toString(), "TEST " + UUID.randomUUID().toString(), "TESTOV " + UUID.randomUUID().toString());
                createdCache.put(person.getId(), person);
            }
            // Finished in 167995 ms for 1 million records with write-behind and settings above - with H2 indexing turned on!
            // Finished in 132101 ms - without indexing
            // ~ 0,17 ms on one record (could be much less with proper Ignite cache configuration),
            // main goal was to decouple insert time from SQL db
            // Then after 10 minutes Ignite started flushing data into Oracle asynchronously

            // Finished in 504936 ms for 1 million records in pure inserts into Oracle (but with C3PO pool)
            // ~ 0,5 ms on one record

            // ONLY SERVER NODE - Finished in 9294 ms
            /**
             * If you want to use it as Ignite client,
             * package person-store and to libs folder following jars:
                 person-store-1.0-SNAPSHOT.jar
                 mchange-commons-java-0.2.11.jar
                 c3p0-0.9.5.2.jar
                 ojdbc6.jar
             */
            System.out.println("Finished in "+ (System.currentTimeMillis() - start) + " ms");
        }
    }
}
