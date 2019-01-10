package com.test.ignite;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CacheJdbcPersonStore implements CacheStore<String, Person>, Serializable {

    private static ComboPooledDataSource ods;

    static {
        try{
            ods = new ComboPooledDataSource();
            ods.setDriverClass("oracle.jdbc.pool.OracleDataSource");
            ods.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:orcl");
            ods.setUser("kronos");
            ods.setPassword("KRONOS");
            ods.setMinPoolSize(5);
            ods.setMaxPoolSize(100);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public Person load(String key) {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from metastore.PERSONS where id=?")) {
                st.setString(1, key);
                ResultSet rs = st.executeQuery();
                return rs.next() ? new Person(rs.getString(1), rs.getString(2), rs.getString(3)) : null;
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load: " + key, e);
        }
    }

    // INSERT INTO metastore.PERSONS (id, firstName, lastName) VALUES ('11', 'AAB', 'BB') ON DUPLICATE KEY UPDATE firstName = VALUES(firstName), lastName = VALUES(lastName);
    @Override
    public void write(Cache.Entry<? extends String, ? extends Person> entry){
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("MERGE INTO persons USING dual ON ( persons.id = ? ) " +
                    "WHEN MATCHED THEN UPDATE SET firstname = ? , lastname = ? " +
                    "WHEN NOT MATCHED THEN INSERT ( id, firstname, lastname) VALUES ( ?, ?, ?)")) {
                Person val = entry.getValue();
                st.setString(1, entry.getKey());
                st.setString(2, val.getFirstName());
                st.setString(3, val.getLastName());
                st.setString(4, entry.getKey());
                st.setString(5, val.getFirstName());
                st.setString(6, val.getLastName());
                st.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to write [key=" + entry.getKey() + ", val=" + entry.getValue() + ']', e);
        }
    }

    @Override
    public void delete(Object key) {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("delete from metastore.PERSONS where id=?")) {
                st.setString(1, (String)key);
                st.executeUpdate();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to delete: " + key, e);
        }
    }

    @Override
    public void loadCache(IgniteBiInClosure<String, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");
        final int entryCnt = (Integer)args[0];
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from metastore.PERSONS")) {
                try (ResultSet rs = st.executeQuery()) {
                    int cnt = 0;
                    while (cnt < entryCnt && rs.next()) {
                        Person person = new Person(rs.getString(1), rs.getString(2), rs.getString(3));
                        clo.apply(person.getId(), person);
                        cnt++;
                    }
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }

    private Connection connection() throws SQLException  {
        Connection conn = ods.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    @Override
    public Map<String, Person> loadAll(Iterable<? extends String> keys) {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("select firstName, lastName from PERSONS where id=?")) {
                Map<String, Person> loaded = new HashMap<>();
                for (String key : keys) {
                    st.setString(1, key);
                    try(ResultSet rs = st.executeQuery()) {
                        if (rs.next())
                            loaded.put(key, new Person(key, rs.getString(1), rs.getString(2)));
                    }
                }
                return loaded;
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to loadAll: " + keys, e);
        }
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends String, ? extends Person>> entries) {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("MERGE INTO persons USING dual ON ( persons.id = ? ) " +
                    "WHEN MATCHED THEN UPDATE SET firstname = ? , lastname = ? " +
                    "WHEN NOT MATCHED THEN INSERT ( id, firstname, lastname) VALUES ( ?, ?, ?)")) {
                for (Cache.Entry<? extends String, ? extends Person> entry : entries) {
                    Person val = entry.getValue();
                    st.setString(1, entry.getKey());
                    st.setString(2, val.getFirstName());
                    st.setString(3, val.getLastName());
                    st.setString(4, entry.getKey());
                    st.setString(5, val.getFirstName());
                    st.setString(6, val.getLastName());
                    st.addBatch();
                }
                st.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to writeAll: " + entries, e);
        }
    }

    @Override
    public void deleteAll(Collection<?> keys) {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("delete from metastore.PERSONS where id=?")) {
                for (Object key : keys) {
                    st.setString(1, (String)key);
                    st.addBatch();
                }
                st.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to deleteAll: " + keys, e);
        }
    }

    @Override
    public void sessionEnd(boolean commit) {
        // No-op.
    }
}
