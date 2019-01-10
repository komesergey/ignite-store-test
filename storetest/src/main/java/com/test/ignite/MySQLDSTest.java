package com.test.ignite;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class MySQLDSTest {
    public static void main(String... args) throws Exception{
        MysqlDataSource mysqlDS = new MysqlDataSource();
        mysqlDS.setURL("jdbc:mysql://localhost:3306/mysql");
        mysqlDS.setUser("root");
        mysqlDS.setPassword("root");
        mysqlDS.setServerTimezone("Europe/Moscow");

        Person person = new Person("1111", "TEST", "TESTOV");

        try (Connection conn = mysqlDS.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement st = conn.prepareStatement("INSERT INTO metastore.PERSONS (id, firstName, lastName) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE firstName = VALUES(firstName), lastName = VALUES(lastName)")) {
                st.setString(1, person.getId());
                st.setString(2, person.getFirstName());
                st.setString(3, person.getLastName());
                st.executeUpdate();
            }
            conn.commit();
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
}
