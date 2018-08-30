package com.amazonaws.lambda;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

import static java.sql.DriverManager.println;

public class ConnectionPool {

    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection(String url, String user, String passWork) {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 5; i++) {
                    Connection conn = DriverManager.getConnection(url, user, passWork);
                    connectionQueue.push(conn);
                }
            }
            println("成功连接数据库");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();

    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}