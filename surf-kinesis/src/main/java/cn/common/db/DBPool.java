package cn.common.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBPool {
    private ComboPooledDataSource cpds = null;
    private static Properties props = new Properties();
    static{
        InputStream in = DBPool.class.getResourceAsStream("/c3p0-config.properties");
        try {
            props.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class DBPoolHolder{
        private static final DBPool instantce = new DBPool();
    }
    private DBPool(){}

    public static final DBPool getInstance(){
        return DBPoolHolder.instantce;
    }

    public Connection getConnection(){
        try {
            if(cpds == null){
                cpds = new ComboPooledDataSource();
            }
            cpds.setDriverClass(props.getProperty("driverClass"));
            cpds.setJdbcUrl(props.getProperty("jdbcUrl"));
            cpds.setUser(props.getProperty("user"));
            cpds.setPassword(props.getProperty("password"));
            cpds.setInitialPoolSize(Integer.parseInt(props.getProperty("initialPoolSize")));
            cpds.setMaxIdleTime(Integer.parseInt(props.getProperty("maxIdleTime")));
            cpds.setMaxPoolSize(Integer.parseInt(props.getProperty("maxPoolSize")));
            cpds.setMinPoolSize(Integer.parseInt(props.getProperty("minPoolSize")));
            return cpds.getConnection();
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close(Connection conn){
        try {
            if(conn != null){
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close(Connection conn, Statement state){
        try {
            if(conn != null){
                close(conn);
            }
            if(state != null){
                state.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Connection ut = DBPool.getInstance().getConnection();
        System.out.println(ut);
    }
}
