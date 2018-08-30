package com.amazonaws.lambda;

import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
//import java.util.Date;
import java.util.Map;

public class JDBCMetaData {
    private final static String URL = "jdbc:sqlserver://****:1433;DatabaseName=";
    private static final String USER = "";
    private static final String PASSWORD = "";

    public static void main(String args[]) throws Exception {
        String sql = new String("");
        String bb = "";

        System.out.println(sql.trim().isEmpty());

        String s = " ";
        BigDecimal aa = new BigDecimal((s==null||s.trim().isEmpty()) ? String.valueOf("0.00"):s);
        aa = aa.setScale(2, BigDecimal.ROUND_HALF_UP);
        System.out.println(aa);

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
        Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
        PreparedStatement preparedStatement = conn.prepareStatement("");
        DatabaseMetaData m_DBMetaData = conn.getMetaData();
        ResultSet colRet = m_DBMetaData.getColumns(null, "%", "SSBI_SC_HD", "%");
//        List columns = new ArrayList();
        String columnString = "";
        String valueString = "";
//        while (colRet.next()) {
//            String columnName = colRet.getString("COLUMN_NAME");
//            String columnType = colRet.getString("TYPE_NAME");
////            int datasize = colRet.getInt("COLUMN_SIZE");
////            int digits = colRet.getInt("DECIMAL_DIGITS");
////            int nullable = colRet.getInt("NULLABLE");
////            System.out.println(columnName + " " + columnType + " " + datasize + " " + digits + " " + nullable);
////            columns.add(columnName);
//            System.out.println(columnName + " " + columnType);
//            columnString = columnString + columnName + ",";
//            valueString = valueString + "?,";
//        }
//        String columns = columnString.substring(0, columnString.length() - 1);
//        String values = valueString.substring(0, valueString.length() - 1);
//        System.out.println(columns);
//        System.out.println(values);

//        Map<String,String> typeMap = new HashMap<>();
//        while (colRet.next()) {
//            String columnName = colRet.getString("COLUMN_NAME");
//            String columnType = colRet.getString("TYPE_NAME");
//            if (columnType.toUpperCase().equals("NVARCHAR")) {
//                typeMap.put(columnName,"NVARCHAR");
//            }
//            if (columnType.toUpperCase().equals("NUMERIC")) {
//                typeMap.put(columnName,"NUMERIC");
//            }
//            if (columnType.toUpperCase().equals("DATETIME")) {
//                typeMap.put(columnName,"DATETIME");
//            }
//        }
//        for (Map.Entry<String,String> entry : map.entrySet()) {
////                    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
//
//        }
        String columns = "";
        String[] columnList = columns.split(",");
        Map<String,String> columnMap = new HashMap<>();
        for (int i = 0; i < columnList.length; i++) {
            columnMap.put(String.valueOf(i),columnList[i]);
        }

//        String data ="";
//        String[] dataList = data.split(",");
//        if (!dataList[0].equals("Op")) {
//            for (int j = 0; j < dataList.length; j++) {
//                String columnType = typeMap.get(columnMap.get(String.valueOf(j)));
//                switch(columnType){
//                    case "NVARCHAR":
//                        preparedStatement.setString(j + 1, dataList[j]);
//                    case "NUMERIC":
//                        BigDecimal number = new BigDecimal(dataList[j]);
//                        number = number.setScale(2, BigDecimal.ROUND_HALF_UP);
//                        preparedStatement.setBigDecimal(j + 1, number);
//                    case "DATETIME":
//                        SimpleDateFormat ss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                        java.util.Date d = null;
//                        try {
//                            d = ss.parse(columnType);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        java.sql.Date date = new java.sql.Date(d.getTime());
//                        preparedStatement.setDate(j + 1, date);
//                    default:
//                        System.out.println("default");
//                }
//            }
//            preparedStatement.addBatch();
//        }

    }
}
