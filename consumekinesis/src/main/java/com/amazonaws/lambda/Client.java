package com.amazonaws.lambda;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import org.apache.commons.lang.ArrayUtils;

import java.math.BigDecimal;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.lambda.ConnectionPool.getConnection;
import static com.amazonaws.lambda.ConnectionPool.returnConnection;
import static org.apache.commons.lang.StringUtils.split;

public class Client {

<<<<<<< HEAD
    private final static String URL = "jdbc:sqlserver://*****:1433;DatabaseName=";
    private static final String USER = "";
    private static final String PASSWORD = "";
=======
    private final static String URL = "jdbc:sqlserver://******:1433;DatabaseName=ETL";
    private static final String USER = "*****";
    private static final String PASSWORD = "*****";
>>>>>>> d7f7026522f5cbb8e7fe564c2b6a13bfea86f877

    private static AmazonKinesis kinesis;

    private static void init() throws Exception {
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
//        ClientConfiguration config = new ClientConfiguration();
//                config.setProxyUsername("");
//                config.setProxyPassword("");
        kinesis = AmazonKinesisClientBuilder.standard()
//                .withClientConfiguration(config)
                .withCredentials(credentialsProvider)
                .withRegion("ap-southeast-1")
                .build();
    }

    public static void main(String args[]) throws Exception {
        init();
//        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
//        listStreamsRequest.setLimit(10);
//        ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
//        List<String> streamNames = listStreamsResult.getStreamNames();
//
//        for (String streamName1 : streamNames) {
//            System.out.println("=======================" + streamName1);
        String streamName = "ESCM_EEL-ESCMOWNER-SC_HD1";
//        String streamName = "ESCM_EEL-ESCMOWNER-BOM_HD";

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);

        StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
        String streamStatus = streamDescription.getStreamStatus();
        System.out.printf("Stream %s has a status of %s.\n", streamName, streamStatus);

        if ("DELETING".equals(streamStatus)) {
            System.out.println("Stream is being deleted. This sample will now exit.");
            System.exit(0);
        }

        if ("ACTIVE".equals(streamStatus)) {
            String sql = new String("");
            Map<String, String> columnMap = new HashMap<>();
            Map<String, String> typeMap = new HashMap<>();
            List<Shard> shards = streamDescription.getShards();
            for (Shard shard : shards) {
                String shardId = shard.getShardId();
                System.out.println("=======streamName: " + streamName);//TRIM_HORIZON  LATEST
                String shardIterator = kinesis.getShardIterator(streamName, shardId, "LATEST").getShardIterator();

                List<Record> records;
                Connection conn;
                conn = getConnection(URL, USER, PASSWORD);
                while (true) {
                    GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
                    getRecordsRequest.setShardIterator(shardIterator);
                    getRecordsRequest.setLimit(1000);

                    GetRecordsResult getRecordsResult = kinesis.getRecords(getRecordsRequest);
                    records = getRecordsResult.getRecords();
                    System.out.println("=======records.size: " + records.size());

                    if (records.size() > 0) {

                        if (conn != null && !conn.isClosed()) {
                            System.out.println("Database connection Success.");
                        } else {
                            conn = getConnection(URL, USER, PASSWORD);
//                            System.out.println("Database connection Failed.");
//                            return;
                        }
                        String[] strs = streamName.split("-");
                        String tableName = "SSBI_SC_HD";//strs[strs.length - 1];
                        if (typeMap.size() == 0) {
                            try {
                                typeMap = getTypeMap(conn, tableName);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                        if (columnMap.size() == 0 || sql.trim().isEmpty()) {
                            String columns = StandardCharsets.UTF_8.decode(records.get(0).getData()).toString();
                            String[] columnList = columns.split(",");
                            columnMap = getcolumnMap(columnList);
                            sql = getSqlMode(tableName, columnList, columns);
                        }
                        insertDB(records, conn, tableName, sql, columnMap, typeMap);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException exception) {
                        throw new RuntimeException(exception);
                    }
                    shardIterator = getRecordsResult.getNextShardIterator();
                }
            }
        }
//        }
    }

    public static void insertDB(List<Record> records, Connection conn, String tableName,
                                 String sql, Map<String, String> columnMap, Map<String, String> typeMap) {

        PreparedStatement preparedStatement = null;
        int n = 0;
        int batchSize = 5000;
        try {
            preparedStatement = conn.prepareStatement(sql);
            // 关闭事务自动提交 ,这一行必须加上，否则每插入一条数据会向log插入一条日志
            conn.setAutoCommit(false);
            for (int i = 1; i < records.size(); i++) {
                ++n;
                String data = StandardCharsets.UTF_8.decode(records.get(i).getData()).toString();
                System.out.println("data: " + data);
                String[] dataList = data.split(",");
                addBatch(preparedStatement, dataList, columnMap, typeMap);
                if (n == records.size() - 1 || n % batchSize == 0 || n == batchSize) {
                    System.out.println("The number of records inserted into DB is: " +
                            preparedStatement.executeBatch().length);
                    preparedStatement.executeBatch();
                    conn.commit();
                    System.out.println("Successfully submit data to DB");
                }
            }
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.rollback();
//                    preparedStatement.close();
//                    conn.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
            System.out.println("Insert data to DB failed");
        } finally {
            if (n == records.size() - 1 && conn != null) {
                try {
                    preparedStatement.close();
//                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void addBatch(PreparedStatement preparedStatement, String[] dataList,
                                 Map<String, String> columnMap, Map<String, String> typeMap) throws SQLException {
        if (!dataList[0].equals("Op")) {
            for (int j = 0; j < dataList.length; j++) {
                String columnType = typeMap.get(columnMap.get(String.valueOf(j)).toUpperCase());
                if (columnType == null) {
                    String aa = columnMap.get(String.valueOf(j)).toUpperCase();
                    String columnType1 = typeMap.get(aa);
                    System.out.println("=========aa:" + aa);
                    System.out.println("=========columnType1:" + columnType1);
                }
                switch (columnType) {
                    case "NVARCHAR":
                        preparedStatement.setString(j + 1, dataList[j]);
                        break;
                    case "NUMERIC":
//                        System.out.println("j:" + j + "   dataList[j]:" + dataList[j] + "   columnName:" + columnMap.get(String.valueOf(j)).toUpperCase());
                        BigDecimal number = new BigDecimal((dataList[j] == null || dataList[j].trim().isEmpty()) ? String.valueOf("0.00") : dataList[j]);
                        number = number.setScale(2, BigDecimal.ROUND_HALF_UP);
                        preparedStatement.setBigDecimal(j + 1, number);
                        break;
                    case "DATETIME":
                        preparedStatement.setString(j + 1, dataList[j]);
                        break;
                    default:
                        preparedStatement.setString(j + 1, dataList[j]);
                        break;
                }
            }
            preparedStatement.addBatch();
        }
    }

    public static Map<String, String> getTypeMap(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData m_DBMetaData = conn.getMetaData();
        ResultSet colRet = m_DBMetaData.getColumns(null, "%", tableName, "%");
        Map<String, String> typeMap = new HashMap<>();
        while (colRet.next()) {
            String columnName = colRet.getString("COLUMN_NAME");
            String columnType = colRet.getString("TYPE_NAME");
            if (columnType.toUpperCase().equals("NVARCHAR")) {
                typeMap.put(columnName, "NVARCHAR");
            }
            if (columnType.toUpperCase().equals("NUMERIC")) {
                typeMap.put(columnName, "NUMERIC");
            }
            if (columnType.toUpperCase().equals("DATETIME")) {
                typeMap.put(columnName, "DATETIME");
            }
        }
        return typeMap;
    }

    public static Map<String, String> getcolumnMap(String[] columnList) {
        Map<String, String> columnMap = new HashMap<>();
        for (int i = 0; i < columnList.length; i++) {
            columnMap.put(String.valueOf(i), columnList[i]);
        }
        return columnMap;
    }

    public static String getSqlMode(String tableName, String[] columnList, String columns) {
        String str = "";
        for (String s : columnList) {
            str = str + "?,";
        }
        String values = str.substring(0, str.length() - 1);
        String sql = "INSERT INTO " + tableName + " (" + columns + ")" + " VALUES " + "(" + values + ");";
        return sql;
    }

    public static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);

        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }
        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
    }


}
