package org.ekbana.connector;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JDBCConnector {

    private static final Logger logger = LoggerFactory.getLogger(JDBCConnector.class);
    private Connection connection;
    private ConnectorConfigurationProperty connectorConfigurationProperty;

    public JDBCConnector(ConnectorConfigurationProperty connectorConfigurationProperty) {
        this.connectorConfigurationProperty = connectorConfigurationProperty;
    }

    public void connect() throws SQLException, ClassNotFoundException {
        getOrCreateConnection();
    }

    public void getOrCreateConnection() throws ClassNotFoundException, SQLException {
        Class.forName(connectorConfigurationProperty.getJdbcDriver());
        connection = DriverManager.getConnection(connectorConfigurationProperty.getJdbcUrl(), connectorConfigurationProperty.getJdbcUsername(), connectorConfigurationProperty.getJdbcPassword());
        if (connection != null) {
            // connection successful
            logger.info("JDBC Connection Success : {}", connectorConfigurationProperty.getJdbcUrl());
//                connection.createStatement().execute("select * from " + table + " limit 1");
        } else {
            // error creating connection
            logger.error("JDBC connection failed : {}", connectorConfigurationProperty.getJdbcUrl());
        }
    }

    public List<JSONObject> executeSelectOperation() {
        return executeSelectOperation(createSelectQuery());
    }

    public List<JSONObject> executeSelectOperation(String query) {
        logger.info(query);
        try {
            final Statement statement = connection.createStatement();
            statement.setFetchSize(connectorConfigurationProperty.getFetchSize());
            final ResultSet resultSet = statement.executeQuery(query);
            final ResultSetMetaData metaData = resultSet.getMetaData();

            if (connectorConfigurationProperty.getIncrementalColumnName()!=null){
                connectorConfigurationProperty.setPrevIncrementalColumnValue(connectorConfigurationProperty.getIncrementalColumnValue());
            }

            if (connectorConfigurationProperty.getIncrementalTimeStampColumnName()!=null){
                connectorConfigurationProperty.setPrevIncrementalTimeStampColumnValue(connectorConfigurationProperty.getIncrementalTimeStampColumnValue());
            }

            List<JSONObject> rows = new ArrayList<>();
            while (resultSet.next()) {
                final JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    final String columnName = metaData.getColumnName(i);
                    jsonObject.put(columnName, resultSet.getObject(i));
                }
                rows.add(jsonObject);
                if (connectorConfigurationProperty.getIncrementalColumnName() != null) {
                    connectorConfigurationProperty.setIncrementalColumnValue(jsonObject.getLong(connectorConfigurationProperty.getIncrementalColumnName()));
                }

                if (connectorConfigurationProperty.getIncrementalTimeStampColumnName() != null) {
                    connectorConfigurationProperty.setIncrementalTimeStampColumnValue(jsonObject.getLong(connectorConfigurationProperty.getIncrementalTimeStampColumnName()));
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void executeBatchInsertOperation(JsonArray rows){
        try {
            final Statement statement = connection.createStatement();
            connection.setAutoCommit(false);

            final Iterator<JsonElement> iterator = rows.iterator();

            while (iterator.hasNext()){
                final String insertStatement = createInsertStatement(connectorConfigurationProperty.getTable(), new JSONObject(iterator.next().getAsString()));
                logger.info("{}", insertStatement);
                statement.addBatch(insertStatement);
            }
            final int[] ints = statement.executeBatch();
            connection.commit();

            for (int i : ints) {
                System.out.println(i);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void executeBatchInsertOperation(List<JSONObject> rows) {

        try {
            final Statement statement = connection.createStatement();
            connection.setAutoCommit(false);
            for (JSONObject row : rows) {
                final String insertStatement = createInsertStatement(connectorConfigurationProperty.getTable(), row);
                logger.info("{}", insertStatement);
                statement.addBatch(insertStatement);
            }
            final int[] ints = statement.executeBatch();
            connection.commit();

            for (int i : ints) {
                System.out.println(i);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String createInsertStatement(String tableName, JSONObject jsonObject) {
        String keys = null;
        String values = "";
        for (String key : jsonObject.keySet()) {
            if (keys == null) {
                keys = key;
                values = values + process(jsonObject.get(key));
            } else {
                keys = keys + "," + key;
                values = values + "," + process(jsonObject.get(key));
            }
        }

        final String insert_into_ = new StringBuilder()
                .append("INSERT INTO ")
                .append(tableName)
                .append(" (")
                .append(keys)
                .append(")")
                .append(" VALUES (")
                .append(values)
                .append(" )")
                .append(" ON CONFLICT DO NOTHING")
                .toString();

        System.out.println(insert_into_);
        return insert_into_;
    }

    private String process(Object obj) {
        if (obj instanceof Integer || obj instanceof Number) return String.valueOf(obj);
        else if (obj instanceof String) {
            return "'" + obj.toString().replace("'", "''") + "'";
        } else {
            return "'" + obj + "'";
        }
    }

    private String createSelectQuery() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder = stringBuilder.append("SELECT * FROM ")
                .append(connectorConfigurationProperty.getTable());
        if (connectorConfigurationProperty.getIncrementalColumnName() != null || connectorConfigurationProperty.getIncrementalTimeStampColumnName() != null) {
            stringBuilder = stringBuilder.append(" WHERE ");
            if (connectorConfigurationProperty.getIncrementalColumnName() != null && connectorConfigurationProperty.getIncrementalTimeStampColumnName() != null)
                stringBuilder = stringBuilder.append(connectorConfigurationProperty.getIncrementalColumnName())
                        .append(">")
                        .append(connectorConfigurationProperty.getIncrementalColumnValue())
                        .append(" AND ")
                        .append(connectorConfigurationProperty.getIncrementalTimeStampColumnName())
                        .append(">")
                        .append(connectorConfigurationProperty.getIncrementalTimeStampColumnValue());
            else if (connectorConfigurationProperty.getIncrementalColumnName() != null) {
                stringBuilder = stringBuilder.append(connectorConfigurationProperty.getIncrementalColumnName())
                        .append(">")
                        .append(connectorConfigurationProperty.getIncrementalColumnValue());
            } else if (connectorConfigurationProperty.getIncrementalTimeStampColumnName() != null) {
                stringBuilder = stringBuilder.append(connectorConfigurationProperty.getIncrementalTimeStampColumnName())
                        .append(">")
                        .append(connectorConfigurationProperty.getIncrementalTimeStampColumnValue());
            }
        }
        stringBuilder = stringBuilder.append(" LIMIT ")
                .append(Math.max(1, connectorConfigurationProperty.getFetchSize()));

        return stringBuilder.toString();
    }

}
