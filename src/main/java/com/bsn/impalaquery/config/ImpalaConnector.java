package com.bsn.impalaquery.config;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ImpalaConnector implements Connection {
    private final Connection originalConnection;
    public ImpalaConnector(Connection originalConnection) {
        System.out.println("?zzzz");
        this.originalConnection = originalConnection;
    }
    @Override
    public Statement createStatement() throws SQLException {
        //return null;
        return originalConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        //return null;
        System.out.println("???????????????");
        return originalConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return originalConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return originalConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        originalConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return originalConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        originalConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        originalConnection.rollback();
    }

    @Override
    public void close() throws SQLException {
        originalConnection.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return originalConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return originalConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        originalConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return originalConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        originalConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return originalConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        originalConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return originalConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return originalConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        originalConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return originalConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return originalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return originalConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return originalConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        originalConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        originalConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return originalConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return originalConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return originalConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        originalConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        originalConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return originalConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return originalConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return originalConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return originalConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return originalConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return originalConnection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return originalConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return originalConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return originalConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return originalConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return originalConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        originalConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        originalConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return originalConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return originalConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return originalConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return originalConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        originalConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return originalConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        originalConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        originalConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        System.out.println("??");
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return originalConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return originalConnection.isWrapperFor(iface);
    }
}
