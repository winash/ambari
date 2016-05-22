package org.apache.ambari.view.hive2;

import com.google.common.base.Optional;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.DataStoreStorage;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.ConnectionSupplier;
import org.apache.ambari.view.hive2.internal.DataStorageSupplier;
import org.apache.ambari.view.hive2.internal.HdfsApiSupplier;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;

abstract class MockSupport {


    protected HiveJdbcConnectionDelegate connectionDelegate;
    protected HiveConnection connection;
    protected Statement statement;
    protected ResultSet resultSet;
    protected DataStorageSupplier supplier;
    protected HdfsApiSupplier hdfsSupplier;
    protected ConnectionSupplier connectionSupplier;
    protected HdfsApi hdfsApi;
    protected ViewContext viewContext;
    protected Connect connect;
    protected ResultSetMetaData resultSetMetaData;
    protected Connectable connectable;

    public void setUpDefaultExpectations() throws SQLException, ConnectionException {
        expect(supplier.get(viewContext)).andReturn(new DataStoreStorage(viewContext));
        expect(hdfsSupplier.get(viewContext)).andReturn(Optional.of(hdfsApi)).times(2);
        expect(connection.createStatement()).andReturn(statement);
        expect(connect.getConnectable()).andReturn(connectable);
        expect(connectable.isOpen()).andReturn(false);
        Optional<HiveConnection> connectionOptional = Optional.of(connection);
        expect(connectable.getConnection()).andReturn(connectionOptional).anyTimes();
        expect(connectionSupplier.get(viewContext)).andReturn(connectionDelegate).times(1);
        expect(statement.getResultSet()).andReturn(resultSet);
        expect(resultSet.getMetaData()).andReturn(resultSetMetaData);
        expect(resultSetMetaData.getColumnCount()).andReturn(1);
        expect(resultSetMetaData.getColumnLabel(1)).andReturn("test");
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getObject(1)).andReturn("test");

        connectable.connect();
    }

    public void mockDependencies() {
        connectionDelegate = new HiveJdbcConnectionDelegate();
        connection = createNiceMock(HiveConnection.class);
        statement = createNiceMock(HiveStatement.class);
        resultSet = createNiceMock(ResultSet.class);
        supplier = createNiceMock(DataStorageSupplier.class);
        hdfsSupplier = createNiceMock(HdfsApiSupplier.class);
        connectionSupplier = createNiceMock(ConnectionSupplier.class);
        hdfsApi = createNiceMock(HdfsApi.class);
        viewContext = createNiceMock(ViewContext.class);
        connect = createNiceMock(Connect.class);
        resultSetMetaData = createNiceMock(ResultSetMetaData.class);
        connectable = createNiceMock(Connectable.class);
    }



}
