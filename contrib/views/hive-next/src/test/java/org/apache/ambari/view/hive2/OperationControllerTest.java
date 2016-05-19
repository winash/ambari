package org.apache.ambari.view.hive2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive.persistence.DataStoreStorage;
import org.apache.ambari.view.hive.persistence.Storage;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.SyncJob;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.DataStorageSupplier;
import org.apache.ambari.view.hive2.internal.DefaultSupplier;
import org.apache.ambari.view.hive2.internal.HdfsApiSupplier;
import org.apache.ambari.view.hive2.internal.HiveResult;
import org.apache.ambari.view.utils.hdfs.HdfsApi;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.easymock.EasyMock.*;

public class OperationControllerTest {


    private static ActorSystem actorSystem;

    @BeforeClass
    public static void setup() {
        actorSystem = ActorSystem.create("TestingActorSystem");
        Logger.getRootLogger().setLevel(Level.DEBUG);
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(actorSystem);
    }


    @Test
    public void testSyncJobSubmission() throws SQLException, ConnectionException, InterruptedException {
        HiveJdbcConnectionDelegate connectionDelegate = new HiveJdbcConnectionDelegate();
        HiveConnection connection = createNiceMock(HiveConnection.class);
        Statement statement = createNiceMock(HiveStatement.class);
        ResultSet resultSet = createNiceMock(ResultSet.class);
        DataStorageSupplier supplier = createNiceMock(DataStorageSupplier.class);
        HdfsApiSupplier hdfsSupplier = createNiceMock(HdfsApiSupplier.class);
        HdfsApi hdfsApi = createNiceMock(HdfsApi.class);
        ViewContext viewContext = createNiceMock(ViewContext.class);
        Connect connect = createNiceMock(Connect.class);
        ResultSetMetaData resultSetMetaData = createNiceMock(ResultSetMetaData.class);
        Connectable connectable = createNiceMock(Connectable.class);
        HiveResult hiveResult = createNiceMock(HiveResult.class);

        expect(supplier.get()).andReturn(new DataStoreStorage(viewContext));
        expect(hdfsSupplier.get()).andReturn(Optional.of(hdfsApi)).times(2);
        expect(connection.createStatement()).andReturn(statement);
        expect(connect.getConnectable()).andReturn(connectable);
        expect(connectable.isOpen()).andReturn(false);
        Optional<HiveConnection> connectionOptional = Optional.of(connection);
        expect(connectable.getConnection()).andReturn(connectionOptional).anyTimes();

        connectable.connect();
        String[] statements = {"select * from test"};
        SyncJob job = new SyncJob("admin", statements,viewContext);
        for (String s : statements) {
            expect(statement.execute(s)).andReturn(true);
        }
        expect(statement.getResultSet()).andReturn(resultSet);
        expect(resultSet.getMetaData()).andReturn(resultSetMetaData);
        expect(resultSetMetaData.getColumnCount()).andReturn(1);
        expect(resultSetMetaData.getColumnLabel(1)).andReturn("test");
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getObject(1)).andReturn("test");

        ActorRef operationControl = actorSystem.actorOf(
                Props.create(OperationController.class, actorSystem, new DefaultSupplier<>(connectionDelegate),supplier,hdfsSupplier), "operationController-test");


        ExecuteJob executeJob = new ExecuteJob(connect, job);
        replay(connection,resultSet,resultSetMetaData,statement,viewContext,connect,connectable,hdfsSupplier,hdfsApi,supplier);
        operationControl.tell(executeJob,ActorRef.noSender());

       Thread.sleep(5000);
        verify(connection,resultSet,resultSetMetaData,statement,viewContext,connect,connectable,hdfsApi,supplier,hdfsSupplier);


    }





}
