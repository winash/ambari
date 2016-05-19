package org.apache.ambari.view.hive2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.actor.message.Connect;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.SyncJob;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.DefaultSupplier;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
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
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(actorSystem);
    }


    @Test
    public void testSyncJobSubmission() throws SQLException, ConnectionException, InterruptedException {
        HiveJdbcConnectionDelegate connectionDelegate = createNiceMock(HiveJdbcConnectionDelegate.class);
        HiveConnection connection = createNiceMock(HiveConnection.class);
        Statement statement = createNiceMock(HiveStatement.class);
        ResultSet resultSet = createNiceMock(ResultSet.class);
        ResultSetMetaData resultSetMetaData = createNiceMock(ResultSetMetaData.class);
        expect(connection.createStatement()).andReturn(statement);
        Connect connect = createNiceMock(Connect.class);
        Connectable connectable = createNiceMock(Connectable.class);
        expect(connect.getConnectable()).andReturn(connectable);
        expect(connectable.isOpen()).andReturn(false);
        expect(connectable.getConnection()).andReturn(Optional.of(connection));
        connectable.connect();
        String[] statements = {"select * from test"};
        SyncJob job = new SyncJob("admin", statements);
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
                Props.create(OperationController.class, actorSystem, new DefaultSupplier<>(connectionDelegate)), "operationController-test");


        ExecuteJob executeJob = new ExecuteJob(connect, job);
        replay(connection,resultSet,statement,resultSetMetaData,connectionDelegate,connect,connectable);
        operationControl.tell(executeJob,ActorRef.noSender());

        Thread.sleep(5000);
        verify(connection,resultSet,statement,resultSetMetaData,connectionDelegate,connect,connectable);


    }





}
