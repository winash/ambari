package org.apache.ambari.view.hive2;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import org.apache.ambari.view.hive.client.Row;
import org.apache.ambari.view.hive2.actor.OperationController;
import org.apache.ambari.view.hive2.actor.ResultSetIterator;
import org.apache.ambari.view.hive2.actor.message.ExecuteJob;
import org.apache.ambari.view.hive2.actor.message.SyncJob;
import org.apache.ambari.view.hive2.actor.message.job.ExecutionFailed;
import org.apache.ambari.view.hive2.actor.message.job.FetchFailed;
import org.apache.ambari.view.hive2.actor.message.job.Next;
import org.apache.ambari.view.hive2.actor.message.job.NoMoreItems;
import org.apache.ambari.view.hive2.actor.message.job.NoResult;
import org.apache.ambari.view.hive2.actor.message.job.Result;
import org.apache.ambari.view.hive2.actor.message.job.ResultSetHolder;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class SyncQueriesTest extends MockSupport {


    private  ActorSystem actorSystem;

    @Before
    public void setup() {
        actorSystem = ActorSystem.create("TestingActorSystem");
        Logger.getRootLogger().setLevel(Level.DEBUG);
    }

    @After
    public void teardown() {
        JavaTestKit.shutdownActorSystem(actorSystem);
    }



    @Test
    public void testSyncJobSubmission() throws SQLException, ConnectionException, InterruptedException {
        mockDependencies();
        setUpDefaultExpectations();
        String[] statements = {"select * from test"};
        SyncJob job = new SyncJob("admin", statements,viewContext);
        for (String s : statements) {
            expect(statement.execute(s)).andReturn(true);
        }

        ActorRef operationControl = actorSystem.actorOf(
                Props.create(OperationController.class, actorSystem, connectionSupplier, supplier, hdfsSupplier), "operationController-test");

        Inbox inbox = Inbox.create(actorSystem);

        ExecuteJob executeJob = new ExecuteJob(connect, job);
        inbox.send(operationControl, executeJob);

        replay(connection, resultSet, resultSetMetaData, statement, viewContext, connect, connectable, hdfsSupplier, hdfsApi, supplier, connectionSupplier);

        try {

            Object jdbcResult = inbox.receive(Duration.create(1, TimeUnit.MINUTES));

            if (jdbcResult instanceof NoResult) {
                fail();
            } else if (jdbcResult instanceof ExecutionFailed) {

                ExecutionFailed error = (ExecutionFailed) jdbcResult;
                fail();
                error.getError().printStackTrace();

            } else if (jdbcResult instanceof ResultSetHolder) {
                ResultSetHolder holder = (ResultSetHolder) jdbcResult;
                ActorRef iterator = holder.getIterator();

                inbox.send(iterator, new Next());
                Object receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));


                Result result = (Result) receive;
                List<Row> rows = result.getRows();
                System.out.println("Fetched " + rows.size() + " entries.");
                for (Row row : rows) {
                    assertArrayEquals(row.getRow(), new String[]{"test"});
                }

                inbox.send(iterator, new Next());
                receive = inbox.receive(Duration.create(1, TimeUnit.MINUTES));
                assertTrue(receive instanceof NoMoreItems);


                if (receive instanceof FetchFailed) {
                    fail();
                }

            }

        } catch (Throwable ex) {
            fail();
        }


        verify(connection, resultSet, resultSetMetaData, statement, viewContext, connect, connectable, hdfsSupplier, hdfsApi, supplier);

    }


}
