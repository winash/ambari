package org.apache.ambari.view.hive2;


import akka.actor.ActorSystem;
import akka.actor.TypedActor;
import akka.actor.TypedProps;
import akka.japi.Creator;
import akka.testkit.JavaTestKit;
import com.google.common.base.Optional;
import org.apache.ambari.view.hive2.actors.HiveTaskRouter;
import org.apache.ambari.view.hive2.actors.HiveTaskRouterImpl;
import org.apache.ambari.view.hive2.internal.Connectable;
import org.apache.ambari.view.hive2.internal.ConnectionException;
import org.apache.ambari.view.hive2.internal.ConnectionProperties;
import org.apache.ambari.view.hive2.internal.HiveTask;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;

import static org.easymock.EasyMock.*;

public class HiveActorSystemTest {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
        system = null;
    }



    @Test
    public void testCanProcessAHiveTask() throws InterruptedException, ConnectionException {
        new JavaTestKit(system) {{
            HiveTaskRouter hiveTaskRouter = TypedActor.get(system).typedActorOf(
                    new TypedProps<>(HiveTaskRouter.class,
                            new Creator<HiveTaskRouter>() {
                                public HiveTaskRouter create() {
                                    return new HiveTaskRouterImpl(system);
                                }
                            }),
                    "name");

            HiveTask hiveTask = createNiceMock(HiveTask.class);
            expect(hiveTask.getId()).andReturn(new Long(1));
            expect(hiveTask.getUser()).andReturn("admin");
            expect(hiveTask.getInstance()).andReturn("hive");

            Connection connection = createNiceMock(Connection.class);
            Connectable connectable = createMock(Connectable.class);
            ConnectionProperties connectionProperties = new ConnectionProperties();

            expect(hiveTask.getConnectionClass()).andReturn(connectable);
            expect(hiveTask.getConnectionProperties()).andReturn(connectionProperties);
            expect(connectable.getConnection()).andReturn(Optional.of(connection));
            expect(connectable.isOpen()).andReturn(false);
            connectable.setProperties(connectionProperties);
            expectLastCall();
            connectable.connect();
            expectLastCall();

            replay(hiveTask,connection,connectable);
            hiveTaskRouter.execute(hiveTask);

            expectNoMsg(duration("3 second"));
            verify(hiveTask,connection,connectable);
        }};

    }


}





