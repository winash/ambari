package org.apache.ambari.view.hive2.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.hive2.actor.message.ExtractResultSet;
import org.apache.ambari.view.hive2.internal.HiveResult;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Extract and start buffering the result set information to the
 *
 *
 */
public class ResultSetExtractor extends UntypedActor {

    private final ActorRef actorRef;
    private final ActorSystem system;
    private final ViewContext viewContext;

    public ResultSetExtractor(ViewContext viewContext, ActorSystem system,ActorRef actorRef ) {
        this.actorRef = actorRef;
        this.system = system;
        this.viewContext = viewContext;
    }

    @Override
  public void onReceive(Object message)  {
      if(message instanceof ExtractResultSet)
      {
          try {
              HiveResult hiveResult = pullResultSet(message);
              sender().tell(hiveResult,self());
          } catch (SQLException e) {
              //TODO: handle exception
          }
      }




  }

  private HiveResult pullResultSet(Object extractMessage) throws SQLException {
    ExtractResultSet message = (ExtractResultSet) extractMessage;
    ResultSet resultSet = message.getResultSet();
    // make a blocking call to get the result set data
    // get the Hive result
      HiveResult hiveResult = new HiveResult(resultSet);
     return hiveResult;
  }
}
