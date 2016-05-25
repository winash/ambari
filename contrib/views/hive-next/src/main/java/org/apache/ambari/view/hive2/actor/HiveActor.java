package org.apache.ambari.view.hive2.actor;

import akka.actor.UntypedActor;
import org.apache.ambari.view.hive2.actor.message.HiveMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HiveActor extends UntypedActor {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    @Override
    final public void onReceive(Object message) throws Exception {
        HiveMessage hiveMessage = new HiveMessage(message);
        if(LOG.isDebugEnabled()){
            LOG.debug("Received message: " + message.getClass().getName() + ", generated id: " + hiveMessage.getId() +
                    " sent by: " + sender() + ", recieved by" + self());
        }

        handleMessage(hiveMessage);

        if(LOG.isDebugEnabled()){
            LOG.debug("Message submitted: " + hiveMessage.getId());

        }

    }

    abstract void handleMessage(HiveMessage hiveMessage);



}
