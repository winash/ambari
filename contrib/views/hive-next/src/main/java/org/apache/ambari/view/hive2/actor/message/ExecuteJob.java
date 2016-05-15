package org.apache.ambari.view.hive2.actor.message;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Message to be sent when a statement has to be executed
 */
public class ExecuteJob {
    private final String jobId;
    private final String username;
    private final String[] statements;

    public ExecuteJob(String jobId, String username, String[] statements) {
        this.jobId = jobId;
        this.username = username;
        this.statements = statements;
    }

    public String getJobId() {
        return jobId;
    }

    public String[] getAllStatements() {
        return statements;
    }

    /**
     * Get the statements to be executed synchronously
     * @return
     */
    public Collection<String> getSyncStatements() {
        if (!(statements.length > 1))
            return Collections.emptyList();
        else
            return ImmutableList.copyOf(Arrays.copyOfRange(statements, 0, statements.length - 2));
    }

    /**
     * Get the statement to be executed asynchronously
     * @return async statement
     */
    public String getAsyncStatement(){
        return statements[statements.length-1];
    }

    public String getUsername() {
        return username;
    }
}
