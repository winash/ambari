package org.apache.ambari.view.hive2.internal;


import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;

/**
 * Holder for query submitted by the user
 * This may contain multiple hive queries
 */
public class HiveQuery {

    private String query;

    public HiveQuery(String query) {
        this.query = query;
    }

    public HiveQueries fromMultiLineQuery(String multiLineQuery){
        return new HiveQueries(multiLineQuery);
    }


    public static class HiveQueries{

        static final String QUERY_SEP = ";";
        Collection<HiveQuery> hiveQueries;

        private HiveQueries(String userQuery) {
            hiveQueries = FluentIterable.from(Arrays.asList(userQuery.split(QUERY_SEP)))
                    .transform(new Function<String, HiveQuery>() {
                        @Nullable
                        @Override
                        public HiveQuery apply(@Nullable String input) {
                            return new HiveQuery(input.trim());
                        }
                    }).toList();
        }





    }




};
