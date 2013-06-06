/*
 * Copyright 2013 Simply Measured, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.simplymeasured.prognosticator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * @author rob@simplymeasured.com
 * @since 5/6/13
 */
public class HiveJDBCQueryImpl implements HiveJDBCQuery {
    private static final Log LOG = LogFactory.getLog(HiveJDBCQueryImpl.class);
    private ExecutorService executorService;
    private DataSource dataSource;

    public HiveJDBCQueryImpl() {
    }

    @Override
    public Iterator<Map<String, Object>> runQuery(String resultTable, String queryStatement,
                                                  Map<String, Object> parameters) {
        final ArrayBlockingQueue<Map<String, Object>> rowQueue = new ArrayBlockingQueue<Map<String, Object>>(1000);

        ThreadedQueryRunnable runnable =
                new ThreadedQueryRunnable(dataSource, queryStatement, parameters, rowQueue);

        executorService.submit(runnable);

        return new Iterator<Map<String, Object>>() {
            private boolean done = false;
            private Map<String, Object> cachedRow = null;
            private final Map<String, Object> emptyMap = Collections.emptyMap();

            @Override
            public boolean hasNext() {
                try {
                    if(done)
                        return false;

                    cachedRow = rowQueue.take();

                    if(cachedRow == null || cachedRow == emptyMap) {
                        done = true;
                        return false;
                    }

                    return true;
                } catch(InterruptedException ie) {
                    throw new RuntimeException("Iterator thread killed!", ie);
                }
            }

            @Override
            public Map<String, Object> next() {
                if(done || cachedRow == emptyMap) {
                    throw new IllegalStateException("End of iterator reached");
                } else if(cachedRow == null) {
                    boolean hasMore = hasNext();

                    if(!hasMore) {
                        done = true;
                        throw new IllegalStateException("End of iterator reached");
                    }
                }

                return cachedRow;
            }

            @Override
            public void remove() {
                // intentionally non-op
            }
        };
    }

    @Required
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Required
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
}
