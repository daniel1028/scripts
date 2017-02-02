package org.demo.scripts;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CompareCfRowCount {

  private final static ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

  private static Session cluster1Session = null;
  private static Session cluster2Session = null;

  public static void main(String args[]) {
    System.out.println("Daniel.......");
    initializeCluster1();
    initializeCluster2();
    ResultSet columnFamilies = getAllColumnFamilies("event_logger_insights");
    if (columnFamilies != null) {
      for (Row cfName : columnFamilies) {
        long cluster1RowCount = 0;
        long cluster2RowCount = 0;
        String status = null;
        System.out.println(cfName.getString("table_name"));
        try {
          ResultSet cluster1Rows = getCfRowCount(cluster1Session, "event_logger_insights", cfName.getString("table_name"));
          for (Row cluster1Row : cluster1Rows) {
            cluster1RowCount = cluster1Row.getLong(1);
          }
        } catch (Exception e) {
          status = e.getMessage();
        }

        try {
          ResultSet cluster2Rows = getCfRowCount(cluster2Session, "event_logger_insights", cfName.getString("table_name"));

          for (Row cluster2Row : cluster2Rows) {
            cluster2RowCount = cluster2Row.getLong(1);
          }
        } catch (Exception e) {
          status = e.getMessage();
        }

        if (cluster1RowCount == cluster2RowCount) {
          status = "matched";
        } else {
          status = "not-matched";
        }

        saveStatus("cluster1", cfName.getString("table_name"), cluster1RowCount, status);
        saveStatus("cluster2", cfName.getString("table_name"), cluster1RowCount, status);
      }
    }
  }

  private static void initializeCluster1() {

    Cluster cluster = Cluster.builder().withClusterName("cassandra").addContactPoint("127.0.0.1").withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    cluster1Session = cluster.connect("event_logger_insights");
    System.out.println("Cluster2 initialized successfully...");
  }

  private static void initializeCluster2() {

    Cluster cluster = Cluster.builder().withClusterName("cassandra").addContactPoint("127.0.0.1").withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    cluster2Session = cluster.connect("event_logger_insights");
    System.out.println("Cluster2 initialized successfully...");
  }

  private static ResultSet getCfRowCount(Session cassSession, String keyspaceName, String cfName) throws Exception {
    ResultSet result = null;
    Statement selectCount = QueryBuilder.select().countAll().from(keyspaceName, cfName).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    ResultSetFuture resultSetFuture = cassSession.executeAsync(selectCount);
    result = resultSetFuture.get();
    return result;
  }

  private static void saveStatus(String clusterName, String cfName, long rowCount, String status) {
    Insert insertStmt = QueryBuilder.insertInto("event_logger_insights", "cf_wise_row_count").value("cf_name", cfName)
            .value("cluster_name", clusterName).value("row_count", rowCount).value("status", status);
    ResultSetFuture resultSetFuture = cluster1Session.executeAsync(insertStmt);
    try {
      resultSetFuture.get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static ResultSet getAllColumnFamilies(String keyspaceName) {
    ResultSet result = null;
    Statement selectStmt =
            QueryBuilder.select().column("table_name").from("system", "size_estimates").where(QueryBuilder.eq("keyspace_name", keyspaceName));
    ResultSetFuture resultSetFuture = cluster2Session.executeAsync(selectStmt);
    try {
      result = resultSetFuture.get();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

  /**
   * 
   * Create this columnfamily in cluster 1
   * 
   * CREATE TABLE event_logger_insights.cf_wise_row_count ( cf_name text,
   * cluster_name text, row_count bigint, status text, PRIMARY KEY (cf_name,
   * cluster_name) );
   * 
   */
}
