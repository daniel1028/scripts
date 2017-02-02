package org.demo.scripts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ImportCSV {

  private static Session cluster1Session = null;

  public static void main(String args[]) {
    initializeCluster1();
    String filePath = args[0];
    //System.out.println("fileName->" + filePath);
    File f = new File(filePath);
    if (f.exists()) {
      processCsv(filePath);
    }
  }

  private static void processCsv(String file) {
    BufferedReader fileReader = null;
    try {
      String line = "";
      String cvsSplitBy = ",";

      fileReader = new BufferedReader(new FileReader(file));
      while ((line = fileReader.readLine()) != null) {

        // use comma as separator
        String[] statMetrics = line.split(cvsSplitBy);
        System.out.println("gooruOid:" + statMetrics[0].trim());
        saveStatus(statMetrics[0].trim());

      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fileReader != null) {
        try {
          fileReader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void saveStatus(String gooruOid) {
    Insert insertStmt = QueryBuilder.insertInto("event_logger_insights", "stat_publisher_queue").value("metrics_name", "migrateMetrics")
            .value("gooru_oid", gooruOid).value("event_time", 111111111L).value("type", "resource");
    ResultSetFuture resultSetFuture = cluster1Session.executeAsync(insertStmt);
    try {
      resultSetFuture.get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void initializeCluster1() {

    // Cluster cluster =
    // Cluster.builder().withClusterName("Cassandra-3.0").addContactPoint("cassandra-analytics-01.internal.gooru.org")
    Cluster cluster = Cluster.builder().withClusterName("Events Prod Cluster").addContactPoint("52.53.227.2")
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE).withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    cluster1Session = cluster.connect("event_logger_insights");
    System.out.println("Cluster2 initialized successfully...");
  }
}
