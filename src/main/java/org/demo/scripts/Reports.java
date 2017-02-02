package org.demo.scripts;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;

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

public class Reports {

  private final static ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

  private static Session cluster1Session = null;

  private static final Timer timer = new Timer();

  private static final String BASE_PATH = "/opt/nucleus/reports/";

  public static void main(String[] a) {
    initializeCluster1();

    String reportUrl =
            "http://nucleus-qa.gooru.org/api/nucleus-download-reports/v1/class/{classUid}/course/{courseUid}/download/request?sessionToken={token}&userId={userId}";
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        ResultSet classIdsInfo = null;
        try {
          classIdsInfo = getClassIds();
        } catch (Exception e) {
          e.printStackTrace();
        }
        if (classIdsInfo != null) {
          for (Row classIdInfo : classIdsInfo) {
            String classId = classIdInfo.getString("class_uid");
            String courseId = classIdInfo.getString("course_uid");
            String emailId = classIdInfo.getString("email_id");
            String userId = classIdInfo.getString("user_uid");
            String zippedReport = BASE_PATH + classId + "-" + userId + ".zip";
            System.out.println("zipped report path->" + zippedReport);
            File f = new File(zippedReport);
            if (!f.exists()) {
               String token = "nothing";
              //String token = getAccessToken(emailId);
                String url = reportUrl.replace("{classUid}", classId).replace("{courseUid}", courseId).replace("{token}", token).replace("{userId}",
                        userId);
                // System.out.println("url ->" + url);
                int status = generateReport(url);
                System.out.println("Status->" + status + " - class ->" + classId + " - user ->" + userId);
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                saveStatus(classId, courseId, userId, String.valueOf(status));
             
            } else {
              System.out.println("Report already available. ");
              saveStatus(classId, courseId, userId, "exists");

            }
          }
        }
      }
    };
    timer.scheduleAtFixedRate(task, 0, 6000L);
  }

  private static String getAccessToken(String emailId) {
    HttpClient httpClient = null;
    HttpResponse response = null;
    String accessToken = null;
    HttpPost postClient = new HttpPost("http://www.gooru.org/api/nucleus-auth/v1/authorize");
    JSONObject inputJson = new JSONObject();
    JSONObject user = new JSONObject();
    inputJson.put("client_key", "c2hlZWJhbkBnb29ydWxlYXJuaW5nLm9yZw==");
    inputJson.put("grant_type", "google");
    inputJson.put("client_id", "ba956a97-ae15-11e5-a302-f8a963065976");
    inputJson.put("return_url", "http://www.gooru.org");
    user.put("identity_id", emailId);
    inputJson.put("user", user);

    try {
      postClient.setHeader("Content-Type", "application/json");
      StringEntity entity = new StringEntity(inputJson.toString());
      entity.setContentType("application/json");
      postClient.setEntity(entity);
      httpClient = HttpClientBuilder.create().build();
      response = httpClient.execute(postClient);
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
      StringBuffer result = new StringBuffer();
      String line = "";
      while ((line = rd.readLine()) != null) {
        result.append(line);
      }
      JSONObject results = new JSONObject(result.toString());
      // System.out.println("response -> " + results);
      accessToken = results.getString("access_token");
      rd.close();
    } catch (Exception e) {
      System.out.println("Exception");
      e.printStackTrace();
    }
    return accessToken;
  }

  private static int generateReport(String url) {
    HttpGet getClient = new HttpGet(url);
    HttpResponse response = null;
    HttpClient httpClient = null;
    try {

      httpClient = HttpClientBuilder.create().build();
      response = httpClient.execute(getClient);
      return response.getStatusLine().getStatusCode();
    } catch (ClientProtocolException e) {
      System.out.println("ClientProtocolException");
      e.printStackTrace();
    } catch (IOException e) {
      System.out.println("IOException");
      e.printStackTrace();
    }
    return 500;
  }

  private static ResultSet getClassIds() throws Exception {
    ResultSet result = null;
    Statement selectCount = QueryBuilder.select().all().from("event_logger_insights", "archieved_job_queue")
            .where(QueryBuilder.eq("status", "Availed")).limit(50).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
    ResultSetFuture resultSetFuture = cluster1Session.executeAsync(selectCount);
    result = resultSetFuture.get();
    return result;
  }

  private static void saveStatus(String classUid, String courseUid, String userUid, String status) {
    Insert insertStmt = QueryBuilder.insertInto("event_logger_insights", "archieved_job_queue").value("key", "job").value("class_uid", classUid)
            .value("course_uid", courseUid).value("user_uid", userUid).value("status", status);
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
    Cluster cluster = Cluster.builder().withClusterName("us-west").addContactPoint("52.53.178.25").withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    cluster1Session = cluster.connect("event_logger_insights");
    System.out.println("Cluster2 initialized successfully...");
  }
}
