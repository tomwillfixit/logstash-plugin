package jenkins.plugins.logstash.persistence;

import com.github.wnameless.json.flattener.JsonFlattener;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import jenkins.plugins.logstash.LogstashConfiguration;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Logz.io Data Access Object.
 *
 * @author Ido Halevi
 */

public class LogzioDao extends AbstractLogstashIndexerDao {
    private static final String TYPE = "jenkins_plugin";
    private final LogzioSender logzioSender;

    private String key;
    private String host;
    private int counter; //todo delete

    //primary constructor used by indexer factory
    public LogzioDao(String host, String key){
        this(null, host, key);
    }

    // Factored for unit testing
    LogzioDao(LogzioSender factory, String host, String key){
        System.out.print("LogzioDao\n"); //todo delete
        this.host = host;
        this.key = key;
        this.logzioSender = factory == null ? new LogzioSender(key, host) : factory;
    }

    @Override
    public void push(String data) {
        JSONObject jsonData = JSONObject.fromObject(data);
        JSONArray logMessages = jsonData.getJSONArray("message");
        for (Object logMsg : logMessages) {
            JSONObject logLine = createLogLine(jsonData, logMsg.toString());
            System.out.print("add: " + counter + "\n"); //todo delete
            this.counter++;//todo delete
            this.logzioSender.add(logLine);
        }
        System.out.print("flush\n"); //todo delete
        this.logzioSender.flush();
    }

    protected JSONObject createLogLine(JSONObject jsonData, String logMsg) {
        JSONObject logLine = new JSONObject();
        logLine.put("message", logMsg);
        logLine.put("@timestamp", LogstashConfiguration.getInstance().getDateFormatter().format(Calendar.getInstance().getTime()));
        logLine.put("micro_timestamp", TimeUnit.NANOSECONDS.toMicros(System.nanoTime()));
        jsonData.keySet().stream()
        .filter(key -> !(key.equals("message")))
        .forEach(key -> logLine.put(key.toString(), jsonData.getString(key.toString())));
        return logLine;
    }

    @Override
    public JSONObject buildPayload(BuildData buildData, String jenkinsUrl, List<String> logLines) {
        JSONObject payload = new JSONObject();
        payload.put("message", logLines);
        payload.put("source", "jenkins");
        payload.put("source_host", jenkinsUrl);
        payload.put("@buildTimestamp", buildData.getTimestamp());
        payload.put("@version", 1);
        // flatten build data
        Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(buildData.toString());
        for (Map.Entry<String, Object> entry : flattenJson.entrySet()) {
            String key = entry.getKey().replace('.','_');
            Object value = entry.getValue();
            payload.put(key, value);
        }

        return payload;
    }

    @Override
    public String getDescription(){ return host; }

    public String getHost(){ return host; }

    public String getKey(){ return key; }

    public String getType(){ return TYPE; }

    public class LogzioSender{
        private static final int MAX_SIZE_IN_BYTES = 3 * 1024 * 1024;  // 3 MB
        static final int INITIAL_WAIT_BEFORE_RETRY_MS = 2000;
        static final int MAX_RETRIES_ATTEMPTS = 3;
        private static final int CONNECT_TIMEOUT = 10*1000;
        private static final int SOCKET_TIMEOUT = 10*1000;
        private final String logzioToken;
        private final String logzioUrl;
        private final String DEFAULT_URL = "https://listener.logz.io:8071";
        private final Logger logger;
        private URL logzioListenerUrl;
        private HttpURLConnection conn;
        private ByteArrayOutputStream logLines;

        LogzioSender(String logzioToken, String logzioUrl){
            this.logzioToken = logzioToken;
            if (logzioUrl == null)
                logzioUrl = DEFAULT_URL;
            this.logzioUrl = logzioUrl;
            this.logger = Logger.getLogger("LogzioSender");
            this.logLines = new ByteArrayOutputStream();
            try {
                logzioListenerUrl = new URL(this.logzioUrl + "/?token=" + this.logzioToken + "&type=" + TYPE);
            } catch (MalformedURLException e) {
                logger.severe("[LogzioSender] Can't connect to Logzio: " +e.getMessage());
            }
            logger.info("[LogzioSender] Created new LogzioSender class");
        }

        private void reset(){
            this.logLines.reset();
        }

        void flush(){
            if (logLines.size() != 0){
                this.sendToLogzio();
                this.reset();
            }
        }

        void add(JSONObject logLine){
            try {
                logLines.write((logLine.toString() + "\n").getBytes("utf-8"));
                if (logLines.size() >= MAX_SIZE_IN_BYTES){
                    this.sendToLogzio();
                    this.reset();
                }
            }catch (IOException e){
                this.logger.severe("[LogzioSender] Something went wrong. Can't add " + logLine.toString() + ": " + e.getMessage());
            }
        }

        private boolean shouldRetry(int statusCode) {
            boolean shouldRetry = true;
            switch (statusCode) {
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_BAD_REQUEST:
                case HttpURLConnection.HTTP_UNAUTHORIZED:
                    shouldRetry = false;
                    break;
            }
            return shouldRetry;
        }

        private void sendToLogzio(){
            logger.info("sendToLogzio\n"); //todo delete
            try {
                int currentRetrySleep = INITIAL_WAIT_BEFORE_RETRY_MS;
                for (int currTry = 1; currTry <= MAX_RETRIES_ATTEMPTS; currTry++) {
                    boolean shouldRetry = true;
                    int responseCode;
                    String responseMessage;
                    IOException savedException = null;
                    try {
                        conn = (HttpURLConnection) logzioListenerUrl.openConnection();
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-length", String.valueOf(this.logLines.size()));
                        conn.setRequestProperty("Content-Type", "text/plain");
                        conn.setReadTimeout(SOCKET_TIMEOUT);
                        conn.setConnectTimeout(CONNECT_TIMEOUT);
                        conn.setDoOutput(true);
                        conn.setDoInput(true);
                        conn.getOutputStream().write(this.logLines.toByteArray());

                        responseCode = conn.getResponseCode();
                        responseMessage = conn.getResponseMessage();

                        if (responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
                            BufferedReader bufferedReader = null;
                            try {
                                StringBuilder problemDescription = new StringBuilder();
                                InputStream errorStream = this.conn.getErrorStream();
                                if (errorStream != null) {
                                    bufferedReader = new BufferedReader(new InputStreamReader((errorStream)));
                                    bufferedReader.lines().forEach(line -> problemDescription.append("\n").append(line));
                                    logger.severe(String.format("[LogzioSender] Got 400 from logzio, here is the output: %s", problemDescription));
                                }
                            } finally {
                                if (bufferedReader != null) {
                                    try {
                                        bufferedReader.close();
                                    } catch(Exception ignored) {}
                                }
                            }
                        }
                        if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
                            logger.severe("[LogzioSender] God forbidden! Your token is not right. Unfortunately, dropping logs. Message: " + responseMessage);
                        }
                        shouldRetry = shouldRetry(responseCode);
                    } catch (IOException e) {
                        savedException = e;
                        logger.warning("[LogzioSender] Got IO exception - " + e.getMessage());
                    }
                    if (!shouldRetry) {
                        logger.info("[LogzioSender] Successfully sent bulk to logz.io, size: " + logLines.size());
                        break;
                    } else {
                        if (currTry == MAX_RETRIES_ATTEMPTS) {
                            if (savedException != null) {
                                logger.severe("[LogzioSender] Got IO exception on the last bulk try to logz.io " + savedException.getMessage());
                            }
                            // Giving up, something is broken on Logz.io side, we will try again later
                            return;
                        }
                        logger.info("[LogzioSender] Could not send log to logz.io, retry (" + currTry + "/" + MAX_RETRIES_ATTEMPTS + ")");
                        logger.info("[LogzioSender] Sleeping for " + currentRetrySleep + " ms and will try again.");
                        Thread.sleep(currentRetrySleep);
                        currentRetrySleep *= 2;
                    }
                }
            } catch (InterruptedException e) {
                logger.warning("[LogzioSender] Got interrupted exception");
                Thread.currentThread().interrupt();
            }
        }

    }
}

