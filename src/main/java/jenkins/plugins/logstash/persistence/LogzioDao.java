package jenkins.plugins.logstash.persistence;

import com.github.wnameless.json.flattener.JsonFlattener;
import io.logz.sender.LogzioSender;
import io.logz.sender.SenderStatusReporter;
import io.logz.sender.com.google.gson.JsonObject;
import io.logz.sender.exceptions.LogzioParameterErrorException;

import jenkins.plugins.logstash.LogstashConfiguration;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import java.io.*;

import java.util.*;
import java.util.concurrent.Executors;

/**
 * Logz.io Data Access Object.
 *
 * @author Ido Halevi
 */
public class LogzioDao extends AbstractLogstashIndexerDao {
    private static final String TYPE = "jenkins_plugin";
    private LogzioSender logzioSender = null;

//    private List<String> buildDataExcludeList = new ArrayList<>();
    private String key;
    private String host;

    //primary constructor used by indexer factory
    public LogzioDao(String host, String key) throws LogzioParameterErrorException{
        this(null, host, key);
    }

    // Factored for unit testing
    LogzioDao(LogzioSender factory, String host, String key) throws LogzioParameterErrorException {
        if (host == null)
        {
            throw new IllegalArgumentException("host field must not be empty");
        }

        this.host = host;
        this.key = key;

        this.logzioSender = factory == null ? LogzioSender.getOrCreateSenderByType(key, TYPE, 2,
                98, new File("./logzio_jenkins"), host, 10 * 1000,
                10 * 1000,false, new LogzioDaoLogger(),
                Executors.newScheduledThreadPool(2),30) : factory;

        this.logzioSender.start();
    }

    @Override
    public void push(String data) throws IOException {
        JSONObject jsonData = JSONObject.fromObject(data);
        JSONArray jsonArray = jsonData.getJSONArray("message");
        for (Object msg : jsonArray) {
            JsonObject log = new JsonObject();
            log.addProperty("message", msg.toString());
            for (Object key : jsonData.keySet()){
                String keyStr = (String)key;
                if(!keyStr.equals("message")){
                    log.addProperty(keyStr, jsonData.getString(keyStr));
                }
            }
            this.logzioSender.send(log);
        }
    }

    @Override
    public JSONObject buildPayload(BuildData buildData, String jenkinsUrl, List<String> logLines) {
        JSONObject payload = new JSONObject();
        payload.put("message", logLines);
        payload.put("source", "jenkins");
        payload.put("source_host", jenkinsUrl);
        payload.put("@buildTimestamp", buildData.getTimestamp());
        payload.put("@timestamp", LogstashConfiguration.getInstance().getDateFormatter().format(Calendar.getInstance().getTime()));
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

    public class LogzioDaoLogger implements SenderStatusReporter {

        private final OutputStream logStream = System.out;

        private void pringLogMessage(String msg) {
            try {
                msg = msg + "\n";
                logStream.write(msg.getBytes());
                logStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        @Override
        public void error(String msg) { pringLogMessage("[LogzioSender]ERROR: " + msg); }

        @Override
        public void error(String msg, Throwable e) { pringLogMessage("[LogzioSender]ERROR: " + msg + "\n" +e); }

        @Override
        public void warning(String msg) {pringLogMessage("[LogzioSender]WARNING: " + msg);}

        @Override
        public void warning(String msg, Throwable e) {pringLogMessage("[LogzioSender]WARNING: " + msg + "\n" + e);}

        @Override
        public void info(String msg) {pringLogMessage("[LogzioSender]INFO: " + msg);}

        @Override
        public void info(String msg, Throwable e) {pringLogMessage("[LogzioSender]INFO: " + msg + "\n" + e);}
    }
}

