package jenkins.plugins.logstash.persistence;

import io.logz.sender.LogzioSender;
import io.logz.sender.com.google.gson.JsonObject;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import jenkins.plugins.logstash.LogstashConfiguration;
import net.sf.json.JSONObject;
import net.sf.json.test.JSONAssert;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.crypto.*"})
@PrepareForTest(LogstashConfiguration.class)
public class LogzioDaoTest {

    private static final String data = "{\"a\":{\"b\":1,\"c\":2,\"d\":[false, true]},\"e\":\"f\",\"g\":2.3}";
    private static final String flat_data = "\"a_b\":1,\"a_c\":2,\"a_d[0]\":false,\"a_d[1]\":true,\"e\":\"f\",\"g\":2.3";
    private static final String EMPTY_STRING_WITH_DATA = "{\"@buildTimestamp\":\"2000-01-01\"," + flat_data + ",\"message\":[],\"source\":\"jenkins\",\"source_host\":\"http://localhost:8080/jenkins\",\"@version\":1}";
    private static final String ONE_LINE_STRING_WITH_DATA = "{\"@buildTimestamp\":\"2000-01-01\"," + flat_data + ",\"message\":[\"LINE 1\"],\"source\":\"jenkins\",\"source_host\":\"http://localhost:8080/jenkins\",\"@version\":1}";
    private static final String TWO_LINE_STRING_WITH_DATA = "{\"@buildTimestamp\":\"2000-01-01\"," + flat_data + ",\"message\":[\"LINE 1\", \"LINE 2\"],\"source\":\"jenkins\",\"source_host\":\"http://localhost:8080/jenkins\",\"@version\":1}";
    private static final String EMPTY_STRING_NO_DATA = "{\"@buildTimestamp\":\"2000-01-01\",\"message\":[],\"source\":\"jenkins\",\"source_host\":\"http://localhost:8080/jenkins\",\"@version\":1}";
    private static final String ONE_LINE_STRING_NO_DATA = "{\"@buildTimestamp\":\"2000-01-01\",\"message\":[\"LINE 1\"],\"source\":\"jenkins\",\"source_host\":\"http://localhost:8080/jenkins\",\"@version\":1}";
    private static final String TWO_LINE_STRING_NO_DATA = "{\"@buildTimestamp\":\"2000-01-01\",\"message\":[\"LINE 1\", \"LINE 2\"],\"source\":\"jenkins\",\"source_host\":\"http://localhost:8080/jenkins\",\"@version\":1}";
    private LogzioDao dao;

    @Mock private LogzioSender logzioSender;
    @Mock private BuildData mockBuildData;
    @Mock private LogstashConfiguration logstashConfiguration;

    private LogzioDao createDao(String host, String key) throws LogzioParameterErrorException {
        return new LogzioDao(logzioSender, host, key);
    }

    @Before
    public void before() throws Exception {
        System.out.print("before\n"); //todo delete
        PowerMockito.mockStatic(LogstashConfiguration.class);
        when(LogstashConfiguration.getInstance()).thenReturn(logstashConfiguration);
        when(logstashConfiguration.getDateFormatter()).thenCallRealMethod();
        when(mockBuildData.getTimestamp()).thenReturn("2000-01-01");

        doNothing().when(logzioSender).start();
        doNothing().when(logzioSender).send(any(JsonObject.class));
        dao = createDao("http://localhost:8200/", "123456789");
    }

    @Test
    public void constructorSuccess1() throws Exception {
        // Unit under test
        dao = createDao("https://localhost:8201/", "123");

        // Verify results
        assertEquals("Wrong host name", "https://localhost:8201/", dao.getHost());
        assertEquals("Wrong key", "123", dao.getKey());
    }

    @Test
    public void buildPayloadSuccessEmpty(){
        when(mockBuildData.toString()).thenReturn("{}");
        // Unit under test
        JSONObject result = dao.buildPayload(mockBuildData, "http://localhost:8080/jenkins", new ArrayList<String>());
        result.remove("@timestamp");

        // Verify results
        JSONAssert.assertEquals("Results don't match", JSONObject.fromObject(EMPTY_STRING_NO_DATA), result);
    }

    @Test
    public void buildPayloadSuccessOneLine(){
        when(mockBuildData.toString()).thenReturn("{}");
        // Unit under test
        JSONObject result = dao.buildPayload(mockBuildData, "http://localhost:8080/jenkins", Arrays.asList("LINE 1"));
        result.remove("@timestamp");

        // Verify results
        JSONAssert.assertEquals("Results don't match", JSONObject.fromObject(ONE_LINE_STRING_NO_DATA), result);
    }

    @Test
    public void buildPayloadSuccessTwoLines(){
        when(mockBuildData.toString()).thenReturn("{}");
        // Unit under test
        JSONObject result = dao.buildPayload(mockBuildData, "http://localhost:8080/jenkins", Arrays.asList("LINE 1", "LINE 2"));
        result.remove("@timestamp");

        // Verify results
        JSONAssert.assertEquals("Results don't match", JSONObject.fromObject(TWO_LINE_STRING_NO_DATA), result);
    }

    @Test
    public void buildPayloadWithDataSuccessEmpty(){
        when(mockBuildData.toString()).thenReturn(data);
        // Unit under test
        JSONObject result = dao.buildPayload(mockBuildData, "http://localhost:8080/jenkins", new ArrayList<String>());
        result.remove("@timestamp");

        // Verify results
        JSONAssert.assertEquals("Results don't match", JSONObject.fromObject(EMPTY_STRING_WITH_DATA), result);
    }

    @Test
    public void buildPayloadWithDataSuccessOneLine(){
        when(mockBuildData.toString()).thenReturn(data);
        // Unit under test
        JSONObject result = dao.buildPayload(mockBuildData, "http://localhost:8080/jenkins", Arrays.asList("LINE 1"));
        result.remove("@timestamp");

        // Verify results
        JSONAssert.assertEquals("Results don't match", JSONObject.fromObject(ONE_LINE_STRING_WITH_DATA), result);
    }

    @Test
    public void buildPayloadWithDataSuccessTwoLines(){
        when(mockBuildData.toString()).thenReturn(data);
        // Unit under test
        JSONObject result = dao.buildPayload(mockBuildData, "http://localhost:8080/jenkins", Arrays.asList("LINE 1", "LINE 2"));
        result.remove("@timestamp");

        // Verify results
        JSONAssert.assertEquals("Results don't match", JSONObject.fromObject(TWO_LINE_STRING_WITH_DATA), result);
    }

    @Test
    public void pushNoMessage(){
        String jsonString = "{'message': []}";

        // Unit under test
        try {
            dao.push(jsonString);
            verify(logzioSender, times(0)).send(any(JsonObject.class));
        }catch (IOException e ){
            fail("Failed to push with no message");
        }
    }

    @Test
    public void pushOneMessage(){
        String jsonString = "{'message': ['bar1']}";

        // Unit under test
        try {
            dao.push(jsonString);
            verify(logzioSender, times(1)).send(any(JsonObject.class));
        }catch (IOException e ){
            fail("Failed to push with a message");
        }
    }

    @Test
    public void pushMultiMessages(){
        String jsonString = "{'message': ['bar1', 'bar2']}";

        // Unit under test
        try {
            dao.push(jsonString);
            verify(logzioSender, times(2)).send(any(JsonObject.class));
        }catch (IOException e ){
            fail("Failed to push with multi messages");
        }
    }
}
