package dmq.test.logging.kafka;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


// 提供kafka uri，可以简化配置方式
// 典型的uri例如：kafka://192.168.56.211:9092/demo
// 其中“kafka://”是前缀，“192.168.56.211”是服务地址，“9092”是端口，“demo”是topic
public class KafkaURI {
    public static String KAFKA= "kafka://";
    public static int PORT= 9092;
    private static String PATTERN= "^"+KAFKA+"([^\\s/]+)/([^\\s/]+)\\??(.*)$";
    private static int GROUPS= 3;
    private static int GROUP_HOSTS= 1;
    private static int GROUP_TOPIC= 2;
    private static int GROUP_QUERY= 3;

    public static KafkaURI tryParse(String uri) {
        if(uri == null || uri.length() == 0)
            return null;
        if(!uri.startsWith(KAFKA))
            return null;

        try {
            return new KafkaURI(uri);
        } catch(Exception x) {
            x.printStackTrace();
        }
        return null;
    }

    private String uriString;
    private String serverHosts, topicName, queryString;
    public KafkaURI(String uri) throws KafkaURIException {
        uriString= uri;
        try {
            parseUri(uri);
            checkKafkaURI();
        } catch(KafkaURIException x) {
            throw x;
        } catch(Exception x) {
            throw new KafkaURIException(uri, x);
        }
    }

    private void parseUri(String uri) throws KafkaURIException {
        Pattern pattern= Pattern.compile(PATTERN);
        Matcher matcher= pattern.matcher(uri);

        if(!matcher.matches() || matcher.groupCount() != GROUPS)
            throw new KafkaURIException(uri);

        serverHosts= matcher.group(GROUP_HOSTS);
        topicName= matcher.group(GROUP_TOPIC);
        queryString= matcher.group(GROUP_QUERY);

        resolvePort();
    }
    private void resolvePort() {
        if(serverHosts == null || serverHosts.isEmpty())
            return;

        int pos= serverHosts.indexOf(':');
        if(pos < 0)
            serverHosts= String.format("%s:%s", serverHosts, PORT);
    }

    private void checkKafkaURI() throws KafkaURIException {
        if(serverHosts == null || serverHosts.length() == 0)
            throw new KafkaURIException(uriString, "Bad server hosts!");
        if(topicName == null || topicName.length() == 0)
            throw new KafkaURIException(uriString, "Bad topic name!");

    }

    public String getServerHosts() {
        return serverHosts;
    }
    public String getTopicName() {
        return topicName;
    }
    public String getQueryString() {
        return queryString;
    }

    @Override
    public String toString() {
        return uriString;
    }
}
