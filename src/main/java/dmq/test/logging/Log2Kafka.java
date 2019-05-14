package dmq.test.logging;

import dmq.test.logging.log4j.KafkaAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

// 将必要日志提交至kafka
// 配置示例：log2kafka_uri=kafka://192.168.0.1:9090/topic
public class Log2Kafka {
    public static final String LOG2KAFKA_URI= "log2kafka_uri";

    private final Level level;
    private final String uri;
    public Log2Kafka(String uri, Level level) {
        this.uri= uri;
        this.level= level;
    }
    public Log2Kafka(Level level) {
        this(System.getProperty(LOG2KAFKA_URI, ""), level);
    }

    // 将实例化的MongoAppender添加到log4j的RootLogger
    List<KafkaAppender> appenderList= new ArrayList<>();
    KafkaAppender kafkaAppender = null;
    public void appendKafkaAppender() {
        if(uri.length() == 0)
            return;

        if(kafkaAppender == null) {
            kafkaAppender = new KafkaAppender(uri);
            appenderList.add(kafkaAppender);
            kafkaAppender.setName(uri);
            kafkaAppender.setThreshold(level);
            //mongoAppender.putExtraElement("hello", "log2mongo_test");
            //mongoAppender.renameSaveTime("save_time");
            Logger.getRootLogger().addAppender(kafkaAppender);
        }
    }

    public void removeKafkaAppender() {
        if(kafkaAppender != null) {
            Logger.getRootLogger().removeAppender(kafkaAppender);
            kafkaAppender.close();
            kafkaAppender = null;
        }
    }

    public String dump() {
        return String.format("level=%s, uri=%s",
                level.toString(), uri);
    }

    public void flush() {
        appenderList.forEach(x-> {
            try {
                if (x != null) {
                    x.flush();
                }
            } catch(Exception ex) {
                ex.printStackTrace();
            }
        });
    }
    public void close() {
        appenderList.forEach(x-> {
            try {
                if (x != null) {
                    x.close();
                }
            } catch(Exception ex) {
                ex.printStackTrace();
            }
        });
    }

}
