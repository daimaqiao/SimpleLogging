package dmq.test.logging.log4j;

import dmq.test.logging.kafka.KafkaURIException;
import dmq.test.logging.kafka.sender.KafkaSender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static dmq.test.logging.log4j.Helper.SAVE_TIME;


// 为log4j定制的KafkaAppender
// 为满足KafkaSink的特殊需求，添加几个特殊控制
//  1. putExtraElement(String, Object)  是插入数据的时候，可以添加额外的键名与键值（添加到存档JSON中）
public class KafkaAppender extends AppenderSkeleton implements ICloseAppender {
    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaAppender.class);

    //  *** 重要 ***
    // 过虑kafka驱动程序包的前缀
    private static final String KAFKA_PREFIX= "org.apache.kafka.";


    private KafkaSender kafkaSender;// KafkaSender已提供TimedBuffer缓冲策略
    public KafkaAppender(String uri) {
        setName(this.getName());
        try {
            kafkaSender = new KafkaSender(uri, 50);
        } catch(KafkaURIException x) {
            kafkaSender= null;
            LOGGER.error("Can NOT init KafkaAppender/KafkaSender!", x);
        }// try/catch
    }

    private Calendar defaultCalendar= Calendar.getInstance();// 默认日历，用于统一时区
    public void resetCalendar(Calendar calendar) {
        defaultCalendar= calendar != null? calendar: Calendar.getInstance();
    }
    public void resetCalendar(TimeZone timezone) {
        defaultCalendar= timezone != null? Calendar.getInstance(timezone): Calendar.getInstance();
    }
    public void resetCalendar(String timezone) {
        resetCalendar((timezone == null || timezone.isEmpty())? TimeZone.getDefault(): TimeZone.getTimeZone(timezone));
    }

    private Map<String, Object> extraElements= new HashMap<String, Object>();
    public void putExtraElement(String key, Object val) {
        extraElements.put(key, val);
    }
    private String saveTime= SAVE_TIME;
    public void renameSaveTime(String name) {
        saveTime= (name == null || name.isEmpty())?
                SAVE_TIME: name;
    }
    private boolean writeSaveTime= true;
    public void disableSaveTime() {
        writeSaveTime= false;
    }

    // 过虑掉不必要的event时返回true
    private boolean filterEvent(LoggingEvent loggingEvent) {
        String name= loggingEvent.getLoggerName();
        return name.startsWith(KAFKA_PREFIX);
    }

    @Override// AppenderSkeleton
    protected void append(LoggingEvent loggingEvent) {
        if(kafkaSender == null)
            return;

        // 过虑掉不期望看到的日志，比如来自驱动程序的日志等
        if(filterEvent(loggingEvent))
            return;

        BSONObject object= Helper.formatEvent(defaultCalendar, loggingEvent);
        object.putAll(extraElements);
        if(writeSaveTime)
            object.put(saveTime, new Date());
        kafkaSender.sendJson(object.toString());
    }
    @Override// AppenderSkeleton
    public void close() {
        LOGGER.debug("Close mongo appender .");
        if(kafkaSender != null) {
            kafkaSender.close();
            kafkaSender= null;
        }
    }
    @Override// AppenderSkeleton
    public boolean requiresLayout() {
        return false;
    }

    public void flush() {
         if(kafkaSender != null)
            kafkaSender.flush();
    }

    @Override// ICloseAppender
    public void close(boolean flush) {
        if(flush)
            flush();
        close();
    }
}
