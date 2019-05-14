package dmq.test.logging.log4j;

import dmq.test.logging.common.TimedBuffer;
import dmq.test.logging.mongo.MongoSink;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;

// 为log4j定制的MongoAppender
// 为满足MongoSink的特殊需求，添加几个特殊控制
//  1. putExtraElement(String, Object)  是插入数据的时候，可以添加额外的键名与键值（添加到存档JSON中）
//  2. ignoreException(false)   在构造mongodb连接时，确认连接可用性并忽略底层异常（配置为放弃数据）。默认false，用于构造函数
public class MongoAppender extends AppenderSkeleton
        implements TimedBuffer.BufferHandler<BSONObject>, TimedBuffer.BufferDroppedNotify<BSONObject>, ICloseAppender{
    private static final Logger LOGGER= LoggerFactory.getLogger(MongoAppender.class);


    //  *** 重要 ***
    // 过虑mongodb驱动程序包的前缀
    private static final String MONGO_PREFIX= "org.mongodb.";


    // 缓存多一些的时候，IO性能会好一些
    // 但是遇到异常问题，缓存内没有存盘的信息就有可能会丢失
    private static final int SIZE_BUFFER= TimedBuffer.MAX_CAPACITY;     // 最大缓存数量，默认10000，超出的数据被丢弃
    private static final int SIZE_IN_BULK= TimedBuffer.THRESHOLD_SIZE;  // 批量写入的数量，默认1000
    private static final int TIME_IN_BULK= TimedBuffer.THRESHOLD_TIME;  // 定时写入的周期，默认1000 (ms)
    private static final int SIZE_POOL= TimedBuffer.MAX_THREADS;        // 定时服务线程池大小
    private static final boolean PER_DELAY= TimedBuffer.FIX_DELAY;      // 使用两次调用之间的间隔时间计算延时

    private MongoSink mongoSink;
    private TimedBuffer<BSONObject> timedBuffer;
    private final boolean usingTimedBuffer;// 是否启用timedBuffer，默认true
    private final Calendar defaultCalendar;// 默认日历，根据MongoSink的timezone设置默认日历
    // 启用了timedBuffer的appender，写入mongoSink的数据是异步完成的，因此底层mongodb的响应效率，不会影响logging的执行效率
    // 但是mongoSink的写入平均效率低于logging的平均执行效率时，notifyBufferDroppped应该会有所通报
    public MongoAppender(String uri, boolean usingTimedBuffer, boolean ignoreException) {
        this.usingTimedBuffer= usingTimedBuffer;
        setName(this.getName());
        mongoSink= new MongoSink(uri, ignoreException);
        defaultCalendar= Calendar.getInstance(mongoSink.getCollectionTimezone());
        if(usingTimedBuffer)
            timedBuffer= new TimedBuffer<BSONObject>(SIZE_BUFFER, SIZE_IN_BULK, TIME_IN_BULK, SIZE_POOL, PER_DELAY,
                    this, this);
        else
            timedBuffer= null;
    }
    public MongoAppender(String uri) {
        // 默认打开TimedBuffer，忽略MongoSink的异常
        this(uri, true, true);
    }

    public void putExtraElement(String key, Object val) {
        mongoSink.putExtraElement(key, val);
    }
    public void renameSaveTime(String name) {
        mongoSink.renameSaveTime(name);
    }


    // 过虑来自mongodb驱动程序的日志，通常是TRACE, DEBUG, INFO等
    private boolean filterEvent(LoggingEvent loggingEvent) {
        String name= loggingEvent.getLoggerName();
        return name.startsWith(MONGO_PREFIX);
    }


    @Override// AppenderSkeleton
    protected void append(LoggingEvent loggingEvent) {
        // 过虑掉不期望看到的日志，比如来自mongodb驱动程序的日志等
        if(filterEvent(loggingEvent))
            return;

        BSONObject object= Helper.formatEvent(defaultCalendar, loggingEvent);
        // TimedBuffer可以改善连续向mongodb写入数据的性能
        if(usingTimedBuffer)
            timedBuffer.put(object);
        else
            mongoSink.write(object);
    }
    @Override// AppenderSkeleton
    public void close() {
        LOGGER.debug("Close mongo appender .");
        if(timedBuffer != null) {
            timedBuffer.close(() -> mongoSink.close());
            timedBuffer= null;
        }
        else {
            mongoSink.close();
            mongoSink= null;
        }
    }
    @Override// AppenderSkeleton
    public boolean requiresLayout() {
        return false;
    }


    @Override// TimedBuffer.BufferHandler<BSONObject>
    public void processBuffer(List<BSONObject> bufferList) {
        //System.out.printf("processBuffer: writing in bulk (count= %d)\n", bufferList.size());
        LOGGER.trace("processBuffer: writing in bulk (count= {})", bufferList.size());
        // 使用timedBuffer批量写入数据
        mongoSink.writeList(bufferList);
    }

    @Override// TimedBuffer.BufferDroppedNotify<BSONObject>
    public void notifyBufferDroppped(long count) {
        String message= String.format("notifyBufferDropped: something dropped by the buffer. full? (count= %d)", count);
        //System.out.println(message);
        LOGGER.trace(message);
        // 出现这个提示，通常表示缓存过程中出现过过载的情况
        // 这个信息可能对优化服务很有必要，作为一种特殊日志信息写入mongoSink
        mongoSink.message(message);
    }

    @Override// ICloseAppender
    public void close(boolean flush) {
        close();
    }
}
