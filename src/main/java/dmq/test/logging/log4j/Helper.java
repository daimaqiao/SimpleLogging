package dmq.test.logging.log4j;

import org.apache.log4j.Appender;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.util.*;


public class Helper {
    static final String SAVE_TIME= "savetime";

    private static final int MAX_THROWABLE= 5;// throwable最大嵌套层次

    public static void removeAndCloseLocalAppenders(boolean flush) {
        List<ICloseAppender> list= new LinkedList<>();
        Enumeration enumeration=org.apache.log4j.Logger.getRootLogger().getAllAppenders();
        if(enumeration != null) {
            while(enumeration.hasMoreElements()) {
                Object object= enumeration.nextElement();
                if(object != null && object instanceof  ICloseAppender)
                    list.add((ICloseAppender)object);
            }// while
        }
        list.forEach(x-> {
            // remove appender
            org.apache.log4j.Logger.getRootLogger().removeAppender((Appender)x);
            // close appender
            x.close(flush);
        });
    }

    // 只转换一些常用的信息
    static String formatTime(Calendar calendar, long time) {
        calendar.setTimeInMillis(time);
        int offset= calendar.get(Calendar.ZONE_OFFSET);
        return String.format("%4d-%02d-%02d %02d:%02d:%02d %+03d%02d",
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH)+1,// 月份是0~11
                calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE),
                calendar.get(Calendar.SECOND),
                offset/3600000,
                Math.abs(offset)%3600000/60000);
    }

    static BSONObject formatEvent(Calendar calendar, LoggingEvent event) {
        BasicBSONObject object= new BasicBSONObject();
        object.put("level", event.getLevel().toString());
        object.put("message", event.getMessage());

        object.put("timestamp", new Date(event.getTimeStamp()));
        object.put("timetext", formatTime(calendar, event.getTimeStamp()));

        LocationInfo info= event.getLocationInformation();
        object.put("file", info.getFileName());
        object.put("line", info.getLineNumber());
        object.put("class", info.getClassName());
        object.put("method", info.getMethodName());

        object.put("exception", formatThrowable(event.getThrowableInformation()));

        return object;
    }
    static BSONObject formatThrowable(ThrowableInformation info) {
        if(info == null)
            return null;

        BasicBSONObject object= new BasicBSONObject();
        String[] array= info.getThrowableStrRep();
        object.put("trace", array==null?null: String.join("\n", array));

        Throwable throwable= info.getThrowable();
        if(throwable != null) {
            object.put("name", throwable.getClass().getCanonicalName());
            object.put("message", throwable.getMessage());

            object.put("cause", formatThrowable(throwable.getCause(), 0));
        }

        return object;
    }
    static BSONObject formatThrowable(Throwable throwable, int level) {
        if(throwable == null)
            return null;
        BasicBSONObject object= new BasicBSONObject();
        if(level < MAX_THROWABLE) {
            object.put("name", throwable.getClass().getCanonicalName());
            object.put("message", throwable.getMessage());
            object.put("cause", formatThrowable(throwable.getCause(), level+1));
        } else {
            object.put("name", "discarded");
            object.put("message", String.format("Cause level more than %d will be discarding.", level));
            object.put("cause", null);
        }

        return object;
    }

}
