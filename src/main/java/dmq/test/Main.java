package dmq.test;

import dmq.test.logging.Log2Kafka;
import dmq.test.logging.Log2Mongo;
import dmq.test.utils.Local;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final String LOG2MONGO_URI= Log2Mongo.LOG2MONGO_URI;
    private static final String LOG2KAFKA_URI= Log2Kafka.LOG2KAFKA_URI;
    private static final String MONGO_URI= "mongodb://localhost/database.collection?collection_append=false";
    private static final String KAFKA_URI= "kafka://localhost/topic";
    private static final String DEMO_RUN= "java -jar";
    private static final String DEMO_JAR= "<SimpleLogging-xxx.jar>";

    private static final Logger LOGGER= LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        for(String one: args) {
            if(one.startsWith("mongodb://"))
                System.setProperty(LOG2MONGO_URI, one);
            else if(one.startsWith("kafka://"))
                System.setProperty(LOG2KAFKA_URI, one);
        }// for

        String uriMongo= System.getProperty(LOG2MONGO_URI);
        String uriKafka= System.getProperty(LOG2KAFKA_URI);

        if(uriMongo == null && uriKafka == null) {
            String pkg= Local.getAppName();
            if(pkg == null || !pkg.endsWith(".jar"))
                pkg= DEMO_JAR;

            System.out.println();
            System.out.println(String.format("USAGE: %s %s <%s>|<%s>", DEMO_RUN, pkg, LOG2MONGO_URI, LOG2KAFKA_URI));
            System.out.println(String.format("eg. %s %s %s", DEMO_RUN, pkg, MONGO_URI));
            System.out.println(String.format(" or %s %s %s", DEMO_RUN, pkg, KAFKA_URI));
            System.out.println();
            return;
        }

        if(uriMongo != null)
            LOGGER.debug("Using mongodb uri: {} = {}", LOG2MONGO_URI, uriMongo);
        Log2Mongo log2Mongo= new Log2Mongo(Level.INFO);
        log2Mongo.appendMongoAppender();

        if(uriKafka != null)
            LOGGER.debug("Using kafka uri: {} = {}", LOG2KAFKA_URI, uriKafka);
        Log2Kafka log2Kafka= new Log2Kafka(Level.INFO);
        log2Kafka.appendKafkaAppender();


        LOGGER.info("Demo message from SimpleLogging 1.");
        LOGGER.warn("Demo message from SimpleLogging 2.");
        LOGGER.error("Demo message from SimpleLogging 3.");

        log2Mongo.removeMongoAppender();

        log2Kafka.removeKafkaAppender();

        System.out.println();
        System.out.println(" === end === ");
    }

}
