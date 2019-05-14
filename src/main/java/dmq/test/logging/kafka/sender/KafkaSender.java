package dmq.test.logging.kafka.sender;

import dmq.test.logging.common.TimedBuffer;
import dmq.test.logging.kafka.KafkaURI;
import dmq.test.logging.kafka.KafkaURIException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;


public class KafkaSender implements ISendJson, TimedBuffer.BufferHandler<String>, TimedBuffer.BufferDroppedNotify<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);
    public static final String KEY= "json";

    // TimedBuffer默认参数
    private int timedBufferCapacity= 1000;      // 缓存容量
    private int timedBufferThresholdSize= 5;    // 每N条数据触发一次定时事件
    private int timedBufferThresholdTime= 5000; // 每N毫秒触发一次定时事件
    private int timedBufferThreads= 1;          // 定时器线程数量
    private boolean timedBufferFixDelay= true;  // 固定延时（忽略定时事件占用的时长）

    private TimedBuffer<String> timedBuffer;
    private Producer<String, String> producer;
    private final String servers, topic;
    public KafkaSender(String servers, String topic) {
        this.servers= servers;
        this.topic= topic;
        timedBuffer= null;
        producer= null;
        open();
    }
    public KafkaSender(KafkaURI uri) {
        this(uri.getServerHosts(), uri.getTopicName());
    }
    public KafkaSender(String uri) throws KafkaURIException {
        this(new KafkaURI(uri));
    }
    public KafkaSender(String uri, int thresholdSize)
            throws KafkaURIException {
        this.timedBufferThresholdSize= thresholdSize;
        // init and open
        KafkaURI kafkaURI= new KafkaURI(uri);
        this.servers= kafkaURI.getServerHosts();
        this.topic= kafkaURI.getTopicName();
        timedBuffer= null;
        producer= null;
        open();
    }

    public void open() {
        if(producer != null)
            return;
        if(timedBuffer == null)
            timedBuffer= new TimedBuffer<String>(timedBufferCapacity, timedBufferThresholdSize,
                    timedBufferThresholdTime, timedBufferThreads, timedBufferFixDelay, this, this);
        producer= new KafkaProducer<String, String>(customProducer(servers));
    }
    public void flush() {
        if(timedBuffer != null)
            timedBuffer.flush();
        if(producer != null)
            producer.flush();
    }
    public void close() {
        if(timedBuffer != null) {
            timedBuffer.close(()-> {
                if(producer != null)
                    producer.close();
            });
            timedBuffer = null;
        }
        producer= null;
    }
    private Future<RecordMetadata> send(String key, String value) {
        if(producer == null)
            return null;
        ProducerRecord<String, String> record= new ProducerRecord<>(topic, key, value);
        return producer.send(record);
    }

    private boolean doSendJson(String json) {
        if(producer == null)
            return false;

        try {
            //Future<RecordMetadata> future =
            send(KEY, json);

            return true;
        } catch(Exception x) {
            //x.printStackTrace();
            String message= String.format("KafkaSender sendJson error!\njson= %s", json);
            LOGGER.error(message, x);
        }
        return false;
    }

    @Override// ISendJson
    public boolean sendJson(String json) {
        if(producer == null)
            return false;

        // 将数据写入缓存中，避免阻塞主线程
        return timedBuffer.put(json);
    }

    @Override// TimedBuffer.BufferHandler
    public void processBuffer(List<String> bufferList) {
        int count= 0;
        for(String one: bufferList) {
            if(!doSendJson(one)) {
                LOGGER.error("KafkaSender failed to send the report (total={}, count={})", bufferList.size(), count);
                return;
            }
            count++;
        }// for
    }

    @Override// TimedBuffer.BufferDroppedNotify
    public void notifyBufferDroppped(long count) {
        LOGGER.error("KafkaSender dropped buffer (count={})", count);
    }

    private Properties customProducer(String servers) {
        Properties properties= new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("client.id", this.getClass().getSimpleName());
        //properties.put("batch.size", 4096);
        properties.put("linger.ms", 0);
        properties.put("max.block.ms", 1000);
        properties.put("metadata.fetch.timeout.ms", 5000);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

}
