package com.kstream.impl.registry;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
 

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer2 {
    
    public void consume()
    {
    	String topic ="customerPolicy5";
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:8080");
        props.put("group.id", "group1");
        // to read messages from the beginning
        // first time for every group
        props.put("auto.offset.reset", "smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        try {
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put("topic1", 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            if (streams.size() > 0)
            {
                readMessages(streams.get(0));
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private void readMessages(KafkaStream<byte[], byte[]> stream) {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String firstName = null;
            try {
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
             //   System.out.println(messageAndMetaData);
                // LOG.info("raeding message: " + messageAndMetadata.offset());
                GenericRecord genericRecord = byteArrayToDatum(getSchema(), messageAndMetadata.message());
                System.out.println(genericRecord);
                firstName = getValue(genericRecord, "firstName", String.class);

                // LOG.info("reading record: " + server + ", " + timestamp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static GenericRecord byteArrayToDatum(Schema schema, byte[] byteData) {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayInputStream byteArrayInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(byteData);
            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            return null;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {

            }
        }
    }

    public static Schema getSchema()
    {
        String schemaStr = "{\"namespace\": \"org.test.data\",\n" +
                "    \"type\": \"record\",\n" +
                "    \"name\": \"GSD\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"firstName\", \"type\": \"string\"},\n" +
                "    ]\n" +
                "}";
        return new Schema.Parser().parse(schemaStr);
    }

    public static <T> T getValue(GenericRecord genericRecord, String name, Class<T> clazz)
    {
        Object obj = genericRecord.get(name);
        if (obj == null)
            return null;
        if (obj.getClass() == Utf8.class)
        {
            return (T) obj.toString();
        }
        if (obj.getClass() ==  Integer.class)
        {
            return (T) obj;
        }
        return null;
    }
}