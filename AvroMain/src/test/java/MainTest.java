import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created with IntelliJ IDEA.
 * User: rans
 * Date: 2/5/13
 * Time: 2:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class MainTest {

    public final String zkConnection = "tlvwhale1:2181,tlvwhale2:2181,tlvwhale3:2181";
    public final String topic = "avro-test";

    @Test
    public void testGenericRecord() throws IOException, InterruptedException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("LPEvent.avsc"));


        GenericRecord datum = new GenericData.Record(schema);
        datum.put("revision", 1L);
        datum.put("siteId", "28280110");
        datum.put("eventType", "PLine");
        datum.put("timeStamp", System.currentTimeMillis());
        datum.put("sessionId", "123456II");

        Map<String, Schema> unions = new HashMap<String, Schema>();
        List<Schema> typeList = schema.getField("subrecord").schema().getTypes();
        for (Schema sch : typeList){
            unions.put(sch.getName(), sch);
        }

        GenericRecord plineDatum = new GenericData.Record(unions.get("pline"));
        plineDatum.put("text", "How can I help you?");
        plineDatum.put("lineType", 1);
        plineDatum.put("repId", "REPID12345");

        datum.put("subrecord", plineDatum );


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();
        Message message = new Message(out.toByteArray());


        Properties props = new Properties();
        props.put("zk.connect", zkConnection);
        Producer<Message, Message> producer = new kafka.javaapi.producer.Producer<Message, Message>(new ProducerConfig(props));
        producer.send(new ProducerData<Message, Message>(topic, message));

    }

    @Test
    public void testCompiledDatumRecord() throws IOException, InterruptedException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("LPEvent.avsc"));


        LPEvent datum = new LPEvent();
        datum.setRevision(1L);
        datum.setSiteId("28280110");
        datum.setEventType("PLine");
        datum.setTimeStamp(System.currentTimeMillis());
        datum.setSessionId("123456II");

        pline plineDatum = new pline();
        plineDatum.setText("Hello, I am your agent");
        plineDatum.setLineType(2);
        plineDatum.setRepId("REPID7777");

        datum.setSubrecord(plineDatum);


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<LPEvent> writer = new SpecificDatumWriter<LPEvent>(LPEvent.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();
        Message message = new Message(out.toByteArray());


        Properties props = new Properties();
        props.put("zk.connect", zkConnection);
        Producer<Message, Message> producer = new kafka.javaapi.producer.Producer<Message, Message>(new ProducerConfig(props));
        producer.send(new ProducerData<Message, Message>(topic, message));

    }

    @Test
    public void testDataFile() throws IOException {

        File fileOut = new File("data.avro");
        File fileIn = new File("data.avro");
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("LPEvent.avsc"));


        GenericRecord datum = new GenericData.Record(schema);
        datum.put("revision", 1L);
        datum.put("siteId", "28280110");
        datum.put("eventType", "PLine");
        datum.put("timeStamp", System.currentTimeMillis());
        datum.put("sessionId", "123456II");

        Map<String, Schema> unions = new HashMap<String, Schema>();
        List<Schema> typeList = schema.getField("subrecord").schema().getTypes();
        for (Schema sch : typeList){
            unions.put(sch.getName(), sch);
        }

        GenericRecord plineDatum = new GenericData.Record(unions.get("pline"));
        plineDatum.put("text", "How can I help you?");
        plineDatum.put("lineType", 1);
        plineDatum.put("repId", "REPID12345");

        datum.put("subrecord", plineDatum );

        //write the file
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, fileOut);
        dataFileWriter.append(datum);
        dataFileWriter.append(datum);
        dataFileWriter.append(datum);
        dataFileWriter.close();

        //read the file
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(fileIn, reader);
        assertThat("Scema is the same", schema, is(dataFileReader.getSchema()));

        for(GenericRecord record : dataFileReader){
            assertThat(record.get("siteId").toString(), is("28280110"));
            assertThat(record.get("eventType").toString(), is("PLine"));
        }
    }



}
