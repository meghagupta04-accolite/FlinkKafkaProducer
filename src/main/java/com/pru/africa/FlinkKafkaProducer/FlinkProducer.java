package com.pru.africa.FlinkKafkaProducer;
import java.io.File;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSink;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Class to read file from an ftp folder and put the file records to Kafka consumer
 *
 */
public class FlinkProducer 
{
	public static void main( String[] args )
	{
		try {
			writeToKafka();
		} catch (Exception e) {
			e.printStackTrace();
		}   
	}

	public static void	writeToKafka() throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
	/*	FTPClient ftp = new FTPClient();
        ftp.connect("test.rebex.net");
        System.out.println(ftp.login("demo","password" ));
        InputStream in = ftp.retrieveFileStream("pub/example/readme.txt");
        String result = IOUtils.toString(in, StandardCharsets.UTF_8);
    */  
		
		File file = new File(Constants.FILE_NAME);
		String path = file.getAbsolutePath();

		DataStream<String> sourceData = env.readTextFile(path).setParallelism(1);
		
		sourceData.addSink(new KafkaSink<>(Constants.BOOTSTRAP_SERVER_CONFIG, Constants.TOPIC, new SimpleStringSchema()));

		env.execute();

	}

	public static class SimpleStringSchema implements DeserializationSchema<String>, SerializationSchema<String, byte[]> {
		private static final long serialVersionUID = 1L;

		public SimpleStringSchema() {
		}

		public String deserialize(byte[] message) {
			return new String(message);
		}

		public boolean isEndOfStream(String nextElement) {
			return false;
		}

		public byte[] serialize(String element) {
			return element.getBytes();
		}

		public TypeInformation<String> getProducedType() {
			return TypeExtractor.getForClass(String.class);
		}

	}
}
