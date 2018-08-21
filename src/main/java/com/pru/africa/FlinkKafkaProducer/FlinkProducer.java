package com.pru.africa.FlinkKafkaProducer;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
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
			produceMessagesToKafka();
		} catch (Exception e) {
			e.printStackTrace();
		}   
	}

	public static void	produceMessagesToKafka() throws Exception{
		File dir = new File(Constants.IL_FOLDER_LOCATION);
		watchDirectoryPath(dir.toPath());
	}
	
	public static void	writeToKafka(String path) throws Exception{
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		
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

	public static void watchDirectoryPath(Path path) throws Exception {
	/*	FtpClient ftp = new FtpClient("localhost", 21, "megha", "test");
		  ftp.open();
		  ftp.listFiles("\\");
	*/	
		try {
			Boolean isFolder = (Boolean) Files.getAttribute(path,
					"basic:isDirectory", NOFOLLOW_LINKS);
			if (!isFolder) {
				throw new IllegalArgumentException("Path: " + path
						+ " is not a folder");
			}
		} catch (IOException ioe) {
			// Folder does not exists
			ioe.printStackTrace();
		}

		System.out.println("Watching path: " + path);

		// We obtain the file system of the Path
		FileSystem fs = path.getFileSystem();

		// We create the new WatchService using the new try() block
		try (WatchService service = fs.newWatchService()) {

			// We register the path to the service
			// We watch for creation events
			path.register(service, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE); 

			// Start the infinite polling loop
			WatchKey key = null;
			while (true) {
				key = service.take();

				// Dequeueing events
				Kind<?> kind = null;
				for (WatchEvent<?> watchEvent : key.pollEvents()) {
					// Get the type of the event
					kind = watchEvent.kind();
					if (OVERFLOW == kind) {
						continue; // loop
					} else if (ENTRY_CREATE == kind) {
						// A new Path was created
						Path newPath = ((WatchEvent<Path>) watchEvent)
								.context();
						// Output
						System.out.println("New path created: " + newPath);
						writeToKafka(Constants.IL_FOLDER_LOCATION+"//"+newPath);


					} else if (ENTRY_MODIFY == kind) {
						// modified
						Path newPath = ((WatchEvent<Path>) watchEvent)
								.context();
						// Output
						System.out.println("New path modified: " + newPath);

					}
				}

				if (!key.reset()) {
					break; // loop
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}

	}


}
