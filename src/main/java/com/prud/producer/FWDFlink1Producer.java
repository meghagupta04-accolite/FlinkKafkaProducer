package com.prud.producer;
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
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.prud.constant.ConfigConstants;

/**
 * Class to read file from an ftp folder and put the file records to Kafka consumer
 *
 */
public class FWDFlink1Producer 
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
		File dir = new File(ConfigConstants.IL_INPUT_FOLDER_LOCATION);
		watchDirectoryPath(dir.toPath());
	}


	public static void	writeToKafka(String path,String filename,String lineCount) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> sourceData = env.readTextFile(path).setParallelism(1);

		/*Source Data Processing*/
		String fixedFileName = fillValue(filename, ConfigConstants.FIXED_FILE_NAME_LENGTH);

		DataStream<String> processedStream = sourceData.map(new MapFunction<String, String>() {
			boolean firstRecord = true;
			@Override
			public String map(String inputRecord) throws Exception {
				if(firstRecord){
					firstRecord = !firstRecord;
					return "H"+lineCount+"|"+fixedFileName+"##"+inputRecord;
				}
				else  {
					return inputRecord;
				}
			}

		});

		processedStream.addSink(new KafkaSink<>(ConfigConstants.BOOTSTRAP_SERVER_CONFIG, ConfigConstants.FLINK1_TOPIC, new SimpleStringSchema()));

		env.execute();

	}

	private static String fillValue(String value, int size) {
		if(value != null) {
			int noOfFillersNeeded = size - value.length();
			return value + String.join("",Collections.nCopies(noOfFillersNeeded, " "));
		}
		return null;
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
					//	File file = newPath.toFile();
						long lineCount = Files.lines(Paths.get(ConfigConstants.IL_INPUT_FOLDER_LOCATION+"//"+newPath)).count();
						System.out.println("New path created: " + newPath + "lineCount"+lineCount);
						writeToKafka(ConfigConstants.IL_INPUT_FOLDER_LOCATION+"//"+newPath,newPath.toString(),Long.toString(lineCount));


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
