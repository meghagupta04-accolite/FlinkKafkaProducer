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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;

import com.prud.constant.ConfigConstants;
import com.prud.producer.FWDFlink1Producer.SimpleStringSchema;

public class FWDFlink2Producer {

	public static void main(String[] args) {
		try {
			produceMessagesToKafka();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void produceMessagesToKafka() throws Exception {
		File dir = new File(ConfigConstants.TRANSFORMED_IL_FILE_FOLDER_LOCATION);
		watchDirectoryPath(dir.toPath());
	}

	public static void watchDirectoryPath(Path path) throws Exception {
		try {
			Boolean isFolder = (Boolean) Files.getAttribute(path, "basic:isDirectory", NOFOLLOW_LINKS);
			if (!isFolder) {
				throw new IllegalArgumentException("Path: " + path + " is not a folder");
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
						Path newPath = ((WatchEvent<Path>) watchEvent).context();
						// Output
						// File file = newPath.toFile();
						long lineCount = Files
								.lines(Paths
										.get(ConfigConstants.TRANSFORMED_IL_FILE_FOLDER_LOCATION + "//" + newPath))
								.count();
						System.out.println("New path created: " + newPath + "lineCount" + lineCount);
						writeToKafka(ConfigConstants.TRANSFORMED_IL_FILE_FOLDER_LOCATION + "//" + newPath,
								newPath.toString(), Long.toString(lineCount));

					} else if (ENTRY_MODIFY == kind) {
						// modified
						Path newPath = ((WatchEvent<Path>) watchEvent).context();
						// Output
						writeToKafka(ConfigConstants.TRANSFORMED_IL_FILE_FOLDER_LOCATION + "//" + newPath,
								newPath.toString(), "1");
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
	
	public static void	writeToKafka(String path,String filename,String lineCount) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> sourceData = env.readTextFile(path).setParallelism(1);

		sourceData.addSink(new KafkaSink<>(ConfigConstants.BOOTSTRAP_SERVER_CONFIG, ConfigConstants.FLINK2_TOPIC, new SimpleStringSchema()));

		env.execute();

	}


}
