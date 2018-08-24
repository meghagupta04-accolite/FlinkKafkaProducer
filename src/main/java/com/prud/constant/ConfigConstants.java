package com.prud.constant;

public class ConfigConstants {

	public static final String BOOTSTRAP_SERVER_CONFIG = "localhost:9092";
	public static final String FLINK1_TOPIC = "ILSourceRequestIn";
	public static final String BANK_RESPONSE_IN_TOPIC = "BankResponseIn"; 
	public static final String FLINK2_TOPIC = "TransformBankOut";
	public static final String IL_INPUT_FOLDER_LOCATION = "C:\\D\\Prudential\\newWork\\DropZone\\IL_INPUT";
	public static final String ZOOKEEPER_CONFIG = "localhost:2181";
	public static final String TRANSFORMED_IL_FILE_FOLDER_LOCATION = "C:\\D\\Prudential\\newWork\\consume\\FlinkKafkaConsumer";
	public static final String BOOTSTRAP_SERVER = "bootstrap.servers";
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final int FIXED_FILE_NAME_LENGTH = 32;
	public static final String BANK_IN_FOLDER_LOCATION ="C:\\D\\Prudential\\newWork\\DropZone\\BANK_OUTPUT";
}
