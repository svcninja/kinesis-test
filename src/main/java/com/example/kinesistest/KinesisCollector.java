package com.example.kinesistest;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.internal.AwsProfileNameLoader;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collector;

public class KinesisCollector implements Closeable{

    private final String appName;
    private final String streamName;
    private final AWSCredentialsProvider credentialsProvider;

    private final Executor executor;
    private Worker worker;
    private IRecordProcessorFactory processor;
    public static final class Builder{

        AWSCredentialsProvider credentialsProvider;
        String appName;
        String streamName;

        public Builder credentialsProvider(AWSCredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public Builder appName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public KinesisCollector build() {
            return new KinesisCollector(this);
        }

        Builder() {
        }
    }
    public static Builder builder() {
        return new Builder();
    }

    KinesisCollector(Builder builder) {
        this.appName = builder.appName;
        this.streamName = builder.streamName;
        this.credentialsProvider = builder.credentialsProvider;
        executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("KinesisCollector-" + streamName + "-%d").build());
    }
    @Override
    public void close() throws IOException {
        worker.shutdown();
    }
    public KinesisCollector start() {
        String workerId = null;
        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            workerId = UUID.randomUUID().toString();
        }
        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(appName, streamName, credentialsProvider, workerId).withRegionName("us-west-2").withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        processor = new KinesisRecordProcessorFactory();
        worker = new Worker.Builder()
                .recordProcessorFactory(processor)
                .config(config)
                //.dynamoDBClient(getOffsetConnection())
                .build();

        executor.execute(worker);
        return this;
    }
    private static final class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

        KinesisRecordProcessorFactory(){
        }

        @Override
        public IRecordProcessor createProcessor() {
            return new KinesisSpanProcessor();
        }
    }

    public AmazonDynamoDB getOffsetConnection() {
        AmazonDynamoDBClient dynamoDB = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain(), getHttpConfiguration());
        dynamoDB.setRegion(Region.getRegion(Regions.US_WEST_2));
        return dynamoDB;
    }
    protected ClientConfiguration getHttpConfiguration() {

        final Optional<String> httpProxyhost = Optional.ofNullable("webproxysea.nordstrom.net");
        final Optional<String> httpProxyPort = Optional.ofNullable("8181");

        ClientConfiguration clientConfiguration = new ClientConfiguration();

        if (httpProxyhost.isPresent()) {
            clientConfiguration.setProxyHost(httpProxyhost.get());
        }

        if (httpProxyPort.isPresent()) {
            clientConfiguration.setProxyPort(Integer.valueOf(httpProxyPort.get()));
        }

        return clientConfiguration;
    }

}
