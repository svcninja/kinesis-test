package com.example.kinesistest;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

public class KinesisSpanProcessor implements IRecordProcessor {
	@Override
	public void initialize(InitializationInput initializationInput) {

	}

	@Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
    	System.out.println("Processing...");
    	for (Record record : processRecordsInput.getRecords()) {
    		ByteBuffer bb = record.getData();
        	System.out.println(new String(bb.array()));
        }
    }

	@Override
	public void shutdown(ShutdownInput shutdownInput) {

	}
}
