package cn.aws.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.util.List;

public class KinesisRecordProcessor implements IRecordProcessor,IShutdownNotificationAware {
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;
    private String kinesisShardId;

    @Override
    public void initialize(InitializationInput initializationInput) {
        this.kinesisShardId = initializationInput.getShardId();
        System.out.println("Initializing record processor for shard: " + kinesisShardId);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        System.out.println("invoke processRecords...");
        List<Record> records = processRecordsInput.getRecords();
        System.out.println("Processing " + records.size() + " records from " + kinesisShardId);
        records.forEach(record -> {
            ByteBuffer buffer = record.getData();
            System.out.println("sequence number: " + record.getSequenceNumber() + ", partitionKey: " + record.getPartitionKey() + ", data: " + new String(buffer.array()));
        });
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            this.checkpoint(processRecordsInput.getCheckpointer());
            this.nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (InvalidStateException e) {
            e.printStackTrace();
        } catch (ShutdownException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        System.out.println("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                /** Ignore checkpoint if the processor instance has been shutdown (fail over). **/
                System.out.println("Caught shutdown exception, skipping checkpoint.");
                se.printStackTrace();
                break;
            } catch (ThrottlingException e) {
                /** Backoff and re-attempt checkpoint upon transient failures **/
                if (i >= (NUM_RETRIES - 1)) {
                    System.out.println("Checkpoint failed after " + (i + 1) + "attempts.");
                    e.printStackTrace();
                    break;
                } else {
                    System.out.println("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES);
                    e.printStackTrace();
                }
            } catch (InvalidStateException e) {
                /** This indicates an issue with the DynamoDB table (check for table, provisioned IOPS). **/
                System.out.println("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.");
                e.printStackTrace();
                break;
            }
            this.backOff();
        }

    }

    /***
     * Called to back off when processing failed records
     */
    private void backOff() {
        try {
            Thread.sleep(BACKOFF_TIME_IN_MILLIS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
