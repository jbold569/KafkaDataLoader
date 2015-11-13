
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer extends Thread {
	private final int id;
	private final KafkaProducer<String, String> producer;
	private final String topic;
	private final ConcurrentLinkedQueue<String> filenames;
	
	public Producer(int id, ConcurrentLinkedQueue<String> filenames, Properties props, String topic) {
		producer = new KafkaProducer<String, String>(props);
		this.id = id;
		this.topic = topic;
		this.filenames = filenames;
	}

	private static BufferedReader getBufferedReaderForCompressedFile(String fileIn) throws FileNotFoundException, CompressorException {
	    FileInputStream fin = new FileInputStream(fileIn);
	    BufferedInputStream bis = new BufferedInputStream(fin);
	    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, bis);
	    BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
	    return br2;
	}
	
	private void markFileComplete(String filename) {
		File oldfile = new File(filename);
		File newfile = new File(filename+".PROCESSED");
		if(oldfile.renameTo(newfile))
			System.out.println("Renamed: " + newfile.getName());
		else
			System.out.println("Failed to rename: " + oldfile.getName());
	}
	
	public void run() {
		while(true) {
			String filename = this.filenames.poll();
			if(filename == null) {
				try {
					System.out.println("No files in queue, Thread{"+this.id+"} sleeping for 15 sec.");
					sleep(15*1000);
					continue;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("Thread{"+this.id+"} processing file: " + filename);
			try {
				BufferedReader fin = getBufferedReaderForCompressedFile(filename);
				for(String line = fin.readLine(); line != null; line = fin.readLine()) {
					String[] tokens = line.split("\t");
					String server = tokens[1];
					ProducerRecord<String, String> data = new ProducerRecord<String, String>(this.topic, server, line);
					producer.send(data);
					//producer.send(data , new ProducerCallBack(startTime, server, line));
				}
				fin.close();
				markFileComplete(filename);
				
			} catch(Exception e) {
				System.out.println(e.getStackTrace());
			}
		}
	}
}

class ProducerCallBack implements Callback {

	private long startTime;
	private String key;
	private String message;

	public ProducerCallBack(long startTime, String key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	/**
	 * A callback method the user can implement to provide asynchronous handling
	 * of request completion. This method will be called when the record sent to
	 * the server has been acknowledged. Exactly one of the arguments will be
	 * non-null.
	 * 
	 * @param metadata
	 *            The metadata for the record that was sent (i.e. the partition
	 *            and offset). Null if an error occurred.
	 * @param exception
	 *            The exception thrown during processing of this record. Null if
	 *            no error occurred.
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			System.out.println("message(" + key + ", " + message
					+ ") sent to partition(" + metadata.partition() + "), "
					+ "offset(" + metadata.offset() + ") in " + elapsedTime
					+ " ms");
		} else {
			exception.printStackTrace();
		}
	}
}