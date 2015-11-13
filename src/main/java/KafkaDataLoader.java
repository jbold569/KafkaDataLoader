import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaDataLoader {
	
	private static void markFileQueued(String filename) {
		File oldfile = new File(filename);
		File newfile = new File(filename+".QUEUED");
		if(oldfile.renameTo(newfile))
			System.out.println("Renamed: " + newfile.getName());
		else
			System.out.println("Failed to rename: " + oldfile.getName());
	}
	
	public static void main(String[] args) throws InterruptedException {
		
		if(args.length < 3) {
			System.out.println("usage: <input dir> <kafka properties dir> <topic>");
			System.exit(1);
		}
		
		// Get input files
		String inputDir = args[0];
		File folder = new File(inputDir);
		
		// Setup Kafka producer
		String kafkaProperties = args[1];
		Properties props = new Properties();
		
		String topic = args[2];
		
		try {
			props.load(new FileInputStream(kafkaProperties));
		} catch(IOException e) {}
		
		ConcurrentLinkedQueue<String> filenames = new ConcurrentLinkedQueue<String>();
		
		for(int i=0; i<4; i++) {
			Producer worker = new Producer(i, filenames, props, topic);
			worker.start();
		}
		
		while(true) {
			for(File file : folder.listFiles()){
				if(!file.getName().endsWith(".bz2"))
					continue;
				String filename = inputDir+file.getName();
				markFileQueued(filename);
				filenames.add(filename+".QUEUED");
				System.out.println("Adding file to queue: " + filename);
			}
			System.out.println("No more files to add, sleeping for 15 sec.");
			Thread.sleep(15*1000);
		}
		
	}
}
