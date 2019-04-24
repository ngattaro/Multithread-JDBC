import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryData  {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException, InterruptedException
    {

        //Creating shared object
        ArrayBlockingQueue<String> sharedQueue = new ArrayBlockingQueue<String>(1000000);

        Scanner scanner = new Scanner(new File("res/small.csv"));

        //  Thread prodThread = new Thread(new Producer(sharedQueue,scanner));
        try {
            while (scanner.hasNextLine())
            {
                String Line = scanner.nextLine();
                sharedQueue.put(Line);
            }
        } catch (InterruptedException ex) {
        }
        Thread consThread = new Thread(new Consumer(sharedQueue));
        Thread consThread1 = new Thread(new Consumer(sharedQueue));
        Thread consThread2 = new Thread(new Consumer(sharedQueue));
        Thread consThread3 = new Thread(new Consumer(sharedQueue));
        //Starting producer and Consumer thread
        // prodThread.start();

        consThread.start();
        consThread1.start();
        consThread2.start();
        consThread3.start();


    }

}
