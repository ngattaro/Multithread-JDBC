import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;


public class Consumer implements Runnable
{
    private final String sql = "insert into TESTDATA   (column1, column2, column3,column4,column5, column6, column7,column8,column9, column10, column11,column12,column13,column14) values (?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?,?, ?)";
    private static final char SEPARATOR = ',';
    private static final char QUOTE = '"';
    private final ArrayBlockingQueue<String> sharedQueue;
    Connection connection;

    private static int count = 0;
    public Consumer (ArrayBlockingQueue sharedQueue) {
        this.sharedQueue = sharedQueue;
        try
        {
            connection = ConnectionUtils.getMyConnection();
            connection.setAutoCommit(false);
        } catch (SQLException e)
        {
            e.printStackTrace();
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }


    public static String deleteQoute(String curVal)
    {
        if(curVal.charAt(0) == QUOTE && curVal.charAt(curVal.length()-1) == QUOTE) curVal = curVal.substring(1, curVal.length()-1);
        return curVal;
    }
    public static List<String> parseLine(String csvLine) {
        List<String> result = new ArrayList<>();

        if (csvLine == null && csvLine.isEmpty()) { return result; }
        String curVal = "";

        boolean inQuotes = false;

        for (char ch : csvLine.toCharArray()) {

            if (inQuotes)
            {
                if (ch == QUOTE) inQuotes = false;
            }
            else
            {
                if (ch == QUOTE) inQuotes = true;
                else if (ch == SEPARATOR)
                {

                    result.add(deleteQoute(curVal));
                    curVal = "";
                    continue;

                } else if (ch == '\r') continue;
                else if (ch == '\n') break;
            }
            curVal+=ch;
        }
        result.add(deleteQoute(curVal));
        return result;
    }

    @Override
    public void run()
    {
        int count = 0;
        PreparedStatement ps = null;
        try
        {
            ps = connection.prepareStatement(sql);
            while (!sharedQueue.isEmpty())
            {

                List<String> lineValue = parseLine((String) sharedQueue.take());

                for (int i = 0; i < lineValue.size(); i++)
                {
                    if (i == 0)
                        ps.setString(i + 1, count + " " + lineValue.get(i));
                    else ps.setString(i + 1, lineValue.get(i));
                }
                ps.addBatch();
                count++;
                if (count == 20 || sharedQueue.isEmpty())
                {
                    ps.executeBatch();
                    connection.commit();
                    count = 0;
                }


            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        } catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
}
