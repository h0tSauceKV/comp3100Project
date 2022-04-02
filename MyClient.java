
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class MyClient {
    public static void main(String[] args) {
        try {
            Socket s = new Socket("localhost", 50000);
            String msg = "";
            String[] largestServer = { " " };

            DataOutputStream dout = new DataOutputStream(s.getOutputStream());

            // hanshake protocol
            dout.write(("HELO\n").getBytes());
            msg = readMsg(s);
            System.out.println("Server says: " + msg);
            dout.write(("AUTH " + System.getProperty("user.name") + "\n").getBytes());
            msg = readMsg(s);
            System.out.println("Server says: " + msg);

            while (!msg.contains("NONE\n")) {
                dout.write(("REDY\n").getBytes());
                msg = readMsg(s);

                if (msg.contains("JOBN")) {
                    // Identify the largest server type
                    String[] Jobs = msg.split(" ");
                    dout.write(("GETS Avail " + Jobs[4] + " " + Jobs[5] + " " + Jobs[6] + "\n").getBytes());
                    // Receive DATA nRecs recSize
                    msg = readMsg(s);
                    dout.write(("OK\n").getBytes());

                    msg = readMsg(s);
                    dout.write(("OK\n").getBytes());

                    // Keep track of the largest server type and the number of servers of that type
                    largestServer = findLargestServer(msg);

                    // Reads "." from the server
                    msg = readMsg(s);

                    // Schedule a job
                    dout.write(("SCHD " + Jobs[2] + " " + largestServer[0] + " " + largestServer[1] + "\n").getBytes());

                    msg = readMsg(s);
                    System.out.println("SCHD: " + msg);

                } else if (msg.contains("DATA")) {
                    dout.write(("OK\n").getBytes());
                }
            }

            dout.write(("QUIT\n").getBytes());
            msg = readMsg(s);

            dout.flush();
            s.close();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static String readMsg(Socket s) {
        String msg = "";
        try {
            DataInputStream dis = new DataInputStream(s.getInputStream());
            // Create byte array and reset it to 0 for storing
            // incoming messages from the server
            byte[] byteArray = new byte[dis.available()];
            // available() method blocks until input data is available
            byteArray = new byte[0];
            while (byteArray.length == 0) {
                // Read the bytestream from the server
                byteArray = new byte[dis.available()];
                dis.read(byteArray);
                msg = new String(byteArray, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Return the msg just recieved from the server
        return msg;
    }

    public static String[] findLargestServer(String msg) {
        // Servers in the msg spilt into an array
        String[] serverRecs = msg.split("\n");
        int mostCores = 0;
        String[] currentServer = { " " };
        // Search for the most core a server holds in the given server
        for (int i = 0; i < serverRecs.length; i++) {
            currentServer = serverRecs[i].split(" ");
            int currentCores = Integer.valueOf(currentServer[4]);
            if (currentCores > mostCores) {
                mostCores = currentCores;
            }
        }

        // search the biggest server (most cores)
        for (int i = 0; i < serverRecs.length; i++) {
            currentServer = serverRecs[i].split(" ");
            int currentCores = Integer.valueOf(currentServer[4]);
            if (currentCores == mostCores) {
                return currentServer;
            }
        }
        return currentServer;
    }
}
