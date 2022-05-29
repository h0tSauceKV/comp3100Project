
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class MyClient2 {
    public static void main(String[] args) {
        try {
            Socket s = new Socket("localhost", 50000);
            String msg = "";
            String[] bestFit = { " " };

            // hanshake protocol
            sendMsg(s, "HELO\n");
            msg = readMsg(s);
            System.out.println("RCVD: " + msg);
            sendMsg(s, "AUTH " + System.getProperty("user.name") + "\n");
            msg = readMsg(s);
            System.out.println("RCVD: " + msg);

            while (!msg.contains("NONE\n")) {
                sendMsg(s, "REDY\n");
                msg = readMsg(s);

                if (msg.contains("JOBN")) {
                    String[] Jobs = msg.split(" ");
                    sendMsg(s, "GETS Avail " + Jobs[4] + " " + Jobs[5] + " " + Jobs[6] + "\n");
                    // Receive DATA nRecs recSize
                    msg = readMsg(s);
                    sendMsg(s, "OK\n");

                    msg = readMsg(s);
                    sendMsg(s, "OK\n");

                    // Request for the best fitted server
                    bestFit = bestFit(msg, Jobs[4], s);

                    // Reads "." from the server
                    msg = readMsg(s);

                    // Schedule a job to an available server that can eventually provide sufficient
                    // resource
                    sendMsg(s, "SCHD " + Jobs[2] + " " + bestFit[0] + " " + bestFit[1] + "\n");
                    msg = readMsg(s);
                    System.out.println("SCHD: " + msg);

                } else if (msg.contains("DATA")) {
                    sendMsg(s, "OK\n");
                }
            }
            sendMsg(s, "QUIT\n");
            msg = readMsg(s);
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

    public static void sendMsg(Socket s, String msg) {
        try {
            DataOutputStream dout = new DataOutputStream(s.getOutputStream());
            byte[] byteArray = msg.getBytes();
            dout.write(byteArray);
            dout.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String[] bestFit(String msg, String jobCores, Socket s) {
        // Servers in the msg spilt into an array
        String[] servers = msg.split("\n");
        String[] jobRunningServer = { "" };
        String[] bestFitServer = servers[0].split("");
        int currentCores = Integer.parseInt(bestFitServer[4]);
        int coresRemain = currentCores - Integer.parseInt(jobCores);
        int fitnessValue = coresRemain;
        int jobCount;
        int mostJobs = -1;
        int jobRunningServerFitnessValue = -1;

        // Traverse through the list of available servers to find best fit
        for (int i = 0; i < servers.length; i++) {
            // null check on servers list
            if (!servers[i].contains(".")) {
                // Picking the server with the best fitness value (lowest) to be compared
                String[] currentServer = servers[i].split(" ");
                currentCores = Integer.parseInt(currentServer[4]);
                coresRemain = currentCores - Integer.parseInt(jobCores);

                // Request the current jobs running on the server
                String currMsg = "CNTJ " + currentServer[0] + " " + currentServer[1] + " " + 2 + "\n";
                sendMsg(s, currMsg);

                // Save the jobs to be compared
                String currentJobs = readMsg(s);

                String[] jobs = currentJobs.split("");
                if (jobs.length == 2) {
                    jobCount = Integer.parseInt(jobs[1]);
                } else {
                    jobCount = Integer.parseInt(jobs[0]);
                }
                // Picking the server with the most running jobs
                if (jobCount > 0) {
                    if (jobCount > mostJobs) {
                        mostJobs = jobCount;
                        fitnessValue = coresRemain;
                        currentServer = bestFitServer;
                    }
                }
            }
        }
        // high prioritise on finding server with jobs running
        // server with the lowest fitness value is prioritised
        // if fv = 0, return the current server to be best fitted
        if (bestFitServer.length > 1) {
            if (coresRemain == 0) {
                return bestFitServer;
            } else {
                jobRunningServer = bestFitServer;
                jobRunningServerFitnessValue = fitnessValue;
            }
        } else if (coresRemain != 0 || coresRemain < fitnessValue) {
            if (coresRemain < jobRunningServerFitnessValue) {
                return bestFitServer;
            } else {
                return jobRunningServer;
            }
        }
        return bestFitServer;
    }
}
