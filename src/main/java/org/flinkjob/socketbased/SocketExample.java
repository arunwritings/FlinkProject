package org.flinkjob.socketbased;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class SocketExample {
    public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException {

        try {
            ServerSocket serverSocket = new ServerSocket(9000);
            Socket connectionSocket  = serverSocket.accept();
            OutputStream outputStream = connectionSocket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(outputStream,"UTF-8"),true);

            for (int i=0;i<1000000;i++){
                printWriter.println(i);
                System.out.println("Server sends data:: "+i);
                Thread.sleep(2000);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
