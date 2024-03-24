package com.zaxxer.hikari.util;

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public final class PortLock {
   // Default port to lock on, configurable via system property
   private static final int PORT = Integer.getInteger("com.zaxxer.hikari.lock.port", 32172);
   private static final ReentrantLock reentrantLock = new ReentrantLock();
   private static ServerSocket serverSocket = null;
   private static Socket clientSocket = null;
   private static final Map<Integer, Socket> clientSockets = new ConcurrentSkipListMap<>();
   private static Thread serverThread = null;

   private static final String HIKARI_PREFIX = "HIKARI_PORT_LOCK::";
   private static final String CHECK_REQUEST = "CHECK";
   private static final String CHECK_SUCCESS = "SUCCESS";
   private static final String RELEASE = "RELEASE";

   /**
    * Attempts to acquire a lock on the specified port. If the port is already in use,
    * it tries to authenticate with the existing server on that port.
    *
    * @return true if the lock was acquired successfully.
    * @throws IOException If an I/O error occurs while attempting to acquire the lock.
    */
   public static boolean acquire() {
      System.out.println(Thread.currentThread().toString() + "::PortLock.acquire");
      reentrantLock.lock();
      while (true) {
         try {
            serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(""));
            startServerThread();

            System.out.println(Thread.currentThread().toString() + "::get lock");
            return true;
         } catch (BindException e) {
            // The port is already in use. Attempting to authenticate with the existing server.
            System.out.println(Thread.currentThread().toString() + "::BindException");
            authenticateWithServer();
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   /**
    * Starts a server thread that listens for connections on the server socket.
    */
   private static void startServerThread() {
      serverThread = new Thread(() -> {
         try {
            while (!serverSocket.isClosed()) {
               Socket clientSocket = serverSocket.accept();

               PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
               BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
               String inputLine = in.readLine();
               if (inputLine != null && inputLine.equals(HIKARI_PREFIX + CHECK_REQUEST)) {
                  out.println(HIKARI_PREFIX + CHECK_SUCCESS);
                  clientSockets.put(clientSocket.getPort(), clientSocket);
               }
            }
         } catch (IOException e) {
            // socket closed
         }
      });
      serverThread.start();
   }

   /**
    * Attempts to authenticate with the existing server on the locked port.
    *
    * @return true if authentication was successful, indicating the port is being
    * used by another instance of the same service; false otherwise.
    */

   private static boolean auth() throws IOException {
      // IOException은 커넥션이 실패한 경우,
      // 서버 소켓이 닫혀서 발생한것인지에 대해서는 사용하는측에서 검증해야함
      try (Socket socket = new Socket("localhost", PORT);
           PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
           BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

         socket.setSoTimeout(300); // Set timeout for initial authentication

         out.println(HIKARI_PREFIX + CHECK_REQUEST);
         String response = in.readLine();

         return response != null && response.equals(HIKARI_PREFIX + CHECK_SUCCESS);

      } catch (SocketTimeoutException e) {
         return false;
      }
   }

   private static boolean authenticateWithServer() {
      try {
         if (auth()) {
            try (Socket socket = new Socket("localhost", PORT)) {
               socket.setSoTimeout(1000); // Set timeout for waiting release
               BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

               // Check authentication response
               System.out.println(Thread.currentThread().toString() + "::Authentication successful.");
               // Authentication successful, continue to listen until an exit message is received or socket is closed
               while (true) {
                  try {
                     String fromServer = in.readLine();
                     // Check for exit message
                     if (fromServer != null || fromServer.equals(HIKARI_PREFIX + RELEASE)) {

                        System.out.println(Thread.currentThread().toString() + "::Exit message received. Closing connection.");
                        break;
                     }

                     System.out.println(Thread.currentThread().toString() + ":: receive message: " + fromServer);
                  } catch (SocketTimeoutException e) {
                     // In case of timeout, just try to read again
                     System.out.println(Thread.currentThread().toString() + "::Read timeout, waiting for next message...");

                     if (!auth()) {
                        return true;
                     }
                  }
               }

               return true;

            }
         } else {
            // Authentication failed or received unexpected response
            throw new RuntimeException("Failed to authenticate on port " + PORT);
         }
      } catch (IOException e) {
         System.out.println(Thread.currentThread().toString() + "::authenticateWithServer IOException");
         return true;
      }
   }

   /**
    * Releases the lock.
    */
   public static void release() {
      System.out.println(Thread.currentThread().toString() + "::PortLock.release");
      if (serverSocket != null && !serverSocket.isClosed()) {
         try {
            for (Socket clientSocket : clientSockets.values()) {
               if (!clientSocket.isClosed()) {
                  try (clientSocket;
                       PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                     out.println(HIKARI_PREFIX + RELEASE); // 클라이언트에게 종료 메시지 전송
                  } catch (IOException e) {
                     System.out.println(Thread.currentThread().toString() + "::Error sending exit message to client: " + e.getMessage());
                  }
               }
            }
            serverSocket.close();
            serverThread.join();
         } catch (IOException | InterruptedException e) {
            // continue
         } finally {
            serverSocket = null;
            serverThread = null;
            reentrantLock.unlock();
         }
      }
   }
}
