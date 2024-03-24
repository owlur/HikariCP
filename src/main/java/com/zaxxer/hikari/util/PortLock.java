package com.zaxxer.hikari.util;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PortLock ensures a single global lock within a JVM, effective even when multiple classloaders
 * might load a class multiple times, thus not guaranteeing singleton behavior. It utilizes a
 * network port for global locking, offering a unique advantage in environments where singleton
 * instances are critical but hard to maintain. Be aware, due to its network port-based mechanism,
 * PortLock may experience higher delays in lock acquisition compared to other lock types,
 * especially under high contention.
 */
public final class PortLock {
   // Default port to lock on, configurable via system property
   private static final int PORT = Integer.getInteger("com.zaxxer.hikari.lock.port", 32172);
   private static final ReentrantLock reentrantLock = new ReentrantLock();
   private static ServerSocket serverSocket = null;
   private static Socket clientSocket = null;
   private static final Map<Integer, Socket> connectedClients = new ConcurrentSkipListMap<>();
   private static Thread serverThread = null;

   private static final String HIKARI_PREFIX = "HIKARI_PORT_LOCK::";
   private static final String CHECK_REQUEST = "CHECK";
   private static final String CHECK_SUCCESS = "SUCCESS";
   private static final String RELEASE = "RELEASE";

   /**
    * Attempts to acquire a lock by binding to a port. If the port is already in use,
    * tries to authenticate with an existing server to determine if it's another instance.
    *
    * @return true if the lock was successfully acquired.
    * @throws RuntimeException if unable to acquire the lock due to port being in use by a non-PortLock process.
    */
   public static boolean acquire() {
      System.out.println(Thread.currentThread().toString() + "::PortLock.acquire");
      reentrantLock.lock();
      while (true) {
         try {
            serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName(""));
            startServerListener();

            System.out.println(Thread.currentThread().toString() + "::get lock");
            return true;
         } catch (BindException e) {
            // The port is already in use. Attempting to authenticate with the existing server.
            System.out.println(Thread.currentThread().toString() + "::BindException");
            waitForPortRelease();
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   /**
    * Starts a thread that listens for connections on the server socket,
    * handling client authentication requests according to protocol.
    */
   private static void startServerListener() {
      serverThread = new Thread(() -> {
         try {
            while (!serverSocket.isClosed()) {
               Socket client = serverSocket.accept();

               handleClientRequest(client);
            }
         } catch (IOException e) {
            // Handle server socket closure
         }
      }, "PortLockServerThread");
      serverThread.start();
   }

   /**
    * Handles incoming client requests to authenticate with the port lock server.
    *
    * @param client The client socket connection.
    * @throws IOException if an I/O error occurs during communication with the client.
    */
   private static void handleClientRequest(Socket client) throws IOException {
      try (PrintWriter out = new PrintWriter(client.getOutputStream(), true);
           BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()))) {
         String inputLine = in.readLine();
         if (inputLine != null && inputLine.equals(HIKARI_PREFIX + CHECK_REQUEST)) {
            out.println(HIKARI_PREFIX + CHECK_SUCCESS);
            connectedClients.put(client.getPort(), client);
         }
      }
   }

   /**
    * Attempts to authenticate with the existing server on the locked port.
    *
    * @return true if authentication was successful, indicating the port is being
    * used by another instance of the same service; false otherwise.
    */
   private static boolean isPortOccupiedByPortLock() throws IOException {
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

   /**
    * Checks if the port is currently locked by another instance of this PortLock mechanism.
    *
    * @return true if the port is occupied by another PortLock instance, false otherwise.
    * @throws IOException if there's an error during communication.
    */
   private static boolean checkPortLockStatus() throws IOException {
      try (Socket socket = new Socket("localhost", PORT);
           PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
           BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

         socket.setSoTimeout(300); // Timeout for the initial authentication

         out.println(HIKARI_PREFIX + CHECK_REQUEST);
         String response = in.readLine();


         return (HIKARI_PREFIX + CHECK_SUCCESS).equals(response);
      } catch (SocketTimeoutException e) {
         return false;
      }
   }


   /**
    * Waits for the port to be released if it's currently locked by another PortLock instance.
    *
    * @return true if it successfully waits for the lock to be released, false otherwise.
    */
   private static boolean waitForPortRelease() {
      try {
         if (checkPortLockStatus()) {
            try (Socket socket = new Socket("localhost", PORT)) {
               socket.setSoTimeout(1000); // Timeout while waiting for the release command
               BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

               System.out.println(Thread.currentThread() + "::Authentication successful. Waiting for release.");
               while (true) {
                  try {
                     String message = in.readLine();
                     if ((HIKARI_PREFIX + RELEASE).equals(message)) {
                        System.out.println(Thread.currentThread() + "::Release command received. Disconnecting.");
                        break;
                     }
                  } catch (SocketTimeoutException e) {
                     // In case of timeout, just try to read again
                     System.out.println(Thread.currentThread().toString() + "::Read timeout, waiting for next message...");

                     if (!isPortOccupiedByPortLock()) {
                        return true;
                     }
                  }
               }
               return true;
            }
         } else {
            throw new RuntimeException("Authentication failed on port " + PORT);
         }
      } catch (IOException e) {
         System.out.println(Thread.currentThread() + "::Error during authentication process");
         return false;
      }
   }

   /**
    * Releases the lock by closing the server socket and notifying all connected clients.
    */
   public static void release() {
      System.out.println(Thread.currentThread().toString() + "::PortLock.release");
      if (serverSocket != null && !serverSocket.isClosed()) {
         try {
            for (Socket clientSocket : connectedClients.values()) {
               if (!clientSocket.isClosed()) {
                  try (clientSocket;
                       PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                     out.println(HIKARI_PREFIX + RELEASE);
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
