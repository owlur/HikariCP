package com.zaxxer.hikari.util;

import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class PortLockTest {
   private int counter = 0;

   private Class<?> loadNewPortLockClass() {
      try{
         File file = new File("/Users/user/IdeaProjects/HikariCP/target/classes/");
         URL[] urls = {file.toURI().toURL()};

         URLClassLoader loader = new URLClassLoader(urls, null);
         return Class.forName("com.zaxxer.hikari.util.PortLock", true, loader);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Test
   public void testRaceCondition() {
      try {
         int numberOfThreads = 10;
         ExecutorService service = Executors.newFixedThreadPool(10);
         CountDownLatch latch = new CountDownLatch(numberOfThreads);
         for (int i = 0; i < numberOfThreads; i++) {
            service.submit(() -> {
               System.out.println(Thread.currentThread().toString() + "::start");
               this.incrementViaClassLoader();
               latch.countDown();
               System.out.println("counter = " + counter);
               System.out.println("latch = " + latch.getCount());
            });
         }
         latch.await();

         assertEquals(counter, numberOfThreads);


      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }


   private void incrementViaClassLoader() {
      try {
         Class<?> loadedClass = loadNewPortLockClass();

         Method acquire = loadedClass.getMethod("acquire");
         acquire.invoke(null);

         int c = counter;
         Thread.sleep(200);
         counter = c + 1;

         Method release = loadedClass.getMethod("release");
         release.invoke(null);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }


}
