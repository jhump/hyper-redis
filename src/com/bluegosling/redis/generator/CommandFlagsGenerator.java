package com.bluegosling.redis.generator;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Comparator;

import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.protocol.ArrayListener;
import com.bluegosling.redis.protocol.CommandInfoListener;
import com.bluegosling.redis.protocol.ResponseProtocol;
import com.bluegosling.redis.values.CommandInfo;
import com.google.common.base.Charsets;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Generates the "command-flags.json" by extracting the command flags from a running Redis server.
 * This class's {@code main} method requires two arguments: the hostname and port number of the
 * redis server. It prints the resulting JSON to standard out.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class CommandFlagsGenerator {
   public static void main(String[] args) throws Exception {
      if (args.length != 2) {
         System.err.println("Usage:");
         System.err.printf("  java %s <hostname> <port>%n", CommandFlagsGenerator.class.getName());
         System.exit(1);
      }
      
      CommandInfo[][] info = new CommandInfo[1][];
      Callback<CommandInfo[]> callback = new Callback<CommandInfo[]>() {
         @Override
         public void onSuccess(CommandInfo[] t) {
            info[0] = t;
         }

         @Override
         public void onFailure(Throwable th) {
            throw new RuntimeException(th);
         }
      };
      
      CompositeByteBuf buf = Unpooled.compositeBuffer();
      try (Socket s = new Socket(InetAddress.getByName(args[0]), Integer.valueOf(args[1]))) {
         OutputStream out = s.getOutputStream();
         out.write("*1\r\n$7\r\nCOMMAND\r\n".getBytes(Charsets.UTF_8));
         s.shutdownOutput();
         InputStream in = s.getInputStream();
         byte[] bytes = new byte[8192];
         int read;
         while ((read = in.read(bytes)) != -1) {
            if (read > 0) {
               buf.addComponent(true, Unpooled.wrappedBuffer(bytes, 0, read));
            }
            bytes = new byte[8192];
         }
      }
      ResponseProtocol resp = new ResponseProtocol(
            new ArrayListener<>(callback, CommandInfo[]::new, CommandInfoListener::new));
      if (!resp.parseReply(buf)) {
         throw new RuntimeException("RESP was not complete");
      } else if (buf.isReadable()) {
         throw new RuntimeException("RESP had more content than expected");
      }
      
      System.out.println("{");
      boolean firstCommand = true;
      Arrays.sort(info[0], Comparator.comparing(CommandInfo::name));
      for (CommandInfo c : info[0]) {
         if (firstCommand) {
            firstCommand = false;
         } else {
            System.out.println(",");
         }
         System.out.printf("  \"%s\": [%n", c.name());
         boolean firstFlag = true;
         for (CommandInfo.CommandFlag flag : c.flags()) {
            if (firstFlag) {
               firstFlag = false;
            } else {
               System.out.println(",");
            }
            System.out.printf("    \"%s\"", flag.flagName());
         }
         System.out.println();
         System.out.print("  ]");
      }
      System.out.println();
      System.out.println("}");
   }
}
