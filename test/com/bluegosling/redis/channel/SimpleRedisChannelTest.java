package com.bluegosling.redis.channel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.concurrent.CompletableFutureCallback;
import com.bluegosling.redis.protocol.MarshallingListener;
import com.bluegosling.redis.protocol.VoidListener;
import com.google.common.base.Charsets;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class SimpleRedisChannelTest {

   Channel nettyChannel;
   SimpleRedisChannel redisChannel;
   
   @Before public void setup() throws Exception {
      ChannelFuture ch = new RedisChannel.Builder("localhost")
            .createBootstrap()
            .connect();
      nettyChannel = ch.channel();
      ch.get();
      redisChannel = new SimpleRedisChannel(nettyChannel);
   }
   
   @After public void tearDown() throws Exception {
      nettyChannel.close().sync();
   }
   
   @Test(timeout = 5000) public void basic() {
      CompletableFutureCallback<Void> set = new CompletableFutureCallback<>();
      redisChannel.newCommand(
            new VoidListener(set))
            .nextToken("SET")
            .nextToken("FOO")
            .nextToken("BAR")
            .finish();
      CompletableFutureCallback<String> get = new CompletableFutureCallback<>();
      redisChannel.newCommand(
            new MarshallingListener<String>(get, Marshaller.ofStrings(Charsets.UTF_8)))
            .nextToken("GET")
            .nextToken("FOO")
            .finish();
      assertNull(set.join());
      assertEquals("BAR", get.join());
   }
}
