package com.bluegosling.redis.generator;

import java.util.Set;

import com.bluegosling.redis.RedisVersion;
import com.bluegosling.redis.generator.HyperRedisParser.CommandContext;

/**
 * Represents a Redis command. This assembles information from "commands.json",
 * "command-flags.json", and "hyper-redis-commands.txt".
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class Command {
   final CommandContext astContext;
   final String commandName;
   final String summary;
   final String complexity;
   final RedisVersion since;
   final Set<String> flags;
   
   Command(CommandContext astContext, String commandName, String summary, String complexity,
         RedisVersion since, Set<String> flags) {
      this.commandName = commandName;
      this.astContext = astContext;
      this.summary = summary;
      this.complexity = complexity;
      this.since = since;
      this.flags = flags;
   }
}
