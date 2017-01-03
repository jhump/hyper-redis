package com.bluegosling.redis.generator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BufferedTokenStream;

import com.bluegosling.redis.generator.HyperRedisParser.BlockContext;
import com.bluegosling.redis.generator.HyperRedisParser.FileContext;
import com.bluegosling.redis.generator.MoshiAdapters.ArgAdapter;
import com.bluegosling.redis.generator.MoshiAdapters.CommandDef;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.Types;

import okio.Okio;

/**
 * The main entry point into code generation of the Redis interfaces and implementations.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class HyperRedisGenerator {
   
   final GeneratorContext context;
   
   private HyperRedisGenerator(FileContext astRoot, Multimap<String, String> commandFlags,
         Map<String, CommandDef> commands) {
      context = new GeneratorContext(astRoot, commandFlags, commands);
   }
   
   public void generate(File outputDirectory, String basePackageName) throws IOException {
      // TODO: generate
      for (SyncType syncType : SyncType.values()) {
         for (BlockContext block : context.astRoot.block()) {
            for (RedisKind kind : syncType.applicableKinds()) {
               if (context.redisKindIsApplicable(block, kind)) {
                  new BlockGenerator(context,
                        outputDirectory,
                        basePackageName + "." + syncType.subPackage(),
                        block,
                        syncType,
                        kind)
                  .generateBlock();
               }
            }
         }
      }
   }
         
   public static void main(String[] args) throws IOException {
      FileContext astRoot = loadCommandModel("hyper-redis-commands.txt");
      Multimap<String, String> flagsByCommand = loadCommandFlags("command-flags.json");
      Map<String, CommandDef> commandDefs = loadCommands("commands.json");
      HyperRedisGenerator generator = new HyperRedisGenerator(astRoot, flagsByCommand, commandDefs); 
//      System.out.println("versions = " + generator.versions);
//      for (BlockContext block : generator.astRoot.block()) {
//         String name = block.Identifier().getText();
//         System.out.print(name);
//         if (generator.blocksThatNeedKeyTypeArg.contains(name)
//               || generator.blocksThatNeedValTypeArg.contains(name)) {
//            boolean first = true;
//            System.out.print("<");
//            if (generator.blocksThatNeedKeyTypeArg.contains(name)) {
//               first = false;
//               System.out.print("K");
//            }
//            if (generator.blocksThatNeedValTypeArg.contains(name)) {
//               if (!first) {
//                  System.out.print(",");
//               }
//               System.out.print("V");
//            }
//            System.out.print(">");
//         }
//         System.out.println();
//      }
      File output = new File("/Users/jh/Development/personal/hyper-redis/gen");
      output.mkdirs();
      generator.generate(output, "com.bluegosling.redis");
      
      
      Multimap<String, String> flagsByBlock = HashMultimap.create();
      for (BlockContext block : astRoot.block()) {
         String blockName = block.Identifier().getText();
         for (Command cmd : generator.context.commandsByBlockName.get(blockName)) {
            if (cmd.flags.contains("readonly")) {
               flagsByBlock.put(blockName, "readonly");
            }
            if (cmd.flags.contains("write")) {
               flagsByBlock.put(blockName, "write");
            }
            
            if (cmd.flags.isEmpty()) {
               System.err.println("Unable to find command flags for "
                     + cmd.commandName);
            } else if (GeneratorContext.usesKeys(cmd.astContext)) {
               if (cmd.flags.contains("readonly")) {
                  flagsByBlock.put(blockName, "readonly");
               }
               if (cmd.flags.contains("write")) {
                  flagsByBlock.put(blockName, "write");
               }
               if (!cmd.flags.contains("readonly") && !cmd.flags.contains("write")) {
                  System.err.println("Key-related command " + cmd.commandName + " (" + blockName
                        + ") has neither readonly nor write flag!");
                  System.err.flush();
               }
            } else {
               if (cmd.flags.contains("readonly") || cmd.flags.contains("write")) {
                  System.out.println("Command " + cmd.commandName + " (" + blockName
                        + ") has readonly|write but is not key-related.");
                  System.out.flush();
               }
            }
         }
      }
      for (Entry<String, String> e : flagsByBlock.entries()) {
         System.out.println("Block " + e.getKey() + " has flag " + e.getValue());
      }
   }
   
   private static Multimap<String, String> loadCommandFlags(String resourcePath) throws IOException {
      return loadCommandFlags(loadResource(resourcePath));
   }

   public static SetMultimap<String, String> loadCommandFlags(InputStream input)
         throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, List<String>> flags = (Map<String, List<String>>) new Moshi.Builder()
            .build()
            .adapter(Types.newParameterizedType(Map.class,
                  String.class,
                  Types.newParameterizedType(List.class, String.class)))
            .fromJson(Okio.buffer(Okio.source(input)));
      ImmutableSetMultimap.Builder<String, String> ret = ImmutableSetMultimap.builder();
      flags.forEach((k, v) -> ret.putAll(k, v));
      return ret.build();
   }
   
   private static Map<String, CommandDef> loadCommands(String resourcePath) throws IOException {
      return loadCommands(loadResource(resourcePath));
   }
   
   public static Map<String, CommandDef> loadCommands(InputStream input) throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, CommandDef> commands = (Map<String, CommandDef>) new Moshi.Builder()
            .add(new ArgAdapter())
            .build()
            .adapter(Types.newParameterizedType(Map.class, String.class, CommandDef.class))
            .fromJson(Okio.buffer(Okio.source(input)));
      for (Entry<String, CommandDef> entry : commands.entrySet()) {
         entry.getValue().commandName = entry.getKey();
      }
      return Collections.unmodifiableMap(commands);
   }
   
   private static FileContext loadCommandModel(String resourcePath) throws IOException {
      return loadCommandModel(loadResource(resourcePath));
   }
   
   public static FileContext loadCommandModel(InputStream input) throws IOException {
      HyperRedisLexer lexer = 
            new HyperRedisLexer(new ANTLRInputStream(input));
      HyperRedisParser parser = new HyperRedisParser(new BufferedTokenStream(lexer));
      return parser.file();
   }
   
   private static InputStream loadResource(String resourcePath) throws IOException {
      return HyperRedisGenerator.class.getResourceAsStream(resourcePath);
   }
}
