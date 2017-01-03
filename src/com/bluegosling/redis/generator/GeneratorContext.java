package com.bluegosling.redis.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.bluegosling.redis.RedisVersion;
import com.bluegosling.redis.generator.HyperRedisParser.ArgContext;
import com.bluegosling.redis.generator.HyperRedisParser.BlockContext;
import com.bluegosling.redis.generator.HyperRedisParser.CommandContext;
import com.bluegosling.redis.generator.HyperRedisParser.DeclaredTypeContext;
import com.bluegosling.redis.generator.HyperRedisParser.FileContext;
import com.bluegosling.redis.generator.HyperRedisParser.NameContext;
import com.bluegosling.redis.generator.MoshiAdapters.CommandDef;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

/**
 * Holds information about the current generator invocation. Its state represents the contents of
 * the "commands.json", "command-flags.json", and "hyper-redis-commands.txt" files that describe
 * the code being generated.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public class GeneratorContext {
   /**
    * The name of the block with the main set of commands. The main block is the entry point to
    * interacting with Redis. Commands that are grouped into other blocks can be accessed from the
    * main block.
    */
   static final String MAIN_BLOCK_NAME = "Redis";
   
   final FileContext astRoot;
   final BlockContext mainBlock;
   final Multimap<String, Command> commandsByBlockName;
   final Set<String> blocksThatNeedKeyTypeArg = new HashSet<>();
   final Set<String> blocksThatNeedValTypeArg = new HashSet<>();
   final NavigableSet<RedisVersion> allVersions = new TreeSet<>();
   final Map<String, NavigableSet<RedisVersion>> versionsPerBlock = new HashMap<>();
   final RedisVersion multiVersion;
   final RedisVersion watchVersion;
   final RedisVersion scanVersion;
   final RedisVersion pubsubVersion;
   
   GeneratorContext(FileContext astRoot, Multimap<String, String> commandFlags,
         Map<String, CommandDef> commands) {
      this.astRoot = astRoot;
      this.mainBlock = findMainBlock();
      this.commandsByBlockName = buildCommands(astRoot.block(), commandFlags, commands);
      findBlocksThatNeedTypeArgs();
      extractVersions();
      this.multiVersion = RedisVersion.parse(commands.get("MULTI").since).canonicalize();
      this.watchVersion = RedisVersion.parse(commands.get("WATCH").since).canonicalize();
      this.scanVersion = RedisVersion.parse(commands.get("SCAN").since).canonicalize();
      this.pubsubVersion = RedisVersion.parse(commands.get("SUBSCRIBE").since).canonicalize();
   }

   private BlockContext findMainBlock() {
      for (BlockContext block : astRoot.block()) {
         String name = block.Identifier().getText();
         if (MAIN_BLOCK_NAME.equals(name)) {
            return block;
         }
      }
      throw new IllegalStateException("No block named \"" + MAIN_BLOCK_NAME + "\"");
   }

   private Multimap<String, Command> buildCommands(List<BlockContext> blocks,
         Multimap<String, String> commandFlags, Map<String, CommandDef> commandDefs) {
      ImmutableMultimap.Builder<String, Command> cmds = ImmutableMultimap.builder();
      for (BlockContext block : blocks) {
         String blockName = block.Identifier().getText();
         for (CommandContext cmd : block.command()) {
            CommandDef def = findCommandDef(commandDefs, cmd);
            Collection<String> flags = findCommandFlags(commandFlags, cmd);
            Command command = new Command(cmd, def.commandName, def.summary, def.complexity,
                  RedisVersion.parse(def.since).canonicalize(), ImmutableSet.copyOf(flags));
            cmds.put(blockName, command);
         }
      }
      return cmds.build();
   }

   private void findBlocksThatNeedTypeArgs() {
      for (BlockContext block : astRoot.block()) {
         if (usesKeys(block)) {
            blocksThatNeedKeyTypeArg.add(block.Identifier().getText());
         }
         if (usesValues(block)) {
            blocksThatNeedValTypeArg.add(block.Identifier().getText());
         }
      }
   }
   
   static boolean usesKeys(ParseTree ast) {
      return usesTypeArg(ast, "K");
   }
   
   static boolean usesValues(ParseTree ast) {
      return usesTypeArg(ast, "V");
   }
   
   private static boolean usesTypeArg(ParseTree ast, String typeArgName) {
      boolean[] ret = new boolean[1];
      ParseTreeWalker.DEFAULT.walk(
            new HyperRedisBaseListener() {
               @Override
               public void enterDeclaredType(DeclaredTypeContext ctx) {
                  // skip types w/ args since type variables cannot themselves have arguments
                  if (ctx.typeArgs() == null) {
                     String name = ctx.qualifiedIdentifier().getText();
                     if (name.equals(typeArgName)) {
                        ret[0] = true;
                     }
                  }
               }
            },
            ast);
      return ret[0];
   }
   
   private void extractVersions() {
      for (BlockContext block : astRoot.block()) {
         NavigableSet<RedisVersion> versionsForBlock = new TreeSet<>();
         String blockName = block.Identifier().getText();
         for (Command cmd : commandsByBlockName.get(blockName)) {
            if (cmd.since == null) {
               throw new IllegalStateException("Unable to determine version for "
                     + cmd.astContext.commandName().name().Identifier().get(0).getText() + " ("
                     + block.Identifier().getText() + ")");
            }
            versionsForBlock.add(cmd.since);
         }
         versionsPerBlock.put(block.Identifier().getText(), versionsForBlock);
         allVersions.addAll(versionsForBlock);
      }
      assert RedisVersion.parse("1.0.0").compareTo(allVersions.first()) == 0;
   }
   
   private Collection<String> findCommandFlags(Multimap<String, String> commandFlags,
         CommandContext cmd) {
      return commandFlags.get(getCommandWord(cmd.commandName().name()).toLowerCase());
   }
   
   private CommandDef findCommandDef(Map<String, CommandDef> commandDefs, CommandContext cmd) {
      List<String> commandWords = new ArrayList<>();
      commandWords.add(getCommandWord(cmd.commandName().name()));
      for (ArgContext arg : cmd.arg()) {
         if (arg.symbol() != null) {
            commandWords.add(getCommandWord(arg.symbol().name()));
         }
      }
      StringBuilder sb = new StringBuilder();
      for (String w : commandWords) {
         if (sb.length() > 0) {
            sb.append(' ');
         }
         sb.append(w);
      }
      int i = commandWords.size() - 1;
      while (true) {
         CommandDef ret = commandDefs.get(sb.toString());
         if (ret != null) {
            return ret;
         }
         if (i > 0) {
            sb.setLength(sb.length() - commandWords.get(i--).length() - 1);
         } else {
            break;
         }
      }
      // still haven't found anything? see if the command was renamed
      if (cmd.rename() == null) {
         return null;
      }
      return commandDefs.get(cmd.rename().Identifier().getText().toUpperCase());
   }
   
   private String getCommandWord(NameContext name) {
      return (name.Identifier().size() > 1 ? name.Identifier().get(1) : name.Identifier().get(0))
            .getText()
            .toUpperCase();
   }
   
   private boolean consumesKeys(Command cmd) {
      for (ArgContext arg : cmd.astContext.arg()) {
         if (usesKeys(arg)) {
            return true;
         }
      }
      return false;
   }

   public boolean isMainBlock(BlockContext block) {
      return block == mainBlock;
   }

   public boolean redisKindIsApplicable(BlockContext block, RedisKind kind) {
      if (kind == RedisKind.COMMANDS) {
         return block == mainBlock;
      }
      if (kind == RedisKind.STANDARD
            || kind == RedisKind.TRANSACTING) {
         return true;
      }
      
      for (Command cmd : commandsByBlockName.get(block.Identifier().getText())) {
         if (redisKindIsApplicable(cmd, kind)) {
            return true;
         }
      }
      return false;
   }
   
   public boolean redisKindIsApplicable(Command cmd, RedisKind kind) {
      if (kind == RedisKind.COMMANDS || kind == RedisKind.STANDARD
            || kind == RedisKind.TRANSACTING) {
         return true;
      }
      
      switch (kind) {
      case READONLY_KEY_COMMANDS: case WATCHING:
         if (cmd.flags.contains("readonly") && consumesKeys(cmd)) {
            return true;
         }
         break;
      case READONLY_COMMANDS:
         if (cmd.flags.contains("readonly")) {
            return true;
         }
         break;
      default:
         // other cases already handled above
         throw new AssertionError();
      }
      return false;
   }
   
   public NavigableSet<RedisVersion> applicableVersions(BlockContext block, RedisKind kind) {
      String blockName = block.Identifier().getText();
      if (block == mainBlock) {
         switch (kind) {
         case TRANSACTING:
            return allVersions.tailSet(multiVersion, true);
         case WATCHING:
            return versionsPerBlock.get(MAIN_BLOCK_NAME).tailSet(watchVersion, true);
         default:
            return allVersions;
         }
      }
      return versionsPerBlock.get(blockName);
   }
}
