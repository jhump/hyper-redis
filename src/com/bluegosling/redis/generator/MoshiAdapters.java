package com.bluegosling.redis.generator;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.squareup.moshi.FromJson;
import com.squareup.moshi.Json;
import com.squareup.moshi.JsonQualifier;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.ToJson;
import com.squareup.moshi.JsonReader.Token;

/**
 * Classes that model the contents of the Redis "commands.json", for parsing the JSON via Moshi. 
 */
public final class MoshiAdapters {
   private MoshiAdapters() { throw new AssertionError(); }
   
   static class CommandDef {
      String commandName;
      String summary;
      String complexity;
      List<CommandArg> arguments;
      String since;
      String group;
   }
   
   abstract static class CommandArg {
      enum Kind {
         ARGUMENT, SUBCOMMAND, SUBCOMMAND_WITH_ARGS;
      }
      Kind kind;
      boolean optional;
      boolean multiple;
   }
   
   static class Argument extends CommandArg {
      String name;
      String type;
      List<String> enumVals;
   }

   static class SubCommand extends Argument {
      String command;
   }

   static class SubCommandWithArgs extends CommandArg {
      String command;
      List<String> name;
      List<String> type;
   }

   @JsonQualifier
   @Retention(RetentionPolicy.RUNTIME)
   @interface StringOrList {
   }

   static class ArgJson {
      String command;
      @StringOrList Object name;// either String or String[]
      @StringOrList Object type;
      @Json(name = "enum") List<String> enumVals;
      boolean optional;
      boolean multiple;
   }

   static class ArgAdapter {
      @SuppressWarnings("unchecked")
      @FromJson CommandArg fromJson(ArgJson json) {
         if (json.name instanceof List || json.type instanceof List) {
            SubCommandWithArgs ret = new SubCommandWithArgs();
            ret.kind = CommandArg.Kind.SUBCOMMAND_WITH_ARGS;
            ret.command = json.command;
            ret.name = (List<String>) json.name;
            ret.type = (List<String>) json.type;
            ret.optional = json.optional;
            ret.multiple = json.multiple;
            return ret;
         } else {
            Argument ret;
            if (json.command != null) {
               SubCommand cmd = new SubCommand();
               cmd.command = json.command;
               cmd.kind = CommandArg.Kind.SUBCOMMAND;
               ret = cmd;
            } else {
               ret = new Argument();
               ret.kind = CommandArg.Kind.ARGUMENT;
            }
            ret.name = (String) json.name;
            ret.type = (String) json.type;
            ret.enumVals = json.enumVals;
            ret.optional = json.optional;
            ret.multiple = json.multiple;
            return ret;
         }
      }
      
      @FromJson @StringOrList Object fromJson(JsonReader reader) throws IOException {
         if (reader.peek() == Token.BEGIN_ARRAY) {
            List<String> strings = new ArrayList<>();
            reader.beginArray();
            while (reader.hasNext()) {
               strings.add(reader.nextString());
            }
            reader.endArray();
            return Collections.unmodifiableList(strings);
         } else {
            return reader.nextString();
         }
      }

      // These methods are only present to make Moshi happy, which wants both "to" and "from".
      // We only deserialize from JSON and don't actually need to serialize to JSON, so they throw
      
      @ToJson String toJson(CommandArg arg) {
         throw new UnsupportedOperationException();
      }
      
      @ToJson String toJson(@StringOrList Object o) {
         throw new UnsupportedOperationException();
      }
   }
}
