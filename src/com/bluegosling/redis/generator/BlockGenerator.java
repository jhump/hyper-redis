package com.bluegosling.redis.generator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.lang.model.element.Modifier;

import org.antlr.v4.runtime.tree.TerminalNode;

import com.bluegosling.redis.Marshaller;
import com.bluegosling.redis.RedisVersion;
import com.bluegosling.redis.channel.RedisChannel;
import com.bluegosling.redis.concurrent.Callback;
import com.bluegosling.redis.concurrent.CompletionStageFuture;
import com.bluegosling.redis.concurrent.Observable;
import com.bluegosling.redis.concurrent.Observer;
import com.bluegosling.redis.generator.HyperRedisParser.ArgContext;
import com.bluegosling.redis.generator.HyperRedisParser.ArgListContext;
import com.bluegosling.redis.generator.HyperRedisParser.BlockContext;
import com.bluegosling.redis.generator.HyperRedisParser.CommandContext;
import com.bluegosling.redis.generator.HyperRedisParser.CommandNameContext;
import com.bluegosling.redis.generator.HyperRedisParser.NameContext;
import com.bluegosling.redis.generator.HyperRedisParser.ParamContext;
import com.bluegosling.redis.transaction.Promise;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;

/**
 * Generates files that correspond to a block of redis commands.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 * 
 * @see HyperRedisGenerator
 */
class BlockGenerator {
   private final GeneratorContext context;
   private final File outputDirectory;
   private final String packageName;
   private final BlockContext block;
   private final String blockName;
   private final SyncType syncType;
   private final RedisKind kind;
   private final SetMultimap<String, MethodSig> methods = HashMultimap.create();
   private final SetMultimap<String, String> typeHierarchy = HashMultimap.create();
   
   BlockGenerator(GeneratorContext context, File outputDirectory, String packageName,
         BlockContext block, SyncType type, RedisKind kind) {
      this.context = context;
      this.outputDirectory = outputDirectory;
      this.packageName = packageName;
      this.block = block;
      this.blockName = block.Identifier().getText();
      this.syncType = type;
      this.kind = kind;
   }
   
   void generateBlock() throws IOException {
      // We generate a set of files per distinct applicable version.
      NavigableSet<RedisVersion> versions = context.applicableVersions(block, kind);
      
      String className = kind.getGeneratedClassName(blockName);
      Set<String> superTypeNames = new LinkedHashSet<>();
      findSuperTypeNames(kind.superKinds(), superTypeNames);
      
      Predicate<Command> commandFilter = cmd -> context.redisKindIsApplicable(cmd, kind);

      String suffix = null;
      for (RedisVersion version : versions) {
         String previousSuffix = suffix;
         suffix = versionSuffix(version);
         Predicate<RedisVersion> versionFilter = v -> v == null
               || v.compareTo(version) <= 0; 
         Predicate<Command> currentFilter = addVersionCheck(commandFilter, versionFilter);

         generateVersionOfBlock(className, superTypeNames, suffix, previousSuffix,
               currentFilter, version);
      }
      
      // Finally, generate a "latest version" interface that has no
      // commands but simply extends the latest one we know about
      generateVersionOfBlock(className, superTypeNames, "", suffix, commandFilter, null);
   }
   
   private void findSuperTypeNames(Set<RedisKind> kinds, Collection<String> superTypeNames) {
      for (RedisKind superKind : kinds) {
         if (this.context.redisKindIsApplicable(block, superKind)) {
            superTypeNames.add(superKind.getGeneratedClassName(blockName));
         } else {
            findSuperTypeNames(superKind.superKinds(), superTypeNames);
         }
      }
   }
   
   private void generateVersionOfBlock(String baseClassName,
         Set<String> baseSuperTypeNames, String suffix, String previousSuffix,
         Predicate<Command> commandFilter, RedisVersion version) throws IOException {
      String className = baseClassName + suffix;
      
      // Type Variables
      List<TypeVariableName> argsList = new ArrayList<>(2);
      boolean usesKeys = false;
      boolean usesValues = false;
      if (this.context.blocksThatNeedKeyTypeArg.contains(blockName)) {
         argsList.add(TypeVariableName.get("K"));
         usesKeys = true;
      }
      if (this.context.blocksThatNeedValTypeArg.contains(blockName)) {
         argsList.add(TypeVariableName.get("V"));
         usesValues = true;
      }
      TypeVariableName[] args = argsList.toArray(new TypeVariableName[0]);

      TypeSpec.Builder iface = TypeSpec.interfaceBuilder(className)
            .addModifiers(Modifier.PUBLIC);
      argsList.forEach(iface::addTypeVariable);
      TypeSpec.Builder impl = TypeSpec.classBuilder(className + "$Impl")
            .addSuperinterface(makeTypeName(ClassName.get(packageName, className), args));
      argsList.forEach(impl::addTypeVariable);
      
      // Add class doc
      impl.addJavadoc("Concrete implementation of {@link $L}.", className);
      if (version == null && kind == RedisKind.STANDARD) {
         // Emit the doc comments from the grammar file into the "main" version of this interface
         // (the standard interface kind, no version suffix)
         javadoc(block.DocComment(), c -> iface.addJavadoc("$L\n", c));
      } else {
         // synthesize doc comments for the others
         switch (kind) {
         case READONLY_KEY_COMMANDS:
            iface.addJavadoc(
                  "The subset of {@link $L} commands containing only read-only operations that\n"
                  + "accept one or more keys as arguments$L.\n",
                  blockName,
                  version == null ? "" : ", as of version " + version.toString());
            break;
         case READONLY_COMMANDS:
            iface.addJavadoc(
                  "The subset of {@link $L} commands containing only read-only operations$L.\n",
                  blockName,
                  version == null ? "" : ", as of version " + version.toString());
            break;
         case COMMANDS:
            iface.addJavadoc(
                  "An interface representing all {@link $L} commands$L.\n",
                  blockName,
                  version == null ? "" : ", as of version " + version.toString());
            break;
         case TRANSACTING:
            String blockingPackageName = packageName.replace(".transaction", ".blocking"); 
            iface.addJavadoc(
                  "An interface for enqueueing {@link $T} commands in a transaction$L.\n"
                  + "\n@see $T#transactor",
                  ClassName.get(blockingPackageName, blockName),
                  version == null ? "" : ", as of version " + version.toString(),
                  ClassName.get(blockingPackageName, GeneratorContext.MAIN_BLOCK_NAME + suffix));
            break;
         case WATCHING:
            iface.addJavadoc(
                  "An interface with read-only {@link $L} commands that automatically issue\n"
                  + "WATCH operations for all queried keys$L.\n",
                  blockName,
                  version == null ? "" : ", as of version " + version.toString());
            break;
         default:
            assert kind == RedisKind.STANDARD;
            iface.addJavadoc(
                  "A version of {@link $L} that restricts the API to only commands available as\n"
                  + "of version $L.\n",
                  blockName, version.toString());
         }
      }

      // add super-type representing previous version
      boolean implHasSuperclass = false;
      if (previousSuffix != null) {
         String superTypeName = baseClassName + previousSuffix;
         iface.addSuperinterface(makeTypeName(ClassName.get(packageName, superTypeName), args));
         if (context.isMainBlock(block)) {
            // we generate an impl per version only of the main block
            impl.superclass(
                  makeTypeName(ClassName.get(packageName, superTypeName + "$Impl"), args));
            implHasSuperclass = true;
         }
         typeHierarchy.put(className, superTypeName);
      }
      // if optional other super-type given, interface also extends that
      for (String baseSuperTypeName : baseSuperTypeNames) {
         String superTypeName = baseSuperTypeName + suffix;
         iface.addSuperinterface(makeTypeName(ClassName.get(packageName, superTypeName), args));
         typeHierarchy.put(className, superTypeName);
      }
      
      //////////////////////////////////////////////////////////////////////////////
      // MIN_VERSION constant
      //////////////////////////////////////////////////////////////////////////////
      if (version != null) {
         iface.addField(FieldSpec.builder(String.class, "MIN_VERSION")
               .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
               .initializer(CodeBlock.builder().add("$S", version).build())
               .addJavadoc(
                     "The minimum server version required to use all methods in this interface")
               .build());
      }
      
      //////////////////////////////////////////////////////////////////////////////
      // Constructor / newInstance() factory method
      //////////////////////////////////////////////////////////////////////////////
      if (kind == RedisKind.STANDARD || (version == null && kind == RedisKind.TRANSACTING)) {
         MethodSpec.Builder factoryMethod = MethodSpec.methodBuilder("newInstance")
               .addModifiers(Modifier.STATIC, Modifier.PUBLIC)
               .addParameter(ParameterSpec.builder(RedisChannel.class, "channel").build());
         MethodSpec.Builder ctor = MethodSpec.constructorBuilder()
               .addParameter(ParameterSpec.builder(RedisChannel.class, "channel").build());
         StringBuilder factoryMethodArgs = new StringBuilder().append("channel");
         CodeBlock.Builder ctorImpl = CodeBlock.builder().add("this.channel = channel;\n");
         if (!implHasSuperclass) {
            impl.addField(FieldSpec.builder(RedisChannel.class, "channel", Modifier.FINAL).build());
         }
         
         if (usesKeys) {
            TypeName type = ParameterizedTypeName.get(
                  ClassName.get(Marshaller.class),
                  TypeVariableName.get("K"));
            factoryMethod.addParameter(ParameterSpec.builder(type, "keyMarshaller").build());
            factoryMethodArgs.append(", keyMarshaller");
            ctor.addParameter(ParameterSpec.builder(type, "keyMarshaller").build());
            if (!implHasSuperclass) {
               impl.addField(FieldSpec.builder(type, "keyMarshaller", Modifier.FINAL).build());
               ctorImpl.add("this.keyMarshaller = keyMarshaller;\n");
            }
         }
         if (usesValues) {
            TypeName type = ParameterizedTypeName.get(
                  ClassName.get(Marshaller.class),
                  TypeVariableName.get("V"));
            factoryMethod.addParameter(ParameterSpec.builder(type, "valueMarshaller").build());
            factoryMethodArgs.append(", valueMarshaller");
            ctor.addParameter(ParameterSpec.builder(type, "valueMarshaller").build());
            if (!implHasSuperclass) {
               impl.addField(FieldSpec.builder(type, "valueMarshaller", Modifier.FINAL).build());
               ctorImpl.add("this.valueMarshaller = valueMarshaller;\n");
            }
         }
         factoryMethod.addJavadoc(
               "Constructs a new {@link $L} backed by the given channel.\n"
               + "\n"
               + "@param channel the channel used to issue commands to a server\n"
               + "@param keyMarshaller handles serialization and de-serialization of keys\n"
               + "@param valueMarshaller handles serialization and de-serialization of values\n",
               className);
         if (kind == RedisKind.TRANSACTING) {
            TypeName txController =
                  ClassName.get("com.bluegosling.redis.transaction", "TransactionController");
            factoryMethodArgs.append(", controller");
            factoryMethod
                  .addParameter(ParameterSpec.builder(txController, "controller").build())
                  .addCode(CodeBlock.builder()
                        .add("return new $L$$Impl$L($L);\n", className,
                              args.length > 0 ? "<>" : "",
                              factoryMethodArgs.toString())
                        .build());
            factoryMethod.addJavadoc("@param controller the controller for interacting with the\n"
                  + "current transaction");
            ctor.addParameter(ParameterSpec.builder(txController, "controller").build());
            if (!implHasSuperclass) {
               impl.addField(FieldSpec.builder(txController, "controller", Modifier.FINAL).build());
               ctorImpl.add("this.controller = controller;\n");
            }
         } else {
            factoryMethod.addCode(CodeBlock.builder()
                  .add("return new $L$$Impl$L($L);\n", className,
                        args.length > 0 ? "<>" : "",
                        factoryMethodArgs.toString())
                  .build());
         }
         factoryMethod.returns(makeTypeName(ClassName.get(packageName, className), args));

         if (context.isMainBlock(block)) {
            iface.addMethod(factoryMethod.build());
         }
         if (context.isMainBlock(block) && previousSuffix != null) {
            ctor.addCode("super($L);\n", factoryMethodArgs.toString());
         } else {
            ctor.addCode(ctorImpl.build());
         }
         impl.addMethod(ctor.build());
      }

      // Several things are only included in the main "Redis" block
      if (context.isMainBlock(block)) {

         //////////////////////////////////////////////////////////////////////////////
         // Accessors for other blocks
         //////////////////////////////////////////////////////////////////////////////
         for (BlockContext otherBlock : context.astRoot.block()) {
            if (otherBlock == block) {
               continue;
            }
            if (!context.redisKindIsApplicable(otherBlock, kind)) {
               continue;
            }
            String otherBlockName = otherBlock.Identifier().getText();
            RedisVersion blockVersion;
            if (version == null) {
               blockVersion = null;
            } else {
               blockVersion = context.versionsPerBlock.get(otherBlockName).floor(version);
               if (blockVersion == null) {
                  continue;
               }
            }
            String versionSuffix = blockVersion == null ? "" : versionSuffix(blockVersion);
            
            List<TypeVariableName> otherBlockArgsList = new ArrayList<>();
            boolean otherBlockUsesKeys = false;
            boolean otherBlockUsesVals = false;
            if (this.context.blocksThatNeedKeyTypeArg.contains(otherBlockName)) {
               otherBlockArgsList.add(TypeVariableName.get("K"));
               otherBlockUsesKeys = true;
            }
            if (this.context.blocksThatNeedValTypeArg.contains(otherBlockName)) {
               otherBlockArgsList.add(TypeVariableName.get("V"));
               otherBlockUsesVals = true;
            }
            TypeVariableName[] otherBlockArgs = otherBlockArgsList.toArray(new TypeVariableName[0]);

            ClassName otherBlockClass = ClassName.get(packageName,
                  kind.getGeneratedClassName(otherBlockName) + versionSuffix);
            TypeName otherBlockType = makeTypeName(otherBlockClass, otherBlockArgs);
            
            MethodSpec.Builder method = MethodSpec.methodBuilder(initLower(otherBlockName))
                  .addModifiers(Modifier.PUBLIC)
                  .returns(otherBlockType);
            MethodSig sig = newSignature(className, method);
            if (isOverride(className, sig)) {
               method.addAnnotation(Override.class);
            }
            MethodSpec.Builder implMethod = method.build().toBuilder();
            // TODO: method doc
            iface.addMethod(method.addModifiers(Modifier.ABSTRACT).build());

            ClassName otherBlockImplClass = ClassName.get(packageName,
                  kind.getGeneratedClassName(otherBlockName) + "$Impl");
            StringBuilder ctorArgs = new StringBuilder("channel");
            if (otherBlockUsesKeys) {
               ctorArgs.append(", keyMarshaller");
            }
            if (otherBlockUsesVals) {
               ctorArgs.append(", valueMarshaller");
            }
            impl.addMethod(ensureOverrides(implMethod).addCode(
                  CodeBlock.builder()
                        .add("return new $T$L($L);\n",
                              otherBlockImplClass,
                              (otherBlockUsesKeys || otherBlockUsesVals) ? "<>" : "",
                              ctorArgs.toString())
                        .build())
                  .build());
         }
         
         if (kind == RedisKind.STANDARD) {
            //////////////////////////////////////////////////////////////////////////////
            // Factory methods to get different sync types
            //////////////////////////////////////////////////////////////////////////////
            // TODO: factory methods to create different sync types
            // (e.g. get blocking interface from an async one)
            for (SyncType otherType : SyncType.values()) {
               if (otherType == syncType || otherType == SyncType.PROMISES) {
                  continue;
               }
               String methodName = "to" + initCap(otherType.subPackage());
               MethodSpec.Builder method = MethodSpec.methodBuilder(methodName)
                     .addModifiers(Modifier.PUBLIC);
               
               
               
            }
            
            //////////////////////////////////////////////////////////////////////////////
            // transactor() factory method
            //////////////////////////////////////////////////////////////////////////////
            // TODO: transactor factory methods
            
         } else if (kind == RedisKind.TRANSACTING) {
            //////////////////////////////////////////////////////////////////////////////
            // execute() and discard() methods
            //////////////////////////////////////////////////////////////////////////////
            // TODO: execute() and discard() methods for tx
            
         } else if (kind == RedisKind.WATCHING) {
            //////////////////////////////////////////////////////////////////////////////
            // watch(...) and unwatch() methods
            //////////////////////////////////////////////////////////////////////////////
            // TODO: watch() and unwatch() methods for watchers
            
         }
      }

      //////////////////////////////////////////////////////////////////////////////
      // Methods for each command
      //////////////////////////////////////////////////////////////////////////////
      for (Command cmd : context.commandsByBlockName.get(blockName)) {
         if (commandFilter.test(cmd)) {
            generateCommandMethods(cmd.astContext, className, version, iface, impl);
         }
      }
      
      //////////////////////////////////////////////////////////////////////////////
      // Finished with the interface
      //////////////////////////////////////////////////////////////////////////////
      JavaFile.builder(packageName, iface.build())
            .skipJavaLangImports(true)
            .build()
            .writeTo(outputDirectory);
      
      //////////////////////////////////////////////////////////////////////////////
      // Now for the implementation class
      //////////////////////////////////////////////////////////////////////////////
      //
      // For the main block, we create an implementation class per version, for proper type-safety
      // of transactor methods without resulting in the final version have many per-version
      // overloads (the arguments for each overload would have basically the same functional
      // shape, so lambdas would be unusable for defining transaction blocks).
      //
      // For the other blocks, we only need a single implementation class (not per version).
      if ((kind == RedisKind.STANDARD || kind == RedisKind.TRANSACTING)
            && (version == null || context.isMainBlock(block))) {
         JavaFile.builder(packageName, impl.build())
               .skipJavaLangImports(true)
               .build()
               .writeTo(outputDirectory);
      }
   }
   
   void generateCommandMethods(CommandContext cmd, String className, RedisVersion version,
         TypeSpec.Builder iface, TypeSpec.Builder impl) {
      for (List<ArgContext> cmdArgs : expandArgPermutations(cmd.arg())) {
         String methodName = computeMethodName(cmd.commandName(), cmdArgs);
         TypeName returnType =
               cmd.type() == null ? TypeName.VOID : TypeNames.fromParseTree(cmd.type());

         for (int arrayTypePass = 0; arrayTypePass < 2; arrayTypePass++) {
            MethodSpec.Builder method = MethodSpec.methodBuilder(methodName)
                  .addModifiers(Modifier.PUBLIC);
            
            int builderStartIndex = addParameters(method, cmdArgs, arrayTypePass != 0);
            if (builderStartIndex >= 0) {
               // TODO: builder class
//                  iface.addType(createBuilder(methodName,
//                        cmdArgs.subList(builderStartIndex, cmdArgs.size())));
            } else {
               addReturnType(method, syncType, cmd.stream() != null, returnType);
            }

            int arrayCount = 0;
            boolean isVarArg = false;
            if (arrayTypePass == 0) {
               boolean lastIsArray = false;
               for (ParameterSpec param : method.build().parameters) {
                  if (param.type instanceof ArrayTypeName) {
                     arrayCount++;
                     lastIsArray = true;
                  } else {
                     lastIsArray = false;
                  }
               }
               if (lastIsArray) {
                  method.varargs();
                  isVarArg = true;
               }
            }
            
            MethodSig signature = newSignature(className, method);
            
            if (isOverride(className, signature)) {
               // we're just re-specifying overridden method on interface
               iface.addMethod(method.addModifiers(Modifier.ABSTRACT)
                     .addAnnotation(Override.class)
                     .build());
               // so we can now bail before adding implementation
               if (arrayCount > 0) {
                  // add override for version of method that uses collections
                  continue;
               }
               break;
            } else if (arrayTypePass == 0 && (arrayCount > 1 || (arrayCount == 1 && !isVarArg))) {
               if (isVarArg) {
                  arrayCount--;
               }
               int[] expandedSizes = new int[arrayCount];
               // There is at least one array parameter that is not a var-arg parameter.
               // So we generate convenient overloads for 1, 2, 3 and fixed parameters (eliding
               // where necessary to prevent a collision, which can happen for the 1-argument
               // case for some commands)
               generateExpandedMethods(cmd, iface, impl, method, arrayCount - 1, expandedSizes);
               for (int i = 0; i < expandedSizes.length; i++) {
                  expandedSizes[i] = -1;
               }
               generateMethod(cmd, iface, impl, method, true, null);
            } else {
               generateMethod(cmd, iface, impl, method, false, null);
            }

            if (arrayCount == 0) {
               // no need for a second pass to overload array args with collections
               break;
            }
         }
      }
   }
   
   private void generateExpandedMethods(CommandContext cmd, TypeSpec.Builder iface,
         TypeSpec.Builder impl, MethodSpec.Builder method, int arrayArgIndex,
         int[] arrayExpansions) {
      if (arrayArgIndex < 0) {
         generateMethod(cmd, iface, impl, method, true, arrayExpansions);
         return;
      }
      for (int i = 0; i < 3; i++) {
         arrayExpansions[arrayArgIndex] = i;
         generateExpandedMethods(cmd, iface, impl, method, arrayArgIndex - 1, arrayExpansions);
      }
   }
   
   private void generateMethod(CommandContext cmd, TypeSpec.Builder iface, TypeSpec.Builder impl,
         MethodSpec.Builder method, boolean useArray, int[] arrayExpansions) {
      if (useArray && arrayExpansions != null) {
         // TODO: replace arrays with fixed arg lists per given expansion sizes
         return;
      }
      
      // clone the method spec before adding abstract modifier
      // so we can re-use it for the impl
      MethodSpec.Builder implMethod = ensureOverrides(method.build().toBuilder());

      // We just add java doc to original definition of method (not to
      // various overrides in sub-interfaces and in concrete impl)
      javadoc(cmd.DocComment(), c -> method.addJavadoc("$L\n", c));
      iface.addMethod(method.addModifiers(Modifier.ABSTRACT).build());
      
      // TODO: real body
      impl.addMethod(implMethod.addCode(todoImpl(method)).build());
   }

   private String initLower(String name) {
      StringBuilder sb = new StringBuilder(name);
      sb.setCharAt(0, Character.toLowerCase(sb.charAt(0)));
      return sb.toString();
   }
   
   private MethodSig newSignature(String declaringClassName, MethodSpec.Builder method) {
      MethodSig sig = new MethodSig(method);
      methods.put(declaringClassName, sig);
      return sig;
   }

   private boolean isOverride(String className, MethodSig sig) {
      for (String superType : typeHierarchy.get(className)) {
         if (methods.containsEntry(superType, sig)) {
            return true;
         }
      }
      return false;
   }
   
   private MethodSpec.Builder ensureOverrides(MethodSpec.Builder method) {
      if (method.build().annotations.stream().noneMatch(
            a -> a.type.equals(ClassName.get(Override.class)))) {
         method.addAnnotation(Override.class);
      }
      return method;
   }
   
   private CodeBlock todoImpl(MethodSpec.Builder method) {
      TypeName ret = method.build().returnType;
      CodeBlock.Builder block = CodeBlock.builder().add("// TODO: implement me!\n");
      if (ret.equals(TypeName.BOOLEAN)) {
         block.add("return false;");
      } else if (ret.equals(TypeName.BYTE)) {
         block.add("return (byte) 0;");
      } else if (ret.equals(TypeName.SHORT)) {
         block.add("return (short) 0;");
      } else if (ret.equals(TypeName.CHAR)) {
         block.add("return (char) 0;");
      } else if (ret.equals(TypeName.INT) || ret.equals(TypeName.LONG)
            || ret.equals(TypeName.DOUBLE) || ret.equals(TypeName.FLOAT)) {
         block.add("return 0;");
      } else if (ret.equals(TypeName.VOID)) {
         block.add("return;");
      } else {
         block.add("return null;");
      }
      return block.add("\n").build();
   }

   Predicate<Command> addVersionCheck(Predicate<Command> cmdFilter,
         Predicate<RedisVersion> versionFilter) {
      Predicate<Command> cmdFilterByVersion = cmd -> {
         return versionFilter.test(cmd.since);
      };
      return cmdFilter == null ? cmdFilterByVersion : cmdFilter.and(cmdFilterByVersion);
   }
   
   void addReturnType(Builder method, SyncType generatedType, boolean stream, TypeName returnType) {
      switch (generatedType) {
      case BLOCKING:
         if (stream) {
            method.returns(ParameterizedTypeName.get(ClassName.get(Iterator.class), returnType));
         } else {
            method.returns(returnType);
         }
         break;
      case CALLBACKS:
         TypeName callbackType;
         if (stream) {
            callbackType =
                  ParameterizedTypeName.get(ClassName.get(Observer.class), returnType.box());
         } else if (returnType.equals(TypeName.LONG)) {
            callbackType = ClassName.get(Callback.OfLong.class);
         } else if (returnType.equals(TypeName.INT)) {
            callbackType = ClassName.get(Callback.OfInt.class);
         } else if (returnType.equals(TypeName.DOUBLE)) {
            callbackType = ClassName.get(Callback.OfDouble.class);
         } else if (returnType.equals(TypeName.BOOLEAN)) {
            callbackType = ClassName.get(Callback.OfBoolean.class);
         } else if (returnType.equals(TypeName.VOID)) {
               callbackType = ClassName.get(Callback.OfVoid.class);
         } else {
            callbackType =
                  ParameterizedTypeName.get(ClassName.get(Callback.class), returnType.box());
         }
         method.addParameter(ParameterSpec.builder(callbackType, "callback").build());
         break;
      case FUTURES:
         if (stream) {
            method.returns(ParameterizedTypeName.get(
                  ClassName.get(Observable.class),
                  returnType.box()));
         } else {
            method.returns(ParameterizedTypeName.get(
                  ClassName.get(CompletionStageFuture.class),
                  returnType.box()));
         }
         break;
      case PROMISES:
         if (stream) {
            method.returns(ParameterizedTypeName.get(
                  ClassName.get(Promise.class),
                  ArrayTypeName.of(returnType)));
         } else {
            method.returns(ParameterizedTypeName.get(
                  ClassName.get(Promise.class),
                  returnType.box()));
         }
         break;
      }
   }

   int addParameters(MethodSpec.Builder method, List<ArgContext> args,
         boolean useCollection) {
      int index = 0;
      for (ArgContext arg : args) {
         if (arg.repeatedChoice() != null) {
            return index;
         }
         if (arg.param() != null) {
            TypeName type = TypeNames.fromParseTree(arg.param());
            if (useCollection && type instanceof ArrayTypeName) {
               type = asCollection((ArrayTypeName) type);
            }
            method.addParameter(
                  ParameterSpec.builder(type, determineParamName(arg.param())).build());
         }
         index++;
      }
      return -1;
   }

   private TypeName asCollection(ArrayTypeName type) {
      TypeName elementType = type.componentType.box();
      if (elementType instanceof ArrayTypeName) {
         elementType = asCollection((ArrayTypeName) elementType);
      }
      return ParameterizedTypeName.get(
            ClassName.get(Collection.class),
            WildcardTypeName.subtypeOf(elementType));
   }

   private String determineParamName(ParamContext ctx) {
      if (ctx.paramDef().Identifier() != null) {
         return ctx.paramDef().Identifier().getText();
      }
      String name = ctx.paramDef().type().declaredType().qualifiedIdentifier().getText();
      boolean isArray = !ctx.paramDef().type().ArrayIndicator().isEmpty()
            || ctx.ArrayIndicator() != null;
      if (name.equals("K")) {
         return isArray ? "keys" : "key";
      } else if (name.equals("V")) {
         return isArray ? "vals" : "val";
      }
      if (Character.isLowerCase(name.charAt(0)) && !isArray) {
         return name;
      }
      StringBuilder sb = new StringBuilder(name);
      sb.setCharAt(0, Character.toLowerCase(sb.charAt(0)));
      if (isArray) {
         if (sb.charAt(sb.length() - 1) == 's') {
            sb.append("es");
         } else {
            sb.append('s');
         }
      }
      return sb.toString();
   }

   List<List<ArgContext>> expandArgPermutations(List<ArgContext> args) {
      List<List<ArgContext>> argLists = new ArrayList<>();
      recursivelyExpandArgs(args, new ArrayList<>(), argLists::add);
      return argLists;
   }
   
   private void recursivelyExpandArgs(List<ArgContext> args, ArrayList<ArgContext> soFar,
         Consumer<ArrayList<ArgContext>> atEnd) {
      if (args.isEmpty()) {
         atEnd.accept(soFar);
         return;
      }
      ArgContext next = args.get(0);
      // we expand optionals and choices into distinct arg lists
      if (next.optional() != null) {
         // without option
         recursivelyExpandArgs(args.subList(1, args.size()), soFar, atEnd);
         // and with it
         @SuppressWarnings("unchecked")
         ArrayList<ArgContext> option = (ArrayList<ArgContext>) soFar.clone();
         recursivelyExpandArgs(next.optional().argList().arg(), option,
               list -> recursivelyExpandArgs(args.subList(1, args.size()), list, atEnd));
      } else if (next.choice() != null) {
         for (ArgListContext argListChoice : next.choice().argList()) {
            // evaluate for each choice
            @SuppressWarnings("unchecked")
            ArrayList<ArgContext> choice = (ArrayList<ArgContext>) soFar.clone();
            recursivelyExpandArgs(argListChoice.arg(), choice,
                  list -> recursivelyExpandArgs(args.subList(1, args.size()), list, atEnd));
         }
      } else {
         // nothing to expand: add the argument and continue
         soFar.add(next);
         recursivelyExpandArgs(args.subList(1, args.size()), soFar, atEnd);
      }
   }
   
   String computeMethodName(CommandNameContext cmdName, List<ArgContext> args) {
      StringBuilder sb = new StringBuilder();
      Consumer<NameContext> appender = n -> {
         String s = n.Identifier().get(0).getText();
         int i = sb.length();
         // strip out dashes (allowed for Redis wire format, disallowed in method name)
         int cur = 0;
         int next = s.indexOf('-');
         while (next >= 0) {
            sb.append(s, cur, next);
            cur = next + 1;
            next = s.indexOf('-', cur);
         }
         sb.append(s, cur, s.length());
         if (i > 0) {
            // capitalize subsequent word to achieve camel-case
            char ch = sb.charAt(i);
            sb.setCharAt(i, Character.toUpperCase(ch));
         }
      };
      if (cmdName.Question() == null) {
         appender.accept(cmdName.name());
      }
      for (ArgContext arg : args) {
         if (arg.symbol() != null && arg.symbol().Question() == null) {
            appender.accept(arg.symbol().name());
         }
      }
      return sb.toString();
   }
   
   TypeName makeTypeName(ClassName clazz, TypeName[] typeArgs) {
      return typeArgs.length == 0
         ? clazz
         : ParameterizedTypeName.get(clazz, typeArgs);
   }
   
   void javadoc(Iterable<TerminalNode> comments, Consumer<String> commentWriter) {
      for (TerminalNode cmt : comments) {
         String cmtText = cmt.getText();
         assert cmt.getText().startsWith("//");
         commentWriter.accept(cmtText.substring(2).trim());
      }
   }
   
   String versionSuffix(RedisVersion version) {
      StringBuilder sb = new StringBuilder();
      sb.append(version.majorVersion()).append(version.minorVersion());
      if (version.pointVersion() != 0) {
         sb.append(version.pointVersion());
      }
      return sb.toString();
   }
   
   String initCap(String s) {
      char first = s.charAt(0);
      if (Character.isUpperCase(first)) {
         return s;
      }
      StringBuilder sb = new StringBuilder(s);
      sb.setCharAt(0, Character.toUpperCase(first));
      return sb.toString();
   }
}
