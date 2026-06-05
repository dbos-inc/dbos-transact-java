package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppVersionComputer {

  private static Logger logger = LoggerFactory.getLogger(AppVersionComputer.class);

  public static String computeAppVersion(
      String dbosVersion, Collection<RegisteredWorkflow> workflows) {
    try {
      final var hasher = MessageDigest.getInstance("SHA-256");
      hasher.update(dbosVersion.getBytes(StandardCharsets.UTF_8));

      var workflowIterator =
          workflows.stream()
              .sorted(Comparator.comparing(RegisteredWorkflow::fullyQualifiedName))
              .iterator();

      while (workflowIterator.hasNext()) {
        var wf = workflowIterator.next();
        var klass = wf.workflowMethod().getDeclaringClass();
        var klassPath = klass.getName().replace('.', '/') + ".class";
        var methodDesc = getMethodDescriptor(wf.workflowMethod());

        hasher.update(wf.fullyQualifiedName().getBytes(StandardCharsets.UTF_8));
        hasher.update(methodDesc.getBytes(StandardCharsets.UTF_8));
        var cl = klass.getClassLoader();
        if (cl == null) cl = ClassLoader.getSystemClassLoader();
        try (var in = cl.getResourceAsStream(klassPath)) {
          if (in == null) throw new IOException("%s class not found".formatted(klass.getName()));
          hashMethodBytecode(hasher, in, wf.workflowMethod().getName(), methodDesc);
        }
      }

      return HexFormat.of().formatHex(hasher.digest());
    } catch (Exception e) {
      logger.warn("Failed to compute app version", e);
      return "unknown-" + System.currentTimeMillis();
    }
  }

  /**
   * Manually parses the JVM class file format (JVMS 4.1, 4.4, 4.5, 4.6, 4.7) to locate the method
   * matching {@code methodName/methodDesc}, then hashes the raw bytecodes and exception table from
   * its Code attribute (JVMS 4.7.3).
   *
   * <p>Only CONSTANT_Utf8 entries are decoded from the constant pool — all other entry types are
   * skipped by size, since we only need string values to match method names and descriptors.
   *
   * <p>Once the matching Code attribute is found, the code[] bytes and exception_table are fed into
   * the SHA-256 digest. The method stops early — remaining methods and attributes after the target
   * are not parsed.
   */
  private static void hashMethodBytecode(
      MessageDigest hasher, InputStream in, String methodName, String methodDesc) {
    try {
      var data = new DataInputStream(in);

      // --- ClassFile header (JVMS 4.1) ---
      if (data.readInt() != 0xCAFEBABE) {
        throw new IOException("Invalid class file: bad magic number");
      }
      data.readUnsignedShort(); // minor_version
      data.readUnsignedShort(); // major_version

      // --- Constant pool (JVMS 4.4) ---
      // Indices start at 1; entry 0 is reserved and not stored.
      int cpCount = data.readUnsignedShort();
      var utf8Pool = new String[cpCount];

      for (int i = 1; i < cpCount; i++) {
        int tag = data.readUnsignedByte();
        switch (tag) {
          case 1: // CONSTANT_Utf8 (JVMS 4.4.7) — stored in modified UTF-8
            utf8Pool[i] = data.readUTF();
            break;
          case 3: // CONSTANT_Integer
          case 4: // CONSTANT_Float
            data.skipNBytes(4);
            break;
          case 5: // CONSTANT_Long
          case 6: // CONSTANT_Double
            data.skipNBytes(8);
            i++; // Long and Double occupy two consecutive constant pool slots
            break;
          case 7: // CONSTANT_Class
          case 8: // CONSTANT_String
          case 16: // CONSTANT_MethodType
          case 19: // CONSTANT_Module
          case 20: // CONSTANT_Package
            data.skipNBytes(2);
            break;
          case 9: // CONSTANT_Fieldref
          case 10: // CONSTANT_Methodref
          case 11: // CONSTANT_InterfaceMethodref
          case 12: // CONSTANT_NameAndType
          case 17: // CONSTANT_Dynamic
          case 18: // CONSTANT_InvokeDynamic
            data.skipNBytes(4);
            break;
          case 15: // CONSTANT_MethodHandle
            data.skipNBytes(3);
            break;
          default:
            throw new IOException("Unknown constant pool tag: " + tag);
        }
      }

      // --- Class header fields (JVMS 4.1) ---
      data.readUnsignedShort(); // access_flags
      data.readUnsignedShort(); // this_class
      data.readUnsignedShort(); // super_class

      // --- Interfaces (JVMS 4.1) ---
      int ifCount = data.readUnsignedShort();
      data.skipNBytes(ifCount * 2L); // each entry is a u2 constant pool index

      // --- Fields (JVMS 4.5) — skip entirely ---
      int fieldCount = data.readUnsignedShort();
      for (int i = 0; i < fieldCount; i++) {
        data.skipNBytes(6); // access_flags + name_index + descriptor_index
        int attrCount = data.readUnsignedShort();
        for (int j = 0; j < attrCount; j++) {
          data.skipNBytes(2); // attribute_name_index
          data.skipNBytes(Integer.toUnsignedLong(data.readInt())); // attribute_length (u4)
        }
      }

      // --- Methods (JVMS 4.6) ---
      int methodCount = data.readUnsignedShort();
      boolean found = false;
      for (int i = 0; i < methodCount && !found; i++) {
        data.readUnsignedShort(); // access_flags
        int nameIndex = data.readUnsignedShort();
        int descIndex = data.readUnsignedShort();
        int attrCount = data.readUnsignedShort();

        // Resolve method name and descriptor from the constant pool.
        String name = nameIndex < cpCount ? utf8Pool[nameIndex] : null;
        String desc = descIndex < cpCount ? utf8Pool[descIndex] : null;
        boolean isTarget = methodName.equals(name) && methodDesc.equals(desc);

        for (int j = 0; j < attrCount; j++) {
          int attrNameIndex = data.readUnsignedShort();
          long attrLen = Integer.toUnsignedLong(data.readInt()); // attribute_length (u4)

          if (isTarget) {
            String attrName = attrNameIndex < cpCount ? utf8Pool[attrNameIndex] : null;
            if ("Code".equals(attrName)) {

              // --- Code attribute (JVMS 4.7.3) ---
              // Contains the actual JVM bytecodes and exception table for the method body.
              data.skipNBytes(4); // max_stack (u2) + max_locals (u2)

              // code[] — the raw JVM instruction stream.
              int codeLen = data.readInt();
              if (codeLen < 0) throw new IOException("code_length exceeds maximum supported size");
              byte[] code = new byte[codeLen];
              data.readFully(code);
              hasher.update(code);

              // exception_table — structured try-catch entries.
              // Each entry is 8 bytes: start_pc (u2) + end_pc (u2) + handler_pc (u2) + catch_type
              // (u2).
              int excLen = data.readUnsignedShort();
              for (int k = 0; k < excLen; k++) {
                byte[] entry = new byte[8];
                data.readFully(entry);
                hasher.update(entry);
              }

              // Skip sub-attributes (LineNumberTable, LocalVariableTable, etc.).
              int codeAttrCount = data.readUnsignedShort();
              for (int k = 0; k < codeAttrCount; k++) {
                data.skipNBytes(2); // attribute_name_index
                data.skipNBytes(Integer.toUnsignedLong(data.readInt())); // attribute_length (u4)
              }

              found = true;
            } else {
              data.skipNBytes(attrLen);
            }
          } else {
            data.skipNBytes(attrLen);
          }
        }
      }

      if (!found) {
        throw new IOException(
            "Method %s%s not found in class file".formatted(methodName, methodDesc));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to hash method bytecode", e);
    }
  }

  private static String getMethodDescriptor(Method method) {
    var sb = new StringBuilder("(");
    for (var param : method.getParameterTypes()) {
      sb.append(getTypeDescriptor(param));
    }
    sb.append(")");
    sb.append(getTypeDescriptor(method.getReturnType()));
    return sb.toString();
  }

  private static String getTypeDescriptor(Class<?> type) {
    if (type.isPrimitive()) {
      if (type == void.class) return "V";
      if (type == int.class) return "I";
      if (type == long.class) return "J";
      if (type == byte.class) return "B";
      if (type == short.class) return "S";
      if (type == char.class) return "C";
      if (type == float.class) return "F";
      if (type == double.class) return "D";
      if (type == boolean.class) return "Z";
    }
    if (type.isArray()) {
      return "[" + getTypeDescriptor(type.getComponentType());
    }
    return "L" + type.getName().replace('.', '/') + ";";
  }
}
