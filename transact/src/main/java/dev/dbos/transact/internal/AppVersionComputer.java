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

      var sortedWorkflows =
          workflows.stream()
              .sorted(Comparator.comparing(RegisteredWorkflow::fullyQualifiedName))
              .iterator();

      while (sortedWorkflows.hasNext()) {
        var wf = sortedWorkflows.next();
        var klass = wf.workflowMethod().getDeclaringClass();
        var klassPath = klass.getName().replace('.', '/') + ".class";
        var methodDesc = getMethodDescriptor(wf.workflowMethod());

        hasher.update(wf.fullyQualifiedName().getBytes(StandardCharsets.UTF_8));
        hasher.update(methodDesc.getBytes(StandardCharsets.UTF_8));
        try (var in = klass.getClassLoader().getResourceAsStream(klassPath)) {
          if (in == null) throw new IOException("%s class not found".formatted(klass.getName()));
          hashMethodBytecode(hasher, in, wf.workflowMethod().getName(), methodDesc);
        }
      }

      return HexFormat.of().formatHex(hasher.digest());
    } catch (Throwable e) {
      logger.warn("Failed to compute app version", e);
      return "unknown-" + System.currentTimeMillis();
    }
  }

  /**
   * Manually parses the JVM class file format (JVMS Chapter 4) to locate the method matching {@code
   * methodName/methodDesc}, then hashes the raw bytecode and exception table from its Code
   * attribute.
   */
  private static void hashMethodBytecode(
      MessageDigest hasher, InputStream in, String methodName, String methodDesc) {
    try {
      var data = new DataInputStream(in);

      if (data.readInt() != 0xCAFEBABE) {
        throw new IOException("Invalid class file: bad magic number");
      }

      data.readUnsignedShort(); // minor_version
      data.readUnsignedShort(); // major_version

      int cpCount = data.readUnsignedShort();
      var utf8Pool = new String[cpCount];

      for (int i = 1; i < cpCount; i++) {
        int tag = data.readUnsignedByte();
        switch (tag) {
          case 1: // CONSTANT_Utf8
            int len = data.readUnsignedShort();
            byte[] bytes = new byte[len];
            data.readFully(bytes);
            utf8Pool[i] = new String(bytes, StandardCharsets.UTF_8);
            break;
          case 3: // CONSTANT_Integer
          case 4: // CONSTANT_Float
            data.skipBytes(4);
            break;
          case 5: // CONSTANT_Long
          case 6: // CONSTANT_Double
            data.skipBytes(8);
            i++; // Long and Double occupy two constant pool slots
            break;
          case 7: // CONSTANT_Class
          case 8: // CONSTANT_String
          case 16: // CONSTANT_MethodType
          case 19: // CONSTANT_Module
          case 20: // CONSTANT_Package
            data.skipBytes(2);
            break;
          case 9: // CONSTANT_Fieldref
          case 10: // CONSTANT_Methodref
          case 11: // CONSTANT_InterfaceMethodref
          case 12: // CONSTANT_NameAndType
          case 17: // CONSTANT_Dynamic
          case 18: // CONSTANT_InvokeDynamic
            data.skipBytes(4);
            break;
          case 15: // CONSTANT_MethodHandle
            data.skipBytes(3);
            break;
          default:
            throw new IOException("Unknown constant pool tag: " + tag);
        }
      }

      data.readUnsignedShort(); // access_flags
      data.readUnsignedShort(); // this_class
      data.readUnsignedShort(); // super_class

      int ifCount = data.readUnsignedShort();
      data.skipBytes(ifCount * 2);

      int fieldCount = data.readUnsignedShort();
      for (int i = 0; i < fieldCount; i++) {
        data.skipBytes(6); // access_flags + name_index + descriptor_index
        int attrCount = data.readUnsignedShort();
        for (int j = 0; j < attrCount; j++) {
          data.skipBytes(2); // attribute_name_index
          data.skipBytes(data.readInt()); // attribute_length
        }
      }

      int methodCount = data.readUnsignedShort();
      boolean found = false;
      for (int i = 0; i < methodCount; i++) {
        data.readUnsignedShort(); // access_flags
        int nameIndex = data.readUnsignedShort();
        int descIndex = data.readUnsignedShort();
        int attrCount = data.readUnsignedShort();

        String name = nameIndex < cpCount ? utf8Pool[nameIndex] : null;
        String desc = descIndex < cpCount ? utf8Pool[descIndex] : null;
        boolean isTarget = methodName.equals(name) && methodDesc.equals(desc);

        for (int j = 0; j < attrCount; j++) {
          int attrNameIndex = data.readUnsignedShort();
          int attrLen = data.readInt();

          if (isTarget && !found) {
            String attrName = attrNameIndex < cpCount ? utf8Pool[attrNameIndex] : null;
            if ("Code".equals(attrName)) {

              data.skipBytes(4); // max_stack + max_locals
              int codeLen = data.readInt();
              byte[] code = new byte[codeLen];
              data.readFully(code);
              hasher.update(code);

              int excLen = data.readUnsignedShort();
              for (int k = 0; k < excLen; k++) {
                byte[] entry = new byte[8]; // start_pc + end_pc + handler_pc + catch_type
                data.readFully(entry);
                hasher.update(entry);
              }

              int codeAttrCount = data.readUnsignedShort();
              for (int k = 0; k < codeAttrCount; k++) {
                data.skipBytes(2); // attribute_name_index
                data.skipBytes(data.readInt()); // attribute_length
              }

              found = true;
            } else {
              data.skipBytes(attrLen);
            }
          } else {
            data.skipBytes(attrLen);
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
