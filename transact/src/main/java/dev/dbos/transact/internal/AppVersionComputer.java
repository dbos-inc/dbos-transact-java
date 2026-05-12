package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
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
          workflows.stream().sorted(Comparator.comparing(RegisteredWorkflow::fullyQualifiedName));
      var it = sortedWorkflows.iterator();

      while (it.hasNext()) {
        var wf = it.next();
        hasher.update(wf.fullyQualifiedName().getBytes(StandardCharsets.UTF_8));

        var klass = wf.workflowMethod().getDeclaringClass();
        var klassPath = klass.getName().replace('.', '/') + ".class";
        var methodDesc = Type.getMethodDescriptor(wf.workflowMethod());
        var methodName = wf.workflowMethod().getName();

        try (var in = klass.getClassLoader().getResourceAsStream(klassPath)) {
          if (in == null) throw new IOException("%s class not found".formatted(klass.getName()));
          var reader = new ClassReader(in);
          reader.accept(
              new ClassVisitor(Opcodes.ASM9) {
                @Override
                public MethodVisitor visitMethod(
                    int access, String name, String desc, String signature, String[] exceptions) {
                  return (name.equals(methodName) && desc.equals(methodDesc))
                      ? new HashingMethodVisitor(hasher)
                      : null;
                }
              },
              0);
        }
      }

      return bytesToHex(hasher.digest());
    } catch (NoSuchAlgorithmException | IOException e) {
      logger.warn("Failed to compute app version", e);
      return "unknown-" + System.currentTimeMillis();
    }
  }

  static class HashingMethodVisitor extends MethodVisitor {
    private final MessageDigest md;
    private final Map<Label, Integer> labelOrdinals = new LinkedHashMap<>();
    private int nextLabelOrdinal = 0;

    public HashingMethodVisitor(MessageDigest md) {
      super(Opcodes.ASM9);
      this.md = md;
    }

    private int labelOrdinal(Label label) {
      return labelOrdinals.computeIfAbsent(label, l -> nextLabelOrdinal++);
    }

    private void update(String... values) {
      for (var v : values) {
        if (v != null) md.update(v.getBytes(StandardCharsets.UTF_8));
      }
    }

    private void update(int... values) {
      for (var v : values) {
        md.update((byte) (v >>> 24));
        md.update((byte) (v >>> 16));
        md.update((byte) (v >>> 8));
        md.update((byte) v);
      }
    }

    @Override
    public void visitLabel(Label label) {
      labelOrdinal(label);
    }

    @Override
    public void visitInsn(int opcode) {
      update(opcode);
    }

    @Override
    public void visitIntInsn(int opcode, int operand) {
      update(opcode, operand);
    }

    @Override
    public void visitVarInsn(int opcode, int varIndex) {
      update(opcode, varIndex);
    }

    @Override
    public void visitTypeInsn(int opcode, String type) {
      update(opcode);
      update(type);
    }

    @Override
    public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
      update(opcode);
      update(owner, name, descriptor);
    }

    @Override
    public void visitMethodInsn(
        int opcode, String owner, String name, String descriptor, boolean isInterface) {
      update(opcode);
      update(owner, name, descriptor);
      update(isInterface ? 1 : 0);
    }

    @Override
    public void visitInvokeDynamicInsn(
        String name,
        String descriptor,
        Handle bootstrapMethodHandle,
        Object... bootstrapMethodArguments) {
      update(name, descriptor);
      update(bootstrapMethodHandle.toString());
      for (var arg : bootstrapMethodArguments) {
        if (arg != null) update(arg.toString());
      }
    }

    @Override
    public void visitJumpInsn(int opcode, Label label) {
      update(opcode, labelOrdinal(label));
    }

    @Override
    public void visitLdcInsn(Object value) {
      update(Opcodes.LDC);
      if (value != null) update(value.toString());
    }

    @Override
    public void visitIincInsn(int varIndex, int increment) {
      update(Opcodes.IINC, varIndex, increment);
    }

    @Override
    public void visitTableSwitchInsn(int min, int max, Label dflt, Label... labels) {
      update(Opcodes.TABLESWITCH, min, max, labelOrdinal(dflt));
      for (var l : labels) update(labelOrdinal(l));
    }

    @Override
    public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels) {
      update(Opcodes.LOOKUPSWITCH, labelOrdinal(dflt));
      update(keys);
      for (var l : labels) update(labelOrdinal(l));
    }

    @Override
    public void visitMultiANewArrayInsn(String descriptor, int numDimensions) {
      update(Opcodes.MULTIANEWARRAY, numDimensions);
      update(descriptor);
    }

    @Override
    public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {
      update(labelOrdinal(start), labelOrdinal(end), labelOrdinal(handler));
      update(type);
    }
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }
}
