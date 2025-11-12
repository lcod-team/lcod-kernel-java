package work.lcod.kernel.core;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

public final class CoreEnvPrimitives {
    private CoreEnvPrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://contract/core/env/get@1", CoreEnvPrimitives::envGet);
        registry.register("lcod://contract/core/runtime/info@1", CoreEnvPrimitives::runtimeInfo);
        return registry;
    }

    private static Object envGet(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String name = OptionalString.of(input.get("name")).orElse(null);
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is required");
        }
        boolean required = Boolean.TRUE.equals(input.get("required"));
        boolean expand = Boolean.TRUE.equals(input.get("expand"));
        String defaultValue = OptionalString.of(input.get("default")).orElse(null);

        String raw = System.getenv(name);
        boolean exists = raw != null;
        String value = exists ? raw : defaultValue;
        if (value == null && required) {
            throw new IllegalStateException("environment variable " + name + " is not defined");
        }
        if (value != null && expand) {
            value = expandPlaceholders(value);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("exists", exists);
        result.put("value", value);
        return result;
    }

    private static Object runtimeInfo(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        boolean includePlatform = !Boolean.FALSE.equals(input.get("includePlatform"));
        boolean includePid = Boolean.TRUE.equals(input.get("includePid"));
        Path cwd = ctx.workingDirectory().toAbsolutePath().normalize();
        String tmpDir = System.getProperty("java.io.tmpdir");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cwd", cwd.toString());
        result.put("homeDir", System.getProperty("user.home"));
        result.put("tmpDir", tmpDir == null ? null : Path.of(tmpDir).toAbsolutePath().normalize().toString());
        if (includePlatform) {
            result.put("platform", System.getProperty("os.name", "unknown").toLowerCase(Locale.ROOT));
        }
        if (includePid) {
            result.put("pid", ProcessHandle.current().pid());
        }
        return result;
    }

    private static String expandPlaceholders(String value) {
        StringBuilder builder = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '$' && i + 1 < value.length() && value.charAt(i + 1) == '{') {
                int close = value.indexOf('}', i + 2);
                if (close == -1) {
                    builder.append(value.substring(i));
                    break;
                }
                String token = value.substring(i + 2, close);
                if (!token.isEmpty()) {
                    String replacement = System.getenv(token);
                    if (replacement != null) {
                        builder.append(replacement);
                    }
                }
                i = close;
                continue;
            }
            builder.append(ch);
        }
        return builder.toString();
    }

    private record OptionalString(String value) {
        static OptionalString of(Object raw) {
            if (raw == null) return new OptionalString(null);
            String str = String.valueOf(raw).trim();
            return new OptionalString(str.isEmpty() ? null : str);
        }

        String orElse(String fallback) {
            return value == null ? fallback : value;
        }
    }
}
