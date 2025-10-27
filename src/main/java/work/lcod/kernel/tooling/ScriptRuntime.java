package work.lcod.kernel.tooling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.StepMeta;

final class ScriptRuntime {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String LOG_CONTRACT_ID = "lcod://contract/tooling/log@1";

    private ScriptRuntime() {}

    static Object run(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) throws Exception {
        String source = optionalString(input.get("source"));
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("tooling/script requires non-empty source");
        }

        long timeoutMs = readTimeout(input.get("timeoutMs"));
        Map<String, Object> initialState = prepareInitialState(ctx, input.get("input"), input);
        Map<String, Object> scopeState = deepClone(initialState);
        Map<String, Object> bindings = resolveBindings(initialState, asObject(input.get("bindings")));
        Map<String, Object> metaInput = asObject(input.get("meta"));
        Map<String, Object> config = asObject(input.get("config"));

        Map<String, Object> scopeMeta = deepClone(metaInput);
        registerStreams(ctx, scopeState, input.get("streams"));
        List<String> messages = new ArrayList<>();

        try (Context polyglot = Context
            .newBuilder("js")
            .allowHostAccess(HostAccess.ALL)
            .allowExperimentalOptions(true)
            .allowAllAccess(true)
            .option("engine.WarnInterpreterOnly", "false")
            .option("js.ecmascript-version", "2023")
            .build()) {

            injectProcessGlobal(polyglot, ctx);
            injectConsoleGlobal(polyglot, ctx, messages);
            ToolsRegistry tools = compileTools(polyglot, input.get("tools"));
            ImportsRegistry imports = buildImports(ctx, input.get("imports"), meta);
            Value importsObject = buildImportsObject(polyglot, imports.view());
            ApiBridge api = new ApiBridge(ctx, polyglot, config, messages, tools, imports, importsObject, meta);

            Value scopeValue = MapBuilder.buildScope(polyglot, bindings, scopeState, scopeMeta, importsObject);
            Value apiValue = polyglot.asValue(api);
            Value function = compileFunction(polyglot, source);
            Value rawResult = function.execute(scopeValue, apiValue);
            Object result = awaitValue(polyglot, rawResult);

            if (messages.isEmpty()) {
                return result;
            }
            if (result instanceof Map<?, ?> map) {
                Map<String, Object> copy = new LinkedHashMap<>();
                map.forEach((k, v) -> copy.put(String.valueOf(k), v));
                mergeMessages(copy, messages);
                return copy;
            }
            return Map.of("result", result, "messages", new ArrayList<>(messages));
        }
    }

    private static void injectProcessGlobal(Context context, ExecutionContext ctx) {
        Value bindings = context.getBindings("js");
        if (bindings.hasMember("process")) {
            return;
        }
        Value process = context.eval("js", "({})");
        Map<String, String> env = System.getenv();
        Value envValue = toJsValue(context, env);
        process.putMember("env", envValue);
        ProxyExecutable cwdFn = args -> context.asValue(ctx.workingDirectory().toString());
        process.putMember("cwd", cwdFn);
        bindings.putMember("process", process);
    }

    private static void injectConsoleGlobal(Context context, ExecutionContext ctx, List<String> messages) {
        Value bindings = context.getBindings("js");
        Value console = context.eval("js", "({})");
        console.putMember("log", createConsoleFunction(ctx, messages, "info"));
        console.putMember("info", createConsoleFunction(ctx, messages, "info"));
        console.putMember("warn", createConsoleFunction(ctx, messages, "warn"));
        console.putMember("error", createConsoleFunction(ctx, messages, "error"));
        console.putMember("debug", createConsoleFunction(ctx, messages, "debug"));
        console.putMember("trace", createConsoleFunction(ctx, messages, "debug"));
        bindings.putMember("console", console);
    }

    private static ProxyExecutable createConsoleFunction(ExecutionContext ctx, List<String> messages, String level) {
        return args -> {
            String rendered = renderConsoleArgs(args);
            if (rendered != null && !rendered.isEmpty()) {
                messages.add(rendered);
                try {
                    ctx.call(LOG_CONTRACT_ID, Map.of("level", level, "message", rendered), null);
                } catch (Exception ignored) {
                    // best-effort logging
                }
            }
            return null;
        };
    }

    private static String renderConsoleArgs(Value[] args) {
        if (args == null || args.length == 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            Object value = valueToJava(args[i]);
            if (i > 0) {
                builder.append(' ');
            }
            builder.append(value == null ? "null" : String.valueOf(value));
        }
        return builder.toString();
    }

    private static Value buildImportsObject(Context context, Map<String, ImportFunction> imports) {
        Value jsImports = context.eval("js", "({})");
        if (imports == null || imports.isEmpty()) {
            return jsImports;
        }
        for (var entry : imports.entrySet()) {
            String alias = entry.getKey();
            ImportFunction fn = entry.getValue();
            if (alias == null || fn == null) continue;
            ProxyExecutable executable = args -> {
                Object payload = args.length > 0 ? valueToJava(args[0]) : Map.of();
                Map<String, Object> input = asObject(payload);
                try {
                    Object result = fn.invoke(input);
                    return toJsValue(context, result);
                } catch (Exception ex) {
                    throw new RuntimeException("Import " + alias + " failed: " + ex.getMessage(), ex);
                }
            };
            jsImports.putMember(alias, executable);
        }
        return jsImports;
    }

    private static void mergeMessages(Map<String, Object> target, List<String> messages) {
        Object existing = target.get("messages");
        List<String> merged = new ArrayList<>();
        if (existing instanceof List<?> list) {
            for (Object item : list) {
                merged.add(String.valueOf(item));
            }
        }
        merged.addAll(messages);
        target.put("messages", merged);
    }

    private static Map<String, Object> prepareInitialState(ExecutionContext ctx, Object rawInput, Map<String, Object> payload) {
        Map<String, Object> attributeState = asObject(ctx.getAttribute("__lcod_state__"));
        if (!attributeState.isEmpty()) {
            return deepClone(attributeState);
        }
        Map<String, Object> state = asObject(rawInput);
        if (!state.isEmpty()) {
            return deepClone(state);
        }
        Map<String, Object> fallback = new LinkedHashMap<>();
        for (var entry : payload.entrySet()) {
            String key = entry.getKey();
            if (List.of("source", "language", "timeoutMs", "tools", "imports", "bindings", "config", "meta", "streams", "input").contains(key)) {
                continue;
            }
            fallback.put(key, deepClone(entry.getValue()));
        }
        return fallback;
    }

    private static long readTimeout(Object raw) {
        if (raw instanceof Number number) {
            return Math.max(1, number.longValue());
        }
        if (raw instanceof String str && !str.isBlank()) {
            try {
                return Math.max(1, Long.parseLong(str.trim()));
            } catch (NumberFormatException ignored) {
                return 1000L;
            }
        }
        return 1000L;
    }

    private static Map<String, Object> resolveBindings(Map<String, Object> state, Map<String, Object> bindings) {
        Map<String, Object> resolved = new LinkedHashMap<>();
        for (var entry : bindings.entrySet()) {
            String name = entry.getKey();
            Object descriptor = entry.getValue();
            if (!(descriptor instanceof Map<?, ?> descMap)) continue;
            if (descMap.containsKey("value")) {
                resolved.put(name, deepCloneScalar(descMap.get("value")));
                continue;
            }
            Object path = descMap.get("path");
            if (path instanceof String str && !str.isBlank()) {
                Object value = resolvePath(state, str);
                if (value == null && descMap.containsKey("default")) {
                    resolved.put(name, deepCloneScalar(descMap.get("default")));
                } else {
                    resolved.put(name, deepCloneScalar(value));
                }
            }
        }
        return resolved;
    }

    private static Object resolvePath(Object root, String path) {
        if (root == null || path == null || path.isBlank()) return null;
        String normalized = path.startsWith("$") ? path.substring(1) : path;
        if (normalized.startsWith(".")) normalized = normalized.substring(1);
        if (normalized.isEmpty()) return root;
        String[] parts = normalized.split("\\.");
        Object cursor = root;
        for (String part : parts) {
            if (!(cursor instanceof Map<?, ?> map)) {
                return null;
            }
            cursor = map.get(part);
            if (cursor == null) return null;
        }
        return cursor;
    }

    private static ImportsRegistry buildImports(ExecutionContext ctx, Object rawImports, StepMeta meta) {
        if (!(rawImports instanceof Map<?, ?> map)) {
            return ImportsRegistry.empty();
        }
        Map<String, ImportFunction> entries = new LinkedHashMap<>();
        for (var entry : map.entrySet()) {
            String alias = optionalString(entry.getKey());
            String target = optionalString(entry.getValue());
            if (alias == null || target == null) continue;
            entries.put(alias, payload -> ctx.call(target, asObject(payload), meta));
        }
        return new ImportsRegistry(entries);
    }

    private static ToolsRegistry compileTools(Context context, Object rawTools) {
        if (!(rawTools instanceof List<?> list)) {
            return ToolsRegistry.empty(context);
        }
        Map<String, Tool> compiled = new LinkedHashMap<>();
        for (Object entry : list) {
            if (!(entry instanceof Map<?, ?> map)) continue;
            String name = optionalString(map.get("name"));
            String source = optionalString(map.get("source"));
            if (name == null || source == null) continue;
            Value fn = compileFunction(context, source);
            compiled.put(name, new Tool(fn));
        }
        return new ToolsRegistry(context, compiled);
    }

    private static Value compileFunction(Context context, String source) {
        String trimmed = source.trim();
        String normalized = trimmed.endsWith(";") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
        String wrapped = "(function(scope, api){ const fn = (" + normalized + "); return fn(scope, api); })";
        return context.eval("js", wrapped);
    }

    private static Object awaitValue(Context context, Value value) throws ExecutionException, InterruptedException {
        if (value != null && value.canInvokeMember("then")) {
            CompletableFuture<Object> future = new CompletableFuture<>();
            ProxyExecutable resolve = args -> {
                future.complete(valueToJava(args.length > 0 ? args[0] : context.eval("js", "undefined")));
                return null;
            };
            ProxyExecutable reject = args -> {
                Value raw = args.length > 0 ? args[0] : null;
                Object reason = raw == null ? "Promise rejected" : valueToJava(raw);
                String message;
                try {
                    if (reason instanceof Map || reason instanceof List) {
                        message = JSON.writeValueAsString(reason);
                    } else {
                        message = String.valueOf(reason);
                    }
                } catch (Exception jsonEx) {
                    message = String.valueOf(reason);
                }
                String rawDebug = raw == null ? "null" : raw.toString();
                System.err.println("tooling/script promise rejected: " + message + " (raw=" + rawDebug + ")");
                future.completeExceptionally(new RuntimeException(message));
                return null;
            };
            value.invokeMember("then", resolve, reject);
            return future.get();
        }
        return valueToJava(value);
    }

    private static Object valueToJava(Value value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isNumber()) {
            if (value.fitsInInt()) return value.asInt();
            if (value.fitsInLong()) return value.asLong();
            return value.asDouble();
        }
        if (value.isString()) {
            return value.asString();
        }
        if (value.isHostObject()) {
            Object host = value.asHostObject();
            if (host instanceof Map<?, ?> map) {
                Map<String, Object> copy = new LinkedHashMap<>();
                map.forEach((k, v) -> copy.put(String.valueOf(k), v));
                return copy;
            }
            if (host instanceof List<?> list) {
                return new ArrayList<>(list);
            }
            return host;
        }
        if (value.hasArrayElements()) {
            List<Object> list = new ArrayList<>();
            long size = value.getArraySize();
            for (long i = 0; i < size; i++) {
                list.add(valueToJava(value.getArrayElement(i)));
            }
            return list;
        }
        if (value.hasMembers()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (String key : value.getMemberKeys()) {
                map.put(key, valueToJava(value.getMember(key)));
            }
            return map;
        }
        return value.toString();
    }

    private static Value toJsValue(Context context, Object value) {
        try {
            String serialized = JSON.writeValueAsString(value);
            return context.eval("js", "JSON").getMember("parse").execute(serialized);
        } catch (Exception ex) {
            return context.asValue(value);
        }
    }

    private static void registerStreams(ExecutionContext ctx, Map<String, Object> state, Object rawStreams) {
        if (!(rawStreams instanceof List<?> specs)) {
            return;
        }
        for (Object spec : specs) {
            if (!(spec instanceof Map<?, ?> map)) continue;
            String target = optionalString(map.get("target"));
            Object chunks = map.get("chunks");
            if (target == null || !(chunks instanceof List<?> list)) continue;
            List<byte[]> decoded = new ArrayList<>();
            String encoding = optionalString(map.get("encoding"));
            for (Object chunk : list) {
                decoded.add(decodeChunk(String.valueOf(chunk), encoding));
            }
            setDeepValue(state, target, decoded);
        }
    }

    private static byte[] decodeChunk(String chunk, String encoding) {
        if (encoding == null || encoding.equalsIgnoreCase("utf-8")) {
            return chunk.getBytes(StandardCharsets.UTF_8);
        }
        if (encoding.equalsIgnoreCase("base64")) {
            return Base64.getDecoder().decode(chunk);
        }
        if (encoding.equalsIgnoreCase("hex")) {
            int len = chunk.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) ((Character.digit(chunk.charAt(i), 16) << 4) + Character.digit(chunk.charAt(i + 1), 16));
            }
            return data;
        }
        return chunk.getBytes(StandardCharsets.UTF_8);
    }

    private static void setDeepValue(Map<String, Object> target, String path, Object value) {
        if (target == null || path == null || path.isBlank()) return;
        String normalized = path.startsWith("$") ? path.substring(1) : path;
        if (normalized.startsWith(".")) normalized = normalized.substring(1);
        String[] parts = normalized.split("\\.");
        Map<String, Object> cursor = target;
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            Object next = cursor.get(part);
            if (!(next instanceof Map)) {
                next = new LinkedHashMap<String, Object>();
                cursor.put(part, next);
            }
            cursor = (Map<String, Object>) next;
        }
        cursor.put(parts[parts.length - 1], value);
    }

    private static Map<String, Object> asObject(Object raw) {
        if (raw == null) {
            return Map.of();
        }
        if (raw instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), v));
            return copy;
        }
        if (raw instanceof Value value) {
            Object converted = valueToJava(value);
            if (converted instanceof Map<?, ?> convertedMap) {
                Map<String, Object> copy = new LinkedHashMap<>();
                convertedMap.forEach((k, v) -> copy.put(String.valueOf(k), v));
                return copy;
            }
        }
        throw new IllegalArgumentException("Expected object, got " + raw.getClass().getSimpleName());
    }

    private static Map<String, Object> deepClone(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), deepCloneScalar(v)));
            return copy;
        }
        if (value instanceof Value hostValue) {
            Object converted = valueToJava(hostValue);
            if (converted instanceof Map<?, ?> convertedMap) {
                return deepClone(convertedMap);
            }
        }
        return Map.of();
    }

    private static Object deepCloneScalar(Object value) {
        if (value == null) return null;
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), deepCloneScalar(v)));
            return copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(deepCloneScalar(item));
            }
            return copy;
        }
        if (value instanceof Value hostValue) {
            return deepCloneScalar(valueToJava(hostValue));
        }
        return value;
    }

    private static String optionalString(Object value) {
        if (value == null) return null;
        String str = value instanceof String ? (String) value : String.valueOf(value);
        return str.isBlank() ? null : str;
    }



    private static final class MapBuilder {
        private MapBuilder() {}

        static Value buildScope(Context context, Map<String, Object> bindings, Map<String, Object> state, Map<String, Object> meta, Value imports) {
            Value scope = context.eval("js", "({})");
            scope.putMember("bindings", toJsValue(context, bindings));
            scope.putMember("input", toJsValue(context, bindings));
            scope.putMember("state", toJsValue(context, state));
            scope.putMember("meta", toJsValue(context, meta));
            scope.putMember("imports", imports);
            return scope;
        }
    }

    private interface ImportFunction {
        Object invoke(Object payload) throws Exception;
    }

    private static final class ImportsRegistry {
        private final Map<String, ImportFunction> imports;

        ImportsRegistry(Map<String, ImportFunction> imports) {
            this.imports = imports;
        }

        static ImportsRegistry empty() {
            return new ImportsRegistry(Map.of());
        }

        Map<String, ImportFunction> view() {
            return imports;
        }
    }

    private static final class ToolsRegistry {
        private final Context context;
        private final Map<String, Tool> tools;

        ToolsRegistry(Context context, Map<String, Tool> tools) {
            this.context = context;
            this.tools = tools;
        }

        static ToolsRegistry empty(Context context) {
            return new ToolsRegistry(context, Map.of());
        }

        Object run(String name, Object payload, ApiBridge api) throws Exception {
            Tool tool = tools.get(name);
            if (tool == null) {
                throw new IllegalArgumentException("Unknown tool: " + name);
            }
            Value result = tool.function.execute(context.asValue(payload), context.asValue(api));
            return awaitValue(context, result);
        }
    }

    private record Tool(Value function) {}

    public static final class ApiBridge {
        private final ExecutionContext ctx;
        private final Context jsContext;
        private final Map<String, Object> config;
        private final List<String> messages;
        private final ToolsRegistry tools;
        @HostAccess.Export
        public final Value imports;
        private final StepMeta meta;

        ApiBridge(ExecutionContext ctx, Context jsContext, Map<String, Object> config, List<String> messages,
                  ToolsRegistry tools, ImportsRegistry imports, Value importsObject, StepMeta meta) {
            this.ctx = ctx;
            this.jsContext = jsContext;
            this.config = config;
            this.messages = messages;
            this.tools = tools;
            this.imports = importsObject;
            this.meta = meta;
        }

        @HostAccess.Export
        public Object call(String id) throws Exception {
            Object result = ctx.call(id, Map.of(), meta);
            return convertToJsValue(result);
        }

        @HostAccess.Export
        public Object call(String id, Object payload) throws Exception {
            Object result = ctx.call(id, toMap(payload), meta);
            return convertToJsValue(result);
        }

        @HostAccess.Export
        public Object runSlot(String name, Object state) throws Exception {
            return runSlot(name, state, Map.of());
        }

        @HostAccess.Export
        public Object runSlot(String name, Object state, Object slotVars) throws Exception {
            Map<String, Object> localState = toMap(state);
            Map<String, Object> vars = toMap(slotVars);
            Object result = ctx.runSlot(name, localState, vars);
            return convertToJsValue(result);
        }

        @HostAccess.Export
        public void log(Object value) {
            messages.add(String.valueOf(value));
        }

        @HostAccess.Export
        public void log(Object a, Object b) {
            messages.add(String.valueOf(a) + " " + String.valueOf(b));
        }

        @HostAccess.Export
        public Map<String, Object> config() {
            return deepClone(config);
        }

        @HostAccess.Export
        public Object config(String path) {
            Object resolved = resolvePath(config, path);
            return deepCloneScalar(resolved);
        }

        @HostAccess.Export
        public Object config(String path, Object fallback) {
            Object resolved = resolvePath(config, path);
            return resolved == null ? deepCloneScalar(fallback) : deepCloneScalar(resolved);
        }

        @HostAccess.Export
        public Object run(String name, Object payload) throws Exception {
            return tools.run(name, deepCloneScalar(payload), this);
        }

        @HostAccess.Export
        public Object run(String name, Object payload, Object options) throws Exception {
            return tools.run(name, deepCloneScalar(payload), this);
        }

        @HostAccess.Export
        public Value getImports() {
            return imports;
        }

        private Object convertToJsValue(Object value) {
            if (value == null) {
                return null;
            }
            return toJsValue(jsContext, value);
        }

        private Map<String, Object> toMap(Object value) {
            if (value == null) {
                return Map.of();
            }
            if (value instanceof Map<?, ?> map) {
                Map<String, Object> copy = new LinkedHashMap<>();
                map.forEach((k, v) -> copy.put(String.valueOf(k), v));
                return copy;
            }
            if (value instanceof Value val) {
                Object converted = valueToJava(val);
                if (converted instanceof Map<?, ?> map) {
                    Map<String, Object> copy = new LinkedHashMap<>();
                    map.forEach((k, v) -> copy.put(String.valueOf(k), v));
                    return copy;
                }
            }
            throw new IllegalArgumentException("Expected object, got " + value.getClass().getSimpleName());
        }
    }
}
