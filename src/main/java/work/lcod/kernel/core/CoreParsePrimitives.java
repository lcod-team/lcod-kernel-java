package work.lcod.kernel.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.tomlj.Toml;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.runtime.StepMeta;

/**
 * Parsing helpers (JSON/TOML/CSV) used by the spec fixtures.
 */
public final class CoreParsePrimitives {
    private static final ObjectMapper JSON = new ObjectMapper();

    private CoreParsePrimitives() {}

    public static Registry register(Registry registry) {
        registry.register("lcod://contract/core/parse/json@1", CoreParsePrimitives::parseJson);
        registry.register("lcod://contract/core/parse/toml@1", CoreParsePrimitives::parseToml);
        registry.register("lcod://contract/core/parse/csv@1", CoreParsePrimitives::parseCsv);
        return registry;
    }

    private static Object parseJson(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String text = resolveInputText(ctx, input);
        try {
            Object value = JSON.readValue(text, Object.class);
            return Map.of(
                "value", value,
                "bytes", text.getBytes(StandardCharsets.UTF_8).length,
                "validated", false
            );
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("json parse error: " + ex.getOriginalMessage(), ex);
        }
    }

    private static Object parseToml(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String text = resolveInputText(ctx, input);
        TomlParseResult result = Toml.parse(text);
        if (result.hasErrors()) {
            throw new RuntimeException("toml parse error: " + result.errors().get(0).toString());
        }
        return Map.of("value", convertTomlTable(result));
    }

    private static Object parseCsv(ExecutionContext ctx, Map<String, Object> input, StepMeta meta) {
        String text = resolveInputText(ctx, input);
        CSVFormat format = (Boolean.TRUE.equals(input.get("header")) ? CSVFormat.DEFAULT.withFirstRecordAsHeader() : CSVFormat.DEFAULT)
            .withDelimiter(resolveDelimiter(input.get("delimiter")))
            .withTrim(Boolean.TRUE.equals(input.get("trim")));
        try (CSVParser parser = new CSVParser(new StringReader(text), format)) {
            List<String> headers = parser.getHeaderNames();
            boolean hasHeader = headers != null && !headers.isEmpty();
            List<Map<String, String>> rows = new java.util.ArrayList<>();
            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                if (hasHeader) {
                    for (String header : headers) {
                        row.put(header, record.get(header));
                    }
                } else {
                    for (int i = 0; i < record.size(); i++) {
                        row.put(String.valueOf(i), record.get(i));
                    }
                }
                rows.add(row);
            }
            return Map.of("rows", rows);
        } catch (IOException ex) {
            throw new RuntimeException("csv parse error: " + ex.getMessage(), ex);
        }
    }

    private static char resolveDelimiter(Object raw) {
        if (raw instanceof String str && !str.isBlank()) {
            return str.charAt(0);
        }
        return ',';
    }

    private static String resolveInputText(ExecutionContext ctx, Map<String, Object> input) {
        if (input == null) {
            return "";
        }
        if (input.get("text") != null) {
            return String.valueOf(input.get("text"));
        }
        if (input.get("path") != null) {
            Path path = ctx.workingDirectory().resolve(String.valueOf(input.get("path"))).normalize();
            try {
                return Files.readString(path);
            } catch (IOException ex) {
                throw new RuntimeException("Unable to read path: " + path, ex);
            }
        }
        throw new IllegalArgumentException("text or path is required");
    }

    private static Map<String, Object> convertTomlTable(TomlTable table) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (String key : table.keySet()) {
            map.put(key, convertTomlValue(table.get(key)));
        }
        return map;
    }

    private static Object convertTomlValue(Object value) {
        if (value instanceof TomlTable table) {
            return convertTomlTable(table);
        }
        if (value instanceof TomlArray array) {
            List<Object> list = new java.util.ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                list.add(convertTomlValue(array.get(i)));
            }
            return list;
        }
        return value;
    }
}
