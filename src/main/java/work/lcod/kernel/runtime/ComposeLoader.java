package work.lcod.kernel.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads compose definitions (local path or HTTP URL) into in-memory step maps.
 */
public final class ComposeLoader {
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private ComposeLoader() {}

    public static List<Map<String, Object>> loadFromLocalFile(Path path) {
        try (var in = Files.newInputStream(path)) {
            return parseCompose(in);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to read compose: " + path, ex);
        }
    }

    public static List<Map<String, Object>> loadFromHttp(URI uri) {
        try {
            var client = HttpClient.newHttpClient();
            var request = HttpRequest.newBuilder(uri).GET().build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() >= 400) {
                throw new IllegalStateException("HTTP " + response.statusCode() + " while downloading compose: " + uri);
            }
            try (var body = response.body()) {
                return parseCompose(body);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while downloading compose: " + uri, ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to download compose: " + uri, ex);
        }
    }

    private static List<Map<String, Object>> parseCompose(InputStream in) throws IOException {
        var root = YAML_MAPPER.readTree(in);
        if (root == null || !root.hasNonNull("compose")) {
            return List.of();
        }
        var composeNode = root.get("compose");
        if (!composeNode.isArray()) {
            return List.of();
        }
        var steps = new ArrayList<Map<String, Object>>();
        for (var stepNode : composeNode) {
            steps.add(toMap(stepNode));
        }
        return steps;
    }

    private static Map<String, Object> toMap(JsonNode node) throws IOException {
        if (node == null || node.isNull()) {
            return Map.of();
        }
        if (!node.isObject()) {
            throw new IOException("Compose step must be an object: " + node);
        }
        var map = new LinkedHashMap<String, Object>();
        var fields = node.fields();
        while (fields.hasNext()) {
            var entry = fields.next();
            map.put(entry.getKey(), convertNode(entry.getValue()));
        }
        return map;
    }

    private static Object convertNode(JsonNode node) throws IOException {
        if (node.isObject()) {
            var map = new LinkedHashMap<String, Object>();
            var fields = node.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                map.put(entry.getKey(), convertNode(entry.getValue()));
            }
            return map;
        }
        if (node.isArray()) {
            var list = new ArrayList<Object>();
            for (var item : node) {
                list.add(convertNode(item));
            }
            return list;
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isNull()) {
            return null;
        }
        return node.asText();
    }
}
