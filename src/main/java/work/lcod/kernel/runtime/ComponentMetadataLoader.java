package work.lcod.kernel.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

/**
 * Utility helpers to derive {@link ComponentMetadata} instances from lcp.toml manifests.
 */
public final class ComponentMetadataLoader {
    private ComponentMetadataLoader() {}

    public static Optional<ComponentMetadata> load(Path manifestPath) {
        if (manifestPath == null || !Files.isRegularFile(manifestPath)) {
            return Optional.empty();
        }
        try {
            TomlParseResult result = Toml.parse(Files.readString(manifestPath));
            if (result == null || result.hasErrors()) {
                return Optional.empty();
            }
            return fromToml(result);
        } catch (IOException ex) {
            return Optional.empty();
        }
    }

    public static Optional<ComponentMetadata> fromToml(TomlParseResult result) {
        if (result == null) {
            return Optional.empty();
        }
        List<String> inputs = readKeys(result.getTable("inputs"));
        List<String> outputs = readKeys(result.getTable("outputs"));
        List<String> slots = readKeys(result.getTable("slots"));
        if (inputs.isEmpty() && outputs.isEmpty() && slots.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ComponentMetadata(inputs, outputs, slots));
    }

    private static List<String> readKeys(TomlTable table) {
        if (table == null || table.isEmpty()) {
            return Collections.emptyList();
        }
        return table.keySet().stream().map(Object::toString).collect(Collectors.toList());
    }
}
