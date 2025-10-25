package work.lcod.kernel.tooling;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import work.lcod.kernel.runtime.ComposeLoader;
import work.lcod.kernel.runtime.ComposeRunner;
import work.lcod.kernel.runtime.ExecutionContext;
import work.lcod.kernel.runtime.Registry;
import work.lcod.kernel.spec.SpecPaths;

public final class SpecComponentRegistry {
    private SpecComponentRegistry() {}

    private record ComponentDef(String id, String relativePath) {}

    private static final List<ComponentDef> DEFINITIONS = List.of(
        new ComponentDef("lcod://tooling/value/default_object@0.1.0", "tooling/value/default_object/compose.yaml"),
        new ComponentDef("lcod://tooling/value/default_array@0.1.0", "tooling/value/default_array/compose.yaml"),
        new ComponentDef("lcod://tooling/value/is_object@0.1.0", "tooling/value/is_object/compose.yaml"),
        new ComponentDef("lcod://tooling/value/is_array@0.1.0", "tooling/value/is_array/compose.yaml"),
        new ComponentDef("lcod://tooling/value/is_string_nonempty@0.1.0", "tooling/value/is_string_nonempty/compose.yaml"),
        new ComponentDef("lcod://tooling/array/append@0.1.0", "tooling/array/append/compose.yaml"),
        new ComponentDef("lcod://tooling/array/compact@0.1.0", "tooling/array/compact/compose.yaml"),
        new ComponentDef("lcod://tooling/array/concat@0.1.0", "tooling/array/concat/compose.yaml"),
        new ComponentDef("lcod://tooling/array/filter_objects@0.1.0", "tooling/array/filter_objects/compose.yaml"),
        new ComponentDef("lcod://tooling/array/length@0.1.0", "tooling/array/length/compose.yaml"),
        new ComponentDef("lcod://tooling/array/shift@0.1.0", "tooling/array/shift/compose.yaml"),
        new ComponentDef("lcod://tooling/fs/read_optional@0.1.0", "tooling/fs/read_optional/compose.yaml"),
        new ComponentDef("lcod://tooling/json/decode_object@0.1.0", "tooling/json/decode_object/compose.yaml"),
        new ComponentDef("lcod://tooling/hash/sha256_base64@0.1.0", "tooling/hash/sha256_base64/compose.yaml"),
        new ComponentDef("lcod://tooling/hash/to_key@0.1.0", "tooling/hash/to_key/compose.yaml"),
        new ComponentDef("lcod://tooling/component/build_artifacts@0.1.0", "tooling/component/build_artifacts/compose.yaml"),
        new ComponentDef("lcod://tooling/path/join_chain@0.1.0", "tooling/path/join_chain/compose.yaml"),
        new ComponentDef("lcod://tooling/path/dirname@0.1.0", "tooling/path/dirname/compose.yaml"),
        new ComponentDef("lcod://tooling/path/is_absolute@0.1.0", "tooling/path/is_absolute/compose.yaml"),
        new ComponentDef("lcod://tooling/path/to_file_url@0.1.0", "tooling/path/to_file_url/compose.yaml"),
        new ComponentDef("lcod://core/array/append@0.1.0", "core/array/append/compose.yaml"),
        new ComponentDef("lcod://core/json/decode@0.1.0", "core/json/decode/compose.yaml"),
        new ComponentDef("lcod://core/json/encode@0.1.0", "core/json/encode/compose.yaml"),
        new ComponentDef("lcod://core/object/merge@0.1.0", "core/object/merge/compose.yaml"),
        new ComponentDef("lcod://core/string/format@0.1.0", "core/string/format/compose.yaml"),
        new ComponentDef("lcod://tooling/registry/source/load@0.1.0", "tooling/registry/source/compose.yaml"),
        new ComponentDef("lcod://tooling/registry/index@0.1.0", "tooling/registry/index/compose.yaml"),
        new ComponentDef("lcod://tooling/registry/select@0.1.0", "tooling/registry/select/compose.yaml"),
        new ComponentDef("lcod://tooling/registry/resolution@0.1.0", "tooling/registry/resolution/compose.yaml"),
        new ComponentDef("lcod://tooling/registry/catalog/generate@0.1.0", "tooling/registry/catalog/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/build_inline_entry@0.1.0", "tooling/registry_sources/build_inline_entry/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/collect_entries@0.1.0", "tooling/registry_sources/collect_entries/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/collect_queue@0.1.0", "tooling/registry_sources/collect_queue/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/load_config@0.1.0", "tooling/registry_sources/load_config/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/merge_inline_entries@0.1.0", "tooling/registry_sources/merge_inline_entries/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/normalize_pointer@0.1.0", "tooling/registry_sources/normalize_pointer/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/partition_normalized@0.1.0", "tooling/registry_sources/partition_normalized/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/prepare_env@0.1.0", "tooling/registry_sources/prepare_env/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/process_catalogue@0.1.0", "tooling/registry_sources/process_catalogue/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/process_pointer@0.1.0", "tooling/registry_sources/process_pointer/compose.yaml"),
        new ComponentDef("lcod://tooling/registry_sources/resolve@0.1.0", "tooling/registry_sources/resolve/compose.yaml"),
        new ComponentDef("lcod://tooling/resolver/context/prepare@0.1.0", "tooling/resolver/context/compose.yaml"),
        new ComponentDef("lcod://tooling/resolver/replace/apply@0.1.0", "tooling/resolver/replace/compose.yaml"),
        new ComponentDef("lcod://tooling/resolver/warnings/merge@0.1.0", "tooling/resolver/warnings/compose.yaml"),
        new ComponentDef("lcod://tooling/resolver/internal/load-sources@0.1.0", "packages/resolver/components/internal/load_sources/compose.yaml")
    );

    private static final Map<Path, List<Map<String, Object>>> COMPOSE_CACHE = new ConcurrentHashMap<>();

    public static void register(Registry registry) {
        var specRoot = SpecPaths.locateSpecRoot().orElse(null);
        if (specRoot == null) {
            return;
        }
        for (ComponentDef def : DEFINITIONS) {
            if (registry.get(def.id()) != null) {
                continue;
            }
            Path composePath = specRoot.resolve(def.relativePath()).normalize();
            if (!Files.isRegularFile(composePath)) {
                continue;
            }
            registry.register(def.id(), (ctx, input, meta) -> runSpecCompose(ctx, composePath, input, specRoot));
        }
    }

    private static Object runSpecCompose(ExecutionContext ctx, Path composePath, Map<String, Object> input, Path specRoot) {
        List<Map<String, Object>> steps = COMPOSE_CACHE.computeIfAbsent(composePath, ComposeLoader::loadFromLocalFile);
        Map<String, Object> initial = input == null ? new LinkedHashMap<>() : new LinkedHashMap<>(input);
        initial.putIfAbsent("specRoot", specRoot.toString());
        try {
            return ComposeRunner.runSteps(ctx, steps, initial, Map.of());
        } catch (Exception ex) {
            String reason = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
            throw new IllegalStateException("Failed to execute spec component from " + composePath + ": " + reason, ex);
        }
    }
}
