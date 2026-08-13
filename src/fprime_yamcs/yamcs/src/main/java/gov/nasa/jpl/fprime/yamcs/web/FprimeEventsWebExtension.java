package gov.nasa.jpl.fprime.yamcs.web;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Map;

import org.yamcs.Plugin;
import org.yamcs.PluginException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.web.WebPlugin;

/**
 * Registers the "F Prime Events" web extension with the Yamcs web interface.
 *
 * The extension provides a GDS-style event display: whole-row severity
 * coloring and filtering on event ID, name, and F Prime severity. Static web
 * assets are packaged on the classpath and extracted to a temporary directory
 * at load time, as required by {@link WebPlugin#addExtension}.
 */
public class FprimeEventsWebExtension implements Plugin {

    /** Extension id: also the custom element mounted at /ext/&lt;id&gt; */
    public static final String EXTENSION_ID = "fprime-events";

    private static final String RESOURCE_ROOT = "/fprime-events-web/";
    private static final String[] RESOURCES = { EXTENSION_ID + ".js" };

    @Override
    public void onLoad(YConfiguration config) throws PluginException {
        var webPlugin = YamcsServer.getServer().getPluginManager().getPlugin(WebPlugin.class);
        if (webPlugin == null) {
            throw new PluginException("The yamcs-web plugin is required by the F Prime Events extension");
        }
        try {
            var staticRoot = extractStaticResources();
            webPlugin.addExtension(EXTENSION_ID, Map.of(), staticRoot);
        } catch (IOException e) {
            throw new PluginException("Could not deploy F Prime Events web extension", e);
        }
    }

    private Path extractStaticResources() throws IOException {
        var staticRoot = Files.createTempDirectory("fprime-events-web");
        staticRoot.toFile().deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteRecursively(staticRoot)));
        for (var resource : RESOURCES) {
            try (var in = getClass().getResourceAsStream(RESOURCE_ROOT + resource)) {
                if (in == null) {
                    throw new IOException("Missing classpath resource " + RESOURCE_ROOT + resource);
                }
                Files.copy(in, staticRoot.resolve(resource), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return staticRoot;
    }

    private static void deleteRecursively(Path root) {
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    // Best-effort cleanup of a temp directory
                }
            });
        } catch (IOException e) {
            // Best-effort cleanup of a temp directory
        }
    }
}
