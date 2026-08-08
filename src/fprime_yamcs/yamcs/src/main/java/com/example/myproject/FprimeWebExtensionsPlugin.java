package com.example.myproject;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.yamcs.Plugin;
import org.yamcs.PluginException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.logging.Log;
import org.yamcs.web.WebPlugin;

/**
 * Registers project-supplied yamcs-web extensions.
 *
 * Extension directories are passed through the "fprime.yamcs.webExtensions"
 * system property (comma-separated paths). Every .js file in a directory is
 * injected as a module script into the yamcs-web index page; other files in
 * the directory are served alongside the webapp static files.
 */
public class FprimeWebExtensionsPlugin implements Plugin {
    private static final Log log = new Log(FprimeWebExtensionsPlugin.class);

    public static final String SYSTEM_PROPERTY = "fprime.yamcs.webExtensions";

    @Override
    public void onLoad(YConfiguration config) throws PluginException {
        String property = System.getProperty(SYSTEM_PROPERTY, "").trim();
        if (property.isEmpty()) {
            return;
        }
        WebPlugin web = YamcsServer.getServer().getPluginManager().getPlugin(WebPlugin.class);
        if (web == null) {
            throw new PluginException("Cannot register web extensions: yamcs-web plugin not available");
        }
        for (String element : property.split(",")) {
            String trimmed = element.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            Path root = Path.of(trimmed).toAbsolutePath().normalize();
            if (!Files.isDirectory(root)) {
                throw new PluginException("Web extension directory not found: " + root);
            }
            String id = root.getFileName().toString();
            web.addExtension(id, Collections.emptyMap(), root);
            log.info("Registered yamcs-web extension '{}' from {}", id, root);
        }
    }
}
