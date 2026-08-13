package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DownlinkMirrorTest {

    @TempDir
    Path tmp;

    /** Skips the test on platforms where symlink creation is unavailable. */
    private static void createSymlinkOrSkip(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (IOException | UnsupportedOperationException e) {
            Assumptions.assumeTrue(false, "symlinks not supported: " + e);
        }
    }

    @Test
    public void writesFileInsideMirrorRoot() throws Exception {
        Path root = tmp.resolve("mirror");
        DownlinkMirror.write(root, "sub/dir/file.bin", new byte[] { 1, 2, 3 });
        assertArrayEquals(new byte[] { 1, 2, 3 },
                Files.readAllBytes(root.resolve("sub/dir/file.bin")));
    }

    @Test
    public void nullMirrorDirIsNoop() throws Exception {
        DownlinkMirror.write(null, "file.bin", new byte[] { 1 });
        // A disabled mirror must write nothing anywhere under the test root.
        try (var walk = Files.walk(tmp)) {
            assertTrue(walk.allMatch(Files::isDirectory));
        }
    }

    @Test
    public void refusesLexicalEscape() throws Exception {
        Path root = tmp.resolve("mirror");
        DownlinkMirror.write(root, "../escaped.bin", new byte[] { 1 });
        assertFalse(Files.exists(tmp.resolve("escaped.bin")));
    }

    @Test
    public void refusesSymlinkedDirectoryEscape() throws Exception {
        Path root = tmp.resolve("mirror");
        Path outside = Files.createDirectories(tmp.resolve("outside"));
        Files.createDirectories(root);
        createSymlinkOrSkip(root.resolve("link"), outside);

        DownlinkMirror.write(root, "link/escaped.bin", new byte[] { 1 });

        assertFalse(Files.exists(outside.resolve("escaped.bin")),
                "write must not follow a symlink out of the mirror root");
    }

    @Test
    public void refusesSymlinkedFileTarget() throws Exception {
        Path root = Files.createDirectories(tmp.resolve("mirror"));
        Path outsideFile = Files.createFile(tmp.resolve("victim.bin"));
        createSymlinkOrSkip(root.resolve("file.bin"), outsideFile);

        DownlinkMirror.write(root, "file.bin", new byte[] { 9 });

        assertTrue(Files.readAllBytes(outsideFile).length == 0,
                "write must not follow a symlinked file out of the mirror root");
    }
}
