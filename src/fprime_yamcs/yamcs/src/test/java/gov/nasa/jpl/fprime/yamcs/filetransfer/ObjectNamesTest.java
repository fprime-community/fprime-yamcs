package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ObjectNamesTest {

    @Test
    public void sanitizeStripsLeadingSlash() {
        assertEquals("a/b.bin", ObjectNames.sanitize("/a/b.bin"));
        assertEquals("b.bin", ObjectNames.sanitize("b.bin"));
    }

    @Test
    public void sanitizeRejectsUnsafePaths() {
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize(""));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("/"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("/a/../b"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("../b"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("a//b"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("./b"));
    }

    @Test
    public void sanitizeRejectsBackslashesAndControlCharacters() {
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("a\\b"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("..\\evil"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("a\u0000b"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("a\nb"));
        assertThrows(IllegalArgumentException.class, () -> ObjectNames.sanitize("a\u007Fb"));
    }

    @Test
    public void forLogMasksControlCharacters() {
        assertEquals("a?b?c", ObjectNames.forLog("a\nb\rc"));
        assertEquals("plain", ObjectNames.forLog("plain"));
        assertEquals("?", ObjectNames.forLog("\u0000"));
        assertNull(ObjectNames.forLog(null));
    }
}
