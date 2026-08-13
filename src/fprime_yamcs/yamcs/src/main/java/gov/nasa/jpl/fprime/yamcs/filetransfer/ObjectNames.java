package gov.nasa.jpl.fprime.yamcs.filetransfer;

/**
 * Bucket object-name utilities shared by the downlink handlers.
 */
final class ObjectNames {

    private ObjectNames() {
    }

    /**
     * Turn a wire-supplied destination path into a bucket object key:
     * strip the leading '/', and reject empty or '..'-bearing paths so a
     * corrupt or malicious transfer cannot address objects outside the
     * bucket namespace (or, via a local mirror, outside the mirror
     * directory).
     *
     * @throws IllegalArgumentException if the path is empty, contains an
     *         empty, '.' or '..' segment, or contains backslashes or
     *         control characters (which could bypass the segment checks on
     *         Windows-hosted or filesystem-backed storage)
     */
    static String sanitize(String destinationPath) {
        String name = destinationPath.startsWith("/")
                ? destinationPath.substring(1)
                : destinationPath;
        if (name.isEmpty()) {
            throw new IllegalArgumentException("empty destination path");
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '\\' || c < 0x20 || c == 0x7F) {
                throw new IllegalArgumentException(
                        "path contains backslash or control character: " + forLog(destinationPath));
            }
        }
        for (String segment : name.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException(
                        "path contains empty, '.' or '..' segment: " + forLog(destinationPath));
            }
        }
        return name;
    }

    /**
     * Replace control characters in a wire-supplied string with '?' so
     * embedded CR/LF cannot forge log records when the value is logged.
     */
    static String forLog(String wireValue) {
        if (wireValue == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(wireValue.length());
        for (int i = 0; i < wireValue.length(); i++) {
            char c = wireValue.charAt(i);
            sb.append((c < 0x20 || c == 0x7F) ? '?' : c);
        }
        return sb.toString();
    }
}
