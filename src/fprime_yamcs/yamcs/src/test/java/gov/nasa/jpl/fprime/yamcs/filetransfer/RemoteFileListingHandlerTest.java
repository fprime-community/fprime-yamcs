package gov.nasa.jpl.fprime.yamcs.filetransfer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.yamcs.filetransfer.InvalidRequestException;
import org.yamcs.filetransfer.RemoteFileListMonitor;
import org.yamcs.protobuf.ListFilesResponse;
import org.yamcs.protobuf.RemoteFile;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.protobuf.Db.Event;

public class RemoteFileListingHandlerTest {

    private final RemoteFileListingHandler handler = new RemoteFileListingHandler("fprime", Runnable::run);

    private static Tuple eventTuple(Event evt) {
        TupleDefinition tdef = new TupleDefinition();
        tdef.addColumn("body", DataType.protobuf(Event.class.getName()));
        return new Tuple(tdef, List.of(evt));
    }

    private void feed(String type, String message) {
        Event evt = Event.newBuilder()
                .setSource("test")
                .setSeqNumber(0)
                .setGenerationTime(0)
                .setReceptionTime(0)
                .setType(type)
                .setMessage(message)
                .build();
        handler.onTuple(null, eventTuple(evt));
    }

    private void feedStructured(String type, Map<String, String> extra) {
        Event evt = Event.newBuilder()
                .setSource("test")
                .setSeqNumber(0)
                .setGenerationTime(0)
                .setReceptionTime(0)
                .setType(type)
                .setMessage("")
                .putAllExtra(extra)
                .build();
        handler.onTuple(null, eventTuple(evt));
    }

    @Test
    public void inProgressListingCountIsBounded() {
        for (int i = 0; i < RemoteFileListingHandler.MAX_CACHED_LISTINGS; i++) {
            handler.beginListing("/dir" + i);
        }
        assertThrows(InvalidRequestException.class,
                () -> handler.beginListing("/overflow"));
        // Refreshing an already-in-progress path is still allowed at the cap.
        handler.beginListing("/dir0");
    }

    @Test
    public void regexListingAccumulatesAndCompletes() {
        handler.beginListing("/data");
        feed("DirectoryListing", "[DirectoryListing] Directory /data: a.bin (100 bytes)");
        feed("DirectoryListingSubdir", "[DirectoryListingSubdir] Directory /data: sub");
        feed("ListDirectorySucceeded", "[ListDirectorySucceeded] Directory /data contains 2 files");

        ListFilesResponse listing = handler.getFileList("/data");
        assertEquals("completed", listing.getState());
        assertEquals(2, listing.getFilesCount());
        RemoteFile file = listing.getFiles(0);
        assertEquals("a.bin", file.getName());
        assertEquals(100, file.getSize());
        assertTrue(listing.getFiles(1).getIsDirectory());
    }

    @Test
    public void fileNamesWithSpacesParse() {
        handler.beginListing("/data");
        feed("DirectoryListing", "[DirectoryListing] Directory /data: my file.bin (5 bytes)");
        feed("ListDirectorySucceeded", "[ListDirectorySucceeded] Directory /data contains 1 files");
        assertEquals("my file.bin", handler.getFileList("/data").getFiles(0).getName());
    }

    @Test
    public void structuredArgsPreferredOverMessage() {
        handler.beginListing("/d");
        // A conflicting parseable message rides along: the structured extra
        // args must win.
        Event evt = Event.newBuilder()
                .setSource("test")
                .setSeqNumber(0)
                .setGenerationTime(0)
                .setReceptionTime(0)
                .setType("DirectoryListing")
                .setMessage("[DirectoryListing] Directory /d: wrong.bin (3 bytes)")
                .putAllExtra(Map.of("dirName", "/d", "fileName", "x.bin", "fileSize", "7"))
                .build();
        handler.onTuple(null, eventTuple(evt));
        feedStructured("ListDirectorySucceeded", Map.of("dirName", "/d"));

        ListFilesResponse listing = handler.getFileList("/d");
        assertEquals("completed", listing.getState());
        assertEquals(1, listing.getFilesCount());
        assertEquals("x.bin", listing.getFiles(0).getName());
        assertEquals(7, listing.getFiles(0).getSize());
    }

    @Test
    public void errorTerminalFailsListing() {
        handler.beginListing("/data");
        feed("ListDirectoryError",
                "[ListDirectoryError] Directory /data could not be read, status 1");
        assertEquals("failed", handler.getFileList("/data").getState());
    }

    @Test
    public void errorTerminalMatchesDirectoryNameWithSpaces() {
        handler.beginListing("/my data dir");
        feed("ListDirectoryError",
                "[ListDirectoryError] Directory /my data dir could not be read, status 1");
        assertEquals("failed", handler.getFileList("/my data dir").getState());
    }

    @Test
    public void cachedListingsAreBoundedLru() {
        for (int i = 0; i <= RemoteFileListingHandler.MAX_CACHED_LISTINGS; i++) {
            String dir = "/dir" + i;
            handler.beginListing(dir);
            feed("ListDirectorySucceeded",
                    "[ListDirectorySucceeded] Directory " + dir + " contains 0 files");
        }
        assertNull(handler.getFileList("/dir0"), "oldest cached listing must be evicted");
        assertEquals("completed", handler
                .getFileList("/dir" + RemoteFileListingHandler.MAX_CACHED_LISTINGS).getState());
    }

    @Test
    public void entriesForUnknownDirectoryIgnored() {
        handler.beginListing("/data");
        feed("DirectoryListing", "[DirectoryListing] Directory /other: a.bin (1 bytes)");
        feed("ListDirectorySucceeded", "[ListDirectorySucceeded] Directory /data contains 0 files");
        assertEquals(0, handler.getFileList("/data").getFilesCount());
        assertNull(handler.getFileList("/other"));
    }

    @Test
    public void malformedMessagesIgnored() {
        handler.beginListing("/data");
        feed("DirectoryListing", "garbage that does not match");
        feed("ListDirectorySucceeded", "[ListDirectorySucceeded] Directory /data contains 0 files");
        assertEquals(0, handler.getFileList("/data").getFilesCount());
    }

    @Test
    public void throwingMonitorDoesNotBlockOthers() {
        List<ListFilesResponse> seen = new ArrayList<>();
        RemoteFileListMonitor bad = l -> {
            throw new IllegalStateException("boom");
        };
        RemoteFileListMonitor good = seen::add;
        handler.registerMonitor(bad);
        handler.registerMonitor(good);

        handler.beginListing("/data");
        feed("ListDirectorySucceeded", "[ListDirectorySucceeded] Directory /data contains 0 files");
        assertEquals(1, seen.size());

        handler.unregisterMonitor(bad);
        handler.unregisterMonitor(good);
        assertTrue(handler.getMonitors().isEmpty());
    }

    @Test
    public void failListingFlipsInProgressListing() {
        handler.beginListing("/data");
        handler.failListing("/data");
        assertEquals("failed", handler.getFileList("/data").getState());
    }

    @Test
    public void staleListingExpiresAsFailed() {
        handler.beginListing("/data");
        handler.expireStaleListings(System.currentTimeMillis()
                + RemoteFileListingHandler.LISTING_EXPIRY_MS + 1);
        assertEquals("failed", handler.getFileList("/data").getState());
    }

    @Test
    public void freshListingSurvivesExpirySweep() {
        handler.beginListing("/data");
        handler.expireStaleListings();
        assertNull(handler.getFileList("/data"), "listing still in progress");
        feed("ListDirectorySucceeded", "[ListDirectorySucceeded] Directory /data contains 0 files");
        assertEquals("completed", handler.getFileList("/data").getState());
    }

    @Test
    public void saveFileListRoundTripsThroughCache() {
        ListFilesResponse listing = ListFilesResponse.newBuilder()
                .setRemotePath("/saved")
                .setState("success")
                .addFiles(RemoteFile.newBuilder().setName("a.bin").setSize(1))
                .build();
        handler.saveFileList(listing);
        assertEquals(listing, handler.getFileList("/saved"));

        handler.saveFileList(null);
        assertEquals(listing, handler.getFileList("/saved"), "null save must be a no-op");
    }
}
