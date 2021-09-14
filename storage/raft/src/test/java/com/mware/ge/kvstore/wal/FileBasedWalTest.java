package com.mware.ge.kvstore.wal;

import com.mware.ge.kvstore.utils.LogIterator;
import com.mware.ge.kvstore.wal.FileBasedWal.FileBasedWalInfo;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileBasedWalTest {
    static String kLongMsg = "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789" +
            "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz-%d";

    @Test
    public void appendLogs() throws Exception {
        FileBasedWalInfo info = new FileBasedWalInfo();
        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        Path walDir = Files.createTempDirectory("testWal.");

        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(0, wal.lastLogId());
            for (int i = 1; i <= 10; i++) {
                wal.appendLog(i, 1, 0, String.format("Test string %d", i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(10, wal.lastLogId());
        }

        // Now let's open it to read
        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(10, wal.lastLogId());

            try (LogIterator it = wal.iterator(1, 10)) {
                long id = 1;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format("Test string %d", id), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }

                assertEquals(11, id);
            }
        }

        FileUtils.deleteDirectory(walDir.toFile());
    }

    @Test
    public void cacheOverflow() throws Exception {
        // Force to make each file 1MB, each buffer is 1MB, and there are two
        // buffers at most
        FileBasedWalInfo info = new FileBasedWalInfo();
        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        policy.fileSize = 1024L * 1024L;
        policy.bufferSize = 1024 * 1024;
        Path walDir = Files.createTempDirectory("testWal.");

        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(0, wal.lastLogId());
            for (int i = 1; i <= 10000; i++) {
                wal.appendLog(i, 1, 0, String.format(kLongMsg, i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(10000, wal.lastLogId());
        }

        Collection<File> walFiles = FileUtils.listFiles(walDir.toFile(), new String[] { "wal" } , false);
        assertEquals(11, walFiles.size());

        // Now let's open it to read
        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(10000, wal.lastLogId());
            try (LogIterator it = wal.iterator(1, 10000)) {
                long id = 1;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format(kLongMsg, id), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }


                assertEquals(10001, id);
            }
        }

        FileUtils.deleteDirectory(walDir.toFile());
    }

    @Test
    public void rollback() throws Exception {
        // Force to make each file 1MB, each buffer is 1MB, and there are two
        // buffers at most
        FileBasedWalInfo info = new FileBasedWalInfo();
        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        policy.fileSize = 1024L * 1024L;
        policy.bufferSize = 1024 * 1024;
        Path walDir = Files.createTempDirectory("testWal.");

        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(0, wal.lastLogId());
            for (int i = 1; i <= 10000; i++) {
                wal.appendLog(i, 1, 0, String.format(kLongMsg, i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(10000, wal.lastLogId());

            // Rollback 100 logs
            wal.rollbackToLog(9900);
            assertEquals(9900, wal.lastLogId());

            // Try to read the last 10 logs
            try (LogIterator it = wal.iterator(9891, 9900)) {
                long id = 9891;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format(kLongMsg, id), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }
                assertEquals(9901, id);
            }

            // Now let's rollback even more
            wal.rollbackToLog(5000);
            assertEquals(5000, wal.lastLogId());

            // Try to read the last 10 logs
            try (LogIterator it = wal.iterator(4991, 5000)) {
                long id = 4991;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format(kLongMsg, id), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }
                assertEquals(5001, id);
            }

            // Now let's append 1000 more logs
            for (int i = 5001; i <= 6000; i++) {
                wal.appendLog(i, 1, 0, String.format(kLongMsg, i + 1000).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(6000, wal.lastLogId());

            // Let's verify the last 10 logs
            try (LogIterator it = wal.iterator(5991, 6000)) {
                long id = 5991;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format(kLongMsg, id + 1000), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }
                assertEquals(6001, id);
            }
        }

        FileUtils.deleteDirectory(walDir.toFile());
    }

    @Test
    public void rollbackThenReopen() throws Exception {
        // Force to make each file 1MB, each buffer is 1MB, and there are two
        // buffers at most
        FileBasedWalInfo info = new FileBasedWalInfo();
        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        policy.fileSize = 1024L * 1024L;
        policy.bufferSize = 1024 * 1024;
        Path walDir = Files.createTempDirectory("testWal.");

        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(0, wal.lastLogId());
            for (int i = 1; i <= 1500; i++) {
                wal.appendLog(i, 1, 0, String.format(kLongMsg, i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(1500, wal.lastLogId());

            // Rollbacking to 800
            wal.rollbackToLog(800);
            assertEquals(800, wal.lastLogId());
        }

        // Check the number of files
        Collection<File> walFiles = FileUtils.listFiles(walDir.toFile(), new String[] { "wal" } , false);
        assertEquals(1, walFiles.size());

        // Now let's open it to read
        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(800, wal.lastLogId());
            try (LogIterator it = wal.iterator(1, 800)) {
                long id = 1;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format(kLongMsg, id), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }
                assertEquals(801, id);
            }
        }

        FileUtils.deleteDirectory(walDir.toFile());
    }

    @Test
    public void rollbackToZero() throws Exception {
        // Force to make each file 1MB, each buffer is 1MB, and there are two
        // buffers at most
        FileBasedWalInfo info = new FileBasedWalInfo();
        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        policy.fileSize = 1024L * 1024L;
        policy.bufferSize = 1024 * 1024;
        Path walDir = Files.createTempDirectory("testWal.");

        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(0, wal.lastLogId());
            assertTrue(wal.rollbackToLog(0));
            assertEquals(0, wal.lastLogId());

            // Append < 1MB logs in total
            for (int i = 1; i <= 100; i++) {
                wal.appendLog(i, 1, 0, String.format(kLongMsg, i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(100, wal.lastLogId());

            // Rollback
            assertTrue(wal.rollbackToLog(0));
            assertEquals(0, wal.lastLogId());

            // Append > 1MB logs in total
            for (int i = 1; i <= 1000; i++) {
                wal.appendLog(i, 1, 0, String.format(kLongMsg, i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(1000, wal.lastLogId());

            // Rollback
            assertTrue(wal.rollbackToLog(0));
            assertEquals(0, wal.lastLogId());
        }

        FileUtils.deleteDirectory(walDir.toFile());
    }

    @Test
    public void backAndForth() throws Exception {
        // Force to make each file 1MB, each buffer is 1MB, and there are two
        // buffers at most
        FileBasedWalInfo info = new FileBasedWalInfo();
        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        policy.fileSize = 1024L * 1024L;
        policy.bufferSize = 1024 * 1024;
        Path walDir = Files.createTempDirectory("testWal.");

        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(0, wal.lastLogId());

            // Go back and forth several times, each time append 200 and rollback 100 wal
            for (int count = 0; count < 10; count++) {
                for (int i = count * 100 + 1; i <= (count + 1) * 200; i++) {
                    wal.appendLog(i, 1, 0, String.format(kLongMsg, i).getBytes(StandardCharsets.UTF_8));
                }
                assertTrue(wal.rollbackToLog(100 * (count + 1)));
                assertEquals(100 * (count + 1), wal.lastLogId());
            }

            Collection<File> walFiles = FileUtils.listFiles(walDir.toFile(), new String[] { "wal" } , false);
            assertEquals(10, walFiles.size());

            // Rollback
            for (int i = 999; i >= 0; i--) {
                assertTrue(wal.rollbackToLog(i));
                assertEquals(i, wal.lastLogId());
            }
            walFiles = FileUtils.listFiles(walDir.toFile(), new String[] { "wal" } , false);
            assertEquals(0, walFiles.size());

            for (int i = 1; i <= 10; i++) {
                wal.appendLog(i, 1, 0, String.format(kLongMsg, i).getBytes(StandardCharsets.UTF_8));
            }
            walFiles = FileUtils.listFiles(walDir.toFile(), new String[] { "wal" } , false);
            assertEquals(1, walFiles.size());
        }
    }

    @Test
    public void ttlTest() throws Exception {
        FileBasedWal.WAL_TTL = 3;
        Path walDir = Files.createTempDirectory("testWal.");
        FileBasedWalInfo info = new FileBasedWalInfo();
        FileBasedWalPolicy policy = new FileBasedWalPolicy();
        policy.fileSize = 1024L;
        policy.bufferSize = 128;

        long startIdAfterGC;
        int expiredFilesNum;

        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(0, wal.lastLogId());

            for (int i = 1; i <= 100; i++) {
                wal.appendLog(i, 1, 0, String.format("Test string %d", i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(100, wal.lastLogId());

            startIdAfterGC = wal.walFiles.getLast().other().firstId();
            expiredFilesNum = wal.walFiles.size() - 1;
            Thread.sleep((FileBasedWal.WAL_TTL + 1) * 1000L);

            for (int i = 101; i <= 200; i++) {
                wal.appendLog(i, 1, 0, String.format("Test string %d", i).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(200, wal.lastLogId());
        }

        // Now let's open it to read
        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(200, wal.lastLogId());
            try (LogIterator it = wal.iterator(1, 200)) {
                long id = 1;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format("Test string %d", id), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }
                assertEquals(201, id);
            }

            int totalFilesNum = wal.walFiles.size();
            wal.cleanWAL();
            int numFilesAfterGC = wal.walFiles.size();

            // We will hold the last expired file.
            assertEquals(numFilesAfterGC, totalFilesNum - expiredFilesNum);
        }

        // Now let's open it to read
        try (FileBasedWal wal = FileBasedWal.getWal(walDir.toString(), info, policy, ((logId, termId, clusterId, log) -> true), null)) {
            assertEquals(200, wal.lastLogId());
            assertEquals(startIdAfterGC, wal.firstLogId());
            try (LogIterator it = wal.iterator(startIdAfterGC, 200)) {
                long id = startIdAfterGC;
                while (it.valid()) {
                    assertEquals(id, it.logId());
                    assertEquals(String.format("Test string %d", id), new String(it.logMsg(), StandardCharsets.UTF_8));
                    it.next();
                    ++id;
                }
                assertEquals(201, id);
            }
        }

    }
}
