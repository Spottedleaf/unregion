package ca.spottedleaf.unregion;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Scanner;

public final class Main {

    public static void main(final String[] args) {
        final Scanner input = new Scanner(System.in);

        final File directory = new File(input.nextLine());
        final File[] files = directory.listFiles((File unused, String name) -> name.endsWith(".mca") && name.startsWith("r"));

        if (files == null) {
            System.err.println("Invalid directory " + directory.getAbsolutePath());
            return;
        }

        final File rawChunkDirectory = new File(directory, "rawchunks");
        rawChunkDirectory.mkdirs();

        ByteBuffer chunkTempBuffer = ByteBuffer.allocateDirect(2 * 1024 * 1024 * 100);

        for (int k = 0, len = files.length; k < len; ++k) {
            final File regionFile = files[k];
            // r.x.z.mca

            final String[] nameSplit = regionFile.getName().split("\\.");

            // get chunk offsets

            final int chunkXOff;
            final int chunkZOff;

            try {
                final int regionX = Integer.parseInt(nameSplit[1]);
                final int regionZ = Integer.parseInt(nameSplit[2]);
                chunkXOff = regionX * 32;
                chunkZOff = regionZ * 32;
            } catch (final NumberFormatException ex) {
                System.err.println("Region file " + regionFile.getAbsolutePath() + " has an invalid name");
                continue;
            }

            final long requiredLength = regionFile.length();

            if (requiredLength >= Integer.MAX_VALUE) {
                System.err.println("Region file " + regionFile.getAbsolutePath() + " is too large");
            }

            // presize

            if (requiredLength > chunkTempBuffer.limit()) {
                chunkTempBuffer = ByteBuffer.allocateDirect((int)requiredLength);
            }

            // read data fully

            try (FileInputStream in = new FileInputStream(regionFile)) {
                in.getChannel().read(chunkTempBuffer);
                chunkTempBuffer.position(0);
                chunkTempBuffer.limit((int)requiredLength);
            } catch (IOException ex) {
                synchronized (System.err) {
                    System.err.println("Failed to read region file data for " + regionFile.getAbsolutePath());
                    ex.printStackTrace(System.err);
                }
                continue;
            }

            // prepare header
            final IntBuffer regionFileAsInt = chunkTempBuffer.asIntBuffer();

            for (int i = 0; i < (32 * 32); ++i) {
                // i = x | (z << 5)
                final int location = regionFileAsInt.get(i); // location = (offset << 8) | (length in sectors & 255)
                final int offset = location >>> 8; // in sectors from the start of file (1 sector = 4096)
                final int x = i & 32;
                final int z = i >>> 5;

                if (offset == 0) {
                    continue;
                }


                final int totalBytes = regionFileAsInt.get(offset * 4096 / 4) - 1; // subtract 1 byte for the compression type, divide offset by 4 for sizeof(int)
                final int sectorsRequired = (totalBytes + 5) / 4096 + 1;

                if (sectorsRequired != (location & 255)) {
                    System.err.println("Invalid chunk (" + x + "," + z + ") in region file " + regionFile.getAbsolutePath());
                    continue;
                }

                // offset is 5 bytes from here (4 for length, 1 for type)
                final int rawDataOffset = 5 + offset * 4096;


                // create target file now
                final String targetName = "rawchunk." + (chunkXOff + x) + "." + (chunkZOff + z) + ".zsucks";

                final File targetFile = new File(rawChunkDirectory, targetName);

                try (FileOutputStream out = new FileOutputStream(targetFile, false)) {
                    chunkTempBuffer.position(rawDataOffset);
                    chunkTempBuffer.limit(totalBytes + rawDataOffset);
                    out.getChannel().write(chunkTempBuffer);
                    chunkTempBuffer.position(0);
                    chunkTempBuffer.limit((int)requiredLength);
                } catch (IOException ex) {
                    synchronized (System.err) {
                        System.err.println("Failed to write region file data for " + regionFile.getAbsolutePath());
                        ex.printStackTrace(System.err);
                    }
                }
            }

            // completed this region file
            System.out.println("Completed data for region file " + regionFile.getName() + ", " + k + "/" + len);
        }
        System.out.println("Completed");
    }
}