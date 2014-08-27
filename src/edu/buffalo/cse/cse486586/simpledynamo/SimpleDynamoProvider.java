package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    private static final String TAG = "SimpleDynamoProvider";
    private static final int TIMEOUT = 800;
    private static SortedMap<String, Integer> hashToId;
    static Set<String> existingFiles;

    {
        hashToId = new TreeMap<String, Integer>();
        for (int n = 0; n < 3; n++) {
            int id = 5554 + n * 2;
            try {
                hashToId.put(genHash("" + id), id);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        existingFiles = new HashSet<String>();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = values.getAsString("key");
        String value = values.getAsString("value");
        Log.d(TAG, "Inserting " + key);

        int targetId = getTargetEmulator(key);

        Log.d(TAG, "Target Id: " + targetId);

        String fileName = targetId + key;

        if (targetId == SimpleDynamoActivity.id) {

            Log.d(TAG, key + " is stored on this emulator.");
            //fileName = key;

            // Check the version.
            int version = getVersion(fileName);
            Log.d(TAG, "Bumping version to " + version);

            value += ("#" + version);
            try {
                FileOutputStream output = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                output.write(value.getBytes());
                output.close();
                existingFiles.add(fileName);
            } catch (FileNotFoundException fnfe) {
                fnfe.printStackTrace();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

            // Replicate it.
            String message = "replicate:" + fileName + ":" + value;
            for (int n = 5554; n <= 5558; n += 2) {
                if (n != SimpleDynamoActivity.id) {
                    sendMessage(n, message);
                }
            }

        } else {

            Log.d(TAG, key + " is replicated on this emulator.");

            int version = getVersion(fileName);
            Log.d(TAG, "Bumping version to " + version);
            // Replicate it first.
            try {
                value += ("#" + version);
                FileOutputStream output = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                output.write(value.getBytes());
                output.close();
                existingFiles.add(fileName);
            } catch (FileNotFoundException fnfe) {
                fnfe.printStackTrace();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

            // Insert it to the right emulator.
            String message = "insert:" + fileName + ":" + value;
            sendMessage(targetId, message);

            // Replicate it to other emulators.
            for (int n = 5554; n <= 5558; n += 2) {
                if (n != SimpleDynamoActivity.id && n != targetId) {
                    message = "replicate:" + fileName + ":" + value;
                    sendMessage(n, message);
                }
            }
        }
        return null;
    }

    @Override
    public boolean onCreate() {

        existingFiles.addAll(Arrays.asList(getContext().fileList()));
        Log.d(TAG, "Existing files: " + existingFiles);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        
        FileInputStream input;

        String value = "";
        int version = 0;
        int targetId = getTargetEmulator(selection);
        String fileName = targetId + selection;

        try {
            input = getContext().openFileInput(fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            String message = reader.readLine();
            Log.d(TAG, "Read " + message);
            if (message != null) {
            	 String[] tokens = message.split("#");
                 int v = Integer.parseInt(tokens[1]);
                 if (v > version) {
                     version = v;
                     value = tokens[0];
                 }
            }
            reader.close();
            input.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (NumberFormatException ne) {
        	ne.printStackTrace();
        }

        for (int n = 5554; n <= 5558; n++) {
            if (n != SimpleDynamoActivity.id) {
                Socket socket = new Socket();
                try {
                    socket.connect(new InetSocketAddress("10.0.2.2", n * 2), TIMEOUT);
                    socket.setSoTimeout(TIMEOUT);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    writer.println("query:" + fileName);
                    String reply = reader.readLine();
                    Log.d(TAG, "Fetched " + reply);
                    if (reply != null) {
                    	String[] tokens = reply.split("#");
                        int v = Integer.parseInt(tokens[1]);
                        if (v > version) {
                            version = v;
                            value = tokens[0];
                        }
                    }
                } catch (SocketTimeoutException ste) {
                    ste.printStackTrace();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                } catch (NumberFormatException ne) {
                	ne.printStackTrace();
                }
            }
        }

        Log.d(TAG, value + "#" + version);
        MatrixCursor cursor = new MatrixCursor(new String[] { "key", "value", "version" });
        cursor.addRow(new String[] { selection, value, ""+version });
        cursor.close();

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private void sendMessage(int id, String message) {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", id * 2));
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println(message);
            writer.close();
            socket.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * Each node will keep the key which has a greater id than itself. Except
     * the last node, which will also keep the key which has a id less than the
     * smallest node.
     */
    static int getTargetEmulator(String key) {

        int targetId = 0;

        try {
            String keyHash = genHash(key);
            targetId = hashToId.get(hashToId.lastKey());
            for (String hash : hashToId.keySet()) {
                if (keyHash.compareTo(hash) >= 0) {
                    targetId = hashToId.get(hash);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.d(TAG, "Oops");
        }

        return targetId;
    }

    int getVersion(String key) {

        int version = 0;

        if (existingFiles.contains(key)) {
            try {
                FileInputStream input = getContext().openFileInput(key);
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                String message = reader.readLine();
                if (message!=null) {
                	version = Integer.parseInt(message.split("#")[1]);
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (NumberFormatException ne) {
            	ne.printStackTrace();
            }
        }

        for (int n = 5554; n <= 5558; n += 2) {
            if (n != SimpleDynamoActivity.id) {
                Socket socket = new Socket();
                try {
                    socket.connect(new InetSocketAddress("10.0.2.2", n * 2), TIMEOUT);
                    socket.setSoTimeout(TIMEOUT);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    writer.println("version:" + key);
                    String reply = reader.readLine();
                    Log.d(TAG, "Fetched " + reply);
                    if (reply != null) {
                    	int v = Integer.parseInt(reply);
                        if (v > version) {
                            version = v;
                        }
                    }
                    reader.close();
                    socket.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                } catch (NumberFormatException ne) {
                	ne.printStackTrace();
                }
            }
        }

        return (version + 1);
    }

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }

        Log.d(TAG, "Hash for " + input + " is " + formatter);

        return formatter.toString();
    }

}
