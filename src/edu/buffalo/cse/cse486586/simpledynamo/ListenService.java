package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

import android.app.IntentService;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

public class ListenService extends IntentService {

    private static final String TAG = "ListenService";
    private static int KEY_COUNT = 20;
    private final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
    private ServerSocket serverSocket;
    
    public ListenService() {
        super("ListenService");
    }

    @Override
    public void onDestroy() {
        try {
            serverSocket.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        super.onDestroy();
    }
    
    @Override
    protected void onHandleIntent(Intent intent) {
        
        Log.d(TAG, "Listening at " + SimpleDynamoActivity.id);
        
        try {
            serverSocket = new ServerSocket(10000);
            while (true) {
                Socket socket = serverSocket.accept();
                Log.d(TAG, "new connection from " + socket.getInetAddress());
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String message = reader.readLine();
                String[] tokens = message.split(":");
                Log.d(TAG, "message tokens " + Arrays.toString(tokens));
                if (tokens.length >= 2) {
                    /* The key is prefixed with targetId. */
                    if (tokens[0].equals("insert") || tokens[0].equals("replicate")) {
                        
                        try {
                            FileOutputStream output = getBaseContext().openFileOutput(tokens[1], Context.MODE_PRIVATE);
                            output.write(tokens[2].getBytes());
                            output.close();
                            SimpleDynamoProvider.existingFiles.add(tokens[1]);
                        } catch (FileNotFoundException fnfe) {
                            fnfe.printStackTrace();
                        } catch (IOException ioe) {
                            ioe.printStackTrace();
                        }
                    } else if (tokens[0].equals("query")) {
                        try {
                            FileInputStream input = getBaseContext().openFileInput(tokens[1]);
                            BufferedReader localReader = new BufferedReader(new InputStreamReader(input));
                            String value = localReader.readLine();
                            Log.d(TAG, "Reading " + value);
                            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                            writer.println(value);
                            input.close();
                            localReader.close();
                        } catch (IOException ioe) {
                            ioe.printStackTrace();
                        }
                    } else if (tokens[0].equals("version")) {

                        String version = "0";
                        if (SimpleDynamoProvider.existingFiles.contains(tokens[1])) {
                            try {
                                FileInputStream input = getBaseContext().openFileInput(tokens[1]);
                                BufferedReader localReader = new BufferedReader(new InputStreamReader(input));
                                String value = localReader.readLine();
                                version = value.split("#")[1];
                                input.close();
                                localReader.close();
                            } catch(IOException ioe) {
                                ioe.printStackTrace();
                            }
                        }
                        
                        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                        writer.println(version);
                        
                    } else {
                        Log.d(TAG, "Unknown action for " + tokens[0]);
                    }
                } else {
                    Log.e(TAG, "Malformed message: " + message);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
        }
    }

    // FIXME This is lame.
    int getVersion(String key) {
        
        int version = 0;
        if (SimpleDynamoProvider.existingFiles.contains(key)) {
            try {
                FileInputStream input = getBaseContext().openFileInput(key);
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                String message = reader.readLine();
                version = Integer.parseInt(message.split("#")[1]);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        
        return version++;
    }

}
