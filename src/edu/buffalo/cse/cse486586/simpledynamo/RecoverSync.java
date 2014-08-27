package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import android.content.ContentResolver;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;

public class RecoverSync implements Runnable{
	
	private static final String TAG = "RecoverSync";
	private static int KEY_COUNT = 20;
    private final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	private final ContentResolver mContentResolver;
	private final Context mBaseContext;
	
	public RecoverSync (ContentResolver _mcr, Context mbc) {
		mContentResolver = _mcr;
		mBaseContext = mbc;
	}
	
	@Override
	public void run() {
		for (int n=0; n<KEY_COUNT; n++) {
            Cursor c = mContentResolver.query(uri, null, "" + n, null, null);
            if (c != null) {
                if (c.moveToFirst()) {
                	String key = c.getString(0);
                    String value = c.getString(1);
                    String version = c.getString(2);
                    int avdId = SimpleDynamoProvider.getTargetEmulator(key);
                    String fileName = avdId+key;
                    try {
                        FileOutputStream output = mBaseContext.openFileOutput(fileName, Context.MODE_PRIVATE);
                        output.write((value+"#"+version).getBytes());
                        output.close();
                        SimpleDynamoProvider.existingFiles.add(fileName);
                    } catch (FileNotFoundException fnfe) {
                        fnfe.printStackTrace();
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
	}

}
