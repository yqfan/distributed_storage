package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity implements OnClickListener {

    private static final String TAG = "SimpleDynamoActivity";
    private static final int KEY_COUNT = 20;
    private final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
    private TextView tv;
    static int id;
    
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dynamo);

        tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        Button buttonOne = (Button) findViewById(R.id.button1);
        buttonOne.setOnClickListener(this);
        Button buttonTwo = (Button) findViewById(R.id.button2);
        buttonTwo.setOnClickListener(this);
        Button buttonThree = (Button) findViewById(R.id.button3);
        buttonThree.setOnClickListener(this);
        Button buttonFour = (Button) findViewById(R.id.button4);
        buttonFour.setOnClickListener(this);
        Button buttonFive = (Button) findViewById(R.id.button5);
        buttonFive.setOnClickListener(this);

        TelephonyManager tManager = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        id = Integer.parseInt(tManager.getLine1Number().substring(tManager.getLine1Number().length() - 4));
        Log.d(TAG, "Emulator " + id);
        
        Intent intent = new Intent(this, ListenService.class);
        startService(intent);
        (new Thread(new RecoverSync(getContentResolver(), getBaseContext()))).start();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
        return true;
    }

    @Override
    public void onClick(View v) {
        
        switch (v.getId()) {
        
        case R.id.button1:
            new InsertAsyncTask().execute(new String[]{"Put1"});
            break;
        case R.id.button2:
            new InsertAsyncTask().execute(new String[]{"Put2"});
            break;
        case R.id.button3:
            new InsertAsyncTask().execute(new String[]{"Put3"});
            break;
        case R.id.button5:
        	tv.setText("");
            new QueryAsyncTask().execute();
            break;
        case R.id.button4:
        	tv.setText("");
            for (int n=0; n<KEY_COUNT; n++) {
                Log.d(TAG, "Dumping " + n);
                String fileName = "" + n;
                int targetId = SimpleDynamoProvider.getTargetEmulator("" + n);
//                if (targetId != id) {
//                    fileName = targetId + fileName;
//                }
                fileName = targetId + fileName;
                try {
                    FileInputStream input = openFileInput(fileName);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                    String value = reader.readLine();
                    if (value!= null) {
                    	String[] tokens = value.split("#");
                    	tv.append(n + ": " + tokens[0] + "\n");
                    }
                    reader.close();
                    input.close();
                } catch (FileNotFoundException fnfe) {
                    fnfe.printStackTrace();  
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
            break;
        default:
            Log.e(TAG, "Oops.");
        }
    }

    private class InsertAsyncTask extends AsyncTask<String, Void, Void> {

        private static final String TAG = "InsertAsyncTask";
        
        @Override
        protected Void doInBackground(String... arg) {
        
            Log.d(TAG, "Inserting " + arg[0]);
            
            String prefix = arg[0];
            
            for (int n=0; n<KEY_COUNT; n++) {
                ContentValues cv = new ContentValues();
                cv.put("key", "" + n);
                cv.put("value", prefix + n );
                getContentResolver().insert(uri, cv);
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    // swallow it.
                    ie.printStackTrace();
                }
                
            }
            
            return null;
        }
        
    }
    
    private class QueryAsyncTask extends AsyncTask<Void, String, Void> {

        private static final String TAG = "QueryAsyncTask";

        @Override
        protected void onProgressUpdate(String... update) {
            tv.append(update[0] + "\n");
        }
        
        @Override
        protected Void doInBackground(Void... params) {
            
            Log.d(TAG, "Querying...");
            for (int n=0; n<KEY_COUNT; n++) {
                Cursor c = getContentResolver().query(uri, null, "" + n, null, null);
                if (c != null) {
                    if (c.moveToFirst()) {
                        publishProgress(n + ": " + c.getString(1));
                    }
                }
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
            
            return null;
        }
        
    }
}
