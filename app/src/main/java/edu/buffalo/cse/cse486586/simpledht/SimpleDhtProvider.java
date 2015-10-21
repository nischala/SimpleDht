package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Message;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] PORTS={REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static final String[] avds={"5554","5556","5558","5560","5562"};
    static final int SERVER_PORT = 10000;
    String portStr=null;
    String myPort=null;
    String succId=null;
    String predId=null;
    boolean smallest=false;
    boolean largest=false;
    HashMap<String,String> Map = new HashMap<String,String>();
    ArrayList<String> list = new ArrayList<String>();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    private Uri mUri;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String[] file_list=getContext().fileList();
        if(selection.equals("\"@\"")||selection.equals("\"*\"")) {
            for(int i=0;i<file_list.length;i++) {
                getContext().deleteFile(file_list[i]);

            }

        }
        else {
            for(int i=0;i<file_list.length;i++) {
                if(selection.equals(file_list[i])) {
                    getContext().deleteFile(selection);
                }
                else {

                    String message="delete"+","+selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, succId);
                }
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String filename = values.get("key").toString();
        String string = values.get("value").toString();

        FileOutputStream outputStream;
        if(predId!=null && succId!=null) {
            try {
                if((genHash(portStr).compareTo(genHash(predId))<0)){
                    if(genHash(filename).compareTo(genHash(predId))>0 || genHash(filename).compareTo(genHash(portStr))<0) {
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        outputStream.write(string.getBytes());
                        outputStream.close();
                    } else {
                        String message="insert"+","+filename+","+string;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,String.valueOf(Integer.parseInt(succId)) );

                    }
                } else if((genHash(portStr).compareTo(genHash(predId))>0)){
                    if (genHash(filename).compareTo(genHash(portStr)) <= 0) {
                        if (genHash(filename).compareTo(genHash(predId)) > 0) {
                            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                            outputStream.write(string.getBytes());
                            outputStream.close();
                        } else if (genHash(filename).compareTo(genHash(predId)) < 0)  {
                            String message = "insert" + "," + filename + "," + string;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, String.valueOf(Integer.parseInt(predId)));
                        }
                    } else {
                        String message = "insert" + "," + filename + "," + string;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, String.valueOf(Integer.parseInt(succId)));
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }  else {

            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e("File write failed", filename);
            }
        }
        return null;

    }




    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)*2));
        /*mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        for(String i:avds) {
            try {
                Map.put(genHash(i), i);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }*/
        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            //return;
        }

        if(myPort.equals("11108")) {
            //succId=portStr;
            //predId=portStr;
            try {
                list.add(genHash(portStr));
            } catch(NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            try {
                Map.put(genHash("5554"), "5554");
                Map.put(genHash("5556"), "5556");
                Map.put(genHash("5558"), "5558");
                Map.put(genHash("5560"), "5560");
                Map.put(genHash("5562"), "5562");
            } catch(NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            /*try {
                joinNode(portStr);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }*/

        }
        else if(!myPort.equals("11108")){
            //String nodeHash=null;

            // String small=Map.get(findsmallest(list));
            //String large=Map.get(findlargest(list));
            /*if(portStr==small)
                smallest=true;
            else if(portStr==large)
                largest=true;*/


            String message="join"+","+portStr;
            Log.v("join",portStr);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, REMOTE_PORT0);
        }



        /*try {
            String node_hash = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }*/
        return false;
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            String msg;

            //Reference:PA1
            try {
                while(true) {
                    Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    msg = in.readLine();
                    String[] result=msg.split(",");

                    //publishProgress(msg);
                    if(result[0].equals("join")) {
                        joinNode(result[1]);
                    } else if(result[0].equals("Succ&Pred")) {
                        succId=result[1];
                        predId=result[2];

                    } else if(result[0].equals("SetPred")) {
                        //succId=result[1];
                        predId=result[1];

                    } else if(result[0].equals("SetSucc")) {
                        succId=result[1];
                        //predId=result[2];

                    }

                    else if(result[0].equals("insert")) {

                        String key1=result[1];
                        String value1=result[2];
                        ContentValues values = new ContentValues();
                        values.put("key",key1);
                        values.put("value",value1);
                        insert(mUri,values);
                    }



                    socket.close();
                }
            } catch (Exception e) {
                Log.e(TAG, "Error");
            }


            return null;

        }
       /*protected void onProgressUpdate(String...strings) {

             // The following code displays what is received in doInBackground().


            return;

        }*/
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            String port = msgs[1];



            try {
                //for(String remotePort:PORTS){

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(port));

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.print(msgToSend);
                out.flush();
                socket.close();
                //}
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
    public void joinNode(String node) throws NoSuchAlgorithmException {
        String nodeHash;
        String successor;
        String predecessor;
        //String msg1 = null;

        try {
            nodeHash = genHash(node);
            if (!list.contains(nodeHash))
                list.add(nodeHash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Collections.sort(list);
        int index = list.indexOf(genHash(node));
        if (list.size() == 1) {
            successor = null;
            predecessor = null;


        } else {
            if (index == 0) {
                successor = list.get(index + 1);
                predecessor = list.get(list.size() - 1);
            } else if (index == (list.size() - 1)) {
                successor = list.get(0);
                predecessor = list.get(index-1);
            } else {
                successor = list.get(index + 1);
                predecessor = list.get(index - 1);
            }
        }


        String msg1 = "Succ&Pred" + "," + Map.get(successor) + "," + Map.get(predecessor);

        String msg2="SetPred"+","+Map.get(predecessor);
        String msg3="SetSucc"+","+Map.get(successor);


        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, String.valueOf(Integer.parseInt(node)*2));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, String.valueOf(Integer.parseInt(Map.get(successor))*2));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3, String.valueOf(Integer.parseInt(Map.get(predecessor))*2));


    }
    /*public String findsmallest(ArrayList<String> list) {
        //Collections.sort(list);
        String small=list.get(0);
        return small;
    }
    public String findlargest(ArrayList<String> list) {
        //Collections.sort(list);

        String large=list.get(list.size()-1);
        return large;
    }*/


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor c= new MatrixCursor(new String[]{"key","value"});
        FileInputStream input;
        String[] file_list=getContext().fileList();
        if(selection.equals("\"@\"")||selection.equals("\"*\"")) {
            try {

                for (String file:getContext().fileList()) {


                    input = getContext().openFileInput(file);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input));

                    c.addRow(new String[]{file, reader.readLine()});
                    
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
                Log.e("Error","Cant read file");
            }
            return c;
        }
        else {
            for(int i=0;i<file_list.length;i++) {
                if(selection.equals(file_list[i])) {
                    try {
                        input = getContext().openFileInput(selection);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(input));


                        c.addRow(new String[]{file_list[i], reader.readLine()});
                    } catch(FileNotFoundException e){
                        e.printStackTrace();
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                }

            }

        }

        Log.v("query", selection);

        return c;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
