//latest working
package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import org.apache.http.cookie.CookieAttributeHandler;


public class SimpleDynamoProvider extends ContentProvider {
	private final ReentrantLock lock = new ReentrantLock();
	public static int globalver=-1;
	public static String finalvalue="";
	public static int counter=0;
	public static final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	static final ArrayList<String> remote_ports=new ArrayList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
	static final ArrayList<String> ports=new ArrayList<String>(Arrays.asList("5554", "5556", "5558", "5560", "5562"));
	static final ArrayList<String> chordRing=new ArrayList<String>();
	static final ArrayList<String> actualCRingPorts=new ArrayList<String>(Arrays.asList("11124","11112","11108","11116","11120"));
	static final ArrayList<String> actualCRing =new ArrayList<String>(Arrays.asList("177ccecaec32c54b82d5aaafc18a2dadb753e3b1","208f7f72b198dadd244e61801abe1ec3a4857bc9","33d6357cfaaf0f72991b0ecd8c56da066613c089","abf0fd8db03e5ecb199a9b82929e9db79b909643","c25ddd596aa7c81fa12378fa725f706d54325d12"));
	//  static PriorityQueue<String> chordRing=new PriorityQueue<String>();
	static final Map<String, String> hm = new HashMap<String, String>();
	static final int SERVER_PORT = 10000;
	static String myPort="";
	static String ind="";
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {


		// TODO Auto-generated method stub
		SQLiteHelper sqLiteHelper=new SQLiteHelper(getContext());
		String[] matrixColumns = {"key", "value"};
		MatrixCursor categories = new MatrixCursor(matrixColumns);
		Object[] mRow = new Object[2];
		String selection1=null;
		Log.v("query", selection+"");
		Cursor cursor;

		if(selection.equals("@")) {
			int a = sqLiteHelper.getReadableDatabase().delete("messages", selection, selectionArgs);
		}
		if(selection.equals("*"))
		{
			int a = sqLiteHelper.getReadableDatabase().delete("messages", selection, selectionArgs);
			for (int i = 0; i < chordRing.size(); i++) {
				if (!chordRing.get(i).equals(hm.get(ports.get(remote_ports.indexOf(myPort))))) {
					String e = "";

					for (Map.Entry<String, String> entry : hm.entrySet()) {
						if (entry.getValue().equals(chordRing.get(i))) {
							e = entry.getKey();
							break;
						}
					}
					String portToSend = remote_ports.get(ports.indexOf(e));
					//sendMessage("send#" + portToSend + "#" + hashval);



					String msgtosend = "chordring#deleteall";
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(portToSend));
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					PrintStream ps = null;
					try {
						ps = new PrintStream(socket.getOutputStream());

						ps.println(msgtosend);
						ps.flush();
						Log.d("sda", "Message Sent to port " + portToSend + " mesg sent was of sending new chord from" + myPort + " for "+ msgtosend);
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						StringBuffer s = new StringBuffer();
						int a1 = 0;
						int count = 1;
						String rep = "";


						while ((rep = in.readLine()) != null) {

							s.append(rep);
							a1++;
							if (a1 == count)
								break;
						}

						//if (s.toString().equals("replytoqueryall")) {
						Log.d("sda", "received ack after sending msg queryall to  " + portToSend + "from " + myPort);
						if(s.toString().contains("ack")) {
							socket.close();
						}

						//  return "ack";
						ind = "ack";
						// return null;
						//}
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}

		}
		else {
			selection1 = null;
			Log.d("sda", "inside key selection " + selection + "");
			if (selection.contains("replica")) {
				selection = selection.split("#")[1];
				selection1 = "key" + "=?";

				selectionArgs = new String[]{selection};
				int a = sqLiteHelper.getWritableDatabase().delete("messages", selection1, selectionArgs);
				Log.d("sda"," value delete "+ a);
			} else {
				if (selection != null) {
					selection1 = "key" + "=?";

					selectionArgs = new String[]{selection};
				}
				int a = sqLiteHelper.getWritableDatabase().delete("messages", selection1, selectionArgs);
				Log.d("sda"," value delet1e "+ a);
				int ownIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(myPort))));
				String[] preandsucc = findPreandSucc(ownIndex);
				String successor = preandsucc[0];
				String realPort = "";
				for (Map.Entry<String, String> entry : hm.entrySet()) {
					if (entry.getValue().equals(successor)) {
						realPort = remote_ports.get(ports.indexOf(entry.getKey()));
						break;
					}
				}

				int r = deleteothers(realPort, selection);
				int ownIndex1 = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(realPort))));

				String[] preandsucc1 = findPreandSucc(ownIndex1);
				String successor1 = preandsucc1[0];
				String realPort1 = "";
				for (Map.Entry<String, String> entry : hm.entrySet()) {
					if (entry.getValue().equals(successor1)) {
						realPort1 = remote_ports.get(ports.indexOf(entry.getKey()));
						break;
					}
				}

				int r1 = deleteothers(realPort1, selection);
			}
		}


		return 0;
	}
	public static int deleteothers(String successor, String key)
	{
		String[] matrixColumns = {"key", "value"};
		MatrixCursor categories = new MatrixCursor(matrixColumns);
		Socket socket = null;
		String portToSend="";
		Log.e("sda","Successor is "+ successor);
	/*	for (Map.Entry<String, String> entry : hm.entrySet()) {
			if (entry.getValue().equals(successor)) {
				portToSend = remote_ports.get(ports.indexOf(entry.getKey()));
				break;
			}
		}*/
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(successor));
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintStream ps = null;
		int flag=1;
		try {
			ps = new PrintStream(socket.getOutputStream());
			ps.println("deletekey#"+key);
			ps.flush();
			Log.d("sda", "Message Sent to port " +successor  + " mesg sent was of deleting key "+key+" from" + myPort);
			BufferedReader in = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));
			StringBuffer s = new StringBuffer();
			int a = 0;
			int count = 1;
			String rep = "";

			Log.d("sda","about to start listening");
			while ((rep = in.readLine()) != null) {
				Log.d("sda","taking in msges "+ rep);
				if(rep.equals(""))
					flag=1;
				s.append(rep);

				break;


			}
			Log.d("sda","delete value returned for key "+ key+ "is " + s.toString());
			String valtoreturn=s.toString();

			if(valtoreturn.equals("ack"))
				socket.close();
		}
		catch(IOException e)
		{
			Log.d("sda","Caught exception in delete others");
			detectedFailure(successor);
		}
		return 1;
	}
	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.d("sda", "Inside insert for key ");

		ContentValues keyValueToInsert = new ContentValues();
		values.valueSet();
		Set<Map.Entry<String, Object>> set=values.valueSet();
		Iterator itr = set.iterator();
		SQLiteHelper sqLiteHelper=new SQLiteHelper(getContext());
		SQLiteDatabase db = sqLiteHelper.getWritableDatabase();
		//Cursor dbCursor = db.query("messages", null, null, null, null, null, null);
		//String[] columnNames = dbCursor.getColumnNames();

		String val=null;
		String key=null;
		String value=null;
		int count=0;
		String portToAssignKey="";
		String keyToInsert="";
		int flag=0;

		int version=0;
		if(values.size()==3) {
			while (itr.hasNext()) {
				count++;
				Map.Entry me = (Map.Entry) itr.next();
				Log.d("sda", "INSIDE ITERATOR " + count + " value " + me.getValue().toString());

				if (count == 3) {
					Log.d("sda","Version Received from someone else ");
					if (me.getValue().toString().contains("replicate#")||me.getValue().toString().contains("direct#")) {
						key = me.getValue().toString().split("#")[1];
						flag = 1;
						try {
							keyToInsert = genHash(key);
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						}
					} else {
						key = me.getValue().toString();

						try {
							keyToInsert = genHash(key);
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						}
					}

				} else if (count == 2) {
					if (me.getValue().toString().length() == 1)
						version = Integer.parseInt(me.getValue().toString());
				} else
					value = me.getValue().toString();


			//	Log.d("sda", "Key to insert 1" + me.getValue().toString() + " hashed value : " + keyToInsert + ", values:" + (String) (value == null ? null : value.toString()) + " port to assign key " + portToAssignKey);
			}
			Log.d("sda", "Key to insert 2 "+key+" hashed value : "+ keyToInsert + " port to assign key "+portToAssignKey);
		}
		else
		{
			while(itr.hasNext())
			{
				count++;
				Map.Entry me = (Map.Entry)itr.next();


				if(count%2==0) {
					if(me.getValue().toString().contains("replicate#"))
					{
						key = me.getValue().toString().split("#")[1];
						flag=1;
					}
					else
						key = me.getValue().toString();

					try {
						keyToInsert=genHash(key);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}


				}
				else
					value=me.getValue().toString();

							}
			Log.d("sda", "Key to insert 1 "+key+" hashed value : "+ keyToInsert + " port to assign key "+portToAssignKey);

		}
		Log.d("sda","fLAG VALUE SET IS "+flag +" for key "+ key);
		if(flag==1)
		{
			Log.d("sda","inside the flag condition");
			keyValueToInsert.put("key", key);
			keyValueToInsert.put("value", value);
			keyValueToInsert.put("ver",version);
			//addhere
			Log.d("sda","KEY VALUE VERSION INSERTED IS "+ key + " "+ value + " "+version);
			String[] selection=new String[1];
			selection[0]=key;
			ServerTask st=new ServerTask();
			//impcode
			String selection1 = "key" + "=?";

			String[] selectionArgs=new String[]{key};
			//Cursor resultCursor = sqLiteHelper.getReadableDatabase().query("messages",null,selection1,selectionArgs,null,null,null);

			long newRowId = db.insertWithOnConflict("messages", null, keyValueToInsert,SQLiteDatabase.CONFLICT_REPLACE);
			getContext().getContentResolver().notifyChange(uri, null);
			Log.d("sda", "Key value pair replicated in node " + myPort + " is" + key + "-" + value);

		}
		else {
			int ownIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(myPort))));

			Log.d("sda", "first finding remoteports in insert method" + remote_ports.indexOf(myPort));
			Log.d("sda", "first finding ports" + ports.get(remote_ports.indexOf(myPort)));
			Log.d("sda", "first finding hm" + hm.get(ports.get(remote_ports.indexOf(myPort))));
			Log.d("sda", "first finding own index in insert method" + ownIndex);
			String[] preandsucc = findPreandSucc(ownIndex);
			String successor = preandsucc[0];
			String predecessor = preandsucc[1];
			String endLimit = chordRing.get(ownIndex);
			String startLimit = predecessor;
			String realPort = "";

			String firstPort = "";
			String secondPort="";
			//   if(myPort.equals("11108") && keyToInsert.compareTo(predecessor)>0)
			//normalcondition||only1avd present||
			String myHashKey = hm.get(ports.get(remote_ports.indexOf(myPort)));
			for (int i = 1; i < chordRing.size(); i++) {
				if (keyToInsert.compareTo(chordRing.get(i - 1)) > 0 && keyToInsert.compareTo(chordRing.get(i)) < 0) {

					if (chordRing.get(i).equals(myHashKey)) {
						Log.d("sha", "insert in my own partition for key " + keyToInsert);
						keyValueToInsert.put("key", key);
						keyValueToInsert.put("value", value);
						int oldversion=-1;
						ServerTask st=new ServerTask();
						Cursor resultCursor = st.mContentResolver.query(st.mUri, null,
								key, null, null);
						if (resultCursor != null) {

							while(resultCursor.moveToNext()) {
								Log.d("sda","Inside is last 2 ");
//
								if(resultCursor.isLast()) {
									try {
										Log.d("sda"," Version obtained is "+ oldversion);
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}
							Log.d("sda","old version of key are"+ oldversion);
						}
						else
							oldversion=-1;
						lock.lock();
						globalver++;
						oldversion = globalver;
						lock.unlock();
						keyValueToInsert.put("ver",oldversion);
						long newRowId = db.insertWithOnConflict("messages", null, keyValueToInsert,SQLiteDatabase.CONFLICT_REPLACE);
						getContext().getContentResolver().notifyChange(uri, null);
						Log.d("sda", "Key value pair inserted in node " + myPort + " is" + key + "-" + value+ "-"+(oldversion));
						int myindex=actualCRingPorts.indexOf(myPort);
						String portOfActPred=ports.get(remote_ports.indexOf(actualCRingPorts.get(myindex-1)));
						replicateMessage(myHashKey, key, value,oldversion);

						break;
					} else {
						Log.d("sha", "sending message to right node to insert key " + keyToInsert + "to port ");
						for (Map.Entry<String, String> entry : hm.entrySet()) {
							if (entry.getValue().equals(chordRing.get(i))) {
								realPort = remote_ports.get(ports.indexOf(entry.getKey()));
								break;
							}
						}
						Log.d("sda", "insert message sending to port" + realPort);
						Log.d("sda","Message sent to send class 2 " + "insertkey#" + key + "#" + value + "#" + realPort);
						replicateMessage(hm.get(ports.get(remote_ports.indexOf(realPort))), key, value,1000);
						new sendMessageClass().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertkey#" + key + "#" + value + "#" + realPort);

					}
				}
			}
			if (keyToInsert.compareTo(chordRing.get(chordRing.size()-1)) > 0 || keyToInsert.compareTo(chordRing.get(0)) < 0) {
				Log.d("sha", "sending message to right node to insert key " + keyToInsert + "to port ");
				for (Map.Entry<String, String> entry : hm.entrySet()) {
					if (entry.getValue().equals(chordRing.get(0))) {
						realPort = remote_ports.get(ports.indexOf(entry.getKey()));
						break;
					}
				}
				Log.d("sda", "insert message sending to last port" + realPort);
				if(realPort.equals(myPort))
				{
					Log.d("sha", "insert in my own partition for key " + keyToInsert);
					keyValueToInsert.put("key", key);
					keyValueToInsert.put("value", value);
					int oldversion=-1;
					ServerTask st=new ServerTask();
					Cursor resultCursor = st.mContentResolver.query(st.mUri, null,
							"query#"+key, null, null);
					if (resultCursor != null) {

						while(resultCursor.moveToNext()) {
							Log.d("sda","outside is last 1 ");
							if(resultCursor.isLast()) {
//								Log.d("sda","Inside is last 1 "+ resultCursor.getInt(2));
								try {
//									oldversion = resultCursor.getInt(2);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
						Log.d("sda","old version of key are"+ oldversion);
					}
					else
						oldversion=-1;
					lock.lock();
					globalver++;
					oldversion=globalver;
					lock.unlock();
					keyValueToInsert.put("ver",oldversion);
					long newRowId = db.insertWithOnConflict("messages", null, keyValueToInsert,SQLiteDatabase.CONFLICT_REPLACE);
					getContext().getContentResolver().notifyChange(uri, null);
					Log.d("sda", "Key value pair inserted in node " + myPort + " is" + key + "-" + value+"- "+oldversion);
					replicateMessage(myHashKey, key, value,oldversion);

				}
				else {
					Log.d("sda","Message sent to send class 1 " + "insertkey#" + key + "#" + value + "#" + realPort);
					replicateMessage(hm.get(ports.get(remote_ports.indexOf(realPort))), key, value,1000);
					new sendMessageClass().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertkey#" + key + "#" + value + "#" + realPort);
				}

			}


			//new sendKeyToNode().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key+"#"+value+"#"+myPort,realPort);
			//       Log.d("sda","Key value pair sent to successor node "+ realPort+"from "+ myPort +" is" + key +"-"+ value);


		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		Log.d("sda","rrr");
		Log.d("sda","rrr");
		SQLiteHelper sqLiteHelper=new SQLiteHelper(getContext());
		Cursor res = sqLiteHelper.getReadableDatabase().rawQuery( "select max(ver) as ver from messages", null );
		if(res.getCount()!=0) {
			while (res.moveToNext()) {
                int maxver=res.getInt(0);
                lock.lock();
                globalver=maxver+1;
                lock.unlock();
			}
		}
		Log.d("sda","Max version is "+globalver);
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.d("sda","port assigned"+ myPort);
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(SERVER_PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//int n = sqLiteHelper.getReadableDatabase().delete("messages", null, null);
	new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		for(int i=0;i<5;i++)
		{
			SimpleDynamoProvider sdp=new SimpleDynamoProvider();
			String hashedVal="";
			try {
				hashedVal= sdp.genHash(ports.get(i));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			hm.put(ports.get(i),hashedVal);
			Log.d("sda","Hashedvalues for port"+ports.get(i)+" is "+hashedVal);
		}
		/*if(myPort.equals("11108"))
		{
			chordRing.add(hm.get(ports.get(remote_ports.indexOf(myPort))));

		}
		else
		{
			//  chordRing.add(hm.get("5554"));
			chordRing.add(hm.get(ports.get(remote_ports.indexOf(myPort))));
			sendMessage("join#"+myPort);
		}*/
	joinMessage();

		for (Map.Entry<String, String> entry : hm.entrySet())
		{
			chordRing.add(entry.getValue());
		}
		Collections.sort(chordRing);
		Log.d("sda","Chordring has ports"+chordRing);
		/*if(myPort.equals("11108")) {
			ContentValues cv = new ContentValues();

			cv.put("key", "Kjg6g1e9pmqvX4Qqv38afH9h5VLTChak");
			cv.put("value", "abcd");

			ServerTask st = new ServerTask();
			Log.d("sda", "going to insert 1st");
			st.mContentResolver.insert(st.mUri, cv);
			ContentValues cv1 = new ContentValues();
			Log.d("sda", "inserted 1");
			cv1.put("key", "Kjg6g1e9pmqvX4Qqv38afH9h5VLTChak");
			cv1.put("value", "abcddifferent");
			Log.d("sda", "going to insert 2nd");

			st.mContentResolver.insert(st.mUri, cv1);
			Log.d("sda", "inserted 2");
		} */

		return false;
	}
	public  void joinMessage()
	{

		new checkifrejoining().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);


	}
	public  void replicateMessage(String myhaskey, String key,String val, int ver)
	{

		new replicateOthers().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,myhaskey+"#"+key+"#" +val+"#"+ver);


	}
	public static void detectedFailure(String portFailed)
	{
		if(chordRing.size()==5) {
			chordRing.remove(hm.get(ports.get(remote_ports.indexOf(portFailed))));
			Collections.sort(chordRing);
			Log.d("sda","Handling failure of port "+portFailed+" new chord is "+chordRing);
			Log.d("sda", "inside detected");
			broadcastFailedPort(hm.get(ports.get(remote_ports.indexOf(portFailed))));
		}
	}
	public static void broadcastFailedPort(String portfailed)
	{
		String myHashKey=hm.get(ports.get(remote_ports.indexOf(myPort)));
		for (int i = 0; i < chordRing.size(); i++) {
			if (!chordRing.get(i).equals(myHashKey)){
				String e = "";
				if (!chordRing.get(i).equals(portfailed)) {
					for (Map.Entry<String, String> entry : hm.entrySet()) {
						if (entry.getValue().equals(chordRing.get(i))) {
							e = entry.getKey();
							break;
						}
					}
					String portToSend = remote_ports.get(ports.indexOf(e));
					//sendMessage("send#" + portToSend + "#" + hashval);



					String msgtosend = "failedport#" + portfailed;
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(portToSend));
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					PrintStream ps = null;
					try {
						ps = new PrintStream(socket.getOutputStream());

						ps.println(msgtosend);
						ps.flush();
						Log.d("sda", "Message Sent of failed port " + portfailed + " mesg sent was of sending new chord from" + myPort +"to port " +portToSend);
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						StringBuffer s = new StringBuffer();
						int a = 0;
						int count = 1;
						String rep = "";


						while ((rep = in.readLine()) != null) {

							s.append(rep);
							a++;
							if (a == count)
								break;
						}

						if (s.toString().equals("ack")) {
							Log.d("sda", "received ack after sending failed port msg to  " + portToSend + "from " + myPort);

							socket.close();
							//  return "ack";
							ind = "ack";
							// return null;
						}
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
		}
	}
	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		Log.d("sda","Grader QUERY RECEIVED FOR KEY "+selection);

		// TODO Auto-generated method stub
		SQLiteHelper sqLiteHelper=new SQLiteHelper(getContext());
		String[] matrixColumns = {"key", "value"};
		MatrixCursor categories = new MatrixCursor(matrixColumns);
		Object[] mRow = new Object[2];
		String selection1=null;
		int flag=0;
		int ownIndex=chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(myPort))));
		if(selection.contains("#"))
		{
		flag=1;
			selection=selection.split("#")[1];
		}
		Log.v("query", selection+"");
		Cursor cursor=null;
		String[] preandsucc=findPreandSucc(ownIndex);
		String successor=preandsucc[0];
		String predecessor=preandsucc[1];
		String endLimit=chordRing.get(ownIndex);
		String startLimit=predecessor;
		String realPort="";
		int flag1=0;
		Object[] mRow1=null;
		String[] matrixColumns1 = {"key", "value","ver"};
		MatrixCursor categories1 = new MatrixCursor(matrixColumns1);
		if(selection.equals("@@"))
		{
			flag1=1;
			selection="@";
			mRow1 = new Object[3];

		}
		if((selection!=null && selection.equals("@"))|| flag1==1)
		{
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			Log.d("sda","Entering @ if loop");
			cursor = sqLiteHelper.getReadableDatabase().query("messages",projection,selection1,selectionArgs,null,null,null);
			while(cursor.moveToNext()) {
//mightchange
				if(flag1==1)
				{
					mRow1[0] = cursor.getString(0);
					if (cursor.getString(1) != null)
						mRow1[1] = cursor.getString(1);
					mRow1[2] = cursor.getInt(2);
					categories1.addRow(mRow1);

					//  Log.d("sda","inside query method keys is "+  cursor.getString(0));

				}
				else {
					try {
						if(myPort.equals("11124"))
						{
							Log.d("sda","keyy is "+cursor.getString(0) );
							if (genHash(cursor.getString(0)).compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") > 0 || genHash(cursor.getString(0)).compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") < 0 )
							{
								Log.d("sda","Added key is "+cursor.getString(0));
								mRow[0] = cursor.getString(0);
								if (cursor.getString(1) != null)
									mRow[1] = cursor.getString(1);
								categories.addRow(mRow);
							}
						}

						if (myPort.equals("11108")) {
							Log.d("sda","keyy is "+cursor.getString(0) );
							if (genHash(cursor.getString(0)).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0 || genHash(cursor.getString(0)).compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") < 0)
							{
								Log.d("sda","Added key is "+cursor.getString(0));
								mRow[0] = cursor.getString(0);
								if (cursor.getString(1) != null)
									mRow[1] = cursor.getString(1);
								categories.addRow(mRow);
							}
						}
						if (myPort.equals("11112")) {
							Log.d("sda","keyy is "+cursor.getString(0) );
							if (genHash(cursor.getString(0)).compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") > 0 || genHash(cursor.getString(0)).compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") < 0)
							{
								Log.d("sda","Added key is "+cursor.getString(0));
								mRow[0] = cursor.getString(0);
								if (cursor.getString(1) != null)
									mRow[1] = cursor.getString(1);
								categories.addRow(mRow);
							}
						}
						if (myPort.equals("11116")) {
							Log.d("sda","keyy is "+cursor.getString(0) );
							if (genHash(cursor.getString(0)).compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") > 0 && genHash(cursor.getString(0)).compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") < 0)
							{
								Log.d("sda","Added key is "+cursor.getString(0));
								mRow[0] = cursor.getString(0);
								if (cursor.getString(1) != null)
									mRow[1] = cursor.getString(1);
								categories.addRow(mRow);
							}
						}
						if (myPort.equals("11120")) {
							Log.d("sda","keyy is "+cursor.getString(0) );
							if (genHash(cursor.getString(0)).compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") > 0 && genHash(cursor.getString(0)).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") < 0)
							{
								Log.d("sda","Added key is "+cursor.getString(0));
								mRow[0] = cursor.getString(0);
								if (cursor.getString(1) != null)
									mRow[1] = cursor.getString(1);
								categories.addRow(mRow);
							}
						}
					}
					catch(Exception e)
					{

					}

				}


			}
			Log.d("sda", "categories returned in at the rate loop are "+categories.getCount());

		}
		else if (selection!=null && selection.contains("*")) {
			Log.d("sda","request received for *");
			for (int i = 0; i < chordRing.size(); i++) {
				if (!chordRing.get(i).equals(hm.get(ports.get(remote_ports.indexOf(myPort))))) {
					String e = "";

					for (Map.Entry<String, String> entry : hm.entrySet()) {
						if (entry.getValue().equals(chordRing.get(i))) {
							e = entry.getKey();
							break;
						}
					}
					String portToSend = remote_ports.get(ports.indexOf(e));
					//sendMessage("send#" + portToSend + "#" + hashval);



					String msgtosend = "chordring#queryall";
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(portToSend));
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					PrintStream ps = null;
					try {
						ps = new PrintStream(socket.getOutputStream());

						ps.println(msgtosend);
						ps.flush();
						Log.d("sda", "Message Sent to port " + portToSend + " mesg sent was of sending new chord from" + myPort + " for "+ msgtosend);
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						StringBuffer s = new StringBuffer();
						int a = 0;
						int count = 1;
						String rep = "";


						while ((rep = in.readLine()) != null) {

							s.append(rep);
							a++;
							if (a == count)
								break;
						}

						//if (s.toString().equals("replytoqueryall")) {
						Log.d("sda", "received ack after sending msg queryall to  " + portToSend + "from " + myPort);
						if(s.toString().contains(":")) {
							String[] kv = s.toString().split(":");
							for (int k = 0; k < kv.length; k++) {

								Object[] Row = new Object[2];
								//	if(kv[k].split("-")[0].contains("*"))
								//		break;
								Row[0] = kv[k].split("-")[0];
								Row[1] = kv[k].split("-")[1];
								categories.addRow(Row);
							}
						}
						socket.close();
						//  return "ack";
						ind = "ack";
						// return null;
						//}
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}

			}
			return categories;
		}
		else
		{
			selection1=null;
			Log.v("query", "inside key selection "+selection+"");
			if(selection!=null) {
				selection1 = "key" + "=?";

				selectionArgs=new String[]{selection};
			}
			Log.d("sda", "Grader is asking for key to query "+selection);
			//try {
			/*	if(myPort.equals("11124"))
				{
					Log.d("sda","keyy is "+selection );
					if (genHash(selection).compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") > 0 || genHash(selection).compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") < 0 )
					{
						cursor = sqLiteHelper.getReadableDatabase().query("messages",projection,selection1,selectionArgs,null,null,null);

					}
				}

				if (myPort.equals("11108")) {
					Log.d("sda","keyy is "+selection );
					if (genHash(selection).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0 || genHash(selection).compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") < 0)
					{
						cursor = sqLiteHelper.getReadableDatabase().query("messages",projection,selection1,selectionArgs,null,null,null);

					}
				}
				if (myPort.equals("11112")) {
					Log.d("sda","keyy is "+selection );
					if (genHash((selection)).compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") > 0 || genHash(selection).compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") < 0)
					{
						cursor = sqLiteHelper.getReadableDatabase().query("messages",projection,selection1,selectionArgs,null,null,null);

					}
				}
				if (myPort.equals("11116")) {
					Log.d("sda","keyy is "+selection);
					if (genHash(selection).compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") > 0 && genHash(selection).compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") < 0)
					{
						cursor = sqLiteHelper.getReadableDatabase().query("messages",projection,selection1,selectionArgs,null,null,null);

					}
				}
				if (myPort.equals("11120")) {
					Log.d("sda","keyy is "+selection );
					if (genHash(selection).compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") > 0 && genHash(selection).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") < 0)
					{
						cursor = sqLiteHelper.getReadableDatabase().query("messages",projection,selection1,selectionArgs,null,null,null);

					}
				}
			}
			catch(Exception e)
			{

			}*/
				cursor = sqLiteHelper.getReadableDatabase().query("messages",projection,selection1,selectionArgs,null,null,null);

				Log.d("sda", "Cursor count inside query is "+cursor.getCount()+"");
			// Log.v("query3", cursor.getString(1)+"");

			if(cursor.getCount()==0 && flag==0)
			{
				Log.d("sda","Key "+ selection+" not found in my db,hence searching for the right node to call");
				String keyToQuery="";
				try {
					keyToQuery=genHash(selection);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				Log.d("sda","Hashed query key "+ keyToQuery);
				String myHashKey=hm.get(ports.get(remote_ports.indexOf(myPort)));
				for(int i=1;i<chordRing.size();i++)
				{

					if(keyToQuery.compareTo(chordRing.get(i-1))>0&&keyToQuery.compareTo(chordRing.get(i))<0)
					{
						Log.d("sda","Found the right pair for hashring "+chordRing.get(i));
						if(chordRing.get(i).equals(myHashKey))
						{
							break;
						}
						else
						{
							Log.d("sha","sending message to right node to insert key "+keyToQuery + "to port ");
							for (Map.Entry<String, String> entry : hm.entrySet())
							{
								if(entry.getValue().equals(chordRing.get(i)))
								{
									realPort=remote_ports.get(ports.indexOf(entry.getKey()));
									break;
								}
							}
							int realIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(realPort))));
							String realPort1="";
							String[] preandsucc1 = findPreandSucc(realIndex);
							String successor1 = preandsucc1[0];
							for (Map.Entry<String, String> entry : hm.entrySet())
							{
								if(entry.getValue().equals(successor1))
								{
									realPort1=remote_ports.get(ports.indexOf(entry.getKey()));
									break;
								}
							}
							Log.d("sda","query message sending to port"+realPort);
							MatrixCursor finalkvPair= queryothers(realPort,selection);

							if(chordRing.size()==4) {
								Log.d("sda","asking to the secondary node");
								finalkvPair = queryothers(realPort1, selection);
							}
							return finalkvPair;
						}
					}
				}
				if(keyToQuery.compareTo(chordRing.get(chordRing.size()-1))>0 || keyToQuery.compareTo(chordRing.get(0))<0)
				{
					Log.d("sha","sending message to right node to insert key "+keyToQuery + "to port ");
					for (Map.Entry<String, String> entry : hm.entrySet())
					{
						if(entry.getValue().equals(chordRing.get(0)))
						{
							realPort=remote_ports.get(ports.indexOf(entry.getKey()));
							break;
						}
					}
					Log.d("sda","query message sending to last port"+realPort);
					//new sendMessageClass().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "insertkey#"+key+"#"+value+"#"+realPort);
					int realIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(realPort))));
					String realPort1="";
					String[] preandsucc1 = findPreandSucc(realIndex);
					String successor1 = preandsucc1[0];
					for (Map.Entry<String, String> entry : hm.entrySet())
					{
						if(entry.getValue().equals(successor1))
						{
							realPort1=remote_ports.get(ports.indexOf(entry.getKey()));
							break;
						}
					}
					MatrixCursor finalkvPair=null;
					if(!myPort.equals(realPort))
					    finalkvPair= queryothers(realPort,selection);

					/*if(chordRing.size()==4) {
						Log.d("sda","asking to the secondary node");
						//if(!myPort.equals(realPort1))
						finalkvPair = queryothers(realPort1, selection);
					}*/
//					Log.d("sda","final kvpair is "+ finalkvPair.getString(0)+ " val "+ finalkvPair.getString(1));
					return finalkvPair;
				}

			}

			else {
				Log.d("sda","Query key found in my dht");
				int maxver=-1;
				while (cursor.moveToNext()) {
					//Log.d("sda","cursor has version "+cursor.getInt(2) +" and max version is "+maxver);
					if(cursor.getInt(2)>maxver) {
						maxver=cursor.getInt(2);
						mRow[0] = cursor.getString(0);
						if (cursor.getString(1) != null)
						{
							mRow[1] = cursor.getString(1).replace(",","");
						}
						//mRow[2]=maxver;
					}


				}
				categories.addRow(mRow);
				Log.d("sda","Value for key "+ mRow[0] + " is " + mRow[1]);
			}

		}
		Log.d("sda","final categories returned is "+ mRow[0]+ " val "+ mRow[1]);
		if(flag1==1)
			return categories1;
		else
		return categories;
	}
	private class replicateOthers extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			Log.d("sda","Org is "+msgs[0]);
			String[] org=msgs[0].split("#");
			String original = org[0];

			String key = org[1];
			String value = org[2];
			int version = Integer.parseInt(org[3]);

			Log.d("sda", "replicatekey inside replicate others " + key + " to " + value + " " + version);
			String[] matrixColumns = {"key", "value", "ver"};
			MatrixCursor categories = new MatrixCursor(matrixColumns);
			int ownIndex = chordRing.indexOf(original);

			String[] succandpre = findPreandSucc(ownIndex);
			String[] hashes = new String[2];
			hashes[0] = succandpre[0];
			hashes[1] = findPreandSucc(chordRing.indexOf(hashes[0]))[0];
			Log.d("sda", "2 successors to replicate are" + hashes[0] + " : " + hashes[1]);
			for (int i = 0; i < 2; i++) {
				Socket socket = null;
				String portToSend = "";
				for (Map.Entry<String, String> entry : hm.entrySet()) {
					if (entry.getValue().equals(hashes[i])) {
						portToSend = remote_ports.get(ports.indexOf(entry.getKey()));
						break;
					}
				}
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(portToSend));
				} catch (IOException e) {
					e.printStackTrace();
				}
				PrintStream ps = null;
				int flag = 1;
				try {
					ps = new PrintStream(socket.getOutputStream());


					ps.println("replicatekey#" + key + "#" + value + "#" + version);
					Log.d("sda", "Message sent was replicatekey#" + key + " to " + portToSend);
					flag = 2;

					ps.flush();
					Log.d("sda", "Message Sent to port " + portToSend + " mesg sent was of replicating key from" + myPort);
					BufferedReader in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));
					StringBuffer s = new StringBuffer();
					int a = 0;
					int count = 1;
					String rep = "";

					Log.d("sda", "about to start listening");
					while ((rep = in.readLine()) != null) {
						Log.d("sda", "taking in msges " + rep);
						if (rep.equals(""))
							flag = 1;
						s.append(rep);

						break;


					}
					Log.d("sda", "Query value returned for key " + key + "is " + s.toString());
					String valtoreturn = s.toString();


					if (valtoreturn.equals("ack"))
						socket.close();
				} catch (IOException e) {
					Log.d("sda", "Caught exception");
					detectedFailure(portToSend);
				}
			}
			return null;
		}
	}
int flag=0;
	public MatrixCursor queryothers(String successor, String key)
	{
		String[] matrixColumns = {"key", "value"};
		MatrixCursor categories = new MatrixCursor(matrixColumns);
		Socket socket = null;
		String portToSend="";
		Log.e("sda","Successor is "+ successor);
	/*	for (Map.Entry<String, String> entry : hm.entrySet()) {
			if (entry.getValue().equals(successor)) {
				portToSend = remote_ports.get(ports.indexOf(entry.getKey()));
				break;
			}
		}*/
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(successor));
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintStream ps = null;
		int flag=1;
		try {
			ps = new PrintStream(socket.getOutputStream());
			if(key.contains("*")) {

				ps.println("querykey#" + key);
				Log.d("sda","Message sent was querykey#"+key+" to "+successor);
				flag=2;
			}
			else
				ps.println("querykey#"+key);
			ps.flush();
			Log.d("sda", "Message Sent to port " +successor  + " mesg sent was of querying key "+key+" from" + myPort);
			BufferedReader in = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));
			StringBuffer s = new StringBuffer();
			int a = 0;
			int count = 1;
			String rep = "";

			Log.d("sda","about to start listening");
			while ((rep = in.readLine()) != null) {
				Log.d("sda","taking in msges "+ rep);
				if(rep.equals(""))
					flag=1;
				s.append(rep);

				break;


			}
			Log.d("sda","Query value returned for key "+ key+ "is " + s.toString());

			String valtoreturn=s.toString();
             if(valtoreturn==null || valtoreturn.equals(""))
			 {
			        flag=0;
					Log.d("sda", "inside manual exception");
					throw new IOException("exception coz value not received");

			 }
			 else {
				 Object[] mRow = new Object[2];
				 mRow[0] = key;
				 mRow[1] = valtoreturn.replace(",", "");
				 categories.addRow(mRow);
			 }
			socket.close();
		}
		catch(IOException e)
		{

			Log.d("sda","Caught exception");
			int realIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(successor))));
			String realPort1="";
			String[] preandsucc1 = findPreandSucc(realIndex);
			String successor1 = preandsucc1[0];
			for (Map.Entry<String, String> entry : hm.entrySet())
			{
				if(entry.getValue().equals(successor1))
				{
					realPort1=remote_ports.get(ports.indexOf(entry.getKey()));
					break;
				}
			}
			//if()
			detectedFailure(successor);
			categories = queryothers(realPort1, key);
			return categories;

		}
		return categories;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		SQLiteHelper sqLiteHelper=new SQLiteHelper(getContext());
		int cursor=sqLiteHelper.getReadableDatabase().update("messages",values,selection,selectionArgs);
		return cursor;
	}

	public String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
	public static String[] findPreandSucc(int ownIndex)
	{
		String successor="";
		String predecessor="";
		if(chordRing.size()==1)
		{
			successor= chordRing.get(0);
			predecessor=chordRing.get(0);
		}
		else if(ownIndex==chordRing.size()-1)
		{
			successor=chordRing.get(0);
			predecessor=chordRing.get(ownIndex-1);
		}
		else if(ownIndex==0)
		{
			successor=chordRing.get(ownIndex+1);
			predecessor=chordRing.get(chordRing.size()-1);
		}
		else
		{
			successor=chordRing.get(ownIndex+1);
			Log.d("sda","own index is"+ ownIndex);
			predecessor=chordRing.get(ownIndex-1);
		}

		String[] arr=new String[2];
		arr[0]=successor;
		arr[1]=predecessor;
		//portToAssignKey = SimpleDhtActivity.chordRing.get(ownIndex);
		return arr;
	}

	public  void sendMessage(String indicator)
	{

		new sendMessageClass().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, indicator);


	}

	static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	private class checkifrejoining extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			for (int i = 0; i < remote_ports.size(); i++) {
				if (!remote_ports.get(i).equals(myPort)) {
					String e = "";


					String portToSend = remote_ports.get(i);
					//sendMessage("send#" + portToSend + "#" + hashval);


					String msgtosend = "checkrejoin";
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(portToSend));
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					PrintStream ps = null;
					try {
						ps = new PrintStream(socket.getOutputStream());

						ps.println(msgtosend);
						ps.flush();
						Log.d("sda", "Message Sent of checkrejoin port " + " mesg sent was of sending new chord from" + myPort + "to port " + portToSend);
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						StringBuffer s = new StringBuffer();
						int a = 0;
						int count = 1;
						String rep = "";


						while ((rep = in.readLine()) != null) {

							s.append(rep);
							a++;
							if (a == count)
								break;
						}
						Log.d("sda","reply for checkrejoin is "+s.toString());
						if (s.toString().equals("4")) {
							Log.d("sda", "received 4 after sending checkrejoin port msg to  " + portToSend + "from " + myPort);

							socket.close();
							//  return "ack";

							sendMessage("join#" + myPort);

							// return null;
						}
						break;

					} catch (IOException e1) {

						e1.printStackTrace();
					}

				}
			}
			return null;
		}
	}

	private class sendMessageClass extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			counter++;
			String indicator=msgs[0];
			Log.d("sda","inside sendmessagebg");
			if(indicator.contains("chordring"))
			{
				String[] reply = indicator.split("#");
				for(int i=1;i<reply.length;i++)
				{
					String hashval=reply[1];
					chordRing.add(hashval);
					Collections.sort(chordRing);
				}
				Log.d("sda", "received reply with chordring after sending msg to  " + "11108" + "from " + myPort);
				Collections.sort(chordRing);
				Log.d("sda","At the end, chordring has ports "+chordRing);

			}
			if(indicator.contains("join"))
			{


				String myHashKey=hm.get(ports.get(remote_ports.indexOf(myPort)));
				for (int i = 0; i < remote_ports.size(); i++) {
					if (!remote_ports.get(i).equals(myPort)){



						String portToSend =remote_ports.get(i);
						//sendMessage("send#" + portToSend + "#" + hashval);



						String msgtosend = "addme#" + myPort;
						Socket socket = null;
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(portToSend));
						} catch (IOException e1) {
							e1.printStackTrace();
						}
						PrintStream ps = null;
						try {
							ps = new PrintStream(socket.getOutputStream());

							ps.println(msgtosend);
							ps.flush();
							Log.d("sda", "Message Sent of add port " + myPort + " mesg sent was of sending new chord from" + myPort +"to port " +portToSend);
							BufferedReader in = new BufferedReader(
									new InputStreamReader(socket.getInputStream()));
							StringBuffer s = new StringBuffer();
							int a = 0;
							int count = 1;
							String rep = "";


							while ((rep = in.readLine()) != null) {

								s.append(rep);
								a++;
								if (a == count)
									break;
							}

							if (s.toString().equals("ack")) {
								Log.d("sda", "received ack after sending failed port msg to  " + portToSend + "from " + myPort);

								socket.close();
								//  return "ack";
								ind = "ack";
								// return null;
							}
							else
							{
								//changeherforversion
								socket.close();
								String[] keyvalues = s.toString().split("#");
								Log.d("sda","Keys received after addme message are from port "+portToSend+ " " + s.toString());
								for(int h=1;h<keyvalues.length;h++)
								{
									String key=keyvalues[h].split(":")[0];
									String val=keyvalues[h].split(":")[1];
									int version=Integer.parseInt(keyvalues[h].split(":")[2]);
									ContentValues cv = new ContentValues();

									cv.put("key", "direct#"+key);
									cv.put("value", val);
									cv.put("ver",version);
									ServerTask st=new ServerTask();
									st.mContentResolver.insert(st.mUri, cv);
								}
							}
						} catch (IOException e1) {
							e1.printStackTrace();
						}

					}
				}

			}
			if(indicator.contains("needkeys"))
			{
				String msgtosend = "needkeys";
				Socket socket = null;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(indicator.split("#")[1]));
				} catch (IOException e) {
					e.printStackTrace();
				}
				PrintStream ps = null;
				try {
					ps = new PrintStream(socket.getOutputStream());

					ps.println(msgtosend);
					ps.flush();
					Log.d("sda", "Message Sent to port " + indicator.split("#")[1] + " mesg sent was of needing keys from" + myPort);
					BufferedReader in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));
					StringBuffer s = new StringBuffer();
					int a = 0;
					int count = 1;
					String rep = "";


					while ((rep = in.readLine()) != null) {

						s.append(rep);
						a++;
						// if (a == count)
						//   break;
					}

					if (s.toString().contains("donatekeys")) {
						Log.d("sda", "received keys donation after sending msg to  " + indicator.split("#")[1] + "from " + myPort);
						String[] keyvalues=s.toString().split("#");
						if(s.toString().contains(":")) {
							for (int k = 1; k < keyvalues.length; k++) {
								String key = keyvalues[k].split(":")[0];
								String val = keyvalues[k].split(":")[1];
								int version=Integer.parseInt(keyvalues[k].split(":")[2]);
								ContentValues cv = new ContentValues();

								cv.put("key", key);
								cv.put("value", val);
								cv.put("ver",version);
								ServerTask st=new ServerTask();
								st.mContentResolver.insert(st.mUri, cv);
							}
						}
						socket.close();
						//  return "ack";
						ind="ack";
						return null;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(indicator.contains("deleteall")) {
				Log.d("sda","inside deleteall block");
				int ownIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(myPort))));
				SQLiteHelper sqLiteHelper = null;
				String[] preandsucc = findPreandSucc(ownIndex);
				String successor = preandsucc[0];
				String predecessor = preandsucc[1];
				String realPort = "";
				int n = sqLiteHelper.getReadableDatabase().delete("messages", null, null);
				for (Map.Entry<String, String> entry : hm.entrySet()) {
					if (entry.getValue().equals(successor)) {
						realPort = remote_ports.get(ports.indexOf(entry.getKey()));
						break;
					}
				}

				if (realPort.equals(indicator.split("#")[1])) {
					// return "ack";
					ind="ack";
					return null;
				} else {
					String msgtosend = "deleteall#" + myPort;
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(realPort));
					} catch (IOException e) {
						e.printStackTrace();
					}
					PrintStream ps = null;
					try {
						ps = new PrintStream(socket.getOutputStream());

						ps.println(msgtosend);
						ps.flush();
						Log.d("sda", "Message Sent to port " + realPort + " mesg sent was of delete from" + myPort);
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						StringBuffer s = new StringBuffer();
						int a = 0;
						int count = 1;
						String rep = "";


						while ((rep = in.readLine()) != null) {

							s.append(rep);
							a++;
							if (a == count)
								break;
						}

						if (s.toString().equals("ack")) {
							Log.d("sda", "received ack after sending msg to  " + realPort + "from " + myPort);

							socket.close();
							//  return "ack";
							ind="ack";
							return null;
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			if(indicator.contains("send")) {
				for (int i = 0; i < chordRing.size(); i++) {
					if (!chordRing.get(i).equals(hm.get("5554"))) {
						String e = "";
						if (!chordRing.get(i).equals(indicator.split("#")[1])) {
							for (Map.Entry<String, String> entry : hm.entrySet()) {
								if (entry.getValue().equals(chordRing.get(i))) {
									e = entry.getKey();
									break;
								}
							}
							String portToSend = remote_ports.get(ports.indexOf(e));
							//sendMessage("send#" + portToSend + "#" + hashval);



							String msgtosend = "chordring#" + indicator.split("#")[1];
							Socket socket = null;
							try {
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(portToSend));
							} catch (IOException e1) {
								e1.printStackTrace();
							}
							PrintStream ps = null;
							try {
								ps = new PrintStream(socket.getOutputStream());

								ps.println(msgtosend);
								ps.flush();
								Log.d("sda", "Message Sent to port " + indicator.split("#")[1] + " mesg sent was of sending new chord from" + myPort);
								BufferedReader in = new BufferedReader(
										new InputStreamReader(socket.getInputStream()));
								StringBuffer s = new StringBuffer();
								int a = 0;
								int count = 1;
								String rep = "";


								while ((rep = in.readLine()) != null) {

									s.append(rep);
									a++;
									if (a == count)
										break;
								}

								if (s.toString().equals("ack")) {
									Log.d("sda", "received ack after sending msg to  " + indicator.split("#")[1] + "from " + myPort);

									socket.close();
									//  return "ack";
									ind = "ack";
									// return null;
								}
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						}
					}
				}
			}
			if(indicator.contains("insertkey") || indicator.contains("querykey"))
			{
				String msgtosend="";
				String receiverPort = indicator.split("#")[3];
				Log.d("sda","received port in insertkeys send class is "+receiverPort);
				if(indicator.contains("querykey"))
					msgtosend = "querykey#"+indicator.split("#")[1]+"#"+myPort;
				else
					msgtosend = "direct#"+indicator.split("#")[1]+"#"+indicator.split("#")[2]+"#"+myPort;
				Socket socket = null;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(receiverPort));


					//References : https://www.tutorialspoint.com/java/io/objectoutputstream_writeobject.htm
					PrintStream ps = new PrintStream
							(socket.getOutputStream());
					ps.println(msgtosend);
					ps.flush();
					Log.d("sda","Message Sent to port 1  "+ receiverPort + " mesg sent was "+msgtosend);
					BufferedReader in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));

					StringBuffer s = new StringBuffer();
					int a = 0;
					int count = 1;
					String rep = "";


					while ((rep = in.readLine()) != null) {

						s.append(rep);
						a++;
						if (a == count)
							break;
					}
					Log.d("sda","ACK received from port "+ receiverPort + " mesg was "+s.toString());

					if (s.toString().equals("ack")) {
						Log.d("sda","received ack after sending msg to  "+receiverPort + "from "+ myPort);
						socket.close();
					}
					else
					{
						socket.close();
						Log.d("sda","Caught NULL Exception in insert sending for port " + receiverPort);
						detectedFailure(receiverPort);
						ContentValues cv = new ContentValues();

						cv.put("key", indicator.split("#")[1]);
						cv.put("value", indicator.split("#")[2]);

						Log.d("sda","After failure handling Calling khudka insert again for key" + indicator.split("#")[1]);
						final ContentResolver mContentResolver = getContext().getContentResolver();
						final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
						mContentResolver.insert(mUri, cv);

					}
				}
				catch (SocketTimeoutException e)
				{
					try {
						socket.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					Log.d("sda","Caught Socket Exception in insert sending for port " + receiverPort);
					detectedFailure(receiverPort);
				}
				catch (IOException e) {
					try {
						socket.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					Log.d("sda","Caught IO Exception in insert sending for port " + receiverPort);
					detectedFailure(receiverPort);
					ContentValues cv = new ContentValues();

					cv.put("key", indicator.split("#")[1]);
					cv.put("value", indicator.split("#")[2]);

					Log.d("sda","After failure handling Calling khudka insert again for key" + indicator.split("#")[1]);
					final ContentResolver mContentResolver = getContext().getContentResolver();
					final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
					mContentResolver.insert(mUri, cv);



				}

			}
			Collections.sort(chordRing);
			Log.d("sda","At the end, chordring has ports "+chordRing);
			return null;
		}
	}

	public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		private  final ContentResolver mContentResolver = getContext().getContentResolver();
		private  final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		public void sendMethod(String msg) {
			//String s="";
			//   new SimpleDhtProvider.sendKeyToNode().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryall#"+SimpleDhtActivity.myPort,realPort);

		}
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			counter++;
			Cursor toSendCursor;
			ServerSocket serverSocket = sockets[0];
			BufferedReader in = null;

			Socket socket = null;
			try {
				//serverSocket.bind(new InetSocketAddress("10.0.2.2", SERVER_PORT));

				while (true) {
				/*	if(myPort.equals("11116")) {
						ContentValues cv = new ContentValues();

						cv.put("key", "IBDsxDatXJ011M1Ctx3xO4VUqnnsjfYB");
						cv.put("value", "KfVhStJVqrUV2QHero75W4uuMYPwhY8j");
                       Log.d("sda","Calling insert");
						mContentResolver.insert(mUri, cv);

						mContentResolver.query(mUri, null,
								"@", null, null);
					} */
					socket = serverSocket.accept();

					int bytesRead;

					String reply1 = "";
					//References : https://www.tutorialspoint.com/java/io/objectoutputstream_writeobject.htm
					byte[] read = null;
					//  ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
					in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));
					StringBuffer s = new StringBuffer();
					int a = 0;
					int count = 1;

					while ((reply1 = in.readLine()) != null) {
						s.append(reply1);
						a++;
						if (a == count)
							break;
					}
					String reply = s.toString();
					Log.d("sda","Reply received is "+reply);

					if(reply.contains("checkrejoin")) {

						Log.d("sda", "AVD has chordring after receiving check size " + chordRing.size());
						StringBuilder msgToSend = new StringBuilder();
						if(chordRing.size()==4)
							msgToSend.append("4");
						else
							msgToSend.append("5");


						PrintStream ps = new PrintStream
								(socket.getOutputStream());
						ps.println(msgToSend.toString());
						ps.flush();
					}
					else if(reply.contains("addme"))
					{
						String[] rep=reply.split("#");
						String portToAdd=rep[1];
						String hashval=hm.get(ports.get(remote_ports.indexOf(portToAdd)));
						chordRing.add(hashval);
						Collections.sort(chordRing);
						Log.d("sda","AVD has chordring after adding new avd "+chordRing);
						StringBuilder msgToSend=new StringBuilder();
						int ownIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(myPort))));
						SQLiteHelper sqLiteHelper = null;
						String[] preandsucc = findPreandSucc(ownIndex);
						String successor1 = preandsucc[0];
						String predecessor = preandsucc[1];
						String my2ndPred=findPreandSucc(chordRing.indexOf(predecessor))[1];
						String my3rdPred=findPreandSucc(chordRing.indexOf(my2ndPred))[1];
						String successor2 = findPreandSucc(chordRing.indexOf(successor1))[0];
						String predOf3rd =findPreandSucc(chordRing.indexOf(my3rdPred))[1];

						String realPort = "";

						//	int n = sqLiteHelper.getReadableDatabase().delete("messages", "@", null);
						String realPort1 = "";
						String realPort2 = "";
						String realPort3 = "";
						String realPort4 ="";
						//	int n = sqLiteHelper.getReadableDatabase().delete("messages", "@", null);
						for (Map.Entry<String, String> entry : hm.entrySet()) {
							if (entry.getValue().equals(successor1)) {
								realPort1 = remote_ports.get(ports.indexOf(entry.getKey()));

							}
							if (entry.getValue().equals(successor2)) {
								realPort2 = remote_ports.get(ports.indexOf(entry.getKey()));

							}
							if (entry.getValue().equals(predecessor)) {
								realPort3 = remote_ports.get(ports.indexOf(entry.getKey()));

							}
							if (entry.getValue().equals(my3rdPred)) {
								realPort4 = remote_ports.get(ports.indexOf(entry.getKey()));

							}
						}
						int flagr=0;
						int flage=0;
						int flagw=0;
						PrintStream ps = new PrintStream
								(socket.getOutputStream());
						if(realPort4.equals(portToAdd))
						{
							flage=1;
							/*Log.d("sda","I am the one who has your unnecessary keys "+ myPort);
							Cursor resultCursor = mContentResolver.query(mUri, null, "@", null, null);
							StringBuilder strtosend=new StringBuilder();
							//strtosend.append("donatekeys");
							String myHashKey = hm.get(ports.get(remote_ports.indexOf(myPort)));
							StringBuilder justForDebug=new StringBuilder();
							Log.d("sda","Resultcursor count after querying ics1 "+resultCursor.getCount());
							if (resultCursor != null) {
								Log.d("sda","Resultcursor count after querying ics1 "+resultCursor.getCount());
								while(resultCursor.moveToNext()) {
									try {   // Log.d("sda","inside result cursor , key currently is "+ resultCursor.getString(0) +" and hash is "+genHash(resultCursor.getString(0)));
										if(myPort.equals("11124"))
										{
											if (genHash(resultCursor.getString(0)).compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") > 0 || genHash(resultCursor.getString(0)).compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") < 0 )
											   Log.d("sda","do nothing");
												else
											{
												//int b = mContentResolver.delete(mUri, resultCursor.getString(0), null);
												justForDebug.append(resultCursor.getString(0));
											}
										}
										if(myPort.equals("11108"))
										{
											if (genHash(resultCursor.getString(0)).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0 || genHash(resultCursor.getString(0)).compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") < 0 )
												Log.d("sda","do nothing");
											else
											{
												//int b = mContentResolver.delete(mUri, resultCursor.getString(0), null);
												justForDebug.append(resultCursor.getString(0));
											}
										}
										if(myPort.equals("11112"))
										{
											if (genHash(resultCursor.getString(0)).compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") > 0 || genHash(resultCursor.getString(0)).compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") < 0 )
												Log.d("sda","do nothing");
											else
											{
												//int b = mContentResolver.delete(mUri, resultCursor.getString(0), null);
												justForDebug.append(resultCursor.getString(0));
											}
										}
										if(myPort.equals("11116"))
										{
											if (genHash(resultCursor.getString(0)).compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") > 0 && genHash(resultCursor.getString(0)).compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") < 0 )
												Log.d("sda","do nothing");
											else
											{
											//	int b = mContentResolver.delete(mUri, resultCursor.getString(0), null);
												justForDebug.append(resultCursor.getString(0));
											}
										}
										if(myPort.equals("11120"))
										{
											if (genHash(resultCursor.getString(0)).compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") > 0 && genHash(resultCursor.getString(0)).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") < 0 )
												Log.d("sda","do nothing");
											else
											{
												//int b = mContentResolver.delete(mUri, resultCursor.getString(0), null);
												justForDebug.append(resultCursor.getString(0));
											}
										}

									} catch (NoSuchAlgorithmException e) {
										e.printStackTrace();
									}
								}
								Log.d("sda","Keys deleted are"+ justForDebug);
							}
							//ps.println("ack");
							//ps.flush();
						*/
						}

						 if(realPort1.equals(portToAdd)|| realPort2.equals(portToAdd))
						{
							flagr=1;
							Log.d("sda","I am the one who has your replicating keys "+ myPort);
							Cursor resultCursor = mContentResolver.query(mUri, null, "@@", null, null);
							StringBuilder strtosend=new StringBuilder();
							//strtosend.append("donatekeys");
							String myHashKey = hm.get(ports.get(remote_ports.indexOf(myPort)));
							Log.d("sda","Resultcursor count after querying ics "+resultCursor.getCount());
							if (resultCursor != null) {
								Log.d("sda","Resultcursor count after querying ics "+resultCursor.getCount());
								while(resultCursor.moveToNext()) {
									try {   // Log.d("sda","inside result cursor , key currently is "+ resultCursor.getString(0) +" and hash is "+genHash(resultCursor.getString(0)));
										if(myPort.equals("11124"))
										{
											if (genHash(resultCursor.getString(0)).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0 || genHash(resultCursor.getString(0)).compareTo(myHashKey) < 0) {

												strtosend.append("#" + resultCursor.getString(0) + ":");
												if (resultCursor.getString(1) != null)
													strtosend.append(resultCursor.getString(1) + ":");
												strtosend.append(resultCursor.getInt(2));
												//Log.d("sda","Appending string is "+strtosend);
											}
										}
										else {
											if (genHash(resultCursor.getString(0)).compareTo(predecessor) > 0 && genHash(resultCursor.getString(0)).compareTo(myHashKey) < 0) {

												strtosend.append("#" + resultCursor.getString(0) + ":");
												if (resultCursor.getString(1) != null)
													strtosend.append(resultCursor.getString(1) + ":");
												strtosend.append(resultCursor.getInt(2));
												//Log.d("sda","Appending string is "+strtosend);
											}
										}
									} catch (NoSuchAlgorithmException e) {
										e.printStackTrace();
									}
								}
								Log.d("sda","Keys to be donated are"+ strtosend);
							}
							ps.println(strtosend);
						}
						Log.d("sda","predecessor is "+realPort + "port to add is 1 "+ portToAdd);
						if(realPort3.equals(portToAdd))
						{
							flagw=1;
							Log.d("sda","predecessor is "+predecessor + "port to add is "+ portToAdd);
							Log.d("sda","I am the one who has your keys "+ myPort);
							Cursor resultCursor = mContentResolver.query(mUri, null, "@@", null, null);
							StringBuilder strtosend=new StringBuilder();
							//strtosend.append("donatekeys");
                            Log.d("sda","Resultcursor count after querying is "+resultCursor.getCount());
							if (resultCursor != null) {
								Log.d("sda","Resultcursor count after querying is1 1 "+resultCursor.getCount());

								while(resultCursor.moveToNext()) {
									//Log.d("sda","Resultcursor count after querying is 1111 "+resultCursor.getCount());

									try {
										if(portToAdd.equals("11124"))
										{
											if (genHash(resultCursor.getString(0)).compareTo(predecessor) < 0 ||genHash(resultCursor.getString(0)).compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0 ) {
												strtosend.append("#" + resultCursor.getString(0) + ":");
												if (resultCursor.getString(1) != null)
													strtosend.append(resultCursor.getString(1) + ":");
												strtosend.append(resultCursor.getInt(2));
											}
										}
										else if(portToAdd.equals("11120"))
										{
											if (genHash(resultCursor.getString(0)).compareTo(predecessor) < 0 && genHash(resultCursor.getString(0)).compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") > 0 ) {
												strtosend.append("#" + resultCursor.getString(0) + ":");
												if (resultCursor.getString(1) != null)
													strtosend.append(resultCursor.getString(1) + ":");
												strtosend.append(resultCursor.getInt(2));
											}
										}
										else {
											if (genHash(resultCursor.getString(0)).compareTo(predecessor) < 0) {
												strtosend.append("#" + resultCursor.getString(0) + ":");
												if (resultCursor.getString(1) != null)
													strtosend.append(resultCursor.getString(1) + ":");
												strtosend.append(resultCursor.getInt(2));
											}
										}
									} catch (NoSuchAlgorithmException e) {
										e.printStackTrace();
									}
								}
								Log.d("sda","Keys to be donated are"+ strtosend);
							}
							ps.println(strtosend);
						}

						if(flage==0 && flagw==0 && flagr==0)
						             ps.println("ack");
						ps.flush();
						if(flage==1 && flagr==0)
							ps.println("ack");
						ps.flush();

					}
					else if (reply.contains("#")) {
						String[] msg=reply.split("#");
						StringBuilder msgtoSend=new StringBuilder();
						if(msg[0].equals("failedport"))
						{
							PrintStream ps = new PrintStream
									(socket.getOutputStream());
							ps.println("ack");
							ps.flush();
							chordRing.remove(msg[1]);
							Collections.sort(chordRing);

						}
						else if(msg[0].equals("deletekey"))
						{
							PrintStream ps = new PrintStream
									(socket.getOutputStream());
							ps.println("ack");
							ps.flush();
							Log.d("sda","delete replica of "+ msg[1]);
							int b = mContentResolver.delete(mUri, "replica#"+msg[1], null);
						}
						else if(msg[0].equals("replicatekey"))
						{
							ContentValues cv = new ContentValues();

							cv.put("key", "replicate#"+msg[1]);
							cv.put("value", msg[2]);
							cv.put("ver",msg[3]);
							Log.d("sda","Uri is" + mUri);
							PrintStream ps = new PrintStream
									(socket.getOutputStream());
							ps.println("ack");
							ps.flush();
							mContentResolver.insert(mUri, cv);


						}
						else if(msg[1].equals("queryall"))
						{
							Cursor resultCursor = mContentResolver.query(mUri, null, "@", null, null);
							if(resultCursor.getCount()!=0) {


								Log.d("sda", "Popp");
								while (resultCursor.moveToNext()) {
									msgtoSend.append(resultCursor.getString(0) + "-");
									// msgtosend.append(resultCursor.getString(0) + "#");
									if (resultCursor.getString(1) != null)
										msgtoSend.append(resultCursor.getString(1)+":");
								}


							}
							PrintStream ps = new PrintStream
									(socket.getOutputStream());
							ps.println(msgtoSend);
							ps.flush();


						}
						else if(msg[1].equals("deleteall"))
						{
							int b = mContentResolver.delete(mUri, "@", null);
							SimpleDynamoProvider simpleDhtProvider=new SimpleDynamoProvider();

							if(ind.equals("ack")) {
								PrintStream ps = new PrintStream
										(socket.getOutputStream());
								ps.println("ack");
								ps.flush();
							}
						}
						else if(reply.contains("ack"))
						{
							socket.close();
						}
						else if(reply.contains("chordring"))
						{
							String[] rep = reply.split("#");
							if(!chordRing.contains(hm.get("5554")))
								chordRing.add(hm.get("5554"));
							for(int i=1;i<rep.length;i++)
							{
								String hashval=rep[1];
								if(!chordRing.contains(hashval))
									chordRing.add(hashval);
								Collections.sort(chordRing);
							}
							Log.d("sda", "received reply with chordring after sending msg to  " + "11108" + "from " + myPort);
							Collections.sort(chordRing);
							Log.d("sda","At the end, chordring has ports "+chordRing);
							PrintStream ps = new PrintStream
									(socket.getOutputStream());
							ps.println("ack");
							ps.flush();

						}
						else if(reply.contains("querykey"))
						{
							StringBuilder msgtosend=new StringBuilder();
							Cursor resultCursor;
							//msgtosend.append("finalpair#");
							Log.d("sda","Key to find in its own dht sent from someone else"+reply.split("#")[1]);

							resultCursor = mContentResolver.query(mUri, null, "querykey#"+reply.split("#")[1], null, null);


							if(resultCursor.getCount()!=0) {


								Log.d("sda", "MAIN REPLY ");
								while (resultCursor.moveToNext()) {
									// msgtosend.append(resultCursor.getString(0) + "#");
									if (resultCursor.getString(1) != null)
										msgtosend.append(resultCursor.getString(1));
								}

							}
							Log.d("sda","Message to be sent with keyvalue pairs is "+ msgtosend);
							PrintStream ps = new PrintStream
									(socket.getOutputStream());
							ps.println(msgtosend);
							ps.flush();
							Log.d("sda", "Message Sent to port " + " mesg sent was of querykey from" + myPort);





						}
						else if(reply.contains("direct")){
							PrintStream ps = new PrintStream
									(socket.getOutputStream());
							ps.println("ack");
							ps.flush();
							//call to insert in db

							ContentValues cv = new ContentValues();

							cv.put("key", msg[1]);
							cv.put("value", msg[2]);
							cv.put("ver",1000);
							Log.d("sda","Key value received for insertion in my port " + myPort + " is "+ msg[0]);
							mContentResolver.insert(mUri, cv);

						}
					}
					else if(reply.contains("needkeys")) {

						Cursor resultCursor = mContentResolver.query(mUri, null, "@", null, null);
						StringBuilder strtosend=new StringBuilder();
						strtosend.append("donatekeys");
						int ownIndex = chordRing.indexOf(hm.get(ports.get(remote_ports.indexOf(myPort))));

						String[] succandpre = findPreandSucc(ownIndex);
						String realPort = "";
						String successor = succandpre[0];
						String predecessor = succandpre[1];
						if (resultCursor != null) {

							while(resultCursor.moveToNext()) {
//change
								try {
									if(genHash(resultCursor.getString(0)).compareTo(predecessor)>0) {
										strtosend.append("#" + resultCursor.getString(0) + ":");
										if (resultCursor.getString(1) != null)
											strtosend.append(resultCursor.getString(1));
									}
								} catch (NoSuchAlgorithmException e) {
									e.printStackTrace();
								}
							}
							Log.d("sda","Keys to be donated are"+ strtosend);
						}
						PrintStream ps = new PrintStream
								(socket.getOutputStream());
						ps.println(strtosend);
						ps.flush();
					}
				}

			} catch (IOException e) {

				e.printStackTrace();
			} finally {


				// Log.d(TAG, "Socket Closed");
			}
			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */
			return null;
		}

		protected void onProgressUpdate(String... strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */
			Log.d("final","inside publishprogress for"+ strings[0] );
			if (strings[0] != null) {
				String strReceived = strings[0].trim();
				//  TextView tv = (TextView) findViewById(R.id.textView1);
				//tv.append(strReceived + "\t\n");
				Log.d("final","message "+ strReceived+ "DISPLAYED ");
				// TextView localTextView = (TextView) findViewById(R.id.local_text_display);
				//localTextView.append("\n");

				/*
				 * The following code creates a file in the AVD's internal storage and stores a file.
				 *
				 * For more information on file I/O on Android, please take a look at
				 * http://developer.android.com/training/basics/data-storage/files.html
				 */

				String filename = "SimpleMessengerOutput";
				String string = strReceived + "\n";
				FileOutputStream outputStream;

				try {
					//  outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
					//outputStream.write(string.getBytes());
					//outputStream.close();
				} catch (Exception e) {

				}
			}
			return;
		}
	}



}

