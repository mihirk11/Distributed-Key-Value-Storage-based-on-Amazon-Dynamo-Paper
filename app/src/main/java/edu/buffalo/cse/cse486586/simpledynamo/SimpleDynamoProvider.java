package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.renderscript.Element;
import android.telephony.TelephonyManager;
import android.util.Log;

public class 	SimpleDynamoProvider extends ContentProvider {

	String PROVIDERTAG = "PROVIDERTAG";
	Context c;
	SQLiteDatabase readDb;
	SQLiteDatabase writeDb;
	//String myPort;
	String myId;
	public static final int SERVER_PORT = 10000;

	Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	ArrayList<Node> nodeList = new ArrayList<Node>();

	Node myNode;

	class NodeComparator implements Comparator<Node> {

		@Override
		public int compare(Node lhs, Node rhs) {
			return lhs.hash.compareTo(rhs.hash);
		}
	}

	DictionaryOpenHelper mDbHelper;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.d(PROVIDERTAG,"delete:In Delete function");
		if(selection.equals("*")||selection.equals("@")){
			Log.e(PROVIDERTAG,"* and @ not implemented yet. returning");
			return -1;
		}
		int failedAvdCount=0;
		ArrayList<Node> deleteNodeList=getNodesForKey(selection);
		//TODO Send to replicas remaining
		Message message=null;
		try{
		message=new Message("delete",selection,null,myNode.id,deleteNodeList.get(0).id,true);
		Log.d(PROVIDERTAG,"delete:Calling unicast Messsage");
		unicastMessage(message);
		} catch (StreamCorruptedException e) {
			failedAvdCount++;
		}

		try{
		message=new Message("delete",selection,null,myNode.id,deleteNodeList.get(1).id,true);
		Log.d(PROVIDERTAG,"delete:Calling unicast Messsage");
		unicastMessage(message);
		} catch (StreamCorruptedException e) {
			failedAvdCount++;
		}

		try{
		message=new Message("delete",selection,null,myNode.id,deleteNodeList.get(2).id,true);
		Log.d(PROVIDERTAG,"delete:Calling unicast Messsage");
		unicastMessage(message);
		} catch (StreamCorruptedException e) {
			failedAvdCount++;
		}

		Log.d(PROVIDERTAG,"delete: Failed AVD count is "+failedAvdCount);
		if(failedAvdCount >1)	Log.e(PROVIDERTAG,"delete: FAILED AVD COUNT >1 SOMETHING FISHY");

		//return null;
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

		Log.d(PROVIDERTAG,"insert:In Insert function");
		ArrayList<Node> insertNodeList=getNodesForKey(values.get("key").toString());
		//TODO Send to replicas remaining
		Message message=null;

		int failedAvdCount=0;
		try {
			message = new Message("insert", values.get("key").toString(), values.get("value").toString(), myNode.id, insertNodeList.get(0).id, true);
			Log.d(PROVIDERTAG, "insert:Calling unicast Messsage");
			unicastMessage(message);
		} catch (StreamCorruptedException e) {
			failedAvdCount++;
		}
		try{
			message=new Message("insert",values.get("key").toString(),values.get("value").toString(),myNode.id,insertNodeList.get(1).id,true);
			Log.d(PROVIDERTAG,"insert:Calling unicast Messsage");
			unicastMessage(message);
		} catch (StreamCorruptedException e) {
			failedAvdCount++;
		}

		try{
			message=new Message("insert",values.get("key").toString(),values.get("value").toString(),myNode.id,insertNodeList.get(2).id,true);
			Log.d(PROVIDERTAG,"insert:Calling unicast Messsage");
			unicastMessage(message);
		} catch (StreamCorruptedException e) {
			failedAvdCount++;
		}

		Log.d(PROVIDERTAG,"insert: Failed AVD count is "+failedAvdCount);
		if(failedAvdCount >1)	Log.e(PROVIDERTAG,"insert: FAILED AVD COUNT >1 SOMETHING FISHY");


		return null;
	}

	private Message unicastMessage(Message message) throws StreamCorruptedException {
		ObjectOutputStream op;
		Socket socket=null;
		Integer toPort = Integer.valueOf(message.to) * 2;
		Log.d(PROVIDERTAG, "unicastMessage: Sending message");

		int retryCount=0;
		while(true) {
			try {
				socket = new Socket();
				Log.d(PROVIDERTAG, "unicastMessage: Connecting to port " + toPort);
				socket.connect(new InetSocketAddress("10.0.2.2", toPort));

				//Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
				op = new ObjectOutputStream(socket.getOutputStream());
				//Log.e(RECURRINGTTAG, "Sending Unicast Message " + msgToSend);
				op.writeObject(message);
				//op.flush();
				//op.close();
				//socket.close();

				//wait for reply
				Log.d(PROVIDERTAG, "unicastMessage: Reading reply");
				socket.setSoTimeout(1000);
				ObjectInputStream ip = new ObjectInputStream(socket.getInputStream());
				Message queryReply = (Message) ip.readObject();
				Log.d(PROVIDERTAG, "unicastMessage: Message reply recieved type:" + queryReply.type);
				socket.close();
				return queryReply;


			}catch (SocketTimeoutException e){
				retryCount++;
				Log.e(PROVIDERTAG,"unicastMessage: SocketTimeoutException Increased retryCount:"+retryCount);
				if(retryCount<3) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}else{
					Log.e(PROVIDERTAG,"unicastMessage: SocketTimeoutException: AVD has Failed :"+toPort);
					throw new StreamCorruptedException();//TODO Do we need to handle this differently?
				}
			}
			catch (StreamCorruptedException e) {
				e.printStackTrace();
				retryCount++;
				Log.e(PROVIDERTAG,"unicastMessage: Increased retryCount:"+retryCount);
				if(retryCount<3) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}else{
					Log.e(PROVIDERTAG,"unicastMessage: AVD has Failed :"+toPort);
					throw e;
				}
			} catch (IOException e) {
				//e.printStackTrace();
				retryCount++;
				Log.e(PROVIDERTAG,"unicastMessage: IOException Increased retryCount:"+retryCount);
				if(retryCount<3) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}else{
					Log.e(PROVIDERTAG,"unicastMessage: IOException: AVD has failed:"+toPort);
					throw new StreamCorruptedException();
				}

				//return null;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				return null;
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}


		//return null;
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		try {
			initDB();//Init DB

			//nodeList.add(myNode);
			for (int i = 5554; i <= 5562; i = i + 2) {
				Node node = new Node(Integer.toString(i), genHash(Integer.toString(i)));
				Log.d(PROVIDERTAG, "Node added id:" + Integer.toString(i) + " hash:" + genHash(Integer.toString(i)));
				nodeList.add(node);
			}

			knowSelf();//Init self

			//getMissedData();

			//Deploy servertask
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);


		} catch (NoSuchAlgorithmException nsae) {
			nsae.printStackTrace();
		} /*catch (IOException e) {
			Log.e("TAG", "Can't create a ServerSocket" + e);
		}*/ catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private void getMissedData() {
		Message queryReply=null;
		HashMap<String,String> resultMap=new HashMap<String, String>();
		Log.d(PROVIDERTAG,"getMissedData: Sending query *");
		Message message;
		for(Node n:nodeList){
			if(n.id.equals(myNode.id)) continue;
			/*Message message=new Message("join",null,null,myNode.id,n.id,true);
			Log.d(PROVIDERTAG,"getMissedData: sending unicast to:"+n.id);
			try {
				queryReply = unicastMessage(message);
			} catch (StreamCorruptedException e) {
				Log.e(PROVIDERTAG,"getMissedData: SOME OTHER AVD FAILED BEFORE RETRIEVING DATA- (OKAY AT START ELSE SOMETHING FISHY)");
			}*/


			message=new Message("query","*",null,myNode.id,n.id,true);
			Log.d(PROVIDERTAG, "getMissedData: sending unicast to:" + n.id);
			try {
				queryReply = unicastMessage(message);
			} catch (StreamCorruptedException e) {
				Log.e(PROVIDERTAG,"getMissedData: SOME OTHER AVD FAILED BEFORE RETRIEVING DATA- (OKAY AT START ELSE SOMETHING FISHY)");
			}

			if(queryReply!=null && queryReply.keyValueMap!=null) resultMap.putAll(queryReply.keyValueMap);
		}


		Log.d(PROVIDERTAG, "getMissedData: Final resultMap is:");
		for(String key:resultMap.keySet()){
			Log.d(PROVIDERTAG, "getMissedData:Received map element key:" + key + " value:" + resultMap.get(key));
		}

		deleteMe(mUri,"@",null,true);
		//Store the data
		for(String key:resultMap.keySet()) {

			ArrayList<Node> keyNodeList=getNodesForKey(key);
			for (Node n:keyNodeList){
				if(n.id.equals(myNode.id)){
					Log.d(PROVIDERTAG,"getMissedData: Inserting in self key:"+key+" value:"+resultMap.get(key));
					ContentValues contentValues = new ContentValues();
					contentValues.put("key", key);
					contentValues.put("value", resultMap.get(key));
					insertMe(mUri, contentValues);
				}
			}

		}


	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		//TODO  * and @ remaining

		Log.d(PROVIDERTAG,"query:In query function selection:"+selection);
		Message message=null;
		Message queryReply=null;
		HashMap<String,String> resultMap=new HashMap<String, String>();
		if(selection.equals("*")){
			Log.d(PROVIDERTAG,"query: Processing query *");
			int failedAvdCount=0;
			for(Node n:nodeList){
				message=new Message("query",selection,null,myNode.id,n.id,true);
				Log.d(PROVIDERTAG,"query: sending unicast to:"+n.id);
				try {
					queryReply = unicastMessage(message);
				} catch (StreamCorruptedException e) {
					Log.d(PROVIDERTAG,"query: Ignoring failed AVD as the operation is query *");
					failedAvdCount++;
				}
				if(queryReply!=null && queryReply.keyValueMap!=null) resultMap.putAll(queryReply.keyValueMap);
			}

			Log.d(PROVIDERTAG,"query: Failed AVD count is "+failedAvdCount);
			if(failedAvdCount >1)	Log.e(PROVIDERTAG,"query: FAILED AVD COUNT >1 SOMETHING FISHY");
		}
		else if(selection.equals("@")){
			message=new Message("query",selection,null,myNode.id,myNode.id,true);
			try {
				queryReply = unicastMessage(message);
			} catch (StreamCorruptedException e) {
				Log.e(PROVIDERTAG,"query: SOMETHING FISHY- AVD FAILED WHEN CALLING SELF");
			}
			for(String key:queryReply.keyValueMap.keySet()){
				Log.d(PROVIDERTAG,"query:Received map element key:"+key+" value:"+queryReply.keyValueMap.get(key));
			}
			if(queryReply!=null) resultMap=new HashMap<String, String>(queryReply.keyValueMap);
		}
		else{
			ArrayList<Node> queryNodeList=getNodesForKey(selection);
			int failedAvdCount=0;
			for(int i=2;i>=0;i--) {
				message = new Message("query", selection, null, myNode.id, queryNodeList.get(i).id, true);
				try{
					queryReply = unicastMessage(message);
				} catch (StreamCorruptedException e) {
					failedAvdCount++;
					continue;
				}
				break;
			}

			Log.d(PROVIDERTAG,"query: Failed AVD count is "+failedAvdCount);
			if(failedAvdCount >1)	Log.e(PROVIDERTAG,"query: FAILED AVD COUNT >1 SOMETHING FISHY");

			for(String key:queryReply.keyValueMap.keySet()){
				Log.d(PROVIDERTAG,"query:Received map element key:"+key+" value:"+queryReply.keyValueMap.get(key));
			}
			if(queryReply!=null) resultMap=new HashMap<String, String>(queryReply.keyValueMap);//TODO check if If condition is good
		}


		Log.d(PROVIDERTAG, "query: Final resultMap is:");
		for(String key:resultMap.keySet()){
			Log.d(PROVIDERTAG, "query:Received map element key:" + key + " value:" + resultMap.get(key));
		}

		String[] columnNames = {"key", "value"};
		MatrixCursor mc = new MatrixCursor(columnNames, 1);
		for(String key:resultMap.keySet()) {

			String[] columnValues = new String[2];
			columnValues[0] = key;
			columnValues[1] = resultMap.get(key);
			mc.addRow(columnValues);
		}
		return mc;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

	private void knowSelf() throws NoSuchAlgorithmException {
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		calculateVarsFromNodelist();
	}

	private void calculateVarsFromNodelist() {
		Collections.sort(nodeList, new NodeComparator());
		Log.d(PROVIDERTAG, "Calculating variable from nodelist");
		int count = -1;
		int myIndex = -1;
		for (Node n : nodeList) {
			count++;
			if (n.id.equals(myId)) {
				myIndex = count;
			}
			Log.d(PROVIDERTAG, "Sorted id:" + n.id + " hash:" + n.hash);
		}
		myNode = nodeList.get(myIndex);
		Log.d(PROVIDERTAG, "My Index is " + myIndex + " My id is " + nodeList.get(myIndex).id);
	}

	private void initDB() {
		//initialized=true;
		Log.d(PROVIDERTAG, "init called");
		c = SimpleDynamoProvider.this.getContext();
		//c=;
		if (c == null) {
			Log.d(PROVIDERTAG, "Context is null");
		}
		mDbHelper = new DictionaryOpenHelper(this.getContext());
		readDb = mDbHelper.getReadableDatabase();
		writeDb = mDbHelper.getWritableDatabase();
	}

	private ArrayList<Node> getNodesForKey(String key) {
		String keyHash = null;
		try {
			keyHash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.d(PROVIDERTAG, "getNodesForKey key:" + key + " hash:" + keyHash);

		int count = -1;
		int index = -1;
		for (Node n : nodeList) {
			count++;
			if (keyHash.compareTo(n.hash) < 0) {
				index = count;
				break;
			}
		}
		if (index == -1) index = 0;
		Log.d(PROVIDERTAG, "Index is " + index);
		int nextIndex = index + 1;
		if (nextIndex >= nodeList.size()) nextIndex = 0;

		int nextNextIndex = nextIndex + 1;
		if (nextNextIndex >= nodeList.size()) nextNextIndex = 0;

		ArrayList<Node> resultList = new ArrayList<Node>();
		resultList.add(nodeList.get(index));
		resultList.add(nodeList.get(nextIndex));
		resultList.add(nodeList.get(nextNextIndex));

		Log.d(PROVIDERTAG, "Nodes for the key: " + nodeList.get(index).id + " " + nodeList.get(nextIndex).id + " " + nodeList.get(nextNextIndex).id);
		return resultList;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private HashMap<String, String> cursorToMap(Cursor cursor) {
		HashMap<String, String> hashMap = new HashMap<String, String>();
		int keyIndex = cursor.getColumnIndex("key");
		int valueIndex = cursor.getColumnIndex("value");
		//publishProgress("LOCAL DUMP:\n");
		while (cursor.moveToNext()) {
			String returnKey = cursor.getString(keyIndex);
			String returnValue = cursor.getString(valueIndex);
			//publishProgress("Key: "+returnKey+" value: "+returnValue+"\n");
			hashMap.put(returnKey, returnValue);
		}
		return hashMap;
	}

	private Cursor MapToCursor(HashMap<String, String> queryReplyMap) {
		for (String key : queryReplyMap.keySet()) {
			Log.d(PROVIDERTAG, "Map element key:" + key + " value:" + queryReplyMap.get(key));
		}
		String[] columnNames = {"key", "value"};
		MatrixCursor mc = new MatrixCursor(columnNames, 1);
		for (String key : queryReplyMap.keySet()) {

			String[] columnValues = new String[2];
			columnValues[0] = key;
			columnValues[1] = queryReplyMap.get(key);
			mc.addRow(columnValues);
		}
		return mc;
	}





	private Uri insertMe(Uri uri, ContentValues values) {
		Log.d(PROVIDERTAG, "In Insert Uri:" + uri.toString() + " ContentValues:" + values.toString());
		try {


			writeDb.insertWithOnConflict("dictionary", null, values, SQLiteDatabase.CONFLICT_REPLACE);

			Log.v("insert", values.toString());
			//delete(uri,values.get("key").toString(),null);//Just for testing. Remove later
			return uri;
		} catch (Exception e) {
			e.printStackTrace();
			//Log.e(PROVIDERTAG,e.printStackTrace());
		}
		return null;
	}

	public int deleteMe(Uri uri, String selection, String[] selectionArgs, boolean lDump) {

		if (lDump == true) {
			int rowsDeleted = writeDb.delete("dictionary", null, null);
			Log.d(PROVIDERTAG, "Delete local: rowsdeleted:" + rowsDeleted);

			return rowsDeleted;
		}
		int rowsDeleted = writeDb.delete("dictionary", "key ='" + selection + "'", selectionArgs);
		Log.d(PROVIDERTAG, "Delete row:  key:" + selection + " rowsdeleted:" + rowsDeleted);

		return rowsDeleted;
	}

	public Cursor queryMe(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder, boolean lDump) {
		Log.d(PROVIDERTAG, "In Query Uri:" + uri.toString() + " Selection:" + selection);


		if (lDump == true) {
			//Cursor cursor=readDb.rawQuery("select * from dictionary",null);
			Cursor cursor = readDb.query("dictionary", projection, null, selectionArgs, null, null, sortOrder);
			Log.d(PROVIDERTAG, "Cursor has values " + cursor.getCount());

			return cursor;
		}
		Cursor cursor = readDb.query("dictionary", projection, "key ='" + selection + "'", selectionArgs, null, null, sortOrder);
		Log.d(PROVIDERTAG, "Cursor has values " + cursor.getCount());

		return cursor;

	}











	//SERVERTASK
	private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Log.e("mihir", "Inside servertask");


			getMissedData();
			//Reference with Dr. Ko's permission: https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
			while (true) {
				try {
					String jsonMessageStr;
					Log.e(PROVIDERTAG, "before accept");
					Socket s = serverSocket.accept();
					Log.e(PROVIDERTAG, "Data incoming");

					ObjectOutputStream op;
					ObjectInputStream ip = new ObjectInputStream(s.getInputStream());


					Message recievedMessage = (Message) ip.readObject();
					Log.d(PROVIDERTAG, "Message recieved type:" + recievedMessage.type + " key:" + recievedMessage.key + " value:" + recievedMessage.value + " from:" + recievedMessage.from + " to:" + recievedMessage.to);

					Message reply=new Message(recievedMessage);
					reply.type="ack";

					Log.d(PROVIDERTAG,"receivedMessage type:"+recievedMessage.type);



					if (recievedMessage.type.equals("insert")) {
						//Insert in self.
						Log.d(PROVIDERTAG, "Storing message key:" + recievedMessage.key + " value:" + recievedMessage.value);
						//Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
						ContentValues contentValues = new ContentValues();
						contentValues.put("key", recievedMessage.key);
						contentValues.put("value", recievedMessage.value);
						insertMe(mUri, contentValues);
						//Will send defaul ack reply
					} else if (recievedMessage.type.equals("query")) {
						Log.d(PROVIDERTAG,"Processing query message");
						HashMap<String, String> hashMap;
						if (recievedMessage.key.equals("*")||recievedMessage.key.equals("@")) {
							//get data from self
							Cursor cursor = queryMe(mUri, null, recievedMessage.key, null, null, true);
							//queryMe(mUri,null,recievedMessage.key,null,null,true);
							hashMap = cursorToMap(cursor);
						} else {
							//get data from self
							Log.d(PROVIDERTAG, "queried key is in my range key:" + recievedMessage.key);
							Cursor cursor = queryMe(mUri, null, recievedMessage.key, null, null, false);
							hashMap = cursorToMap(cursor);
						}
						//Overwrite reply

						reply = new Message("queryreply", myNode.id, recievedMessage.from, hashMap);
					} else if (recievedMessage.type.equals("delete")) {
						if (recievedMessage.key.equals("*")) {
							deleteMe(mUri, recievedMessage.key, null, true);
						} else {
							deleteMe(mUri, recievedMessage.key, null, false);
						}
						//Will send defaul ack reply
					}

					//Send reply
					Log.d(PROVIDERTAG,"ServerTask: Sending reply type:"+reply.type);

					op = new ObjectOutputStream(s.getOutputStream());
					op.writeObject(reply);
					op.flush();

					ip.close();
					s.close();
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}

		}

	}



	private class ClientTask extends AsyncTask<Message, Void, Message> {

		@Override
		protected Message doInBackground(Message... msgs) {

			Log.d("mihir","In ClientTask");
			//	multicastLibrary.multicastFirstMessage(msgs[0]);

			return null;
			//return new Message("xsa","xsa","xsa","xsa","xsa",false);
		}
	}

}


