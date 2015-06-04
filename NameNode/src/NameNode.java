import generated.Hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import com.google.protobuf.InvalidProtocolBufferException;

public class NameNode extends UnicastRemoteObject implements INameNode,Runnable{
	
	private Map<Integer, FileOutputStream> fileHandleToStreamMap;
	private Map<Integer, String> fileHandleToNameMap;
	private Map<Integer, String> dataNodeLocationMap;
	//private Map<String, Map<Integer, List<String>>> fileNameToDataLocationMap;
	private Map<String, List<Integer>> fileNameToBlockNumberMap;
	//private Map<Integer, List<Integer>> idToBlockNumbersMap;
	private Map<Integer, List<String>> blockNumberToDataLocationMap;
	private int blockNumber;
	public static boolean [] isDataNodeAlive= new boolean[4];
	Thread nameNodeThread;
	LinkedHashSet<String> runningDataNodeIpList;
	protected NameNode() throws RemoteException {
		super();
		fileHandleToStreamMap = new HashMap<Integer, FileOutputStream>();
		fileHandleToNameMap = new HashMap<Integer, String>();
		dataNodeLocationMap = new HashMap<Integer, String>();
		//fileNameToDataLocationMap = new HashMap<String, Map<Integer, List<String>>>();
		fileNameToBlockNumberMap = new HashMap<String, List<Integer>>();
		//idToBlockNumbersMap = new HashMap<Integer, List<Integer>>();
		runningDataNodeIpList = new LinkedHashSet<String>();
		blockNumberToDataLocationMap = new HashMap<Integer, List<String>>();
		blockNumber = 0;
		initialize();
		nameNodeThread = new Thread(this, "nameNodeThread"); // Create a new thread.
		nameNodeThread.start();
		
	}
	
	private void initialize() {
		Properties prop = new Properties();
		String propFileName = "config.properties";
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

		try {
			if (inputStream != null) {
				prop.load(inputStream);
			}
			inputStream.close();
		}catch (IOException e) {
			e.printStackTrace();
		}
		dataNodeLocationMap.put(1, prop.getProperty("dn1_ip"));
		dataNodeLocationMap.put(2, prop.getProperty("dn2_ip"));
		dataNodeLocationMap.put(3, prop.getProperty("dn3_ip"));
		dataNodeLocationMap.put(4, prop.getProperty("dn4_ip"));
		runningDataNodeIpList.add(prop.getProperty("dn1_ip"));
		runningDataNodeIpList.add(prop.getProperty("dn2_ip"));
		runningDataNodeIpList.add(prop.getProperty("dn3_ip"));
		runningDataNodeIpList.add(prop.getProperty("dn4_ip"));
		for(int i=0;i<4;i++) {
			isDataNodeAlive[i]=false;
		}
		
		// populate fileNameToBlockNumberMap from fsimage
		try {
			FileInputStream fsImageInputStream = new FileInputStream(new File("/home/fsimage"));
			BufferedReader fsImageFile = new BufferedReader
					(new InputStreamReader(fsImageInputStream));
			String line;
			String fileName;
			String temp;
			int blockNo = 0;
			while((line=fsImageFile.readLine()) != null) {
				List<Integer> tempList = new ArrayList<Integer>();
				fileName = line.substring(0, line.lastIndexOf(":"));
				temp = line.substring(line.lastIndexOf(":")+1, line.length());
				StringTokenizer inputTokens = new StringTokenizer(temp, ",");
				while(inputTokens.hasMoreTokens()) {
					blockNo = Integer.parseInt(inputTokens.nextToken());
					tempList.add(blockNo);
				}
				fileNameToBlockNumberMap.put(fileName, tempList);
			}
			blockNumber = blockNo; // Initialise blockNumber with last token
			fsImageFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	private int randInt(int min, int max) {
	    Random rand = new Random();
	    int randomNum = rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	private int getNoOfDataNodesAlive() {
		int count = 0;
		for(boolean dataNodeStatus : isDataNodeAlive) {
			if(dataNodeStatus)
				count++;
		}
		return count;
	}
	
	/*private String getFileNameForBlockNumber(List<Integer> blockNumberList)
	{
		
		for(int blockNo : blockNumberList) {
			for(Entry<String,List<Integer>> entry : fileNameToBlockNumberMap.entrySet()) {
				for(int tempBlockNo : entry.getValue()) {
					if(tempBlockNo == blockNo)
						return entry.getKey();
				}
			}
		}
		return null;
	}*/

	@Override
	public byte[] openFile(byte[] openFileRequest) throws RemoteException {
		
		System.out.println("openFile invoked");
		
		Hdfs.OpenFileResponse.Builder openFileResponseObj = Hdfs.OpenFileResponse.newBuilder();
		try {
			Hdfs.OpenFileRequest openFileRequestObj = Hdfs.OpenFileRequest.parseFrom(openFileRequest);
			String fileName = openFileRequestObj.getFileName();
			if(getNoOfDataNodesAlive() == 0) {
				System.out.println("All the datanodes are down");
				openFileResponseObj.setStatus(1);
			}
			else {
				if(openFileRequestObj.getForRead()) {
					// for reading
					for(int blockNo : fileNameToBlockNumberMap.get(fileName)) {
						openFileResponseObj.addBlockNums(blockNo);
					}
					openFileResponseObj.setStatus(0);
				}
				else {
					if(fileNameToBlockNumberMap.containsKey(fileName)) {
						openFileResponseObj.setStatus(1);
						System.out.println("Duplicate filename");
					}
					else {
						// for writing
						FileOutputStream outputStream = new FileOutputStream(
								new File("/home/"+ fileName));
						fileHandleToStreamMap.put(outputStream.hashCode(), outputStream);
						fileHandleToNameMap.put(outputStream.hashCode(), fileName);
						openFileResponseObj.setHandle(outputStream.hashCode());
						openFileResponseObj.setStatus(0);
					}
				}

			}
		} catch (InvalidProtocolBufferException e) { 
			openFileResponseObj.setStatus(1);
		} catch (FileNotFoundException e) {
			openFileResponseObj.setStatus(1);
		} catch (NumberFormatException e) {
			openFileResponseObj.setStatus(1);
		} 
		return openFileResponseObj.build().toByteArray();
	}

	@Override
	public byte[] closeFile(byte[] closeFileRequest) throws RemoteException {
		System.out.println("closeFile invoked");
		Hdfs.CloseFileResponse.Builder closeFileResponseObj = Hdfs.CloseFileResponse.newBuilder();
		try {
			Hdfs.CloseFileRequest closeFileRequestObj = Hdfs.CloseFileRequest.parseFrom(closeFileRequest);
			fileHandleToStreamMap.get(closeFileRequestObj.getHandle()).close();
			
			// update fsimage
			FileOutputStream fileOutputStream = new FileOutputStream(new File("/home/fsimage"));
			BufferedOutputStream fsImageFileWriter = new BufferedOutputStream(fileOutputStream);
			for(String fileName : fileNameToBlockNumberMap.keySet()) {
				String temp = fileName + ":";
				for(int blockNumber : fileNameToBlockNumberMap.get(fileName)) {
					temp = temp+blockNumber+",";
				}
				temp = temp.substring(0, temp.length()-1);
				temp=temp+"\n";
				fsImageFileWriter.write(temp.getBytes());
			}
			fsImageFileWriter.close();
			//fileOutputStream.close();
			closeFileResponseObj.setStatus(0);
		} catch (InvalidProtocolBufferException e) {
			closeFileResponseObj.setStatus(1);
			
		} catch (IOException e) {
			closeFileResponseObj.setStatus(1);
		}
		return closeFileResponseObj.build().toByteArray();
	}

	@Override
	public byte[] getBlockLocations(byte[] blockLocationRequest)
			throws RemoteException {
		System.out.println("getBlockLocations invoked");
		Hdfs.BlockLocationResponse.Builder blockLocationResponseObj = 
							Hdfs.BlockLocationResponse.newBuilder();
		try {
			Hdfs.BlockLocationRequest blockLocationRequestObj = 
					Hdfs.BlockLocationRequest.parseFrom(blockLocationRequest);
			/*String fileName = getFileNameForBlockNumber(blockLocationRequestObj.getBlockNumsList());
			for(int blockNo : blockLocationRequestObj.getBlockNumsList()) {
				Hdfs.BlockLocations.Builder blockLocationsObj = Hdfs.BlockLocations.newBuilder();
				blockLocationsObj.setBlockNumber(blockNo);
				Map<Integer,List<String>> tempMap = fileNameToDataLocationMap.get(fileName);
				for(String ip : tempMap.get(blockNo)) {
					Hdfs.DataNodeLocation.Builder dataNodeObj = Hdfs.DataNodeLocation.newBuilder();
					dataNodeObj.setIp(ip);
					blockLocationsObj.addLocations(dataNodeObj);
				}

				blockLocationResponseObj.addBlockLocations(blockLocationsObj);
			}*/
			for(int blockNo: blockLocationRequestObj.getBlockNumsList()) {
				Hdfs.BlockLocations.Builder blockLocationsObj = Hdfs.BlockLocations.newBuilder();
				blockLocationsObj.setBlockNumber(blockNo);
				for(String ip : blockNumberToDataLocationMap.get(blockNo)) {
					if(runningDataNodeIpList.contains(ip)) {
						Hdfs.DataNodeLocation.Builder dataNodeObj = Hdfs.DataNodeLocation.newBuilder();
						dataNodeObj.setIp(ip);
						blockLocationsObj.addLocations(dataNodeObj);
					}
				}

				blockLocationResponseObj.addBlockLocations(blockLocationsObj);
			}
			blockLocationResponseObj.setStatus(0);
		} catch(InvalidProtocolBufferException e) {
			blockLocationResponseObj.setStatus(1);	
		}
		return blockLocationResponseObj.build().toByteArray();
	}

	@Override
	public byte[] assignBlock(byte[] assignBlockRequest) throws RemoteException {
		System.out.println("assignBlock invoked");
		Hdfs.AssignBlockResponse.Builder assignBlockResponseObj = Hdfs.AssignBlockResponse.newBuilder();
		try {
			
			Hdfs.AssignBlockRequest assignBlockRequestObj = Hdfs.AssignBlockRequest.parseFrom(assignBlockRequest);
			int handle = assignBlockRequestObj.getHandle();
			String fileName = fileHandleToNameMap.get(handle);
			List<String> dataNodeList = new ArrayList<String>();
			// get the dataNode List
			if(getNoOfDataNodesAlive()==1) {
				dataNodeList.add((String)runningDataNodeIpList.toArray()[0]);
			}
			else {
				int size = runningDataNodeIpList.size();
				int firstDataNode = randInt(0,size-1);
				int secondDataNode = firstDataNode;
				while(firstDataNode == secondDataNode) {
					secondDataNode = randInt(0,size-1);
				}
				dataNodeList.add((String)runningDataNodeIpList.toArray()[firstDataNode]);
				dataNodeList.add((String)runningDataNodeIpList.toArray()[secondDataNode]);
			}
			
			if(fileNameToBlockNumberMap.containsKey(fileName)) {
				// increment blockNumber for the next assigns
				//int blockNumber = fileNameToBlockNumberMap.get(fileName);
				//fileNameToDataLocationMap.get(fileName).put(++blockNumber, dataNodeList);
				fileNameToBlockNumberMap.get(fileName).add(++blockNumber);
				blockNumberToDataLocationMap.put(blockNumber, dataNodeList);
				
			}
			else {
				//Map<Integer,List<String>> tempMap = new HashMap<Integer,List<String>>();
				
				//tempMap.put(++blockNumber, dataNodeList); // insert first block number
				//fileNameToDataLocationMap.put(fileName, tempMap);
				List<Integer> tempList = new ArrayList<Integer>();
				tempList.add(++blockNumber);
				fileNameToBlockNumberMap.put(fileName, tempList);
				blockNumberToDataLocationMap.put(blockNumber, dataNodeList);
			}
			
			Hdfs.BlockLocations.Builder blockLocations = Hdfs.BlockLocations.newBuilder();
			for(String ip : dataNodeList ) {
				Hdfs.DataNodeLocation.Builder dataNodeLocations = Hdfs.DataNodeLocation.newBuilder();
				dataNodeLocations.setIp(ip);
				blockLocations.addLocations(dataNodeLocations);
			
			}
			blockLocations.setBlockNumber(blockNumber);
			assignBlockResponseObj.setNewBlock(blockLocations);
			assignBlockResponseObj.setStatus(0);
		} catch (InvalidProtocolBufferException e) {
			
			assignBlockResponseObj.setStatus(1);
		} 
		return assignBlockResponseObj.build().toByteArray();
	}

	@Override
	public byte[] list(byte[] listFilesRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] blockReport(byte[] blockReportRequest) throws RemoteException {
		Hdfs.BlockReportResponse.Builder blockReportResponseObj = Hdfs.BlockReportResponse.newBuilder();
		try {
			Hdfs.BlockReportRequest blockReportRequestObj = 
					Hdfs.BlockReportRequest.parseFrom(blockReportRequest);
			updateBlockNumberToDataLocationMap(blockReportRequestObj.getId(), 
											blockReportRequestObj.getBlockNumbersList());
			blockReportResponseObj.addStatus(0);
		}catch (InvalidProtocolBufferException e) {
			
			blockReportResponseObj.addStatus(1);
		}
		return blockReportResponseObj.build().toByteArray();
	}
	
	private void updateBlockNumberToDataLocationMap(int id, List<Integer> blockNumberList) {
		 String ip = dataNodeLocationMap.get(id);
		 for(int blockNo : blockNumberList) {
			 if(blockNumberToDataLocationMap.containsKey(blockNo)) {
				 if(blockNumberToDataLocationMap.get(blockNo).size() != 2) {
					 blockNumberToDataLocationMap.get(blockNo).add(ip);
				 }
			 }
			 else {
				 List<String> tempList = new ArrayList<String>();
				 tempList.add(ip);
				 blockNumberToDataLocationMap.put(blockNo,tempList);
			 }
		 }
	}

	@Override
	public byte[] heartBeat(byte[] heartBeatRequest) throws RemoteException {

		Hdfs.HeartBeatResponse.Builder heartBeatResponseObj = Hdfs.HeartBeatResponse.newBuilder();
		try {
			Hdfs.HeartBeatRequest heartBeatRequestObj = Hdfs.HeartBeatRequest.parseFrom(heartBeatRequest);
			int id = heartBeatRequestObj.getId();
			isDataNodeAlive[id-1] = true;
			heartBeatResponseObj.setStatus(0);
		}catch (InvalidProtocolBufferException e) {
			
			heartBeatResponseObj.setStatus(1);
		}
		
		return heartBeatResponseObj.build().toByteArray();
	}
	
	
	public static void main( String args[])
	{
		try {
			
			/*Registry temp = LocateRegistry.getRegistry();
			NameNode nameNode = new NameNode();
			temp.bind("NameNode", nameNode);*/
			//System.setProperty("java.rmi.server.hostname", "192.168.1.149");
			Registry registry = LocateRegistry.createRegistry(1099);
			NameNode nameNode = new NameNode();
			Naming.rebind("NameNode", nameNode);

			System.out.println("NameNode ready");
		} catch (Exception e) {
			System.err.println("NameNode exception: " + e.toString());
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		
		try {
			Thread.sleep(300);
			while(true) {
				for(int i=0;i<4;i++)
					isDataNodeAlive[i]=false;
				Thread.sleep(1000);
				for(int i=0;i<4;i++) {
					if(!isDataNodeAlive[i]) {
						System.out.println("datanode "+(i+1)+ " down");
						runningDataNodeIpList.remove(dataNodeLocationMap.get(i+1));
						//break;
					}
					else {
						runningDataNodeIpList.add(dataNodeLocationMap.get(i+1));
					}
				}
			}
		} catch (InterruptedException e) {
			
	 }
		
		
	}

}
