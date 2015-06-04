import generated.Hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class DataNode extends UnicastRemoteObject implements IDataNode,Runnable{

	Thread dataNodeThread;
	private int dataNodeId;
	private String nameNodeIp;
	protected DataNode(int id, String ip) throws RemoteException {
		super();
	
		dataNodeId = id;
		//setNameNodeIp(ip);
		nameNodeIp = ip;
		dataNodeThread = new Thread(this, "dataNodeThread"); // (1) Create a new thread.
		dataNodeThread.start();
	}

	@Override
	public byte[] readBlock(byte[] readBlockRequest) {
		System.out.println("readBlock");
		Hdfs.ReadBlockResponse.Builder readBlockResponseObj = Hdfs.ReadBlockResponse.newBuilder();
		try {
			Hdfs.ReadBlockRequest readBlockRequestObj = Hdfs.ReadBlockRequest.parseFrom(readBlockRequest);
			Integer blockNumber = readBlockRequestObj.getBlockNumber();
			FileInputStream inputStream = new FileInputStream(new File("/home/"+blockNumber.toString()));
			final int  blockSize = 16*1024*1024;
			byte [] block = new byte[blockSize];
			//String block = null;
			int bytesRead = inputStream.read(block);
			//ByteString.cop
			readBlockResponseObj.addData(ByteString.copyFrom(block, 0, bytesRead));
			readBlockResponseObj.setStatus(0);
			inputStream.close();
		}catch (InvalidProtocolBufferException e) {
			readBlockResponseObj.setStatus(1);
		} catch (FileNotFoundException e) {
			readBlockResponseObj.setStatus(1);
		} catch (IOException e) {
			readBlockResponseObj.setStatus(1);
		}
		return readBlockResponseObj.build().toByteArray();
	}

	@Override
	public byte[] writeBlock(byte[] writeBlockRequest) {
		System.out.println("writeBlock");
		Hdfs.WriteBlockResponse.Builder writeBlockResponseObj = Hdfs.WriteBlockResponse.newBuilder();
		try {
			Hdfs.WriteBlockRequest writeBlockRequestObj = 
					Hdfs.WriteBlockRequest.parseFrom(writeBlockRequest);
			Integer blockNumber = writeBlockRequestObj.getBlockInfo().getBlockNumber();

			FileOutputStream outputStream = new FileOutputStream(
					new File("/home/"+ blockNumber.toString()));

			outputStream.write(writeBlockRequestObj.getDataList().get(0).toByteArray());

			writeBlockResponseObj.setStatus(0);
			outputStream.close();
		} catch (InvalidProtocolBufferException e) {
			writeBlockResponseObj.setStatus(1);
		}
		catch (FileNotFoundException e) {
			writeBlockResponseObj.setStatus(1);
		}
		catch (IOException e) {
			writeBlockResponseObj.setStatus(1);
		}
		return writeBlockResponseObj.build().toByteArray();
	}
	
	public static void main( String args[])
	{
		try {
			if(args.length != 2)
			{
				System.out.println("invalid no. of args");
				return;
			}
			Registry registry = LocateRegistry.createRegistry(1099);
			DataNode dataNode = new DataNode(Integer.parseInt(args[0]), args[1]);
			Naming.rebind("DataNode", dataNode);
			System.out.println("DataNode ready");
		} catch (Exception e) {
			System.err.println("DataNode exception: " + e.toString());
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		String nameNodeServerURL = "rmi://" + nameNodeIp + "/NameNode";
		while(true)
		{
			try {
				INameNode nameNodeStub = (INameNode)Naming.lookup(nameNodeServerURL);
				Hdfs.HeartBeatRequest.Builder heartBeatRequestObj = Hdfs.HeartBeatRequest.newBuilder();
				heartBeatRequestObj.setId(dataNodeId);

				nameNodeStub.heartBeat(heartBeatRequestObj.build().toByteArray());
				Hdfs.BlockReportRequest.Builder blockReportRequestObj = Hdfs.BlockReportRequest.newBuilder();
				File blockLocationDirecotry = new File("/home/");
				for(File fileEntry : blockLocationDirecotry.listFiles()) {
					if(fileEntry.isFile()) {
						blockReportRequestObj.addBlockNumbers(Integer.parseInt(fileEntry.getName()));
					}
				}
				blockReportRequestObj.setId(dataNodeId);
				nameNodeStub.blockReport(blockReportRequestObj.build().toByteArray());
				
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (RemoteException e) {
				//e.printStackTrace();
				//Thread.sleep(300);
			} catch (NotBoundException e) {
				//e.printStackTrace();
			}
		}
	}

	public String getNameNodeIp() {
		return nameNodeIp;
	}

	public void setNameNodeIp(String dataNodeIp) {
		this.nameNodeIp = dataNodeIp;
	}

}
