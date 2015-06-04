
import generated.Hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.Naming;
import java.util.Random;
import java.util.Scanner;

import com.google.protobuf.ByteString;

public class Client {
		
	public boolean hdfsWriteFile(String nameNodeIp, String fileName)
	{
		try {
			String nameNodeServerURL = "rmi://" + nameNodeIp+ "/NameNode";
			INameNode nameNodeStub = (INameNode)Naming.lookup(nameNodeServerURL);

			//OpenFileRequest
			int openFileHandle = 0;
			Hdfs.OpenFileRequest.Builder openFileRequestObj = Hdfs.OpenFileRequest.newBuilder();

			String tempFileName = fileName.substring(fileName.lastIndexOf("/")+1);
			openFileRequestObj.setFileName(tempFileName);
			openFileRequestObj.setForRead(false);

			byte[] openFileResponse = nameNodeStub.openFile(openFileRequestObj.build().toByteArray());
			Hdfs.OpenFileResponse openFileResponseObj = 
					Hdfs.OpenFileResponse.parseFrom(openFileResponse);
			if(openFileResponseObj.getStatus() != 0)
			{
				System.out.println("OpenFileRequest Failed");
				return false;
			}
			openFileHandle = openFileResponseObj.getHandle();

			FileInputStream	inputFileStream = new FileInputStream(fileName);
			
			final int blockSize = 16*1024*1024; 
			byte[] block = new byte[blockSize];
			int value = 0;
			byte[] assignBlockResponseArray;
			//byte[] writeResponseArray;
			while((value=inputFileStream.read(block,0,blockSize)) != -1)
			{
				Hdfs.AssignBlockRequest.Builder assignBlockRequestObj = 
						Hdfs.AssignBlockRequest.newBuilder();
				assignBlockRequestObj.setHandle(openFileHandle);
				assignBlockResponseArray = nameNodeStub.assignBlock(
						assignBlockRequestObj.build().toByteArray());
				Hdfs.AssignBlockResponse assignBlockResponseObj = 
						Hdfs.AssignBlockResponse.parseFrom(assignBlockResponseArray);
				if(assignBlockResponseObj.getStatus() == 0)
				{
					Hdfs.BlockLocations blockLocationsObj = assignBlockResponseObj.getNewBlock();
					for(Hdfs.DataNodeLocation dataNode : blockLocationsObj.getLocationsList())
					{
						Hdfs.WriteBlockRequest.Builder writeBlockRequestObj = 
								Hdfs.WriteBlockRequest.newBuilder();
						String ip = dataNode.getIp().replace("\"", "");
						String dataNodeServerURL = "rmi://" + ip + "/DataNode";
						IDataNode dataNodeStub = (IDataNode)Naming.lookup(dataNodeServerURL);

						writeBlockRequestObj.addData(ByteString.copyFrom(block,0,value));
						writeBlockRequestObj.setBlockInfo(blockLocationsObj);
						dataNodeStub.writeBlock(writeBlockRequestObj.build().toByteArray());
					}
				} // if

			} // while

			inputFileStream.close();
			Hdfs.CloseFileRequest.Builder closeFileRequestObj = Hdfs.CloseFileRequest.newBuilder();
			closeFileRequestObj.setHandle(openFileHandle);
			nameNodeStub.closeFile(closeFileRequestObj.build().toByteArray());
		} catch(Exception e) {
			System.err.println("Client exception: " + e.toString());
			return false;
		}
		return true;
	} // hdfsWriteFile
	
	
	public boolean hdfsReadFile(String nameNodeIp, String fileName)
	{
		try {
			String nameNodeServerURL = "rmi://" + nameNodeIp+ "/NameNode";
			INameNode nameNodeStub = (INameNode)Naming.lookup(nameNodeServerURL);

			//OpenFileRequest
			int openFileHandle = 0;
			Hdfs.OpenFileRequest.Builder openFileRequestObj = Hdfs.OpenFileRequest.newBuilder();

			fileName = fileName.substring(fileName.lastIndexOf("/")+1);
			openFileRequestObj.setFileName(fileName);
			openFileRequestObj.setForRead(true);

			byte[] openFileResponseResponseArray = nameNodeStub.openFile(openFileRequestObj.build().toByteArray());
			Hdfs.OpenFileResponse openFileResponseObj = 
					Hdfs.OpenFileResponse.parseFrom(openFileResponseResponseArray);
			if(openFileResponseObj.getStatus() != 0)
			{
				System.out.println("OpenFileRequest Failed");
				return false;
			}
			openFileHandle = openFileResponseObj.getHandle();
            Hdfs.BlockLocationRequest.Builder blockLocationRequestObj = Hdfs.BlockLocationRequest.newBuilder();
            for(int blockNumber : openFileResponseObj.getBlockNumsList())
            {
            	blockLocationRequestObj.addBlockNums(blockNumber);
            }
            byte[] blockLocationResponseArray = nameNodeStub.getBlockLocations(
            										blockLocationRequestObj.build().toByteArray());
            Hdfs.BlockLocationResponse blockLocationResponseObj = 
            						Hdfs.BlockLocationResponse.parseFrom(blockLocationResponseArray);
            FileOutputStream outputStream = new FileOutputStream (new File("/tmp/outputFile"));
            byte[] readBlockResponseArray;
            int dataNodeNumber = 0;
            for (Hdfs.BlockLocations blockLocation : blockLocationResponseObj.getBlockLocationsList())
            {
            	// get the datanode replica to take the block from
            	if(blockLocation.getLocationsList().size() == 1) {
            		dataNodeNumber = 0;
            	}
            	else if(blockLocation.getLocationsList().size() == 2) {
            		dataNodeNumber = randInt(0,1);
            	}
            	else {
            		System.out.println("DataLocation empty for block: "+ blockLocation.getBlockNumber());
            		outputStream.close();
            		return false;
            	}
            		
            	Hdfs.DataNodeLocation dataNode = blockLocation.getLocations(dataNodeNumber);
            	Hdfs.ReadBlockRequest.Builder readBlockRequestObj = Hdfs.ReadBlockRequest.newBuilder();
            	String ip = dataNode.getIp().replace("\"", "");
            	String dataNodeServerURL = "rmi://" + ip + "/DataNode";
            	IDataNode dataNodeStub = (IDataNode)Naming.lookup(dataNodeServerURL);

            	readBlockRequestObj.setBlockNumber(blockLocation.getBlockNumber());
            	readBlockResponseArray = dataNodeStub.readBlock(readBlockRequestObj.build().toByteArray());
            	Hdfs.ReadBlockResponse readBlockResponseObj = 
            							Hdfs.ReadBlockResponse.parseFrom(readBlockResponseArray);
            	outputStream.write(readBlockResponseObj.getData(0).toByteArray());
            }
			outputStream.close();
			
		} catch(Exception e) {
			System.err.println("Client exception: " + e.toString());
			return false;
		}
		return true;
	}
	
	private int randInt(int min, int max) {
	    Random rand = new Random();
	    int randomNum = rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	
	public static void main(String[] args) {

		if(args.length != 1)
		{
			System.out.println("Invalid No. of arguments,NameNode IP not given");
			return;
		}
		Client clientObj = new Client();
		String fileName;
		Scanner in = new Scanner(System.in);
		int choice;
		while(true)
		{
			System.out.println("1.ReadFile");
			System.out.println("2.WriteFile");
			System.out.println("3.Exit");
			choice = Integer.parseInt(in.nextLine());
			if(choice == 1 ) {
				System.out.println("Enter file name to read:");
				fileName = in.nextLine();
				if(!clientObj.hdfsReadFile(args[0], fileName)) {
					System.out.println("hdfsReadFile Failed");
				}
			}
			else if(choice == 2) {
				System.out.println("Enter file name to write:");
				fileName = in.nextLine();
				if(!clientObj.hdfsWriteFile(args[0], fileName)) {
					System.out.println("hdfsWriteFile Failed");				
				}
			}
			else {
				break;
			}
			
		}
		
		
	}
}
