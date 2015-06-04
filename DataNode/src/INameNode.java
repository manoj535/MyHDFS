import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INameNode extends Remote {

	/* OpenFileResponse openFile(OpenFileRequest) */
	/* Method to open a file given file name with read-write flag*/
	byte[] openFile(byte[] openFileRequest) throws RemoteException;;
	
	/* CloseFileResponse closeFile(CloseFileRequest) */
	byte[] closeFile(byte[] closeFileRequest)throws RemoteException;;
	
	/* BlockLocationResponse getBlockLocations(BlockLocationRequest) */
	/* Method to get block locations given an array of block numbers */
	byte[] getBlockLocations(byte[] blockLocationRequest)throws RemoteException;;
	
	/* AssignBlockResponse assignBlock(AssignBlockRequest) */
	/* Method to assign a block which will return the replicated block locations */
	byte[] assignBlock(byte[] assignBlockRequest)throws RemoteException;;
	
	/* ListFilesResponse list(ListFilesRequest) */
	/* List the file names (no directories needed for current implementation */
	byte[] list(byte[] listFilesRequest)throws RemoteException;;
	
	/*
		Datanode <-> Namenode interaction methods
	*/
	
	/* BlockReportResponse blockReport(BlockReportRequest) */
	/* Get the status for blocks */
	byte[] blockReport(byte[] blockReportRequest)throws RemoteException;;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	/* Heartbeat messages between NameNode and DataNode */
	byte[] heartBeat(byte[] heartBeatRequest)throws RemoteException;;
}
