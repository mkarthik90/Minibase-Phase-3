package tests;
import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

//Define the Sailor schema
class TableEntry {
	public int rel1;
	public int rel2;
	public int rel3;
	public int rel4;

	public TableEntry (int _r1, int _r2, int _r3, int _r4) {
		rel1 = _r1;
		rel2 = _r2;
		rel3 = _r3;
		rel4 = _r4;
	}
}

public class DBBuilder implements GlobalConst{
	private ArrayList<TableEntry> RTable;
	private ArrayList<TableEntry> QTable;
	private ArrayList<TableEntry> STable;

	public static void main(String args[])
	{
		DBBuilder db = new DBBuilder();
	}
	
	public DBBuilder(){
		RTable = new ArrayList<TableEntry>();
		QTable = new ArrayList<TableEntry>();
		STable = new ArrayList<TableEntry>();
		String line;
		String parts[];
		int r1;
		int r2;
		int r3;
		int r4;
		TableEntry te;
		//R----------------------------------------------------------------------
		try{
			FileReader fr = new FileReader("R.txt");
			BufferedReader bufferedReader = new BufferedReader(fr);
			line = bufferedReader.readLine();
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				te = new TableEntry(r1,r2,r3,r4);
				RTable.add(te);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}
		//Q------------------------------------------------------------
		try{
			FileReader fr = new FileReader("Q.txt");
			BufferedReader bufferedReader = new BufferedReader(fr);
			line = bufferedReader.readLine();
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				te = new TableEntry(r1,r2,r3,r4);
				QTable.add(te);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}
		//S-------------------------------------------------------------------
		try{
			FileReader fr = new FileReader("S.txt");
			BufferedReader bufferedReader = new BufferedReader(fr);
			line = bufferedReader.readLine();
			while((line = bufferedReader.readLine()) != null) 
			{
				parts = line.split(",");
				r1 = Integer.parseInt(parts[0]);
				r2 = Integer.parseInt(parts[1]);
				r3 = Integer.parseInt(parts[2]);
				r4 = Integer.parseInt(parts[3]);
				te = new TableEntry(r1,r2,r3,r4);
				STable.add(te);
			}
			bufferedReader.close();
		}
		catch(FileNotFoundException ex)
		{
			System.out.println("Unable to open file ");
		} catch (IOException e) 
		{
			System.out.println("Error");
		}


		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		}
		catch (IOException e) {
			System.err.println (""+e);
		}
		SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

		// creating the table relation
		AttrType [] Stypes = new AttrType[4];
		Stypes[0] = new AttrType (AttrType.attrInteger);
		Stypes[1] = new AttrType (AttrType.attrInteger);
		Stypes[2] = new AttrType (AttrType.attrInteger);
		Stypes[3] = new AttrType (AttrType.attrInteger);

		short [] Ssizes = new short [1];
		Ssizes[0] = 30; //first elt. is 30

		Tuple t = new Tuple();

		try {
			t.setHdr((short) 4,Stypes, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		int size = t.size();

		// inserting the tuple into file "R"===============================
		RID             rid;
		Heapfile        f = null;
		try {
			f = new Heapfile("R.in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<RTable.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry)RTable.get(i)).rel1);
				t.setIntFld(2, ((TableEntry)RTable.get(i)).rel2);
				t.setIntFld(3, ((TableEntry)RTable.get(i)).rel3);
				t.setIntFld(4, ((TableEntry)RTable.get(i)).rel4);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
		//Q========================================================================
		/*f = null;
		try {
			f = new Heapfile("Q.in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<QTable.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry)QTable.get(i)).rel1);
				t.setIntFld(2, ((TableEntry)QTable.get(i)).rel2);
				t.setIntFld(3, ((TableEntry)QTable.get(i)).rel3);
				t.setIntFld(4, ((TableEntry)QTable.get(i)).rel4);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}*/
		//S=====================================================================
		f = null;
		try {
			f = new Heapfile("S.in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}

		for (int i=0; i<STable.size(); i++) {
			try {
				t.setIntFld(1, ((TableEntry)STable.get(i)).rel1);
				t.setIntFld(2, ((TableEntry)STable.get(i)).rel2);
				t.setIntFld(3, ((TableEntry)STable.get(i)).rel3);
				t.setIntFld(4, ((TableEntry)STable.get(i)).rel4);
			}
			catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}      
		}
	}

}
