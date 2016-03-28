package tests;
import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.ByteBuffer;

import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

//not designed to handle printing from table not involved in join
public class ModifiedJoinTest 
{

	public static void main(String args[])
	{
		
		HelpBundle hb = new HelpBundle();
        hb.parseQuery("queries/query_1b.txt");
        if(hb.get_table2().equals("")||hb.get_table1().equals(hb.get_table2()))
        {
        	DBBuilder.build(hb.get_table1());
        }
        else
        {
        	DBBuilder.build(hb.get_table1(), hb.get_table2());
        }

		Tuple t = new Tuple();

		AttrType [] Rtypes = new AttrType[4];
		Rtypes[0] = new AttrType (AttrType.attrInteger);
		Rtypes[1] = new AttrType (AttrType.attrInteger);
		Rtypes[2] = new AttrType (AttrType.attrInteger);
		Rtypes[3] = new AttrType (AttrType.attrInteger);

		short [] Rsizes = new short[1];
		Rsizes[0] = 30;

		FldSpec [] Rprojection = new FldSpec[4];
		Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Rprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		CondExpr [] selects = new CondExpr [1];
		selects = null;


		FileScan am = null;
		try {
			am  = new FileScan(hb.get_table1() + ".in", Rtypes, Rsizes, 
					(short)4, (short)4,
					Rprojection, null);
		}
		catch (Exception e) {
			System.err.println (""+e);
		}
		
		
		AttrType [] Stypes = new AttrType[4];
		Stypes[0] = new AttrType (AttrType.attrInteger);
		Stypes[1] = new AttrType (AttrType.attrInteger);
		Stypes[2] = new AttrType (AttrType.attrInteger);
		Stypes[3] = new AttrType (AttrType.attrInteger);

		short [] Ssizes = new short[1];
		Ssizes[0] = 30;

		FldSpec [] Sprojection = new FldSpec[4];
		Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);


		//Changes output--------------------------------------------------------------------------------------
		FldSpec [] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		//proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		//proj_list[3] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		//data type of output---------------------------------------------------------------------------------
		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrInteger);
		jtype[1] = new AttrType (AttrType.attrInteger);
		//jtype[2] = new AttrType (AttrType.attrInteger);
		//jtype[3] = new AttrType (AttrType.attrInteger);
		//-----------------------------------------------------------------------------------------------------

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		
		NestedLoopsJoins nlj = null;
		
		try{//==================================================================CHANGE BASED ON NUM OUTPUTS==============================
			nlj = new NestedLoopsJoins(Stypes, 4, Ssizes, Rtypes, 4, Rsizes, 10, am, hb.get_table2() + ".in",
					hb.get_cond(), null, hb.get_fs(), hb.get_out_size());
		}
		catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***"); 
			System.err.println (""+e);
			e.printStackTrace();
		}


		t = null;

		try {
			while ((t = nlj.get_next()) != null) {
				t.print(jtype);
				
			}
		}
		catch(TupleUtilsException e)
		{
			
		}
		catch (Exception e) {
			System.err.println (""+e);
			e.printStackTrace();
		}

		try {
			nlj.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println ("\nfin\n"); 
	}
}
