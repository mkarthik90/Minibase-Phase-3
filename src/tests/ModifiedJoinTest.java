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
		DBBuilder db = new DBBuilder();

		//Query 1-----------------------------------------------------------------------------------------------------
		CondExpr[] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		
		//Condition 1-------------------------------------------------------------------------------------------------------
		outFilter[0].next  = null;
		outFilter[0].op    = new AttrOperator(AttrOperator.aopGT);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		//outFilter[0].type2 = new AttrType(AttrType.attrInteger);
		outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		//outFilter[0].operand2.integer = 324535;

		//Condition 2--------------------------------------------------------------------------------------------------------
//		outFilter[1].op    = new AttrOperator(AttrOperator.aopLT);
//		outFilter[1].next  = null;
//		outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
//		outFilter[1].type2 = new AttrType(AttrType.attrSymbol);
//		outFilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//		outFilter[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		outFilter[1] = null;
		//----------------------------------------------------------------------------------------------------------------

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
			am  = new FileScan("R.in", Rtypes, Rsizes, 
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
		FldSpec [] proj_list = new FldSpec[4];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		//data type of output---------------------------------------------------------------------------------
		AttrType [] jtype = new AttrType[4];
		jtype[0] = new AttrType (AttrType.attrInteger);
		jtype[1] = new AttrType (AttrType.attrInteger);
		jtype[2] = new AttrType (AttrType.attrInteger);
		jtype[3] = new AttrType (AttrType.attrInteger);
		//-----------------------------------------------------------------------------------------------------

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		
		ModifiedNestedLoopJoin nlj = null;
		
		try{//==================================================================CHANGE BASED ON NUM OUTPUTS==============================
			nlj = new ModifiedNestedLoopJoin(Stypes, 4, Ssizes, Rtypes, 4, Rsizes, 10, am, "S.in", outFilter, null, proj_list, 4, false);
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
