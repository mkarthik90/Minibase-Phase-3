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
public class Pt2DJoinTest 
{

	public static void main(String args[])
	{
		DBBuilder db = new DBBuilder();

		//Query 1-----------------------------------------------------------------------------------------------------
		CondExpr[] testCond1 = new CondExpr[2];
		CondExpr[] testCond2 = new CondExpr[2];
		testCond1[0] = new CondExpr();
		testCond1[1] = new CondExpr();
		testCond2[0] = new CondExpr();
		testCond2[1] = new CondExpr();


		//Test Condition 1============================================================================================================
		//Condition 1-------------------------------------------------------------------------------------------------------
		testCond1[0].next  = null;
		testCond1[0].op    = new AttrOperator(AttrOperator.aopLE);
		testCond1[0].type1 = new AttrType(AttrType.attrSymbol);
		testCond1[0].type2 = new AttrType(AttrType.attrSymbol);
		//testCond1[0].type2 = new AttrType(AttrType.attrInteger);
		testCond1[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), 1);
		testCond1[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 1);
		//testCond1[0].operand2.integer = 324535;

		//Condition 2--------------------------------------------------------------------------------------------------------
//		testCond1[1].op    = new AttrOperator(AttrOperator.aopGT);
//		testCond1[1].next  = null;
//		testCond1[1].type1 = new AttrType(AttrType.attrSymbol);
//		testCond1[1].type2 = new AttrType(AttrType.attrSymbol);
//		testCond1[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), 2);
//		testCond1[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 2);
		testCond1[1] = null;
		//----------------------------------------------------------------------------------------------------------------

		//Test Condition 2============================================================================================================
		//Condition 1-------------------------------------------------------------------------------------------------------
		testCond2[0].next  = null;
		testCond2[0].op    = new AttrOperator(AttrOperator.aopLT);
		testCond2[0].type1 = new AttrType(AttrType.attrSymbol);
		testCond2[0].type2 = new AttrType(AttrType.attrSymbol);
		testCond2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), 4);
		testCond2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 4);

		//Condition 2--------------------------------------------------------------------------------------------------------
		testCond2[1].op    = new AttrOperator(AttrOperator.aopGE);
		testCond2[1].next  = null;
		testCond2[1].type1 = new AttrType(AttrType.attrSymbol);
		testCond2[1].type2 = new AttrType(AttrType.attrSymbol);
		testCond2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), 3);
		testCond2[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 3);
		//----------------------------------------------------------------------------------------------------------------
		//============================================================================================================================

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
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
		//proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		//proj_list[3] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		//data type of output---------------------------------------------------------------------------------
		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrInteger);
		jtype[1] = new AttrType (AttrType.attrInteger);
		//jtype[2] = new AttrType (AttrType.attrInteger);
		//jtype[3] = new AttrType (AttrType.attrInteger);
		//-----------------------------------------------------------------------------------------------------
		
		Pt2DIEJoin nlj = null;
		
		try{//==================================================================CHANGE BASED ON COND==============================
			nlj = new Pt2DIEJoin("R", Rprojection, Rtypes, 4, "S", Sprojection, Stypes, 4, proj_list, jtype, 2, testCond2, 10);
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
		
		System.out.println ("\nfin"); 
	}
}
