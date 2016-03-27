package tests;

import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.IESelfJoinSinglePredicate;
import iterator.IESelfJoinTwoPredicate;
import iterator.RelSpec;

import java.io.File;
import java.util.Scanner;

public class IESelfJoinTest {

	FldSpec[] proj_list = new FldSpec[2];
	String conditionalOperator = "1";
	String conditionalOperatorTwo = "1";
	String tableName = "";
	int columnNumberForCondition = 0;
	int columnNumberForConditionTwo = 0;
	int SIZEOFTABLE = 0;

	private boolean parseQuery(String filename) {
		File file = new File(filename);

		if (!file.exists()) {
			System.out.println("File at '" + filename + "' not found.");
			return false;
		}
		Scanner scan;

		try {
			scan = new Scanner(file);

			// Selection Values
			String line = scan.nextLine();
			String[] selectionCondition = line.split(" ");

			String[] selection = selectionCondition[0].split(" ");
			String[] projectionValue = selection[0].split("_");
			// System.out.println(projectionValue[1]);

			String[] projectionValue2 = selectionCondition[1].split("_");
			// System.out.println(projectionValue2[1]);

			// Projection list specified what should be in the output tuple.
			proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer),
					Integer.parseInt(projectionValue[1]));
			proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel),
					Integer.parseInt(projectionValue2[1]));

			// Table name
			line = scan.nextLine();
			tableName = line + ".in";
			System.out.println(tableName);

			// Condition
			line = scan.nextLine();
			String[] conditions = line.split(" ");
			conditionalOperator = conditions[1];
			System.out.println(conditionalOperator);

			// Column to be checked in condition
			String[] operandNumber = conditions[0].split("_");
			columnNumberForCondition = Integer.parseInt(operandNumber[1]);

			if (scan.hasNext() == true) {

				scan.nextLine();
				String[] conditions2 = scan.nextLine().split(" ");
				conditionalOperatorTwo = conditions2[1];
				System.out.println(conditionalOperatorTwo);

				// Column to be checked in condition
				String[] operandNumberTwo = conditions2[0].split("_");
				columnNumberForConditionTwo = Integer
						.parseInt(operandNumberTwo[1]);

			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return true;
	}

	private void Query2a_CondExpr(CondExpr[] expr, String conditionalOperator) {

		/*
		 * Q_1 Q_1 Q Q_3 1 Q_3
		 */
		expr[0].next = null;
		if (conditionalOperator.equalsIgnoreCase("1")) {
			expr[0].op = new AttrOperator(AttrOperator.aopLT);
		} else if (conditionalOperator.equalsIgnoreCase("2")) {
			expr[0].op = new AttrOperator(AttrOperator.aopLE);
		} else if (conditionalOperator.equalsIgnoreCase("3")) {
			expr[0].op = new AttrOperator(AttrOperator.aopGE);
		} else {
			expr[0].op = new AttrOperator(AttrOperator.aopGT);
		}

		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				columnNumberForCondition);/* Q1 < Q1 */
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),
				columnNumberForCondition);
	}

	public void Query2a() {

		parseQuery("queries/query_2a.txt");

		// 1 is LE
		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();

		Query2a_CondExpr(outFilter, conditionalOperator);

		AttrType[] Stypes = new AttrType[4];
		Stypes[0] = new AttrType(AttrType.attrInteger);
		Stypes[1] = new AttrType(AttrType.attrInteger);
		Stypes[2] = new AttrType(AttrType.attrInteger);
		Stypes[3] = new AttrType(AttrType.attrInteger);

		// SOS
		short[] Ssizes = new short[1];
		Ssizes[0] = 30; // first elt. is 30

		FldSpec[] Sprojection = new FldSpec[4];
		Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		FileScan am = null;
		DBBuilder builder = new DBBuilder();
		try {
			am = new FileScan(tableName, Stypes, Ssizes, (short) 4, (short) 4,
					Sprojection, null);
		} catch (Exception e) {
			System.err.println("" + e);
		}

		AttrType[] Rtypes = new AttrType[4];
		Rtypes[0] = new AttrType(AttrType.attrInteger);
		Rtypes[1] = new AttrType(AttrType.attrInteger);
		Rtypes[2] = new AttrType(AttrType.attrInteger);
		Rtypes[3] = new AttrType(AttrType.attrInteger);

		short[] Rsizes = new short[1];
		Rsizes[0] = 30;
		FldSpec[] Rprojection = new FldSpec[4];
		Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Rprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		FileScan am2 = null;
		try {
			am2 = new FileScan(tableName, Rtypes, Rsizes, (short) 4, (short) 4,
					Rprojection, null);
		} catch (Exception e) {
			System.err.println("" + e);
		}

		try {
			FileScan am3 = new FileScan(tableName, Rtypes, Rsizes, (short) 4,
					(short) 4, Rprojection, null);

			while (am3.get_next() != null) {
				SIZEOFTABLE++;
			}
		} catch (Exception ex) {
			System.err.println("" + ex);
		}

		// Need to set output type as integer. Since all our output are in
		// integers
		AttrType[] jtype = new AttrType[2];
		jtype[0] = new AttrType(AttrType.attrInteger);
		jtype[1] = new AttrType(AttrType.attrInteger);

		// Based on the algo setting up the tuple order.
		// If operator is <,<= sort in descending
		TupleOrder tupleOrderFor1 = null;
		TupleOrder tupleOrderFor2 = null;
		if (conditionalOperator.equalsIgnoreCase("4")
				|| conditionalOperator.equalsIgnoreCase("3")) {
			tupleOrderFor1 = new TupleOrder(TupleOrder.Ascending);
			tupleOrderFor2 = new TupleOrder(TupleOrder.Descending);
		}

		// else if operator is < and >= sort in ascending
		else {
			tupleOrderFor1 = new TupleOrder(TupleOrder.Descending);
			tupleOrderFor2 = new TupleOrder(TupleOrder.Descending);
		}

		IESelfJoinSinglePredicate ie = null;
		try {

			ie = new IESelfJoinSinglePredicate(Stypes, 4, Ssizes, Rtypes, 4,
					Rsizes, columnNumberForCondition, 10,
					columnNumberForCondition, 10, 10, am, am2, false, false,
					tupleOrderFor1, tupleOrderFor2, outFilter, proj_list,
					2/*
					 * number of output fields
					 */, SIZEOFTABLE,conditionalOperator);
		} catch (Exception e) {
			System.err
					.println("*** join error in IESelfJoinTest constructor ***");
			System.err.println("" + e);
			e.printStackTrace();
		}

		try {
			ie.get_next();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public void Query2b() {

		parseQuery("queries/query_2b.txt");
		
		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		Query2b_CondExpr(outFilter, conditionalOperator,
				conditionalOperatorTwo);

		Tuple t = new Tuple();

		AttrType[] Q1Types = new AttrType[4];
		Q1Types[0] = new AttrType(AttrType.attrInteger);
		Q1Types[1] = new AttrType(AttrType.attrInteger);
		Q1Types[2] = new AttrType(AttrType.attrInteger);
		Q1Types[3] = new AttrType(AttrType.attrInteger);

		// SOS
		short[] Ssizes = new short[1];
		Ssizes[0] = 30; // first elt. is 30

		FldSpec[] Q1Projection = new FldSpec[4];
		Q1Projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Q1Projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Q1Projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Q1Projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		FileScan am = null;
		DBBuilder builder = new DBBuilder();
		try {
			am = new FileScan(tableName, Q1Types, Ssizes, (short) 4, (short) 4,
					Q1Projection, null);
		} catch (Exception e) {
			System.err.println("" + e);
		}

		AttrType[] Q2Types = new AttrType[4];
		Q2Types[0] = new AttrType(AttrType.attrInteger);
		Q2Types[1] = new AttrType(AttrType.attrInteger);
		Q2Types[2] = new AttrType(AttrType.attrInteger);
		Q2Types[3] = new AttrType(AttrType.attrInteger);

		short[] Rsizes = new short[1];
		Rsizes[0] = 30;
		FldSpec[] Q2Projection = new FldSpec[4];
		Q2Projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Q2Projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Q2Projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Q2Projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		FileScan am2 = null;
		try {
			am2 = new FileScan(tableName, Q2Types, Rsizes, (short) 4, (short) 4,
					Q2Projection, null);
		} catch (Exception e) {
			System.err.println("" + e);
		}

		AttrType[] Q3Types = new AttrType[4];
		Q3Types[0] = new AttrType(AttrType.attrInteger);
		Q3Types[1] = new AttrType(AttrType.attrInteger);
		Q3Types[2] = new AttrType(AttrType.attrInteger);
		Q3Types[3] = new AttrType(AttrType.attrInteger);

		short[] Q3sizes = new short[1];
		Q3sizes[0] = 30;

		FldSpec[] Q3Projection = new FldSpec[4];
		Q3Projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Q3Projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Q3Projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Q3Projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		FileScan am3 = null;
		try {
			am3 = new FileScan(tableName, Q3Types, Q3sizes, (short) 4, (short) 4,
					Q3Projection, null);
			while (am3.get_next() != null) {
				SIZEOFTABLE++;
			}
		} catch (Exception e) {
			System.err.println("" + e);
		}

		AttrType[] Q4Types = new AttrType[4];
		Q4Types[0] = new AttrType(AttrType.attrInteger);
		Q4Types[1] = new AttrType(AttrType.attrInteger);
		Q4Types[2] = new AttrType(AttrType.attrInteger);
		Q4Types[3] = new AttrType(AttrType.attrInteger);

		short[] Q4sizes = new short[1];
		Q3sizes[0] = 30;

		FldSpec[] Q4Projection = new FldSpec[4];
		Q4Projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Q4Projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Q4Projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Q4Projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		// Need to set output type as integer. Since all our output are in
		// integers
		AttrType[] jtype = new AttrType[2];
		jtype[0] = new AttrType(AttrType.attrInteger);
		jtype[1] = new AttrType(AttrType.attrInteger);

		// Based on the algo setting up the tuple order.
		// If operator is <,<= sort in descending
		TupleOrder tupleOrderFor1 = null;
		TupleOrder tupleOrderFor2 = null;
		if (conditionalOperator.equalsIgnoreCase("4")
				|| conditionalOperator.equalsIgnoreCase("3")) {
			tupleOrderFor1 = new TupleOrder(TupleOrder.Ascending);
		}

		else {
			tupleOrderFor1 = new TupleOrder(TupleOrder.Descending);
		}

		if (conditionalOperatorTwo.equalsIgnoreCase("4")
				|| conditionalOperatorTwo.equalsIgnoreCase("3")) {
			tupleOrderFor2 = new TupleOrder(TupleOrder.Descending);
		}

		else {
			tupleOrderFor2 = new TupleOrder(TupleOrder.Ascending);
		}

		// Calculating eqOff

		int eqOff = 1;

		if (conditionalOperator.equalsIgnoreCase("2")
				|| conditionalOperator.equalsIgnoreCase("3")) {
			if (conditionalOperatorTwo.equalsIgnoreCase("2")
					|| conditionalOperatorTwo.equalsIgnoreCase("3")) {
				eqOff = 0;
			}

		}

		IESelfJoinTwoPredicate ie = null;
		try {

			ie = new IESelfJoinTwoPredicate(Q1Types, 4, Ssizes, Q2Types, 4,
					Rsizes, 1/* Join condition column */, 10, 2/*
															 * Join condition
															 * column
															 */, 10, 10, am,
					am2, false, false, tupleOrderFor1, tupleOrderFor2,
					outFilter, proj_list, 2/* number of output fields */,
					eqOff, SIZEOFTABLE);
		} catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***");
			System.err.println("" + e);
			e.printStackTrace();
		}

		t = null;
		try {
			ie.get_next();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	private void Query2b_CondExpr(CondExpr[] expr,String conditionalOperatorOne, String conditionalOperatorTwo) {
		  
	  	/*Q_1 Q_1
	    Q
	    Q_3 1 Q_3\
	    AND
	    Q_4 1 Q_4
	    */
	    if(conditionalOperatorOne.equalsIgnoreCase("1")){
	    	expr[0].op    = new AttrOperator(AttrOperator.aopLT);
	    }
	    else if(conditionalOperatorOne.equalsIgnoreCase("2")){
	    	expr[0].op    = new AttrOperator(AttrOperator.aopLE);
	    }
	    else if(conditionalOperatorOne.equalsIgnoreCase("3")){
	    	expr[0].op    = new AttrOperator(AttrOperator.aopGE);
	    }
	    else{
	    	expr[0].op    = new AttrOperator(AttrOperator.aopGT);
	    }
	    
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),columnNumberForCondition); // Changed from 1 to 3 because want to compare the third one
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),columnNumberForCondition);
	    
	    //TODO
	    
	    expr[1].next  = null;
	    if(conditionalOperatorTwo.equalsIgnoreCase("1")){
	    	expr[1].op    = new AttrOperator(AttrOperator.aopLT);
	    }
	    else if(conditionalOperatorTwo.equalsIgnoreCase("2")){
	    	expr[1].op    = new AttrOperator(AttrOperator.aopLE);
	    }
	    else if(conditionalOperatorTwo.equalsIgnoreCase("3")){
	    	expr[1].op    = new AttrOperator(AttrOperator.aopGE);
	    }
	    else{
	    	expr[1].op    = new AttrOperator(AttrOperator.aopGT);
	    }
	    
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrSymbol);
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),columnNumberForConditionTwo); // Changed from 1 to 3 because want to compare the third one
	    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),columnNumberForConditionTwo);
	  }

	public boolean runTests() {
		try {
			Query2a();
			//Query2b();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		boolean sortstatus;
		IESelfJoinTest test = new IESelfJoinTest();
		sortstatus = test.runTests();
		if (sortstatus != true) {
			System.out.println("Error ocurred during join tests");
		} else {
			System.out.println("join tests completed successfully");
		}

	}

}
