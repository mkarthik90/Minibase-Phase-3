package tests;

import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.TupleOrder;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.IESelfJoinSinglePredicate;
import iterator.IESelfJoinTwoPredicate;
import iterator.RelSpec;

class SelfJoinDriver implements GlobalConst {

	private boolean OK = true;
	private boolean FAIL = false;

	
	public SelfJoinDriver(){
		DBBuilder builder = new DBBuilder();
	}
	
	private void Query7_CondExpr(CondExpr[] expr, String conditionalOperator) {

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
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
	}

	private void Query8_CondExpr(CondExpr[] expr,
			String conditionalOperatorOne, String conditionalOperatorTwo) {

		if (conditionalOperatorOne.equalsIgnoreCase("1")) {
			expr[0].op = new AttrOperator(AttrOperator.aopLT);
		} else if (conditionalOperatorOne.equalsIgnoreCase("2")) {
			expr[0].op = new AttrOperator(AttrOperator.aopLE);
		} else if (conditionalOperatorOne.equalsIgnoreCase("3")) {
			expr[0].op = new AttrOperator(AttrOperator.aopGE);
		} else {
			expr[0].op = new AttrOperator(AttrOperator.aopGT);
		}

		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1); 
		
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

		// TODO
		expr[1].next = null;
		if (conditionalOperatorTwo.equalsIgnoreCase("1")) {
			expr[1].op = new AttrOperator(AttrOperator.aopLT);
		} else if (conditionalOperatorTwo.equalsIgnoreCase("2")) {
			expr[1].op = new AttrOperator(AttrOperator.aopLE);
		} else if (conditionalOperatorTwo.equalsIgnoreCase("3")) {
			expr[1].op = new AttrOperator(AttrOperator.aopGE);
		} else {
			expr[1].op = new AttrOperator(AttrOperator.aopGT);
		}

		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrSymbol);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2); 
		expr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

	}

	public void Query7() {
		boolean status = OK;

		String conditionalOperator = "2";

		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();

		Query7_CondExpr(outFilter, conditionalOperator);

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
		try {
			am = new FileScan("R.in", Stypes, Ssizes, (short) 4, (short) 4,
					Sprojection, null);

		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
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
			am2 = new FileScan("R.in", Rtypes, Rsizes, (short) 4, (short) 4,
					Rprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		if (status != OK) {
			System.err.println("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}


		// Projection list specified what should be in the output tuple.
		// So adding 1 -> column of outer and 1 -> column of inner for
		// displaying output tuple
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

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
					Rsizes, 1, 10, 1, 10, 10, am, am2, false, false,
					tupleOrderFor1, tupleOrderFor2, outFilter, proj_list, 2/*
																			 * number
																			 * of
																			 * output
																			 * fields
																			 */);
		} catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***");
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}

		try {
			ie.get_next();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error in get next tuple ");
			Runtime.getRuntime().exit(1);
		}

		System.out.println("\n");
		if (status != OK) {
			// bail out
			System.err.println("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}
	}

	public void Query8() {

		String conditionalOperatorOne = "1";
		String conditionalOperatorTwo = "3";

		System.out
				.print("**********************TASK 2B strating *********************\n");
		boolean status = OK;

		System.out
				.println("Joining column1 with column1 with self join with less than equal to");

		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		Query8_CondExpr(outFilter, conditionalOperatorOne,
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
		try {
			am = new FileScan("R.in", Q1Types, Ssizes, (short) 4, (short) 4,
					Q1Projection, null);
		} catch (Exception e) {
			status = FAIL;
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
			am2 = new FileScan("R.in", Q2Types, Rsizes, (short) 4, (short) 4,
					Q2Projection, null);
		} catch (Exception e) {
			status = FAIL;
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

		FileScan am4 = null;
		try {
			am4 = new FileScan("R.in", Q4Types, Q4sizes, (short) 4, (short) 4,
					Q4Projection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		// Projection list specified what should be in the output tuple.
		// So adding 1 -> column of outer and 1 -> column of inner for
		// displaying output tuple
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

		// Need to set output type as integer. Since all our output are in
		// integers
		AttrType[] jtype = new AttrType[2];
		jtype[0] = new AttrType(AttrType.attrInteger);
		jtype[1] = new AttrType(AttrType.attrInteger);

		// Based on the algo setting up the tuple order.
		// If operator is <,<= sort in descending
		TupleOrder tupleOrderFor1 = null;
		TupleOrder tupleOrderFor2 = null;
		if (conditionalOperatorOne.equalsIgnoreCase("4")
				|| conditionalOperatorOne.equalsIgnoreCase("3")) {
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

		if (conditionalOperatorOne.equalsIgnoreCase("2")
				|| conditionalOperatorOne.equalsIgnoreCase("3")) {
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
					eqOff);
		} catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***");
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}

		t = null;
		try {
			ie.get_next();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error in get next tuple ");
			Runtime.getRuntime().exit(1);
		}

		System.out.println("\n");
		if (status != OK) {
			// bail out
			System.err.println("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}
	}

	public void runTests() {
		
		Query7();
		Query8();
	}
}

public class SelfJoinTests {
	public static void main(String argv[]) {
		SelfJoinDriver joinDriver = new SelfJoinDriver();
		joinDriver.runTests();
	}
}
