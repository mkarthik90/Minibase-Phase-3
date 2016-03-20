package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.file.NotDirectoryException;

import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

/**
   Here is the implementation for the tests. There are N tests performed.
   We start off by showing that each operator works on its own.
   Then more complicated trees are constructed.
   As a nice feature, we allow the user to specify a selection condition.
   We also allow the user to hardwire trees together.
 */

//Define the Boat schema
class Q {
	public int    bid;
	public String bname;
	public String color;

	public Q (int _bid, String _bname, String _color) {
		bid   = _bid;
		bname = _bname;
		color = _color;
	}
}

//Define the Reserves schema
class R {
	public int    sid;
	public int    bid;
	public String date;

	public R (int _sid, int _bid, String _date) {
		sid  = _sid;
		bid  = _bid;
		date = _date;
	}
}

//Define the Sailor schema
class S {
	public int    sid;
	public String sname;
	public int    rating;
	public double age;

	public S (int _sid, String _sname, int _rating,double _age) {
		sid    = _sid;
		sname  = _sname;
		rating = _rating;
		age    = _age;
	}
}

class ModifiedJoinsRelation{
	public String name;
	public int col;
	public RelSpec relSpec;

	public ModifiedJoinsRelation(String _name, int _relSpec){
		name = _name.toUpperCase();
		relSpec = new RelSpec(_relSpec);
		col = -1;
	}

	public ModifiedJoinsRelation(String _name, int _relSpec, int _col){
		name = _name.toUpperCase();
		relSpec = new RelSpec(_relSpec);
		col = _col;
	}

	@Override
	public String toString(){
		return name + "_" + col;
	}

	@Override
	public boolean equals(Object o){
		if (o == null || !ModifiedJoinsRelation.class.isAssignableFrom(o.getClass())) {
			return false;
		}

		ModifiedJoinsRelation other = (ModifiedJoinsRelation) o;

		return this.name.equals(other.name) && this.col == other.col;
	}
}

class ModifiedJoinsQuery{
	private static final AttrType[] _ATTR_TYPES = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
			new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};

	private boolean _prevOrConj;
	private List<CondExpr> _filter;
	private List<ModifiedJoinsRelation> _relations;

	public ModifiedJoinsQuery(){
		_prevOrConj = false;
		_filter = new ArrayList<CondExpr>();
		_relations = new ArrayList<ModifiedJoinsRelation>();
	}

	private void _addToRelations(ModifiedJoinsRelation rel){
		if(!_relations.contains(rel)){
			_relations.add(rel);
		}
	}

	public void runQuery(){
		FileScan scan1 = null, scan2 = null;
		Map<String, FileScan> scans = new HashMap<String, FileScan>();
		List<FldSpec> projection1 = new ArrayList<FldSpec>(), projection2 = new ArrayList<FldSpec>();

		ModifiedJoinsRelation rel1 = _relations.get(0), rel2 = _relations.get(1);

		for(int i = 1; i < 5; i++){
			projection1.add(new FldSpec(new RelSpec(RelSpec.outer), i));
			projection2.add(new FldSpec(new RelSpec(RelSpec.outer), i));
		}

		try {
			scan1 = new FileScan(rel1.name + ".in", _ATTR_TYPES, null, 
					(short)4, (short)4, projection1.toArray(new FldSpec[projection1.size()]), null);

			scan2 = new FileScan(rel2.name + ".in", _ATTR_TYPES, null, 
					(short)4, (short)4, projection2.toArray(new FldSpec[projection2.size()]), null);
		} catch (FileScanException | TupleUtilsException | InvalidRelation | IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		FldSpec [] proj_list = new FldSpec[4];
		proj_list[0] = new FldSpec(rel1.relSpec, 1);
		proj_list[1] = new FldSpec(rel2.relSpec, 1);
		proj_list[2] = new FldSpec(rel1.relSpec, 1);
		proj_list[3] = new FldSpec(rel1.relSpec, 1);


		AttrType [] jtype = new AttrType[4];
		jtype[0] = new AttrType (AttrType.attrInteger);
		jtype[1] = new AttrType (AttrType.attrInteger);
		jtype[2] = new AttrType (AttrType.attrInteger);
		jtype[3] = new AttrType (AttrType.attrInteger);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		SortMerge sm = null;

		try {
			sm = new SortMerge(_ATTR_TYPES, 4, null,
					_ATTR_TYPES, 4, null,
					rel1.col, 4, 
					rel2.col, 4, 
					10,
					scan1, scan2, 
					false, false, ascending,
					getFilter(), proj_list, 4);

			Tuple t = null;

			while((t = sm.get_next()) != null){
				System.out.println("TUPLES!!!");
				t.print(jtype);
			}
		}
		catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***"); 
			System.err.println (""+e);
			e.printStackTrace();
		}
	}

	public void addExpression(ModifiedJoinsRelation rel1, ModifiedJoinsRelation rel2, AttrOperator op, String conj){
		_addToRelations(rel1);
		_addToRelations(rel2);

		CondExpr expr = new CondExpr();

		if(_prevOrConj){
			expr.next  = new CondExpr();
			_prevOrConj = false;
		}

		switch(conj){
		case "OR":
			_prevOrConj = true;

			if(expr.next == null){
				expr.next  = new CondExpr();
			}
			break;
		default:
			expr.next  = null;
		}

		expr.op    = op;
		expr.type1 = new AttrType(AttrType.attrSymbol);
		expr.type2 = new AttrType(AttrType.attrSymbol);
		expr.operand1.symbol = new FldSpec (rel1.relSpec, rel1.col);
		expr.operand2.symbol = new FldSpec (rel2.relSpec, rel2.col);

		_filter.add(expr);
	}

	public List<ModifiedJoinsRelation> getRelations(){
		return _relations;
	}

	public CondExpr[] getFilter(){
		return _filter.toArray(new CondExpr[_filter.size()]);
	}
}

class ModifiedJoinsDriver implements GlobalConst {
	private static final String _QUERY_DIR = "src/tests/modified_join_test_queries";
	private boolean OK = true;
	private boolean FAIL = false;

	public ModifiedJoinsDriver() {
		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";
		SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
	}

	public boolean runTests() throws NotDirectoryException {
		File queryDir = new File(_QUERY_DIR);

		if(!queryDir.isDirectory()){
			throw new NotDirectoryException(_QUERY_DIR);
		}

		for(File queryFile : queryDir.listFiles()){
			ModifiedJoinsQuery query = _parseQuery(queryFile);

			query.runQuery();
		}
		System.exit(0);

		return true;
	}

	private static ModifiedJoinsQuery _parseQuery(File queryFile){
		ModifiedJoinsQuery query = new ModifiedJoinsQuery();
		Scanner scan;
		String line, conj = "";
		ModifiedJoinsRelation rel1 = null, rel2 = null;
		AttrOperator op = null;
		String[] parts;

		try {
			scan = new Scanner(queryFile);

			while(scan.hasNextLine()){
				line = scan.nextLine();
				parts = line.split(" ");

				switch(parts.length){
				case 1:
					conj = parts[0].toUpperCase();
					switch(conj){
					case "AND":
					case "OR":
						break;
					default:
						if((rel1 != null && rel2 != null) && !(rel1.name.equals(conj) || rel2.name.equals(conj))){
							System.out.println("What on Earth has happened in ModifiedJoinTest._parseQuery 3?");
							System.out.println(queryFile.getName());
							System.exit(1);
						}
					}

					break;
				case 2:
					//Redundant line
					break;
				case 3:
					rel1 = _parseRelation(parts[0], RelSpec.outer);
					rel2 = _parseRelation(parts[2], RelSpec.innerRel);

					switch(Integer.parseInt(parts[1])){
					case 1:
						op = new AttrOperator(AttrOperator.aopLT);
						break;
					case 2:
						op = new AttrOperator(AttrOperator.aopLE);
						break;
					case 3:
						op = new AttrOperator(AttrOperator.aopGE);
						break;
					case 4:
						op = new AttrOperator(AttrOperator.aopGT);
						break;
					default:
						System.out.println("What on Earth has happened in ModifiedJoinTest._parseQuery 1?");
						System.exit(1);
					}

					System.out.println(rel1 + " " + op + " " + rel2);

					query.addExpression(rel1, rel2, op, conj);
					break;
				default:
					System.out.println("What on Earth has happened in ModifiedJoinTest._parseQuery 2?");
					System.exit(1);
				}
			}

			scan.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		return query;
	}

	private static ModifiedJoinsRelation _parseRelation(String relation, int relSpec){
		String[] parts = relation.split("_");
		ModifiedJoinsRelation retVal = null;

		switch(parts.length){
		case 1:
			retVal = new ModifiedJoinsRelation(parts[0], relSpec);
			break;
		case 2:
			retVal = new ModifiedJoinsRelation(parts[0], relSpec, Integer.parseInt(parts[1]));
			break;
		default:
			System.out.println("What on Earth has happened in ModifiedJoinTest._parseRelation 1?");
			System.exit(1);
		}

		return retVal;
	}

	private void Query1_CondExpr(CondExpr[] expr) {

		System.out.print ("Query: Find the names of sailors who have reserved "
				+ "boat number 1.\n"
				+ "       and print out the date of reservation.\n\n"
				+ "  SELECT S.sname, R.date\n"
				+ "  FROM   Sailors S, Reserves R\n"
				+ "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[1].next  = null;
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrInteger);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		expr[1].operand2.integer = 1;

		expr[2] = null;
	}

	private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

		/*
		  ("Query: Find the names of sailors who have reserved "

				+ "a red boat\n"
				+ "       and return them in alphabetical order.\n\n"
				+ "  SELECT   S.sname\n"
				+ "  FROM     Sailors S, Boats B, Reserves R\n"
				+ "  WHERE    S.sid = R.sid AND R.bid = B.bid AND B.color = 'red'\n"
				+ "  ORDER BY S.sname\n"
		 */

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr[1] = null;

		expr2[0].next  = null;
		expr2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);   
		expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
		expr2[1].next = null;
		expr2[1].type1 = new AttrType(AttrType.attrSymbol);
		expr2[1].type2 = new AttrType(AttrType.attrString);
		expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
		expr2[1].operand2.string = "red";

		expr2[2] = null;
	}

	private void Query3_CondExpr(CondExpr[] expr) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
		expr[1] = null;
	}

	private CondExpr[] Query5_CondExpr() {
		CondExpr [] expr2 = new CondExpr[3];
		expr2[0] = new CondExpr();

		System.out.print 
		("Query: Find the names of old sailors or sailors with "
				+ "a rating less\n       than 7, who have reserved a boat, "
				+ "(perhaps to increase the\n       amount they have to "
				+ "pay to make a reservation).\n\n"
				+ "  SELECT S.sname, S.rating, S.age\n"
				+ "  FROM   Sailors S, Reserves R\n"
				+ "  WHERE  S.sid = R.sid and (S.age > 40 || S.rating < 7)\n\n"
				+ "(Tests FileScan, Multiple Selection, Projection, "
				+ "and Sort-Merge Join.)\n\n");

		expr2[0].next  = null;
		expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr2[1] = new CondExpr();
		expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
		expr2[1].next = null;
		expr2[1].type1 = new AttrType(AttrType.attrSymbol);

		expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
		expr2[1].type2 = new AttrType(AttrType.attrReal);
		expr2[1].operand2.real = (float)40.0;


		expr2[1].next = new CondExpr();
		expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
		expr2[1].next.next = null;
		expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
		expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
		expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
		expr2[1].next.operand2.integer = 7;

		expr2[2] = null;
		return expr2;
	}

	private void Query6_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);

		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);

		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr[1].next  = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopGT);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);

		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
		expr[1].type2 = new AttrType(AttrType.attrInteger);
		expr[1].operand2.integer = 7;

		expr[2] = null;

		expr2[0].next  = null;
		expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr2[1].next = null;
		expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
		expr2[1].type1 = new AttrType(AttrType.attrSymbol);

		expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
		expr2[1].type2 = new AttrType(AttrType.attrString);
		expr2[1].operand2.string = "red";

		expr2[2] = null;
	}

	public void Query1() {

		System.out.print("**********************Query1 strating *********************\n");
		boolean status = OK;

		// Sailors, Boats, Reserves Queries.
		System.out.print ("Query: Find the names of sailors who have reserved "
				+ "boat number 1.\n"
				+ "       and print out the date of reservation.\n\n"
				+ "  SELECT S.sname, R.date\n"
				+ "  FROM   Sailors S, Reserves R\n"
				+ "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");

		System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");

		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();

		Query1_CondExpr(outFilter);

		Tuple t = new Tuple();

		//Column Types
		AttrType [] Stypes = new AttrType[4];
		Stypes[0] = new AttrType (AttrType.attrInteger);
		Stypes[1] = new AttrType (AttrType.attrString);
		Stypes[2] = new AttrType (AttrType.attrInteger);
		Stypes[3] = new AttrType (AttrType.attrReal);

		//SOS
		short [] Ssizes = new short[1];
		Ssizes[0] = 30; //first elt. is 30

		FldSpec [] Sprojection = new FldSpec[4];
		//Outer or Inner, offset of field (?)
		Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		CondExpr [] selects = new CondExpr [1];
		selects = null;

		FileScan am = null;
		try {
			am  = new FileScan("sailors.in", Stypes, Ssizes, 
					(short)4, (short)4,
					Sprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}

		AttrType [] Rtypes = new AttrType[3];
		Rtypes[0] = new AttrType (AttrType.attrInteger);
		Rtypes[1] = new AttrType (AttrType.attrInteger);
		Rtypes[2] = new AttrType (AttrType.attrString);

		short [] Rsizes = new short[1];
		Rsizes[0] = 15; 
		FldSpec [] Rprojection = new FldSpec[3];
		Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);

		FileScan am2 = null;
		try {
			am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
					(short)3, (short) 3,
					Rprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}


		FldSpec [] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrString);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		SortMerge sm = null;
		try {
			sm = new SortMerge(Stypes, 4, Ssizes,
					Rtypes, 3, Rsizes,
					1, 4, 
					1, 4, 
					10,
					am, am2, 
					false, false, ascending,
					outFilter, proj_list, 2);
		}
		catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***"); 
			status = FAIL;
			System.err.println (""+e);
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}



		QueryCheck qcheck1 = new QueryCheck(1);


		t = null;

		try {
			while ((t = sm.get_next()) != null) {
				t.print(jtype);

				qcheck1.Check(t);
			}
		}
		catch (Exception e) {
			System.err.println (""+e);
			e.printStackTrace();
			status = FAIL;
		}
		if (status != OK) {
			//bail out
			System.err.println ("*** Error in get next tuple ");
			Runtime.getRuntime().exit(1);
		}

		qcheck1.report(1);
		try {
			sm.close();
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		System.out.println ("\n"); 
		if (status != OK) {
			//bail out
			System.err.println ("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}
	}
}

public class ModifiedJoinTest
{

	public static void main(String argv[]) throws NotDirectoryException
	{
		boolean sortstatus;
		//SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
		//JavabaseDB.openDB("/tmp/nwangdb", 5000);

		ModifiedJoinsDriver jjoin = new ModifiedJoinsDriver();

		DBBuilder db = new DBBuilder();

		sortstatus = jjoin.runTests();
		if (sortstatus != true) {
			System.out.println("Error ocurred during join tests");
		}
		else {
			System.out.println("join tests completed successfully");
		}
	}
}

