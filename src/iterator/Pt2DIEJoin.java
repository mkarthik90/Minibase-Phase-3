package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import tests.TableEntry;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class Pt2DIEJoin {
	private final AttrType[] _ATTR_TYPES = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
			new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	private final FldSpec[] _BASIC_PROJECTION = {new FldSpec(new RelSpec(RelSpec.outer), 1),
			new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
			new FldSpec(new RelSpec(RelSpec.outer), 4)};
	private String f1name, f2name;
	private FldSpec r1f[], r2f[], pf[];
	private AttrType r1t[], r2t[], pt[];
	private int r1size, r2size, projsize;
	private CondExpr cond[];
	private int memsize;
	private int permutationArray[], backPArr[];
	private static final int comprsize = 10;

	private IoBuf io_buf1, io_buf2;

	private Tuple tuple1, tuple2;


	private int outsize, insize;
	private int _n_pages;
	private int bitArr[], bloomArr[];
	private boolean tableArr1[], tableArr2[];
	private int eqoff;
	
	private int innerindex, outerindex;
	private boolean firsttime, finished, continueing;
	
	/*
	 * r1.r1c1 op1 r2.r2c1
	 * r1.r1c2 op2 r2.r2c2
	 */
	public Pt2DIEJoin(String r1, FldSpec[] r1_fields, AttrType[] r1_types, int r1_size, String r2, FldSpec[] r2_fields, AttrType[] r2_types, int r2_size,
			FldSpec[] proj_list, AttrType[] proj_type, int proj_size, CondExpr[] expr, int mem_size) 
					throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
	{
		innerindex = 0;
		outerindex = 0;
		firsttime = true;
		finished = false;
		continueing = false;

		//copy all the info needed
		f1name = r1;
		f2name = r2;

		r1size = r1_size;
		r2size = r2_size;

		r1f = new FldSpec[r1_size];
		r2f = new FldSpec[r2_size];

		System.arraycopy(r1_fields,0,r1f,0,r1_size);
		System.arraycopy(r2_fields,0,r2f,0,r2_size);

		r1t = new AttrType[r1_size];
		r2t = new AttrType[r2_size];

		System.arraycopy(r1_types,0,r1t,0,r1_size);
		System.arraycopy(r2_types,0,r2t,0,r2_size);

		pf = new FldSpec[proj_size];
		pt = new AttrType[proj_size];

		System.arraycopy(proj_list, 0, pf, 0, proj_size);
		System.arraycopy(proj_type, 0, pt, 0, proj_size);
		
		cond = expr;

		memsize = mem_size;
		projsize = proj_size;

		
		
		
		//
		//line 3-6
		TupleOrder updown1 = null, updown2 = null;
		switch (cond[0].op.attrOperator)
		{
		case(AttrOperator.aopLT):
		case(AttrOperator.aopLE):
		{
			updown1 = new TupleOrder(TupleOrder.Descending);
			break;
		}
		case(AttrOperator.aopGE):
		case(AttrOperator.aopGT):
		{
			updown1 = new TupleOrder(TupleOrder.Ascending);
			break;
		}
		}
		switch (cond[1].op.attrOperator)
		{
		case(AttrOperator.aopLT):
		case(AttrOperator.aopLE):
		{
			updown2 = new TupleOrder(TupleOrder.Ascending);
			break;
		}
		case(AttrOperator.aopGE):
		case(AttrOperator.aopGT):
		{
			updown2 = new TupleOrder(TupleOrder.Descending);
			break;
		}
		}
		

		//line 7-8
		//set up for sort=======================================================================================
		int r1c1, r1c2, r2c1, r2c2;
		boolean out1, out2;
		if(cond[0].operand1.symbol.relation.key == RelSpec.outer)
		{ 
			r1c1 = cond[0].operand1.symbol.offset;
			r2c1 = cond[0].operand2.symbol.offset;
			out1 = true;
		}
		else
		{
			r2c1 = cond[0].operand1.symbol.offset;
			r1c1 = cond[0].operand2.symbol.offset;
			out1 = false;
		}
		if(cond[1].operand1.symbol.relation.key == RelSpec.outer)
		{
			r1c2 = cond[1].operand1.symbol.offset;
			r2c2 = cond[1].operand2.symbol.offset;
			out2 = true;
		}
		else
		{
			r2c2 = cond[1].operand1.symbol.offset;
			r1c2 = cond[1].operand2.symbol.offset;
			out2 = false;
		}
		FileScan scan1 = new FileScan(r1 + ".in", r1t, null, (short)4, (short)4, r1f, null);
		FileScan scan2 = new FileScan(r2 + ".in", r2t, null, (short)4, (short)4, r2f, null);
		//get num relations
		outsize = 0;
		while(scan1.get_next() != null)
		{
			outsize++;
		}
		insize = 0;
		while(scan2.get_next() != null)
		{
			insize++;
		}

		tableArr1 = new boolean[outsize + insize];
		tableArr2 = new boolean[outsize + insize];
		int tableArrIndex = 0;
		
		//reset scanners
		scan1 = new FileScan(r1 + ".in", r1t, null, (short)4, (short)4, r1f, null);
		scan2 = new FileScan(r2 + ".in", r2t, null, (short)4, (short)4, r2f, null);
		//sort1================================================================================================
		Sort combiner1 = new Sort(r1t, (short)r1size, null, scan1, r1c1,
				updown1, (short)4, 4);
		Sort combiner2 = new Sort(r2t, (short)r2size, null, scan2, r2c1,
				updown1, (short)4, 4);
		

		Tuple TempTuple1 = combiner1.get_next();
		Tuple TempTuple2 = combiner2.get_next();
		RID rid;
		Heapfile f = null;
		try {
			f = new Heapfile("tempsort1.in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		Tuple t = new Tuple(TempTuple1.size());
		try {
			t.setHdr((short) 4, r1t, null);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		
		while(TempTuple1 != null && TempTuple2 != null)
		{
			if(updown1.tupleOrder == TupleOrder.Ascending)
			{
				if(TempTuple1.getIntFld(r1c1) <= TempTuple2.getIntFld(r2c1))
				{
					try {
						rid = f.insertRecord(TempTuple1.returnTupleByteArray());
						TempTuple1 = combiner1.get_next();
						tableArr1[tableArrIndex] = false;
						tableArrIndex++;
						
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
				else
				{
	
					try {
						rid = f.insertRecord(TempTuple2.returnTupleByteArray());
						TempTuple2 = combiner2.get_next();
						tableArr1[tableArrIndex] = true;
						tableArrIndex++;
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
			}
			else
			{
				if(TempTuple1.getIntFld(r1c1) >= TempTuple2.getIntFld(r2c1))
				{
					try {
						rid = f.insertRecord(TempTuple1.returnTupleByteArray());
						TempTuple1 = combiner1.get_next();
						tableArr1[tableArrIndex] = false;
						tableArrIndex++;
						
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
				else
				{
	
					try {
						rid = f.insertRecord(TempTuple2.returnTupleByteArray());
						TempTuple2 = combiner2.get_next();
						tableArr1[tableArrIndex] = true;
						tableArrIndex++;
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
			}
		}
		
		while(TempTuple1 != null)
		{
			try {
				rid = f.insertRecord(TempTuple1.returnTupleByteArray());
				TempTuple1 = combiner1.get_next();
				tableArr1[tableArrIndex] = false;
				tableArrIndex++;
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}
		}
		while(TempTuple2 != null)
		{
			try {
				rid = f.insertRecord(TempTuple2.returnTupleByteArray());
				TempTuple2 = combiner2.get_next();
				tableArr1[tableArrIndex] = true;
				tableArrIndex++;
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}
		}
		
		//sort2===============================================================================================
		tableArrIndex = 0;
		//reset scanners
		scan1 = new FileScan(r1 + ".in", r1t, null, (short)4, (short)4, r1f, null);
		scan2 = new FileScan(r2 + ".in", r2t, null, (short)4, (short)4, r2f, null);
		combiner1 = new Sort(r1t, (short)r1size, null, scan1, r1c2,
				updown2, (short)4, 4);
		combiner2 = new Sort(r2t, (short)r2size, null, scan2, r2c2,
				updown2, (short)4, 4);
		


		//combine
		TempTuple1 = combiner1.get_next();
		TempTuple2 = combiner2.get_next();
		
		f = null;
		try {
			f = new Heapfile("tempsort2.in");
		}
		catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		t = new Tuple(TempTuple1.size());
		try {
			t.setHdr((short) 4, r1t, null);
		}
		catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		
		while(TempTuple1 != null && TempTuple2 != null)
		{
			if(updown2.tupleOrder == TupleOrder.Ascending)
			{
				if(TempTuple1.getIntFld(r1c2) <= TempTuple2.getIntFld(r2c2))
				{
					try {
						rid = f.insertRecord(TempTuple1.returnTupleByteArray());
						TempTuple1 = combiner1.get_next();
						tableArr2[tableArrIndex] = false;
						tableArrIndex++;
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
				else
				{
					try {
						rid = f.insertRecord(TempTuple2.returnTupleByteArray());
						TempTuple2 = combiner2.get_next();
						tableArr2[tableArrIndex] = true;
						tableArrIndex++;
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
			}
			else
			{
				if(TempTuple1.getIntFld(r1c2) >= TempTuple2.getIntFld(r2c2))
				{
					try {
						rid = f.insertRecord(TempTuple1.returnTupleByteArray());
						TempTuple1 = combiner1.get_next();
						tableArr2[tableArrIndex] = false;
						tableArrIndex++;
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
				else
				{
					try {
						rid = f.insertRecord(TempTuple2.returnTupleByteArray());
						TempTuple2 = combiner2.get_next();
						tableArr2[tableArrIndex] = true;
						tableArrIndex++;
					}
					catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						e.printStackTrace();
					}
				}
			}
		}
		
		while(TempTuple1 != null)
		{
			try {
				rid = f.insertRecord(TempTuple1.returnTupleByteArray());
				TempTuple1 = combiner1.get_next();
				tableArr2[tableArrIndex] = false;
				tableArrIndex++;
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}
		}
		while(TempTuple2 != null)
		{
			try {
				rid = f.insertRecord(TempTuple2.returnTupleByteArray());
				TempTuple2 = combiner2.get_next();
				tableArr2[tableArrIndex] = true;
				tableArrIndex++;
			}
			catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}
		}
		//==============================================================================================

		// Now, that stuff is setup, all we have to do is a get_next !!!!

		// Setting up permutation array
		scan1 = new FileScan("tempsort1.in", r1t, null, (short)4, (short)4, r1f, null);
		scan2 = new FileScan("tempsort2.in", r2t, null, (short)4, (short)4, r2f, null);
		
		

		permutationArray = new int[outsize + insize];
		backPArr = new int[outsize + insize];
		int permutationPosition = 0;
		
		Tuple t1 = scan1.get_next();
		Tuple t2 = scan2.get_next();
		
		while(t2 != null)
		{
			int position = 0;
			while (t1 != null) 
			{
				byte[] temp1 = t1.getData();
				byte[] temp2 = t2.getData();
				if (!Arrays.equals(temp1, temp2)) 
				{
					t1 = scan1.get_next();
					position++;
				} 
				else 
				{
					t1 = null;
				}
			}
			permutationArray[permutationPosition] = position;
			backPArr[position] = permutationPosition;
			scan1 = new FileScan("tempsort1.in", r1t, null, (short)4, (short)4, r1f, null);

			t2 = scan2.get_next();
			t1 = scan1.get_next();
			permutationPosition++;
		}

		//line 11
		// SETTING up bit array
		bitArr = new int[outsize + insize];
		bloomArr = new int[((outsize + insize) / comprsize) + 1];
		int bitArrayPoisiton = 0;
		for(; bitArrayPoisiton < outsize + insize; bitArrayPoisiton++)
		{
			bitArr[bitArrayPoisiton] = 0;
		}
		
		for(bitArrayPoisiton = 0; bitArrayPoisiton < ((outsize + insize) / comprsize) + 1; bitArrayPoisiton++)
		{
			bloomArr[bitArrayPoisiton] = 0;
		}


		//line 12-13
		if(cond[0].op.attrOperator == AttrOperator.aopGE || cond[0].op.attrOperator == AttrOperator.aopLE)
		{
			if(cond[1].op.attrOperator == AttrOperator.aopGE || cond[1].op.attrOperator == AttrOperator.aopLE)
			{
				eqoff = 0;
			}
			else
			{
				eqoff = 1;
			}
		}
		else
		{
			eqoff = 1;
		}

		
		//loop?
/*

		for(int i = 1; i < 5; i++){
			off2 = o2[i];

			for(int j = o2[i-1]; j < o2[i-1]; j++){
				bitArray[permArr2[j]] = 1;
			}

			off1 = o1[permArr1[i]];

			for(int j = off1 + eqOff; j < 5; j++){
				if(bitArray[j] == 1){

				}
			}
		}*/
	}
	
	public Tuple get_next()
	{
		if(finished)
		{
			return null;
		}
		if(firsttime)
		{
			innerindex += eqoff;
		}
		for(;outerindex < outsize + insize; outerindex++)
		{
			//if we are picking up where we left off
			if(continueing)
			{
				continueing = false;
			}
			else
			{
				//otherwise update innerindex
				innerindex = permutationArray[outerindex] + eqoff;
				bitArr[outerindex] = 1;
				bloomArr[outerindex/comprsize] = 1;
			}
			for(;innerindex < outsize + insize; innerindex++)
			{
				if(bloomArr[innerindex / comprsize] == 0)
				{
					innerindex = ((innerindex / comprsize) +1 ) * comprsize;
				}
				else
				{
					if(bitArr[innerindex] == 1 && !tableArr1[outerindex] && tableArr1[outerindex] != tableArr2[innerindex])
					{
						try{
							//get tuple L1[i],L2[j]
							FileScan scan1 = new FileScan("tempsort1.in", r1t, null, (short)4, (short)4, r1f, null);
							FileScan scan2 = new FileScan("tempsort2.in", r2t, null, (short)4, (short)4, r2f, null);
							Tuple t1 = scan1.get_next();
							Tuple t2 = scan2.get_next();
							
							for(int i = 0; i < outerindex; i++)
							{
								t1 = scan1.get_next();
							}
							for(int i = 0; i < innerindex; i++)
							{
								t2 = scan2.get_next();
							}
							
							Tuple Jtuple = new Tuple();
							Jtuple.setHdr((short)projsize, pt, null);
							
							Projection.Join(t1, r1t, t2, r2t, Jtuple, pf, projsize);
							
							continueing = true;
							innerindex++;
							return Jtuple;
						}
						catch(Exception e)
						{
							
						}
					}
				}
			}
		}
		finished = true;
		return null;
	}

	private class TuplePair{
		private Tuple _t1, _t2;

		public TuplePair(Tuple t1, Tuple t2){
			_t1 = t1;
			_t2 = t2;
		}
	}

	private class IEJoinRelation{
		public String name;
		public int col;
		public RelSpec relSpec;

		public IEJoinRelation(String _name, int _relSpec, int _col){
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
			if (o == null || !IEJoinRelation.class.isAssignableFrom(o.getClass())) {
				return false;
			}

			IEJoinRelation other = (IEJoinRelation) o;

			return this.name.equals(other.name) && this.col == other.col;
		}
	}

	private class IEJoinQuery{
		private final AttrType[] _ATTR_TYPES = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
				new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};

		private List<CondExpr> _filter;
		private List<IEJoinRelation> _relations;

		public IEJoinQuery(){
			_filter = new ArrayList<CondExpr>();
			_relations = new ArrayList<IEJoinRelation>();
		}

		private void _addToRelations(IEJoinRelation rel){
			if(!_relations.contains(rel)){
				_relations.add(rel);
			}
		}

		public void runQuery(){
			FileScan scan1 = null, scan2 = null;
			Map<String, FileScan> scans = new HashMap<String, FileScan>();
			List<FldSpec> projection1 = new ArrayList<FldSpec>(), projection2 = new ArrayList<FldSpec>();

			IEJoinRelation rel1 = _relations.get(0), rel2 = _relations.get(1);

			for(int i = 1; i < 5; i++){
				projection1.add(new FldSpec(new RelSpec(RelSpec.outer), i));
				projection2.add(new FldSpec(new RelSpec(RelSpec.outer), i));
			}

			try {
				scan1 = new FileScan(rel1.name + ".in", _ATTR_TYPES, null, 
						(short)4, (short)4, projection1.toArray(new FldSpec[projection1.size()]), null);

				Tuple tuple;

				while((tuple = scan1.get_next()) != null){
				}

				scan2 = new FileScan(rel2.name + ".in", _ATTR_TYPES, null, 
						(short)4, (short)4, projection2.toArray(new FldSpec[projection2.size()]), null);
			} catch (FileScanException | TupleUtilsException | InvalidRelation | IOException | JoinsException | InvalidTupleSizeException | InvalidTypeException | PageNotReadException | PredEvalException | UnknowAttrType | FieldNumberOutOfBoundException | WrongPermat e) {
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

		public void addExpression(IEJoinRelation rel1, IEJoinRelation rel2, AttrOperator op, String conj){
			_addToRelations(rel1);
			_addToRelations(rel2);

			CondExpr expr = new CondExpr();

			expr.op    = op;
			expr.type1 = new AttrType(AttrType.attrSymbol);
			expr.type2 = new AttrType(AttrType.attrSymbol);
			expr.operand1.symbol = new FldSpec (rel1.relSpec, rel1.col);
			expr.operand2.symbol = new FldSpec (rel2.relSpec, rel2.col);

			_filter.add(expr);
		}

		public List<IEJoinRelation> getRelations(){
			return _relations;
		}

		public CondExpr[] getFilter(){
			return _filter.toArray(new CondExpr[_filter.size()]);
		}
	}
}
