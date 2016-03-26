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
	private boolean finished, continueing;
	
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
	}
	
	public Tuple get_next()
	{
		if(finished)
		{
			return null;
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
				//line 12
				innerindex = backPArr[outerindex];
				//line 13
				//set the bit and bloom array
				bitArr[innerindex] = 1;
				bloomArr[innerindex/comprsize] = 1;
				//line 14
				innerindex += eqoff;
			}
			for(;innerindex < outsize + insize; innerindex++)
			{
				//first check the bloom array for large scale iteration
				if(bloomArr[innerindex / comprsize] == 0)
				{
					innerindex = ((innerindex / comprsize) +1 ) * comprsize;
				}
				else
				{
					//else check fine grain
					if(bitArr[innerindex] == 1 && !tableArr1[outerindex] && tableArr2[innerindex])
					{
						//on success
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
							//increment so that we do not loop forever
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
}
