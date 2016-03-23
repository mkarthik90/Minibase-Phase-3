package iterator;


import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
/** 
 *
 *  This file contains an implementation of the nested loops join
 *  algorithm as described in the Shapiro paper.
 *  The algorithm is extremely simple:
 *
 *      foreach tuple r in R do
 *          foreach tuple s in S do
 *              if (ri == sj) then add (r, s) to the result.
 */

public class ModifiedNestedLoopJoin  extends Iterator 
{
	private AttrType      _in1[],  _in2[];
	private   int        in1_len, in2_len;
	private   Iterator  outer;
	private   short t2_str_sizescopy[];
	private   CondExpr OutputFilter[];
	private   CondExpr RightFilter[];
	private   int        n_buf_pgs;        // # of buffer pages available.
	private   boolean        done,         // Is the join complete
	get_from_outer;                 // if TRUE, a tuple is got from outer
	private   Tuple     outer_tuple, inner_tuple;
	private   Tuple     Jtuple;           // Joined tuple
	private   FldSpec   perm_mat[];
	private   int        nOutFlds;
	private   Heapfile  hf;
	private   FileScan      inner;
	private AttrType [] Itypes;
	private short [] Isizes; 
	private FldSpec [] Iprojection;
	private boolean _one;

	/**constructor
	 *Initialize the two relations which are joined, including relation type,
	 *@param in1  Array containing field types of R.
	 *@param len_in1  # of columns in R.
	 *@param t1_str_sizes shows the length of the string fields.
	 *@param in2  Array containing field types of S
	 *@param len_in2  # of columns in S
	 *@param  t2_str_sizes shows the length of the string fields.
	 *@param amt_of_mem  IN PAGES
	 *@param am1  access method for left i/p to join
	 *@param relationName  access hfapfile for right i/p to join
	 *@param outFilter   select expressions
	 *@param rightFilter reference to filter applied on right i/p
	 *@param proj_list shows what input fields go where in the output tuple
	 *@param n_out_flds number of outer relation fileds
	 *@exception IOException some I/O fault
	 *@exception NestedLoopException exception from this class
	 */
	public ModifiedNestedLoopJoin( AttrType    in1[],    
			int     len_in1,           
			short   t1_str_sizes[],
			AttrType    in2[],         
			int     len_in2,           
			short   t2_str_sizes[],   
			int     amt_of_mem,        
			Iterator     am1,          
			String relationName,      
			CondExpr outFilter[],      
			CondExpr rightFilter[],    
			FldSpec   proj_list[],
			int        n_out_flds,
			boolean one_table
			) throws IOException,NestedLoopException
			{

		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1,0,_in1,0,in1.length);
		System.arraycopy(in2,0,_in2,0,in2.length);
		in1_len = len_in1;
		in2_len = len_in2;

		outer = am1;
		t2_str_sizescopy =  t2_str_sizes;
		inner_tuple = new Tuple();
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		RightFilter  = rightFilter;

		n_buf_pgs    = amt_of_mem;
		inner = null;
		done  = false;
		get_from_outer = true;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[]    t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		try {
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					in1, len_in1, in2, len_in2,
					t1_str_sizes, t2_str_sizes,
					proj_list, nOutFlds);
		}catch (TupleUtilsException e){
			throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
		}

		_one = one_table;

		try{
			outer_tuple = outer.get_next();
			//inner = hf.openScan();

			Itypes = new AttrType[4];
			Itypes[0] = new AttrType (AttrType.attrInteger);
			Itypes[1] = new AttrType (AttrType.attrInteger);
			Itypes[2] = new AttrType (AttrType.attrInteger);
			Itypes[3] = new AttrType (AttrType.attrInteger);

			Isizes = new short[1];
			Isizes[0] = 30;

			Iprojection = new FldSpec[4];
			Iprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
			Iprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
			Iprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
			Iprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
			inner = new FileScan("S.in", Itypes, Isizes, (short)4, (short)4, Iprojection, null);
		}
		catch(Exception e)
		{

		}
			}

	/**  
	 *@return The joined tuple is returned
	 *@exception IOException I/O errors
	 *@exception JoinsException some join exception
	 *@exception IndexException exception from super class
	 *@exception InvalidTupleSizeException invalid tuple size
	 *@exception InvalidTypeException tuple type not valid
	 *@exception PageNotReadException exception from lower layer
	 *@exception TupleUtilsException exception from using tuple utilities
	 *@exception PredEvalException exception from PredEval class
	 *@exception SortException sort exception
	 *@exception LowMemException memory error
	 *@exception UnknowAttrType attribute type unknown
	 *@exception UnknownKeyTypeException key type unknown
	 *@exception Exception other exceptions

	 */
	public Tuple get_next()
			throws IOException,
			JoinsException ,
			IndexException,
			InvalidTupleSizeException,
			InvalidTypeException, 
			PageNotReadException,
			TupleUtilsException, 
			PredEvalException,
			SortException,
			LowMemException,
			UnknowAttrType,
			UnknownKeyTypeException,
			Exception
			{
		// This is a DUMBEST form of a join, not making use of any key information...

		//You calling me dumb?

		if (done)
			return null;

		if(!_one)
		{
			inner_tuple = inner.get_next();
			//creates 2 valid tuples
			if(inner_tuple == null)
			{
				outer_tuple = outer.get_next();
				if(outer_tuple == null)
				{
					done = true;
					inner = null;
					return null;
				}
				inner = null;
				inner = new FileScan("S.in", Itypes, Isizes, (short)4, (short)4, Iprojection, null);
				inner_tuple = inner.get_next();
			}

			while(outer_tuple != null)
			{
				Tuple t1, t2;
				int col1, col2;

				col1 = OutputFilter[0].operand1.symbol.offset;
				if(OutputFilter[0].operand1.symbol.relation.key == RelSpec.outer)
				{
					t1 = outer_tuple;
				}
				else
				{
					t1 = inner_tuple;
				}

				if(OutputFilter[0].type2.attrType == AttrType.attrInteger)
				{
					t2 = null;
					col2 = OutputFilter[0].operand2.integer;
				}
				else if(OutputFilter[0].operand2.symbol.relation.key == RelSpec.outer)
				{
					t2 = outer_tuple;
					col2 = OutputFilter[0].operand2.symbol.offset;
				}
				else
				{
					t2 = inner_tuple;
					col2 = OutputFilter[0].operand2.symbol.offset;
				}

				boolean first_cond = eval_cond(OutputFilter[0], t1, col1, t2, col2);

				if(first_cond)
				{
					if(OutputFilter[1] == null)
					{
						//success
						Projection.Join(outer_tuple, _in1, inner_tuple, _in2, Jtuple, perm_mat, nOutFlds);
						return Jtuple;

					}
					else
					{
						col1 = OutputFilter[1].operand1.symbol.offset;
						if(OutputFilter[1].operand1.symbol.relation.key == RelSpec.outer)
						{
							t1 = outer_tuple;
						}
						else
						{
							t1 = inner_tuple;
						}

						if(OutputFilter[1].type2.attrType == AttrType.attrInteger)
						{
							t2 = null;
							col2 = OutputFilter[1].operand2.integer;
						}
						else if(OutputFilter[1].operand2.symbol.relation.key == RelSpec.outer)
						{
							t2 = outer_tuple;
							col2 = OutputFilter[1].operand2.symbol.offset;
						}
						else
						{
							t2 = inner_tuple;
							col2 = OutputFilter[1].operand2.symbol.offset;
						}
						if(eval_cond(OutputFilter[1], t1, col1, t2, col2))
						{
							//success
							Projection.Join(outer_tuple, _in1, inner_tuple, _in2, Jtuple, perm_mat, nOutFlds);
							return Jtuple;
						}
					}
				}

				inner_tuple = inner.get_next();

				//creates 2 valid tuples
				if(inner_tuple == null)
				{
					outer_tuple = outer.get_next();
					if(outer_tuple == null)
					{
						done = true;
						inner = null;
						return null;
					}
					inner = null;
					inner = new FileScan("S.in", Itypes, Isizes, (short)4, (short)4, Iprojection, null);
					inner_tuple = inner.get_next();
				}
			}
		}
		else
		{
			//creates one valid tuple
			outer_tuple = outer.get_next();
			if(outer_tuple == null)
			{
				done = true;
				inner = null;
				return null;
			}

			while(outer_tuple != null)
			{
				Tuple t1, t2;
				int col1, col2;

				col1 = OutputFilter[0].operand1.symbol.offset;
				t1 = outer_tuple;

				if(OutputFilter[0].type2.attrType == AttrType.attrInteger)
				{
					t2 = null;
					col2 = OutputFilter[0].operand2.integer;
				}
				else
				{
					t2 = outer_tuple;
					col2 = OutputFilter[0].operand2.symbol.offset;
				}

				boolean first_cond = eval_cond(OutputFilter[0], t1, col1, t2, col2);

				if(first_cond)
				{
					if(OutputFilter[1] == null)
					{
						//success
						Projection.Join(outer_tuple, _in1, outer_tuple, _in2, Jtuple, perm_mat, nOutFlds);
						return Jtuple;

					}
					else
					{
						t1 = outer_tuple;

						if(OutputFilter[1].type2.attrType == AttrType.attrInteger)
						{
							t2 = null;
							col2 = OutputFilter[1].operand2.integer;
						}
						else
						{
							t2 = outer_tuple;
							col2 = OutputFilter[1].operand2.symbol.offset;
						}
						
						if(eval_cond(OutputFilter[1], t1, col1, t2, col2))
						{
							//success
							Projection.Join(outer_tuple, _in1, outer_tuple, _in2, Jtuple, perm_mat, nOutFlds);
							return Jtuple;
						}
					}
				}

				//creates one valid tuple
				outer_tuple = outer.get_next();
				if(outer_tuple == null)
				{
					done = true;
					inner = null;
					return null;
				}
			}
		}
		//should not happen
		done = true;
		return null;
		/*
		do
		{
			// If get_from_outer is true, Get a tuple from the outer, delete
			// an existing scan on the file, and reopen a new scan on the file.
			// If a get_next on the outer returns DONE?, then the nested loops
			//join is done too.

			if (get_from_outer == true)
			{
				get_from_outer = false;
				if (inner != null)     // If this not the first time,
				{
					// close scan
					inner = null;
				}

				try {
					inner = hf.openScan();
				}
				catch(Exception e){
					throw new NestedLoopException(e, "openScan failed");
				}

				if ((outer_tuple=outer.get_next()) == null)
				{
					done = true;
					if (inner != null) 
					{

						inner = null;
					}

					return null;
				}   
			}  // ENDS: if (get_from_outer == TRUE)


			// The next step is to get a tuple from the inner,
			// while the inner is not completely scanned && there
			// is no match (with pred),get a tuple from the inner.


			rid = new RID();
			while ((inner_tuple = inner.getNext(rid)) != null)
			{
				inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
				if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
				{
					if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true)
					{
						// Apply a projection on the outer and inner tuples.
						Projection.Join(outer_tuple, _in1, 
								inner_tuple, _in2, 
								Jtuple, perm_mat, nOutFlds);
						return Jtuple;
					}
				}
			}

			// There has been no match. (otherwise, we would have 
			//returned from t//he while loop. Hence, inner is 
			//exhausted, => set get_from_outer = TRUE, go to top of loop

			get_from_outer = true; // Loop back to top and get next outer tuple.	      
		} while (true);*/
			} 

	private boolean eval_cond(CondExpr e, Tuple t1, int c1, Tuple t2, int c2)
	{
		if(t2 != null)
		{
			//compare the two tuple
			int comp_res = 0;
			try {
				comp_res = TupleUtils.CompareTupleWithTuple(new AttrType(AttrType.attrInteger), t1, c1, t2, c2);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return false;
			}

			//for the different kinds of inequality
			switch(e.op.attrOperator)
			{
			case AttrOperator.aopEQ:
			{
				//if we succeeded
				if(comp_res == 0)
				{
					return true;
				}
				break;
			}
			case AttrOperator.aopLT:
			{
				//if we succeeded
				if(comp_res < 0)
				{
					return true;
				}
				break;
			}
			case AttrOperator.aopGT:
			{
				//if we succeeded
				if(comp_res > 0)
				{
					return true;
				}
				break;
			}
			case AttrOperator.aopLE:
			{
				//if we succeeded
				if(comp_res <= 0)
				{
					return true;
				}
				break;
			}
			case AttrOperator.aopGE:
			{
				//if we succeeded
				if(comp_res >= 0)
				{
					return true;
				}
				break;
			}
			case AttrOperator.aopNE:
			{
				//if we succeeded
				if(comp_res != 0)
				{
					return true;
				}
				break;
			}
			}
			//if condition is not met
			return false;
		}
		else
		{
			try{
				switch(e.op.attrOperator)
				{
				case AttrOperator.aopEQ:
				{
					//if we succeed
					if(t1.getIntFld(c1) == c2)
					{
						return true;
					}
					break;
				}
				case AttrOperator.aopLT:
				{
					//if we succeed
					if(t1.getIntFld(c1) < c2)
					{
						return true;
					}
					break;
				}
				case AttrOperator.aopGT:
				{
					//if we succeed
					if(t1.getIntFld(c1) > c2)
					{
						return true;
					}
					break;
				}
				case AttrOperator.aopLE:
				{
					//if we succeed
					if(t1.getIntFld(c1) <= c2)
					{
						return true;
					}
					break;

				}
				case AttrOperator.aopGE:
				{
					//if we succeed
					if(t1.getIntFld(c1) >= c2)
					{
						return true;
					}
					break;

				}
				case AttrOperator.aopNE:
				{
					//if we succeed
					if(t1.getIntFld(c1) != c2)
					{
						return true;
					}
					break;
				}
				}
			}
			catch (Exception ex)
			{
				System.out.println("error on tuple read");
			}
			return false;
		}
	}
	/**
	 * implement the abstract method close() from super class Iterator
	 *to finish cleaning up
	 *@exception IOException I/O error from lower layers
	 *@exception JoinsException join error from lower layers
	 *@exception IndexException index access error 
	 */
	public void close() throws JoinsException, IOException,IndexException 
	{
		if (!closeFlag) {

			try {
				outer.close();
			}catch (Exception e) {
				throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}






