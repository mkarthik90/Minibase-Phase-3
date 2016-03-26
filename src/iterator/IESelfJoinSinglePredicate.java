package iterator;

import global.AttrType;
import global.GlobalConst;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;
import java.util.Arrays;

import bufmgr.PageNotReadException;

public class IESelfJoinSinglePredicate extends Iterator implements GlobalConst {
	private AttrType _in1[], _in2[];
	private int in1_len, in2_len;
	private Iterator p_i1, // pointers to the two iterators. If the
			p_i2; // inputs are sorted, then no sorting is done
	private CondExpr OutputFilter[];

	private short inner_str_sizes[];
	private IoBuf io_buf1, io_buf2;
	private Tuple TempTuple1, TempTuple2;
	private Tuple tuple1, tuple2;
	private int _n_pages;
	private Heapfile temp_file_fd1, temp_file_fd2;
	private Tuple Jtuple;
	private FldSpec perm_mat[];
	private int nOutFlds;
	private int[] permutationArray;
	private int[] bitArray;
	// TODO
	private int SIZEOFTABLE = 0;
	private int totalNumberOfResult = 0;

	/**
	 * constructor,initialization
	 * 
	 * @param in1
	 *            [] Array containing field types of R
	 * @param len_in1
	 *            # of columns in R
	 * @param s1_sizes
	 *            shows the length of the string fields in R.
	 * @param in2
	 *            [] Array containing field types of S
	 * @param len_in2
	 *            # of columns in S
	 * @param s2_sizes
	 *            shows the length of the string fields in S
	 * @param sortFld1Len
	 *            the length of sorted field in R
	 * @param sortFld2Len
	 *            the length of sorted field in S
	 * @param join_col_in1
	 *            The col of R to be joined with S
	 * @param join_col_in2
	 *            the col of S to be joined with R
	 * @param amt_of_mem
	 *            IN PAGES
	 * @param am1
	 *            access method for left input to join
	 * @param am2
	 *            access method for right input to join
	 * @param in1_sorted
	 *            is am1 sorted?
	 * @param in2_sorted
	 *            is am2 sorted?
	 * @param order
	 *            the order of the tuple: assending or desecnding?
	 * @param outFilter
	 *            [] Ptr to the output filter
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param n_out_flds
	 *            number of outer relation fileds
	 * @throws Exception
	 * @throws UnknownKeyTypeException
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws PredEvalException
	 * @throws PageNotReadException
	 * @throws InvalidTypeException
	 * @throws InvalidTupleSizeException
	 * @throws IndexException
	 * @throws JoinsException
	 */
	public IESelfJoinSinglePredicate(AttrType in1[], int len_in1, short s1_sizes[],
			AttrType in2[], int len_in2, short s2_sizes[],

			int join_col_in1, int sortFld1Len, int join_col_in2,
			int sortFld2Len,

			int amt_of_mem, Iterator am1, Iterator am2,

			boolean in1_sorted, boolean in2_sorted, TupleOrder order,
			TupleOrder order2,

			CondExpr outFilter[], FldSpec proj_list[], int n_out_flds, int sizeOfTable)
			throws JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, PredEvalException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception

	{
		SIZEOFTABLE = sizeOfTable;
		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		in1_len = len_in1;
		in2_len = len_in2;

		Jtuple = new Tuple();
		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] ts_size = null;
		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		try {
			ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1,
					in2, len_in2, s1_sizes, s2_sizes, proj_list, n_out_flds);
		} catch (Exception e) {
			throw new TupleUtilsException(e,
					"Exception is caught by SortMerge.java");
		}

		int n_strs2 = 0;

		for (int i = 0; i < len_in2; i++)
			if (_in2[i].attrType == AttrType.attrString)
				n_strs2++;
		inner_str_sizes = new short[n_strs2];

		for (int i = 0; i < n_strs2; i++)
			inner_str_sizes[i] = s2_sizes[i];

		p_i1 = am1;
		p_i2 = am2;

		if (!in1_sorted) {
			try {
				p_i1 = new Sort(in1, (short) len_in1, s1_sizes, am1,
						join_col_in1, order, sortFld1Len, amt_of_mem / 2);
			} catch (Exception e) {
				throw new SortException(e, "Sort failed");
			}
		}

		if (!in2_sorted) {
			try {
				p_i2 = new Sort(in2, (short) len_in2, s2_sizes, am2,
						join_col_in2, order, sortFld2Len, amt_of_mem / 2);
			} catch (Exception e) {
				throw new SortException(e, "Sort failed");
			}
		}

		OutputFilter = outFilter;

		// open io_bufs
		io_buf1 = new IoBuf();
		io_buf2 = new IoBuf();

		// Allocate memory for the temporary tuples
		TempTuple1 = new Tuple();
		TempTuple2 = new Tuple();
		tuple1 = new Tuple();
		tuple2 = new Tuple();

		if (io_buf1 == null || io_buf2 == null || TempTuple1 == null
				|| TempTuple2 == null || tuple1 == null || tuple2 == null)
			throw new JoinNewFailed("SortMerge.java: allocate failed");
		if (amt_of_mem < 2)
			throw new JoinLowMemory("SortMerge.java: memory not enough");

		try {
			TempTuple1.setHdr((short) in1_len, _in1, s1_sizes);
			tuple1.setHdr((short) in1_len, _in1, s1_sizes);
			TempTuple2.setHdr((short) in2_len, _in2, s2_sizes);
			tuple2.setHdr((short) in2_len, _in2, s2_sizes);
		} catch (Exception e) {
			throw new SortException(e, "Set header failed");
		}

		// Two buffer pages to store equivalence classes
		// NOTE -- THESE PAGES ARE NOT OBTAINED FROM THE BUFFER POOL
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		_n_pages = 1;

		temp_file_fd1 = null;
		temp_file_fd2 = null;
		try {
			temp_file_fd1 = new Heapfile(null);
			temp_file_fd2 = new Heapfile(null);

		} catch (Exception e) {
			throw new SortException(e, "Create heap file failed");
		}

		// Now, that stuff is setup, all we have to do is a get_next !!!!

		// Setting up permutation array

		Iterator temp_p_i1 = (Iterator) p_i1.clone();
		permutationArray = new int[SIZEOFTABLE];
		int permutationPosition = 0;
		int i = 1;
		Tuple l1 = null;
		Tuple l2 = null;
		while ((l1 = temp_p_i1.get_next()) != null) {
			tuple1 = l1;

			Iterator temp_p_i2 = (Iterator) p_i2.clone();
			int position = 1;

			while ((l2 = temp_p_i2.get_next()) != null) {
				tuple2 = l2;
				byte[] temp1 = tuple1.getData();
				byte[] temp2 = tuple2.getData();
				if (!Arrays.equals(temp1, temp2)) {
					// System.out.println(position);
					position++;
				} else {
					while ((l2 = temp_p_i2.get_next()) != null) {
					}
				}

			}
			permutationArray[permutationPosition] = position;
			permutationPosition++;
			// i++;
		}

		// SETTING up bit array
		temp_p_i1 = (Iterator) p_i1.clone();
		bitArray = new int[SIZEOFTABLE];
		int bitArrayPOisiton = 0;
		while ((l1 = temp_p_i1.get_next()) != null) {
			try {
				bitArray[bitArrayPOisiton] = 0;
				bitArrayPOisiton++;
			} catch (Exception ex) {
				System.out.println("Hold");
			}
		}
	}

	/**
	 * The tuple is returned All this function has to do is to get 1 tuple from
	 * one of the Iterators (from both initially), use the sorting order to
	 * determine which one gets sent up. Amit) Hmmm it seems that some thing
	 * more has to be done in order to account for duplicates.... => I am
	 * following Raghu's 564 notes in order to obtain an algorithm for this
	 * merging. Some funda about "equivalence classes"
	 * 
	 * @return the joined tuple is returned
	 * @exception IOException
	 *                I/O errors
	 * @exception JoinsException
	 *                some join exception
	 * @exception IndexException
	 *                exception from super class
	 * @exception InvalidTupleSizeException
	 *                invalid tuple size
	 * @exception InvalidTypeException
	 *                tuple type not valid
	 * @exception PageNotReadException
	 *                exception from lower layer
	 * @exception TupleUtilsException
	 *                exception from using tuple utilities
	 * @exception PredEvalException
	 *                exception from PredEval class
	 * @exception SortException
	 *                sort exception
	 * @exception LowMemException
	 *                memory error
	 * @exception UnknowAttrType
	 *                attribute type unknown
	 * @exception UnknownKeyTypeException
	 *                key type unknown
	 * @exception Exception
	 *                other exceptions
	 */

	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {

		AttrType[] jtype = new AttrType[2];
		jtype[0] = new AttrType(AttrType.attrInteger);
		jtype[1] = new AttrType(AttrType.attrInteger);

		// change based on condition
		int eqOff = 1;

		/* IEJoin - code */

		int n = permutationArray.length;
		for (int i = 0; i < n; i++) {
			int position = permutationArray[i];
			// Find position tuple from p_i1
			Iterator tempP_i1 = (Iterator) p_i1.clone();
			Iterator tempP_i2 = (Iterator) p_i2.clone();

			int tempPosition = position;
			while (tempPosition > 0) {
				tempPosition--;
				tuple1 = tempP_i1.get_next();
			}

			// Now tuple1 contains the position tuple
			// tuple1 = p_i2.get_next();
			bitArray[position - 1] = 1;
			for (int j = 0; j < n; j++) {
				TempTuple1 = null;
				TempTuple1 = (Tuple) tuple1.clone();
				if (bitArray[j] == 1) {
					// Find j tuple from p_i2
					tempP_i2 = (Iterator) p_i2.clone();
					int tempj = j;
					tuple2 = null;
					while (tempj >= 0) {
						tempj--;
						tuple2 = tempP_i2.get_next();
						TempTuple2 = (Tuple) tuple2.clone();
					}
					// Now tuple2 contains the jth position record.
					// Add to projection

					AttrType[] jtype2 = new AttrType[4];
					jtype2[0] = new AttrType(AttrType.attrInteger);
					jtype2[1] = new AttrType(AttrType.attrInteger);
					jtype2[2] = new AttrType(AttrType.attrInteger);
					jtype2[3] = new AttrType(AttrType.attrInteger);

					/*
					 * System.out.println("*****"); TempTuple1.print(jtype2);
					 * tuple1.print(jtype2); TempTuple2.print(jtype2);
					 * tuple2.print(jtype2);
					 */

					if (PredEval.Eval(OutputFilter, TempTuple1, tuple2, _in1,
							_in2) == true) {

						Projection.Join(tuple1, _in1, tuple2, _in2, Jtuple,
								perm_mat, nOutFlds);

						totalNumberOfResult++;
						Jtuple.print(jtype);
						// return Jtuple;
					}

				}
				while (tempP_i2.get_next() != null)
					;
			}
			while (tempP_i1.get_next() != null)
				;
		}
		System.out.println("Tottal "+totalNumberOfResult);
		return null;
		/* IE Join - endcode */
	}

	/**
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 * 
	 * @exception IOException
	 *                I/O error from lower layers
	 * @exception JoinsException
	 *                join error from lower layers
	 * @exception IndexException
	 *                index access error
	 */
	public void close() throws JoinsException, IOException, IndexException {
		if (!closeFlag) {

			try {
				p_i1.close();
				p_i2.close();
			} catch (Exception e) {
				throw new JoinsException(e,
						"SortMerge.java: error in closing iterator.");
			}
			if (temp_file_fd1 != null) {
				try {
					temp_file_fd1.deleteFile();
				} catch (Exception e) {
					throw new JoinsException(e,
							"SortMerge.java: delete file failed");
				}
				temp_file_fd1 = null;
			}
			if (temp_file_fd2 != null) {
				try {
					temp_file_fd2.deleteFile();
				} catch (Exception e) {
					throw new JoinsException(e,
							"SortMerge.java: delete file failed");
				}
				temp_file_fd2 = null;
			}
			closeFlag = true;
		}
	}

}
