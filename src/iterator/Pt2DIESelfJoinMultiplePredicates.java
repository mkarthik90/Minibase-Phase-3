package iterator;

import global.AttrType;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;
import java.util.Arrays;

import bufmgr.PageNotReadException;

public class Pt2DIESelfJoinMultiplePredicates extends Iterator {

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
	private int SIZEOFTABLE = 0;
	private int eqOff = 0;
	private int totalNumberOfResult = 0;

	private final int COMPRATE = 10;
	private int bloomArr[];
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
	public Pt2DIESelfJoinMultiplePredicates(AttrType in1[], int len_in1,
			short s1_sizes[], AttrType in2[], int len_in2, short s2_sizes[],
			int join_col_in1, int sortFld1Len, int join_col_in2,
			int sortFld2Len, int amt_of_mem, Iterator am1, Iterator am2,
			boolean in1_sorted, boolean in2_sorted, TupleOrder order,
			TupleOrder order2, CondExpr outFilter[], FldSpec proj_list[],
			int n_out_flds, int eqOf, int sizeOfTable) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, Exception

	{
		SIZEOFTABLE = sizeOfTable;
		eqOff = eqOf;
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
					"Exception is caught by IE Self Join Two Predicate.java");
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
						join_col_in2, order2, sortFld2Len, amt_of_mem / 2);
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
			throw new JoinNewFailed(
					"IE Self Join Two predicate.java: allocate failed");
		if (amt_of_mem < 2)
			throw new JoinLowMemory(
					"IE Self Join Two predicate.java: memory not enough");

		try {
			TempTuple1.setHdr((short) in1_len, _in1, s1_sizes);
			tuple1.setHdr((short) in1_len, _in1, s1_sizes);
			TempTuple2.setHdr((short) in2_len, _in2, s2_sizes);
			tuple2.setHdr((short) in2_len, _in2, s2_sizes);
		} catch (Exception e) {
			throw new SortException(e, "Set header failed");
		}

		_n_pages = 1;

		temp_file_fd1 = null;
		temp_file_fd2 = null;
		try {
			temp_file_fd1 = new Heapfile(null);
			temp_file_fd2 = new Heapfile(null);

		} catch (Exception e) {
			throw new SortException(e, "Create heap file failed");
		}

		// Setting up permutation array

		Iterator temp_p_i2 = (Iterator) p_i2.clone();
		permutationArray = new int[SIZEOFTABLE];
		int permutationPosition = 0;
		int i = 1;
		Tuple l1 = null;
		Tuple l2 = null;
		while ((l2 = temp_p_i2.get_next()) != null) {
			tuple1 = l2;

			Iterator temp_p_i1 = (Iterator) p_i1.clone();
			int position = 1;

			while ((l1 = temp_p_i1.get_next()) != null) {

				tuple2 = l1;
				byte[] temp1 = tuple1.getData();
				byte[] temp2 = tuple2.getData();
				if (!Arrays.equals(temp1, temp2)) {
					position++;
				} else {
					while ((l1 = temp_p_i1.get_next()) != null) {
					}
				}

			}
			permutationArray[permutationPosition] = position;
			permutationPosition++;
		}

		// SETTING up bit array
		temp_p_i2 = (Iterator) p_i1.clone();
		bitArray = new int[SIZEOFTABLE];
		int bitArrayPOisiton = 0;
		while ((l1 = temp_p_i2.get_next()) != null) {
			try {
				bitArray[bitArrayPOisiton] = 0;
				bitArrayPOisiton++;
			} catch (Exception ex) {
				System.out.println("Hold");
			}
		}

		//bloom array
		bloomArr = new int[SIZEOFTABLE / COMPRATE + 1];
		for(int k = 0; k < SIZEOFTABLE / COMPRATE + 1; k++)
		{
			bloomArr[k] = 0;
		}
		
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {

		AttrType[] jtype = new AttrType[4];
		jtype[0] = new AttrType(AttrType.attrInteger);
		jtype[1] = new AttrType(AttrType.attrInteger);
		jtype[2] = new AttrType(AttrType.attrInteger);
		jtype[3] = new AttrType(AttrType.attrInteger);

		/* IEJoin - code */

		int n = permutationArray.length;
		for (int i = 0; i < n; i++) {
			int position = permutationArray[i];
			// Find position tuple from p_i1
			Iterator tempP_i1 = (Iterator) p_i1.clone();
			Iterator tempP_i2 = (Iterator) p_i1.clone();

			int tempPosition = position;
			while (tempPosition > 0) {
				tempPosition--;
				// L1[p[i]] = tuple 1
				tuple1 = tempP_i1.get_next();
			}

			// Now tuple1 L1[p[i]]
			// tuple1 = p_i2.get_next();
			bitArray[position - 1] = 1;

			bloomArr[(position - 1) / COMPRATE] = 1;
			
			TempTuple1.tupleCopy(tuple1);
			for (int j = position - 1 + eqOff; j < n; j++) {
				if(bloomArr[j/COMPRATE] == 0)
				{
					j = (((j / COMPRATE) + 1) * COMPRATE) - 1;
				}
				else
				{
					if (bitArray[j] == 1) {
						// Find j tuple from p_i1
						tempP_i2 = (Iterator) p_i1.clone();
						int tempj = j;
						tuple2 = null;
						while (tempj >= 0) {
							tempj--;
							tuple2 = tempP_i2.get_next();
							TempTuple2.tupleCopy(tuple2);
						}
	
						Projection.Join(TempTuple1, _in1, TempTuple2, _in2, Jtuple,
								perm_mat, nOutFlds);
						Jtuple.print(jtype);
						totalNumberOfResult++;
					}
				}
				while (tempP_i2.get_next() != null)
					;

			}
			while (tempP_i1.get_next() != null)
				;
		}
		System.out.println("Total Number Of Result" + totalNumberOfResult);
		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		// TODO Auto-generated method stub

	}

}