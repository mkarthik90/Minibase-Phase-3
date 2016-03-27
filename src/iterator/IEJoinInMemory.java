package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class IEJoinInMemory extends Iterator{

	public static AttrType[] _ATTR_TYPES = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
			new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	private final FldSpec[] _BASIC_PROJECTION = {new FldSpec(new RelSpec(RelSpec.outer), 1),
			new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
			new FldSpec(new RelSpec(RelSpec.outer), 4)};
	private int[] l1Offset, l2Offset, bitArray, permArr, primePermArr;
	private int eqOff, m, n;
	private Map<String, Set<Integer>> _projRels;
	private IEJoinArray _elements;

	enum IEJoinArrayType{
		L1, L1Prime, L2, L2Prime;

		public static final IEJoinArrayType[] ALL_TYPES = {L1, L1Prime, L2, L2Prime};
	}

	class TupleComparator implements Comparator<Tuple>{
		private int _col;
		private int _order;

		public TupleComparator(int col, TupleOrder order){
			_col = col;
			_order = order.tupleOrder;
		}

		@Override
		public int compare(Tuple o1, Tuple o2) {
			int result = Integer.MAX_VALUE;

			try {
				if(_order == TupleOrder.Ascending){
					result = Integer.compare(o1.getIntFld(_col), o2.getIntFld(_col));
				}
				else{
					result = Integer.compare(o2.getIntFld(_col), o1.getIntFld(_col));
				}
			} catch (FieldNumberOutOfBoundException | IOException e) {
				e.printStackTrace();
			}

			return result;
		}

	}

	class IEJoinArray{
		private Map<IEJoinArrayType, List<Tuple>> _values;
		private int _l1TupleOrder, _l2TupleOrder;

		public IEJoinArray(String r1, String r2, int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2){
			_values = new HashMap<IEJoinArrayType, List<Tuple>>();
			_populateValues(r1, r2);
			_sortValues(r1c1, r2c1, r1c2, r2c2, op1, op2);

			//_print();
			//System.exit(0);
		}
		
		private void _print(){
			for(IEJoinArrayType key : _values.keySet()){
				System.out.println(key);
				for(Tuple tuple : _values.get(key)){
					try {
						tuple.print(_ATTR_TYPES);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		public int getR1Size(){
			return _values.get(IEJoinArrayType.L1).size();
		}

		public int getR2Size(){
			return _values.get(IEJoinArrayType.L1Prime).size();
		}

		public int getL1TupleOrder(){
			return _l1TupleOrder;
		}

		public int getL2TupleOrder(){
			return _l2TupleOrder;
		}

		private void _sortValues(int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2){
			TupleComparator r1Comp, r2Comp;

			//line 3-4
			if(op1 == AttrOperator.aopGT || op1 == AttrOperator.aopGE){
				//Sort in Descending Order
				r1Comp = new TupleComparator(r1c1, new TupleOrder(TupleOrder.Descending));
				r2Comp = new TupleComparator(r2c1, new TupleOrder(TupleOrder.Descending));
				_l1TupleOrder = TupleOrder.Descending;
			}
			else{
				//Sort in Ascending Order
				r1Comp = new TupleComparator(r1c1, new TupleOrder(TupleOrder.Ascending));
				r2Comp = new TupleComparator(r2c1, new TupleOrder(TupleOrder.Ascending));
				_l1TupleOrder = TupleOrder.Ascending;
			}

			Collections.sort(_values.get(IEJoinArrayType.L1), r1Comp);
			Collections.sort(_values.get(IEJoinArrayType.L1Prime), r2Comp);

			//line 5-6
			if(op2 == AttrOperator.aopGT || op2 == AttrOperator.aopGE){
				//Sort in Ascending Order
				r1Comp = new TupleComparator(r1c2, new TupleOrder(TupleOrder.Ascending));
				r2Comp = new TupleComparator(r2c2, new TupleOrder(TupleOrder.Ascending));
				_l2TupleOrder = TupleOrder.Ascending;
			}
			else{
				//Sort in Descending Order
				r1Comp = new TupleComparator(r1c2, new TupleOrder(TupleOrder.Descending));
				r2Comp = new TupleComparator(r2c2, new TupleOrder(TupleOrder.Descending));
				_l2TupleOrder = TupleOrder.Descending;
			}

			Collections.sort(_values.get(IEJoinArrayType.L2), r1Comp);
			Collections.sort(_values.get(IEJoinArrayType.L2Prime), r2Comp);
		}

		private void _populateValues(String r1, String r2){
			for(IEJoinArrayType type : IEJoinArrayType.ALL_TYPES){
				_values.put(type, new ArrayList<Tuple>());
			}

			try {
				FileScan r1Scan = new FileScan(r1 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null),
						r2Scan = new FileScan(r2 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null);
				Tuple temp, tempCopy;

				while((temp = r1Scan.get_next()) != null){
					tempCopy = new Tuple();
					tempCopy.setHdr((short)4, _ATTR_TYPES, null);
					tempCopy.tupleCopy(temp);

					_values.get(IEJoinArrayType.L1).add(tempCopy);
					_values.get(IEJoinArrayType.L2).add(tempCopy);
				}

				while((temp = r2Scan.get_next()) != null){
					tempCopy = new Tuple();
					tempCopy.setHdr((short)4, _ATTR_TYPES, null);
					tempCopy.tupleCopy(temp);

					_values.get(IEJoinArrayType.L1Prime).add(tempCopy);
					_values.get(IEJoinArrayType.L2Prime).add(tempCopy);
				}
			} catch (JoinsException | InvalidTupleSizeException | InvalidTypeException | PageNotReadException
					| PredEvalException | UnknowAttrType | FieldNumberOutOfBoundException | WrongPermat
					| IOException | FileScanException | TupleUtilsException | InvalidRelation e) {
				e.printStackTrace();
			}
		}

		public Tuple getFromL1(int idx){
			return _get(IEJoinArrayType.L1, idx);
		}

		public Tuple getFromL1Prime(int idx){
			return _get(IEJoinArrayType.L1Prime, idx);
		}

		public Tuple getFromL2(int idx){
			return _get(IEJoinArrayType.L2, idx);
		}

		public Tuple getFromL2Prime(int idx){
			return _get(IEJoinArrayType.L2Prime, idx);
		}

		private Tuple _get(IEJoinArrayType type, int idx){
			return _values.get(type).get(idx);
		}
	}

	public IEJoinInMemory(String r1, String r2, int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2, Map<String, Set<Integer>> projRels) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		_projRels = projRels;
		_elements = new IEJoinArray(r1, r2, r1c1, r2c1, r1c2, r2c2, op1, op2);
		m = _elements.getR1Size();
		n = _elements.getR2Size();

		//line 7
		permArr = new int[m];
		Tuple l2, l1;

		for(int i = 0; i < m; i++){
			l2 = _elements.getFromL2(i);

			for(int j = 0; j < m; j++){
				l1 = _elements.getFromL1(j);

				if(Arrays.equals(l2.getTupleByteArray(), l1.getTupleByteArray())){
					permArr[i] = j;
					break;
				}
			}
		}

		//line 8
		primePermArr = new int[n];
		Tuple l2Prime, l1Prime;

		for(int i = 0; i < n; i++){
			l2Prime = _elements.getFromL2Prime(i);

			for(int j = 0; j < n; j++){
				l1Prime = _elements.getFromL1Prime(j);

				if(Arrays.equals(l2Prime.getTupleByteArray(), l1Prime.getTupleByteArray())){
					primePermArr[i] = j;
					break;
				}
			}
		}

		//line 9

		l1Offset = new int[Math.min(m, n)];
		l2Offset = new int[Math.min(m, n)];
		boolean found;

		int l1PrimeVal, l1Val;

		for(int i = 0; i < Math.min(m, n); i++){
			found = false;
			l1 = _elements.getFromL1(i);
			l1Val = l1.getIntFld(r1c1);

			for(int j = 0; j < n; j ++){
				l1Prime = _elements.getFromL1Prime(j);
				l1PrimeVal = l1Prime.getIntFld(r2c1);

				if(_elements.getL1TupleOrder() == TupleOrder.Ascending){
					if(l1Val <= l1PrimeVal){
						l1Offset[i] = j;
						found = true;
					}
				}
				else{
					if(l1Val >= l1PrimeVal){
						l1Offset[i] = j;
						found = true;
					}
				}

				if(found){
					break;
				}
				else{
					l1Offset[i] = j+1;
				}
			}
		}

		//line 10
		int l2PrimeVal, l2Val;

		for(int i = 0; i < Math.min(m, n); i++){
			found = false;
			l2 = _elements.getFromL2(i);
			l2Val = l2.getIntFld(r1c2);

			for(int j = 0; j < n; j ++){
				l2Prime = _elements.getFromL2Prime(j);
				l2PrimeVal = l2Prime.getIntFld(r2c2);

				if(_elements.getL2TupleOrder() == TupleOrder.Ascending){
					if(l2Val <= l2PrimeVal){
						l2Offset[i] = j;
						found = true;
					}
				}
				else{
					if(l2Val >= l2PrimeVal){
						l2Offset[i] = j;
						found = true;
					}
				}

				if(found){
					break;
				}
				else{
					l2Offset[i] = j+1;
				}
			}
		}

		//line 11: initialize bit array B' (|B'| = n) and set all bits to 0
		bitArray = new int[n];

		//line 12

		//line 13-14
		if((op1 == AttrOperator.aopLE || op1 == AttrOperator.aopGE) || (op2 == AttrOperator.aopLE || op2 == AttrOperator.aopGE)){
			eqOff = 0;
		}
		else{
			eqOff = 1;
		}

		/*
		System.out.println("===================");
		System.out.println("PrimePermArr:");
		System.out.println(Arrays.toString(primePermArr));

		System.out.println("PermArr:");
		System.out.println(Arrays.toString(permArr));
		System.out.println("===================");
		*/
	}

	public void getResult() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception{
		Tuple l1 = null, l1Prime = null, outTuple;
		int off2, off1, k;
		//List<Tuple> result = new ArrayList<Tuple>();

		List<FldSpec> projMat = new ArrayList<FldSpec>();
		FldSpec[] permMat;
		boolean outer = true;
		RelSpec spec;
		AttrType[] projAttrs; 

		for(String key : _projRels.keySet()){
			for(Integer col : _projRels.get(key)){
				if(outer){
					spec = new RelSpec(RelSpec.outer);
				}
				else{
					spec = new RelSpec(RelSpec.innerRel);
				}

				projMat.add(new FldSpec(spec, col));
			}

			outer = false;
		}

		projAttrs = new AttrType[projMat.size()];

		for(int i = 0; i < projAttrs.length; i++){
			projAttrs[i] = new AttrType (AttrType.attrInteger);
		}	

		permMat = new FldSpec[projMat.size()];
		projMat.toArray(permMat);

		for(int i = 0; i < m; i++){

			off2 = l2Offset[i];

			for(int j = 0; j < off2; j++){
				/*if(j >= primePermArr.length){
					break;
				}
				 */

				int idx = primePermArr[j];

				if(idx < bitArray.length){
					bitArray[idx] = 1;
				}
			}

			off1 = l1Offset[permArr[i]];
			k = eqOff + off1;
			l1 = _elements.getFromL1(permArr[i]);

			while(k < n){
				l1Prime = _elements.getFromL1Prime(k);

				if(bitArray[k] == 1){
					/*
					System.out.println("ADDING L2 & L2 PRIME");
					System.out.println("L2: ");
					l1.print(_ATTR_TYPES);
					System.out.println("L2 PRIME:");
					l1Prime.print(_ATTR_TYPES);
					 */

					Tuple out1 = new Tuple(), out2 = new Tuple();
					out1.setHdr((short)4, _ATTR_TYPES, null);
					out2.setHdr((short)4, _ATTR_TYPES, null);

					out1.tupleCopy(l1);
					out2.tupleCopy(l1Prime);

					outTuple = new Tuple();

					outTuple.setHdr((short)projAttrs.length, projAttrs, null);

					Projection.Join(out1, _ATTR_TYPES, out2, _ATTR_TYPES, outTuple, permMat, permMat.length);

					outTuple.print(projAttrs);
					//result.add(outTuple);
				}

				k++;
			}
		}

		//return result;
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
	InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
	LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub

	}
}
