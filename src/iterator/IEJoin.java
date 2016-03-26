package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class IEJoin extends Iterator{

	public static AttrType[] _ATTR_TYPES = {new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger),
			new AttrType (AttrType.attrInteger), new AttrType (AttrType.attrInteger)};
	private final FldSpec[] _BASIC_PROJECTION = {new FldSpec(new RelSpec(RelSpec.outer), 1),
			new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
			new FldSpec(new RelSpec(RelSpec.outer), 4)};
	private int[] l1Offset, l2Offset, bitArray, permArr, primePermArr;
	private int eqOff, m, n;
	private Iterator l2It, l2PrimeIt;

	/*
	 * r1.r1c1 op1 r2.r2c1
	 * r1.r1c2 op2 r2.r2c2
	 */
	public IEJoin(String r1, String r2, int r1c1, int r2c1, int r1c2, int r2c2, int op1, int op2) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		n = 0;
		m = 0;
		
		Iterator l1It = null, l1PrimeIt = null;
		FileScan l1Scan = new FileScan(r1 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null),
				l1PrimeScan = new FileScan(r2 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null),
				l2Scan = new FileScan(r1 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null),
				l2PrimeScan = new FileScan(r2 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null);
		
		int l1TupleOrder = 0, l2TupleOrder = 0;
		
		while(l1Scan.get_next() != null){
			m++;
		}
		
		while(l1PrimeScan.get_next() != null){
			n++;
		}
		
		l1Scan = new FileScan(r1 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null);
		l1PrimeScan = new FileScan(r2 + ".in", _ATTR_TYPES, null, (short)4, (short)4, _BASIC_PROJECTION, null);

		//line 3-4
		if(op1 == AttrOperator.aopGT || op1 == AttrOperator.aopGE){
			//Sort in Descending Order
			l1It = new Sort(_ATTR_TYPES, (short)4, null, l1Scan, r1c1, new TupleOrder(TupleOrder.Descending), 4, 10);
			l1PrimeIt = new Sort(_ATTR_TYPES, (short)4, null, l1PrimeScan, r2c1, new TupleOrder(TupleOrder.Descending), 4, 10);
			l1TupleOrder = TupleOrder.Descending;
		}
		else if(op1 == AttrOperator.aopLT || op1 == AttrOperator.aopLE){
			//Sort in Ascending Order
			l1It = new Sort(_ATTR_TYPES, (short)4, null, l1Scan, r1c1, new TupleOrder(TupleOrder.Ascending), 4, 10);
			l1PrimeIt = new Sort(_ATTR_TYPES, (short)4, null, l1PrimeScan, r2c1, new TupleOrder(TupleOrder.Ascending), 4, 10);
			l1TupleOrder = TupleOrder.Ascending;
		}

		//line 5-6
		if(op2 == AttrOperator.aopGT || op2 == AttrOperator.aopGE){
			//Sort in Ascending Order
			l2It = new Sort(_ATTR_TYPES, (short)4, null, l2Scan, r1c2, new TupleOrder(TupleOrder.Ascending), 4, 10);
			l2PrimeIt = new Sort(_ATTR_TYPES, (short)4, null, l2PrimeScan, r2c2, new TupleOrder(TupleOrder.Ascending), 4, 10);
			l2TupleOrder = TupleOrder.Ascending;
		}
		else if(op2 == AttrOperator.aopLT || op2 == AttrOperator.aopLE){
			//Sort in Descending Order
			l2It = new Sort(_ATTR_TYPES, (short)4, null, l2Scan, r1c2, new TupleOrder(TupleOrder.Descending), 4, 10);
			l2PrimeIt = new Sort(_ATTR_TYPES, (short)4, null, l2PrimeScan, r2c2, new TupleOrder(TupleOrder.Descending), 4, 10);
			l2TupleOrder = TupleOrder.Descending;
		}
/*
		Tuple l1, l1Prime;
		
		System.out.println("L1:\n");

		while((l1 = l1It.get_next()) != null){
			l1.print(_ATTR_TYPES);
		}
		System.out.println("L1 Prime:\n");

		while((l1Prime = l1PrimeIt.get_next()) != null){
			l1Prime.print(_ATTR_TYPES);
		}
		
		Tuple l2, l2Prime;
		
		System.out.println("L2:\n");
		while((l2 = l2It.get_next()) != null){
			l2.print(_ATTR_TYPES);
		}
		System.out.println("L2 Prime:\n");

		while((l2Prime = l2PrimeIt.get_next()) != null){
			l2Prime.print(_ATTR_TYPES);
		}
		*/
		//line 7
		permArr = new int[m];
		Tuple l2, l1;
		
		Iterator l2It_temp = (Iterator) l2It.clone(), l1It_temp;

		for(int i = 0; i < m; i++){
			l2 = l2It_temp.get_next();
			l1It_temp = (Iterator) l1It.clone();
						
			for(int j = 0; j < m; j++){
				l1 = l1It_temp.get_next();
				
				if(Arrays.equals(l2.getTupleByteArray(), l1.getTupleByteArray())){
					permArr[i] = j;
					while (l1It_temp.get_next() != null) {}
					break;
				}
			}
		}
		
		while (l2It_temp.get_next() != null) {}
		
		//line 8
		primePermArr = new int[n];
		Tuple l2Prime, l1Prime;

		Iterator l2PrimeIt_temp = (Iterator) l2PrimeIt.clone(), l1PrimeIt_temp;

		for(int i = 0; i < n; i++){
			l2Prime = l2PrimeIt_temp.get_next();
			l1PrimeIt_temp = (Iterator) l1PrimeIt.clone();
						
			for(int j = 0; j < n; j++){
				l1Prime = l1PrimeIt_temp.get_next();
				
				if(Arrays.equals(l2Prime.getTupleByteArray(), l1Prime.getTupleByteArray())){
					primePermArr[i] = j;
					while (l1PrimeIt_temp.get_next() != null) {}
					break;
				}
			}
		}
	
		while (l2PrimeIt_temp.get_next() != null) {}

		//line 9
		
		l1Offset = new int[Math.min(m, n)];
		l2Offset = new int[Math.min(m, n)];
		boolean found;
		
		int l1PrimeVal, l1Val;
		l1It_temp = (Iterator)l1It.clone();
		
		for(int i = 0; i < Math.min(m, n); i++){
			found = false;
			l1 = l1It_temp.get_next();
			l1Val = l1.getIntFld(r1c1);
			
			l1PrimeIt_temp = (Iterator)l1PrimeIt.clone();
			
			for(int j = 0; j < n; j ++){
				l1PrimeVal = l1PrimeIt_temp.get_next().getIntFld(r2c1);
				
				if(l1TupleOrder == TupleOrder.Ascending){
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
					while(l1PrimeIt_temp.get_next() != null){}
					break;
				}
				else{
					l1Offset[i] = j+1;
				}
			}
		}
		
		//line 10
		int l2PrimeVal, l2Val;
		l2It_temp = (Iterator)l2It.clone();
		
		for(int i = 0; i < Math.min(m, n); i++){
			found = false;
			l2 = l2It_temp.get_next();
			l2Val = l2.getIntFld(r1c2);
			
			l2PrimeIt_temp = (Iterator)l2PrimeIt.clone();
			
			for(int j = 0; j < n; j ++){
				l2PrimeVal = l2PrimeIt_temp.get_next().getIntFld(r2c2);
				
				if(l2TupleOrder == TupleOrder.Ascending){
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
					while(l2PrimeIt_temp.get_next() != null){}
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
		if((op1 == AttrOperator.aopLE || op1 == AttrOperator.aopGE) && (op2 == AttrOperator.aopLE || op2 == AttrOperator.aopGE)){
			eqOff = 0;
		}
		else{
			eqOff = 1;
		}
	}
	
	public List<Tuple[]> getResult() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception{
		Tuple l2, l2Prime;
		int off2, off1, k, l2Idx;
		List<Tuple[]> result = new ArrayList<Tuple[]>();
		Iterator l2PrimeIt_temp, l2It_temp = (Iterator) l2It.clone();
		
		for(int i = 0; i < m; i++){
			l2 = l2It_temp.get_next();

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
			l2Idx = 0;
			l2PrimeIt_temp = (Iterator) l2PrimeIt.clone();
			
			while(l2Idx < k){
				l2PrimeIt_temp.get_next();
				l2Idx++;
			}
			
			while(k < n){
				l2Prime = l2PrimeIt_temp.get_next();

				
				if(bitArray[k] == 1){
					System.out.println("ADDING L2 & L2 PRIME");
					System.out.println("L2: ");
					l2.print(_ATTR_TYPES);
					System.out.println("L2 PRIME:");
					l2Prime.print(_ATTR_TYPES);
					
					Tuple out1 = new Tuple(), out2 = new Tuple();
					out1.setHdr((short)4, _ATTR_TYPES, null);
					out2.setHdr((short)4, _ATTR_TYPES, null);

					out1.tupleCopy(l2);
					out2.tupleCopy(l2Prime);
					
					result.add(new Tuple[]{out1, out2});
				}
				k++;
			}
			
			while(l2PrimeIt_temp.get_next() != null){}
		}
		
		return result;
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		
		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		
	}
}
