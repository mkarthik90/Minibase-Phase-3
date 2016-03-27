package tests;

import global.AttrOperator;
import global.AttrType;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.File;
import java.util.Scanner;

public class HelpBundle {


    private String Table1, Table2;
    private static final int INNER = 1, OUTER = 0;
    private int outcol1, outtable1, outcol2, outtable2, incol11, incol12, incol21, incol22, op1, op2;
    private int intable11, intable12, intable21, intable22;
    private CondExpr condition[];
    private FldSpec projfs[];
    private AttrType projat[];
    private int outsize;
	
    
	public boolean parseQuery(String filename) {
        File file = new File(filename);

        if (!file.exists()) {
            System.out.println("File at '" + filename + "' not found.");
            return false;
        }

		Table1 = "";
		Table2 = "";
		outcol1 = -1;
		outtable1 = -1;
		outcol2 = -1;
		outtable2 = -1;
		incol11 = -1;
		intable11 = -1;
		incol12 = -1;
		intable12 = -1;
		incol21 = -1;
		intable21 = -1;
		incol22 = -1;
		intable22 = -1;
		op1 = -1;
		op2 = -1;
		condition = new CondExpr[2];
		projfs = null;
		projat = null;
		outsize = -1;
        
        Scanner scan;

        try {
            scan = new Scanner(file);

            // Selection Values
            String line1 = scan.nextLine();
            String line2 = scan.nextLine();
            
            String[] tabs = line2.split(" ");
            Table1 = tabs[0];
            try
            {
            	Table2 = tabs[1];
            }
            catch(ArrayIndexOutOfBoundsException e)
            {}
            
            tabs = line1.split(" ");
            String[] tabs2 = tabs[0].split("_");
            if(tabs2[0].equals(Table1))
            {
            	outtable1 = OUTER;
            }
            else
            {
            	outtable1 = INNER;
            }
            outcol1 = Integer.parseInt(tabs2[1]);
            
            try
            {
            	tabs2 = tabs[1].split("_");
                if(tabs2[0].equals(Table1))
                {
                	outtable2 = OUTER;
                }
                else
                {
                	outtable2 = INNER;
                }
                outcol2 = Integer.parseInt(tabs2[1]);
            }
            catch(ArrayIndexOutOfBoundsException e)
            {}
            
            line1 = scan.nextLine();
            
            tabs = line1.split(" ");
            tabs2 = tabs[0].split("_");
            if(tabs2[0].equals(Table1))
            {
            	intable11 = OUTER;
            }
            else
            {
            	intable11 = INNER;
            }
            incol11 = Integer.parseInt(tabs2[1]);
            op1 = Integer.parseInt(tabs[1]);
            tabs2 = tabs[2].split("_");
            if(tabs2[0].equals(Table1))
            {
            	intable12 = OUTER;
            }
            else
            {
            	intable12 = INNER;
            }
            incol12 = Integer.parseInt(tabs2[1]);
            
            try
            {
            	line1 = scan.nextLine();
            	line1 = scan.nextLine();

                tabs = line1.split(" ");
                tabs2 = tabs[0].split("_");
                if(tabs2[0].equals(Table1))
                {
                	intable21 = OUTER;
                }
                else
                {
                	intable21 = INNER;
                }
                incol21 = Integer.parseInt(tabs2[1]);
                op2 = Integer.parseInt(tabs[1]);
                tabs2 = tabs[2].split("_");
                if(tabs2[0].equals(Table1))
                {
                	intable22 = OUTER;
                }
                else
                {
                	intable22 = INNER;
                }
                incol22 = Integer.parseInt(tabs2[1]);
            }
            catch(Exception e)
            {}
            
            
            condition[0] = new CondExpr();
            condition[0].next  = null;
            condition[0].op    = new AttrOperator(op1);
            condition[0].type1 = new AttrType(AttrType.attrSymbol);
            condition[0].type2 = new AttrType(AttrType.attrSymbol);
            RelSpec rs1 = null;
            if(intable11 == OUTER)
            {
            	rs1 = new RelSpec(RelSpec.outer);
            }
            else
            {
            	rs1 = new RelSpec(RelSpec.innerRel);
            }
            condition[0].operand1.symbol = new FldSpec (rs1, incol11);
            RelSpec rs2 = null;
            if(intable12 == OUTER)
            {
            	rs2 = new RelSpec(RelSpec.outer);
            }
            else
            {
            	rs2 = new RelSpec(RelSpec.innerRel);
            }
            condition[0].operand2.symbol = new FldSpec (rs2, incol12);
            
            
            if(op2 == -1)
            {
            	condition[1] = null;
            }
            else
            {
                condition[1] = new CondExpr();
                condition[1].next  = null;
                condition[1].op    = new AttrOperator(op2);
                condition[1].type1 = new AttrType(AttrType.attrSymbol);
                condition[1].type2 = new AttrType(AttrType.attrSymbol);
                rs1 = null;
                if(intable21 == OUTER)
                {
                	rs1 = new RelSpec(RelSpec.outer);
                }
                else
                {
                	rs1 = new RelSpec(RelSpec.innerRel);
                }
                condition[1].operand1.symbol = new FldSpec (rs1, incol21);
                rs2 = null;
                if(intable22 == OUTER)
                {
                	rs2 = new RelSpec(RelSpec.outer);
                }
                else
                {
                	rs2 = new RelSpec(RelSpec.innerRel);
                }
                condition[1].operand2.symbol = new FldSpec (rs2, incol22);
            }

            if(outcol2 == -1)
            {
            	outsize = 1;
            	projfs = new FldSpec[1];
            	rs1 = null;
            	if(outtable1 == OUTER)
            	{
            		rs1 = new RelSpec(RelSpec.outer);
            	}
            	else
            	{
            		rs1 = new RelSpec(RelSpec.innerRel);
            	}
            	projfs[0] = new FldSpec(rs1, outcol1);
            	projat = new AttrType[1];
            	projat[0] = new AttrType(AttrType.attrInteger);
            }
            else
            {
            	outsize = 2;
            	projfs = new FldSpec[2];
            	rs1 = null;
            	rs2 = null;
            	if(outtable1 == OUTER)
            	{
            		rs1 = new RelSpec(RelSpec.outer);
            	}
            	else
            	{
            		rs1 = new RelSpec(RelSpec.innerRel);
            	}
            	projfs[0] = new FldSpec(rs1, outcol1);
            	if(outtable2 == OUTER)
            	{
            		rs2 = new RelSpec(RelSpec.outer);
            	}
            	else
            	{
            		rs2 = new RelSpec(RelSpec.innerRel);
            	}
            	projfs[1] = new FldSpec(rs2, outcol2);
            	projat = new AttrType[2];
            	projat[0] = new AttrType(AttrType.attrInteger);
            	projat[1] = new AttrType(AttrType.attrInteger);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return true;
    }
	
	public CondExpr[] get_cond()
	{
		return condition;
	}
	
	public FldSpec[] get_fs()
	{
		return projfs;
	}
	
	public AttrType[] get_at()
	{
		return projat;
	}
	
	public int get_out_size()
	{
		return outsize;
	}
	
	public String get_table1()
	{
		return Table1;
	}
	
	public String get_table2()
	{
		return Table2;
	}
}
