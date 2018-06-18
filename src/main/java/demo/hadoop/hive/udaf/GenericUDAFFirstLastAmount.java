package demo.hadoop.hive.udaf;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GenericUDAFFirstLastAmount extends AbstractGenericUDAFResolver{

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		return new FirstLastAmountEvaluator();
	}
	public static class FirstLastAmountEvaluator extends GenericUDAFEvaluator {
		//定义全局输入变量（原始数据输入，中间结果输入变量）
		private PrimitiveObjectInspector inputOI;
		private StandardListObjectInspector loi;
		private StandardListObjectInspector internalMergeOI;
		
		private String typeSameDay;
		
		static class ArrayAggregationBuffer implements AggregationBuffer {
			ArrayList<Tuple> container;
		}
		public static class Tuple {
			public String minCreated;
			public double minAmount;
			public String maxCreated;
			public double maxAmount;
		}
		
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			if(m == Mode.PARTIAL1) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
				return ObjectInspectorFactory.getStandardListObjectInspector(
						ObjectInspectorUtils.getStandardObjectInspector(inputOI)
						);
			}
			//此时输入参数不是原始数据了，是StandardObjectInspector
			else {
				if(!(parameters[0] instanceof StandardListObjectInspector)) {
					inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
					return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
				}
				else {
					internalMergeOI = (StandardListObjectInspector) parameters[0];
					inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
					loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
					return loi;
				}
			}
		}

		//返回一个用户存储中间聚合结果的对象
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
			reset(ret);
			return ret;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((ArrayAggregationBuffer)agg).container = new ArrayList<GenericUDAFFirstLastAmount.FirstLastAmountEvaluator.Tuple>();
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			//type='merge'	合并;
			//type='single' 不用合并
			String created = parameters[0].toString();
			double amount = Double.parseDouble(parameters[1].toString());
			String type = parameters[2].toString();
			
			typeSameDay = type;
			String eL="^\\d{4}-\\d{2}-\\d{2}$";
			Pattern p = Pattern.compile(eL);
			Matcher m = p.matcher(created);
			boolean b = m.matches();
			if(!b) {
				return;
			}
			Tuple resultTuple = new Tuple();
			
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			ArrayAggregationBuffer aggb = (ArrayAggregationBuffer) agg;
			ArrayList<Tuple> ret = new ArrayList<GenericUDAFFirstLastAmount.FirstLastAmountEvaluator.Tuple>(aggb.container.size());
			Tuple partial = new Tuple();
			partial.minCreated=aggb.container.get(0).minCreated;
			partial.minAmount=aggb.container.get(0).minAmount;
			partial.maxCreated=aggb.container.get(0).maxCreated;
			partial.maxAmount=aggb.container.get(0).maxAmount;
			ret.add((Tuple)ObjectInspectorUtils.copyToStandardObject(partial, this.inputOI));
			aggb.container.clear();
			return ret;
		}

		@Override
		public void merge(AggregationBuffer agg, Object o)
				throws HiveException {
			ArrayAggregationBuffer aggb = (ArrayAggregationBuffer) agg;
			ArrayList<Tuple> otherList = (ArrayList<GenericUDAFFirstLastAmount.FirstLastAmountEvaluator.Tuple>)internalMergeOI.getList(o);
			Tuple other = otherList.get(0);
			if(otherList.size()==0) {
				return;
			}
			if(aggb.container.size()==0) {
				aggb.container.add((Tuple)ObjectInspectorUtils.copyToStandardObject(otherList.get(0), this.inputOI));
				return;
			}
			if(aggb.container.get(0).minCreated.compareTo(other.minCreated)>0) {
				aggb.container.get(0).minCreated=other.minCreated;
				aggb.container.get(0).minAmount=other.minAmount;
			}
			if(aggb.container.get(0).maxCreated.compareTo(other.maxCreated)<0) {
				aggb.container.get(0).maxCreated=other.maxCreated;
				aggb.container.get(0).maxAmount=other.maxAmount;
			}
			if(typeSameDay.equalsIgnoreCase("merge")) {
				//同一天不进行合并
				if(aggb.container.get(0).maxCreated.compareTo(other.maxCreated)==0) {
					aggb.container.get(0).maxAmount+=other.maxAmount;
				}
				if(aggb.container.get(0).minCreated.compareTo(other.minCreated)==0) {
					aggb.container.get(0).minAmount+=other.minAmount;
				}
			}
			return;
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			ArrayAggregationBuffer aggb = (ArrayAggregationBuffer) agg;
			ArrayList<Tuple> ret = new ArrayList<GenericUDAFFirstLastAmount.FirstLastAmountEvaluator.Tuple>(aggb.container.size());
			Tuple partial = new Tuple();
			partial.minCreated=aggb.container.get(0).minCreated;
			partial.minAmount=aggb.container.get(0).minAmount;
			partial.maxCreated=aggb.container.get(0).maxCreated;
			partial.maxAmount=aggb.container.get(0).maxAmount;
			ret.add((Tuple)ObjectInspectorUtils.copyToStandardObject(partial, this.inputOI));
			aggb.container.clear();
			return ret;
		}
		
	}
}
