package demo.storm.trident.shuffle;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

/**
 * 实现用户自定义的分区，
 * partition分区函数
 * @author qingjian
 *
 */
public class CountryRepartition implements CustomStreamGrouping {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Map<String, Integer> countries = ImmutableMap.of(
      "India",0,
      "Japan",1,
      "United State",2,
      "China",3,
      "Brazil",4
      );
  
  private int tasks = 0;
  
  /**
   * 准备工作
   */
  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
      List<Integer> targetTasks) {
    this.tasks = targetTasks.size();
  }

  /**
   * 该方法实现分区逻辑和确定任务执行的输入tuple数据
   */
  @Override
  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    String country = (String)values.get(0);
    return ImmutableList.of(countries.get(country)% tasks); 
  }

}
