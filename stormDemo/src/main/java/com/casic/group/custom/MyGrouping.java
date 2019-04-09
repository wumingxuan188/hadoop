package com.casic.group.custom;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义分组
 */
public class MyGrouping implements CustomStreamGrouping {
    //接受目标的taskId的集合
    private List<Integer> targetTasks;
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks=targetTasks;
    }
    /**
     * 设置分组的策略,此策略
     * 是发个半数bolt
     * @param taskId
     * @param values
     * @return
     */
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
       List<Integer> taskIds=new ArrayList<Integer>();
        for (int i = 0; i < targetTasks.size()/2; i++) {
            taskIds.add(targetTasks.get(i));
        }
        return taskIds;
    }
}
