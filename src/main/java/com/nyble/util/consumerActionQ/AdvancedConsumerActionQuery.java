package com.nyble.util.consumerActionQ;

import java.util.List;
import java.util.function.Consumer;

public interface AdvancedConsumerActionQuery {

    void query (String whereClause, Consumer<List<CellObject>> processor);
    boolean close() throws Exception;

    String TABLE_CONSUMER_ACTION_MAPPER = "partitions.consumer_action_partition_mapper";
}
