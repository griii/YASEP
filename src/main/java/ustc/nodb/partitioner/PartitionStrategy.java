package ustc.nodb.partitioner;

import ustc.nodb.IO.PartitionResBuffer;

import java.io.IOException;

public interface PartitionStrategy {
    public void performStep();
    public void performStep(PartitionResBuffer partitionResBuffer);
    public void clear();
    public double getReplicateFactor();
    public double getLoadBalance();
}
