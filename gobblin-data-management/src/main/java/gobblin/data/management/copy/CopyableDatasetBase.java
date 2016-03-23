package gobblin.data.management.copy;

import gobblin.dataset.Dataset;


/**
 * A common superinterface for {@link Dataset}s that can be operated on by distcp.
 * Concrete classes must implement a subinterface of this interface ({@link CopyableDataset} or {@link IterableCopyableDataset}).
 */
interface CopyableDatasetBase extends Dataset {
}
