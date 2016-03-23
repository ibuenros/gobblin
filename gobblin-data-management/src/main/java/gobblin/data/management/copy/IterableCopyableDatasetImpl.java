package gobblin.data.management.copy;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import gobblin.data.management.partition.FileSet;
import gobblin.dataset.Dataset;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import org.apache.hadoop.fs.FileSystem;


/**
 * Wraps a {@link CopyableDataset} to produce an {@link IterableCopyableDataset}.
 */
@AllArgsConstructor
public class IterableCopyableDatasetImpl implements IterableCopyableDataset {

  private final CopyableDataset dataset;

  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    List<FileSet<CopyEntity>> fileSets =
        partitionCopyableFiles(this.dataset, this.dataset.getCopyableFiles(targetFs, configuration));
    return fileSets.iterator();
  }

  @Override
  public String datasetURN() {
    return this.dataset.datasetURN();
  }

  private List<FileSet<CopyEntity>> partitionCopyableFiles(Dataset dataset, Collection<? extends CopyEntity> files) {
    Map<String, FileSet.Builder<CopyEntity>> partitionBuildersMaps = Maps.newHashMap();
    for (CopyEntity file : files) {
      if (!partitionBuildersMaps.containsKey(file.getFileSet())) {
        partitionBuildersMaps.put(file.getFileSet(), new FileSet.Builder<>(file.getFileSet(), dataset));
      }
      partitionBuildersMaps.get(file.getFileSet()).add(file);
    }
    return Lists.newArrayList(Iterables.transform(partitionBuildersMaps.values(),
        new Function<FileSet.Builder<CopyEntity>, FileSet<CopyEntity>>() {
          @Nullable
          @Override public FileSet<CopyEntity> apply(FileSet.Builder<CopyEntity> input) {
            return input.build();
          }
        }));
  }
}
