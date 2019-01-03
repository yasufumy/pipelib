from pathlib import Path
import cloudpickle

import pipelib


def save_pipeline(filename, dataset):
    assert isinstance(dataset, pipelib.core.PipelinedDataset)

    with open(filename, 'wb') as f:
        cloudpickle.dump(dataset._func, f)


def load_pipeline(filename):
    filepath = Path(filename)

    assert filepath.is_file()

    with filepath.open('rb') as f:
        return cloudpickle.load(f)
