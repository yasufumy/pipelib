from pipelib import Dataset


def complicate(x):
    print('I\'m complicated processing...')
    return x ** 2


if __name__ == '__main__':
    filename = 'processed.dataset'

    data_saved = Dataset(range(100)).map(complicate).save(filename)
    data_loaded = Dataset.load(filename)

    assert data_saved.all() == data_loaded.all()

    print(data_saved.take(10))
    print(data_loaded.take(10))
