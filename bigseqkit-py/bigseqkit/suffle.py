from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitShuffleOptions:

    def __init__(self):
        self.__inner = ShuffleOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def seed(self, v: int):
        self.__inner.Seed = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        n = input.partitions()
        return input.partitionByRandom(n, opts.Seed)

class ShuffleOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Seed = None  # int

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Seed", 23)


def suffle(input: IDataFrame, o: SeqKitShuffleOptions = None, **kwargs):
    if o is None:
        o = SeqKitShuffleOptions()
    return o._run(input, **kwargs)
