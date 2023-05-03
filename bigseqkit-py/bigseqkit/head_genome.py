from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitHeadGenomeOptions:

    def __init__(self):
        self.__inner = HeadGenomeOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def headGenomeOptions(self, v: int):
        self.__inner.HeadGenomeOptions = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        firstSeq = input.take(1)

        header = firstSeq.split("\n")[0]
        prefixes = header.split()[1:]

        lib = _libSource("HeadGenome")\
            .addParam("opts", _optionsToString(opts))\
            .addParam("prefixes", prefixes)

        return input.mapPartitionsWithIndex(lib)


class HeadGenomeOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.HeadGenomeOptions = None  # int

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "HeadGenomeOptions", 1)


def headGenome(input: IDataFrame, o: SeqKitHeadGenomeOptions = None, **kwargs):
    if o is None:
        o = SeqKitHeadGenomeOptions()
    return o._run(input, **kwargs)
