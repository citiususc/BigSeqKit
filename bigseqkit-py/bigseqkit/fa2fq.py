from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitFa2FqOptions:

    def __init__(self):
        self.__inner = Fa2FqOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def fastaFile(self, v: str):
        self.__inner.FastaFile = v


    def onlyPositiveStrand(self, v: bool):
        self.__inner.OnlyPositiveStrand = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("Fa2Fq").addParam("opts", _optionsToString(opts))
        return input.mapPartitions(libprepare)


class Fa2FqOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.FastaFile = None  # str
        self.OnlyPositiveStrand = None  # bool

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "FastaFile", "")
        _setDefault(self, "OnlyPositiveStrand", False)


def fa2fq(input: IDataFrame, o: SeqKitFa2FqOptions = None, **kwargs):
    if o is None:
        o = SeqKitFa2FqOptions()
    return o._run(input, **kwargs)
