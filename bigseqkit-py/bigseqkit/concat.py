from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitConcatOptions:

    def __init__(self):
        self.__inner = ConcatOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def full(self, v: bool):
        self.__inner.Full = v

    def separator(self, v: str):
        self.__inner.Separator = v

    def _prepareConcat(self, inputA: IDataFrame, id: str) -> IDataFrame:
        libprepare = _libSource("ConcatPrepare")\
            .addParam("opts", _optionsToString(self.__inner))\
            .addParam("id", id)
        return inputA.mapPartitions(libprepare)
    def _run(self, inputA: IDataFrame, inputB: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        p1 = self._prepareConcat(inputA, "1")
        p2 = self._prepareConcat(inputB, "2")

        u = p1.union(p2, preserveOrder=False)
        grouped = u.toPair().groupByKey()

        join = _libSource("SeqTransform").addParam("opts", _optionsToString(opts))
        return grouped.flatmap(join)

class ConcatOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Full = None  # bool
        self.Separator = None # str

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Full", False)
        _setDefault(self, "Separator", "|")


def concat(inputA: IDataFrame, inputB: IDataFrame, o: SeqKitConcatOptions = None, **kwargs):
    if o is None:
        o = SeqKitConcatOptions()
    return o._run(inputA, inputB, **kwargs)
