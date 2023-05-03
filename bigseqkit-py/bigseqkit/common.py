from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitCommonOptions:

    def __init__(self):
        self.__inner = CommonOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def byName(self, v: bool):
        self.__inner.ByName = v

    def bySeq(self, v: bool):
        self.__inner.BySeq = v

    def ignoreCase(self, v: bool):
        self.__inner.IgnoreCase = v

    def onlyPositiveStrand(self, v: bool):
        self.__inner.OnlyPositiveStrand = v

    def _prepareCommon(self, inputA: IDataFrame, id: str) -> IDataFrame:
        libprepare = _libSource("SeqTransform")\
            .addParam("opts", _optionsToString(self.__inner))\
            .addParam("id", id)
        return inputA.mapPartitions(libprepare)

    def _run(self, inputA: IDataFrame, inputB: IDataFrame, *args: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        u = self._prepareCommon(inputA, "1")

        inputs = [inputB, ] + args

        for i, input in enumerate(inputs):
            pn = self._prepareCommon(input, str(i+2))
            u = u.union(pn, preserveOrder=False)

        grouped = u.toPair().groupByKey()

        join = _libSource("CommonJoin").\
            addParam("opts", _optionsToString(self.__inner)).\
            addParam("ids", str(len(args)+2))

        return grouped.flatmap(join)


class CommonOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.ByName = None  # bool
        self.BySeq = None  # bool
        self.IgnoreCase = None  # bool
        self.OnlyPositiveStrand = None  # bool

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "ByName", False)
        _setDefault(self, "BySeq", False)
        _setDefault(self, "IgnoreCase", False)
        _setDefault(self, "OnlyPositiveStrand", False)

        return self


def common(inputA: IDataFrame, inputB: IDataFrame, *args: IDataFrame, o: SeqKitCommonOptions = None, **kwargs):
    if o is None:
        o = SeqKitCommonOptions()
    return o._run(inputA,inputB, *args, **kwargs)
