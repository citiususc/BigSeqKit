from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitRmDupOptions:

    def __init__(self):
        self.__inner = RmDupOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def byName(self, v: bool):
        self.__inner.ByName = v

    def bySeq(self, v: bool):
        self.__inner.BySeq = v

    def ignoreCase(self, v: bool):
        self.__inner.IgnoreCase = v

    def dupSeqsFile(self, v: str):
        self.__inner.DupSeqsFile = v

    def dupNumFile(self, v: str):
        self.__inner.DupNumFile = v

    def onlyPositiveStrand(self, v: bool):
        self.__inner.OnlyPositiveStrand = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        revcom = not opts.OnlyPositiveStrand

        if opts.BySeq and opts.ByName:
            raise RuntimeError("only one/none of the flags -s (--by-seq) and -n (--by-name) is allowed")

        if not revcom and not opts.BySeq:
            raise RuntimeError("flag -s (--by-seq) needed when using -P (--only-positive-strand)")

        prepare = _libSource("RmDupPrepare").addParam("opts", _optionsToString(opts))

        prepared = input.mapPartitions(prepare)
        grouped = prepared.toPair().groupByKey()

        check = _libSource("RmDupCheck").addParam("opts", _optionsToString(opts))
        return grouped.flatmap(check)

class RmDupOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.ByName = None  # bool
        self.BySeq = None  # bool
        self.IgnoreCase = None  # bool
        self.DupSeqsFile = None  # string
        self.DupNumFile = None  # string
        self.OnlyPositiveStrand = None  # bool

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "ByName", False)
        _setDefault(self, "BySeq", False)
        _setDefault(self, "IgnoreCase", False)
        _setDefault(self, "DupSeqsFile", "")
        _setDefault(self, "DupNumFile", "")
        _setDefault(self, "OnlyPositiveStrand", False)


def rmDup(input: IDataFrame, o: SeqKitRmDupOptions = None, **kwargs):
    if o is None:
        o = SeqKitRmDupOptions()
    return o._run(input, **kwargs)
