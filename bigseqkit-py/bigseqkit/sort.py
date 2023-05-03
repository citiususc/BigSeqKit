from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitSortOptions:

    def __init__(self):
        self.__inner = SortOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def inNaturalOrder(self, v: bool):
        self.__inner.InNaturalOrder = v

    def bySeq(self, v: bool):
        self.__inner.BySeq = v

    def byName(self, v: bool):
        self.__inner.ByName = v

    def byLength(self, v: bool):
        self.__inner.ByLength = v

    def byBases(self, v: bool):
        self.__inner.ByBases = v

    def gapLetters(self, v: str):
        self.__inner.GapLetters = v

    def ignoreCase(self, v: bool):
        self.__inner.IgnoreCase = v

    def seqPrefixLength(self, v: int):
        self.__inner.SeqPrefixLength = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        inNaturalOrder = opts.InNaturalOrder
        bySeq = opts.BySeq
        byName = opts.ByName
        byLength = opts.ByLength
        byBases = opts.ByBases
        reverse = opts.Reverse

        if byBases:
            byLength = True
            opts.ByLength = True

        n = 0
        if bySeq:
            n+=1

        if byName:
            n+=1

        if byLength:
            n+=1

        if n > 1:
            raise RuntimeError("only one of the options (byLength), (byName) and (bySeq) is allowed")

        if byLength:
            parser = _libSource("SortParseInputInt").addParam("opts", _optionsToString(opts))
            conv = input.mapPartitions(parser)
            sorted = conv.toPair().sortByKey(not reverse, src=_libSource("SortInt"))
            return sorted.map(_libSource("ValuesIntString"))
        else:
            parser = _libSource("SortParseInputString").addParam("opts", _optionsToString(opts))
            conv = input.mapPartitions(parser)
            sortName = "SortString"
            if not bySeq and inNaturalOrder:
                sortName = "SortNatural"

            sorted = conv.toPair().sortByKey(not reverse, src=_libSource(sortName))
            return sorted.map(_libSource("ValuesStringString"))

class SortOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.InNaturalOrder = None  # bool
        self.BySeq = None  # bool
        self.ByName = None  # bool
        self.ByLength = None  # bool
        self.ByBases = None  # bool
        self.GapLetters = None  # string
        self.Reverse = None  # bool
        self.IgnoreCase = None  # bool
        self.SeqPrefixLength = None  # int > 0

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "InNaturalOrder", False)
        _setDefault(self, "BySeq", False)
        _setDefault(self, "ByName", False)
        _setDefault(self, "ByLength", False)
        _setDefault(self, "ByBases", False)
        _setDefault(self, "GapLetters", "- 	.")
        _setDefault(self, "Reverse", False)
        _setDefault(self, "IgnoreCase", False)
        _setDefault(self, "SeqPrefixLength", 10000)


def sort(input: IDataFrame, o: SeqKitSortOptions = None, **kwargs):
    if o is None:
        o = SeqKitSortOptions()
    return o._run(input, **kwargs)
