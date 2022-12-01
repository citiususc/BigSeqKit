from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitSeqOptions:

    def __init__(self):
        self.__inner = SeqOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def reverse(self, v: bool):
        self.__inner.Reverse = v

    def complement(self, v: bool):
        self.__inner.Complement = v

    def name(self, v: bool):
        self.__inner.Name = v

    def seq(self, v: bool):
        self.__inner.Seq = v

    def qual(self, v: bool):
        self.__inner.Qual = v

    def onlyId(self, v: bool):
        self.__inner.OnlyId = v

    def removeGaps(self, v: bool):
        self.__inner.RemoveGaps = v

    def gapLetters(self, v: str):
        self.__inner.GapLetters = v

    def lowerCase(self, v: bool):
        self.__inner.LowerCase = v

    def upperCase(self, v: bool):
        self.__inner.UpperCase = v

    def dna2rna(self, v: bool):
        self.__inner.Dna2rna = v

    def rna2dna(self, v: bool):
        self.__inner.Rna2dna = v

    def validateSeq(self, v: bool):
        self.__inner.ValidateSeq = v

    def validateSeqLength(self, v: int):
        self.__inner.ValidateSeqLength = v

    def maxLen(self, v: int):
        self.__inner.MaxLen = v

    def minLen(self, v: int):
        self.__inner.MinLen = v

    def qualAsciiBase(self, v: int):
        self.__inner.QualAsciiBase = v

    def minQual(self, v: float):
        self.__inner.MinQual = v

    def MaxQual(self, v: float):
        self.__inner.MaxQual = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("SeqTransform").addParam("opts", _optionsToString(opts))
        return input.mapPartitions(libprepare)


class SeqOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Reverse = None  # bool
        self.Complement = None  # bool
        self.Name = None  # bool
        self.Seq = None  # bool
        self.Qual = None  # bool
        self.OnlyId = None  # bool
        self.RemoveGaps = None  # bool
        self.GapLetters = None  # string
        self.LowerCase = None  # bool
        self.UpperCase = None  # bool
        self.Dna2rna = None  # bool
        self.Rna2dna = None  # bool
        self.ValidateSeq = None  # bool
        self.ValidateSeqLength = None  # int
        self.MaxLen = None  # int
        self.MinLen = None  # int
        self.QualAsciiBase = None  # int
        self.MinQual = None  # float
        self.MaxQual = None  # float

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Reverse", False)
        _setDefault(self, "Complement", False)
        _setDefault(self, "Name", False)
        _setDefault(self, "Seq", False)
        _setDefault(self, "Qual", False)
        _setDefault(self, "OnlyId", False)
        _setDefault(self, "RemoveGaps", False)
        _setDefault(self, "GapLetters", "- 	.")
        _setDefault(self, "LowerCase", False)
        _setDefault(self, "UpperCase", False)
        _setDefault(self, "Dna2rna", False)
        _setDefault(self, "Rna2dna", False)
        _setDefault(self, "ValidateSeq", False)
        _setDefault(self, "ValidateSeqLength", 10000)
        _setDefault(self, "MaxLen", -1)
        _setDefault(self, "MinLen", -1)
        _setDefault(self, "QualAsciiBase", 33)
        _setDefault(self, "MinQual", -1)
        _setDefault(self, "MaxQual", -1)

        return self


def Seq(input: IDataFrame, o: SeqKitSeqOptions = None, **kwargs):
    if o is None:
        o = SeqKitSeqOptions()
    return o._run(input, **kwargs)
