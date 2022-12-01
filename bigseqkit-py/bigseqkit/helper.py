import os
import json
import ignis.driver.api.ISource
from ignis.driver.api.IDataFrame import IDataFrame
from ignis.driver.api.IWorker import IWorker

lib_prefix = os.path.join(os.getenv("IGNIS_HOME"), "core", "go", "bigseqkit.so:")
defaultIDRegexp = r'^(\S+)\s?'


def _libSource(name):
    return ignis.driver.api.ISource.ISource(lib_prefix + name, native=False)


def _setDefault(self, name, val):
    if getattr(self, name) is None:
        setattr(self, name, val)
    attr = getattr(self, name)
    if type(attr) != type(val):
        fname = name[0].lower() + name[1:]
        raise RuntimeError(fname + ": Invalid type. Expected " + type(attr).__name__ + " but got " + type(val).__name__)
    return attr


class _OptionsEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


def _optionsToString(v):
    return json.dumps(v, cls=_OptionsEncoder)


def _parseKargs(obj, kwargs):
    if len(kwargs) > 0:
        if obj.Config is None:
            obj.Config = KitConfig()
        for name, value in kwargs.items():
            cname = name.capitalize()
            if hasattr(obj, cname):
                setattr(obj, cname, value)
            elif hasattr(obj.Config, cname):
                setattr(obj.Config, cname, value)


def _config(sq):
    return sq._SeqKitConfig__inner


class SeqKitConfig:

    def __init__(self):
        self.__inner = KitConfig()

    def seqType(self, v: str):
        self.__inner.SeqType = v

    def lineWidth(self, v: int):
        self.__inner.LineWidth = v

    def IDRegexp(self, v: str):
        self.__inner.IDRegexp = v

    def IDNCBI(self, v: bool):
        self.__inner.IDNCBI = v

    def quiet(self, v: bool):
        self.__inner.Quiet = v

    def alphabetGuessSeqLength(self, v: int):
        self.__inner.AlphabetGuessSeqLength = v

    def validateSeqLength(self, v: int):
        self.__inner.ValidateSeqLength = v

class KitConfig:

    def __init__(self):
        self.SeqType = None  # string
        self.ChunkSize = None  # int
        self.BufferSize = None  # int
        self.LineWidth = None  # int
        self.IDRegexp = None  # string
        self.IDNCBI = None  # bool
        self.Quiet = None  # bool
        self.AlphabetGuessSeqLength = None  # int
        self.ValidateSeqLength = None  # int

    def setDefaults(self):
        _setDefault(self, "SeqType", "auto")
        # _setDefault(self, "ChunkSize", )
        # _setDefault(self, "BufferSize", )
        _setDefault(self, "LineWidth", 60)
        _setDefault(self, "IDRegexp", defaultIDRegexp)
        _setDefault(self, "IDNCBI", False)
        _setDefault(self, "Quiet", False)
        _setDefault(self, "AlphabetGuessSeqLength", 10000)
        _setDefault(self, "ValidateSeqLength", 10000)

        if self.IDNCBI:
            self.IDRegexp = r'\|([^\|]+)\| '

        return self


def _fixer(input: IDataFrame, delim: str):
    fixer = _libSource("ReadFixer").addParam("delim", delim)
    return input.mapPartitions(fixer)


def readFASTA(path: str, worker: IWorker, minPartitions: int = None) -> IDataFrame:
    return _fixer(worker.plainFile(path, minPartitions, delim='>'), delim='>')


def readFASTQ(path: str, worker: IWorker, minPartitions: int = None) -> IDataFrame:
    return _fixer(worker.plainFile(path, minPartitions, delim='@'), delim='@')


def StoreFASTX(input: IDataFrame, path: str):
    store = _libSource("FileStore").addParam("path", path)
    input.foreachPartition(store)


def StoreFASTXN(input: IDataFrame, path: str):
    input.saveAsTextFile(path)
