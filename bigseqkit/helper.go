package bigseqkit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/shenwei356/bio/seq"
	"github.com/shenwei356/bio/seqio/fastx"
	"ignis/driver/api"
	"os"
	"path/filepath"
	"strings"
)

var lib_prefix string

func init() {
	lib_prefix = filepath.Join(os.Getenv("IGNIS_HOME"), "core", "go", "bigseqkit.so:")
}

func libSource(name string) *api.ISource {
	return api.NewISource(lib_prefix + name)
}

type SeqKitConfig struct {
	inner KitConfig
}

type KitConfig struct {
	SeqType                *string
	ChunkSize              *int
	BufferSize             *int
	LineWidth              *int
	IDRegexp               *string
	IDNCBI                 *bool
	Quiet                  *bool
	AlphabetGuessSeqLength *int
	ValidateSeqLength      *int
}

func setDefault[T any](pvar **T, val T) {
	if *pvar == nil {
		*pvar = &val
	}
}

func OptionsToString[T any](v T) string {
	var buffer bytes.Buffer
	enc := json.NewEncoder(&buffer)
	err := enc.Encode(v)
	if err != nil {
		panic(err)
	}
	return buffer.String()
}

func StringToOptions[T any](v string) T {
	buffer := bytes.NewBufferString(v)
	dec := json.NewDecoder(buffer)
	val := new(T)
	err := dec.Decode(val)
	if err != nil {
		panic(err)
	}
	return *val
}

func (this *KitConfig) GetAlphabet() (*seq.Alphabet, error) {
	value := *this.SeqType
	switch strings.ToLower(value) {
	case "dna":
		return seq.DNAredundant, nil
	case "rna":
		return seq.RNAredundant, nil
	case "protein":
		return seq.Protein, nil
	case "unlimit":
		return seq.Unlimit, nil
	case "auto":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid sequence type: %s, available value: dna|rna|protein|unlimit|auto", value)
	}
}

func (this *KitConfig) setDefaults() *KitConfig {
	setDefault(&this.SeqType, "auto")
	//setDefault(&this.ChunkSize, )
	//setDefault(&this.BufferSize, )
	setDefault(&this.LineWidth, 60)
	setDefault(&this.IDRegexp, fastx.DefaultIDRegexp)
	setDefault(&this.IDNCBI, false)
	setDefault(&this.Quiet, false)
	setDefault(&this.AlphabetGuessSeqLength, 10000)
	setDefault(&this.ValidateSeqLength, 10000)

	if *this.IDNCBI {
		str := `\|([^\|]+)\| `
		this.IDRegexp = &str
	}

	return this
}

func (this *SeqKitConfig) SeqType(v string) *SeqKitConfig {
	this.inner.SeqType = &v
	return this
}

func (this *SeqKitConfig) LineWidth(v int) *SeqKitConfig {
	this.inner.LineWidth = &v
	return this
}

func (this *SeqKitConfig) IDRegexp(v string) *SeqKitConfig {
	this.inner.IDRegexp = &v
	return this
}

func (this *SeqKitConfig) IDNCBI(v bool) *SeqKitConfig {
	this.inner.IDNCBI = &v
	return this
}

func (this *SeqKitConfig) Quiet(v bool) *SeqKitConfig {
	this.inner.Quiet = &v
	return this
}

func (this *SeqKitConfig) AlphabetGuessSeqLength(v int) *SeqKitConfig {
	this.inner.AlphabetGuessSeqLength = &v
	return this
}

func (this *SeqKitConfig) ValidateSeqLength(v int) *SeqKitConfig {
	this.inner.ValidateSeqLength = &v
	return this
}

func fixer(input *api.IDataFrame[string], delim string) (*api.IDataFrame[string], error) {
	fixer, err := api.AddParam(libSource("ReadFixer"), "delim", delim)
	if err != nil {
		return nil, err
	}
	return api.MapPartitions[string, string](input, fixer)
}

func ReadFASTA(path string, worker *api.IWorker) (*api.IDataFrame[string], error) {
	input, err := worker.PlainFile(path, '>')
	if err != nil {
		return nil, err
	}
	return fixer(input, ">")
}

func ReadFASTAN(path string, minPartitions int64, worker *api.IWorker) (*api.IDataFrame[string], error) {
	input, err := worker.PlainFileN(path, minPartitions, '>')
	if err != nil {
		return nil, err
	}
	return fixer(input, ">")
}

func ReadFASTQ(path string, worker *api.IWorker) (*api.IDataFrame[string], error) {
	input, err := worker.PlainFile(path, '@')
	if err != nil {
		return nil, err
	}
	return fixer(input, "@")
}

func ReadFASTQN(path string, minPartitions int64, worker *api.IWorker) (*api.IDataFrame[string], error) {
	input, err := worker.PlainFileN(path, minPartitions, '@')
	if err != nil {
		return nil, err
	}
	return fixer(input, "@")
}

func StoreFASTX(input *api.IDataFrame[string], path string) error {
	store, err := api.AddParam(libSource("FileStore"), "path", path)
	if err != nil {
		return err
	}
	return input.ForeachPartition(store)
}

func StoreFASTXN(input *api.IDataFrame[string], path string) error {
	return input.SaveAsTextFile(path)
}
